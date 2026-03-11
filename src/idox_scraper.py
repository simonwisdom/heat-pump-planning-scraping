"""Idox planning portal document metadata scraper.

Fetches and parses document listings from Idox council planning portals.
Works across common Idox variants by dynamically parsing table headers rather
than using fixed column indices.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Callable
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)


class DomainRateLimiter:
    """Rate limiter that tracks per-domain request timing."""

    def __init__(
        self,
        per_domain_delay: float = IDOX_RATE_LIMIT_PER_DOMAIN,
        max_concurrent: int = IDOX_MAX_CONCURRENT_DOMAINS,
    ):
        self.per_domain_delay = per_domain_delay
        self._last_request: dict[str, float] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def acquire(self, domain: str):
        await self._semaphore.acquire()
        last = self._last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.per_domain_delay:
            await asyncio.sleep(self.per_domain_delay - elapsed)

    def release(self, domain: str):
        self._last_request[domain] = time.monotonic()
        self._semaphore.release()


def parse_idox_documents(html: str, base_url: str) -> list[dict]:
    """Parse document metadata from an Idox portal documents-tab page."""
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", id="Documents")
    if not table:
        logger.warning("No <table id='Documents'> found in HTML")
        return []

    headers = []
    header_row = table.find("tr")
    if header_row:
        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True).lower())

    header_map = {h: i for i, h in enumerate(headers)}
    date_idx = header_map.get("date published")
    type_idx = header_map.get("document type")
    desc_idx = header_map.get("description")
    drawing_idx = header_map.get("drawing number")

    documents = []
    rows = table.find_all("tr")[1:]

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        pdf_link = None
        for link in row.find_all("a", href=True):
            href = link["href"]
            if "/files/" in href and href.endswith(".pdf"):
                pdf_link = href
                break

        if not pdf_link:
            checkbox = row.find("input", {"type": "checkbox", "name": "file"})
            if checkbox and checkbox.get("value", "").endswith(".pdf"):
                val = checkbox["value"]
                prefix_match = re.search(r"(/[^\"]+)/files/", html)
                if prefix_match:
                    pdf_link = f"{prefix_match.group(1)}/files/{val}"

        def cell_text(idx: int | None) -> str:
            if idx is not None and idx < len(cells):
                return cells[idx].get_text(strip=True)
            return ""

        doc = {
            "date_published": cell_text(date_idx),
            "document_type": cell_text(type_idx),
            "description": cell_text(desc_idx),
            "drawing_number": cell_text(drawing_idx) if drawing_idx is not None else "",
            "document_url": urljoin(base_url, pdf_link) if pdf_link else None,
        }

        if not doc["document_type"] and not doc["description"]:
            continue

        documents.append(doc)

    return documents


class IdoxDocumentScraper:
    """Async scraper for Idox planning portal document listings."""

    def __init__(self, rate_limiter: DomainRateLimiter | None = None):
        self.rate_limiter = rate_limiter or DomainRateLimiter()
        self._client: httpx.AsyncClient | None = None
        self._insecure_client: httpx.AsyncClient | None = None
        self.stats = {
            "success": 0,
            "failed": 0,
            "no_docs": 0,
            "captcha_blocked": 0,
            "tls_retry_used": 0,
        }

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": IDOX_USER_AGENT},
            follow_redirects=True,
        )
        self._insecure_client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": IDOX_USER_AGENT},
            follow_redirects=True,
            verify=False,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
        if self._insecure_client:
            await self._insecure_client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with IdoxDocumentScraper() as scraper:'")
        return self._client

    @property
    def insecure_client(self) -> httpx.AsyncClient:
        if not self._insecure_client:
            raise RuntimeError("Use 'async with IdoxDocumentScraper() as scraper:'")
        return self._insecure_client

    @staticmethod
    def _is_tls_verification_error(exc: Exception) -> bool:
        message = str(exc)
        return "CERTIFICATE_VERIFY_FAILED" in message or "certificate verify failed" in message

    async def scrape_documents(self, docs_url: str) -> list[dict]:
        parsed = urlparse(docs_url)
        domain = parsed.netloc
        base_url = f"{parsed.scheme}://{domain}"

        url = docs_url
        if "activeTab=documents" not in url:
            if "activeTab=" in url:
                url = re.sub(r"activeTab=\w+", "activeTab=documents", url)
            elif "?" in url:
                url += "&activeTab=documents"
            else:
                url += "?activeTab=documents"

        try:
            await self.rate_limiter.acquire(domain)
            try:
                resp = await self.client.get(url)
            except httpx.HTTPError as exc:
                if not self._is_tls_verification_error(exc):
                    raise
                logger.warning("TLS verification failed for %s; retrying insecurely", domain)
                self.stats["tls_retry_used"] += 1
                resp = await self.insecure_client.get(url)
            finally:
                self.rate_limiter.release(domain)

            if resp.status_code != 200:
                logger.warning("HTTP %s for %s", resp.status_code, url)
                self.stats["failed"] += 1
                return []

            html = resp.text
            if "captcha" in html.lower():
                logger.warning("Possible CAPTCHA/block page for %s", url)
                self.stats["captcha_blocked"] += 1
                return []

            documents = parse_idox_documents(html, base_url)
            if documents:
                self.stats["success"] += 1
            else:
                self.stats["no_docs"] += 1

            return documents

        except httpx.HTTPError as exc:
            logger.error("HTTP error for %s: %s", url, exc)
            self.stats["failed"] += 1
            return []
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", url, exc)
            self.stats["failed"] += 1
            return []

    async def scrape_batch(
        self,
        applications: list[dict],
        on_result: Callable | None = None,
    ) -> dict[str, list[dict]]:
        results: dict[str, list[dict]] = {}

        for app in applications:
            uid = app["uid"]
            docs_url = app.get("documentation_url")
            if not docs_url:
                logger.debug("No docs_url for %s, skipping", uid)
                continue

            documents = await self.scrape_documents(docs_url)
            results[uid] = documents

            if on_result:
                on_result(uid, documents)

        return results
