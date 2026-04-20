"""Idox Publisher document metadata scraper."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlparse

import httpx

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

logger = logging.getLogger(__name__)

_AJAX_URL_RE = re.compile(r'["\'](/publisher/mvc/getDocumentList[^"\']*)["\']')


def parse_publisher_documents(json_data: dict, base_url: str) -> list[dict]:
    rows = json_data.get("data", [])
    if not rows:
        if json_data.get("serviceError"):
            logger.warning("Publisher service error: %s", json_data["serviceError"])
        return []

    documents = []
    for row in rows:
        if len(row) < 4:
            continue
        date_str = row[0] or ""
        description = row[1] or ""
        doc_type = row[2] or ""
        doc_path = row[3] or ""
        # The JS that wires up the View column prepends "/publisher" to this path
        # (see ctx = "/publisher" in the listing page script). Constructing the
        # URL without this prefix produces a 403/404.
        if doc_path:
            if doc_path.startswith(("http://", "https://")):
                doc_url = doc_path
            elif doc_path.startswith("/publisher/"):
                doc_url = f"{base_url}{doc_path}"
            else:
                doc_url = f"{base_url}/publisher{doc_path if doc_path.startswith('/') else '/' + doc_path}"
        else:
            doc_url = None
        doc = {
            "date_published": date_str,
            "document_type": doc_type,
            "description": description,
            "drawing_number": "",
            "document_url": doc_url,
        }
        if not doc_type and not description:
            continue
        documents.append(doc)
    return documents


def extract_ajax_url(html: str) -> str | None:
    match = _AJAX_URL_RE.search(html)
    return match.group(1) if match else None


class PublisherDocumentScraper:
    """Async scraper for Idox Publisher document listings."""

    def __init__(
        self,
        per_domain_delay: float = IDOX_RATE_LIMIT_PER_DOMAIN,
        max_concurrent: int = IDOX_MAX_CONCURRENT_DOMAINS,
    ):
        self.per_domain_delay = per_domain_delay
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request: dict[str, float] = {}
        self._client: httpx.AsyncClient | None = None
        self.stats = {"success": 0, "failed": 0, "no_docs": 0, "ajax_missing": 0}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with PublisherDocumentScraper() as scraper:'")
        return self._client

    async def _rate_limit(self, domain: str):
        await self._semaphore.acquire()
        last = self._last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.per_domain_delay:
            await asyncio.sleep(self.per_domain_delay - elapsed)

    def _release(self, domain: str):
        self._last_request[domain] = time.monotonic()
        self._semaphore.release()

    async def _get_with_rate_limit(self, url: str, domain: str, **kwargs) -> httpx.Response:
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            return await self.client.get(url, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def scrape_documents(self, docs_url: str) -> list[dict]:
        parsed = urlparse(docs_url)
        domain = parsed.netloc
        base_url = f"{parsed.scheme}://{domain}"
        try:
            resp = await self._get_with_rate_limit(docs_url, domain)
            if resp.status_code != 200:
                logger.warning("HTTP %s for %s", resp.status_code, docs_url)
                self.stats["failed"] += 1
                return []

            ajax_path = extract_ajax_url(resp.text)
            if not ajax_path:
                logger.warning("No AJAX endpoint found in %s", docs_url)
                self.stats["ajax_missing"] += 1
                return []

            ajax_url = urljoin(base_url, ajax_path)
            ajax_resp = await self._get_with_rate_limit(
                ajax_url,
                domain,
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                },
            )
            if ajax_resp.status_code != 200:
                logger.warning("AJAX HTTP %s for %s", ajax_resp.status_code, ajax_url)
                self.stats["failed"] += 1
                return []

            json_data = ajax_resp.json()
            documents = parse_publisher_documents(json_data, base_url)
            if not documents:
                self.stats["no_docs"] += 1
            else:
                self.stats["success"] += 1
            return documents

        except httpx.HTTPError as exc:
            logger.error("HTTP error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return []
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return []

    async def download_document(
        self,
        document_url: str,
        target_path: Path,
        referer: str,
        max_retries: int = 3,
    ) -> int:
        """Download a single document to disk. Returns bytes written, or 0 on failure.

        Publisher issues session-scoped hashes, so this must be called from the same
        scraper/session that produced the document_url via scrape_documents().
        Retries on transient errors (timeouts, 429, 5xx) with exponential backoff.
        Accepts any content-type on a 200 response; Publisher serves PDFs, TIFFs,
        emails (.msg), etc.
        """
        parsed = urlparse(document_url)
        domain = parsed.netloc

        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                resp = await self.client.get(document_url, headers={"Referer": referer})
                if resp.status_code == 200:
                    if not resp.content:
                        logger.warning("Empty 200 response for %s", document_url)
                        return 0
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    target_path.write_bytes(resp.content)
                    return len(resp.content)
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                    backoff = 2**attempt
                    logger.info(
                        "HTTP %s for %s (attempt %d/%d), retrying in %ds",
                        resp.status_code,
                        document_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning("HTTP %s for %s", resp.status_code, document_url)
                return 0
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    backoff = 2**attempt
                    logger.info(
                        "%s for %s (attempt %d/%d), retrying in %ds",
                        type(exc).__name__,
                        document_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error("Network error for %s: %s", document_url, exc)
                return 0
            except Exception as exc:
                logger.error("Download error for %s: %s", document_url, exc)
                return 0
            finally:
                if acquired:
                    self._release(domain)
        return 0

    async def scrape_batch(
        self,
        applications: list[dict],
        on_result: Callable | None = None,
    ) -> dict[str, list[dict]]:
        results = {}
        for app in applications:
            uid = app["uid"]
            docs_url = app.get("documentation_url")
            if not docs_url:
                continue
            documents = await self.scrape_documents(docs_url)
            results[uid] = documents
            if on_result:
                on_result(uid, documents)
        return results
