"""Herefordshire planning document scraper.

The Herefordshire pipeline is a three-step lookup keyed on the application
reference (e.g. ``P214393/L``):

1. Search API at ``restservices.herefordshire.gov.uk/search/planning`` returns
   an application ``id``.
2. Detail page at ``herefordshire.gov.uk/.../details?id=<id>&search=<ref>``
   carries a static ``<div id="planning-application-documents">`` with
   ``<li>`` rows.
3. Each ``<li>`` has an anchor pointing at
   ``myaccount.herefordshire.gov.uk/documents?id=<uuid>`` which streams the
   file directly.

Steps 1–2 are already implemented in ``generic_route_recovery``; this scraper
reuses that and adds doc extraction + download.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT
from .generic_route_recovery import (
    _pick_herefordshire_application_id,
    build_herefordshire_detail_url,
    build_herefordshire_search_api_url,
)

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3

DOCS_HOST = "myaccount.herefordshire.gov.uk"
DOCS_PATH_PREFIX = "/documents"

_MAGIC_EXTS: list[tuple[bytes, str]] = [
    (b"%PDF", ".pdf"),
    (b"PK\x03\x04", ".zip"),
    (b"\xd0\xcf\x11\xe0", ".doc"),
    (b"II*\x00", ".tif"),
    (b"MM\x00*", ".tif"),
    (b"\x89PNG", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"GIF8", ".gif"),
]

_CTYPE_EXTS = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/tiff": ".tif",
    "text/plain": ".txt",
}


def _parse_html(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def _detect_extension(content: bytes, content_type: str) -> str:
    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            return ext
    ct = (content_type or "").split(";", 1)[0].strip().lower()
    return _CTYPE_EXTS.get(ct, ".pdf")


def _http_status_failure_code(status_code: int) -> str:
    if status_code == 403:
        return "http_403"
    if status_code == 404:
        return "http_404"
    if status_code == 429:
        return "http_429"
    if 500 <= status_code < 600:
        return "http_5xx"
    return f"http_{status_code}"


def parse_herefordshire_documents(html: str, page_url: str) -> list[dict]:
    """Pull doc metadata out of the application detail page."""
    soup = _parse_html(html)
    section = soup.find(id="planning-application-documents")
    if section is None:
        return []

    documents: list[dict] = []
    seen: set[str] = set()

    for link in section.find_all("a", href=True):
        href = link["href"].strip()
        if DOCS_HOST not in href or "id=" not in href:
            continue

        document_url = urljoin(page_url, href)
        if document_url in seen:
            continue
        seen.add(document_url)

        description = link.get_text(" ", strip=True)
        title = (link.get("title") or "").strip()
        if title.lower().startswith("view "):
            title = title[5:].strip()

        # File size sibling (e.g. <span class="fileSize">178KB</span>)
        size_text = ""
        size_span = link.find_next_sibling("span", class_="fileSize") or link.find_next("span", class_="fileSize")
        if size_span is not None:
            size_text = size_span.get_text(" ", strip=True)

        documents.append(
            {
                "document_type": title or description,
                "description": description,
                "date_published": "",
                "drawing_number": "",
                "document_url": document_url,
                "file_size_text": size_text,
            }
        )

    return documents


def application_reference_from_row(row: Mapping[str, Any]) -> str:
    return (row.get("reference") or row.get("uid") or "").strip()


class HerefordshireDocumentScraper:
    """Async scraper for Herefordshire planning applications."""

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
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self) -> "HerefordshireDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._client is not None:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use 'async with HerefordshireDocumentScraper() as scraper:'")
        return self._client

    async def _rate_limit(self, domain: str) -> None:
        await self._semaphore.acquire()
        last = self._last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.per_domain_delay:
            await asyncio.sleep(self.per_domain_delay - elapsed)

    def _release(self, domain: str) -> None:
        self._last_request[domain] = time.monotonic()
        self._semaphore.release()

    async def _get(self, url: str, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            return await self.client.get(url, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def _resolve_application_id(self, reference: str) -> tuple[str | None, str | None]:
        """Search API → application id. Returns (app_id, failure_code)."""
        try:
            resp = await self._get(build_herefordshire_search_api_url(reference))
        except httpx.TimeoutException:
            return None, "timeout"
        except httpx.ConnectError as exc:
            message = str(exc).lower()
            if "nodename nor servname" in message or "name or service not known" in message:
                return None, "dns_error"
            return None, "connect_error"
        except httpx.HTTPError:
            return None, "network_error"

        if resp.status_code != 200:
            return None, _http_status_failure_code(resp.status_code)

        try:
            data = resp.json()
        except ValueError:
            return None, "search_invalid_json"

        app_id = _pick_herefordshire_application_id(data)
        if not app_id:
            return None, "search_no_match"
        return app_id, None

    async def scrape_documents(
        self,
        docs_url: str,
        application_reference: str = "",
    ) -> tuple[list[dict], str | None]:
        """Resolve the detail page for ``application_reference`` and parse docs.

        ``docs_url`` is ignored (kept for the runner interface) — the detail
        URL is derived from the reference via the search API.
        """
        reference = (application_reference or "").strip()
        if not reference:
            self.stats["failed"] += 1
            return [], "no_application_reference"

        app_id, code = await self._resolve_application_id(reference)
        if code:
            self.stats["failed"] += 1
            return [], code

        detail_url = build_herefordshire_detail_url(app_id, reference)
        try:
            resp = await self._get(detail_url)
        except httpx.TimeoutException:
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.ConnectError as exc:
            self.stats["failed"] += 1
            message = str(exc).lower()
            if "nodename nor servname" in message or "name or service not known" in message:
                return [], "dns_error"
            return [], "connect_error"
        except httpx.HTTPError:
            self.stats["failed"] += 1
            return [], "network_error"

        if resp.status_code != 200:
            self.stats["failed"] += 1
            return [], _http_status_failure_code(resp.status_code)

        documents = parse_herefordshire_documents(resp.text, detail_url)
        if not documents:
            self.stats["no_docs"] += 1
            return [], None
        # Annotate each doc with the detail URL so downloaders can use it as referer.
        for doc in documents:
            doc.setdefault("listing_url", detail_url)
        self.stats["success"] += 1
        return documents, None

    async def download_document(
        self,
        doc_url: str,
        target_path: Path,
        referer: str = "",
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Download one document from myaccount.herefordshire.gov.uk."""
        domain = urlparse(doc_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                headers = {"Referer": referer} if referer else {}
                resp = await self.client.get(doc_url, headers=headers)
                if resp.status_code == 200 and resp.content:
                    ext = _detect_extension(resp.content, resp.headers.get("Content-Type", ""))
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(resp.content)
                    return len(resp.content), str(final_path)
                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.warning("HTTP %s for %s", resp.status_code, doc_url)
                return 0, str(target_path)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Download network error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            finally:
                if acquired:
                    self._release(domain)
        return 0, str(target_path)
