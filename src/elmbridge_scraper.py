"""Elmbridge planning document scraper.

Elmbridge runs an IDeA/GIS-based emaps portal where the document tab is a
server-rendered HTML template with direct ``IAMLink.aspx?docid=…`` anchors.

Document listings live at::

    https://emaps.elmbridge.gov.uk/ebc_planning.aspx
        ?requesttype=parseTemplate
        &template=PlanningPlansAndDocsTab.tmplt
        &Filter=^APPLICATION_NUMBER^='<ref>'
        &appno:PARAM=<ref>...

Doc anchors look like::

    <a title="View or download 'Application Form-3934312.pdf'"
       href="//edocs.elmbridge.gov.uk/IAM/IAMLink.aspx?docid=3934312"
       target="_blank">

Each anchor sits inside a ``<tr>`` whose other cells carry the document type
and date.

The IAMLink endpoint returns the file body even when the response code is 404
(IIS misconfiguration); the scraper accepts any 2xx/4xx response that has a
non-empty body matching a known file magic header.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT
from .generic_route_recovery import build_elmbridge_documents_url

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3

_DOC_HOST = "edocs.elmbridge.gov.uk"
_PORTAL_HOST = "emaps.elmbridge.gov.uk"
_TITLE_RE = re.compile(r"['\"]([^'\"]+?)-(\d+)\.([A-Za-z0-9]{2,5})['\"]")

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


def _detect_extension(content: bytes, content_type: str, hint_ext: str = "") -> str:
    if hint_ext and hint_ext.lower() in {".pdf", ".doc", ".docx", ".jpg", ".png", ".tif", ".gif", ".xls", ".xlsx"}:
        return hint_ext.lower()
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


def parse_elmbridge_documents(html: str, page_url: str) -> list[dict]:
    """Parse IAMLink doc anchors out of an Elmbridge documents tab page."""
    soup = _parse_html(html)
    documents: list[dict] = []
    seen: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = link["href"].strip()
        if "IAMLink.aspx" not in href or "docid=" not in href:
            continue

        # Resolve protocol-relative // URLs against the page.
        document_url = urljoin(page_url, href)
        if document_url in seen:
            continue
        seen.add(document_url)

        title = (link.get("title") or "").strip()
        title_match = _TITLE_RE.search(title)
        if title_match:
            description = title_match.group(1).strip()
            hint_ext = "." + title_match.group(3).lower()
        else:
            description = title.removeprefix("View or download").strip(" '\"")
            hint_ext = ""

        doc_type = ""
        date_published = ""
        row = link.find_parent("tr")
        if row is not None:
            cells = row.find_all("td")
            if len(cells) >= 1:
                doc_type = cells[0].get_text(" ", strip=True)
            if len(cells) >= 2:
                date_published = cells[1].get_text(" ", strip=True)

        documents.append(
            {
                "document_type": doc_type or description,
                "description": description,
                "date_published": date_published,
                "document_url": document_url,
                "drawing_number": "",
                "_hint_ext": hint_ext,
            }
        )

    return documents


def documents_url_for_reference(reference: str) -> str:
    return build_elmbridge_documents_url(reference)


class ElmbridgeDocumentScraper:
    """Async scraper for Elmbridge emaps document listings."""

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

    async def __aenter__(self) -> "ElmbridgeDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
            raise RuntimeError("Use 'async with ElmbridgeDocumentScraper() as scraper:'")
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

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Scrape one Elmbridge document listing."""
        try:
            resp = await self._get(docs_url)
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

        documents = parse_elmbridge_documents(resp.text, str(resp.url))
        if not documents:
            self.stats["no_docs"] += 1
            return [], None
        self.stats["success"] += 1
        return documents, None

    async def download_document(
        self,
        doc_url: str,
        target_path: Path,
        referer: str = "",
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Download one Elmbridge document via IAMLink.

        Note: the IAMLink endpoint sometimes returns HTTP 404 alongside a
        valid file body (IIS misconfiguration). We accept the body if its
        magic bytes match a known file type.
        """
        domain = urlparse(doc_url).netloc
        hint_ext = ""
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                headers = {"Referer": referer} if referer else {}
                resp = await self.client.get(doc_url, headers=headers)
                content = resp.content or b""
                ct = resp.headers.get("Content-Type", "").lower()

                looks_like_file = bool(content) and (
                    any(content[:8].startswith(sig) for sig, _ in _MAGIC_EXTS)
                    or ct.startswith(("application/", "image/"))
                )

                if resp.status_code == 200 and looks_like_file:
                    ext = _detect_extension(content, ct, hint_ext)
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(content)
                    return len(content), str(final_path)

                # IIS 404 with a real body — accept if magic bytes match.
                if resp.status_code == 404 and looks_like_file:
                    ext = _detect_extension(content, ct, hint_ext)
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(content)
                    return len(content), str(final_path)

                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue

                logger.warning("HTTP %s for %s (looks_like_file=%s)", resp.status_code, doc_url, looks_like_file)
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
