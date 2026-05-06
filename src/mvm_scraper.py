"""MVM planning portal document scraper.

MVM document viewer pages are ASP.NET WebForms pages whose public document
list is rendered server-side in ``table#tblContent``. Each row contains a
direct link to a file beneath ``/MVM.DMS/<search type>/<bucket>/<pk>/...``.

Observed listing URL pattern:

    /MVM/Online/DMS/DocumentViewer.aspx?PK=<id>&SearchType=Planning%20Application

The casing of ``DMS`` and query keys varies by authority, but the document
table and direct file link pattern are consistent for reachable samples.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3

_FILE_EXT_RE = re.compile(r"\.(?:pdf|docx?|xlsx?|tiff?|jpe?g|png|gif|txt)\b", re.IGNORECASE)

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
    "image/tiff": ".tif",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "text/plain": ".txt",
    "text/html": ".html",
}


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


def _detect_extension(content: bytes, content_type: str) -> str:
    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            if ext == ".zip":
                ct = (content_type or "").split(";")[0].strip().lower()
                office = _CTYPE_EXTS.get(ct)
                if office:
                    return office
            return ext
    ct = (content_type or "").split(";")[0].strip().lower()
    return _CTYPE_EXTS.get(ct, ".pdf")


def _filename_from_link(text: str, href: str) -> str:
    filename = text.strip()
    if filename:
        return filename
    path_leaf = unquote(urlparse(href).path.rstrip("/").rsplit("/", 1)[-1])
    return path_leaf or "document"


def _doc_type_from_filename(filename: str) -> str:
    stem = re.sub(_FILE_EXT_RE, "", filename).strip()
    stem = re.sub(r"^[A-Z]{0,3}/?\d[\w/.-]*\s+", "", stem).strip()
    return stem or "Document"


def parse_mvm_documents(html: str, base_url: str) -> list[dict]:
    """Parse public MVM document links from ``table#tblContent``."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="tblContent")
    if table is None:
        return []

    documents: list[dict] = []
    seen_urls: set[str] = set()

    for link in table.find_all("a", href=True):
        href = link["href"].strip()
        if not href:
            continue
        if "MVM.DMS" not in href and not _FILE_EXT_RE.search(href):
            continue

        doc_url = urljoin(base_url, href)
        if doc_url in seen_urls:
            continue
        seen_urls.add(doc_url)

        filename = _filename_from_link(link.get_text(" ", strip=True), href)
        documents.append(
            {
                "date_published": "",
                "document_type": _doc_type_from_filename(filename),
                "description": filename,
                "drawing_number": "",
                "document_url": doc_url,
                "filename": filename,
            }
        )

    return documents


class MvmDocumentScraper:
    """Async scraper for MVM document viewer pages."""

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
        self._insecure_client: httpx.AsyncClient | None = None
        self.stats = {"success": 0, "failed": 0, "no_docs": 0, "tls_retry_used": 0}

    async def __aenter__(self) -> "MvmDocumentScraper":
        headers = {
            "User-Agent": IDOX_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        self._client = httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True)
        self._insecure_client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers,
            follow_redirects=True,
            verify=False,
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._client is not None:
            await self._client.aclose()
        if self._insecure_client is not None:
            await self._insecure_client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use 'async with MvmDocumentScraper() as scraper:'")
        return self._client

    @property
    def insecure_client(self) -> httpx.AsyncClient:
        if self._insecure_client is None:
            raise RuntimeError("Use 'async with MvmDocumentScraper() as scraper:'")
        return self._insecure_client

    async def _rate_limit(self, domain: str) -> None:
        await self._semaphore.acquire()
        last = self._last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.per_domain_delay:
            await asyncio.sleep(self.per_domain_delay - elapsed)

    def _release(self, domain: str) -> None:
        self._last_request[domain] = time.monotonic()
        self._semaphore.release()

    @staticmethod
    def _is_tls_verification_error(exc: Exception) -> bool:
        message = str(exc)
        return "CERTIFICATE_VERIFY_FAILED" in message or "certificate verify failed" in message

    async def _get(self, url: str, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            try:
                return await self.client.get(url, **kwargs)
            except Exception as exc:
                if not self._is_tls_verification_error(exc):
                    raise
                logger.warning("TLS verification failed for %s; retrying insecurely", domain)
                self.stats["tls_retry_used"] += 1
                return await self.insecure_client.get(url, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Scrape an MVM document listing.

        Returns ``(documents, failure_code)``. ``failure_code`` is ``None`` on
        successful page fetches, including empty listings.
        """
        try:
            resp = await self._get(docs_url)
            if resp.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(resp.status_code)

            parsed = urlparse(str(resp.url))
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            documents = parse_mvm_documents(resp.text, base_url)
        except httpx.ConnectTimeout:
            self.stats["failed"] += 1
            return [], "connect_timeout"
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
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "unexpected_error"

        if documents:
            self.stats["success"] += 1
        else:
            self.stats["no_docs"] += 1
        return documents, None

    async def download_document(
        self,
        doc_url: str,
        target_path: Path,
        referer: str = "",
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Download one MVM document, fixing the extension from response bytes."""
        domain = urlparse(doc_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                headers = {"Referer": referer} if referer else {}
                resp = await self.client.get(doc_url, headers=headers)
                if resp.status_code == 200:
                    if not resp.content:
                        logger.warning("Empty 200 response for %s", doc_url)
                        return 0, str(target_path)
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
                    logger.info(
                        "%s for %s (attempt %d/%d)",
                        type(exc).__name__,
                        doc_url,
                        attempt + 1,
                        max_retries,
                    )
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
