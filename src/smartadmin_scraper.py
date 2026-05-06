"""SmartAdmin/Tascomi planning portal document scraper.

SmartAdmin councils expose a server-rendered search form at:

    https://{host}/planning/index.html?fa=search

The application reference is submitted via POST to ``/planning/index.html``.
Search results include a ``button.view_application`` with a numeric
``data-id``. The application detail page is:

    /planning/index.html?fa=getApplication&id=<public_record_id>

The detail page renders a document table with direct download links:

    /planning/?fa=downloadDocument&id=<document_id>&public_record_id=<id>

Some Idox-hosted SmartAdmin/Tascomi portals currently return a WAF/edge block
page (HTTP 406, title ``Error (IDX002)``) to non-browser/data-centre traffic.
This scraper detects that separately from a legitimate empty document list.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3

_IDX002_RE = re.compile(r"Error\s*\(IDX002\)|The page you requested cannot be loaded", re.I)

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
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.ms-excel": ".xls",
    "text/plain": ".txt",
    "text/html": ".html",
}


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


def looks_like_idx002_block(html: str) -> bool:
    """Return True for the SmartAdmin/Idox edge block page."""
    return bool(_IDX002_RE.search(html or ""))


def _root_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _normalise_search_url(docs_url: str) -> str:
    """Return a SmartAdmin planning search URL for any URL on the portal host."""
    parsed = urlparse(docs_url)
    host = parsed.netloc.lower()

    # Coventry's legacy portal is a notice page pointing at the current
    # SmartAdmin host.
    if host == "planningsearch.coventry.gov.uk":
        return "https://planandregulatory.coventry.gov.uk/planning/index.html?fa=search"

    return f"{parsed.scheme}://{parsed.netloc}/planning/index.html?fa=search"


def _http_status_failure_code(status_code: int, html: str = "") -> str:
    if status_code == 406 and looks_like_idx002_block(html):
        return "waf_idx002"
    if status_code == 403:
        return "http_403"
    if status_code == 404:
        return "http_404"
    if status_code == 429:
        return "http_429"
    if 500 <= status_code < 600:
        return "http_5xx"
    return f"http_{status_code}"


def parse_smartadmin_application_ids(html: str) -> list[dict]:
    """Parse application search results into ``[{reference, public_record_id}]``."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="application_results_table")
    if not table:
        return []

    results: list[dict] = []
    for row in table.find_all("tr"):
        button = row.find("button", class_=re.compile(r"\bview_application\b"))
        if not button:
            continue
        public_record_id = (button.get("data-id") or "").strip()
        if not public_record_id:
            continue

        ref_cell = row.find("td", attrs={"data-label": re.compile("Application Reference", re.I)})
        reference = ref_cell.get_text(" ", strip=True) if ref_cell else ""
        results.append({"reference": reference, "public_record_id": public_record_id})
    return results


def parse_smartadmin_documents(html: str, base_url: str) -> list[dict]:
    """Parse document rows from a SmartAdmin/Tascomi application detail page."""
    soup = BeautifulSoup(html, "html.parser")
    documents: list[dict] = []
    seen: set[str] = set()

    for link in soup.find_all("a", href=lambda h: h and "fa=downloadDocument" in h):
        href = link.get("href", "")
        doc_url = urljoin(base_url, href)
        qs = parse_qs(urlparse(doc_url).query)
        doc_id = (qs.get("id") or [""])[0]
        if doc_id and doc_id in seen:
            continue
        if doc_id:
            seen.add(doc_id)

        row = link.find_parent("tr")
        document_type = ""
        description = ""
        date_published = ""

        if row:
            type_cell = row.find("td", attrs={"data-field-name": "document_type"})
            desc_cell = row.find("td", attrs={"data-field-name": "description"})
            date_cell = row.find("td", attrs={"data-field-name": "date_document_added"})
            document_type = type_cell.get_text(" ", strip=True) if type_cell else ""
            description = desc_cell.get_text(" ", strip=True) if desc_cell else ""
            if date_cell:
                date_published = (date_cell.get("data-date-value") or date_cell.get_text(" ", strip=True)).strip()

        documents.append(
            {
                "date_published": date_published,
                "document_type": document_type,
                "description": description,
                "drawing_number": "",
                "document_url": doc_url,
                "document_id": doc_id,
            }
        )

    return documents


class SmartAdminDocumentScraper:
    """Async scraper for SmartAdmin/Tascomi document listings.

    ``scrape_documents`` needs an application reference because SmartAdmin
    samples often store only the generic search URL in ``documentation_url``.
    """

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
        self.stats = {"success": 0, "failed": 0, "no_docs": 0, "waf_blocked": 0}

    async def __aenter__(self) -> "SmartAdminDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use 'async with SmartAdminDocumentScraper() as scraper:'")
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

    async def _post(self, url: str, data: dict | None = None, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            return await self.client.post(url, data=data or {}, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def fetch_listing(self, docs_url: str, application_reference: str) -> tuple[str, str | None]:
        """Search for an application reference and return its detail page HTML."""
        if not application_reference:
            return "", "no_application_reference"

        search_url = _normalise_search_url(docs_url)
        root = _root_url(search_url)

        try:
            landing = await self._get(search_url)
            if landing.status_code != 200:
                return "", _http_status_failure_code(landing.status_code, landing.text)
            if looks_like_idx002_block(landing.text):
                return "", "waf_idx002"

            response = await self._post(
                f"{root}/planning/index.html",
                data={
                    "application_reference_number": application_reference,
                    "fa": "search",
                    "submitted": "true",
                },
                headers={"Referer": search_url},
            )
            if response.status_code != 200:
                return "", _http_status_failure_code(response.status_code, response.text)
            if looks_like_idx002_block(response.text):
                return "", "waf_idx002"

            matches = parse_smartadmin_application_ids(response.text)
            if not matches:
                return "", "no_search_result"

            exact = next(
                (m for m in matches if m.get("reference", "").strip().lower() == application_reference.strip().lower()),
                matches[0],
            )
            detail_url = f"{root}/planning/index.html?fa=getApplication&id={exact['public_record_id']}"
            detail = await self._get(detail_url, headers={"Referer": str(response.url)})
            if detail.status_code != 200:
                return "", _http_status_failure_code(detail.status_code, detail.text)
            if looks_like_idx002_block(detail.text):
                return "", "waf_idx002"

            return detail.text, None

        except httpx.TimeoutException:
            return "", "timeout"
        except httpx.HTTPError:
            return "", "network_error"

    async def scrape_documents(
        self,
        docs_url: str,
        application_reference: str = "",
    ) -> tuple[list[dict], str | None]:
        """Scrape document metadata for one application.

        Returns (documents, failure_code). ``failure_code`` is ``None`` on
        success, including a real empty document list.
        """
        try:
            html, failure_code = await self.fetch_listing(docs_url, application_reference)
            if failure_code:
                if failure_code == "waf_idx002":
                    self.stats["waf_blocked"] += 1
                else:
                    self.stats["failed"] += 1
                return [], failure_code

            docs = parse_smartadmin_documents(html, _root_url(_normalise_search_url(docs_url)))
        except Exception as exc:
            logger.error("Unexpected SmartAdmin scrape error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "unexpected_error"

        if docs:
            self.stats["success"] += 1
        else:
            self.stats["no_docs"] += 1
        return docs, None

    async def download_document(
        self,
        doc_url: str,
        target_path: Path,
        referer: str = "",
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Download a document and fix up the extension from headers/content."""
        domain = urlparse(doc_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                headers = {"Referer": referer} if referer else {}
                resp = await self.client.get(doc_url, headers=headers)
                if resp.status_code == 200:
                    if looks_like_idx002_block(
                        resp.text if "text/html" in resp.headers.get("Content-Type", "") else ""
                    ):
                        logger.warning("WAF block page returned for %s", doc_url)
                        return 0, str(target_path)
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
                    logger.info("%s for %s; retrying", type(exc).__name__, doc_url)
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Network error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            finally:
                if acquired:
                    self._release(domain)
        return 0, str(target_path)
