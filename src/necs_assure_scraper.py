"""NECS LPAssure planning portal document scraper.

LPAssure (by NECS/NEC Software Solutions) is a planning portal used by some UK
councils. The documentation URL points to an overview page that loads the
documents tab via AJAX.

URL patterns observed:
  Broxbourne:  https://planning.broxbourne.gov.uk/LPAssure/ES/Presentation/...
  Charnwood:   https://planningexplorer.charnwood.gov.uk/Assure/ES/Presentation/...

The path prefix varies (/LPAssure/ or /Assure/) but the path suffix after that
is always the same:
  .../ES/Presentation/Planning/OnlinePlanning/OnlinePlanningOverview
      ?applicationNumber=<encoded_ref>

Document listing API (discovered by inspecting the JS bundle):
  POST .../ES/Presentation/Planning/OnlinePlanning/GetOnlineDocuments
       ?applicationNumber=<encoded_ref>&currentPageIndex=0
       &IsDatePublishSortedDescending=false&pageSize=100

Returns an HTML fragment containing:
- <a href=".../DisplaySearchDocument/..."> for each doc
- Hidden inputs DocumentResults[N]__* with Guid, Description, DocumentType, FileType
- Hidden input DocumentCount with total count

Download URL format (works directly, no session needed beyond cookies):
  GET .../ES/Presentation/Planning/OnlineDisplayDocument/DisplaySearchDocument/<title>
      ?applicationNumber=<encoded_ref>&FileName=<name>&fileType=<ext>&aspectGuid=<GUID>
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3

# Extract the base path prefix up to and including /ES/
_ASSURE_PREFIX_RE = re.compile(r"^(https?://[^/]+(?:/[^/]+)*?/ES)/", re.IGNORECASE)


def _assure_base(docs_url: str) -> str:
    """Return the base URL up to and including /ES from an LPAssure URL.

    e.g. 'https://planning.broxbourne.gov.uk/LPAssure/ES/Presentation/...'
         -> 'https://planning.broxbourne.gov.uk/LPAssure/ES'
    """
    m = _ASSURE_PREFIX_RE.match(docs_url)
    if m:
        return m.group(1)
    # Fallback: derive from scheme + netloc + first path segment
    parsed = urlparse(docs_url)
    parts = parsed.path.lstrip("/").split("/")
    # parts[0] = 'LPAssure' or 'Assure', parts[1] = 'ES'
    prefix = "/" + "/".join(parts[:2])
    return f"{parsed.scheme}://{parsed.netloc}{prefix}"


def _extract_app_number(docs_url: str) -> str | None:
    """Extract applicationNumber from the overview URL query string."""
    from urllib.parse import parse_qs

    qs = parse_qs(urlparse(docs_url).query)
    values = qs.get("applicationNumber") or qs.get("applicationnumber")
    return values[0] if values else None


def parse_necs_assure_documents(html: str, base_url: str) -> list[dict]:
    """Parse the GetOnlineDocuments HTML fragment.

    Each document has:
    - An <a> link to DisplaySearchDocument with the PDF/doc URL
    - A surrounding row div containing the date (col-xs-2 div)
    - Hidden inputs DocumentResults[N]__Description, DocumentType, FileType, Guid

    Returns list of dicts with: url, filename, doc_type, date_published.
    """
    try:
        from bs4 import BeautifulSoup as _BS

        soup = _BS(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    documents: list[dict] = []
    idx = 0

    while True:
        desc_field = soup.find("input", {"id": f"DocumentResults_{idx}__Description"})
        if desc_field is None:
            break

        doctype_field = soup.find("input", {"id": f"DocumentResults_{idx}__DocumentType"})
        filetype_field = soup.find("input", {"id": f"DocumentResults_{idx}__FileType"})
        guid_field = soup.find("input", {"id": f"DocumentResults_{idx}__Guid"})

        description = (desc_field.get("value") or "").strip()
        doc_type = (doctype_field.get("value") if doctype_field else "").strip()
        file_type = (filetype_field.get("value") if filetype_field else "").strip()
        guid = (guid_field.get("value") if guid_field else "").strip()

        # Find the anchor link for this doc — the href contains aspectGuid=<GUID>
        link = None
        if guid:
            link = soup.find("a", href=re.compile(re.escape(f"aspectGuid={guid}")))
        if link is None:
            # Fallback: find by DisplaySearchDocument and index proximity
            all_links = soup.find_all("a", href=re.compile("DisplaySearchDocument"))
            if idx < len(all_links):
                link = all_links[idx]

        doc_url = None
        filename = None
        if link:
            href = link.get("href", "")
            doc_url = urljoin(base_url, href) if href.startswith("/") else href
            # Extract filename from href path (slug before the '?')
            path_slug = href.split("?")[0].rstrip("/").rsplit("/", 1)[-1]
            if not path_slug:
                path_slug = description
            filename = path_slug + (file_type if file_type else "")

        # Extract date from surrounding row div (col-xs-2 contains "DD Month YYYY")
        date_published = ""
        if link:
            row_div = link.find_parent("div", class_=re.compile(r"\brow\b"))
            if row_div:
                date_cell = row_div.find("div", class_=re.compile(r"\bcol-xs-2\b"))
                if date_cell:
                    # Remove the checkbox input text, get remaining text
                    for inp in date_cell.find_all("input"):
                        inp.decompose()
                    date_published = date_cell.get_text(strip=True)

        if doc_url:
            documents.append(
                {
                    "url": doc_url,
                    "filename": filename or description,
                    "doc_type": doc_type,
                    "date_published": date_published,
                    "description": description,
                }
            )

        idx += 1

    return documents


_MAGIC_EXTS: list[tuple[bytes, str]] = [
    (b"%PDF", ".pdf"),
    (b"PK\x03\x04", ".zip"),
    (b"\xd0\xcf\x11\xe0", ".doc"),
    (b"II*\x00", ".tif"),
    (b"MM\x00*", ".tif"),
    (b"\x89PNG", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
]

_CTYPE_EXTS = {
    "application/pdf": ".pdf",
    "image/tiff": ".tif",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "text/plain": ".txt",
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


class NecsAssureDocumentScraper:
    """Async scraper for NECS LPAssure planning portal document listings.

    Usage::

        async with NecsAssureDocumentScraper() as scraper:
            docs, failure_code = await scraper.scrape_documents(docs_url)
            for doc in docs:
                size, path = await scraper.download_document(
                    doc['url'], target_path, referer=docs_url
                )
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
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self) -> "NecsAssureDocumentScraper":
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
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use 'async with NecsAssureDocumentScraper() as scraper:'")
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

    async def _post(self, url: str, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            return await self.client.post(url, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Scrape document listing for a single application.

        docs_url is the OnlinePlanningOverview URL (with or without #tabDocuments).
        Strip the fragment first.

        Returns (documents, failure_code). failure_code is None on success
        (including empty listings); otherwise a string such as http_404,
        timeout, network_error, parse_error, unexpected_error.
        """
        # Strip fragment — the overview page is server-rendered, fragment is for tab UX only
        docs_url = docs_url.split("#")[0]

        parsed = urlparse(docs_url)
        domain = parsed.netloc
        base_url = f"{parsed.scheme}://{domain}"
        assure_base = _assure_base(docs_url)

        app_number = _extract_app_number(docs_url)
        if not app_number:
            logger.error("Cannot extract applicationNumber from %s", docs_url)
            return [], "parse_error"

        try:
            # Step 1: GET the overview page to establish the ASP.NET session cookie
            resp = await self._get(docs_url)
            if resp.status_code != 200:
                code = f"http_{resp.status_code}"
                self.stats["failed"] += 1
                return [], code

            # Step 2: POST to GetOnlineDocuments to retrieve the document listing HTML
            from urllib.parse import quote

            api_url = (
                f"{assure_base}/Presentation/Planning/OnlinePlanning/GetOnlineDocuments"
                f"?applicationNumber={quote(app_number, safe='')}"
                f"&currentPageIndex=0&IsDatePublishSortedDescending=false&pageSize=200"
            )
            docs_resp = await self._post(
                api_url,
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": docs_url,
                    "Accept": "text/html, */*",
                },
            )
            if docs_resp.status_code != 200:
                code = f"http_{docs_resp.status_code}"
                logger.warning("GetOnlineDocuments HTTP %s for %s", docs_resp.status_code, api_url)
                self.stats["failed"] += 1
                return [], code

            documents = parse_necs_assure_documents(docs_resp.text, base_url)

        except httpx.TimeoutException as exc:
            logger.error("Timeout for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.HTTPError as exc:
            logger.error("Network error for %s: %s", docs_url, exc)
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
        """Download a single document. Returns (bytes_written, final_path_str).

        Fixes up the extension based on Content-Type / magic bytes.
        Returns (0, str(target_path)) on failure.
        """
        domain = urlparse(doc_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                resp = await self.client.get(
                    doc_url,
                    headers={"Referer": referer} if referer else {},
                )
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
                    backoff = 2**attempt
                    logger.info(
                        "HTTP %s for %s (attempt %d/%d), retrying in %ds",
                        resp.status_code,
                        doc_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning("HTTP %s for %s", resp.status_code, doc_url)
                return 0, str(target_path)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    backoff = 2**attempt
                    logger.info(
                        "%s for %s (attempt %d/%d), retrying in %ds",
                        type(exc).__name__,
                        doc_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
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
