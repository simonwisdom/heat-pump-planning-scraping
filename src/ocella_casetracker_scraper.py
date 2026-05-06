"""Fareham casetracker (Ocella cross-reference) document scraper.

Fareham's stored URLs use the form::

    https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp
        ?type=planning&ref=<URL-encoded planning reference>

This is a thin ASP cross-reference page that 302-redirects to the actual
casetracker view::

    https://www.fareham.gov.uk/casetracker/casetracker.asp?public=Y&caseid=<N>

The casetracker view contains relative ``casetracker_download.asp?case=<N>&
GUID=<UUID>`` links labelled with the document type. Hitting one of those
download URLs returns an HTML "your file is ready" page that embeds the
actual PDF link under ``https://www.fareham.gov.uk/downloadfiles/<name>.pdf``.

So a full document fetch is a three-step pipeline:

    1. cross-reference -> casetracker.asp (HTML, parse document table)
    2. casetracker_download.asp?case&GUID -> HTML wrapper with embedded URL
    3. /downloadfiles/<name>.pdf -> the actual PDF (or other content)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

CASETRACKER_BASE = "https://www.fareham.gov.uk/casetracker/"
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_DOWNLOAD_HREF_RE = re.compile(
    r"casetracker_download\.asp\?case=\d+&GUID=[A-F0-9-]+",
    re.IGNORECASE,
)
_DOWNLOADFILES_RE = re.compile(
    r'href="(https?://[^"]*?/downloadfiles/[^"]+)"',
    re.IGNORECASE,
)

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


def _safe_filename(value: str, fallback: str = "document") -> str:
    safe = _SAFE_NAME_RE.sub("_", value).strip("._-")
    return safe[:140] or fallback


def _detect_extension(content: bytes, content_type: str, url_path: str = "") -> str:
    suffix = Path(urlparse(url_path).path).suffix.lower()
    if suffix in {".pdf", ".doc", ".docx", ".gif", ".jpg", ".png", ".tif", ".tiff", ".xls", ".xlsx"}:
        return ".tif" if suffix == ".tiff" else suffix

    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            if ext == ".zip":
                ct = (content_type or "").split(";", 1)[0].strip().lower()
                office = _CTYPE_EXTS.get(ct)
                if office:
                    return office
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


def extract_reference(docs_url: str) -> str:
    """Extract the planning reference from a Fareham cross-reference URL."""
    parsed = urlparse(docs_url)
    query = parse_qs(parsed.query)
    for key in ("ref", "reference"):
        values = query.get(key)
        if values and values[0].strip():
            return unquote(values[0]).strip()
    raise ValueError(f"Cannot extract Fareham planning reference from {docs_url}")


def crossreference_url(reference: str) -> str:
    return urljoin(
        CASETRACKER_BASE,
        f"ocella_crossreference.asp?type=planning&ref={quote(reference, safe='')}",
    )


def parse_casetracker_documents(html: str, listing_url: str) -> list[dict]:
    """Extract document metadata from a Fareham casetracker page.

    Each document link sits inside a ``div.documentLink`` with anchor text
    describing the document; the label of the document group (e.g. "Floor
    Plans") lives in the preceding ``<td>`` cell of the row.
    """
    soup = _parse_html(html)
    documents: list[dict] = []
    seen_urls: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = link["href"].strip()
        if not _DOWNLOAD_HREF_RE.search(href):
            continue

        document_url = urljoin(listing_url, href)
        if document_url in seen_urls:
            continue
        seen_urls.add(document_url)

        # Try to find a category label in the preceding <td><b><small>... cell.
        category = ""
        row = link.find_parent("tr")
        if row is not None:
            cells = row.find_all("td")
            # Category label normally sits two cells before the documents cell.
            for cell in cells:
                txt = cell.get_text(" ", strip=True)
                if txt and ":" not in txt and "Related Documents" not in txt:
                    category = txt
                    break

        document_type = link.get_text(" ", strip=True)
        documents.append(
            {
                "date_published": "",
                "document_type": document_type,
                "description": category,
                "drawing_number": "",
                "document_url": document_url,
                "listing_url": listing_url,
            }
        )

    return documents


def extract_downloadfiles_url(html: str) -> str | None:
    """Extract the embedded /downloadfiles/<name>.pdf URL from the wrapper page."""
    match = _DOWNLOADFILES_RE.search(html)
    return match.group(1) if match else None


class OcellaCasetrackerScraper:
    """Async scraper for Fareham's casetracker (Ocella cross-reference) portal."""

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

    async def __aenter__(self) -> "OcellaCasetrackerScraper":
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
            raise RuntimeError("Use 'async with OcellaCasetrackerScraper() as scraper:'")
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
        """Resolve a Fareham cross-reference URL and parse its document table."""
        try:
            resp = await self._get(docs_url)
            if resp.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(resp.status_code)
            listing_url = str(resp.url)
            documents = parse_casetracker_documents(resp.text, listing_url)
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

    async def _resolve_download_url(self, wrapper_url: str, referer: str) -> tuple[str | None, str | None]:
        """Hit the casetracker_download.asp page and return the inner PDF URL."""
        for attempt in range(MAX_RETRIES):
            acquired = False
            try:
                domain = urlparse(wrapper_url).netloc
                await self._rate_limit(domain)
                acquired = True
                headers = {"Referer": referer} if referer else {}
                resp = await self.client.get(wrapper_url, headers=headers)
                if resp.status_code == 200:
                    inner = extract_downloadfiles_url(resp.text)
                    if inner is None:
                        return None, "no_downloadfiles_link"
                    return inner, None
                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                return None, _http_status_failure_code(resp.status_code)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Resolve network error for %s: %s", wrapper_url, exc)
                return None, "network_error"
            except Exception as exc:
                logger.error("Resolve error for %s: %s", wrapper_url, exc)
                return None, "unexpected_error"
            finally:
                if acquired:
                    self._release(domain)
        return None, "exhausted_retries"

    async def download_document(
        self,
        doc_url: str,
        target_path: Path,
        referer: str = "",
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Download one Fareham document, returning ``(bytes_written, final_path)``.

        ``doc_url`` is expected to be the wrapper ``casetracker_download.asp``
        URL; this method resolves it to the underlying ``/downloadfiles/...``
        URL and fetches the actual file.
        """
        inner_url, fail = await self._resolve_download_url(doc_url, referer)
        if not inner_url:
            logger.warning("Cannot resolve download for %s: %s", doc_url, fail)
            return 0, str(target_path)

        domain = urlparse(inner_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                headers = {"Referer": doc_url}
                resp = await self.client.get(inner_url, headers=headers)
                if resp.status_code == 200:
                    if not resp.content:
                        return 0, str(target_path)
                    ext = _detect_extension(
                        resp.content,
                        resp.headers.get("Content-Type", ""),
                        inner_url,
                    )
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(resp.content)
                    return len(resp.content), str(final_path)
                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.warning("HTTP %s for %s", resp.status_code, inner_url)
                return 0, str(target_path)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Download network error for %s: %s", inner_url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for %s: %s", inner_url, exc)
                return 0, str(target_path)
            finally:
                if acquired:
                    self._release(domain)
        return 0, str(target_path)


SAMPLES: list[tuple[str, str]] = [
    (
        "P/23/1664/FP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F1664%2FFP",
    ),
    (
        "P/23/1454/MA/A",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F1454%2FMA%2FA",
    ),
    (
        "P/19/0240/LP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F19%2F0240%2FLP",
    ),
    (
        "P/22/0712/LB",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F22%2F0712%2FLB",
    ),
    (
        "P/23/1384/DP/A",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F1384%2FDP%2FA",
    ),
    (
        "P/22/1012/DP/F",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F22%2F1012%2FDP%2FF",
    ),
    (
        "P/24/0694/FP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F24%2F0694%2FFP",
    ),
    (
        "P/23/1454/FP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F1454%2FFP",
    ),
    (
        "P/22/0788/FP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F22%2F0788%2FFP",
    ),
    (
        "P/23/1454/MA/B",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F1454%2FMA%2FB",
    ),
    (
        "P/23/1478/FP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F1478%2FFP",
    ),
    (
        "P/22/1012/DP/B",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F22%2F1012%2FDP%2FB",
    ),
    (
        "P/24/1276/FP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F24%2F1276%2FFP",
    ),
    (
        "P/23/0277/LP",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F0277%2FLP",
    ),
    (
        "P/23/1384/VC",
        "https://www.fareham.gov.uk/casetracker/ocella_crossreference.asp?type=planning&ref=P%2F23%2F1384%2FVC",
    ),
]


async def dry_run(output_dir: Path = Path("_local/recon/ocella_casetracker")) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with OcellaCasetrackerScraper() as scraper:
        for planit_id, docs_url in SAMPLES:
            safe_id = _safe_filename(planit_id.replace("/", "_"))
            app_dir = output_dir / safe_id
            app_dir.mkdir(parents=True, exist_ok=True)
            docs, failure_code = await scraper.scrape_documents(docs_url)

            downloads = []
            if docs:
                doc = docs[0]
                target_name = _safe_filename(
                    f"001_{doc.get('description')}_{doc.get('document_type')}",
                    "001_document",
                )
                size, final_path = await scraper.download_document(
                    doc["document_url"],
                    app_dir / target_name,
                    referer=doc.get("listing_url") or docs_url,
                )
                if size:
                    downloads.append({"path": final_path, "bytes": size})

            manifest = {
                "planit_id": planit_id,
                "source_url": docs_url,
                "failure_code": failure_code,
                "documents_count": len(docs),
                "downloads": downloads,
                "documents": docs,
            }
            (app_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2),
                encoding="utf-8",
            )
            results.append(manifest)

    summary = {
        "samples_total": len(SAMPLES),
        "samples_succeeded": sum(
            1 for result in results if result["failure_code"] is None and result["documents_count"] > 0
        ),
        "documents_total": sum(result["documents_count"] for result in results),
        "downloads_total": sum(len(result["downloads"]) for result in results),
        "results": results,
    }
    (output_dir / "results.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_summary = asyncio.run(dry_run())
    print(
        json.dumps(
            {k: v for k, v in run_summary.items() if k != "results"},
            indent=2,
        )
    )
