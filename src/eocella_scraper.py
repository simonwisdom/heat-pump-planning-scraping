"""eOcella/Arun planning document scraper.

Stored Arun URLs in PlanIt use the retired PlanRec route:

    /planrec/index.cfm?tpKey=eOcella&Keyscheme=Planning&user_key_1=<ref>

That route now returns 404. The same references are available through Arun's
current OcellaWeb pages:

    /aplanning/OcellaWeb/showDocuments?reference=<ref>&module=pl

The document table is server-rendered and uses the same row structure as the
existing Ocella scraper, with direct ``viewDocument?file=...&module=pl`` links.
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

ARUN_OCELLA_BASE = "https://www1.arun.gov.uk/aplanning/OcellaWeb/"
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

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
    """Extract a planning reference from legacy PlanRec or live OcellaWeb URLs."""
    parsed = urlparse(docs_url)
    query = parse_qs(parsed.query)
    for key in ("user_key_1", "reference"):
        values = query.get(key)
        if values and values[0].strip():
            return unquote(values[0]).strip()
    raise ValueError(f"Cannot extract Arun planning reference from {docs_url}")


def eocella_listing_url(docs_url: str) -> str:
    """Return the live Arun OcellaWeb document-listing URL for a stored URL."""
    reference = extract_reference(docs_url)
    return urljoin(
        ARUN_OCELLA_BASE,
        f"showDocuments?reference={quote(reference, safe='')}&module=pl",
    )


def parse_eocella_documents(html: str, listing_url: str) -> list[dict]:
    """Parse Arun OcellaWeb document metadata from the document table."""
    soup = _parse_html(html)
    documents: list[dict] = []
    seen_urls: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = link["href"].strip()
        if "viewDocument" not in href or "file=" not in href:
            continue

        row = link.find_parent("tr")
        if row is None:
            continue
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        document_url = urljoin(listing_url, href)
        if document_url in seen_urls:
            continue
        seen_urls.add(document_url)

        description = cells[4].get_text(" ", strip=True) if len(cells) > 4 else ""
        documents.append(
            {
                "date_published": cells[2].get_text(" ", strip=True),
                "document_type": link.get_text(" ", strip=True),
                "description": description,
                "drawing_number": "",
                "document_url": document_url,
                "listing_url": listing_url,
            }
        )

    return documents


class EocellaDocumentScraper:
    """Async scraper for Arun eOcella/OcellaWeb document listings."""

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

    async def __aenter__(self) -> "EocellaDocumentScraper":
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
            raise RuntimeError("Use 'async with EocellaDocumentScraper() as scraper:'")
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
        """Scrape one eOcella document listing."""
        try:
            listing_url = eocella_listing_url(docs_url)
        except ValueError as exc:
            logger.error("%s", exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        try:
            resp = await self._get(listing_url)
            if resp.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(resp.status_code)
            documents = parse_eocella_documents(resp.text, listing_url)
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
        """Download one document, returning ``(bytes_written, final_path)``."""
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
                        return 0, str(target_path)
                    ext = _detect_extension(
                        resp.content,
                        resp.headers.get("Content-Type", ""),
                        doc_url,
                    )
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


SAMPLES: list[tuple[str, str]] = [
    (
        "A/101/24/NMA",
        "http://www1.arun.gov.uk/planrec/index.cfm?tpKey=eOcella&Keyscheme=Planning&user_key_1=A%2F101%2F24%2FNMA",
    ),
    (
        "A/150/25/HH",
        "https://www1.arun.gov.uk/planrec/index.cfm?tpKey=eOcella&Keyscheme=Planning&user_key_1=A%2F150%2F25%2FHH",
    ),
    (
        "A/151/25/L",
        "https://www1.arun.gov.uk/planrec/index.cfm?tpKey=eOcella&Keyscheme=Planning&user_key_1=A%2F151%2F25%2FL",
    ),
    (
        "A/173/24/HH",
        "https://www1.arun.gov.uk/planrec/index.cfm?tpKey=eOcella&Keyscheme=Planning&user_key_1=A%2F173%2F24%2FHH",
    ),
    (
        "A/63/24/HH",
        "https://www1.arun.gov.uk/planrec/index.cfm?tpKey=eOcella&Keyscheme=Planning&user_key_1=A%2F63%2F24%2FHH",
    ),
]


async def dry_run(output_dir: Path = Path("_local/recon/eocella")) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with EocellaDocumentScraper() as scraper:
        for planit_id, docs_url in SAMPLES:
            safe_id = _safe_filename(planit_id.replace("/", "_"))
            app_dir = output_dir / safe_id
            app_dir.mkdir(parents=True, exist_ok=True)
            docs, failure_code = await scraper.scrape_documents(docs_url)

            downloads = []
            if docs:
                doc = docs[0]
                target_name = _safe_filename(
                    f"001_{doc.get('document_type')}_{doc.get('description')}",
                    "001_document",
                )
                size, final_path = await scraper.download_document(
                    doc["document_url"],
                    app_dir / target_name,
                    referer=doc.get("listing_url") or eocella_listing_url(docs_url),
                )
                if size:
                    downloads.append({"path": final_path, "bytes": size})

            manifest = {
                "planit_id": planit_id,
                "source_url": docs_url,
                "listing_url": eocella_listing_url(docs_url),
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
    print(json.dumps({k: v for k, v in run_summary.items() if k != "results"}, indent=2))
