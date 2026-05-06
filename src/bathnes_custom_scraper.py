"""BathNES custom planning document scraper.

Bath and North East Somerset stores historical documentation URLs in PlanIt as:

    https://www.bathnes.gov.uk/planningdocuments=<encoded_ref>

That host now returns a Drupal 404 page. The live planning details app embeds the
same path as an iframe relative to ``app.bathnes.gov.uk``:

    https://app.bathnes.gov.uk/planningdocuments=<encoded_ref>

The page is server-rendered and initialises a DataTables AJAX endpoint with a
per-session ``;jsessionid=...`` URL. The JSON rows are:

    [document_type, date, drawing_number, description, view_path, measure_path]

``view_path`` is resolved under ``/publisher`` to download the document.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import quote, unquote, urljoin, urlparse

import httpx

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

logger = logging.getLogger(__name__)

BATHNES_DOC_BASE = "https://app.bathnes.gov.uk"
BATHNES_DOC_PATH = "/planningdocuments="
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_AJAX_URL_RE = re.compile(r'"url"\s*:\s*"([^"]*/publisher/mvc/getDocumentList;jsessionid=[^"]+)"')
_REF_RE = re.compile(r"planningdocuments=([^?#]+)", re.IGNORECASE)
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def bathnes_listing_url(docs_url: str) -> str:
    """Rewrite a stored BathNES docs URL to the live app host."""
    match = _REF_RE.search(docs_url)
    if not match:
        raise ValueError(f"Cannot extract BathNES planning reference from {docs_url}")
    ref = unquote(match.group(1))
    return f"{BATHNES_DOC_BASE}{BATHNES_DOC_PATH}{quote(ref, safe='')}"


def parse_bathnes_documents(html: str, listing_url: str, ajax_json: dict) -> list[dict]:
    """Parse BathNES DataTables JSON into standard document dicts."""
    documents: list[dict] = []
    seen_urls: set[str] = set()

    rows = ajax_json.get("data") or []
    if not isinstance(rows, list):
        return []

    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            continue

        document_type = str(row[0] or "").strip()
        date_published = str(row[1] or "").strip()
        drawing_number = str(row[2] or "").strip()
        description = str(row[3] or "").strip()
        view_path = str(row[4] or "").strip()
        if not view_path:
            continue

        document_url = urljoin(f"{BATHNES_DOC_BASE}/publisher/", view_path.lstrip("/"))
        if document_url in seen_urls:
            continue
        seen_urls.add(document_url)

        filename = Path(urlparse(document_url).path).name
        documents.append(
            {
                "date_published": date_published,
                "document_type": document_type,
                "description": description,
                "drawing_number": drawing_number,
                "document_url": document_url,
                "filename": filename,
                "listing_url": listing_url,
            }
        )

    return documents


def extract_ajax_url(html: str) -> str | None:
    """Return the session-scoped document-list endpoint from the listing page."""
    match = _AJAX_URL_RE.search(html)
    if not match:
        return None
    return urljoin(BATHNES_DOC_BASE, match.group(1))


def _http_status_failure_code(status_code: int) -> str:
    if status_code == 403:
        return "http_403"
    if status_code == 404:
        return "http_404"
    if status_code == 405:
        return "http_405"
    if status_code == 429:
        return "http_429"
    if 500 <= status_code < 600:
        return "http_5xx"
    return f"http_{status_code}"


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
    "application/vnd.ms-outlook": ".msg",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/tiff": ".tif",
    "text/plain": ".txt",
}


def _detect_extension(content: bytes, content_type: str, url_path: str = "") -> str:
    suffix = Path(urlparse(url_path).path).suffix.lower()
    if suffix in {".pdf", ".doc", ".docx", ".gif", ".jpg", ".msg", ".png", ".tif", ".tiff", ".xls", ".xlsx"}:
        return ".tif" if suffix == ".tiff" else suffix

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


def _safe_filename(value: str, fallback: str = "document") -> str:
    safe = _SAFE_NAME_RE.sub("_", value).strip("._-")
    return safe[:140] or fallback


class BathnesCustomDocumentScraper:
    """Async scraper for BathNES custom Publisher document listings."""

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

    async def __aenter__(self) -> "BathnesCustomDocumentScraper":
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
            raise RuntimeError("Use 'async with BathnesCustomDocumentScraper() as scraper:'")
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
        """Scrape document listing for one BathNES application."""
        try:
            listing_url = bathnes_listing_url(docs_url)
        except ValueError as exc:
            logger.error("%s", exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        try:
            listing = await self._get(listing_url)
            if listing.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(listing.status_code)

            ajax_url = extract_ajax_url(listing.text)
            if not ajax_url:
                self.stats["failed"] += 1
                return [], "parse_error"

            docs_resp = await self._get(
                ajax_url,
                headers={
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Referer": listing_url,
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            if docs_resp.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(docs_resp.status_code)

            documents = parse_bathnes_documents(
                listing.text,
                listing_url,
                docs_resp.json(),
            )
        except httpx.TimeoutException as exc:
            logger.error("Timeout for %s: %s", listing_url, exc)
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.HTTPError as exc:
            logger.error("Network error for %s: %s", listing_url, exc)
            self.stats["failed"] += 1
            return [], "network_error"
        except (json.JSONDecodeError, ValueError) as exc:
            logger.error("Parse error for %s: %s", listing_url, exc)
            self.stats["failed"] += 1
            return [], "parse_error"
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", listing_url, exc)
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
        """Download one BathNES document, returning (bytes_written, final_path)."""
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
                logger.error("Network error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            finally:
                if acquired:
                    self._release(domain)
        return 0, str(target_path)


SAMPLES: list[tuple[str, str]] = [
    ("15/03671/LBA", "https://www.bathnes.gov.uk/planningdocuments=15%2F03671%2FLBA"),
    ("15/04599/COND", "https://www.bathnes.gov.uk/planningdocuments=15%2F04599%2FCOND"),
    ("15/04878/COND", "https://www.bathnes.gov.uk/planningdocuments=15%2F04878%2FCOND"),
    ("18/05088/LBA", "https://www.bathnes.gov.uk/planningdocuments=18%2F05088%2FLBA"),
    ("19/02102/FUL", "https://www.bathnes.gov.uk/planningdocuments=19%2F02102%2FFUL"),
]


async def dry_run(output_dir: Path = Path("_local/recon/bathnes_custom")) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with BathnesCustomDocumentScraper() as scraper:
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
                    referer=doc.get("listing_url") or bathnes_listing_url(docs_url),
                )
                if size:
                    downloads.append({"path": final_path, "bytes": size})

            manifest = {
                "planit_id": planit_id,
                "source_url": docs_url,
                "listing_url": bathnes_listing_url(docs_url),
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
    summary = asyncio.run(dry_run())
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))
