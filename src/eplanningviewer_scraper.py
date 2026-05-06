"""Stratford-on-Avon eplanningviewer document scraper.

Stored URLs in PlanIt point at the retired eplanningviewer route, e.g.::

    https://apps.stratford.gov.uk/eplanningviewer/Home/index/24_00192_FUL

That route now returns HTTP 500. The same applications are served by Stratford's
current ``eplanningv2`` portal via a JSON API. The lookup chain is:

1. ``GET  /EplanningV2/API/v1/Search/ByReferenceorAddress?searchText=<ref>``
   returns a list with the application GUID (``id``) and details.
2. ``GET  /EplanningV2/API/v1/PlanningApplication/<guid>`` returns the
   ``folderId`` plus metadata such as ``documentCount``.
3. ``GET  /EplanningV2/API/v1/Categories?folderId=<folderId>`` lists the
   document category GUIDs (e.g. APPLICATION, CONSULTATIONS).
4. ``GET  /EplanningV2/API/v1/Documents/Folder/<folderId>/Category/<catId>?orderBy=date``
   lists ``{documentId, description, dateAdded}`` per document.
5. ``POST /EplanningV2/API/v1/Document/<docId>/Request`` (empty body) primes the
   download.
6. ``GET  /EplanningV2/API/v1/Document/<docId>/Download`` returns a JSON object
   with ``fileLocation`` (the public URL of the actual file).
7. ``GET <fileLocation>`` fetches the document bytes.

This scraper is recon-only: it implements the same ``fetch_listing → parse →
download_files → manifest`` shape as the other portal scrapers and writes a
manifest per sample under ``_local/recon/eplanningviewer/<safe_uid>/``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound  # noqa: F401  (kept for symmetry)

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

EPLANNINGV2_API_BASE = "https://apps.stratford.gov.uk/EplanningV2/API/v1"
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
    """Extract a planning reference from a stored eplanningviewer URL.

    Stored URLs look like
    ``https://apps.stratford.gov.uk/eplanningviewer/Home/index/24_00192_FUL``.
    The reference uses underscores in place of slashes.
    """
    parsed = urlparse(docs_url)
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        raise ValueError(f"Cannot extract reference from {docs_url}")
    raw = unquote(parts[-1])
    # Convert underscore-encoded ref back to canonical slash form
    if "_" in raw and "/" not in raw:
        return raw.replace("_", "/")
    return raw


def search_url(reference: str) -> str:
    return f"{EPLANNINGV2_API_BASE}/Search/ByReferenceorAddress?searchText={quote(reference, safe='')}"


class EplanningViewerScraper:
    """Async scraper for Stratford eplanningv2 (replacement for eplanningviewer).

    Mirrors the ``fetch_listing → parse → download_files`` shape of the other
    portal scrapers in ``src/``. ``scrape_documents`` returns the document
    metadata list and ``download_document`` writes a single document to disk.
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

    async def __aenter__(self) -> "EplanningViewerScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "application/json, */*;q=0.8",
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
            raise RuntimeError("Use 'async with EplanningViewerScraper() as scraper:'")
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

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            return await self.client.request(method, url, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def _get(self, url: str, **kwargs) -> httpx.Response:
        return await self._request("GET", url, **kwargs)

    async def _post(self, url: str, **kwargs) -> httpx.Response:
        return await self._request("POST", url, **kwargs)

    async def _resolve_application(self, reference: str) -> dict | None:
        """Find the application GUID by reference."""
        resp = await self._get(search_url(reference))
        if resp.status_code != 200:
            logger.warning("search %s -> HTTP %s", reference, resp.status_code)
            return None
        try:
            payload = resp.json()
        except json.JSONDecodeError:
            logger.warning("search %s returned non-JSON", reference)
            return None
        if not isinstance(payload, list) or not payload:
            return None
        # Prefer exact reference match (case-insensitive)
        ref_lower = reference.lower()
        for item in payload:
            if isinstance(item, dict) and (item.get("reference") or "").lower() == ref_lower:
                return item
        # Fall back to first hit
        first = payload[0]
        return first if isinstance(first, dict) else None

    async def _fetch_categories(self, folder_id: str) -> list[dict]:
        resp = await self._get(f"{EPLANNINGV2_API_BASE}/Categories?folderId={folder_id}")
        if resp.status_code != 200:
            return []
        try:
            data = resp.json()
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    async def _fetch_category_documents(self, folder_id: str, category_id: str) -> list[dict]:
        url = f"{EPLANNINGV2_API_BASE}/Documents/Folder/{folder_id}/Category/{category_id}?orderBy=date"
        resp = await self._get(url)
        if resp.status_code != 200:
            logger.warning("documents %s -> HTTP %s", category_id, resp.status_code)
            return []
        try:
            data = resp.json()
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Return ``(documents, failure_code)`` for a stored eplanningviewer URL."""
        try:
            reference = extract_reference(docs_url)
        except ValueError as exc:
            logger.error("%s", exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        try:
            app = await self._resolve_application(reference)
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
            logger.error("Unexpected error resolving %s: %s", reference, exc)
            self.stats["failed"] += 1
            return [], "unexpected_error"

        if app is None:
            self.stats["no_docs"] += 1
            return [], "not_found"

        guid = app.get("id")
        if not guid:
            self.stats["failed"] += 1
            return [], "missing_guid"

        try:
            detail = await self._get(f"{EPLANNINGV2_API_BASE}/PlanningApplication/{guid}")
            if detail.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(detail.status_code)
            detail_data = detail.json()
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            logger.error("detail fetch failed for %s: %s", guid, exc)
            self.stats["failed"] += 1
            return [], "detail_error"

        folder_id = detail_data.get("folderId")
        if not folder_id:
            self.stats["no_docs"] += 1
            return [], "no_folder"

        categories = await self._fetch_categories(folder_id)
        documents: list[dict] = []
        listing_url = f"https://apps.stratford.gov.uk/eplanningv2/AppDetail/DocumentsV2/{guid}"

        for category in categories:
            cat_id = category.get("id")
            cat_name = category.get("name") or ""
            if not cat_id:
                continue
            entries = await self._fetch_category_documents(folder_id, cat_id)
            for entry in entries:
                doc_id = entry.get("documentId")
                if not doc_id:
                    continue
                documents.append(
                    {
                        "date_published": entry.get("dateAdded") or "",
                        "document_type": cat_name,
                        "description": entry.get("description") or "",
                        "drawing_number": entry.get("reference") or "",
                        "document_id": doc_id,
                        "document_url": (f"{EPLANNINGV2_API_BASE}/Document/{doc_id}/Download"),
                        "listing_url": listing_url,
                        "reference": reference,
                        "application_guid": guid,
                        "folder_id": folder_id,
                        "category_id": cat_id,
                    }
                )

        if documents:
            self.stats["success"] += 1
        else:
            self.stats["no_docs"] += 1
        return documents, None

    async def download_document(
        self,
        doc: dict,
        target_path: Path,
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Download one document, returning ``(bytes_written, final_path)``.

        Performs the two-step ``Request`` + ``Download`` API dance, then fetches
        the file from the returned ``fileLocation`` URL.
        """
        doc_id = doc.get("document_id")
        if not doc_id:
            return 0, str(target_path)

        request_url = f"{EPLANNINGV2_API_BASE}/Document/{doc_id}/Request"
        download_url = f"{EPLANNINGV2_API_BASE}/Document/{doc_id}/Download"

        for attempt in range(max_retries):
            try:
                # Step 1: prime the download
                request_resp = await self._post(request_url)
                if request_resp.status_code not in (200, 204):
                    if request_resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.warning(
                        "Request POST HTTP %s for doc %s",
                        request_resp.status_code,
                        doc_id,
                    )
                    return 0, str(target_path)

                # Step 2: get the redirect/file metadata
                meta_resp = await self._get(download_url)
                if meta_resp.status_code != 200:
                    if meta_resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.warning("Download GET HTTP %s for doc %s", meta_resp.status_code, doc_id)
                    return 0, str(target_path)

                try:
                    meta = meta_resp.json()
                except json.JSONDecodeError:
                    logger.warning("Download GET returned non-JSON for doc %s", doc_id)
                    return 0, str(target_path)

                file_location = meta.get("fileLocation")
                status = (meta.get("status") or "").lower()
                if not file_location or status and status != "available":
                    logger.warning("Doc %s not available (status=%s)", doc_id, meta.get("status"))
                    return 0, str(target_path)

                # Step 3: download the actual bytes
                file_resp = await self._get(file_location)
                if file_resp.status_code != 200:
                    if file_resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.warning(
                        "fileLocation HTTP %s for doc %s",
                        file_resp.status_code,
                        doc_id,
                    )
                    return 0, str(target_path)

                content = file_resp.content
                if not content:
                    return 0, str(target_path)

                ext = _detect_extension(
                    content,
                    file_resp.headers.get("Content-Type", ""),
                    file_location,
                )
                final_path = target_path.with_suffix(ext)
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(content)
                return len(content), str(final_path)

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Download network error for doc %s: %s", doc_id, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for doc %s: %s", doc_id, exc)
                return 0, str(target_path)
        return 0, str(target_path)


SAMPLES: list[tuple[str, str]] = [
    ("24/00192/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/24_00192_FUL"),
    ("20/02216/LDE", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/20_02216_LDE"),
    ("20/00939/LBC", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/20_00939_LBC"),
    ("21/01763/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_01763_FUL"),
    ("24/02218/AMD", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/24_02218_AMD"),
    ("21/01527/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_01527_FUL"),
    ("22/02364/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/22_02364_FUL"),
    ("21/03209/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_03209_FUL"),
    ("20/01181/LDE", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/20_01181_LDE"),
    ("22/02692/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/22_02692_FUL"),
    ("21/01955/VARY", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_01955_VARY"),
    ("21/02596/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_02596_FUL"),
    ("23/02832/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/23_02832_FUL"),
    ("21/03644/AMD", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_03644_AMD"),
    ("20/03409/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/20_03409_FUL"),
    ("21/02436/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_02436_FUL"),
    ("16/00805/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/16_00805_FUL"),
    ("21/02245/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_02245_FUL"),
    ("23/01215/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/23_01215_FUL"),
    ("21/00925/FUL", "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_00925_FUL"),
]


async def dry_run(output_dir: Path = Path("_local/recon/eplanningviewer")) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads.

    Per spec: writes one PDF per sample to verify the download path; full
    manifest of metadata is captured per app.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with EplanningViewerScraper() as scraper:
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
                    doc,
                    app_dir / target_name,
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
