"""Rotherham Plan Portal document scraper.

Rotherham (and possibly other authorities) use a third-party SaaS frontend
served from ``<authority>.planportal.co.uk``. The HTML is a near-empty Ext.JS
SPA shell; all data flows through an Ext.Direct RPC endpoint at
``services/DirectService.ashx`` and a separate ``download.ashx`` for file
bytes.

Stored URLs in PlanIt look like::

    https://rotherham.planportal.co.uk/?id=<percent-encoded ref, e.g. RB2024%2F0729>

The flow this scraper follows:

1. ``POST services/DirectService.ashx`` with method ``GetRelatedDocuments``
   and ``data=[<AppRef>]`` returns ``result.NewDataSet.data`` — a list of
   document records keyed on ``ID_PhysicalDoc``, ``FileName``, ``DocumentType``,
   ``Description``, ``DateCreated``, ``ID_AppRef``.
2. ``POST services/DirectService.ashx`` with method ``DownloadFile`` and
   ``data=[ID_PhysicalDoc, AppRef, FileName]`` returns
   ``{success: 'true', filename: <download token>}``. The token is usually
   the same as ``FileName`` but with a normalised extension (e.g. ``.zip``
   if the server is bundling).
3. ``GET download.ashx?File=<token>`` returns the raw file bytes (PDF, ZIP,
   image, …). ``Content-Disposition`` carries the suggested filename.

The Ext.Direct ``GetApplicationDetailsV2`` and ``GetApplicationDetails``
methods both return ``null`` for valid refs in initial probing, so this
scraper only relies on the document list.

Recon-only: writes per-app manifests under ``_local/recon/planportal/`` and
does not touch the database.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import re
import time
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import httpx

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

DEFAULT_PORTAL_HOST = "rotherham.planportal.co.uk"
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
    "application/zip": ".zip",
    "application/x-zip-compressed": ".zip",
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


def _detect_extension(content: bytes, content_type: str, hint_name: str = "") -> str:
    suffix = Path(hint_name).suffix.lower()
    if suffix in {
        ".pdf",
        ".doc",
        ".docx",
        ".gif",
        ".jpg",
        ".jpeg",
        ".png",
        ".tif",
        ".tiff",
        ".xls",
        ".xlsx",
        ".zip",
        ".txt",
    }:
        if suffix == ".tiff":
            return ".tif"
        if suffix == ".jpeg":
            return ".jpg"
        return suffix

    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            return ext

    ct = (content_type or "").split(";", 1)[0].strip().lower()
    return _CTYPE_EXTS.get(ct, ".bin")


def _unwrap_single_file_zip(content: bytes, hint_name: str = "") -> tuple[bytes, str] | None:
    """If ``content`` is a ZIP wrapping exactly one file, return ``(inner_bytes, ext)``.

    Rotherham planportal serves files inside ZIP wrappers (one PDF per zip in
    every observed sample). Unwrap so the manifest records the inner file's
    bytes and extension, not the zip. Returns ``None`` if the bytes are not a
    ZIP, contain more than one file, are encrypted, or don't decompress
    cleanly — in those cases the caller keeps the original zip.
    """
    if not content[:4] == b"PK\x03\x04":
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            entries = [info for info in zf.infolist() if not info.is_dir()]
            if len(entries) != 1:
                return None
            entry = entries[0]
            if entry.flag_bits & 0x1:  # encrypted
                return None
            inner = zf.read(entry)
    except (zipfile.BadZipFile, RuntimeError, OSError) as exc:
        logger.warning("zip unwrap failed for %s: %s", hint_name or "<unknown>", exc)
        return None
    inner_ext = _detect_extension(inner, "", entry.filename)
    return inner, inner_ext


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
    """Extract the planning reference from a planportal ``?id=`` URL."""
    parsed = urlparse(docs_url)
    query = parse_qs(parsed.query)
    values = query.get("id")
    if values and values[0].strip():
        return unquote(values[0]).strip()
    raise ValueError(f"Cannot extract planportal reference from {docs_url}")


def portal_base(docs_url: str) -> str:
    """Return the ``https://<host>/`` base for a planportal URL."""
    parsed = urlparse(docs_url)
    if not parsed.netloc:
        return f"https://{DEFAULT_PORTAL_HOST}/"
    return f"{parsed.scheme or 'https'}://{parsed.netloc}/"


class PlanPortalDocumentScraper:
    """Async scraper for ``*.planportal.co.uk`` document listings."""

    def __init__(
        self,
        per_domain_delay: float = IDOX_RATE_LIMIT_PER_DOMAIN,
        max_concurrent: int = IDOX_MAX_CONCURRENT_DOMAINS,
    ):
        self.per_domain_delay = per_domain_delay
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request: dict[str, float] = {}
        self._tid = 0
        self._client: httpx.AsyncClient | None = None
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self) -> "PlanPortalDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "*/*",
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
            raise RuntimeError("Use 'async with PlanPortalDocumentScraper() as s:'")
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

    def _next_tid(self) -> int:
        self._tid += 1
        return self._tid

    async def _direct_call(
        self,
        base: str,
        method: str,
        data: list,
        referer: str,
    ) -> dict:
        """Invoke an Ext.Direct RPC method on ``services/DirectService.ashx``."""
        url = urljoin(base, "services/DirectService.ashx")
        payload = {
            "action": "DirectService",
            "method": method,
            "data": data,
            "type": "rpc",
            "tid": self._next_tid(),
        }
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            resp = await self.client.post(
                url,
                content=json.dumps(payload),
                headers={
                    "Content-Type": "application/json",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": referer,
                },
            )
            resp.raise_for_status()
            return resp.json()
        finally:
            if acquired:
                self._release(domain)

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Fetch the document list for one application reference."""
        try:
            reference = extract_reference(docs_url)
        except ValueError as exc:
            logger.error("%s", exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        base = portal_base(docs_url)
        listing_url = f"{base}?id={quote(reference, safe='')}"

        try:
            envelope = await self._direct_call(
                base,
                "GetRelatedDocuments",
                [reference],
                referer=listing_url,
            )
        except httpx.TimeoutException:
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.ConnectError as exc:
            self.stats["failed"] += 1
            message = str(exc).lower()
            if "nodename nor servname" in message or "name or service not known" in message:
                return [], "dns_error"
            return [], "connect_error"
        except httpx.HTTPStatusError as exc:
            self.stats["failed"] += 1
            return [], _http_status_failure_code(exc.response.status_code)
        except httpx.HTTPError:
            self.stats["failed"] += 1
            return [], "network_error"
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "unexpected_error"

        if envelope.get("type") == "exception":
            self.stats["failed"] += 1
            return [], "rpc_exception"

        result = envelope.get("result")
        if not result:
            self.stats["no_docs"] += 1
            return [], None

        rows = (result.get("NewDataSet") or {}).get("data") or []
        documents: list[dict] = []
        for row in rows:
            documents.append(
                {
                    "id_physical_doc": str(row.get("ID_PhysicalDoc", "")),
                    "request_id": str(row.get("RequestID", "")),
                    "file_name": row.get("FileName", "") or "",
                    "doc_no": row.get("DocNo", "") or "",
                    "sheet_no": row.get("SheetNo", "") or "",
                    "rev_no": row.get("RevNo", "") or "",
                    "document_type": row.get("DocumentType", "") or "",
                    "description": row.get("Description", "") or "",
                    "date_created": row.get("DateCreated", "") or "",
                    "publish_date": row.get("PublishDate", "") or "",
                    "file_size_kb": row.get("FileSizeKb", "") or "",
                    "id_app_ref": row.get("ID_AppRef", reference) or reference,
                    "listing_url": listing_url,
                    "_base": base,
                }
            )

        if documents:
            self.stats["success"] += 1
        else:
            self.stats["no_docs"] += 1
        return documents, None

    async def _request_download_token(
        self,
        base: str,
        id_physical_doc: str,
        app_ref: str,
        file_name: str,
        referer: str,
    ) -> str | None:
        """Call ``DirectService.DownloadFile`` and return the download token."""
        envelope = await self._direct_call(
            base,
            "DownloadFile",
            [id_physical_doc, app_ref, file_name],
            referer=referer,
        )
        if envelope.get("type") == "exception":
            return None
        result = envelope.get("result") or {}
        # Server returns "true"/"false" as strings.
        if str(result.get("success", "")).lower() != "true":
            return None
        token = result.get("filename") or result.get("fileurl") or ""
        return token or None

    async def download_document(
        self,
        document: dict,
        target_path: Path,
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Run the two-step download for one Plan Portal document."""
        base = document["_base"]
        listing_url = document.get("listing_url", base)
        try:
            token = await self._request_download_token(
                base,
                document["id_physical_doc"],
                document["id_app_ref"],
                document["file_name"],
                referer=listing_url,
            )
        except httpx.HTTPError as exc:
            logger.warning("DownloadFile RPC failed: %s", exc)
            return 0, str(target_path)

        if not token:
            return 0, str(target_path)

        download_url = urljoin(base, "download.ashx") + "?File=" + quote(token, safe="")
        domain = urlparse(download_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                resp = await self.client.get(
                    download_url,
                    headers={"Referer": listing_url},
                )
                if resp.status_code == 200:
                    if not resp.content:
                        return 0, str(target_path)
                    payload = resp.content
                    ext = _detect_extension(
                        payload,
                        resp.headers.get("Content-Type", ""),
                        token,
                    )
                    if ext == ".zip":
                        unwrapped = _unwrap_single_file_zip(payload, token)
                        if unwrapped is not None:
                            payload, ext = unwrapped
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(payload)
                    return len(payload), str(final_path)
                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.warning("HTTP %s for %s", resp.status_code, download_url)
                return 0, str(target_path)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Download network error for %s: %s", download_url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for %s: %s", download_url, exc)
                return 0, str(target_path)
            finally:
                if acquired:
                    self._release(domain)
        return 0, str(target_path)


# ---------------------------------------------------------------------------
# Recon harness
# ---------------------------------------------------------------------------

SAMPLES: list[tuple[str, str]] = [
    ("RB2024/0729", "https://rotherham.planportal.co.uk/?id=RB2024%2F0729"),
    ("RB2017/0419", "https://rotherham.planportal.co.uk/?id=RB2017%2F0419"),
    ("RB2024/1613", "https://rotherham.planportal.co.uk/?id=RB2024%2F1613"),
    ("RB2025/1461", "https://rotherham.planportal.co.uk/?id=RB2025%2F1461"),
    ("RB2022/0354", "https://rotherham.planportal.co.uk/?id=RB2022%2F0354"),
    ("RB2019/0098", "https://rotherham.planportal.co.uk/?id=RB2019%2F0098"),
    ("RB2024/1465", "https://rotherham.planportal.co.uk/?id=RB2024%2F1465"),
    ("RB2024/1646", "https://rotherham.planportal.co.uk/?id=RB2024%2F1646"),
    ("RB2025/0706", "https://rotherham.planportal.co.uk/?id=RB2025%2F0706"),
    ("RB2024/0160", "https://rotherham.planportal.co.uk/?id=RB2024%2F0160"),
    ("RB2024/1401", "https://rotherham.planportal.co.uk/?id=RB2024%2F1401"),
    ("RB2023/0347", "https://rotherham.planportal.co.uk/?id=RB2023%2F0347"),
    ("RB2024/1599", "https://rotherham.planportal.co.uk/?id=RB2024%2F1599"),
    ("RB2024/1790", "https://rotherham.planportal.co.uk/?id=RB2024%2F1790"),
    ("RB2024/1822", "https://rotherham.planportal.co.uk/?id=RB2024%2F1822"),
]


async def dry_run(
    output_dir: Path = Path("_local/recon/planportal"),
    max_files_per_app: int = 1,
) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with PlanPortalDocumentScraper() as scraper:
        for planit_id, docs_url in SAMPLES:
            safe_id = _safe_filename(planit_id.replace("/", "_"))
            app_dir = output_dir / safe_id
            app_dir.mkdir(parents=True, exist_ok=True)
            docs, failure_code = await scraper.scrape_documents(docs_url)

            downloads: list[dict] = []
            for index, doc in enumerate(docs[:max_files_per_app]):
                target_name = _safe_filename(
                    f"{index + 1:03d}_{doc.get('document_type', '')}_{doc.get('description', '')}",
                    f"{index + 1:03d}_document",
                )
                size, final_path = await scraper.download_document(
                    doc,
                    app_dir / target_name,
                )
                if size:
                    downloads.append(
                        {
                            "path": final_path,
                            "bytes": size,
                            "id_physical_doc": doc.get("id_physical_doc"),
                            "file_name": doc.get("file_name"),
                        }
                    )

            manifest_docs = [{k: v for k, v in d.items() if not k.startswith("_")} for d in docs]
            manifest = {
                "planit_id": planit_id,
                "source_url": docs_url,
                "failure_code": failure_code,
                "documents_count": len(docs),
                "downloads": downloads,
                "documents": manifest_docs,
            }
            (app_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2),
                encoding="utf-8",
            )
            results.append(manifest)

    summary = {
        "samples_total": len(SAMPLES),
        "samples_with_documents": sum(1 for r in results if r["failure_code"] is None and r["documents_count"] > 0),
        "samples_with_downloads": sum(1 for r in results if r["downloads"]),
        "samples_failed": sum(1 for r in results if r["failure_code"] is not None),
        "samples_no_docs": sum(1 for r in results if r["failure_code"] is None and r["documents_count"] == 0),
        "documents_total": sum(r["documents_count"] for r in results),
        "downloads_total": sum(len(r["downloads"]) for r in results),
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
