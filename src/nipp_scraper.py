"""Northern Ireland Planning Portal (NIPP) document scraper.

The stored URLs in PlanIt for NI applications point at landing pages on
``planningregister.planningsystemni.gov.uk/simple-search`` (or the legacy
``epicpublic.planningni.gov.uk/publicaccess/search.do`` Idox portal). Neither
URL embeds the application reference, so the scraper must drive the search by
``uid`` (the application reference number, e.g. ``LA06/2022/0919/F``).

The current Northern Ireland Public Register is a TerraQuest Next.js app backed
by a JSON API at ``https://api-planningregister-planningportal.pr.tqinfra.co.uk
/api/v1``. The lookup chain is:

1. ``GET /applications?SearchTerm=<ref>&SearchStatus=0&PageNumber=1&PageSize=10``
   returns a list with the numeric ``applicationId`` for the matching ref.
2. ``GET /application/<applicationId>`` returns the full application record
   including a ``supportingDocuments`` array with ``documentId`` per entry.
3. ``GET /application/<applicationId>/<documentId>`` returns a JSON object with
   a ``documentUri`` — a short-lived (~5 min) Azure SAS URL on
   ``documentstore.tqinfra.co.uk``.
4. ``GET <documentUri>`` returns the document bytes (most are .zip wrappers
   around PDFs/TIFFs in practice).

All endpoints require the ``TQ-Tenant`` header. No B2C login is needed for
public planning register reads. The same register also covers the legacy
``epicpublic.planningni.gov.uk`` references — they migrated into the unified
register — so the scraper does not need to talk to the old Idox portal.

This module mirrors the ``fetch_listing → parse → download_files → manifest``
shape of the other portal scrapers and writes a manifest per sample under
``_local/recon/nipp/<safe_uid>/``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

NIPP_API_BASE = "https://api-planningregister-planningportal.pr.tqinfra.co.uk/api/v1"
NIPP_TENANT_ID = "cfb86436-414d-4459-9545-93eec37615a2"
NIPP_LISTING_HOST = "https://planningregister.planningsystemni.gov.uk"

MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_REF_RE = re.compile(r"\bLA\d{2}/\d{4}/\d+(?:/[A-Za-z0-9]+)?\b", re.I)

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


def _detect_extension(content: bytes, content_type: str, url_path: str = "") -> str:
    suffix = Path(urlparse(url_path).path).suffix.lower()
    if suffix in {
        ".pdf",
        ".zip",
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
    }:
        return ".tif" if suffix == ".tiff" else suffix

    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            return ext

    ct = (content_type or "").split(";", 1)[0].strip().lower()
    return _CTYPE_EXTS.get(ct, ".bin")


def extract_reference(uid: str, docs_url: str | None = None) -> str:
    """Return a clean NI planning reference (LA##/YYYY/NNNN/Suffix) from a uid.

    Some PlanIt uids use prefixes like ``northernireland/LA06/2022/0919/F`` or
    omit the suffix; this normaliser tolerates both. Falls back to scanning the
    docs_url if the uid itself doesn't contain a ref pattern.
    """
    candidates = [uid]
    if docs_url:
        candidates.append(docs_url)
    for candidate in candidates:
        if not candidate:
            continue
        match = _REF_RE.search(candidate)
        if match:
            return match.group(0).upper()
    # Last resort: strip a possible source prefix and trust the rest.
    cleaned = uid.split("/", 1)[-1] if "/" in uid else uid
    return cleaned.upper()


class NippDocumentScraper:
    """Async scraper for the Northern Ireland Public Register API."""

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

    async def __aenter__(self) -> "NippDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "TQ-Tenant": NIPP_TENANT_ID,
                "Referer": NIPP_LISTING_HOST + "/",
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
            raise RuntimeError("Use 'async with NippDocumentScraper() as scraper:'")
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
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if not acquired:
                    await self._rate_limit(domain)
                    acquired = True
                resp = await self.client.request(method, url, **kwargs)
                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                    delay = 2**attempt
                    logger.warning(
                        "%s %s -> HTTP %s; retrying in %ss (attempt %s/%s)",
                        method,
                        url,
                        resp.status_code,
                        delay,
                        attempt,
                        MAX_RETRIES,
                    )
                    await asyncio.sleep(delay)
                    continue
                return resp
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("%s %s network error: %s", method, url, exc)
                raise
            finally:
                # release on the LAST attempt (or on the successful path)
                pass
        # Should be unreachable — defensive fallback
        if acquired:
            self._release(domain)
        raise RuntimeError("retry loop exited without response")

    # -- because the inner finally is tricky with retries, use a wrapper instead
    async def _get(self, url: str, params: dict | None = None) -> httpx.Response:
        domain = urlparse(url).netloc
        await self._rate_limit(domain)
        try:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    resp = await self.client.get(url, params=params)
                except (httpx.TimeoutException, httpx.NetworkError):
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(2**attempt)
                        continue
                    raise
                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                    delay = 2**attempt
                    logger.warning(
                        "GET %s -> HTTP %s; retrying in %ss",
                        url,
                        resp.status_code,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                return resp
            raise RuntimeError("unreachable")
        finally:
            self._release(domain)

    async def _resolve_application_id(self, reference: str) -> int | None:
        """Find the numeric applicationId for a planning reference."""
        params = {
            "SearchTerm": reference,
            "SearchStatus": 0,
            "PageNumber": 1,
            "PageSize": 10,
            "sortBy": "DateReceived",
            "sortByDescending": "true",
        }
        try:
            resp = await self._get(f"{NIPP_API_BASE}/applications", params=params)
        except httpx.HTTPError:
            return None
        if resp.status_code != 200:
            logger.warning("search %s -> HTTP %s", reference, resp.status_code)
            return None
        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.warning("search %s returned non-JSON", reference)
            return None

        items = (data.get("applications") or {}).get("items") or []
        if not items:
            return None

        ref_lower = reference.lower()
        # Prefer exact reference match
        for item in items:
            if (item.get("applicationReferenceNumber") or "").lower() == ref_lower:
                return item.get("applicationId")
        # Fallback to first hit
        return items[0].get("applicationId")

    async def _fetch_application_detail(self, application_id: int) -> dict[str, Any] | None:
        try:
            resp = await self._get(f"{NIPP_API_BASE}/application/{application_id}")
        except httpx.HTTPError:
            return None
        if resp.status_code != 200:
            return None
        try:
            return resp.json()
        except json.JSONDecodeError:
            return None

    async def scrape_documents(
        self,
        uid: str,
        docs_url: str | None = None,
    ) -> tuple[list[dict], str | None]:
        """Return ``(documents, failure_code)`` for an NI planning reference.

        ``docs_url`` is accepted but only used as a hint when the uid itself
        doesn't contain a recognisable reference (the stored URLs from PlanIt
        are landing pages and don't carry the reference).
        """
        try:
            reference = extract_reference(uid, docs_url)
        except Exception as exc:
            logger.error("Cannot parse reference from %s: %s", uid, exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        try:
            app_id = await self._resolve_application_id(reference)
        except httpx.ConnectError as exc:
            self.stats["failed"] += 1
            message = str(exc).lower()
            if "nodename nor servname" in message or "name or service not known" in message:
                return [], "dns_error"
            return [], "connect_error"
        except httpx.HTTPError:
            self.stats["failed"] += 1
            return [], "network_error"

        if app_id is None:
            self.stats["no_docs"] += 1
            return [], "not_found"

        detail = await self._fetch_application_detail(app_id)
        if detail is None:
            self.stats["failed"] += 1
            return [], "detail_error"

        listing_url = f"{NIPP_LISTING_HOST}/application/{app_id}"
        documents: list[dict] = []
        for entry in detail.get("supportingDocuments") or []:
            doc_id = entry.get("documentId")
            if not doc_id:
                continue
            documents.append(
                {
                    "date_published": entry.get("dateCreated") or "",
                    "document_type": entry.get("documentType") or "",
                    "description": entry.get("description") or entry.get("name") or "",
                    "drawing_number": entry.get("drawingNumber") or "",
                    "document_id": doc_id,
                    "document_name": entry.get("name") or "",
                    "document_url": (f"{NIPP_API_BASE}/application/{app_id}/{doc_id}"),
                    "listing_url": listing_url,
                    "reference": reference,
                    "application_id": app_id,
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
        """Download one document; returns ``(bytes_written, final_path)``.

        Two-step: GET the API endpoint to receive a JSON envelope with a
        short-lived ``documentUri`` SAS URL, then GET that URL for bytes.
        """
        api_url = doc.get("document_url")
        if not api_url:
            return 0, str(target_path)

        for attempt in range(max_retries):
            try:
                envelope_resp = await self._get(api_url)
                if envelope_resp.status_code != 200:
                    if envelope_resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.warning(
                        "envelope HTTP %s for %s",
                        envelope_resp.status_code,
                        api_url,
                    )
                    return 0, str(target_path)

                try:
                    envelope = envelope_resp.json()
                except json.JSONDecodeError:
                    logger.warning("envelope non-JSON for %s", api_url)
                    return 0, str(target_path)

                doc_uri = envelope.get("documentUri")
                if not doc_uri:
                    logger.warning("envelope missing documentUri: %s", envelope)
                    return 0, str(target_path)

                # Fetch the actual file from the SAS URL (no TQ-Tenant header
                # needed; pre-signed URLs are storage-account auth).
                async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as anon:
                    file_resp = await anon.get(doc_uri)

                if file_resp.status_code != 200:
                    if file_resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.warning(
                        "blob HTTP %s for %s",
                        file_resp.status_code,
                        doc_uri,
                    )
                    return 0, str(target_path)

                content = file_resp.content
                if not content:
                    return 0, str(target_path)

                ext = _detect_extension(
                    content,
                    file_resp.headers.get("Content-Type", ""),
                    doc_uri,
                )
                final_path = target_path.with_suffix(ext)
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(content)
                return len(content), str(final_path)

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Download network error for %s: %s", api_url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for %s: %s", api_url, exc)
                return 0, str(target_path)
        return 0, str(target_path)


SAMPLES: list[tuple[str, str, str]] = [
    ("LA06/2022/0919/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Ards and North Down"),
    ("LA10/2022/0886/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Fermanagh and Omagh"),
    ("LA10/2022/0885/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Fermanagh and Omagh"),
    (
        "LA08/2015/0410/F",
        "https://planningregister.planningsystemni.gov.uk/simple-search",
        "Armagh Banbridge Craigavon",
    ),
    ("LA01/2021/1493/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Causeway and Glens"),
    (
        "LA11/2015/0361/F",
        "https://epicpublic.planningni.gov.uk/publicaccess/search.do?action=advanced",
        "Derry and Strabane",
    ),
    ("LA01/2018/1248/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Causeway and Glens"),
    ("LA10/2022/0889/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Fermanagh and Omagh"),
    (
        "LA08/2024/1500/LBC",
        "https://planningregister.planningsystemni.gov.uk/simple-search",
        "Armagh Banbridge Craigavon",
    ),
    ("LA06/2022/0430/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Ards and North Down"),
    ("LA02/2023/1123/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Mid East Antrim"),
    ("LA06/2022/0917/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Ards and North Down"),
    (
        "LA02/2016/0655/F",
        "https://epicpublic.planningni.gov.uk/publicaccess/search.do?action=advanced",
        "Mid East Antrim",
    ),
    ("LA10/2022/0888/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Fermanagh and Omagh"),
    (
        "LA08/2022/0680/F",
        "https://planningregister.planningsystemni.gov.uk/simple-search",
        "Armagh Banbridge Craigavon",
    ),
    ("LA11/2017/1077/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Derry and Strabane"),
    ("LA06/2022/1032/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Ards and North Down"),
    (
        "LA08/2024/1459/F",
        "https://planningregister.planningsystemni.gov.uk/simple-search",
        "Armagh Banbridge Craigavon",
    ),
    (
        "LA08/2022/1211/F",
        "https://planningregister.planningsystemni.gov.uk/simple-search",
        "Armagh Banbridge Craigavon",
    ),
    ("LA06/2020/1180/F", "https://planningregister.planningsystemni.gov.uk/simple-search", "Ards and North Down"),
]


async def dry_run(output_dir: Path = Path("_local/recon/nipp")) -> dict:
    """Run the scraper against the recon samples; write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with NippDocumentScraper() as scraper:
        for uid, docs_url, authority in SAMPLES:
            safe_id = _safe_filename(uid.replace("/", "_"))
            app_dir = output_dir / safe_id
            app_dir.mkdir(parents=True, exist_ok=True)

            docs, failure_code = await scraper.scrape_documents(uid, docs_url)

            downloads: list[dict] = []
            if docs:
                doc = docs[0]
                target_name = _safe_filename(
                    f"001_{doc.get('document_type', '')}_{doc.get('description', '')}",
                    "001_document",
                )
                size, final_path = await scraper.download_document(
                    doc,
                    app_dir / target_name,
                )
                if size:
                    downloads.append({"path": final_path, "bytes": size})

            manifest = {
                "uid": uid,
                "authority": authority,
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
        "samples_succeeded": sum(1 for r in results if r["failure_code"] is None and r["documents_count"] > 0),
        "samples_with_downloads": sum(1 for r in results if r["downloads"]),
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
    print(
        json.dumps(
            {k: v for k, v in run_summary.items() if k != "results"},
            indent=2,
        )
    )
