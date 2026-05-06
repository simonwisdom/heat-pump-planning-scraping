"""Apache Shale dialog (Civica W2 ``Handler.ashx``) document scraper.

Several UK councils still expose planning records via legacy URLs of the form::

    https://<host>/<mount>/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch
        &Param=lg.Planning&viewdocs=true&SDescription=<ref>

Behind those URLs sits a Civica W2 portal. The Apache Shale ``dialog.page``
endpoint is now a thin redirect to the portal's planning landing page; the page
itself is JS-rendered. The same data is exposed through a documented JSON API
under ``/w2webparts/Resource/Civica/Handler.ashx/`` which we hit directly.

Pipeline (per app)::

    1. POST {base}/w2webparts/Resource/Civica/Handler.ashx/keyobject/pagedsearch
       body: {"refType":"GFPlanning","searchFields":{"<field>":"<ref>"}}
       -> {"KeyObjects":[{"KeyNumber":"<key>","Items":[...]}]}
    2. POST {base}/w2webparts/Resource/Civica/Handler.ashx/doc/list
       body: {"KeyNumb":"<key>","KeyText":"Subject","RefType":"GFPlanning"}
       -> {"CompleteDocument":[{"DocNo":..,"Title":..,"FileName":..}, ...]}
    3. GET  {base}/w2webparts/Resource/Civica/Handler.ashx/Doc/pagestream
            ?cd=inline&pdf=true&docno=<DocNo>
       -> bytes (typically ``application/pdf``).

Quirks handled here:

* The lookup field varies. Most councils use ``SDescription`` but Conwy uses
  ``ref_no``. We honour whichever query parameter is in the stored URL and fall
  back to ``SDescription``.
* The original ``dialog.page`` host is sometimes a thin redirect (e.g.
  ``www.torbay.gov.uk/W2Planning/`` -> ``planningdocuments.torbay.gov.uk``).
  We resolve the redirect once and then pin the API host.
* HTTP-only stored URLs (Southend, Warrington) often have an HTTPS sibling that
  is reachable. We retry on HTTPS if the initial scheme fails.

This scraper is recon-only; it implements ``scrape_documents → download_document``
to mirror the other portal scrapers in ``src/`` and writes a manifest per sample
under ``_local/recon/shale_dialog/<safe_uid>/``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse, urlunparse

import httpx

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
HANDLER_PATH = "/w2webparts/Resource/Civica/Handler.ashx"
PAGED_SEARCH_PATH = f"{HANDLER_PATH}/keyobject/pagedsearch"
DOC_LIST_PATH = f"{HANDLER_PATH}/doc/list"
DOC_DOWNLOAD_PATH = f"{HANDLER_PATH}/Doc/pagestream"

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

_REF_FIELDS = ("SDescription", "ref_no", "PlanningApplicationId")

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


def _detect_extension(content: bytes, content_type: str, file_name: str = "") -> str:
    suffix = Path(file_name).suffix.lower()
    if suffix in {".pdf", ".doc", ".docx", ".gif", ".jpg", ".png", ".tif", ".tiff", ".xls", ".xlsx"}:
        return ".tif" if suffix == ".tiff" else suffix

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


def parse_dialog_url(url: str) -> tuple[str, str, str]:
    """Extract ``(api_base, ref_field, reference)`` from a stored dialog URL.

    The stored URL looks like::

        https://documents.norwich.gov.uk/Planning/dialog.page
            ?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch
            &Param=lg.Planning&viewdocs=true&SDescription=24%2F00282%2FF

    The Civica W2 ``Handler.ashx`` API lives at the host root, so ``api_base``
    is the URL scheme + netloc only.
    """
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Cannot parse dialog URL {url!r}")
    api_base = f"{parsed.scheme}://{parsed.netloc}"

    qs = parse_qs(parsed.query, keep_blank_values=True)
    ref_field = "SDescription"
    reference = ""
    for field in _REF_FIELDS:
        values = qs.get(field) or qs.get(field.lower())
        if values and values[0]:
            ref_field = field
            reference = unquote(values[0])
            break
    if not reference:
        raise ValueError(f"No reference found in URL {url!r}")
    return api_base, ref_field, reference


def _alternative_scheme_base(api_base: str) -> str | None:
    """Return an HTTPS sibling for ``http://...`` bases (or vice versa)."""
    parsed = urlparse(api_base)
    if parsed.scheme == "http":
        return urlunparse(("https", parsed.netloc, "", "", "", ""))
    if parsed.scheme == "https":
        return urlunparse(("http", parsed.netloc, "", "", "", ""))
    return None


def _key_number_from_keyobject(keyobject: dict) -> str | None:
    """Civica returns ``KeyNumber`` either as a top-level field or inside ``Items``."""
    if not isinstance(keyobject, dict):
        return None
    if keyobject.get("KeyNumber"):
        return str(keyobject["KeyNumber"])
    for item in keyobject.get("Items") or []:
        if isinstance(item, dict) and item.get("FieldName") == "KeyNumber":
            value = item.get("Value")
            if value:
                return str(value)
    return None


class ShaleDialogScraper:
    """Async scraper for Apache Shale dialog / Civica W2 planning portals.

    Mirrors the ``scrape_documents → download_document`` shape of the other
    portal scrapers in ``src/``.
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

    async def __aenter__(self) -> "ShaleDialogScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "application/json, */*;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
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
            raise RuntimeError("Use 'async with ShaleDialogScraper() as scraper:'")
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

    async def _post_json(self, url: str, payload: dict) -> httpx.Response:
        return await self._request(
            "POST",
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    async def _get(self, url: str, **kwargs) -> httpx.Response:
        return await self._request("GET", url, **kwargs)

    async def _resolve_key_object(
        self, api_base: str, ref_field: str, reference: str
    ) -> tuple[str, dict] | tuple[None, None]:
        """Run pagedsearch and return ``(api_base, keyobject)`` for the matching app.

        Falls back to the alternative scheme (http <-> https) once if the first
        attempt yields a transport-level failure or a 404 — several councils
        ship HTTP URLs that only respond on HTTPS.
        """
        candidate_bases = [api_base]
        alt = _alternative_scheme_base(api_base)
        if alt:
            candidate_bases.append(alt)

        last_failure: str | None = None
        for base in candidate_bases:
            url = base + PAGED_SEARCH_PATH
            payload = {"refType": "GFPlanning", "searchFields": {ref_field: reference}}
            try:
                resp = await self._post_json(url, payload)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_failure = f"network:{type(exc).__name__}"
                continue
            except httpx.HTTPError as exc:
                last_failure = f"http:{type(exc).__name__}"
                continue

            if resp.status_code == 404 and base == api_base and alt is not None:
                last_failure = "http_404"
                continue
            if resp.status_code != 200:
                logger.warning("pagedsearch %s -> HTTP %s", url, resp.status_code)
                return None, None
            try:
                data = resp.json()
            except json.JSONDecodeError:
                logger.warning("pagedsearch %s returned non-JSON", url)
                return None, None
            kos = data.get("KeyObjects") if isinstance(data, dict) else None
            if not kos:
                # Found the API, but no record matches — return base for caller's failure code.
                return base, {}
            return base, kos[0]

        logger.warning(
            "pagedsearch failed for %s (%s): %s",
            reference,
            api_base,
            last_failure,
        )
        return None, None

    async def _fetch_documents(self, api_base: str, key_number: str) -> list[dict]:
        url = api_base + DOC_LIST_PATH
        payload = {
            "KeyNumb": str(key_number),
            "KeyText": "Subject",
            "RefType": "GFPlanning",
        }
        resp = await self._post_json(url, payload)
        if resp.status_code != 200:
            logger.warning("doc/list -> HTTP %s for key %s", resp.status_code, key_number)
            return []
        try:
            data = resp.json()
        except json.JSONDecodeError:
            return []
        docs = data.get("CompleteDocument") if isinstance(data, dict) else None
        return docs or []

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Return ``(documents, failure_code)`` for a stored Shale dialog URL."""
        try:
            api_base, ref_field, reference = parse_dialog_url(docs_url)
        except ValueError as exc:
            logger.error("%s", exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        try:
            resolved_base, keyobject = await self._resolve_key_object(api_base, ref_field, reference)
        except httpx.ConnectError as exc:
            self.stats["failed"] += 1
            message = str(exc).lower()
            if "nodename" in message or "name or service not known" in message:
                return [], "dns_error"
            return [], "connect_error"
        except Exception as exc:
            logger.error("Unexpected pagedsearch error for %s: %s", reference, exc)
            self.stats["failed"] += 1
            return [], "unexpected_error"

        if resolved_base is None:
            self.stats["failed"] += 1
            return [], "network_error"
        if not keyobject:
            self.stats["no_docs"] += 1
            return [], "not_found"

        key_number = _key_number_from_keyobject(keyobject)
        if not key_number:
            self.stats["failed"] += 1
            return [], "missing_key"

        try:
            doc_entries = await self._fetch_documents(resolved_base, key_number)
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            logger.error("doc/list failed for %s: %s", reference, exc)
            self.stats["failed"] += 1
            return [], "doc_list_error"

        documents: list[dict] = []
        listing_url = (
            f"{resolved_base}/w2webparts/Resource/Civica/Handler.ashx//doc/list?KeyNumb={key_number}&RefType=GFPlanning"
        )
        for entry in doc_entries:
            doc_no = entry.get("DocNo")
            if doc_no is None:
                continue
            file_name = entry.get("FileName") or ""
            documents.append(
                {
                    "date_published": (entry.get("DocDate") or "")[:10],
                    "document_type": entry.get("DocDesc") or entry.get("TypeCode") or "",
                    "description": entry.get("Title") or entry.get("DocDesc") or "",
                    "drawing_number": entry.get("TypeCode") or "",
                    "document_id": str(doc_no),
                    "document_url": (f"{resolved_base}{DOC_DOWNLOAD_PATH}?cd=inline&pdf=true&docno={doc_no}"),
                    "viewer_url": f"{resolved_base}/my-requests/document-viewer?DocNo={doc_no}",
                    "listing_url": listing_url,
                    "reference": reference,
                    "ref_field": ref_field,
                    "api_base": resolved_base,
                    "key_number": key_number,
                    "file_name": file_name,
                    "file_extension": (entry.get("FileExtension") or "").lower(),
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
        """Download one document, returning ``(bytes_written, final_path)``."""
        url = doc.get("document_url")
        if not url:
            return 0, str(target_path)

        for attempt in range(max_retries):
            try:
                resp = await self._get(url)
                if resp.status_code != 200:
                    if resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.warning(
                        "download HTTP %s for doc %s",
                        resp.status_code,
                        doc.get("document_id"),
                    )
                    return 0, str(target_path)

                content = resp.content
                if not content:
                    return 0, str(target_path)

                ext = _detect_extension(
                    content,
                    resp.headers.get("Content-Type", ""),
                    doc.get("file_name", ""),
                )
                final_path = target_path.with_suffix(ext)
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(content)
                return len(content), str(final_path)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error(
                    "Network error downloading doc %s: %s",
                    doc.get("document_id"),
                    exc,
                )
                return 0, str(target_path)
            except Exception as exc:
                logger.error(
                    "Unexpected error downloading doc %s: %s",
                    doc.get("document_id"),
                    exc,
                )
                return 0, str(target_path)
        return 0, str(target_path)


SAMPLES: list[tuple[str, str, str]] = [
    (
        "24/00282/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=24%2F00282%2FF",
        "Norwich",
    ),
    (
        "24/00923/FULH",
        "http://publicedrms.southend.gov.uk/Planning/lg/dialog.page?Param=lg.Planning&org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&viewdocs=true&SDescription=24%2F00923%2FFULH",
        "Southend",
    ),
    (
        "21/00704/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=21%2F00704%2FF",
        "Norwich",
    ),
    (
        "22/00857/FULH",
        "http://publicedrms.southend.gov.uk/Planning/lg/dialog.page?Param=lg.Planning&org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&viewdocs=true&SDescription=22%2F00857%2FFULH",
        "Southend",
    ),
    (
        "24/01369/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=24%2F01369%2FF",
        "Norwich",
    ),
    (
        "0/43152",
        "https://edm.conwy.gov.uk/Planning/lg/dialog.page?lang=en&org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&ref_no=0%2F43152",
        "Conwy",
    ),
    (
        "25/00328/D",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=25%2F00328%2FD",
        "Norwich",
    ),
    (
        "P/2025/0568",
        "https://www.torbay.gov.uk/W2Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=P%2F2025%2F0568",
        "Torbay",
    ),
    (
        "24/01285/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=24%2F01285%2FF",
        "Norwich",
    ),
    (
        "2015/25096",
        "http://myplanning.warrington.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&SDescription=2015/25096&viewdocs=true",
        "Warrington",
    ),
    (
        "25/00693/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=25%2F00693%2FF",
        "Norwich",
    ),
    (
        "25/00991/CLP",
        "http://publicedrms.southend.gov.uk/Planning/lg/dialog.page?Param=lg.Planning&org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&viewdocs=true&SDescription=25%2F00991%2FCLP",
        "Southend",
    ),
    (
        "24/00434/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=24%2F00434%2FF",
        "Norwich",
    ),
    (
        "P/2019/0713",
        "https://www.torbay.gov.uk/W2Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=P%2F2019%2F0713",
        "Torbay",
    ),
    (
        "P/2019/0714",
        "https://www.torbay.gov.uk/W2Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=P%2F2019%2F0714",
        "Torbay",
    ),
    (
        "19/01039/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=19%2F01039%2FF",
        "Norwich",
    ),
    (
        "25/00915/FULH",
        "http://publicedrms.southend.gov.uk/Planning/lg/dialog.page?Param=lg.Planning&org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&viewdocs=true&SDescription=25%2F00915%2FFULH",
        "Southend",
    ),
    (
        "P/2024/0471",
        "https://www.torbay.gov.uk/W2Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=P%2F2024%2F0471",
        "Torbay",
    ),
    (
        "2020/38385",
        "http://myplanning.warrington.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&SDescription=2020/38385&viewdocs=true",
        "Warrington",
    ),
    (
        "21/00811/F",
        "https://documents.norwich.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch&Param=lg.Planning&viewdocs=true&SDescription=21%2F00811%2FF",
        "Norwich",
    ),
]


async def dry_run(output_dir: Path = Path("_local/recon/shale_dialog")) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with ShaleDialogScraper() as scraper:
        for planit_id, docs_url, authority in SAMPLES:
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
        "samples_no_docs": sum(1 for r in results if r["documents_count"] == 0 and r["failure_code"] is None),
        "samples_failed": sum(1 for r in results if r["failure_code"] is not None),
        "documents_total": sum(r["documents_count"] for r in results),
        "downloads_total": sum(len(r["downloads"]) for r in results),
        "by_authority": {
            authority: {
                "total": sum(1 for r in results if r["authority"] == authority),
                "ok": sum(
                    1
                    for r in results
                    if r["authority"] == authority and r["failure_code"] is None and r["documents_count"] > 0
                ),
                "failed": sum(1 for r in results if r["authority"] == authority and r["failure_code"] is not None),
            }
            for authority in {row["authority"] for row in results}
        },
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
