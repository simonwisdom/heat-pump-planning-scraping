"""Scraper for Civica-style /planning/planning-documents backends."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urljoin, urlparse

import httpx

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
_SEARCH_QUERY_FIELDS = ("SDescription", "PlanningApplicationId", "ref_no")

_MAGIC_EXTS: list[tuple[bytes, str]] = [
    (b"%PDF", ".pdf"),
    (b"{\\rtf", ".rtf"),
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
    "application/rtf": ".rtf",
    "text/rtf": ".rtf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.ms-powerpoint": ".ppt",
    "image/tiff": ".tif",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
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


def _client_headers() -> dict[str, str]:
    return {
        "User-Agent": IDOX_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _json_headers() -> dict[str, str]:
    return {
        "User-Agent": IDOX_USER_AGENT,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
    }


def _is_tls_error(exc: Exception) -> bool:
    message = str(exc)
    return "CERTIFICATE_VERIFY_FAILED" in message or "certificate verify failed" in message


def _extract_search_reference(docs_url: str) -> tuple[str | None, str | None]:
    query = parse_qs(urlparse(docs_url).query)
    for field in _SEARCH_QUERY_FIELDS:
        values = query.get(field)
        if values and values[0].strip():
            return field, values[0].strip()
    return None, None


def _keyobject_item_value(key_object: dict[str, Any], field_name: str) -> str | None:
    for item in key_object.get("Items", []):
        if item.get("FieldName") != field_name:
            continue
        value = item.get("Value")
        return str(value).strip() if value is not None else None
    return None


def _find_exact_planning_keyobject(payload: dict[str, Any], expected_reference: str) -> dict[str, Any] | None:
    expected = expected_reference.strip().upper()
    key_objects = payload.get("KeyObjects", [])
    for key_object in key_objects:
        candidates = [
            str(key_object.get("DisplayTitle") or "").strip(),
            str(_keyobject_item_value(key_object, "SDescription") or "").strip(),
            str(_keyobject_item_value(key_object, "InternetDesc") or "").strip(),
        ]
        if any(candidate.upper() == expected for candidate in candidates if candidate):
            return key_object
    if len(key_objects) == 1:
        return key_objects[0]
    return None


def parse_planning_documents_list(payload: dict[str, Any], base_url: str) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for row in payload.get("CompleteDocument", []):
        doc_no = str(row.get("DocNo") or "").strip()
        if not doc_no:
            continue

        file_name = str(row.get("FileName") or "").strip()
        title = str(row.get("Title") or "").strip()
        doc_desc = str(row.get("DocDesc") or row.get("TypeCode") or "").strip()
        lower_source = str(row.get("DocSource") or "").strip().upper()
        date_raw = str(row.get("ReceivedDate") or row.get("DocDate") or "").strip()
        date_str = date_raw.split("T", 1)[0] if date_raw else ""

        filename_param = file_name or title or f"document-{doc_no}"
        canonical_url = urljoin(
            base_url,
            f"/w2webparts/Resource/Civica/Handler.ashx/doc/pagestream?DocNo={quote(doc_no)}&pdf=true",
        )
        download_route = urljoin(
            base_url,
            (
                "/w2webparts/Resource/Civica/Handler.ashx/Doc/pagestream"
                f"?cd=download&pdf=false&docno={quote(doc_no)}&filename={quote(filename_param)}"
            ),
        )
        pdf_route = urljoin(
            base_url,
            (
                "/w2webparts/Resource/Civica/Handler.ashx/doc/pagestream"
                f"?DocNo={quote(doc_no)}&pdf=true&filename={quote(filename_param)}"
            ),
        )
        if lower_source in {"X", "O", "L"}:
            candidates = [download_route, pdf_route, canonical_url]
        else:
            candidates = [pdf_route, download_route, canonical_url]

        deduped_candidates: list[str] = []
        for candidate in candidates:
            if candidate not in deduped_candidates:
                deduped_candidates.append(candidate)

        documents.append(
            {
                "doc_no": doc_no,
                "date_published": date_str,
                "document_type": doc_desc,
                "description": title,
                "drawing_number": file_name,
                "document_url": canonical_url,
                "download_candidates": deduped_candidates,
            }
        )
    return documents


def _detect_extension(content: bytes, content_type: str) -> str:
    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            if ext == ".zip" and content_type:
                ct = content_type.split(";", 1)[0].strip().lower()
                office_ext = _CTYPE_EXTS.get(ct)
                if office_ext:
                    return office_ext
            return ext
    ct = (content_type or "").split(";", 1)[0].strip().lower()
    return _CTYPE_EXTS.get(ct, ".pdf")


def _looks_like_html_or_json(response: httpx.Response) -> bool:
    content_type = (response.headers.get("content-type") or "").lower()
    if "text/html" in content_type or "application/json" in content_type:
        return True
    prefix = response.content[:256].lstrip().lower()
    return prefix.startswith(b"<!doctype html") or prefix.startswith(b"<html") or prefix.startswith(b"{")


class PlanningDocsDocumentScraper:
    """Async scraper for /planning/planning-documents backends."""

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
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=60.0, headers=_client_headers(), follow_redirects=True)
        self._insecure_client = httpx.AsyncClient(
            timeout=60.0,
            headers=_client_headers(),
            follow_redirects=True,
            verify=False,
        )
        return self

    async def __aexit__(self, *args):
        if self._client is not None:
            await self._client.aclose()
        if self._insecure_client is not None:
            await self._insecure_client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use 'async with PlanningDocsDocumentScraper() as scraper:'")
        return self._client

    @property
    def insecure_client(self) -> httpx.AsyncClient:
        if self._insecure_client is None:
            raise RuntimeError("Use 'async with PlanningDocsDocumentScraper() as scraper:'")
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

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            try:
                return await self.client.request(method, url, **kwargs)
            except Exception as exc:
                if not _is_tls_error(exc):
                    raise
                return await self.insecure_client.request(method, url, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        try:
            search_field, search_value = _extract_search_reference(docs_url)
            if not search_field or not search_value:
                self.stats["failed"] += 1
                return [], "missing_reference_query_param"

            parsed = urlparse(docs_url)
            root_url = f"{parsed.scheme}://{parsed.netloc}"
            search_url = urljoin(root_url, "/w2webparts/Resource/Civica/Handler.ashx/keyobject/pagedsearch")
            search_payload = {
                "refType": "GFPlanning",
                "fromRow": 1,
                "toRow": 20,
                "keyNumb": None,
                "keyText": None,
                "searchFields": {search_field: search_value},
                "getFields": None,
                "NoTotalRows": None,
                "sort": None,
            }
            search_response = await self._request(
                "POST",
                search_url,
                json=search_payload,
                headers=_json_headers(),
            )
            if search_response.status_code != 200:
                self.stats["failed"] += 1
                return [], f"keyobject_{_http_status_failure_code(search_response.status_code)}"

            key_payload = search_response.json()
            key_object = _find_exact_planning_keyobject(key_payload, search_value)
            if key_object is None:
                self.stats["failed"] += 1
                return [], "no_exact_keyobject_match"

            key_number = key_object.get("KeyNumber")
            if not key_number:
                self.stats["failed"] += 1
                return [], "missing_key_number"

            docs_response = await self._request(
                "POST",
                urljoin(root_url, "/w2webparts/Resource/Civica/Handler.ashx/doc/list"),
                json={
                    "KeyNumb": key_number,
                    "KeyText": "Subject",
                    "RefType": "GFPlanning",
                    "ProcessNo": None,
                    "OrderBy": None,
                    "PageSize": 500,
                    "Filters": None,
                },
                headers=_json_headers(),
            )
            if docs_response.status_code != 200:
                self.stats["failed"] += 1
                return [], f"document_list_{_http_status_failure_code(docs_response.status_code)}"

            documents = parse_planning_documents_list(docs_response.json(), root_url)
        except httpx.TimeoutException:
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.HTTPError as exc:
            self.stats["failed"] += 1
            logger.error("Network error for %s: %s", docs_url, exc)
            return [], "network_error"
        except json.JSONDecodeError as exc:
            self.stats["failed"] += 1
            logger.error("JSON parse error for %s: %s", docs_url, exc)
            return [], "parse_error"
        except Exception as exc:
            self.stats["failed"] += 1
            logger.error("Unexpected error for %s: %s", docs_url, exc)
            return [], "unexpected_error"

        if documents:
            self.stats["success"] += 1
        else:
            self.stats["no_docs"] += 1
        return documents, None

    async def download_document(
        self,
        document: dict[str, Any],
        target_path: Path,
        referer: str,
        max_retries: int = 3,
    ) -> tuple[int, Path]:
        urls = [url for url in (document.get("download_candidates") or []) if url]
        if not urls and document.get("document_url"):
            urls = [str(document["document_url"])]
        if not urls:
            return 0, target_path

        for candidate in urls:
            for attempt in range(max_retries):
                try:
                    response = await self._request(
                        "GET",
                        candidate,
                        headers={"Referer": referer, "User-Agent": IDOX_USER_AGENT},
                    )
                except httpx.TimeoutException:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.error("Timeout for %s", candidate)
                    break
                except httpx.HTTPError as exc:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    logger.error("Network error for %s: %s", candidate, exc)
                    break

                if response.status_code == 200 and response.content and not _looks_like_html_or_json(response):
                    ext = _detect_extension(response.content, response.headers.get("content-type", ""))
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(response.content)
                    return len(response.content), final_path

                if response.status_code in _RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                if response.status_code != 200:
                    logger.warning("HTTP %s for %s", response.status_code, candidate)
                break

        return 0, target_path
