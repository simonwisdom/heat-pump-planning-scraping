"""Aifusion public document viewer scraper.

Central Bedfordshire's current Aifusion viewer is a static HTML shell at:

    https://cbc.aifusion.io/planning/publicViewer.html?caseID=CB%2F25%2F01444%2FFULL

The page JavaScript transforms ``caseID`` by replacing slashes with hyphens and
calls:

    https://api.cbc.aifusion.io/planning/docs?caseId=CB-25-01444-FULL

Legacy rows in ``ashp.db`` may still point at the retired ``cbstor`` hostname,
but the query string case reference can be reused with the current API.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import httpx

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3

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
    "application/vnd.ms-excel": ".xls",
    "text/plain": ".txt",
    "text/html": ".html",
}


def _http_status_failure_code(status_code: int) -> str:
    if status_code == 403:
        return "http_403"
    if status_code == 404:
        return "not_found"
    if status_code == 429:
        return "http_429"
    if 500 <= status_code < 600:
        return "http_5xx"
    return f"http_{status_code}"


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


def extract_case_id(docs_url: str) -> str | None:
    """Extract the Aifusion case reference from a public viewer URL."""
    qs = parse_qs(urlparse(docs_url).query)
    values = qs.get("caseID") or qs.get("caseId") or qs.get("caseid")
    case_id = values[0].strip() if values else ""
    return case_id or None


def transform_case_id(case_id: str) -> str:
    """Match the public viewer JS: replace slash/backslash separators with hyphens."""
    return case_id.replace("/", "-").replace("\\", "-")


def api_url_from_viewer_url(docs_url: str) -> str | None:
    """Build the Aifusion docs API URL from a public viewer URL."""
    case_id = extract_case_id(docs_url)
    if not case_id:
        return None

    parsed = urlparse(docs_url)
    host = parsed.netloc.lower()
    if host.startswith("api."):
        api_host = host
    elif host.endswith(".aifusion.io"):
        api_host = f"api.{host}"
    else:
        # Central Bedfordshire legacy cbstor URLs now map to cbc.aifusion.io.
        api_host = "api.cbc.aifusion.io"

    return f"https://{api_host}/planning/docs?caseId={quote(transform_case_id(case_id))}"


def parse_aifusion_documents(data: dict) -> list[dict]:
    """Flatten an Aifusion ``documentsByType`` JSON response."""
    documents: list[dict] = []
    seen_ids: set[str] = set()
    parent = data.get("parentCase") or {}

    for group in data.get("documentsByType") or []:
        group_type = group.get("type") or ""
        for doc in group.get("documents") or []:
            doc_id = doc.get("id") or doc.get("downloadUrl") or doc.get("url")
            if doc_id and doc_id in seen_ids:
                continue
            if doc_id:
                seen_ids.add(doc_id)

            filename = doc.get("filename") or "document"
            documents.append(
                {
                    "date_published": "",
                    "document_type": doc.get("type") or group_type,
                    "description": filename,
                    "drawing_number": "",
                    "document_url": doc.get("downloadUrl") or doc.get("url"),
                    "source_url": doc.get("url"),
                    "filename": filename,
                    "id": doc.get("id"),
                    "case_id": parent.get("caseId"),
                    "address": parent.get("address"),
                }
            )

    return [doc for doc in documents if doc.get("document_url")]


class AifusionDocumentScraper:
    """Async scraper for Aifusion public viewer document APIs."""

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

    async def __aenter__(self) -> "AifusionDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=45.0,
            follow_redirects=True,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                "Origin": "https://cbc.aifusion.io",
                "Referer": "https://cbc.aifusion.io/planning/publicViewer.html",
            },
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._client is not None:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use 'async with AifusionDocumentScraper() as scraper:'")
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
        """Scrape document metadata from an Aifusion viewer URL."""
        api_url = api_url_from_viewer_url(docs_url)
        if api_url is None:
            self.stats["failed"] += 1
            return [], "missing_case_id"

        try:
            resp = await self._get(api_url)
            if resp.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(resp.status_code)

            content_type = resp.headers.get("Content-Type", "")
            if "application/json" not in content_type.lower():
                self.stats["failed"] += 1
                return [], "non_json_response"

            documents = parse_aifusion_documents(resp.json())
        except httpx.ConnectTimeout:
            self.stats["failed"] += 1
            return [], "connect_timeout"
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
        except ValueError:
            self.stats["failed"] += 1
            return [], "invalid_json"
        except Exception as exc:
            logger.error("Unexpected Aifusion scrape error for %s: %s", docs_url, exc)
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
        """Download one Aifusion document, fixing the extension from response bytes."""
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
                    ext = _detect_extension(resp.content, resp.headers.get("Content-Type", ""))
                    final_path = target_path
                    if not final_path.suffix:
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
                    logger.info(
                        "%s for %s (attempt %d/%d)",
                        type(exc).__name__,
                        doc_url,
                        attempt + 1,
                        max_retries,
                    )
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
