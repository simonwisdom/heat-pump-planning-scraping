"""Agile Applications planning API document metadata scraper."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import httpx

from .config import (
    AGILE_BASE_URL,
    AGILE_MAX_CONCURRENT_DOMAINS,
    AGILE_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)
from .idox_scraper import DomainRateLimiter

logger = logging.getLogger(__name__)

AGILE_AUTHORITIES = (
    "CANNOCK",
    "EXMOOR",
    "FLINTSHIRE",
    "ISLINGTON",
    "LDNPA",
    "MIDDLESBROUGH",
    "MOLE",
    "NFNPA",
    "OPDC",
    "PCNPA",
    "PEMBROKESHIRE",
    "REDBRIDGE",
    "RUGBY",
    "SLOUGH",
    "SNOWDONIA",
    "TMBC",
    "YORKSHIREDALES",
)

# Authorities whose Agile identity / planning-API config is currently broken
# upstream. Recon found Exmoor's identity client returns an empty API_URL and
# the shared planning API responds 500 for all Exmoor references. Gate here
# so production runs don't hammer dead endpoints; revisit if Agile fixes it.
AGILE_DISABLED_AUTHORITIES = frozenset({"EXMOOR"})


class AgileAuthorityDisabled(RuntimeError):
    """Raised when an Agile authority is gated out of production scrapes."""


AGILE_IDENTITY_BASE_URL = "https://identity.agileapplications.co.uk"
AGILE_DEFAULT_HEADERS = {"User-Agent": IDOX_USER_AGENT}
AGILE_MAGIC_EXTS: list[tuple[bytes, str]] = [
    (b"%PDF", ".pdf"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"\x89PNG", ".png"),
    (b"GIF8", ".gif"),
    (b"II*\x00", ".tif"),
    (b"MM\x00*", ".tif"),
    (b"PK\x03\x04", ".zip"),
    (b"\xd0\xcf\x11\xe0", ".doc"),
]
AGILE_CONTENT_TYPE_EXTS = {
    "application/pdf": ".pdf",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/tiff": ".tif",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/zip": ".zip",
}


def _normalise_client_name(client_name: str) -> str:
    return client_name.strip().upper()


def _agile_headers(client_name: str) -> dict[str, str]:
    normalised = _normalise_client_name(client_name)
    return {
        "x-client": normalised,
        "x-product": "CITIZENPORTAL",
        "x-service": "PA",
        "Accept": "application/json",
    }


def _safe_filename(value: str, default: str = "document") -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", value).strip().strip(".")
    name = re.sub(r"\s+", " ", name)
    return name[:180] or default


def _detect_extension(content: bytes, content_type: str) -> str:
    head = content[:8]
    for signature, extension in AGILE_MAGIC_EXTS:
        if head.startswith(signature):
            if extension == ".zip":
                media_type = content_type.split(";", 1)[0].strip().lower()
                return AGILE_CONTENT_TYPE_EXTS.get(media_type, extension)
            return extension
    media_type = content_type.split(";", 1)[0].strip().lower()
    return AGILE_CONTENT_TYPE_EXTS.get(media_type, ".pdf")


def _build_download_url(document_hash: str | None) -> str | None:
    if not document_hash:
        return None
    return f"{AGILE_BASE_URL}/api/application/document/{document_hash}"


def parse_agile_documents(documents: list[dict], client_name: str) -> list[dict]:
    _ = client_name
    parsed_documents = []
    for doc in documents:
        parsed_doc = {
            "date_published": doc.get("receivedDate") or "",
            "document_type": doc.get("mediaDescription") or "",
            "description": doc.get("name") or "",
            "drawing_number": str(doc.get("documentId") or ""),
            "document_url": _build_download_url(doc.get("documentHash")),
        }
        if not parsed_doc["document_type"] and not parsed_doc["description"] and not parsed_doc["document_url"]:
            continue
        parsed_documents.append(parsed_doc)
    return parsed_documents


async def resolve_client_code(client_slug: str) -> str:
    if _normalise_client_name(client_slug) in AGILE_DISABLED_AUTHORITIES:
        raise AgileAuthorityDisabled(f"Agile authority {client_slug!r} is gated out")
    async with httpx.AsyncClient(timeout=30.0, headers=AGILE_DEFAULT_HEADERS) as client:
        response = await client.get(f"{AGILE_IDENTITY_BASE_URL}/api/client/get", params={"url": client_slug})
        response.raise_for_status()
        payload = response.json()
    code = str(payload.get("code") or "").strip()
    if not code:
        raise ValueError(f"No Agile client code returned for slug {client_slug!r}")
    return code


async def _request_json(
    client: httpx.AsyncClient,
    url: str,
    client_name: str,
    params: dict[str, Any] | None = None,
) -> Any:
    response = await client.get(url, params=params, headers=_agile_headers(client_name))
    response.raise_for_status()
    return response.json()


async def search_applications(client_name: str, query: str, size: int = 25) -> dict:
    params = {"proposal": query, "size": size}
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": IDOX_USER_AGENT}) as client:
        return await _request_json(client, f"{AGILE_BASE_URL}/api/application/search", client_name, params)


async def search_applications_by_reference(client_name: str, reference: str, size: int = 10) -> dict:
    params = {"reference": reference, "size": size}
    async with httpx.AsyncClient(timeout=30.0, headers=AGILE_DEFAULT_HEADERS) as client:
        return await _request_json(client, f"{AGILE_BASE_URL}/api/application/search", client_name, params)


def find_exact_application(search_payload: dict[str, Any], reference: str) -> dict[str, Any] | None:
    expected = reference.strip().upper()
    results = search_payload.get("results") or []
    for result in results:
        candidates = [
            str(result.get("reference") or "").strip(),
            str(result.get("webReference") or "").strip(),
        ]
        if any(candidate.upper() == expected for candidate in candidates if candidate):
            return result
    if len(results) == 1:
        return results[0]
    return None


async def get_application_detail(app_id: str | int, client_name: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": IDOX_USER_AGENT}) as client:
        return await _request_json(client, f"{AGILE_BASE_URL}/api/application/{app_id}", client_name)


async def get_application_documents(app_id: str | int, client_name: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": IDOX_USER_AGENT}) as client:
        documents = await _request_json(
            client,
            f"{AGILE_BASE_URL}/api/application/{app_id}/document",
            client_name,
        )
    if not isinstance(documents, list):
        logger.warning("Unexpected response type for Agile document list %s", app_id)
        return []
    return documents


class AgileDocumentScraper:
    """Async scraper for Agile Applications document listings."""

    def __init__(self, rate_limiter: DomainRateLimiter | None = None):
        self.rate_limiter = rate_limiter or DomainRateLimiter(
            per_domain_delay=AGILE_RATE_LIMIT_PER_DOMAIN,
            max_concurrent=AGILE_MAX_CONCURRENT_DOMAINS,
        )
        self._client: httpx.AsyncClient | None = None
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": IDOX_USER_AGENT},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with AgileDocumentScraper() as scraper:'")
        return self._client

    async def scrape_documents(self, app_id: str | int, client_name: str) -> list[dict]:
        domain = urlparse(AGILE_BASE_URL).netloc
        url = f"{AGILE_BASE_URL}/api/application/{app_id}/document"
        try:
            await self.rate_limiter.acquire(domain)
            response = await self.client.get(url, headers=_agile_headers(client_name))
            self.rate_limiter.release(domain)

            if response.status_code != 200:
                logger.warning("HTTP %s for %s", response.status_code, url)
                self.stats["failed"] += 1
                return []

            raw_documents = response.json()
            if not isinstance(raw_documents, list):
                logger.warning("Unexpected response type for %s", url)
                self.stats["failed"] += 1
                return []

            documents = parse_agile_documents(raw_documents, client_name)
            if documents:
                self.stats["success"] += 1
            else:
                self.stats["no_docs"] += 1
            return documents

        except httpx.HTTPError as exc:
            logger.error("HTTP error for %s: %s", url, exc)
            self.stats["failed"] += 1
            try:
                self.rate_limiter.release(domain)
            except ValueError:
                pass
            return []
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", url, exc)
            self.stats["failed"] += 1
            try:
                self.rate_limiter.release(domain)
            except ValueError:
                pass
            return []

    async def scrape_reference(self, reference: str, client_name: str) -> tuple[dict[str, Any] | None, list[dict]]:
        if _normalise_client_name(client_name) in AGILE_DISABLED_AUTHORITIES:
            raise AgileAuthorityDisabled(f"Agile authority {client_name!r} is gated out")
        search_payload = await search_applications_by_reference(client_name, reference)
        application = find_exact_application(search_payload, reference)
        if not application:
            self.stats["failed"] += 1
            return None, []

        documents = await self.scrape_documents(application["id"], client_name)
        return application, documents

    async def download_file(self, document: dict[str, Any], output_dir: Path, client_name: str) -> Path | None:
        document_url = document.get("document_url")
        if not document_url:
            return None

        output_dir.mkdir(parents=True, exist_ok=True)
        response = await self.client.get(str(document_url), headers=_agile_headers(client_name))
        if response.status_code != 200:
            return None
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type.lower() or response.content[:64].lstrip().startswith(b"<"):
            return None

        extension = _detect_extension(response.content, content_type)
        stem = _safe_filename(
            str(document.get("description") or document.get("document_type") or document.get("drawing_number") or "")
        )
        if not stem.lower().endswith(extension):
            filename = f"{stem}{extension}"
        else:
            filename = stem
        output_path = output_dir / filename
        suffix = 2
        while output_path.exists():
            output_path = output_dir / f"{Path(filename).stem}-{suffix}{Path(filename).suffix}"
            suffix += 1
        output_path.write_bytes(response.content)
        return output_path

    async def download_files(
        self,
        documents: list[dict[str, Any]],
        output_dir: Path,
        client_name: str,
        limit: int | None = None,
    ) -> list[Path]:
        downloaded: list[Path] = []
        for document in documents[:limit]:
            path = await self.download_file(document, output_dir, client_name)
            if path:
                downloaded.append(path)
        return downloaded

    async def scrape_batch(
        self,
        applications: list[dict],
        on_result: Callable | None = None,
    ) -> dict[str, list[dict]]:
        results: dict[str, list[dict]] = {}
        for app in applications:
            uid = app["uid"]
            app_id = app.get("app_id") or app.get("id")
            client_name = app.get("client_name") or app.get("authority")
            if not app_id or not client_name:
                continue
            documents = await self.scrape_documents(app_id, client_name)
            results[uid] = documents
            if on_result:
                on_result(uid, documents)
        return results
