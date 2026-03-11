"""Agile Applications planning API document metadata scraper."""

from __future__ import annotations

import logging
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
        if (
            not parsed_doc["document_type"]
            and not parsed_doc["description"]
            and not parsed_doc["document_url"]
        ):
            continue
        parsed_documents.append(parsed_doc)
    return parsed_documents


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
