"""Civica MSP planning portal document scraper.

Misnomer alert: the portal_type label "msp_idox" was applied during URL
classification on the assumption these were a newer Idox skin, but the backend
is actually **Civica** (footer reads "© Civica 2020", page title "Civica
Town"). The shared deployments observed so far are:

  - Havering:        https://msp.havering.gov.uk/
  - Great Yarmouth:  https://portal.great-yarmouth.gov.uk/

The frontend is an SPA driven by a JavaScript fragment URL of the form

  https://<host>/planning/search-applications#VIEW?RefType=PLANNINGCASE&KeyText=<ref>

Document discovery does NOT need a browser. The SPA hits two JSON endpoints
on the same origin:

  POST /civica/Resource/Civica/Handler.ashx/keyobject/search
       body: {"refType":"PLANNINGCASE","fromRow":1,"toRow":1,"keyText":"<ref>"}
       (used to verify the case exists / fetch metadata)

  POST /civica/Resource/Civica/Handler.ashx/doc/list
       body: {"KeyNumb":"0","KeyText":"<ref>","RefType":"PLANNINGCASE"}
       returns JSON: {"CompleteDocument": [...], "RowCount": N}

Each document record exposes a numeric ``DocNo`` plus ``FileName`` /
``FileExtension`` / ``DocDesc`` / ``DocDate``. The download URL pattern is:

  GET /civica/Resource/Civica/Handler.ashx/Doc/pagestream?cd=inline&pdf=true&docno=<DocNo>

The Havering backend is sometimes slow (intermittent 504 Gateway Timeout
from the upstream Civica handler) and benefits from a retry. The Civica
``zipstream`` endpoint exists too but produces flaky downloads in early
testing; per-doc fetches are the safer default.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx

from .idox_scraper import DomainRateLimiter

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3
RETRY_DELAY = 5.0

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

# Havering's Civica handler queues badly under load; recon saw single requests
# take >60s and occasionally several thousand seconds. Use a generous default
# timeout for the whole scraper (Great Yarmouth responds quickly, so this only
# kicks in when Havering is slow). The per-domain lock below ensures we don't
# pile concurrent requests on a slow handler.
DEFAULT_TIMEOUT = 120.0
DEFAULT_PER_DOMAIN_DELAY = 2.0
DEFAULT_MAX_CONCURRENT_DOMAINS = 2

DOC_LIST_PATH = "/civica/Resource/Civica/Handler.ashx/doc/list"
DOC_DOWNLOAD_PATH = "/civica/Resource/Civica/Handler.ashx/Doc/pagestream"


def _safe_name(value: str, fallback: str = "document") -> str:
    safe = _SAFE_NAME_RE.sub("_", value).strip("._-")
    return safe[:140] or fallback


def parse_msp_url(docs_url: str) -> tuple[str, str] | None:
    """Extract (origin, key_text) from a fragment-style MSP URL.

    Example input:
        https://msp.havering.gov.uk/planning/search-applications#VIEW?RefType=PLANNINGCASE&KeyText=P1513.23

    Returns ("https://msp.havering.gov.uk", "P1513.23") or None.
    """
    parsed = urlparse(docs_url)
    if not parsed.scheme or not parsed.netloc:
        return None

    origin = f"{parsed.scheme}://{parsed.netloc}"
    fragment = parsed.fragment or ""
    # The fragment is "VIEW?RefType=PLANNINGCASE&KeyText=06%2F24%2F0687%2FCD"
    if "KeyText=" not in fragment:
        return None

    # Pull KeyText out of the fragment query
    m = re.search(r"KeyText=([^&]+)", fragment)
    if not m:
        return None
    key_text = unquote(m.group(1))
    return origin, key_text


def parse_documents(payload: dict) -> list[dict]:
    """Normalise the doc/list JSON payload into a list of doc dicts."""
    docs = payload.get("CompleteDocument") or []
    out: list[dict] = []
    for d in docs:
        doc_no = str(d.get("DocNo") or "").strip()
        if not doc_no:
            continue
        ext = (d.get("FileExtension") or "").strip().lstrip(".").lower()
        file_name = (d.get("FileName") or "").strip()
        title = (d.get("Title") or "").strip()
        desc = (d.get("DocDesc") or "").strip()
        out.append(
            {
                "doc_no": doc_no,
                "file_name": file_name,
                "title": title or file_name or doc_no,
                "description": desc,
                "file_extension": ext,
                "document_type": d.get("TypeCode") or "",
                "date_published": (d.get("DocDate") or "")[:10],
                "category": d.get("DocCategory") or "",
            }
        )
    return out


def doc_download_url(origin: str, doc_no: str) -> str:
    return f"{origin}{DOC_DOWNLOAD_PATH}?cd=inline&pdf=true&docno={doc_no}"


class MspCivicaScraper:
    """Async scraper for Civica MSP portals (Havering, Great Yarmouth)."""

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        user_agent: str = "Mozilla/5.0 (compatible; NestaResearchScraper/1.0)",
        rate_limiter: Optional[DomainRateLimiter] = None,
    ):
        self._client: Optional[httpx.AsyncClient] = None
        self._timeout = timeout
        self._user_agent = user_agent
        self.rate_limiter = rate_limiter or DomainRateLimiter(
            per_domain_delay=DEFAULT_PER_DOMAIN_DELAY,
            max_concurrent=DEFAULT_MAX_CONCURRENT_DOMAINS,
        )
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            headers={
                "User-Agent": self._user_agent,
                "Accept": "application/json, text/plain, */*",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with MspCivicaScraper() as scraper:'")
        return self._client

    async def _post_json(self, url: str, body: dict, referer: str) -> Optional[httpx.Response]:
        headers = {
            "Content-Type": "application/json",
            "Origin": referer.rsplit("/", 1)[0] if "/" in referer else referer,
            "Referer": referer,
        }
        domain = urlparse(url).netloc
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.rate_limiter.throttle(domain):
                    resp = await self.client.post(url, json=body, headers=headers)
            except httpx.HTTPError as exc:
                if attempt < MAX_RETRIES:
                    logger.warning("POST %s error attempt %s: %s", url, attempt, exc)
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                logger.error("POST %s failed after %s attempts: %s", url, MAX_RETRIES, exc)
                return None

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                logger.warning("POST %s -> %s attempt %s, retrying", url, resp.status_code, attempt)
                await asyncio.sleep(RETRY_DELAY * attempt)
                continue
            return resp
        return None

    async def _get_with_retry(self, url: str, referer: str) -> Optional[httpx.Response]:
        headers = {"Referer": referer}
        domain = urlparse(url).netloc
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.rate_limiter.throttle(domain):
                    resp = await self.client.get(url, headers=headers)
            except httpx.HTTPError as exc:
                if attempt < MAX_RETRIES:
                    logger.warning("GET %s error attempt %s: %s", url, attempt, exc)
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                logger.error("GET %s failed after %s attempts: %s", url, MAX_RETRIES, exc)
                return None

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
                logger.warning("GET %s -> %s attempt %s, retrying", url, resp.status_code, attempt)
                await asyncio.sleep(RETRY_DELAY * attempt)
                continue
            return resp
        return None

    async def list_documents(self, docs_url: str) -> list[dict]:
        parsed = parse_msp_url(docs_url)
        if not parsed:
            logger.warning("Could not parse MSP URL: %s", docs_url)
            self.stats["failed"] += 1
            return []
        origin, key_text = parsed

        resp = await self._post_json(
            url=f"{origin}{DOC_LIST_PATH}",
            body={"KeyNumb": "0", "KeyText": key_text, "RefType": "PLANNINGCASE"},
            referer=f"{origin}/planning/search-applications",
        )
        if resp is None or resp.status_code != 200:
            self.stats["failed"] += 1
            return []

        try:
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Bad JSON from %s: %s", origin, exc)
            self.stats["failed"] += 1
            return []

        docs = parse_documents(payload)
        if not docs:
            self.stats["no_docs"] += 1
        else:
            self.stats["success"] += 1
        # annotate each doc with its absolute download URL
        for d in docs:
            d["document_url"] = doc_download_url(origin, d["doc_no"])
        return docs

    async def download_document(
        self,
        docs_url: str,
        doc_no: str,
        out_path: Path,
    ) -> tuple[bool, Optional[str], Optional[Path]]:
        """Download a single doc. Returns ``(ok, content_type, saved_path)``.

        ``saved_path`` may differ from ``out_path``: Civica's
        ``pagestream?pdf=true`` server-renders every doc to PDF regardless of
        the original ``FileName`` (e.g. a ``.docx`` arrives as PDF bytes).
        When the response is PDF, we force a ``.pdf`` suffix on the saved
        file so downstream extraction can trust the extension.
        """
        parsed = parse_msp_url(docs_url)
        if not parsed:
            return False, None, None
        origin, _ = parsed
        url = doc_download_url(origin, doc_no)
        referer = f"{origin}/planning/search-applications"

        resp = await self._get_with_retry(url, referer=referer)
        if resp is None or resp.status_code != 200:
            return False, None, None

        ct = resp.headers.get("content-type", "")
        is_pdf = "application/pdf" in ct.lower() or resp.content[:4] == b"%PDF"
        saved = out_path
        if is_pdf and out_path.suffix.lower() != ".pdf":
            saved = out_path.with_suffix(".pdf")
        saved.parent.mkdir(parents=True, exist_ok=True)
        saved.write_bytes(resp.content)
        return True, ct, saved
