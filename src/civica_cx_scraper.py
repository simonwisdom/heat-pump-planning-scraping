"""Civica CX planning register document scraper.

Civica CX (`register.civicacx.co.uk/<authority>/Planning/...`) is a multi-tenant
SaaS used by several UK councils (Erewash being the canonical sample in the
ASHP corpus). The detail URL stored in PlanIt looks like::

    https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails
        ?id=<numeric_app_id>&dpid=DocumentsPage

Recon status (2026-04-30)
-------------------------
The host is fronted by Cloudflare with an aggressive WAF rule that returns a
hard ``HTTP 403`` "Sorry, you have been blocked" page on every request from
both a UK residential IP and a UK datacenter VPS, regardless of User-Agent,
HTTP version, or cookie state. There is **no** JS challenge / Turnstile to
solve — it is a flat IP/ASN block. Both ``httpx`` and ``playwright`` (headless
and headful, with anti-fingerprint patches) hit the same wall.

Until the orchestrator can route requests through an egress that Civica's
WAF allows (residential-IP proxy, council-network egress, or after Civica
removes the rule), production runs of this scraper are blocked.

URL conventions (inferred, not yet verified end-to-end)
-------------------------------------------------------
Listing page (HTML, ASP.NET MVC, server-rendered)::

    /<authority>/Planning/Details/ShowDetails?id=<num>&dpid=DocumentsPage

Per-document download endpoint observed in third-party search results::

    /<authority>/Planning/DetailsTable/Download
        ?dpid=DocumentsPage
        &dcid=Documents
        &traid=DocumentDownload
        &id=<document_guid>

The listing HTML almost certainly enumerates each document as a row containing
the GUID; the parser here looks for that pattern and falls back gracefully if
the page shape is different. Verify with a real listing page once egress is
available.

Interface
---------
The scraper follows the standard project shape::

    fetch_listing(detail_url) -> html
    parse_documents(html, listing_url) -> list[dict]
    download_document(doc_url, target_path) -> (bytes_written, final_path)

The dry-run entry point is ``CivicaCxDocumentScraper().scrape_documents`` which
returns ``(documents, failure_code)``; ``failure_code`` is ``"cloudflare_block"``
when the WAF returns a 403 + Cloudflare HTML body — the orchestrator can use
this to short-circuit retries and surface infra-level blockers in the report.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

logger = logging.getLogger(__name__)

CIVICA_HOST = "register.civicacx.co.uk"
CIVICA_BASE = "https://register.civicacx.co.uk"

MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
# Inline GUID pattern used by the document download endpoint.
_GUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------


def parse_listing_url(stored_url: str) -> tuple[str, str]:
    """Extract (authority, app_id) from a stored Civica CX detail URL.

    >>> parse_listing_url(
    ...     "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails"
    ...     "?id=40428&dpid=DocumentsPage"
    ... )
    ('Erewash', '40428')
    """
    parsed = urlparse(stored_url)
    if parsed.netloc.lower() != CIVICA_HOST:
        raise ValueError(f"Not a Civica CX URL: {stored_url}")
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        raise ValueError(f"Empty path in Civica CX URL: {stored_url}")
    authority = parts[0]
    qs = parse_qs(parsed.query)
    app_id = (qs.get("id") or [""])[0]
    if not app_id:
        raise ValueError(f"No id= query param in {stored_url}")
    return authority, app_id


def listing_url(authority: str, app_id: str) -> str:
    """Canonicalise the documents-tab listing URL for an app."""
    return f"{CIVICA_BASE}/{authority}/Planning/Details/ShowDetails?id={app_id}&dpid=DocumentsPage"


def document_download_url(authority: str, document_guid: str) -> str:
    """Build the document download URL given an authority slug and GUID."""
    return (
        f"{CIVICA_BASE}/{authority}/Planning/DetailsTable/Download"
        f"?dpid=DocumentsPage&dcid=Documents&traid=DocumentDownload"
        f"&id={document_guid}"
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_html(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def is_cloudflare_block(html: str, status_code: int) -> bool:
    """Detect the Cloudflare 403 firewall page."""
    if status_code != 403:
        return False
    snippet = html[:1500].lower()
    return "cloudflare" in snippet and ("you have been blocked" in snippet or "attention required" in snippet)


def parse_civica_documents(html: str, authority: str, listing_url_str: str) -> list[dict]:
    """Extract document rows from a Civica CX documents-tab listing.

    The exact DOM was not observed during recon (Cloudflare blocked egress).
    Heuristic parser:

    * Walk every ``<a>`` whose ``href`` matches the
      ``/Planning/DetailsTable/Download...id=<guid>`` pattern; capture text as
      the description.
    * Fall back to scanning for raw GUIDs in onclick / data-* attributes.
    """
    documents: list[dict] = []
    seen: set[str] = set()
    soup = _parse_html(html)

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        absolute = urljoin(listing_url_str, href)
        if "DetailsTable/Download" not in absolute:
            continue
        guid_match = _GUID_RE.search(absolute)
        if not guid_match:
            continue
        guid = guid_match.group(0).lower()
        if guid in seen:
            continue
        seen.add(guid)

        text = " ".join(anchor.get_text(" ", strip=True).split())
        # Try to grab a sibling cell as the document type / description.
        row = anchor.find_parent("tr")
        cells: list[str] = []
        if row is not None:
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
        document_type = cells[0] if cells else ""
        date_published = cells[1] if len(cells) > 1 else ""
        description = text or (cells[2] if len(cells) > 2 else "")

        documents.append(
            {
                "document_guid": guid,
                "document_type": document_type,
                "date_published": date_published,
                "description": description,
                "document_url": document_download_url(authority, guid),
                "listing_url": listing_url_str,
            }
        )

    if documents:
        return documents

    # Fallback: scan attributes / inline JS for GUIDs.
    for guid in {m.group(0).lower() for m in _GUID_RE.finditer(html)}:
        if guid in seen:
            continue
        seen.add(guid)
        documents.append(
            {
                "document_guid": guid,
                "document_type": "",
                "date_published": "",
                "description": "",
                "document_url": document_download_url(authority, guid),
                "listing_url": listing_url_str,
            }
        )
    return documents


# ---------------------------------------------------------------------------
# Download helpers (mirrors other scrapers)
# ---------------------------------------------------------------------------


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


def _detect_extension(content: bytes, content_type: str, url_path: str = "") -> str:
    suffix = Path(urlparse(url_path).path).suffix.lower()
    if suffix in {".pdf", ".doc", ".docx", ".gif", ".jpg", ".png", ".tif", ".tiff", ".xls", ".xlsx"}:
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


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------


class CivicaCxDocumentScraper:
    """Async scraper for Civica CX planning document listings."""

    def __init__(
        self,
        per_domain_delay: float = IDOX_RATE_LIMIT_PER_DOMAIN,
        max_concurrent: int = IDOX_MAX_CONCURRENT_DOMAINS,
        user_agent: str = IDOX_USER_AGENT,
    ):
        self.per_domain_delay = per_domain_delay
        self.max_concurrent = max_concurrent
        self.user_agent = user_agent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request: dict[str, float] = {}
        self._client: httpx.AsyncClient | None = None
        self.stats = {"success": 0, "failed": 0, "no_docs": 0, "cloudflare_block": 0}

    async def __aenter__(self) -> "CivicaCxDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
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
            raise RuntimeError("Use 'async with CivicaCxDocumentScraper() as scraper:'")
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

    async def scrape_documents(self, stored_url: str) -> tuple[list[dict], str | None]:
        """Fetch & parse the documents-tab listing for one Civica CX app."""
        try:
            authority, app_id = parse_listing_url(stored_url)
        except ValueError as exc:
            logger.error("%s", exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        url = listing_url(authority, app_id)
        try:
            resp = await self._get(url)
        except httpx.TimeoutException:
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.HTTPError:
            self.stats["failed"] += 1
            return [], "network_error"

        if resp.status_code != 200:
            if is_cloudflare_block(resp.text, resp.status_code):
                self.stats["cloudflare_block"] += 1
                return [], "cloudflare_block"
            self.stats["failed"] += 1
            return [], _http_status_failure_code(resp.status_code)

        if is_cloudflare_block(resp.text, 200):  # paranoia
            self.stats["cloudflare_block"] += 1
            return [], "cloudflare_block"

        documents = parse_civica_documents(resp.text, authority, url)
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
        """Download one document by GUID."""
        domain = urlparse(doc_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                headers = {"Referer": referer} if referer else {}
                resp = await self.client.get(doc_url, headers=headers)
                if resp.status_code == 200 and resp.content:
                    if is_cloudflare_block(resp.text, 200):
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
                return 0, str(target_path)
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                return 0, str(target_path)
            finally:
                if acquired:
                    self._release(domain)
        return 0, str(target_path)


# ---------------------------------------------------------------------------
# Recon dry-run
# ---------------------------------------------------------------------------


SAMPLES: list[tuple[str, str]] = [
    ("0125/0007", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=40428&dpid=DocumentsPage"),
    ("0324/0009", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=39326&dpid=DocumentsPage"),
    ("0125/0013", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=40436&dpid=DocumentsPage"),
    ("0625/0024", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=41041&dpid=DocumentsPage"),
    ("0724/0014", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=39819&dpid=DocumentsPage"),
    ("0425/0042", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=40876&dpid=DocumentsPage"),
    ("0824/0018", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=39966&dpid=DocumentsPage"),
    ("0123/0021", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=37862&dpid=DocumentsPage"),
    ("0125/0020", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=40451&dpid=DocumentsPage"),
    ("0921/0053", "https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails?id=25368&dpid=DocumentsPage"),
]


async def dry_run(output_dir: Path = Path("_local/recon/civica_cx")) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with CivicaCxDocumentScraper() as scraper:
        for planit_id, stored_url in SAMPLES:
            safe_id = _safe_filename(planit_id.replace("/", "_"))
            app_dir = output_dir / safe_id
            app_dir.mkdir(parents=True, exist_ok=True)

            docs, failure_code = await scraper.scrape_documents(stored_url)

            downloads: list[dict] = []
            if docs:
                doc = docs[0]
                target_name = _safe_filename(
                    f"001_{doc.get('document_type') or 'document'}",
                    "001_document",
                )
                size, final_path = await scraper.download_document(
                    doc["document_url"],
                    app_dir / target_name,
                    referer=doc.get("listing_url", ""),
                )
                if size:
                    downloads.append({"path": final_path, "bytes": size})

            manifest = {
                "planit_id": planit_id,
                "source_url": stored_url,
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
        "samples_cloudflare_blocked": sum(1 for r in results if r["failure_code"] == "cloudflare_block"),
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
    summary = asyncio.run(dry_run())
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))
