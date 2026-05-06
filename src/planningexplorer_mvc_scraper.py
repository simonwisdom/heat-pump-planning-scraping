"""Barnsley PlanningExplorer (ASP.NET MVC) document scraper.

Stored URLs in PlanIt point at two hosts:

    https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=<ref>
    https://wwwapplications.barnsley.gov.uk/PlanningExplorerMVC/Home/ApplicationDetails?planningApplicationNumber=<ref>

The legacy ``wwwapplications`` host returns 404 for all sample references; the
new ``planningexplorer`` host serves every reference (including pre-2018) via
the same SSR HTML. The page renders the documents accordion inline, so plain
``httpx`` is sufficient — no JS / Playwright required.

Document table structure inside ``<div id="documents">``::

    <table>
        <tr><td><b>Application Form</b></td></tr>      <-- category header
        <tr><td><a href="/Home/FileDownload/<id>?ApplicationNumber=<ref>"> filename.pdf (X MB)</a></td></tr>
        <tr><td><b>Plans</b></td></tr>                 <-- next category
        <tr><td><a ...>...</a></td></tr>
        ...

This scraper is recon-only: it implements the same ``fetch_listing → parse →
download_files → manifest`` shape as the other portal scrapers and writes a
manifest per sample under ``_local/recon/planningexplorer_mvc/<safe_uid>/``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

# Authoritative host. The legacy host (wwwapplications.barnsley.gov.uk) is dead
# (404 on every probed reference, 2026-04-30) — rewrite all stored URLs to the
# new host before fetching.
NEW_HOST = "planningexplorer.barnsley.gov.uk"
LEGACY_HOST = "wwwapplications.barnsley.gov.uk"
LEGACY_PATH_PREFIX = "/PlanningExplorerMVC"
NEW_BASE = f"https://{NEW_HOST}"

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


def _parse_html(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


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


def normalise_listing_url(docs_url: str) -> str:
    """Rewrite legacy host URLs onto the live new host.

    The stored URLs come in two shapes — the new host
    ``planningexplorer.barnsley.gov.uk/Home/ApplicationDetails`` and the
    retired ``wwwapplications.barnsley.gov.uk/PlanningExplorerMVC/Home/...``.
    Both expose the same query parameter ``planningApplicationNumber`` and the
    new host can serve every probed reference, so we always route through it.
    """
    parsed = urlparse(docs_url)
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    query = parsed.query or ""

    if netloc == LEGACY_HOST and path.startswith(LEGACY_PATH_PREFIX):
        path = path[len(LEGACY_PATH_PREFIX) :] or "/"

    # Drop any fragment (e.g. #documents) — the page renders the documents
    # panel inline regardless.
    return f"https://{NEW_HOST}{path}" + (f"?{query}" if query else "")


def extract_reference(docs_url: str) -> str | None:
    """Pull the planningApplicationNumber query value (e.g. ``2024/1003``)."""
    parsed = urlparse(docs_url)
    qs = parse_qs(parsed.query)
    raw = qs.get("planningApplicationNumber", [None])[0]
    if not raw:
        return None
    return unquote(raw)


def parse_documents(html: str, base_url: str, listing_url: str) -> list[dict]:
    """Parse the Barnsley PlanningExplorer documents accordion.

    Document type rows are ``<tr><td><b>TYPE</b></td></tr>`` and apply to all
    subsequent rows until the next type row. File rows contain a single anchor
    with ``data-original-title="View Document"`` whose link text is the
    filename and (file size).
    """
    soup = _parse_html(html)
    panel = soup.find("div", id="documents")
    if not panel:
        return []
    table = panel.find("table")
    if not table:
        return []

    documents: list[dict] = []
    current_type = ""

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if not cells:
            continue

        # Category header row: single cell with a <b>TYPE</b>
        if len(cells) == 1 and cells[0].find("b"):
            current_type = cells[0].get_text(strip=True)
            continue

        anchor = row.find("a", attrs={"data-original-title": "View Document"})
        if not anchor:
            # Some rows may use a plain anchor without the tooltip — fall back
            # to any /Home/FileDownload/ href.
            for a in row.find_all("a", href=True):
                if "/Home/FileDownload/" in a["href"]:
                    anchor = a
                    break
        if not anchor:
            continue

        href = (anchor.get("href") or "").strip()
        if not href:
            continue

        # Anchor text is "<filename> ( <size> )" — pull the filename out.
        text = anchor.get_text(" ", strip=True)
        # Filename: take everything before the first opening paren that looks
        # like a size suffix.
        m = re.match(r"^(.*?)\s*\(\s*[\d.,]+\s*[KMG]?B\s*\)\s*$", text, re.I)
        filename = (m.group(1).strip() if m else text).strip()
        size_match = re.search(r"\(\s*([\d.,]+)\s*([KMG]?B)\s*\)", text, re.I)
        size_str = f"{size_match.group(1)} {size_match.group(2).upper()}" if size_match else ""

        documents.append(
            {
                "date_published": "",  # not exposed in the listing
                "document_type": current_type,
                "description": filename,
                "drawing_number": "",
                "filename": filename,
                "size_str": size_str,
                "document_url": urljoin(base_url, href),
                "listing_url": listing_url,
            }
        )

    return documents


class PlanningExplorerMvcScraper:
    """Async scraper for Barnsley's PlanningExplorer MVC portal.

    Mirrors the ``fetch_listing → parse → download_files`` shape used by the
    other portal scrapers. ``scrape_documents`` returns the document metadata
    list and ``download_document`` writes a single document to disk.
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

    async def __aenter__(self) -> "PlanningExplorerMvcScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
            raise RuntimeError("Use 'async with PlanningExplorerMvcScraper() as scraper:'")
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

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Return ``(documents, failure_code)`` for a stored portal URL."""
        listing_url = normalise_listing_url(docs_url)
        reference = extract_reference(listing_url)
        if not reference:
            self.stats["failed"] += 1
            return [], "parse_error"

        try:
            resp = await self._get(listing_url)
        except httpx.TimeoutException:
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.ConnectError as exc:
            message = str(exc).lower()
            self.stats["failed"] += 1
            if "nodename nor servname" in message or "name or service not known" in message:
                return [], "dns_error"
            return [], "connect_error"
        except httpx.HTTPError:
            self.stats["failed"] += 1
            return [], "network_error"

        if resp.status_code != 200:
            self.stats["failed"] += 1
            return [], _http_status_failure_code(resp.status_code)

        html = resp.text
        documents = parse_documents(html, NEW_BASE, listing_url)

        if not documents:
            # Page rendered but no docs in the table — distinguish from failures.
            self.stats["no_docs"] += 1
            return [], "no_documents"

        self.stats["success"] += 1
        return documents, None

    async def download_document(
        self,
        doc: dict,
        target_path: Path,
        max_retries: int = MAX_RETRIES,
    ) -> tuple[int, str]:
        """Download a single document, returning ``(bytes_written, final_path)``."""
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
                    logger.warning("HTTP %s downloading %s", resp.status_code, url)
                    return 0, str(target_path)

                content = resp.content
                if not content:
                    return 0, str(target_path)

                ext = _detect_extension(
                    content,
                    resp.headers.get("Content-Type", ""),
                    url,
                )
                final_path = target_path.with_suffix(ext)
                final_path.parent.mkdir(parents=True, exist_ok=True)
                final_path.write_bytes(content)
                return len(content), str(final_path)

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                logger.error("Network error for %s: %s", url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Error downloading %s: %s", url, exc)
                return 0, str(target_path)

        return 0, str(target_path)


SAMPLES: list[tuple[str, str]] = [
    (
        "2024/1003",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2024%2F1003#documents",
    ),
    (
        "2025/0131",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2025%2F0131#documents",
    ),
    (
        "2021/0843",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2021%2F0843#documents",
    ),
    (
        "2020/0897",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2020%2F0897#documents",
    ),
    (
        "2026/0015",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2026%2F0015#documents",
    ),
    (
        "2022/0817",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2022%2F0817#documents",
    ),
    (
        "2023/0590",
        "https://wwwapplications.barnsley.gov.uk/PlanningExplorerMVC/Home/ApplicationDetails?planningApplicationNumber=2023%2F0590#documents",
    ),
    (
        "2023/0440",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2023%2F0440#documents",
    ),
    (
        "2017/1628",
        "https://wwwapplications.barnsley.gov.uk/PlanningExplorerMVC/Home/ApplicationDetails?planningApplicationNumber=2017%2F1628#documents",
    ),
    (
        "2023/1023",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2023%2F1023#documents",
    ),
    (
        "2024/0898",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2024%2F0898#documents",
    ),
    (
        "2024/1056",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2024%2F1056#documents",
    ),
    (
        "2024/0542",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2024%2F0542#documents",
    ),
    (
        "2025/0451",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2025%2F0451#documents",
    ),
    (
        "2021/0847",
        "https://planningexplorer.barnsley.gov.uk/Home/ApplicationDetails?planningApplicationNumber=2021%2F0847#documents",
    ),
]


async def dry_run(output_dir: Path = Path("_local/recon/planningexplorer_mvc")) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with PlanningExplorerMvcScraper() as scraper:
        for planit_id, docs_url in SAMPLES:
            safe_id = _safe_filename(planit_id.replace("/", "_"))
            app_dir = output_dir / safe_id
            app_dir.mkdir(parents=True, exist_ok=True)
            docs, failure_code = await scraper.scrape_documents(docs_url)

            downloads = []
            if docs:
                doc = docs[0]
                target_name = _safe_filename(
                    f"001_{doc.get('document_type')}_{doc.get('filename')}",
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
                "listing_url": normalise_listing_url(docs_url),
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
        "samples_no_docs": sum(1 for r in results if r["failure_code"] == "no_documents"),
        "samples_failed": sum(1 for r in results if r["failure_code"] not in (None, "no_documents")),
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
