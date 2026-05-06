"""Oracle ORDS / APEX planning document scraper (Neath Port Talbot).

Stored URLs in PlanIt point at the retired ``appsportal.npt.gov.uk`` host:

    http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:<ref>

That host no longer resolves. The current live host is
``appsportal2.npt.gov.uk`` and the same APEX page URL works there:

    https://appsportal2.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:<ref>

The page is a server-rendered Oracle APEX Interactive Report. No POST or
``p_session`` round-trip is needed for read access — the table HTML and the
direct document URLs are returned in the initial GET. Document URLs point at
``https://maps.npt.gov.uk/iDocsPublic/ShowDocument.aspx?id=<n>`` and serve PDFs
directly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import IDOX_MAX_CONCURRENT_DOMAINS, IDOX_RATE_LIMIT_PER_DOMAIN, IDOX_USER_AGENT

logger = logging.getLogger(__name__)

NPT_LIVE_HOST = "appsportal2.npt.gov.uk"
NPT_RETIRED_HOST = "appsportal.npt.gov.uk"
NPT_LISTING_BASE = f"https://{NPT_LIVE_HOST}/ords/idocs12/"

MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

# APEX URL: f?p=Planning:2:0::NO::P2_REFERENCE:<ref>
# The reference is URL-encoded (e.g. "P2025%2F0600").
_APEX_REF_RE = re.compile(
    r"f\?p=Planning:2:[^:]*::[^:]*::P2_REFERENCE:(?P<ref>[^&]+)",
    re.IGNORECASE,
)

# APEX Interactive Report pagination summary, e.g. ``1 - 25 of 145``. The
# default page size on NPT is 25 rows; when total exceeds page size the
# remainder requires AJAX pagination (not yet implemented — see
# ``parse_oracle_ords_pagination``).
_APEX_PAGINATION_RE = re.compile(
    r'a-IRR-pagination-label">\s*(\d+)\s*-\s*(\d+)\s*of\s*(\d+)\s*<',
    re.IGNORECASE,
)

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


def extract_reference(docs_url: str) -> str:
    """Extract a planning reference from an Oracle APEX URL.

    Handles both the live and retired NPT hosts.
    """
    match = _APEX_REF_RE.search(docs_url)
    if match:
        return unquote(match.group("ref")).strip()
    # Fall back to any `P2_REFERENCE:<value>` after a colon-separator in case
    # the URL uses an alternate APEX positional shape.
    parsed = urlparse(docs_url)
    if "P2_REFERENCE" in parsed.query:
        for part in parse_qs(parsed.query).get("p", [""])[0].split("::"):
            if part.startswith("P2_REFERENCE"):
                return unquote(part.split(":", 1)[1]).strip()
    raise ValueError(f"Cannot extract NPT planning reference from {docs_url}")


def oracle_ords_listing_url(docs_url: str) -> str:
    """Return the live Neath Port Talbot APEX listing URL for a stored URL."""
    reference = extract_reference(docs_url)
    return urljoin(
        NPT_LISTING_BASE,
        f"f?p=Planning:2:0::NO::P2_REFERENCE:{quote(reference, safe='')}",
    )


def parse_oracle_ords_pagination(html: str) -> tuple[int, int] | None:
    """Return ``(rows_fetched, rows_total)`` from the IR pagination summary.

    The Oracle APEX Interactive Report renders ``"1 - 25 of 145"`` near the
    top of the table when more rows exist than fit on one page. Returns
    ``None`` if no pagination summary is present (i.e. the report has fewer
    rows than the page size).
    """
    match = _APEX_PAGINATION_RE.search(html)
    if not match:
        return None
    first, last, total = (int(match.group(i)) for i in (1, 2, 3))
    rows_fetched = max(0, last - first + 1)
    return rows_fetched, total


def parse_oracle_ords_documents(html: str, listing_url: str) -> list[dict]:
    """Parse APEX Interactive Report rows into document records."""
    soup = _parse_html(html)
    documents: list[dict] = []
    seen_urls: set[str] = set()

    # Each row: <tr><td>View link</td><td>Group</td><td>Item</td>
    #            <td>Title</td><td>Superseded</td><td>Date</td>
    #            <td>Size Kb</td><td>Ext</td></tr>
    for table in soup.find_all("table", class_="a-IRR-table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 8:
                continue
            link = cells[0].find("a", href=True)
            if link is None:
                continue
            href = link["href"].strip()
            if "ShowDocument" not in href and "iDocsPublic" not in href:
                # Skip non-document anchors (e.g. sort/header artefacts).
                continue

            document_url = urljoin(listing_url, href)
            if document_url in seen_urls:
                continue
            seen_urls.add(document_url)

            documents.append(
                {
                    "date_published": cells[5].get_text(" ", strip=True),
                    "document_type": cells[2].get_text(" ", strip=True),
                    "description": cells[3].get_text(" ", strip=True),
                    "drawing_number": "",
                    "document_url": document_url,
                    "listing_url": listing_url,
                    "group": cells[1].get_text(" ", strip=True),
                    "size_kb": cells[6].get_text(" ", strip=True),
                    "ext": cells[7].get_text(" ", strip=True),
                }
            )

    return documents


class OracleOrdsDocumentScraper:
    """Async scraper for Neath Port Talbot Oracle ORDS / APEX listings."""

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

    async def __aenter__(self) -> "OracleOrdsDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
            verify=False,  # NPT sometimes serves stale intermediates; tolerate.
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._client is not None:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use 'async with OracleOrdsDocumentScraper() as scraper:'")
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
        """Scrape one Oracle ORDS / APEX document listing.

        On success, sets ``self.last_pagination`` to ``(rows_fetched,
        rows_total)`` if the report had more rows than fit on one page,
        otherwise ``None``. Truncated reports still return the first-page
        documents — the caller is expected to record the truncation flag so a
        future pagination loop can re-fetch.
        """
        self.last_pagination: tuple[int, int] | None = None
        try:
            listing_url = oracle_ords_listing_url(docs_url)
        except ValueError as exc:
            logger.error("%s", exc)
            self.stats["failed"] += 1
            return [], "parse_error"

        try:
            resp = await self._get(listing_url)
            if resp.status_code != 200:
                self.stats["failed"] += 1
                return [], _http_status_failure_code(resp.status_code)
            documents = parse_oracle_ords_documents(resp.text, listing_url)
            self.last_pagination = parse_oracle_ords_pagination(resp.text)
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
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "unexpected_error"

        if self.last_pagination is not None:
            fetched, total = self.last_pagination
            if total > fetched:
                logger.warning(
                    "oracle_ords listing truncated: %d of %d rows fetched (%s)",
                    fetched,
                    total,
                    listing_url,
                )
                self.stats["truncated"] = self.stats.get("truncated", 0) + 1

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
        """Download one document, returning ``(bytes_written, final_path)``."""
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
                logger.warning("HTTP %s for %s", resp.status_code, doc_url)
                return 0, str(target_path)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
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


SAMPLES: list[tuple[str, str]] = [
    ("P2025/0600", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2025%2F0600"),
    ("P2023/0696", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2023%2F0696"),
    ("P2023/0432", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2023%2F0432"),
    ("P2025/0259", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2025%2F0259"),
    ("P2024/0613", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2024%2F0613"),
    ("P2025/0405", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2025%2F0405"),
    ("P2022/0481", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2022%2F0481"),
    ("P2021/0874", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2021%2F0874"),
    ("P2024/0301", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2024%2F0301"),
    ("P2024/0264", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2024%2F0264"),
    ("P2022/0955", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2022%2F0955"),
    ("P2025/0674", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2025%2F0674"),
    ("P2025/0060", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2025%2F0060"),
    ("P2021/0120", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2021%2F0120"),
    ("P2025/0449", "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2025%2F0449"),
]


async def dry_run(output_dir: Path = Path("_local/recon/oracle_ords")) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with OracleOrdsDocumentScraper() as scraper:
        for planit_id, docs_url in SAMPLES:
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
                    doc["document_url"],
                    app_dir / target_name,
                    referer=doc.get("listing_url") or oracle_ords_listing_url(docs_url),
                )
                if size:
                    downloads.append({"path": final_path, "bytes": size})

            pagination = scraper.last_pagination
            manifest = {
                "planit_id": planit_id,
                "source_url": docs_url,
                "listing_url": oracle_ords_listing_url(docs_url),
                "failure_code": failure_code,
                "documents_count": len(docs),
                "rows_total": pagination[1] if pagination else len(docs),
                "rows_fetched": pagination[0] if pagination else len(docs),
                "truncated": bool(pagination and pagination[1] > pagination[0]),
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
        "samples_succeeded": sum(
            1 for result in results if result["failure_code"] is None and result["documents_count"] > 0
        ),
        "documents_total": sum(result["documents_count"] for result in results),
        "downloads_total": sum(len(result["downloads"]) for result in results),
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
