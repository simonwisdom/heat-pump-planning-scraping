"""Guernsey direct-PDF planning decision scraper.

Guernsey's Planning Websearch (``planningexplorer.gov.gg``) exposes one
"Decision Notice" PDF per decided application, hosted at the static IIS site
``buildingexplorer.gov.gg`` under one of two path variants:

    /Northgate/Images/Planning%20Decisions%20PDFs/<REF-with-dashes>.pdf  (current)
    /Northgate/Images/PlanningDecisionsPDFs/<REF-with-dashes>.pdf       (legacy)

PlanIt-stored URLs sometimes use the legacy path or contain typos in the
filename. Empirically the current path also serves all pre-2018 PDFs, so the
strategy is:

    1. Try the stored ``documentation_url`` as-is.
    2. On 404, derive a canonical URL from the application uid
       (``FULL/2021/1515`` -> ``FULL-2021-1515``) and retry under the current
       path.

There is **no per-application document bundle** — the decision notice PDF is
the only artefact the portal exposes. The per-application HTML detail page
(``ApplicationSearchServlet?PKID=...``) only links to that same PDF plus a
webmap viewer, so this scraper is essentially "fetch one PDF per app".
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

logger = logging.getLogger(__name__)

GUERNSEY_PDF_HOST = "http://buildingexplorer.gov.gg"
GUERNSEY_CURRENT_PATH = "/Northgate/Images/Planning%20Decisions%20PDFs/"
GUERNSEY_LEGACY_PATH = "/Northgate/Images/PlanningDecisionsPDFs/"

MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_REF_RE = re.compile(r"^[A-Z]+/\d{4}/\d+$")


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


def uid_to_pdf_filename(uid: str) -> str | None:
    """Convert ``FULL/2021/1515`` -> ``FULL-2021-1515.pdf``.

    Returns None if the uid does not match the expected ``TYPE/YEAR/NUMBER``
    shape Guernsey uses.
    """
    if not _REF_RE.match(uid):
        return None
    return uid.replace("/", "-") + ".pdf"


def canonical_pdf_url(uid: str) -> str | None:
    """Return the current-path Decision Notice PDF URL for a Guernsey uid."""
    name = uid_to_pdf_filename(uid)
    if name is None:
        return None
    return GUERNSEY_PDF_HOST + GUERNSEY_CURRENT_PATH + quote(name, safe="")


def candidate_urls(uid: str, stored_url: str | None) -> list[str]:
    """Return ordered list of URLs to try for a single app.

    Tries the stored URL first (preserves any portal-specific casing), then
    the canonical current-path URL derived from the uid, then the legacy
    path as a last resort.
    """
    candidates: list[str] = []
    if stored_url:
        candidates.append(stored_url)

    name = uid_to_pdf_filename(uid)
    if name is not None:
        encoded_name = quote(name, safe="")
        for path in (GUERNSEY_CURRENT_PATH, GUERNSEY_LEGACY_PATH):
            url = GUERNSEY_PDF_HOST + path + encoded_name
            if url not in candidates:
                candidates.append(url)
    return candidates


class GuernseyDirectScraper:
    """Async scraper for Guernsey Decision Notice PDFs."""

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

    async def __aenter__(self) -> "GuernseyDirectScraper":
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "application/pdf,*/*;q=0.8",
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
            raise RuntimeError("Use 'async with GuernseyDirectScraper() as scraper:'")
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

    async def fetch_pdf(self, uid: str, stored_url: str | None) -> tuple[bytes, str | None, str | None]:
        """Fetch the decision PDF for a Guernsey uid.

        Returns ``(content_bytes, source_url, failure_code)``. ``failure_code``
        is None on success.
        """
        urls = candidate_urls(uid, stored_url)
        if not urls:
            self.stats["failed"] += 1
            return b"", None, "no_url"

        last_status: int | None = None
        for url in urls:
            for attempt in range(MAX_RETRIES):
                try:
                    resp = await self._get(url)
                except httpx.TimeoutException:
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    last_status = None
                    break
                except httpx.ConnectError as exc:
                    msg = str(exc).lower()
                    if "nodename nor servname" in msg or "name or service not known" in msg:
                        self.stats["failed"] += 1
                        return b"", url, "dns_error"
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    last_status = None
                    break
                except httpx.HTTPError:
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2**attempt)
                        continue
                    last_status = None
                    break

                if resp.status_code == 200:
                    content = resp.content or b""
                    if not content:
                        last_status = 200
                        break
                    if not content.startswith(b"%PDF"):
                        # Server returned 200 with HTML (rare, but treat as not-a-pdf)
                        last_status = 200
                        break
                    self.stats["success"] += 1
                    return content, url, None

                last_status = resp.status_code
                if resp.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                break  # non-retryable -> try next URL candidate

        self.stats["failed"] += 1
        if last_status is not None:
            return b"", urls[-1], _http_status_failure_code(last_status)
        return b"", urls[-1], "network_error"

    async def download_to(
        self,
        uid: str,
        stored_url: str | None,
        target_dir: Path,
    ) -> dict:
        """Fetch PDF and write to ``target_dir/<safe_uid>.pdf``.

        Returns a per-app manifest dict.
        """
        target_dir.mkdir(parents=True, exist_ok=True)
        content, source_url, failure_code = await self.fetch_pdf(uid, stored_url)
        downloads: list[dict] = []
        if content:
            safe_id = _safe_filename(uid.replace("/", "_"))
            out_path = target_dir / f"{safe_id}.pdf"
            out_path.write_bytes(content)
            downloads.append({"path": str(out_path), "bytes": len(content)})
        return {
            "uid": uid,
            "stored_url": stored_url,
            "fetched_url": source_url,
            "failure_code": failure_code,
            "downloads": downloads,
        }


# --- Recon sample driver ---------------------------------------------------

SAMPLES: list[tuple[str, str]] = [
    (
        "FULL/2021/1515",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2021-1515.pdf",
    ),
    ("FULL/2015/2479", "http://buildingexplorer.gov.gg/Northgate/Images/PlanningDecisionsPDFs/FULL-2015-2423.pdf"),
    (
        "FULL/2018/2373",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2018-2373.pdf",
    ),
    (
        "FULL/2022/1992",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2022-1992.pdf",
    ),
    (
        "FULL/2024/1325",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2024-1325.pdf",
    ),
    (
        "FULL/2018/1602",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2018-1602.pdf",
    ),
    ("FULL/2015/2483", "http://buildingexplorer.gov.gg/Northgate/Images/PlanningDecisionsPDFs/FULL-2015-2483.pdf"),
    (
        "FULL/2024/0888",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2024-0888.pdf",
    ),
    (
        "FULL/2020/1372",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2020-1372.pdf",
    ),
    (
        "FULL/2024/0015",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2024-0015.pdf",
    ),
    (
        "FULL/2019/2611",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2019-2611.pdf",
    ),
    (
        "FULL/2022/1931",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2022-1931.pdf",
    ),
    (
        "FULL/2023/0768",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2023-0768.pdf",
    ),
    (
        "FULL/2023/0425",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2023-0425.pdf",
    ),
    (
        "FULL/2018/2283",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2018-2283.pdf",
    ),
    (
        "FULL/2018/0505",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2018-0505.pdf",
    ),
    ("FULL/2017/0375", "http://buildingexplorer.gov.gg/Northgate/Images/PlanningDecisionsPDFs/FULL-2017-0375.pdf"),
    (
        "FULL/2020/1587",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2020-1587.pdf",
    ),
    (
        "FULL/2017/2066",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2017-2066.pdf",
    ),
    (
        "FULL/2020/2257",
        "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/FULL-2020-2257.pdf",
    ),
]


async def dry_run(output_dir: Path = Path("_local/recon/guernsey_direct")) -> dict:
    """Run the scraper against the recon samples and write per-app manifests."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with GuernseyDirectScraper() as scraper:
        for uid, stored_url in SAMPLES:
            safe_id = _safe_filename(uid.replace("/", "_"))
            app_dir = output_dir / safe_id
            manifest = await scraper.download_to(uid, stored_url, app_dir)
            (app_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2),
                encoding="utf-8",
            )
            results.append(manifest)

    summary = {
        "samples_total": len(SAMPLES),
        "samples_succeeded": sum(1 for r in results if r["failure_code"] is None and r["downloads"]),
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
    print(
        json.dumps(
            {k: v for k, v in summary.items() if k != "results"},
            indent=2,
        )
    )
