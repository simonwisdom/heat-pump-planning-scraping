"""Jersey gov.je planning document scraper.

Jersey is a single-authority portal hosted on Microsoft SharePoint at
``https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx``.
The page accepts a planning reference via the query string:

    ?s=1&r=<REF>

where ``<REF>`` looks like ``P/2024/1456`` or ``RP/2018/0569``. Some apps use a
5-digit suffix (``P/2025/10203``).

Document discovery (httpx + BeautifulSoup)
------------------------------------------
The page is server-rendered ASP.NET WebForms. Each document is a
``<a class="documentdetailnamelink" ...>`` whose ``href`` is a
``WebForm_DoPostBackWithOptions(...)`` JS expression containing a unique
event-target ID. The link text gives the document type/title; a sibling
``<input type="hidden" ... name="...$hfType" value="DC_*">`` gives a category
code (``DC_APP``, ``DC_DECISION`` etc.).

There are no direct file URLs in the HTML — clicking a link issues a
``__doPostBack`` to the same page, and the server replies with a streamed
binary attachment.

Document download (playwright)
------------------------------
Replaying the postback over httpx fails: the server returns a SharePoint
generic error page even when ``__VIEWSTATE``, ``__EVENTVALIDATION`` and the
T&C checkbox value are forwarded. The cause is opaque (likely a session /
``FedAuth`` cookie + WebPart wiring that only the page's own JS sets up).

So downloads run through Chromium:

    1. ``page.goto(<listing url>)``
    2. Click the cookie-banner accept (if present) and tick the ``cbDocumentAgreementCondition``
       checkbox via JS (it is ``visibility:hidden`` so direct clicks fail).
    3. For each document, call ``__doPostBack(<eventTarget>, '')`` and capture
       the resulting download via ``page.expect_download()``.

This is slower than pure httpx but reliable: each click yields a real PDF
with a ``Content-Disposition`` filename.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import IDOX_USER_AGENT

logger = logging.getLogger(__name__)

JERSEY_BASE = "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx"
JERSEY_HOST = "www.gov.je"

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_REF_RE = re.compile(r"^[A-Z]{1,3}/\d{4}/\d{3,6}$")

# Limit per-app downloads in recon to keep dry-run time bounded.
RECON_DOC_LIMIT_PER_APP = 3


def _parse_html(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def _safe_filename(value: str, fallback: str = "document") -> str:
    safe = _SAFE_NAME_RE.sub("_", value).strip("._-")
    return safe[:140] or fallback


def extract_reference(docs_url: str) -> str:
    """Pull the planning reference out of a Jersey documents URL."""
    parsed = urlparse(docs_url)
    qs = parse_qs(parsed.query)
    values = qs.get("r")
    if not values:
        raise ValueError(f"Cannot extract Jersey reference from {docs_url}")
    ref = values[0].strip()
    if not _REF_RE.match(ref):
        # Permissive fallback — Jersey occasionally uses non-standard refs.
        logger.warning("Jersey reference %r does not match standard pattern", ref)
    return ref


def jersey_listing_url(reference: str) -> str:
    """Build the canonical documents URL for a Jersey planning reference."""
    return f"{JERSEY_BASE}?s=1&r={quote(reference, safe='/')}"


def parse_jersey_documents(html: str, listing_url: str) -> list[dict]:
    """Extract per-document metadata from a Jersey listing page.

    Each result has the postback ``event_target`` needed to trigger a
    download (no direct URL). ``document_type`` is the human-readable label
    from the link text; ``category_code`` is the ``DC_*`` value from the
    sibling hidden input (e.g. ``DC_APP``, ``DC_DECISION``).
    """
    soup = _parse_html(html)
    documents: list[dict] = []
    seen_targets: set[str] = set()

    for link in soup.select("a.documentdetailnamelink"):
        href = link.get("href", "")
        m = re.search(r'WebForm_PostBackOptions\("([^"]+)"', href)
        if not m:
            continue
        event_target = m.group(1)
        # Skip the "Back" button which uses the same widget but is not a doc.
        if event_target.endswith("$btnBack"):
            continue
        if event_target in seen_targets:
            continue
        seen_targets.add(event_target)

        text = link.get_text(" ", strip=True)
        # Look for the sibling hidden input that holds the category code.
        category = ""
        # Repeater item naming: ...rptDocumentGroupsItems$ctlNN$LinkButton1
        # The hfType lives at the same parent: ...rptDocumentGroupsItems$ctlNN$hfType
        if "$LinkButton1" in event_target:
            type_target = event_target.replace("$LinkButton1", "$hfType")
            hf = soup.find("input", {"name": type_target})
            if hf is not None:
                category = (hf.get("value") or "").strip()

        documents.append(
            {
                "event_target": event_target,
                "document_type": text,
                "category_code": category,
                "listing_url": listing_url,
            }
        )

    return documents


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


async def fetch_listing_html(
    docs_url: str,
    *,
    timeout: float = 60.0,
) -> tuple[str, str | None]:
    """Fetch the listing page HTML over httpx.

    Returns ``(html, failure_code)``. ``failure_code`` is None on success.
    """
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": IDOX_USER_AGENT},
            follow_redirects=True,
        ) as client:
            resp = await client.get(docs_url)
    except httpx.TimeoutException:
        return "", "timeout"
    except httpx.ConnectError as exc:
        msg = str(exc).lower()
        if "nodename nor servname" in msg or "name or service not known" in msg:
            return "", "dns_error"
        return "", "connect_error"
    except httpx.HTTPError:
        return "", "network_error"

    if resp.status_code != 200:
        return "", _http_status_failure_code(resp.status_code)
    return resp.text, None


async def scrape_documents(docs_url: str) -> tuple[list[dict], str | None]:
    """Discover the documents listed on a Jersey planning page (httpx only)."""
    try:
        reference = extract_reference(docs_url)
    except ValueError as exc:
        logger.error("%s", exc)
        return [], "parse_error"

    listing_url = jersey_listing_url(reference)
    html, failure = await fetch_listing_html(listing_url)
    if failure is not None:
        return [], failure

    docs = parse_jersey_documents(html, listing_url)
    return docs, None


# --- Playwright download driver ------------------------------------------


class JerseyPlaywrightDownloader:
    """Drives a single Chromium browser for Jersey postback downloads.

    Use as ``async with JerseyPlaywrightDownloader() as dl: ...`` so the
    browser is reused across apps.
    """

    def __init__(self, *, headless: bool = True, per_app_delay: float = 1.0) -> None:
        self.headless = headless
        self.per_app_delay = per_app_delay
        self._pw = None
        self._browser = None
        self._context = None

    async def __aenter__(self) -> "JerseyPlaywrightDownloader":
        from playwright.async_api import async_playwright

        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=self.headless)
        self._context = await self._browser.new_context(accept_downloads=True)
        return self

    async def __aexit__(self, *args) -> None:
        if self._context is not None:
            await self._context.close()
        if self._browser is not None:
            await self._browser.close()
        if self._pw is not None:
            await self._pw.stop()

    async def _new_page(self):
        if self._context is None:
            raise RuntimeError("Use 'async with JerseyPlaywrightDownloader() ...'")
        return await self._context.new_page()

    async def _prepare_page(self, page, listing_url: str) -> None:
        # Jersey/SharePoint listing pages are sometimes slow on first hit; the
        # default 30 s navigation timeout occasionally trips. 90 s gives enough
        # headroom without being absurd.
        await page.goto(listing_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(800)
        # Accept the cookiebot banner if present (best-effort).
        try:
            await page.locator("a.cb-btn.cb-accept").first.click(timeout=2000)
        except Exception:
            pass
        # Check the T&C checkbox via JS; it has visibility:hidden so a
        # plain .check() fails the clickability check.
        await page.evaluate(
            """() => {
                const cb = document.querySelector(
                    '#ctl00_PlaceHolderMain_PlanningApplicationDocuments_cbDocumentAgreementCondition'
                );
                if (cb) cb.checked = true;
            }"""
        )

    async def download_one(
        self,
        page,
        event_target: str,
        out_path: Path,
        *,
        timeout_ms: int = 30000,
    ) -> tuple[int, str | None, str | None]:
        """Trigger one postback download. Returns (bytes, suggested_name, failure)."""
        try:
            # The page's __doPostBack is defined in a script that uses
            # Function.arguments under strict mode, which Playwright's wrapper
            # arg-passing trips over. Inline the target as a JS string literal
            # so the call site is plain-old `__doPostBack("...", "")`.
            target_literal = json.dumps(event_target)
            async with page.expect_download(timeout=timeout_ms) as dl_info:
                await page.evaluate(f"__doPostBack({target_literal}, '')")
            download = await dl_info.value
            suggested = download.suggested_filename
            out_path.parent.mkdir(parents=True, exist_ok=True)
            await download.save_as(str(out_path))
            size = out_path.stat().st_size if out_path.exists() else 0
            return size, suggested, None
        except Exception as exc:
            logger.warning("Jersey download failed for %s: %s", event_target, exc)
            return 0, None, "download_error"

    async def download_app(
        self,
        uid: str,
        docs_url: str,
        target_dir: Path,
        *,
        doc_limit: int | None = None,
    ) -> dict:
        """Open one Jersey app page and download up to ``doc_limit`` documents."""
        manifest = {
            "uid": uid,
            "stored_url": docs_url,
            "listing_url": "",
            "failure_code": None,
            "documents_count": 0,
            "documents": [],
            "downloads": [],
        }

        try:
            reference = extract_reference(docs_url)
        except ValueError as exc:
            logger.error("%s", exc)
            manifest["failure_code"] = "parse_error"
            return manifest

        listing_url = jersey_listing_url(reference)
        manifest["listing_url"] = listing_url

        page = await self._new_page()
        try:
            await self._prepare_page(page, listing_url)
            html = await page.content()
            docs = parse_jersey_documents(html, listing_url)
            manifest["documents_count"] = len(docs)
            manifest["documents"] = docs

            if not docs:
                manifest["failure_code"] = "no_documents"
                return manifest

            target_dir.mkdir(parents=True, exist_ok=True)
            picked = docs if doc_limit is None else docs[:doc_limit]
            for idx, doc in enumerate(picked, start=1):
                stem = _safe_filename(
                    f"{idx:03d}_{doc.get('category_code') or 'DOC'}_{doc.get('document_type') or 'document'}",
                    f"{idx:03d}_document",
                )
                out_path = target_dir / f"{stem}.pdf"
                size, suggested, failure = await self.download_one(page, doc["event_target"], out_path)
                if size > 0:
                    manifest["downloads"].append(
                        {
                            "path": str(out_path),
                            "bytes": size,
                            "suggested_filename": suggested,
                            "event_target": doc["event_target"],
                            "category_code": doc.get("category_code"),
                            "document_type": doc.get("document_type"),
                        }
                    )
                else:
                    # Soft-fail: record but keep going.
                    logger.info("Skipping failed doc: %s", doc.get("document_type"))
                # Tiny gap to avoid hammering postback handler.
                await asyncio.sleep(0.5)
            if not manifest["downloads"]:
                manifest["failure_code"] = "all_downloads_failed"
        finally:
            await page.close()
            await asyncio.sleep(self.per_app_delay)

        return manifest


# --- Recon sample driver --------------------------------------------------

SAMPLES: list[tuple[str, str]] = [
    ("P/2024/1456", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2024/1456"),
    ("P/2023/1191", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2023/1191"),
    ("P/2023/0365", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2023/0365"),
    ("P/2024/0675", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2024/0675"),
    ("P/2021/0932", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2021/0932"),
    ("RP/2018/0569", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=RP/2018/0569"),
    ("RP/2016/0704", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=RP/2016/0704"),
    ("P/2024/1058", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2024/1058"),
    ("P/2025/0059", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2025/0059"),
    ("P/2022/1513", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2022/1513"),
    ("P/2021/1853", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2021/1853"),
    ("P/2021/1489", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2021/1489"),
    ("P/2024/0558", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2024/0558"),
    ("P/2025/0355", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2025/0355"),
    ("P/2025/10203", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2025/10203"),
    ("P/2019/1275", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2019/1275"),
    ("P/2020/1098", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2020/1098"),
    ("P/2017/0662", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2017/0662"),
    ("RP/2022/1647", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=RP/2022/1647"),
    ("P/2024/0844", "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2024/0844"),
]


async def dry_run(
    output_dir: Path = Path("_local/recon/jersey_custom"),
    *,
    doc_limit_per_app: int = RECON_DOC_LIMIT_PER_APP,
) -> dict:
    """Run the scraper against the recon samples and write manifests/downloads."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    started = time.monotonic()
    async with JerseyPlaywrightDownloader() as downloader:
        for uid, docs_url in SAMPLES:
            safe_id = _safe_filename(uid.replace("/", "_"))
            app_dir = output_dir / safe_id
            try:
                manifest = await downloader.download_app(uid, docs_url, app_dir, doc_limit=doc_limit_per_app)
            except Exception as exc:
                logger.exception("Unexpected error for %s", uid)
                manifest = {
                    "uid": uid,
                    "stored_url": docs_url,
                    "listing_url": jersey_listing_url(extract_reference(docs_url)) if uid else "",
                    "failure_code": "unexpected_error",
                    "documents_count": 0,
                    "documents": [],
                    "downloads": [],
                    "error": str(exc),
                }

            app_dir.mkdir(parents=True, exist_ok=True)
            (app_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2),
                encoding="utf-8",
            )
            results.append(manifest)
            elapsed = time.monotonic() - started
            print(
                f"[{len(results):2d}/{len(SAMPLES)}] {uid} -> "
                f"docs={manifest['documents_count']} "
                f"downloads={len(manifest['downloads'])} "
                f"failure={manifest['failure_code']} "
                f"elapsed={elapsed:.0f}s"
            )

    summary = {
        "samples_total": len(SAMPLES),
        "samples_succeeded": sum(1 for r in results if r["failure_code"] is None and r["downloads"]),
        "samples_with_listing": sum(1 for r in results if r["documents_count"] > 0),
        "documents_total": sum(r["documents_count"] for r in results),
        "downloads_total": sum(len(r["downloads"]) for r in results),
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "doc_limit_per_app": doc_limit_per_app,
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
