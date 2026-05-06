"""Wiltshire UniDoc planning document scraper.

Stored Wiltshire URLs in PlanIt use the legacy UniDoc document-search route:

    http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,<doc_id>

As of 2026-04-30 this host is unreachable: TCP ports 80/443 (and 8080/8443)
all time out from both a developer workstation and the project VPS, while
DNS still resolves (``194.72.162.232``). This is consistent with the host
being firewalled to an internal/Wiltshire-only network or decommissioned.

Wiltshire's live planning register has migrated to a Salesforce Lightning
Community at ``https://development.wiltshire.gov.uk/pr/s/``. That site is a
client-rendered SPA and would require Playwright plus a query-by-reference
flow to recover the document listing for a given application; legacy
``DSA,<id>`` doc-search anchors do not appear to map onto the new system.

Because no sample URL can be fetched, this module does not implement a
working scrape. It exposes the same ``fetch / parse`` shapes as the other
portal scrapers so that callers can introspect it, but every fetch returns
``("connect_timeout", [])`` so the dry-run can record the outage without
any silent retries. See ``_local/recon/unidoc/REPORT.md`` for the recon
findings and recommended next steps.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path

import httpx

from .config import IDOX_USER_AGENT

logger = logging.getLogger(__name__)

UNIDOC_HOST = "unidoc.wiltshire.gov.uk"
UNIDOC_BASE = "https://unidoc.wiltshire.gov.uk/UniDoc/"
DOC_SEARCH_RE = re.compile(r"/UniDoc/Document/Search/DSA,(\d+)", re.IGNORECASE)
DOC_FILE_RE = re.compile(r"/UniDoc/Document/File/[^\s\"'<>]+", re.IGNORECASE)

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename(value: str, fallback: str = "document") -> str:
    safe = _SAFE_NAME_RE.sub("_", value).strip("._-")
    return safe[:140] or fallback


def extract_dsa_id(docs_url: str) -> str | None:
    """Extract the numeric DSA identifier from a stored UniDoc listing URL."""
    match = DOC_SEARCH_RE.search(docs_url or "")
    return match.group(1) if match else None


def parse_unidoc_documents(html: str, listing_url: str) -> list[dict]:
    """Best-effort parse of a UniDoc document listing page.

    The live host is unreachable so this parser was developed against
    third-party references rather than direct HTML samples. It looks for
    anchors that point at ``/UniDoc/Document/File/...`` and treats each as
    one document; if/when the host comes back the parser will need
    refinement against real markup.
    """
    documents: list[dict] = []
    seen: set[str] = set()
    for match in DOC_FILE_RE.finditer(html or ""):
        href = match.group(0)
        if href in seen:
            continue
        seen.add(href)
        documents.append(
            {
                "date_published": "",
                "document_type": "",
                "description": "",
                "drawing_number": "",
                "document_url": f"https://{UNIDOC_HOST}{href}",
                "listing_url": listing_url,
            }
        )
    return documents


class UnidocDocumentScraper:
    """Async scraper stub for Wiltshire UniDoc.

    The current production host (``unidoc.wiltshire.gov.uk``) is
    unreachable from external networks. This class keeps the shape of the
    other scrapers but every call surfaces the connectivity failure with
    ``connect_timeout``; no silent retries are performed.
    """

    def __init__(self, request_timeout: float = 20.0) -> None:
        self.request_timeout = request_timeout
        self._client: httpx.AsyncClient | None = None
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self) -> "UnidocDocumentScraper":
        self._client = httpx.AsyncClient(
            timeout=self.request_timeout,
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
            raise RuntimeError("Use 'async with UnidocDocumentScraper() as scraper:'")
        return self._client

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Attempt to scrape one UniDoc listing.

        Returns ``([], "connect_timeout")`` (or another structured failure
        code) when the host is unreachable so callers can record the
        outage without incurring retries.
        """
        dsa_id = extract_dsa_id(docs_url)
        if not dsa_id:
            self.stats["failed"] += 1
            return [], "parse_error"

        listing_url = f"https://{UNIDOC_HOST}/UniDoc/Document/Search/DSA,{dsa_id}"

        try:
            resp = await self.client.get(listing_url)
        except httpx.ConnectTimeout:
            self.stats["failed"] += 1
            return [], "connect_timeout"
        except httpx.ConnectError as exc:
            self.stats["failed"] += 1
            message = str(exc).lower()
            if "nodename nor servname" in message or "name or service not known" in message:
                return [], "dns_error"
            return [], "connect_error"
        except httpx.TimeoutException:
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.HTTPError:
            self.stats["failed"] += 1
            return [], "network_error"

        if resp.status_code != 200:
            self.stats["failed"] += 1
            return [], f"http_{resp.status_code}"

        documents = parse_unidoc_documents(resp.text, listing_url)
        if documents:
            self.stats["success"] += 1
        else:
            self.stats["no_docs"] += 1
        return documents, None


SAMPLES: list[tuple[str, str]] = [
    ("19/11387/VAR", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,906279"),
    ("21/00195/DP3", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,918587"),
    ("17/04937/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,876140"),
    ("20/05903/LBC", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,912730"),
    ("19/00366/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,895732"),
    ("20/03083/LBC", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,909987"),
    ("20/08813/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,915588"),
    ("20/09276/VAR", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,916043"),
    ("17/05446/LBC", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,876631"),
    ("17/00383/LBC", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,871693"),
    ("18/05766/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,889224"),
    ("15/11309/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,857731"),
    ("20/10534/DP3", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,917268"),
    ("20/06410/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,913229"),
    ("20/02385/LBC", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,909315"),
    ("17/09713/VAR", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,880802"),
    ("17/12518/VAR", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,883555"),
    ("18/00173/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,883771"),
    ("20/00957/FUL", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,907957"),
    ("21/00198/DP3", "http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,918590"),
]


async def dry_run(output_dir: Path = Path("_local/recon/unidoc")) -> dict:
    """Run the scraper against the recon samples and write manifests.

    With the host currently unreachable every sample is expected to
    record ``connect_timeout``; the dry-run still produces a manifest per
    sample so the failure pattern is easy to inspect.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    async with UnidocDocumentScraper() as scraper:

        async def _one(planit_id: str, docs_url: str) -> dict:
            safe_id = _safe_filename(planit_id.replace("/", "_"))
            app_dir = output_dir / safe_id
            app_dir.mkdir(parents=True, exist_ok=True)

            docs, failure_code = await scraper.scrape_documents(docs_url)

            manifest = {
                "planit_id": planit_id,
                "source_url": docs_url,
                "listing_url": (f"https://{UNIDOC_HOST}/UniDoc/Document/Search/DSA,{extract_dsa_id(docs_url)}"),
                "failure_code": failure_code,
                "documents_count": len(docs),
                "downloads": [],
                "documents": docs,
            }
            (app_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2),
                encoding="utf-8",
            )
            return manifest

        results = list(await asyncio.gather(*[_one(pid, url) for pid, url in SAMPLES]))

    summary = {
        "samples_total": len(SAMPLES),
        "samples_succeeded": sum(1 for r in results if r["failure_code"] is None and r["documents_count"] > 0),
        "failure_breakdown": _failure_breakdown(results),
        "results": results,
    }
    (output_dir / "results.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    return summary


def _failure_breakdown(results: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in results:
        key = r["failure_code"] or "ok"
        counts[key] = counts.get(key, 0) + 1
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    summary = asyncio.run(dry_run())
    print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2))
