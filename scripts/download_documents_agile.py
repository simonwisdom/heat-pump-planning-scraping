#!/usr/bin/env python3
"""Download documents for Agile Applications portal applications.

Agile uses a JSON API: resolve the client slug → search by reference →
list docs → fetch each by hash. Authorities listed in
``AGILE_DISABLED_AUTHORITIES`` (currently Exmoor) are skipped with a
specific failure code.
"""

from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.agile_scraper import (
    AGILE_DISABLED_AUTHORITIES,
    AgileAuthorityDisabled,
    AgileDocumentScraper,
)
from src.download_runner import (
    ProcessResult,
    run_main,
    sanitize_dirname,
)


def _client_slug_from_url(docs_url: str) -> str | None:
    """Extract the client slug from an Agile portal URL.

    Stored URLs look like ``https://<slug>.planning.agileapplications.co.uk/...``
    or ``https://planning.agileapplications.co.uk/<slug>/...``.
    """
    if not docs_url:
        return None
    parsed = urlparse(docs_url)
    host = (parsed.hostname or "").lower()
    if host.endswith(".planning.agileapplications.co.uk"):
        return host.split(".", 1)[0].upper()
    parts = [p for p in parsed.path.split("/") if p]
    if parts:
        return parts[0].upper()
    return None


async def process_app(scraper: AgileDocumentScraper, row, output_dir: Path) -> ProcessResult:
    docs_url = row["documentation_url"]
    authority = row["authority_name"] or "unknown"
    reference = row["reference"] or row["uid"]

    client_slug = _client_slug_from_url(docs_url)
    if not client_slug:
        return ProcessResult(documents=[], file_map={}, reason="no_client_slug")
    if client_slug in AGILE_DISABLED_AUTHORITIES:
        return ProcessResult(documents=[], file_map={}, reason="authority_disabled")

    try:
        application, raw_documents = await scraper.scrape_reference(reference, client_slug)
    except AgileAuthorityDisabled:
        return ProcessResult(documents=[], file_map={}, reason="authority_disabled")

    if not application:
        return ProcessResult(documents=[], file_map={}, reason="reference_not_found")
    if not raw_documents:
        return ProcessResult(documents=[], file_map={}, reason="no_documents_listed")

    target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
    file_map: dict[str, tuple[str, int]] = {}

    for idx, doc in enumerate(raw_documents, start=1):
        doc_url = doc.get("document_url")
        if not doc_url:
            continue
        # AgileDocumentScraper.download_file builds its own filename from
        # the doc metadata; pre-create the target dir and let it land there.
        downloaded = await scraper.download_file(doc, target_dir, client_slug)
        if downloaded is not None and downloaded.exists():
            rel_path = str(downloaded.relative_to(output_dir))
            file_map[doc_url] = (rel_path, downloaded.stat().st_size)

    if not file_map:
        reason = "all_downloads_failed"
    elif len(file_map) < len([d for d in raw_documents if d.get("document_url")]):
        reason = "partial"
    else:
        reason = None
    return ProcessResult(documents=raw_documents, file_map=file_map, reason=reason)


if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="agile",
            description=__doc__ or "",
            make_scraper=AgileDocumentScraper,
            process_app=process_app,
        )
    )
