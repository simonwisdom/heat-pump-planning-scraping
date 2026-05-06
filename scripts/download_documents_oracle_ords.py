#!/usr/bin/env python3
"""Download documents for Neath Port Talbot Oracle ORDS / APEX applications.

The APEX Interactive Report paginates at 25 rows. The scraper records a
``truncated`` flag on apps with >25 docs; this downloader still retains the
first 25 and reports ``partial`` so the next pass can re-list.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.download_runner import (
    ProcessResult,
    run_main,
    sanitize_dirname,
    synthesize_filename,
)
from src.oracle_ords_scraper import OracleOrdsDocumentScraper, oracle_ords_listing_url


async def process_app(scraper: OracleOrdsDocumentScraper, row, output_dir: Path) -> ProcessResult:
    docs_url = row["documentation_url"]
    authority = row["authority_name"] or "unknown"
    reference = row["reference"] or row["uid"]

    documents, failure_code = await scraper.scrape_documents(docs_url)
    if failure_code:
        return ProcessResult(documents=[], file_map={}, reason=failure_code)
    if not documents:
        return ProcessResult(documents=[], file_map={}, reason="no_documents_listed")

    truncated = bool(scraper.last_pagination and scraper.last_pagination[1] > scraper.last_pagination[0])
    listing_url = oracle_ords_listing_url(docs_url)
    target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
    file_map: dict[str, tuple[str, int]] = {}

    for idx, doc in enumerate(documents, start=1):
        doc_url = doc.get("document_url")
        if not doc_url:
            continue
        target_name = synthesize_filename(
            idx,
            doc.get("document_type", ""),
            doc.get("description", ""),
            ext=Path(doc.get("ext", ".pdf")).suffix or ".pdf",
        )
        bytes_written, final_path = await scraper.download_document(
            doc_url,
            target_dir / target_name,
            referer=listing_url,
        )
        if bytes_written > 0:
            rel_path = str(Path(final_path).relative_to(output_dir))
            file_map[doc_url] = (rel_path, bytes_written)

    if not file_map:
        reason = "all_downloads_failed"
    elif truncated or len(file_map) < len(documents):
        reason = "partial"
    else:
        reason = None
    return ProcessResult(documents=documents, file_map=file_map, reason=reason)


if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="oracle_ords",
            description=__doc__ or "",
            make_scraper=OracleOrdsDocumentScraper,
            process_app=process_app,
        )
    )
