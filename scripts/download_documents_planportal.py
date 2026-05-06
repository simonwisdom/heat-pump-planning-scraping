#!/usr/bin/env python3
"""Download documents for Rotherham Plan Portal applications.

Plan Portal serves files inside ZIP wrappers (one PDF per zip in observed
samples). The scraper unwraps single-file zips automatically.
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
from src.planportal_scraper import PlanPortalDocumentScraper


async def process_app(scraper: PlanPortalDocumentScraper, row, output_dir: Path) -> ProcessResult:
    docs_url = row["documentation_url"]
    authority = row["authority_name"] or "unknown"
    reference = row["reference"] or row["uid"]

    documents, failure_code = await scraper.scrape_documents(docs_url)
    if failure_code:
        return ProcessResult(documents=[], file_map={}, reason=failure_code)
    if not documents:
        return ProcessResult(documents=[], file_map={}, reason="no_documents_listed")

    target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
    file_map: dict[str, tuple[str, int]] = {}

    docs_for_db: list[dict] = []
    for idx, doc in enumerate(documents, start=1):
        # Build a synthetic document_url from id_physical_doc since
        # planportal's RPC tokens rotate per session — we keep the stable
        # id_physical_doc as the canonical key.
        doc_key = f"planportal://{doc.get('id_app_ref', '')}/{doc.get('id_physical_doc', '')}"
        target_name = synthesize_filename(
            idx,
            doc.get("document_type", ""),
            doc.get("description", "") or doc.get("file_name", ""),
            ext=".pdf",
        )
        bytes_written, final_path = await scraper.download_document(
            doc,
            target_dir / target_name,
        )
        docs_for_db.append(
            {
                "document_url": doc_key,
                "document_type": doc.get("document_type", ""),
                "description": doc.get("description", ""),
                "date_published": doc.get("publish_date") or doc.get("date_created", ""),
                "drawing_number": doc.get("doc_no", ""),
            }
        )
        if bytes_written > 0:
            rel_path = str(Path(final_path).relative_to(output_dir))
            file_map[doc_key] = (rel_path, bytes_written)

    if not file_map:
        reason = "all_downloads_failed"
    elif len(file_map) < len(docs_for_db):
        reason = "partial"
    else:
        reason = None
    return ProcessResult(documents=docs_for_db, file_map=file_map, reason=reason)


if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="planportal",
            description=__doc__ or "",
            make_scraper=PlanPortalDocumentScraper,
            process_app=process_app,
        )
    )
