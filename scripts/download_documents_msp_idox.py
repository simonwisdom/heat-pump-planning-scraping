#!/usr/bin/env python3
"""Download documents for Civica MSP applications (Havering, Great Yarmouth).

Despite the historical ``msp_idox`` portal_type label, the backend is Civica.
The scraper enforces a per-domain rate limit (Havering's handler queues
badly under load) and forces a ``.pdf`` extension when the response is PDF.
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
from src.msp_idox_scraper import MspCivicaScraper


async def process_app(scraper: MspCivicaScraper, row, output_dir: Path) -> ProcessResult:
    docs_url = row["documentation_url"]
    authority = row["authority_name"] or "unknown"
    reference = row["reference"] or row["uid"]

    documents = await scraper.list_documents(docs_url)
    if not documents:
        return ProcessResult(documents=[], file_map={}, reason="no_documents_listed")

    target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
    file_map: dict[str, tuple[str, int]] = {}
    docs_for_db: list[dict] = []

    for idx, doc in enumerate(documents, start=1):
        doc_url = doc.get("document_url")
        if not doc_url:
            continue
        target_name = synthesize_filename(
            idx,
            doc.get("category", "") or doc.get("document_type", ""),
            doc.get("description", "") or doc.get("title", ""),
            ext=".pdf",
        )
        ok, _ct, saved = await scraper.download_document(
            docs_url,
            doc["doc_no"],
            target_dir / target_name,
        )
        docs_for_db.append(
            {
                "document_url": doc_url,
                "document_type": doc.get("category", "") or doc.get("document_type", ""),
                "description": doc.get("description", "") or doc.get("title", ""),
                "date_published": doc.get("date_published", ""),
                "drawing_number": doc.get("doc_no", ""),
            }
        )
        if ok and saved is not None and saved.exists():
            rel_path = str(saved.relative_to(output_dir))
            file_map[doc_url] = (rel_path, saved.stat().st_size)

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
            portal="msp_idox",
            description=__doc__ or "",
            make_scraper=MspCivicaScraper,
            process_app=process_app,
        )
    )
