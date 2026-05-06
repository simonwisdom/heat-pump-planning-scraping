#!/usr/bin/env python3
"""Download Decision Notice PDFs for Guernsey applications.

Guernsey publishes one decision PDF per planning case at a deterministic
URL derived from the uid. The scraper tries the stored URL first, then a
canonical-path fallback, then a legacy-path fallback.
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
)
from src.guernsey_direct_scraper import GuernseyDirectScraper


async def process_app(scraper: GuernseyDirectScraper, row, output_dir: Path) -> ProcessResult:
    uid = row["uid"]
    docs_url = row["documentation_url"]
    authority = row["authority_name"] or "unknown"
    reference = row["reference"] or uid

    content, source_url, failure_code = await scraper.fetch_pdf(uid, docs_url)
    if failure_code or not content:
        return ProcessResult(documents=[], file_map={}, reason=failure_code or "no_documents_listed")

    target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / "001_Decision_Notice.pdf"
    target_path.write_bytes(content)

    doc_url = source_url or docs_url
    rel_path = str(target_path.relative_to(output_dir))
    document = {
        "document_url": doc_url,
        "document_type": "Decision Notice",
        "description": "Decision Notice",
        "date_published": "",
        "drawing_number": "",
    }
    return ProcessResult(
        documents=[document],
        file_map={doc_url: (rel_path, len(content))},
    )


if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="guernsey_direct",
            description=__doc__ or "",
            make_scraper=GuernseyDirectScraper,
            process_app=process_app,
        )
    )
