#!/usr/bin/env python3
"""Download documents for NIPP-portal applications."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.download_runner import make_doc_process_app, run_main
from src.nipp_scraper import NippDocumentScraper

if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="nipp",
            description=__doc__ or "",
            make_scraper=NippDocumentScraper,
            process_app=make_doc_process_app(),
        )
    )
