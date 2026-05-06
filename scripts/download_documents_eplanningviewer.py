#!/usr/bin/env python3
"""Download documents for eplanningviewer (eplanningv2 JSON API) applications."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.download_runner import make_doc_process_app, run_main
from src.eplanningviewer_scraper import EplanningViewerScraper

if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="eplanningviewer",
            description=__doc__ or "",
            make_scraper=EplanningViewerScraper,
            process_app=make_doc_process_app(),
        )
    )
