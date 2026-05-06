#!/usr/bin/env python3
"""Download documents for Arun Eocella / OcellaWeb applications."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.download_runner import make_url_process_app, run_main
from src.eocella_scraper import EocellaDocumentScraper

if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="eocella",
            description=__doc__ or "",
            make_scraper=EocellaDocumentScraper,
            process_app=make_url_process_app(),
        )
    )
