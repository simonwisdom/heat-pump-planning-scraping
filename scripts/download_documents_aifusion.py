#!/usr/bin/env python3
"""Download documents for Aifusion-portal applications (e.g. Central Bedfordshire)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.aifusion_scraper import AifusionDocumentScraper
from src.download_runner import make_url_process_app, run_main

if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="aifusion",
            description=__doc__ or "",
            make_scraper=AifusionDocumentScraper,
            process_app=make_url_process_app(),
        )
    )
