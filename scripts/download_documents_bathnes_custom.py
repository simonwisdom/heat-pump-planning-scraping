#!/usr/bin/env python3
"""Download documents for Bath & North East Somerset bespoke portal applications."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bathnes_custom_scraper import BathnesCustomDocumentScraper
from src.download_runner import make_url_process_app, run_main

if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="bathnes_custom",
            description=__doc__ or "",
            make_scraper=BathnesCustomDocumentScraper,
            process_app=make_url_process_app(),
        )
    )
