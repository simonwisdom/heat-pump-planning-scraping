#!/usr/bin/env python3
"""Download documents for Barnsley Planning Explorer MVC applications."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.download_runner import make_doc_process_app, run_main
from src.planningexplorer_mvc_scraper import PlanningExplorerMvcScraper

if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="planningexplorer_mvc",
            description=__doc__ or "",
            make_scraper=PlanningExplorerMvcScraper,
            process_app=make_doc_process_app(),
        )
    )
