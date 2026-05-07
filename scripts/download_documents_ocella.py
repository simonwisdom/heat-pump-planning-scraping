#!/usr/bin/env python3
"""Download documents for OcellaWeb planning portal applications.

Covers councils with stored URLs of the form
``<host>/OcellaWeb/showDocuments?...&reference=<ref>&module=pl``:
Hillingdon, Breckland, South Holland, plus any newly-classified Ocella sites.

Rother also stored OcellaWeb URLs but migrated to ``online.rother.gov.uk`` in
2025; those will return a ``portal_migrated`` failure code.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.download_runner import make_url_process_app, run_main
from src.ocella_scraper import OcellaDocumentScraper

if __name__ == "__main__":
    raise SystemExit(
        run_main(
            portal="ocella",
            description=__doc__ or "",
            make_scraper=OcellaDocumentScraper,
            process_app=make_url_process_app(),
        )
    )
