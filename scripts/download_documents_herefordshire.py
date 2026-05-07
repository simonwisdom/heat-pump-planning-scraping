#!/usr/bin/env python3
"""Download documents for Herefordshire planning applications.

Herefordshire's apps land in the DB without a stored documentation_url because
PlanIt only carries the search-page URL. This script:

1. ``--prepare-urls`` fills documentation_url with the search-page URL and
   sets ``portal_type='herefordshire'`` so the runner picks them up.
2. The scraper resolves each app via the council search API → static detail
   page → direct ``myaccount.herefordshire.gov.uk/documents?id=<uuid>`` links.

Usage::

    python3 scripts/download_documents_herefordshire.py --prepare-urls
    python3 scripts/download_documents_herefordshire.py --limit 5
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import DB_PATH
from src.db import get_db
from src.download_runner import make_url_process_app, run_main
from src.herefordshire_scraper import HerefordshireDocumentScraper

PORTAL = "herefordshire"
SEARCH_URL = "https://www.herefordshire.gov.uk/info/200142/planning_services/planning_application_search"


def prepare_urls(conn: sqlite3.Connection) -> int:
    """Fill documentation_url + portal_type for Herefordshire apps."""
    rows = conn.execute(
        """
        SELECT uid, reference, documentation_url, portal_type
        FROM applications
        WHERE authority_name = 'Herefordshire'
          AND (documentation_url IS NULL OR documentation_url = ''
               OR portal_type IS NULL OR portal_type != ?)
        """,
        (PORTAL,),
    ).fetchall()

    updated = 0
    for row in rows:
        reference = row["reference"] or row["uid"]
        if not reference:
            continue
        conn.execute(
            "UPDATE applications SET documentation_url = ?, portal_type = ? WHERE uid = ?",
            (SEARCH_URL, PORTAL, row["uid"]),
        )
        updated += 1
    conn.commit()
    return updated


def main() -> int:
    if "--prepare-urls" in sys.argv:
        sys.argv = [a for a in sys.argv if a != "--prepare-urls"]
        db_path = DB_PATH
        if "--db-path" in sys.argv:
            i = sys.argv.index("--db-path")
            db_path = Path(sys.argv[i + 1])
            del sys.argv[i : i + 2]
        conn = get_db(db_path)
        try:
            n = prepare_urls(conn)
        finally:
            conn.close()
        print(f"Prepared {n} Herefordshire applications (filled documentation_url + portal_type).")
        return 0

    return run_main(
        portal=PORTAL,
        description=__doc__ or "",
        make_scraper=HerefordshireDocumentScraper,
        # Herefordshire derives the per-app detail URL from the reference via
        # the council's search API; the stored documentation_url is only the
        # generic search page, so we route the reference into scrape_documents.
        process_app=make_url_process_app(pass_reference=True),
    )


if __name__ == "__main__":
    raise SystemExit(main())
