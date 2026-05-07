#!/usr/bin/env python3
"""Download documents for Elmbridge emaps planning applications.

Elmbridge apps land in the DB without a stored documentation_url because PlanIt
points to a generic search page. The doc-tab URL is fully derivable from the
application reference (= ``uid`` for Elmbridge), so we pre-fill it from the
reference and let the shared runner pick them up by ``portal_type``.

Run as::

    python3 scripts/download_documents_elmbridge.py --prepare-urls
    python3 scripts/download_documents_elmbridge.py --limit 5 --dry-run

The prepare step is idempotent and safe to run before every download cycle.
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
from src.elmbridge_scraper import ElmbridgeDocumentScraper, documents_url_for_reference

PORTAL = "elmbridge_emaps"


def prepare_urls(conn: sqlite3.Connection) -> int:
    """Fill documentation_url + portal_type for Elmbridge apps that lack them.

    Returns the number of rows updated.
    """
    rows = conn.execute(
        """
        SELECT uid, reference, documentation_url, portal_type
        FROM applications
        WHERE authority_name = 'Elmbridge'
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
        url = documents_url_for_reference(reference)
        conn.execute(
            "UPDATE applications SET documentation_url = ?, portal_type = ? WHERE uid = ?",
            (url, PORTAL, row["uid"]),
        )
        updated += 1
    conn.commit()
    return updated


def main() -> int:
    # Handle --prepare-urls outside the shared runner so we can short-circuit
    # before candidate selection runs.
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
        print(f"Prepared {n} Elmbridge applications (filled documentation_url + portal_type).")
        return 0

    return run_main(
        portal=PORTAL,
        description=__doc__ or "",
        make_scraper=ElmbridgeDocumentScraper,
        process_app=make_url_process_app(),
    )


if __name__ == "__main__":
    raise SystemExit(main())
