#!/usr/bin/env python3
"""Migrate South Oxfordshire + Vale of White Horse apps onto planning-register.co.uk.

Both councils retired their ``data.{southoxon,whitehorsedc}.gov.uk`` CCM portals.
Their replacements are on the ``planning_register`` family which we already have
a working scraper for. The 367 affected apps still carry the dead URL as
``documentation_url`` and are classified as ``portal_type='other'``.

This script rebuilds ``documentation_url`` from ``planit_link`` and switches
``portal_type`` to ``planning_register`` so the existing scraper picks them up
on the next run.

Mapping:
    planit_link:  https://www.planit.org.uk/planapplic/{Council}/{REF1}/{REF2}/{REF3}/
    new URL:      https://{subdomain}.planning-register.co.uk/Planning/Display/{REF1}/{REF2}/{REF3}

Council → subdomain:
    South Oxfordshire   -> southoxfordshire
    Vale of White Horse -> valeofwhitehorse

Examples::

    uv run python scripts/migrate_south_vale_to_planning_register.py            # dry-run
    uv run python scripts/migrate_south_vale_to_planning_register.py --apply
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urlsplit

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "_local" / "workstreams" / "01_heat_pump_applications" / "data" / "raw" / "ashp.db"

SUBDOMAIN = {
    "South Oxfordshire": "southoxfordshire",
    "Vale of White Horse": "valeofwhitehorse",
}


def planit_ref(planit_link: str) -> str | None:
    """Extract the 3-segment reference from a planit planapplic URL.

    Returns ``REF1/REF2/REF3`` (e.g. ``P21/V3202/HH``) or None if the URL
    shape doesn't match.
    """
    if not planit_link:
        return None
    parts = urlsplit(planit_link)
    if parts.netloc != "www.planit.org.uk":
        return None
    segs = [s for s in parts.path.split("/") if s]
    # Expect ['planapplic', '{Council}', REF1, REF2, REF3]
    if len(segs) < 5 or segs[0] != "planapplic":
        return None
    return "/".join(segs[2:5])


def build_new_url(authority: str, planit_link: str) -> str | None:
    sub = SUBDOMAIN.get(authority)
    ref = planit_ref(planit_link)
    if not sub or not ref:
        return None
    return f"https://{sub}.planning-register.co.uk/Planning/Display/{ref}"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help=f"SQLite DB (default: {DEFAULT_DB})")
    p.add_argument("--apply", action="store_true", help="Write changes to the DB")
    args = p.parse_args()

    if not args.db.exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    rows = list(
        conn.execute(
            """
            SELECT uid, authority_name, portal_type, documentation_url, planit_link
            FROM applications
            WHERE authority_name IN ('South Oxfordshire', 'Vale of White Horse')
            """
        )
    )

    print(f"DB: {args.db}")
    print(f"Candidate rows: {len(rows)}")
    print()

    updates: list[tuple[str, str, str]] = []  # (uid, new_url, new_portal_type)
    skipped_parse = []
    skipped_already_migrated = []
    by_authority: dict[str, int] = {}

    for row in rows:
        auth = row["authority_name"]
        cur_pt = row["portal_type"]
        cur_url = row["documentation_url"] or ""

        new_url = build_new_url(auth, row["planit_link"])
        if not new_url:
            skipped_parse.append(row["uid"])
            continue

        # Skip rows already on the new URL — re-running this script is safe.
        if cur_url == new_url and cur_pt == "planning_register":
            skipped_already_migrated.append(row["uid"])
            continue

        updates.append((row["uid"], new_url, "planning_register"))
        by_authority[auth] = by_authority.get(auth, 0) + 1

    print(f"To update:      {len(updates)}")
    for a, n in sorted(by_authority.items()):
        print(f"  {a:<22} {n}")
    print(f"Already migrated: {len(skipped_already_migrated)}")
    print(f"Unparseable planit_link: {len(skipped_parse)}")
    print()

    if updates:
        print("Sample of planned updates (first 5):")
        print(f"{'authority':<22}  {'new documentation_url'}")
        sample_uids = {u for u, _, _ in updates[:5]}
        for row in rows:
            if row["uid"] in sample_uids:
                new_url = build_new_url(row["authority_name"], row["planit_link"])
                print(f"  {row['authority_name']:<22}  {new_url}")
        print()

    if args.apply:
        if not updates:
            print("Nothing to apply.")
            conn.close()
            return 0
        with conn:
            conn.executemany(
                "UPDATE applications SET documentation_url = ?, portal_type = ? WHERE uid = ?",
                [(new_url, new_pt, uid) for uid, new_url, new_pt in updates],
            )
        print(f"Applied {len(updates)} row updates.")
    else:
        print("Dry-run: no rows written. Re-run with --apply to commit.")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
