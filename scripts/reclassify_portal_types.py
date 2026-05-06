#!/usr/bin/env python3
"""Reclassify ``applications.portal_type`` using the current classifier.

After a change to :mod:`src.portal_classification` (new URL signatures, new
CSV, or a different combination rule) existing rows carry a stale
``portal_type``. This script recomputes the verdict for every row using the
current implementation, prints a transition table, and — with ``--apply`` —
writes the new verdicts back to the DB.

Examples::

    uv run python scripts/reclassify_portal_types.py              # dry-run, default DB
    uv run python scripts/reclassify_portal_types.py --db PATH
    uv run python scripts/reclassify_portal_types.py --apply      # write changes
    uv run python scripts/reclassify_portal_types.py --only-changes-authority idox

``--only-changes-authority FAMILY`` filters the transition table down to rows
whose authority CSV verdict is ``FAMILY`` but whose URL signature disagrees —
the "CSV stale for this council" case.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.portal_classification import (  # noqa: E402
    _HOST_REWRITES,
    _VAGUE_VERDICTS,
    classify_authority,
    classify_portal_type,
    classify_url,
    load_authority_portal_types,
    normalise_documentation_url,
)

DEFAULT_CSV = REPO_ROOT / "data" / "buildwithtract_authority_mapping.csv"
DEFAULT_DB = REPO_ROOT / "_local" / "workstreams" / "01_heat_pump_applications" / "data" / "raw" / "ashp.db"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", type=Path, default=DEFAULT_DB, help=f"SQLite DB (default: {DEFAULT_DB})")
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV, help=f"Authority mapping CSV (default: {DEFAULT_CSV})")
    p.add_argument("--apply", action="store_true", help="Write new portal_type values back to the DB")
    p.add_argument(
        "--only-changes-authority",
        metavar="FAMILY",
        help="Restrict the transition table to rows where the authority CSV verdict equals FAMILY (e.g. 'idox')",
    )
    p.add_argument("--limit", type=int, default=0, help="Only examine the first N rows (0 = all)")
    p.add_argument(
        "--allow-demotions",
        action="store_true",
        help=(
            "Also apply changes that demote a specific label (e.g. 'mvm') to a vague one "
            "('other'/'unknown'). Default: skip those."
        ),
    )
    p.add_argument(
        "--normalise-urls",
        action="store_true",
        help=(
            "Also rewrite documentation_url onto the live host (Oracle ORDS appsportal -> "
            "appsportal2; Barnsley wwwapplications/PlanningExplorerMVC -> planningexplorer)."
        ),
    )
    return p.parse_args()


def iter_rows(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    sql = "SELECT uid, authority_name, documentation_url, portal_type FROM applications"
    if limit > 0:
        sql += f" LIMIT {int(limit)}"
    return list(conn.execute(sql))


def main() -> int:
    args = parse_args()

    if not args.db.exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        return 2
    if not args.csv.exists():
        print(f"CSV not found: {args.csv}", file=sys.stderr)
        return 2

    portal_types = load_authority_portal_types(args.csv)
    if not portal_types:
        print(f"Authority CSV loaded 0 rows: {args.csv}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    rows = iter_rows(conn, args.limit)
    print(f"Examining {len(rows):,} application rows from {args.db}")
    print(f"Authority CSV:        {args.csv} ({len(portal_types)} councils)")
    print(f"Filter (--only-changes-authority): {args.only_changes_authority or '(none)'}")
    print()

    # Per-row transition: (old_portal_type, new_portal_type, authority_verdict, url_verdict)
    transitions: Counter[tuple[str, str]] = Counter()
    authority_conflict: Counter[tuple[str, str]] = Counter()  # (authority_verdict, url_verdict) when they disagree
    changed_uids: list[tuple[str, str, str]] = []  # (uid, old, new) — populated only when apply
    demotions_skipped = 0

    # URL host rewrites (only used when --normalise-urls is set).
    url_rewrites: Counter[tuple[str, str]] = Counter()  # (old_host, new_host) -> count
    url_rewrite_uids: list[tuple[str, str]] = []  # (uid, new_url)

    for row in rows:
        old = row["portal_type"] or "(null)"
        new = classify_portal_type(row["authority_name"], row["documentation_url"], portal_types)
        transitions[(old, new)] += 1

        if args.only_changes_authority:
            auth = classify_authority(row["authority_name"], portal_types)
            url = classify_url(row["documentation_url"])
            if auth == args.only_changes_authority and url is not None and url != auth:
                authority_conflict[(auth, url)] += 1

        if args.normalise_urls and row["documentation_url"]:
            normalised = normalise_documentation_url(row["documentation_url"])
            if normalised and normalised != row["documentation_url"]:
                from urllib.parse import urlsplit

                old_host = (urlsplit(row["documentation_url"]).hostname or "").lower()
                new_host = (urlsplit(normalised).hostname or "").lower()
                url_rewrites[(old_host, new_host)] += 1
                url_rewrite_uids.append((row["uid"], normalised))

        if old == new:
            continue

        is_demotion = old not in _VAGUE_VERDICTS and old != "(null)" and new in _VAGUE_VERDICTS
        if is_demotion and not args.allow_demotions:
            demotions_skipped += 1
            continue

        changed_uids.append((row["uid"], old, new))

    # --- Transition table ---
    print(f"{'old':<28} -> {'new':<28}  {'count':>8}")
    print("-" * 72)
    # Sort: moves first (old != new), largest first; then no-ops grouped at the bottom.
    sorted_tx = sorted(transitions.items(), key=lambda kv: (kv[0][0] == kv[0][1], -kv[1]))
    unchanged = sum(c for (o, n), c in sorted_tx if o == n)
    changed = sum(c for (o, n), c in sorted_tx if o != n)
    for (old, new), count in sorted_tx:
        marker = " " if old == new else "*"
        print(f"{marker} {old:<26} -> {new:<28}  {count:>8,}")
    print("-" * 72)
    print(f"{'changed':<26}     {'':<28}  {changed:>8,}")
    print(f"{'unchanged':<26}     {'':<28}  {unchanged:>8,}")
    if demotions_skipped and not args.allow_demotions:
        print(f"{'demotions skipped':<26}     {'(use --allow-demotions)':<28}  {demotions_skipped:>8,}")

    if args.only_changes_authority and authority_conflict:
        print()
        print(f"CSV verdict '{args.only_changes_authority}' but URL signature disagrees:")
        print(f"{'authority':<12} -> {'url signature':<24}  {'count':>8}")
        print("-" * 52)
        for (auth, url), count in sorted(authority_conflict.items(), key=lambda kv: -kv[1]):
            print(f"{auth:<12} -> {url:<24}  {count:>8,}")

    if args.normalise_urls:
        print()
        if url_rewrites:
            print("URL host rewrites:")
            print(f"{'old host':<40} -> {'new host':<40}  {'count':>8}")
            print("-" * 92)
            for (old_host, new_host), count in sorted(url_rewrites.items(), key=lambda kv: -kv[1]):
                print(f"{old_host:<40} -> {new_host:<40}  {count:>8,}")
            print(f"{'total':<40}                                       {sum(url_rewrites.values()):>8,}")
        else:
            print(f"URL host rewrites: 0 (rewrite hosts: {sorted(_HOST_REWRITES)})")

    # --- Apply ---
    if args.apply:
        if not changed_uids and not url_rewrite_uids:
            print("\nNo changes to apply.")
            conn.close()
            return 0
        with conn:
            if changed_uids:
                print(f"\nApplying {len(changed_uids):,} portal_type updates…")
                conn.executemany(
                    "UPDATE applications SET portal_type = ? WHERE uid = ?",
                    [(new, uid) for uid, _old, new in changed_uids],
                )
            if url_rewrite_uids:
                print(f"Applying {len(url_rewrite_uids):,} documentation_url rewrites…")
                conn.executemany(
                    "UPDATE applications SET documentation_url = ? WHERE uid = ?",
                    [(new_url, uid) for uid, new_url in url_rewrite_uids],
                )
        print("Done.")
    else:
        print("\nDry-run: no rows written. Re-run with --apply to commit.")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
