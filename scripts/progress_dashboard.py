#!/usr/bin/env python3
"""Print document-download progress tables for ashp.db.

Usage:
    uv run python scripts/progress_dashboard.py
    uv run python scripts/progress_dashboard.py --db /path/to/ashp.db
    uv run python scripts/progress_dashboard.py --top-authorities 20

Buckets each application into one of:
    success            — at least one download_attempt with status='success'
    partial            — best attempt is status='partial'
    no_docs_available  — best attempt is status='no_files' (portal listed zero)
    failed             — only 'no_zip' attempts (retryable)
    not_attempted      — no row in download_attempts
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB = "/root/heat-pump-planning-scraping/_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"

BUCKETS = ["success", "partial", "no_docs_available", "failed", "not_attempted"]


def bucket_sql() -> str:
    # One bucket per application: preference order success > partial > no_docs > failed > not_attempted.
    return """
    WITH best AS (
        SELECT
            application_uid,
            CASE
                WHEN SUM(status = 'success')  > 0 THEN 'success'
                WHEN SUM(status = 'partial')  > 0 THEN 'partial'
                WHEN SUM(status = 'no_files') > 0 THEN 'no_docs_available'
                ELSE 'failed'
            END AS bucket
        FROM download_attempts
        GROUP BY application_uid
    )
    SELECT
        a.uid,
        COALESCE(a.authority_name, '(unknown)') AS authority_name,
        COALESCE(a.portal_type, '(unknown)')    AS portal_type,
        COALESCE(a.source_scrape, '(unknown)')  AS source_scrape,
        COALESCE(b.bucket, 'not_attempted')     AS bucket
    FROM applications a
    LEFT JOIN best b ON b.application_uid = a.uid
    """


def files_bytes_sql() -> str:
    # Attribute one "best" successful attempt per app, so retries don't double-count.
    return """
    WITH best_success AS (
        SELECT application_uid,
               MAX(files_downloaded)  AS files,
               MAX(bytes_downloaded)  AS bytes
        FROM download_attempts
        WHERE status IN ('success', 'partial')
        GROUP BY application_uid
    )
    SELECT COALESCE(SUM(files), 0)  AS total_files,
           COALESCE(SUM(bytes), 0)  AS total_bytes
    FROM best_success
    """


def aggregate(rows, key_fn):
    groups: dict[str, dict[str, int]] = {}
    for r in rows:
        k = key_fn(r)
        g = groups.setdefault(k, {b: 0 for b in BUCKETS} | {"total": 0})
        g[r["bucket"]] += 1
        g["total"] += 1
    return groups


def pct(num: int, denom: int) -> str:
    if denom == 0:
        return "   —"
    return f"{100 * num / denom:5.1f}%"


def human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{n:,} B"
        n /= 1024


def print_table(title: str, rows: list[dict], name_col: str, name_width: int = 28) -> None:
    print()
    print(title)
    print("-" * len(title))
    header = (
        f"{name_col:<{name_width}}  {'TOTAL':>7}  " + "  ".join(f"{b.upper():>17}" for b in BUCKETS) + f"  {'%DONE':>7}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        done = r["success"] + r["partial"]
        name = (r[name_col][: name_width - 1] + "…") if len(r[name_col]) > name_width else r[name_col]
        cells = "  ".join(f"{r[b]:>17,}" for b in BUCKETS)
        print(f"{name:<{name_width}}  {r['total']:>7,}  {cells}  {pct(done, r['total']):>7}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=DEFAULT_DB, help=f"path to ashp.db (default: {DEFAULT_DB})")
    p.add_argument(
        "--top-authorities",
        type=int,
        default=0,
        help="show only top-N authorities by remaining work (0 = all). default 0",
    )
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = list(conn.execute(bucket_sql()))
    total_files, total_bytes = conn.execute(files_bytes_sql()).fetchone()

    # --- Header ---
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"Document download progress — generated {now}")
    print(f"Database: {db_path}")
    total_apps = len(rows)
    n_success = sum(1 for r in rows if r["bucket"] == "success")
    n_partial = sum(1 for r in rows if r["bucket"] == "partial")
    n_done = n_success + n_partial
    print(
        f"Apps: {total_apps:,} total   "
        f"with docs: {n_done:,} ({pct(n_done, total_apps).strip()})   "
        f"files: {total_files:,}   "
        f"size: {human_bytes(total_bytes)}"
    )

    # --- Overall by source_scrape ---
    by_source = aggregate(rows, lambda r: r["source_scrape"])
    source_rows = sorted(
        [{"source_scrape": k, **v} for k, v in by_source.items()],
        key=lambda r: -r["total"],
    )
    # also a TOTAL row
    totals = {b: sum(r[b] for r in source_rows) for b in BUCKETS}
    totals["total"] = sum(r["total"] for r in source_rows)
    source_rows.append({"source_scrape": "ALL", **totals})
    print_table("Overall by source_scrape", source_rows, "source_scrape", name_width=20)

    # --- By portal ---
    by_portal = aggregate(rows, lambda r: r["portal_type"])
    portal_rows = sorted(
        [{"portal_type": k, **v} for k, v in by_portal.items()],
        key=lambda r: (-r["not_attempted"], -r["total"]),
    )
    print_table("By portal_type (sorted by most remaining first)", portal_rows, "portal_type", name_width=24)

    # --- By authority ---
    by_auth = aggregate(rows, lambda r: r["authority_name"])
    auth_rows = sorted(
        [{"authority_name": k, **v} for k, v in by_auth.items()],
        key=lambda r: (-r["not_attempted"], -(r["failed"]), -r["total"]),
    )
    shown = auth_rows if args.top_authorities <= 0 else auth_rows[: args.top_authorities]
    title = f"By authority ({len(shown)} of {len(auth_rows)})"
    print_table(title, shown, "authority_name", name_width=40)
    if args.top_authorities > 0 and len(auth_rows) > args.top_authorities:
        remaining = len(auth_rows) - args.top_authorities
        print(f"… {remaining} more authorities not shown (use --top-authorities 0 for all)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
