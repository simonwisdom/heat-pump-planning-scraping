#!/usr/bin/env python3
"""Print document-download progress tables for ashp.db.

Usage:
    uv run python scripts/progress_dashboard.py
    uv run python scripts/progress_dashboard.py --db /path/to/ashp.db
    uv run python scripts/progress_dashboard.py --top-authorities 20

When run off-VPS without ``--db``, the script SSHes to the VPS and executes
itself there against the canonical database.

Buckets each application into one of:
    success            — at least one download_attempt with status='success'
    partial            — best attempt is status='partial'
    no_docs_available  — best attempt is status='no_files' (portal listed zero)
    failed             — only 'no_zip' attempts (retryable)
    not_attempted      — no row in download_attempts
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_DB = "/root/heat-pump-planning-scraping/_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
VPS_HOST = "root@178.104.201.79"

BUCKETS = ["success", "partial", "no_docs_available", "failed", "not_attempted"]
NO_URL_BACKEND = "(no URL in DB)"


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def running_on_vps() -> bool:
    return os.environ.get("HOSTNAME") == "178.104.201.79" or Path("/root/heat-pump-planning-scraping").exists()


def rerun_on_vps(top_authorities: int) -> int:
    script_text = Path(__file__).read_text(encoding="utf-8")
    result = subprocess.run(
        [
            "ssh",
            VPS_HOST,
            "python3",
            "-",
            "--db",
            DEFAULT_DB,
            "--top-authorities",
            str(top_authorities),
            "--local-only",
        ],
        input=script_text,
        text=True,
    )
    return result.returncode


def bucket_sql(*, has_source_scrape: bool) -> str:
    # One bucket per application: preference order success > partial > no_docs > failed > not_attempted.
    source_scrape_sql = (
        "COALESCE(a.source_scrape, '(unknown)') AS source_scrape,"
        if has_source_scrape
        else "'(unknown)' AS source_scrape,"
    )
    return f"""
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
        {source_scrape_sql}
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


def backend_progress_sql() -> str:
    return """
    WITH classified AS (
        SELECT
            a.uid,
            a.documentation_url,
            CASE
                WHEN a.documentation_url IS NULL OR a.documentation_url = '' THEN
                    CASE
                        WHEN a.portal_type = 'publisher' THEN 'idox_publisher_no_url'
                        ELSE '__NO_URL__'
                    END
                WHEN a.portal_type = 'publisher' THEN 'idox_publisher'
                WHEN lower(a.documentation_url) LIKE '%runthirdpartysearch%' THEN 'other_bespoke'
                WHEN lower(a.documentation_url) LIKE '%/online-applications/%'
                  OR lower(a.documentation_url) LIKE '%/onlineplanning/%'
                  OR lower(a.documentation_url) LIKE '%/online/applicationdetails%'
                  OR lower(a.documentation_url) LIKE '%/idoxpa-web/%'
                  THEN 'idox_online_applications'
                WHEN lower(a.documentation_url) LIKE '%/northgate/documentexplorer/%'
                  OR lower(a.documentation_url) LIKE '%/documentexplorer/application/folderview%'
                  THEN 'northgate_docexplorer'
                WHEN lower(a.documentation_url) LIKE '%northgateim.websearch%' THEN 'northgateim_websearch'
                WHEN lower(a.documentation_url) LIKE '%aniteim%' THEN 'aniteim_websearch'
                WHEN lower(a.documentation_url) LIKE '%publicaccess_live%'
                  OR lower(a.documentation_url) LIKE '%publicaccess.websearch%'
                  OR lower(a.documentation_url) LIKE '%publicaccess/searchresult%'
                  OR lower(a.documentation_url) LIKE '%externalentrypoint.aspx%'
                  THEN 'northgate_publicaccess'
                WHEN lower(a.documentation_url) LIKE '%/planning/planning-document%' THEN 'gov_template_planning_docs'
                WHEN lower(a.documentation_url) LIKE '%swiftlg%'
                  OR lower(a.documentation_url) LIKE '%wphappdetail%'
                  THEN 'swiftlg'
                WHEN lower(a.documentation_url) LIKE '%causewaydoclist%'
                  OR lower(a.documentation_url) LIKE '%causeway%'
                  THEN 'causeway'
                WHEN lower(a.documentation_url) LIKE '%ocella%' THEN 'ocella'
                WHEN lower(a.documentation_url) LIKE '%arcus%' THEN 'arcus'
                WHEN lower(a.documentation_url) LIKE '%mvm%' THEN 'mvm'
                WHEN lower(a.documentation_url) LIKE '%necsassure%'
                  OR lower(a.documentation_url) LIKE '%necs-assure%'
                  OR lower(a.documentation_url) LIKE '%necs_assure%'
                  THEN 'necs_assure'
                WHEN lower(a.documentation_url) LIKE '%aifusion%' THEN 'aifusion'
                WHEN lower(a.documentation_url) LIKE '%eplanningviewer%' THEN 'eplanningviewer'
                WHEN lower(a.documentation_url) LIKE '%planning-register.co.uk%' THEN 'planning_register'
                WHEN lower(a.documentation_url) LIKE '%planningregister.planningsystemni.gov.uk%'
                  OR lower(a.documentation_url) LIKE '%planningsystemni.gov.uk%'
                  THEN 'nipp_planningregister'
                WHEN lower(a.documentation_url) LIKE '%shale%'
                  OR lower(a.documentation_url) LIKE '%dialog.page%'
                  THEN 'shale_dialog'
                WHEN lower(a.documentation_url) LIKE '%unidoc%' THEN 'unidoc'
                WHEN lower(a.documentation_url) LIKE '%planportal%' THEN 'planportal'
                WHEN lower(a.documentation_url) LIKE '%ords%' THEN 'oracle_ords'
                WHEN lower(a.documentation_url) LIKE '%civica%' THEN 'civica_cx'
                ELSE 'other_bespoke'
            END AS backend
        FROM applications a
    ),
    attempted AS (
        SELECT DISTINCT application_uid AS uid FROM download_attempts
    ),
    doc_ok AS (
        SELECT DISTINCT application_uid AS uid
        FROM documents
        WHERE download_status = 'downloaded'
    )
    SELECT
        CASE WHEN c.backend = '__NO_URL__' THEN '(no URL in DB)' ELSE c.backend END AS backend,
        COUNT(*) AS total,
        SUM(CASE WHEN attempted.uid IS NOT NULL THEN 1 ELSE 0 END) AS attempted,
        SUM(CASE WHEN doc_ok.uid IS NOT NULL THEN 1 ELSE 0 END) AS have_docs,
        SUM(
            CASE
                WHEN attempted.uid IS NULL
                 AND c.documentation_url IS NOT NULL
                 AND c.documentation_url != '' THEN 1
                ELSE 0
            END
        ) AS runnable_backlog,
        SUM(
            CASE
                WHEN c.documentation_url IS NULL
                  OR c.documentation_url = '' THEN 1
                ELSE 0
            END
        ) AS no_url
    FROM classified c
    LEFT JOIN attempted ON attempted.uid = c.uid
    LEFT JOIN doc_ok ON doc_ok.uid = c.uid
    GROUP BY backend
    ORDER BY total DESC, backend ASC
    """


def backend_progress_rows(conn: sqlite3.Connection) -> list[dict]:
    rows = [dict(row) for row in conn.execute(backend_progress_sql())]
    totals = {
        "backend": "Total",
        "total": sum(row["total"] for row in rows),
        "attempted": sum(row["attempted"] for row in rows),
        "have_docs": sum(row["have_docs"] for row in rows),
        "runnable_backlog": sum(row["runnable_backlog"] for row in rows),
        "no_url": sum(row["no_url"] for row in rows),
    }
    rows.append(totals)
    return rows


def print_backend_table(title: str, rows: list[dict]) -> None:
    name_width = max(28, max(len(row["backend"]) for row in rows))
    print()
    print(title)
    print("-" * len(title))
    header = (
        f"{'Backend':<{name_width}}  {'Total':>7}  {'Attempted':>9}  "
        f"{'Have docs':>9}  {'Runnable backlog':>16}  {'No URL':>7}"
    )
    print(header)
    print("-" * len(header))
    for row in rows:
        backlog = "-" if row["backend"] == NO_URL_BACKEND else f"{row['runnable_backlog']:,}"
        print(
            f"{row['backend']:<{name_width}}  "
            f"{row['total']:>7,}  "
            f"{row['attempted']:>9,}  "
            f"{row['have_docs']:>9,}  "
            f"{backlog:>16}  "
            f"{row['no_url']:>7,}"
        )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=None, help="path to ashp.db; when omitted off-VPS, run on the VPS canonical DB")
    p.add_argument(
        "--top-authorities",
        type=int,
        default=0,
        help="show only top-N authorities by remaining work (0 = all). default 0",
    )
    p.add_argument("--local-only", action="store_true", help=argparse.SUPPRESS)
    args = p.parse_args()

    if args.db is None and not args.local_only and not running_on_vps():
        return rerun_on_vps(args.top_authorities)

    db_path = Path(args.db or DEFAULT_DB)
    if not db_path.exists():
        print(f"ERROR: database not found: {db_path}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = list(conn.execute(bucket_sql(has_source_scrape=has_column(conn, "applications", "source_scrape"))))
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

    print_backend_table(
        f"All {total_apps:,} apps classified by document backend",
        backend_progress_rows(conn),
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
