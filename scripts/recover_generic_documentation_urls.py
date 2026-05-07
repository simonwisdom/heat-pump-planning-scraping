#!/usr/bin/env python3
"""Recover app-specific documentation URLs from known generic search routes."""

from __future__ import annotations

import argparse
import asyncio
import csv
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


def _add_repo_root_to_path() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "src").is_dir():
            sys.path.insert(0, str(parent))
            return parent
    raise RuntimeError("Could not find repository root containing 'src'")


REPO_ROOT = _add_repo_root_to_path()

from src.config import DB_PATH
from src.db import get_db, transaction
from src.generic_route_recovery import (
    RouteRecoveryResult,
    build_route_recovery_client,
    is_route_recovery_candidate,
    recover_application_route,
)

FIELDNAMES = [
    "uid",
    "authority_name",
    "original_portal_type",
    "original_url",
    "recovered_portal_type",
    "recovered_url",
    "method",
    "status",
    "note",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--db-path", type=Path, default=DB_PATH, help="SQLite DB to read; default is project DB_PATH")
    source.add_argument("--input-csv", type=Path, help="CSV export to read instead of SQLite; dry-run only")
    parser.add_argument("--authority", help="Limit to one authority")
    parser.add_argument("--limit", type=int, help="Limit candidates after filtering")
    parser.add_argument("--output-csv", type=Path, help="Write per-row recovery results")
    parser.add_argument("--apply", action="store_true", help="Write recovered URLs back to the DB")
    parser.add_argument(
        "--include-non-docs-positive",
        action="store_true",
        help="Include rows where n_documents is blank or zero",
    )
    parser.add_argument("--skip-verify", action="store_true", help="Build routes without fetching detail pages")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent route checks")
    return parser.parse_args()


def load_csv_rows(path: Path) -> list[dict[str, object]]:
    with path.open(newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def load_db_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT uid, reference, authority_name, portal_type, documentation_url, n_documents
        FROM applications
        WHERE COALESCE(portal_type, '') IN ('other', 'unknown')
        ORDER BY authority_name, start_date DESC, uid
        """
    ).fetchall()


def filter_candidates(
    rows: list[sqlite3.Row] | list[dict[str, object]],
    *,
    authority: str | None,
    include_non_docs_positive: bool,
    limit: int | None,
) -> list[sqlite3.Row] | list[dict[str, object]]:
    candidates = []
    for row in rows:
        if authority and str(row["authority_name"] or "") != authority:
            continue
        if not is_route_recovery_candidate(row, include_non_docs_positive=include_non_docs_positive):
            continue
        candidates.append(row)
        if limit and len(candidates) >= limit:
            break
    return candidates


async def recover_many(
    rows: list[sqlite3.Row] | list[dict[str, object]],
    *,
    verify: bool,
    concurrency: int,
) -> list[RouteRecoveryResult]:
    semaphore = asyncio.Semaphore(concurrency)

    async def run_one(row: sqlite3.Row | dict[str, object]) -> RouteRecoveryResult:
        async with semaphore:
            return await recover_application_route(row, client, verify=verify)

    async with build_route_recovery_client() as client:
        tasks = [run_one(row) for row in rows]
        return [await task for task in asyncio.as_completed(tasks)]


def _portal_base_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def write_results(
    path: Path, rows: list[sqlite3.Row] | list[dict[str, object]], results: list[RouteRecoveryResult]
) -> None:
    original_portal_by_uid = {str(row["uid"]): str(row["portal_type"] or "") for row in rows}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for result in sorted(results, key=lambda r: (r.authority_name, r.uid)):
            writer.writerow(
                {
                    "uid": result.uid,
                    "authority_name": result.authority_name,
                    "original_portal_type": original_portal_by_uid.get(result.uid, ""),
                    "original_url": result.original_url,
                    "recovered_portal_type": result.portal_type or "",
                    "recovered_url": result.recovered_url or "",
                    "method": result.method,
                    "status": result.status,
                    "note": result.note,
                }
            )


def apply_results(conn: sqlite3.Connection, results: list[RouteRecoveryResult]) -> int:
    batch = [
        (
            result.recovered_url,
            result.portal_type,
            _portal_base_url(result.recovered_url),
            result.uid,
            result.original_url,
        )
        for result in results
        if result.status == "recovered" and result.recovered_url and result.portal_type
    ]
    if not batch:
        return 0

    with transaction(conn):
        conn.executemany(
            """
            UPDATE applications
            SET documentation_url = ?,
                portal_type = ?,
                portal_base_url = COALESCE(?, portal_base_url)
            WHERE uid = ?
              AND COALESCE(documentation_url, '') = COALESCE(?, '')
              AND COALESCE(portal_type, '') IN ('other', 'unknown')
            """,
            batch,
        )
    return conn.total_changes


async def main() -> int:
    args = parse_args()
    if args.input_csv and args.apply:
        raise SystemExit("--apply requires --db-path; --input-csv is dry-run only")

    conn: sqlite3.Connection | None = None
    try:
        if args.input_csv:
            rows = load_csv_rows(args.input_csv)
        else:
            conn = get_db(args.db_path)
            rows = load_db_rows(conn)

        candidates = filter_candidates(
            rows,
            authority=args.authority,
            include_non_docs_positive=args.include_non_docs_positive,
            limit=args.limit,
        )
        print(f"Loaded {len(rows):,} rows; {len(candidates):,} route-recovery candidates")
        if not candidates:
            return 0

        results = await recover_many(candidates, verify=not args.skip_verify, concurrency=args.concurrency)
        status_counts = Counter(result.status for result in results)
        portal_counts = Counter(result.portal_type or "(none)" for result in results if result.status == "recovered")
        method_counts = Counter(result.method for result in results)

        print("Statuses:")
        for name, count in sorted(status_counts.items()):
            print(f"  {name}: {count:,}")
        print("Recovered portal types:")
        for name, count in sorted(portal_counts.items()):
            print(f"  {name}: {count:,}")
        print("Methods:")
        for name, count in sorted(method_counts.items()):
            print(f"  {name}: {count:,}")

        if args.output_csv:
            write_results(args.output_csv, candidates, results)
            print(f"Wrote {args.output_csv}")

        if args.apply:
            if conn is None:
                raise RuntimeError("DB connection missing")
            changed = apply_results(conn, results)
            print(f"Applied {changed:,} DB updates")
        return 0
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
