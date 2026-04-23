#!/usr/bin/env python3
"""Backfill missing documentation URLs from PlanIt application pages."""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import httpx


def _add_repo_root_to_path() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "src").is_dir():
            sys.path.insert(0, str(parent))
            return parent
    raise RuntimeError("Could not find repository root containing 'src'")


REPO_ROOT = _add_repo_root_to_path()
BUILDWITHTRACT_MAPPING_CSV = REPO_ROOT / "data" / "buildwithtract_authority_mapping.csv"

from src.config import DB_PATH
from src.db import get_db, log_scrape_end, log_scrape_start, transaction
from src.planit_source_recovery import build_planit_recovery_client, recover_documentation_url
from src.portal_classification import classify_portal_type, load_authority_portal_types

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--authority", type=str, help="Limit to a single authority name")
    parser.add_argument("--limit", type=int, help="Limit how many candidates to process")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report without updating the DB")
    parser.add_argument("--concurrency", type=int, default=4, help="Max concurrent PlanIt fetches")
    parser.add_argument("--batch-size", type=int, default=50, help="Rows per DB transaction")
    parser.add_argument("--min-delay", type=float, default=0.5, help="Min seconds between fetches per worker")
    parser.add_argument("--max-delay", type=float, default=1.5, help="Max seconds between fetches per worker")
    return parser.parse_args()


def load_candidates(
    conn: sqlite3.Connection,
    *,
    authority: str | None = None,
    limit: int | None = None,
) -> list[sqlite3.Row]:
    sql = """
        SELECT uid, authority_name, reference, planit_link, other_fields_json,
               documentation_url, n_documents
        FROM applications
        WHERE trim(COALESCE(documentation_url, '')) = ''
          AND COALESCE(n_documents, 0) > 0
          AND trim(COALESCE(planit_link, '')) <> ''
    """
    params: list[object] = []
    if authority:
        sql += "  AND authority_name = ?\n"
        params.append(authority)
    sql += "ORDER BY start_date DESC, authority_name, reference\n"
    if limit:
        sql += "LIMIT ?\n"
        params.append(limit)
    return conn.execute(sql, params).fetchall()


def _portal_base_url(url: str) -> str | None:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


async def _recover_one(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    row: sqlite3.Row,
    min_delay: float,
    max_delay: float,
) -> tuple[sqlite3.Row, str | None, str]:
    """Recover a single row's source URL behind a semaphore + politeness jitter."""
    async with semaphore:
        if max_delay > 0:
            await asyncio.sleep(random.uniform(min_delay, max_delay))
        try:
            url, method = await recover_documentation_url(
                client,
                planit_link=row["planit_link"],
                other_fields=row["other_fields_json"],
            )
            return row, url, method
        except httpx.HTTPError as exc:
            return row, None, f"http_error:{type(exc).__name__}"


def _flush_batch(conn: sqlite3.Connection, batch: list[tuple]) -> None:
    # Idempotent: only write if documentation_url is still blank, to avoid
    # clobbering values another scraper may have filled in concurrently.
    with transaction(conn):
        conn.executemany(
            """
            UPDATE applications
            SET documentation_url = ?,
                portal_type = ?,
                portal_base_url = COALESCE(?, portal_base_url)
            WHERE uid = ?
              AND trim(COALESCE(documentation_url, '')) = ''
            """,
            batch,
        )


async def main() -> int:
    args = parse_args()
    conn = get_db(args.db_path)
    try:
        portal_types = load_authority_portal_types(BUILDWITHTRACT_MAPPING_CSV)
        candidates = load_candidates(conn, authority=args.authority, limit=args.limit)

        if not candidates:
            logger.info("No blank-documentation_url candidates with positive n_documents.")
            return 0

        logger.info("Loaded %s candidate applications", len(candidates))

        log_id = log_scrape_start(
            conn,
            "metadata_recovery",
            "planit_see_source",
            {
                "authority": args.authority,
                "limit": args.limit,
                "dry_run": args.dry_run,
                "concurrency": args.concurrency,
            },
        )

        stats: Counter[str] = Counter()
        updated = 0
        failures: list[str] = []
        batch: list[tuple] = []
        semaphore = asyncio.Semaphore(args.concurrency)

        try:
            async with build_planit_recovery_client() as client:
                tasks = [_recover_one(semaphore, client, row, args.min_delay, args.max_delay) for row in candidates]
                for coro in asyncio.as_completed(tasks):
                    row, recovered_url, method = await coro

                    if method.startswith("http_error:"):
                        stats["http_error"] += 1
                        failures.append(f"{row['uid']}: {method}")
                        continue

                    stats[method] += 1
                    if not recovered_url:
                        continue

                    portal_type = classify_portal_type(row["authority_name"], recovered_url, portal_types)
                    portal_base_url = _portal_base_url(recovered_url)

                    if args.dry_run:
                        logger.info(
                            "[DRY RUN] %s | %s | %s | %s",
                            row["authority_name"],
                            row["reference"] or row["uid"],
                            portal_type,
                            recovered_url,
                        )
                        updated += 1
                        continue

                    batch.append((recovered_url, portal_type, portal_base_url, row["uid"]))
                    if len(batch) >= args.batch_size:
                        _flush_batch(conn, batch)
                        updated += len(batch)
                        batch.clear()

            if batch and not args.dry_run:
                _flush_batch(conn, batch)
                updated += len(batch)
                batch.clear()

            log_scrape_end(
                conn,
                log_id,
                records_processed=len(candidates),
                records_new=updated,
                records_failed=len(failures),
            )
        except Exception as exc:
            log_scrape_end(
                conn,
                log_id,
                records_processed=updated,
                records_new=updated,
                records_failed=len(failures) + 1,
                error_log=f"{type(exc).__name__}: {exc}",
                status="failed",
            )
            raise

        logger.info("Recovered %s documentation URLs", updated)
        for name, count in sorted(stats.items()):
            logger.info("  %s=%s", name, count)
        if failures:
            logger.info("Failures: %s (showing first 10)", len(failures))
            for failure in failures[:10]:
                logger.info("  %s", failure)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
