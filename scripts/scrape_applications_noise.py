#!/usr/bin/env python3
"""Scrape all noise/sound-related planning applications from PlanIt API.

Searches for noise/acoustic terms across all years, deduplicates by uid,
and stores in a separate SQLite DB for workstream 02.

Usage:
    uv run python _local/workstreams/02_sound_assessments_all_apps/scripts/01_scrape_noise_applications.py
    uv run python _local/workstreams/02_sound_assessments_all_apps/scripts/01_scrape_noise_applications.py --dry-run
"""

import argparse
import asyncio
import logging
import sqlite3
import sys
from pathlib import Path

# Repository root
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.config import SCRAPE_YEAR_END, SCRAPE_YEAR_START
from src.db import (
    get_application_count,
    get_db,
    log_scrape_end,
    log_scrape_start,
    transaction,
    upsert_application,
)
from src.planit_client import PlanItClient
from src.portal_classification import (
    classify_portal_type,
)
from src.portal_classification import (
    load_authority_portal_types as _load_authority_portal_types,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# WS2 database location
WS2_DB_PATH = REPO_ROOT / "_local" / "workstreams" / "02_sound_assessments_all_apps" / "data" / "raw" / "noise_apps.db"

# Noise/sound search terms (from probe_sound_assessments.py)
NOISE_SEARCH_TERMS = [
    '"noise assessment"',
    '"acoustic report"',
    '"noise impact assessment"',
    '"noise report"',
    '"noise survey"',
    '"acoustic assessment"',
    '"sound assessment"',
    '"acoustic survey"',
    '"BS4142"',
    '"BS 4142"',
]

BUILDWITHTRACT_MAPPING_CSV = REPO_ROOT / "data" / "buildwithtract_authority_mapping.csv"


def load_existing_uids(*db_paths: Path) -> set[str]:
    """Load all UIDs from one or more existing applications DBs for dedup."""
    uids: set[str] = set()
    for db_path in db_paths:
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            batch = {row["uid"] for row in conn.execute("SELECT uid FROM applications")}
            conn.close()
            logger.info("Loaded %s existing UIDs from %s for dedup", len(batch), db_path.name)
            uids |= batch
        except sqlite3.Error as exc:
            logger.warning("Could not load UIDs from %s: %s", db_path, exc)
    return uids


def load_authority_portal_types() -> dict[str, str]:
    """Load the per-authority portal mapping from the buildwithtract CSV."""
    return _load_authority_portal_types(BUILDWITHTRACT_MAPPING_CSV)


async def scrape_year(
    client: PlanItClient,
    search_term: str,
    year: int,
    conn,
    portal_types: dict[str, str],
    exclude_uids: set[str] | None = None,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """Scrape all applications for a search term and year.

    Returns (total_processed, new_records, skipped_dedup).
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    logger.info(f"  Querying: search={search_term!r} year={year}")

    records = await client.search_all_pages(
        search=search_term,
        start_date=start_date,
        end_date=end_date,
    )

    if dry_run:
        logger.info(f"  [DRY RUN] Would insert {len(records)} records for {year}")
        return len(records), 0, 0

    new_count = 0
    skipped = 0
    with transaction(conn):
        for record in records:
            uid = record.get("uid")
            if exclude_uids and uid in exclude_uids:
                skipped += 1
                continue

            record["_search_term"] = search_term
            authority = record.get("area_name", "")
            docs_url = (record.get("other_fields") or {}).get("docs_url")
            record["_portal_type"] = classify_portal_type(authority, docs_url, portal_types)

            is_new = upsert_application(conn, record)
            if is_new:
                new_count += 1

            conn.execute(
                "UPDATE applications SET portal_type = ? WHERE uid = ?",
                (record["_portal_type"], record["uid"]),
            )

    return len(records), new_count, skipped


async def main():
    parser = argparse.ArgumentParser(description="Scrape noise/sound applications from PlanIt")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--year", type=int, help="Scrape a single year only")
    parser.add_argument("--search", type=str, help="Use a specific search term")
    parser.add_argument(
        "--dedup-db",
        type=Path,
        nargs="*",
        help="Skip apps already in these DBs (e.g. ashp.db heat_pump_second_pass.db)",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Disable dedup (store all apps including those in other DBs)",
    )
    args = parser.parse_args()

    conn = get_db(WS2_DB_PATH)
    portal_types = load_authority_portal_types()

    # Load UIDs to exclude
    exclude_uids: set[str] = set()
    if not args.no_dedup and args.dedup_db:
        exclude_uids = load_existing_uids(*args.dedup_db)

    initial_count = get_application_count(conn)
    logger.info(f"Database has {initial_count} applications before scraping")

    search_terms = [args.search] if args.search else NOISE_SEARCH_TERMS
    years = [args.year] if args.year else list(range(SCRAPE_YEAR_START, SCRAPE_YEAR_END + 1))

    total_processed = 0
    total_new = 0

    async with PlanItClient() as client:
        for term in search_terms:
            log_id = log_scrape_start(
                conn,
                "stage1",
                "planit",
                {"search": term, "years": years},
            )

            term_processed = 0
            term_new = 0
            term_skipped = 0

            try:
                for year in years:
                    processed, new, skipped = await scrape_year(
                        client,
                        term,
                        year,
                        conn,
                        portal_types,
                        exclude_uids,
                        args.dry_run,
                    )
                    term_processed += processed
                    term_new += new
                    term_skipped += skipped
                    logger.info(f"  {year}: {processed} records ({new} new, {skipped} skipped as dupes)")

                log_scrape_end(
                    conn,
                    log_id,
                    records_processed=term_processed,
                    records_new=term_new,
                )
            except Exception as e:
                log_scrape_end(
                    conn,
                    log_id,
                    records_processed=term_processed,
                    records_new=term_new,
                    error_log=str(e),
                    status="failed",
                )
                logger.error(f"Failed on term {term!r}: {e}")
                raise

            total_processed += term_processed
            total_new += term_new
            logger.info(f"Term {term!r}: {term_processed} total, {term_new} new")

    final_count = get_application_count(conn)
    logger.info("\n=== Summary ===")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"New records: {total_new}")
    logger.info(f"Database now has {final_count} applications (deduplicated)")

    # Portal type breakdown
    rows = conn.execute(
        "SELECT portal_type, COUNT(*) as cnt FROM applications GROUP BY portal_type ORDER BY cnt DESC"
    ).fetchall()
    logger.info("\nPortal type breakdown:")
    for row in rows:
        logger.info(f"  {row['portal_type']}: {row['cnt']}")

    # Search term breakdown
    rows = conn.execute(
        "SELECT search_term, COUNT(*) as cnt FROM applications GROUP BY search_term ORDER BY cnt DESC"
    ).fetchall()
    logger.info("\nSearch term breakdown (last-write-wins):")
    for row in rows:
        logger.info(f"  {row['search_term']}: {row['cnt']}")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
