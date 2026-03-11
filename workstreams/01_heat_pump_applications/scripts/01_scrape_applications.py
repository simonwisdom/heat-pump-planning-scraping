#!/usr/bin/env python3
"""Stage 1: Scrape all ASHP planning applications from PlanIt API.

Searches for "air source heat pump" and "ASHP" across all years,
deduplicates by uid, and stores in SQLite.

Usage:
    uv run python scripts/01_scrape_applications.py
    uv run python scripts/01_scrape_applications.py --dry-run
    uv run python scripts/01_scrape_applications.py --year 2024
"""

import argparse
import asyncio
import csv
import json
import logging
import sys
from pathlib import Path


def _add_repo_root_to_path() -> None:
    """Find the repository root (the directory containing `src`) and add it to sys.path."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "src").is_dir():
            sys.path.insert(0, str(parent))
            return
    raise RuntimeError("Could not find repository root containing 'src'")


_add_repo_root_to_path()

from src.config import (
    ASHP_SEARCH_TERMS,
    DATA_DIR,
    SCRAPE_YEAR_END,
    SCRAPE_YEAR_START,
)
from src.db import (
    get_application_count,
    get_db,
    log_scrape_end,
    log_scrape_start,
    now_iso,
    transaction,
    upsert_application,
)
from src.planit_client import PlanItClient, RateLimiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_authority_portal_types() -> dict[str, str]:
    """Load portal type classification from UKPlanning scraper list."""
    csv_path = DATA_DIR / "ukplanning_scraper_list.csv"
    if not csv_path.exists():
        logger.warning(f"Authority CSV not found: {csv_path}")
        return {}

    portal_types = {}
    idox_types = {"Idox", "IdoxReq", "IdoxNI"}

    with open(csv_path) as f:
        for row in csv.DictReader(f):
            name = row.get("scraper", "").strip()
            scraper_type = row.get("scraper_type", "").strip()
            if not name:
                continue

            if scraper_type in idox_types:
                portal_types[name.lower()] = "idox"
            elif scraper_type == "PlanningExplorer":
                portal_types[name.lower()] = "northgate"
            elif scraper_type == "SwiftLG":
                portal_types[name.lower()] = "swiftlg"
            elif scraper_type == "Ocella2":
                portal_types[name.lower()] = "ocella"
            elif scraper_type == "Civica":
                portal_types[name.lower()] = "civica"
            elif scraper_type == "None":
                portal_types[name.lower()] = "none"
            else:
                portal_types[name.lower()] = "other"

    logger.info(f"Loaded portal types for {len(portal_types)} authorities")
    return portal_types


def classify_portal_type(authority_name: str, portal_types: dict[str, str]) -> str:
    """Classify an authority's portal type by fuzzy matching against known types."""
    if not authority_name:
        return "unknown"

    # Direct match
    clean = authority_name.lower().strip()
    if clean in portal_types:
        return portal_types[clean]

    # Try removing common suffixes/words
    for suffix in [" council", " borough", " district", " city"]:
        trimmed = clean.replace(suffix, "").strip()
        if trimmed in portal_types:
            return portal_types[trimmed]

    # Try camelCase conversion (UKPlanning uses CamelCase names)
    camel = clean.replace(" ", "").replace("-", "").replace("'", "")
    if camel in portal_types:
        return portal_types[camel]

    return "unknown"


async def scrape_year(
    client: PlanItClient,
    search_term: str,
    year: int,
    conn,
    portal_types: dict[str, str],
    dry_run: bool = False,
) -> tuple[int, int]:
    """Scrape all applications for a search term and year.

    Returns (total_processed, new_records).
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
        return len(records), 0

    new_count = 0
    with transaction(conn):
        for record in records:
            record["_search_term"] = search_term
            # Classify portal type
            authority = record.get("area_name", "")
            record["_portal_type"] = classify_portal_type(authority, portal_types)

            is_new = upsert_application(conn, record)
            if is_new:
                new_count += 1

            # Set portal_type (upsert doesn't handle this derived field)
            conn.execute(
                "UPDATE applications SET portal_type = ? WHERE uid = ?",
                (record["_portal_type"], record["uid"]),
            )

    return len(records), new_count


async def main():
    parser = argparse.ArgumentParser(description="Scrape ASHP applications from PlanIt")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--year", type=int, help="Scrape a single year only")
    parser.add_argument("--search", type=str, help="Use a specific search term")
    args = parser.parse_args()

    conn = get_db()
    portal_types = load_authority_portal_types()

    initial_count = get_application_count(conn)
    logger.info(f"Database has {initial_count} applications before scraping")

    search_terms = [args.search] if args.search else ASHP_SEARCH_TERMS
    years = [args.year] if args.year else list(range(SCRAPE_YEAR_START, SCRAPE_YEAR_END + 1))

    total_processed = 0
    total_new = 0

    async with PlanItClient() as client:
        for term in search_terms:
            log_id = log_scrape_start(
                conn, "stage1", "planit",
                {"search": term, "years": years},
            )

            term_processed = 0
            term_new = 0

            try:
                for year in years:
                    processed, new = await scrape_year(
                        client, term, year, conn, portal_types, args.dry_run
                    )
                    term_processed += processed
                    term_new += new
                    logger.info(
                        f"  {year}: {processed} records ({new} new)"
                    )

                log_scrape_end(
                    conn, log_id,
                    records_processed=term_processed,
                    records_new=term_new,
                )
            except Exception as e:
                log_scrape_end(
                    conn, log_id,
                    records_processed=term_processed,
                    records_new=term_new,
                    error_log=str(e),
                    status="failed",
                )
                logger.error(f"Failed on term {term!r}: {e}")
                raise

            total_processed += term_processed
            total_new += term_new
            logger.info(
                f"Term {term!r}: {term_processed} total, {term_new} new"
            )

    final_count = get_application_count(conn)
    logger.info(f"\n=== Summary ===")
    logger.info(f"Total processed: {total_processed}")
    logger.info(f"New records: {total_new}")
    logger.info(f"Database now has {final_count} applications")

    # Portal type breakdown
    rows = conn.execute(
        "SELECT portal_type, COUNT(*) as cnt FROM applications GROUP BY portal_type ORDER BY cnt DESC"
    ).fetchall()
    logger.info(f"\nPortal type breakdown:")
    for row in rows:
        logger.info(f"  {row['portal_type']}: {row['cnt']}")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
