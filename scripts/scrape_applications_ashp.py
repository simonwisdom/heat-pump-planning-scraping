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

REPO_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
BUILDWITHTRACT_MAPPING_CSV = REPO_ROOT / "data" / "buildwithtract_authority_mapping.csv"

from src.config import (
    ASHP_SEARCH_TERMS,
    SCRAPE_YEAR_END,
    SCRAPE_YEAR_START,
)
from src.db import (
    get_application_count,
    get_application_years,
    get_db,
    get_resume_start_year,
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


def resolve_years(conn, args: argparse.Namespace) -> list[int]:
    """Resolve which years to scrape for this run."""
    if args.year and args.start_year:
        raise ValueError("Use either --year or --start-year, not both")

    end_year = args.end_year or SCRAPE_YEAR_END
    if end_year < SCRAPE_YEAR_START:
        raise ValueError(f"--end-year must be >= {SCRAPE_YEAR_START}, got {end_year}")

    if args.year:
        if args.year > end_year:
            raise ValueError("--year cannot be greater than --end-year")
        return [args.year]

    if args.start_year:
        if args.start_year > end_year:
            raise ValueError("--start-year cannot be greater than --end-year")
        return list(range(args.start_year, end_year + 1))

    if args.no_resume:
        return list(range(SCRAPE_YEAR_START, end_year + 1))

    resume_year = get_resume_start_year(
        conn,
        min_year=SCRAPE_YEAR_START,
        max_year=end_year,
    )
    if resume_year is None:
        existing_years = get_application_years(conn)
        if existing_years:
            logger.info(
                "Applications already exist for every year in %s-%s. "
                "Use --start-year or --year to rerun specific years.",
                SCRAPE_YEAR_START,
                end_year,
            )
        return []

    logger.info(
        "Auto-resume selected start year %s based on years already present in the DB",
        resume_year,
    )
    return list(range(resume_year, end_year + 1))


def load_authority_portal_types() -> dict[str, str]:
    """Load the per-authority portal mapping from the buildwithtract CSV."""
    return _load_authority_portal_types(BUILDWITHTRACT_MAPPING_CSV)


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
    Uses on_page callback to persist each page incrementally,
    so partial results survive crashes.
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    logger.info(f"  Querying: search={search_term!r} year={year}")

    new_count = 0

    def _persist_page(page_records: list[dict]) -> None:
        """Upsert a page of records immediately."""
        nonlocal new_count
        if dry_run:
            return
        with transaction(conn):
            for record in page_records:
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

    records = await client.search_all_pages(
        search=search_term,
        start_date=start_date,
        end_date=end_date,
        on_page=_persist_page,
    )

    if dry_run:
        logger.info(f"  [DRY RUN] Would insert {len(records)} records for {year}")
        return len(records), 0

    return len(records), new_count


async def main():
    parser = argparse.ArgumentParser(description="Scrape ASHP applications from PlanIt")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--year", type=int, help="Scrape a single year only")
    parser.add_argument("--start-year", type=int, help="Start from this year (skip earlier years)")
    parser.add_argument("--end-year", type=int, help="End at this year")
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore DB state and scrape the full configured year range unless overridden",
    )
    parser.add_argument("--search", type=str, help="Use a specific search term")
    args = parser.parse_args()

    conn = get_db()
    portal_types = load_authority_portal_types()

    initial_count = get_application_count(conn)
    logger.info(f"Database has {initial_count} applications before scraping")

    search_terms = [args.search] if args.search else ASHP_SEARCH_TERMS
    years = resolve_years(conn, args)
    if not years:
        logger.info("No years selected for scraping; exiting.")
        conn.close()
        return

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

            try:
                for year in years:
                    processed, new = await scrape_year(client, term, year, conn, portal_types, args.dry_run)
                    term_processed += processed
                    term_new += new
                    logger.info(f"  {year}: {processed} records ({new} new)")

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
    logger.info(f"Database now has {final_count} applications")

    # Portal type breakdown
    rows = conn.execute(
        "SELECT portal_type, COUNT(*) as cnt FROM applications GROUP BY portal_type ORDER BY cnt DESC"
    ).fetchall()
    logger.info("\nPortal type breakdown:")
    for row in rows:
        logger.info(f"  {row['portal_type']}: {row['cnt']}")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
