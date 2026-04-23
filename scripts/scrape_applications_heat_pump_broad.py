#!/usr/bin/env python3
"""Stage 1b: Scrape broader heat-pump applications from the PlanIt API.

This is intentionally separate from the core ASHP scrape:
- it uses broader search terms that are noisier than the ASHP corpus
- it writes to a separate SQLite database by default
- it can optionally compare results against an existing ASHP database

Usage:
    uv run python scripts/scrape_applications_heat_pump_broad.py
    uv run python scripts/scrape_applications_heat_pump_broad.py --year 2025
    uv run python scripts/scrape_applications_heat_pump_broad.py --compare-db _local/.../ashp.db
"""

import argparse
import asyncio
import logging
import sqlite3
import sys
from collections import Counter
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
DEFAULT_DB_PATH = (
    REPO_ROOT / "_local" / "workstreams" / "01_heat_pump_applications" / "data" / "raw" / "heat_pump_second_pass.db"
)

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
from src.planit_source_recovery import get_portal_hint_url
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


BROAD_HEAT_PUMP_SEARCH_TERMS = [
    '"heat pump" OR "heat pumps"',
    '"ground source heat pump" OR "ground source heat pumps" OR GSHP',
    '"water source heat pump" OR "water source heat pumps" OR WSHP',
]


def load_authority_portal_types() -> dict[str, str]:
    """Load the per-authority portal mapping from the buildwithtract CSV."""
    return _load_authority_portal_types(BUILDWITHTRACT_MAPPING_CSV)


def merge_search_terms(existing_value: str | None, new_value: str) -> str:
    """Keep a compact record of all search queries that matched an application."""
    if not existing_value:
        return new_value

    seen = {part.strip() for part in existing_value.split(" | ") if part.strip()}
    if new_value in seen:
        return existing_value

    return f"{existing_value} | {new_value}"


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

    logger.info("  Querying: search=%r year=%s", search_term, year)

    records = await client.search_all_pages(
        search=search_term,
        start_date=start_date,
        end_date=end_date,
    )

    if dry_run:
        logger.info("  [DRY RUN] Would insert %s records for %s", len(records), year)
        return len(records), 0, 0

    new_count = 0
    skipped = 0
    with transaction(conn):
        for record in records:
            uid = record.get("uid")
            if exclude_uids and uid in exclude_uids:
                skipped += 1
                continue

            existing = conn.execute(
                "SELECT search_term FROM applications WHERE uid = ?",
                (uid,),
            ).fetchone()

            record["_search_term"] = merge_search_terms(
                existing["search_term"] if existing else None,
                search_term,
            )

            authority = record.get("area_name", "")
            portal_hint_url = get_portal_hint_url(record.get("other_fields"))
            record["_portal_type"] = classify_portal_type(authority, portal_hint_url, portal_types)

            is_new = upsert_application(conn, record)
            if is_new:
                new_count += 1

            conn.execute(
                "UPDATE applications SET portal_type = ? WHERE uid = ?",
                (record["_portal_type"], record["uid"]),
            )

    return len(records), new_count, skipped


def _open_read_only_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_existing_uids(db_path: Path) -> set[str]:
    """Load all UIDs from an existing applications DB for dedup."""
    if not db_path.exists():
        return set()
    try:
        conn = _open_read_only_db(db_path)
        uids = {row["uid"] for row in conn.execute("SELECT uid FROM applications")}
        conn.close()
        logger.info("Loaded %s existing UIDs from %s for dedup", len(uids), db_path)
        return uids
    except sqlite3.Error as exc:
        logger.warning("Could not load UIDs from %s: %s", db_path, exc)
        return set()


def summarise_comparison(conn, compare_db_path: Path, sample_limit: int) -> None:
    """Summarise overlap between the second-pass DB and an existing ASHP DB."""
    if not compare_db_path.exists():
        logger.warning("Comparison DB does not exist: %s", compare_db_path)
        return

    try:
        compare_conn = _open_read_only_db(compare_db_path)
    except sqlite3.Error as exc:
        logger.warning("Could not open comparison DB %s: %s", compare_db_path, exc)
        return

    try:
        compare_uids = {row["uid"] for row in compare_conn.execute("SELECT uid FROM applications")}
    except sqlite3.Error as exc:
        logger.warning(
            "Comparison DB %s does not look like an applications DB: %s",
            compare_db_path,
            exc,
        )
        compare_conn.close()
        return

    overlap_count = 0
    additional_count = 0
    additional_by_search_term: Counter[str] = Counter()
    sample_rows: list[sqlite3.Row] = []

    rows = conn.execute(
        """
        SELECT uid, reference, authority_name, description, search_term
        FROM applications
        ORDER BY start_date DESC, uid
        """
    )
    total_second_pass = 0
    for row in rows:
        total_second_pass += 1
        if row["uid"] in compare_uids:
            overlap_count += 1
            continue

        additional_count += 1
        additional_by_search_term[row["search_term"] or "(blank)"] += 1
        if sample_limit > 0 and len(sample_rows) < sample_limit:
            sample_rows.append(row)

    compare_conn.close()

    logger.info("\nComparison against %s", compare_db_path)
    logger.info("  Second-pass applications: %s", total_second_pass)
    logger.info("  Overlap with comparison DB: %s", overlap_count)
    logger.info("  Additional vs comparison DB: %s", additional_count)

    if additional_by_search_term:
        logger.info("  Additional applications by search term:")
        for term, count in additional_by_search_term.most_common():
            logger.info("    %s: %s", term, count)

    if sample_rows:
        logger.info("  Sample additional applications:")
        for row in sample_rows:
            logger.info(
                "    %s | %s | %s",
                row["reference"] or "(no ref)",
                row["authority_name"] or "(no authority)",
                row["description"] or "(no description)",
            )


async def main():
    parser = argparse.ArgumentParser(description="Scrape broader heat-pump applications from PlanIt")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--year", type=int, help="Scrape a single year only")
    parser.add_argument("--search", type=str, help="Use a specific search term")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite DB path (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--compare-db",
        type=Path,
        help="Optional existing applications DB to compare against after scraping",
    )
    parser.add_argument(
        "--dedup-db",
        type=Path,
        default=REPO_ROOT / "_local" / "workstreams" / "01_heat_pump_applications" / "data" / "raw" / "ashp.db",
        help="Skip apps already in this DB (default: ashp.db)",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Disable dedup against ashp.db",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="Number of sample additional applications to print when using --compare-db",
    )
    args = parser.parse_args()

    conn = get_db(args.db_path)
    portal_types = load_authority_portal_types()

    # Load UIDs to exclude (dedup against existing ASHP DB)
    exclude_uids: set[str] = set()
    if not args.no_dedup and args.dedup_db:
        exclude_uids = load_existing_uids(args.dedup_db)

    initial_count = get_application_count(conn)
    logger.info("Database %s has %s applications before scraping", args.db_path, initial_count)

    search_terms = [args.search] if args.search else BROAD_HEAT_PUMP_SEARCH_TERMS
    years = [args.year] if args.year else list(range(SCRAPE_YEAR_START, SCRAPE_YEAR_END + 1))

    total_processed = 0
    total_new = 0

    async with PlanItClient() as client:
        for term in search_terms:
            log_id = log_scrape_start(
                conn,
                "stage1b",
                "planit",
                {
                    "search": term,
                    "years": years,
                    "db_path": str(args.db_path),
                },
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
                    logger.info(
                        "  %s: %s records (%s new, %s skipped as dupes)",
                        year,
                        processed,
                        new,
                        skipped,
                    )

                log_scrape_end(
                    conn,
                    log_id,
                    records_processed=term_processed,
                    records_new=term_new,
                )
            except Exception as exc:
                log_scrape_end(
                    conn,
                    log_id,
                    records_processed=term_processed,
                    records_new=term_new,
                    error_log=str(exc),
                    status="failed",
                )
                logger.error("Failed on term %r: %s", term, exc)
                raise

            total_processed += term_processed
            total_new += term_new
            logger.info("Term %r: %s total, %s new", term, term_processed, term_new)

    final_count = get_application_count(conn)
    logger.info("\n=== Summary ===")
    logger.info("Total processed: %s", total_processed)
    logger.info("New records: %s", total_new)
    logger.info("Database now has %s applications", final_count)

    rows = conn.execute(
        "SELECT portal_type, COUNT(*) AS cnt FROM applications GROUP BY portal_type ORDER BY cnt DESC"
    ).fetchall()
    logger.info("\nPortal type breakdown:")
    for row in rows:
        logger.info("  %s: %s", row["portal_type"], row["cnt"])

    if args.compare_db:
        summarise_comparison(conn, args.compare_db, args.sample_limit)

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
