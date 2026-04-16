#!/usr/bin/env python3
"""Expand the Idox document-metadata corpus for a representative PDF-quality frame."""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import (
    get_db,
    log_scrape_end,
    log_scrape_start,
    transaction,
    upsert_document,
)
from src.idox_scraper import IdoxDocumentScraper

DEFAULT_OUTPUT = ROOT / "_local/workstreams/01_heat_pump_applications/data/intermediate/idox_document_expansion"
RESULT_FIELDNAMES = [
    "uid",
    "authority_name",
    "start_date",
    "documentation_url",
    "documents_found",
    "documents_inserted",
    "documents_skipped_missing_url",
    "status",
    "error",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--sample-apps", type=int, default=150)
    parser.add_argument("--max-per-authority", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def load_candidates(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT a.uid, a.authority_name, a.documentation_url, a.start_date
        FROM applications a
        WHERE a.portal_type = 'idox'
          AND a.documentation_url IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM documents d
              WHERE d.application_uid = a.uid
                AND trim(COALESCE(d.document_url, '')) <> ''
          )
        ORDER BY a.authority_name, a.start_date DESC
        """
    ).fetchall()


def select_applications(
    rows: list[sqlite3.Row],
    sample_apps: int,
    max_per_authority: int,
    seed: int,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row["authority_name"], []).append(row)

    authorities = list(grouped.keys())
    rng.shuffle(authorities)
    for authority in authorities:
        rng.shuffle(grouped[authority])

    selected: list[dict[str, Any]] = []
    authority_counts: Counter[str] = Counter()

    while len(selected) < sample_apps:
        added_this_round = 0
        for authority in authorities:
            if authority_counts[authority] >= max_per_authority:
                continue
            pool = grouped[authority]
            if not pool:
                continue
            row = pool.pop()
            selected.append(
                {
                    "uid": row["uid"],
                    "authority_name": row["authority_name"],
                    "documentation_url": row["documentation_url"],
                    "start_date": row["start_date"],
                }
            )
            authority_counts[authority] += 1
            added_this_round += 1
            if len(selected) >= sample_apps:
                break
        if added_this_round == 0:
            break

    return selected


def persist_documents(
    conn: sqlite3.Connection,
    app: dict[str, Any],
    docs: list[dict[str, Any]],
) -> tuple[int, int]:
    inserted_for_app = 0
    skipped_missing_url = 0

    if not docs:
        return inserted_for_app, skipped_missing_url

    with transaction(conn):
        for doc in docs:
            if not (doc.get("document_url") or "").strip():
                skipped_missing_url += 1
                continue

            created = upsert_document(
                conn,
                {
                    "application_uid": app["uid"],
                    "documentation_url": app["documentation_url"],
                    **doc,
                },
            )
            inserted_for_app += int(created)

    return inserted_for_app, skipped_missing_url


async def scrape_application(
    scraper: IdoxDocumentScraper,
    app: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
    try:
        return app, await scraper.scrape_documents(app["documentation_url"]), None
    except Exception as exc:  # pragma: no cover - defensive wrapper around task scheduling
        return app, [], f"{type(exc).__name__}: {exc}"


async def run_scrape(args: argparse.Namespace) -> int:
    args.output_dir.mkdir(parents=True, exist_ok=True)

    conn: sqlite3.Connection | None = None
    log_id: int | None = None
    results: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    new_docs = 0
    skipped_missing_urls = 0
    task_errors = 0
    status = "completed"
    error_log: str | None = None
    scraper_stats = {
        "success": 0,
        "failed": 0,
        "no_docs": 0,
        "captcha_blocked": 0,
    }

    try:
        conn = get_db(args.db_path)
        log_id = log_scrape_start(
            conn,
            stage="document_metadata",
            source="idox",
            params={
                "sample_apps": args.sample_apps,
                "max_per_authority": args.max_per_authority,
                "seed": args.seed,
            },
        )

        selected = select_applications(
            load_candidates(conn),
            sample_apps=args.sample_apps,
            max_per_authority=args.max_per_authority,
            seed=args.seed,
        )

        if not selected:
            print("No Idox applications available for expansion.")
            return 0

        app_positions = {app["uid"]: index for index, app in enumerate(selected)}

        async with IdoxDocumentScraper() as scraper:
            tasks = [asyncio.create_task(scrape_application(scraper, app)) for app in selected]

            for task in asyncio.as_completed(tasks):
                app, docs, task_error = await task

                inserted_for_app, skipped_for_app = persist_documents(conn, app, docs)
                new_docs += inserted_for_app
                skipped_missing_urls += skipped_for_app
                task_errors += int(task_error is not None)

                results.append(
                    {
                        "uid": app["uid"],
                        "authority_name": app["authority_name"],
                        "start_date": app["start_date"],
                        "documentation_url": app["documentation_url"],
                        "documents_found": len(docs),
                        "documents_inserted": inserted_for_app,
                        "documents_skipped_missing_url": skipped_for_app,
                        "status": "task_error" if task_error else "success" if docs else "no_docs_or_failed",
                        "error": task_error or "",
                    }
                )

            scraper_stats = dict(scraper.stats)

        results.sort(key=lambda row: app_positions[row["uid"]])

        with (args.output_dir / "idox_document_expansion_results.csv").open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=RESULT_FIELDNAMES)
            writer.writeheader()
            writer.writerows(results)

        summary = {
            "applications_selected": len(selected),
            "applications_processed": len(results),
            "applications_with_docs": sum(1 for row in results if row["documents_found"] > 0),
            "new_documents_inserted": new_docs,
            "documents_skipped_missing_url": skipped_missing_urls,
            "task_errors": task_errors,
            "scraper_success": scraper_stats["success"],
            "scraper_failed": scraper_stats["failed"],
            "scraper_no_docs": scraper_stats["no_docs"],
            "scraper_captcha_blocked": scraper_stats["captcha_blocked"],
        }

        with (args.output_dir / "idox_document_expansion_summary.csv").open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(summary.keys()))
            writer.writeheader()
            writer.writerow(summary)

        for key, value in summary.items():
            print(f"{key}={value}")

        return 0
    except Exception as exc:
        status = "failed"
        error_log = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if conn is not None:
            if log_id is not None:
                log_scrape_end(
                    conn,
                    log_id,
                    records_processed=len(results),
                    records_new=new_docs,
                    records_failed=scraper_stats["failed"] + task_errors,
                    error_log=error_log,
                    status=status,
                )
            conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_scrape(parse_args())))
