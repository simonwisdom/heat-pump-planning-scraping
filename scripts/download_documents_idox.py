#!/usr/bin/env python3
"""Download all documents for Idox applications as zip archives.

Prerequisites: run scrape_document_listings.py first to populate document
listings in the DB.  This script reads applications whose documents have
download_status='pending', fetches the zip from the portal (1 GET + 1 POST
per app), unpacks, and records file paths back to the DB.

Optional rclone sync via environment variables:
    SYNC_REMOTE  rclone remote path (e.g. "myremote:path/to/docs/")
    SYNC_EVERY   sync every N apps (default: 50)
    SYNC_CLEAR   delete local files after sync ("1" to enable)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import re
import socket
import sqlite3
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import logging

from src.config import DB_PATH, PDF_DIR
from src.db import (
    get_applications_needing_download,
    get_db,
    log_scrape_end,
    log_scrape_start,
    mark_documents_downloaded,
    record_download_attempt,
    transaction,
    upsert_document,
)
from src.idox_scraper import IdoxDocumentScraper

logger = logging.getLogger(__name__)


def classify_failure(error_str: str) -> str:
    """Map an error message to a short failure code."""
    e = error_str.lower()
    if "name or service not known" in e or "nodename nor servname" in e:
        return "dns_error"
    if "403" in e:
        return "http_403"
    if "500" in e:
        return "http_500"
    if "server disconnected" in e or "connection reset" in e:
        return "connection_reset"
    if "captcha" in e or "block" in e or "access denied" in e:
        return "blocked"
    if "timeout" in e or "timed out" in e:
        return "timeout"
    if "certificate" in e or "ssl" in e or "tls" in e:
        return "tls_error"
    return "unknown"


RESULT_FIELDNAMES = [
    "uid",
    "authority_name",
    "reference",
    "documentation_url",
    "documents_listed",
    "files_downloaded",
    "total_bytes",
    "status",
    "error",
    "timestamp",
    "elapsed_s",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None, help="Max applications to process")
    parser.add_argument("--authority", type=str, default=None, help="Filter to single authority name")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PDF_DIR,
        help="Root directory for downloaded PDFs",
    )
    parser.add_argument("--dry-run", action="store_true", help="List candidates without downloading")
    return parser.parse_args()


def interleave_by_authority(rows: list) -> list:
    """Round-robin across authorities so we don't hammer one portal."""
    from collections import defaultdict

    by_authority: dict[str, list] = defaultdict(list)
    for row in rows:
        by_authority[row["authority_name"] or "unknown"].append(row)

    result = []
    while by_authority:
        empty = []
        for authority in sorted(by_authority):
            result.append(by_authority[authority].pop(0))
            if not by_authority[authority]:
                empty.append(authority)
        for a in empty:
            del by_authority[a]
    return result


def sanitize_dirname(name: str) -> str:
    """Make a string safe for use as a directory name."""
    safe = re.sub(r'[<>:"/\\|?*]', "_", name)
    safe = safe.strip(". ")
    return safe or "_unnamed"


def rclone_sync(local_dir: Path, remote_path: str, clear_local: bool = False) -> bool:
    """Sync local_dir to an rclone remote path. Returns True on success."""
    import shutil
    import subprocess

    if not local_dir.exists() or not any(local_dir.iterdir()):
        return True

    cmd = [
        "rclone",
        "copy",
        str(local_dir),
        remote_path,
        "--transfers",
        "8",
        "--progress",
    ]
    print(f"\n  Syncing to {remote_path} ...")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"  WARNING: rclone sync failed (exit code {result.returncode})")
        return False

    if clear_local:
        # Remove all authority subdirectories but keep the output dir itself
        # and any CSVs/logs at the root level
        for child in local_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
        print(f"  Cleared local staging ({local_dir})")

    return True


def unpack_and_match(
    zip_bytes_list: list[bytes],
    documents: list[dict],
    target_dir: Path,
    output_dir: Path,
) -> dict[str, tuple[str, int]]:
    """Unpack zip(s) and match extracted files to document metadata.

    Returns: {document_url: (relative_file_path, file_size_bytes)}
    """
    target_dir.mkdir(parents=True, exist_ok=True)

    # Build lookup: zip filename stem → document_url
    # The checkbox value is like "HASH/FILENAME.pdf" and the zip member
    # is just "FILENAME.pdf" (the part after the slash).
    stem_to_doc: dict[str, dict] = {}
    for doc in documents:
        cbv = doc.get("file_checkbox_value")
        if cbv and doc.get("document_url"):
            zip_name = cbv.split("/", 1)[1] if "/" in cbv else cbv
            stem_to_doc[zip_name] = doc

    file_map: dict[str, tuple[str, int]] = {}

    for zip_bytes in zip_bytes_list:
        try:
            zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        except zipfile.BadZipFile:
            continue

        for member in zf.infolist():
            if member.is_dir():
                continue

            out_path = target_dir / member.filename
            # Handle filename collisions
            if out_path.exists():
                stem = out_path.stem
                suffix = out_path.suffix
                counter = 1
                while out_path.exists():
                    out_path = target_dir / f"{stem}_{counter}{suffix}"
                    counter += 1

            zf.extract(member, target_dir)
            extracted_path = target_dir / member.filename
            file_size = extracted_path.stat().st_size

            # Match back to document metadata
            doc = stem_to_doc.get(member.filename)
            if doc and doc["document_url"]:
                rel_path = str(extracted_path.relative_to(output_dir))
                file_map[doc["document_url"]] = (rel_path, file_size)

    return file_map


def persist_new_documents(
    conn: sqlite3.Connection,
    app: dict,
    documents: list[dict],
) -> int:
    """Insert document metadata rows that don't exist yet (for combined
    list+download mode). Returns count of newly inserted rows."""
    inserted = 0
    with transaction(conn):
        for doc in documents:
            if not (doc.get("document_url") or "").strip():
                continue
            created = upsert_document(
                conn,
                {
                    "application_uid": app["uid"],
                    "documentation_url": app["documentation_url"],
                    "document_type": doc.get("document_type", ""),
                    "description": doc.get("description", ""),
                    "document_url": doc["document_url"],
                    "date_published": doc.get("date_published", ""),
                    "drawing_number": doc.get("drawing_number", ""),
                },
            )
            inserted += int(created)
    return inserted


def get_cumulative_stats(conn: sqlite3.Connection) -> dict:
    """Query the DB for overall download progress across all runs."""
    doc_row = conn.execute(
        "SELECT"
        "  COUNT(*) AS total_docs,"
        "  SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) AS downloaded,"
        "  SUM(CASE WHEN download_status = 'pending' THEN 1 ELSE 0 END) AS pending,"
        "  SUM(CASE WHEN file_size_bytes IS NOT NULL THEN file_size_bytes ELSE 0 END) AS total_bytes"
        " FROM documents"
    ).fetchone()
    app_row = conn.execute(
        "SELECT"
        "  COUNT(DISTINCT d.application_uid) AS apps_with_downloads"
        " FROM documents d"
        " WHERE d.download_status = 'downloaded'"
    ).fetchone()
    total_apps = conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
    return {
        "total_applications": total_apps,
        "applications_downloaded": app_row["apps_with_downloads"],
        "total_docs": doc_row["total_docs"],
        "docs_downloaded": doc_row["downloaded"],
        "docs_pending": doc_row["pending"],
        "total_bytes": doc_row["total_bytes"],
    }


def write_progress(
    output_dir: Path,
    conn: sqlite3.Connection,
    *,
    started_at: str,
    processed: int,
    total: int,
    success: int,
    failed: int,
    files_downloaded: int,
    bytes_downloaded: int,
    elapsed: float,
    last_app: str,
    last_status: str,
) -> None:
    """Write a progress.json file for external monitoring."""
    rate = processed / elapsed if elapsed > 0 else 0
    eta = (total - processed) / rate if rate > 0 else 0
    progress = {
        "current_run": {
            "started_at": started_at,
            "processed": processed,
            "total": total,
            "success": success,
            "failed": failed,
            "files_downloaded": files_downloaded,
            "bytes_downloaded": bytes_downloaded,
            "apps_per_minute": round(rate * 60, 1),
            "eta_seconds": int(eta),
            "last_app": last_app,
            "last_status": last_status,
        },
        "cumulative": get_cumulative_stats(conn),
    }
    path = output_dir / "progress.json"
    path.write_text(json.dumps(progress, indent=2) + "\n")


def setup_logging(output_dir: Path) -> None:
    """Configure file + console logging."""
    log_path = output_dir / "download.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


async def run_download(args: argparse.Namespace) -> int:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(args.output_dir)
    # Optional rclone sync via environment variables
    sync_remote = os.environ.get("SYNC_REMOTE")
    sync_every = int(os.environ.get("SYNC_EVERY", "50"))
    sync_clear = os.environ.get("SYNC_CLEAR", "").lower() in ("1", "true", "yes")
    if sync_remote:
        logger.info("Sync enabled: %s (every %d apps, clear=%s)", sync_remote, sync_every, sync_clear)

    host_name = socket.gethostname()

    db_path = args.db_path or DB_PATH
    conn = get_db(db_path)

    candidates = get_applications_needing_download(
        conn,
        portal_type="idox",
        authority=args.authority,
    )

    if not candidates:
        print("No applications needing download found.")
        conn.close()
        return 0

    candidates = interleave_by_authority(candidates)
    if args.limit:
        candidates = candidates[: args.limit]
    print(f"Found {len(candidates)} applications with pending downloads.")

    if args.dry_run:
        for row in candidates[:20]:
            print(f"  {row['authority_name']:30s}  {row['reference'] or row['uid']}")
        if len(candidates) > 20:
            print(f"  ... and {len(candidates) - 20} more")
        conn.close()
        return 0

    log_id = log_scrape_start(
        conn,
        stage="document_download",
        source="idox",
        params={
            "limit": args.limit,
            "authority": args.authority,
            "output_dir": str(args.output_dir),
        },
    )

    results: list[dict[str, Any]] = []
    total_files = 0
    total_bytes = 0
    failures = 0
    run_started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    start_time = time.monotonic()

    logger.info(
        "Starting download run: %d candidates, limit=%s, authority=%s",
        len(candidates),
        args.limit,
        args.authority,
    )

    try:
        async with IdoxDocumentScraper() as scraper:
            for i, row in enumerate(candidates, 1):
                uid = row["uid"]
                authority = row["authority_name"] or "unknown"
                reference = row["reference"] or uid
                docs_url = row["documentation_url"]
                app_start = time.monotonic()
                now = datetime.now(timezone.utc).isoformat(timespec="seconds")

                try:
                    documents, zips, zip_reason = await scraper.download_zip(docs_url)

                    if not zips:
                        app_elapsed = time.monotonic() - app_start
                        logger.warning(
                            "[%d/%d] %s/%s: no zip returned (listed %d docs, %.1fs)",
                            i,
                            len(candidates),
                            authority,
                            reference,
                            len(documents),
                            app_elapsed,
                        )
                        results.append(
                            {
                                "uid": uid,
                                "authority_name": authority,
                                "reference": reference,
                                "documentation_url": docs_url,
                                "documents_listed": len(documents),
                                "files_downloaded": 0,
                                "total_bytes": 0,
                                "status": "no_zip",
                                "error": "",
                                "timestamp": now,
                                "elapsed_s": round(app_elapsed, 1),
                            }
                        )
                        failures += 1
                        record_download_attempt(
                            conn,
                            scrape_log_id=log_id,
                            application_uid=uid,
                            status="no_zip",
                            failure_code=zip_reason or "no_zip",
                            failure_message=zip_reason,
                            host_name=host_name,
                            documents_listed=len(documents),
                            elapsed_s=round(app_elapsed, 1),
                        )
                        write_progress(
                            args.output_dir,
                            conn,
                            started_at=run_started_at,
                            processed=i,
                            total=len(candidates),
                            success=i - failures,
                            failed=failures,
                            files_downloaded=total_files,
                            bytes_downloaded=total_bytes,
                            elapsed=time.monotonic() - start_time,
                            last_app=f"{authority}/{reference}",
                            last_status="no_zip",
                        )
                        continue

                    # Ensure document rows exist in DB (handles combined mode)
                    persist_new_documents(conn, dict(row), documents)

                    # Unpack and match
                    auth_dir = sanitize_dirname(authority)
                    ref_dir = sanitize_dirname(reference)
                    target_dir = args.output_dir / auth_dir / ref_dir

                    file_map = unpack_and_match(zips, documents, target_dir, args.output_dir)

                    # Update DB
                    if file_map:
                        with transaction(conn):
                            mark_documents_downloaded(conn, uid, file_map)

                    app_files = len(file_map)
                    app_bytes = sum(s for _, s in file_map.values())
                    total_files += app_files
                    total_bytes += app_bytes
                    app_elapsed = time.monotonic() - app_start

                    results.append(
                        {
                            "uid": uid,
                            "authority_name": authority,
                            "reference": reference,
                            "documentation_url": docs_url,
                            "documents_listed": len(documents),
                            "files_downloaded": app_files,
                            "total_bytes": app_bytes,
                            "status": "success",
                            "error": "",
                            "timestamp": now,
                            "elapsed_s": round(app_elapsed, 1),
                        }
                    )

                    record_download_attempt(
                        conn,
                        scrape_log_id=log_id,
                        application_uid=uid,
                        status="success",
                        host_name=host_name,
                        documents_listed=len(documents),
                        files_downloaded=app_files,
                        bytes_downloaded=app_bytes,
                        elapsed_s=round(app_elapsed, 1),
                    )

                    elapsed = time.monotonic() - start_time
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (len(candidates) - i) / rate if rate > 0 else 0
                    logger.info(
                        "[%d/%d] %s/%s: %d files, %d KB, %.1fs (%.1f apps/min, ETA %ds)",
                        i,
                        len(candidates),
                        authority,
                        reference,
                        app_files,
                        app_bytes // 1024,
                        app_elapsed,
                        rate * 60,
                        int(eta),
                    )
                    write_progress(
                        args.output_dir,
                        conn,
                        started_at=run_started_at,
                        processed=i,
                        total=len(candidates),
                        success=i - failures,
                        failed=failures,
                        files_downloaded=total_files,
                        bytes_downloaded=total_bytes,
                        elapsed=elapsed,
                        last_app=f"{authority}/{reference}",
                        last_status="success",
                    )

                except Exception as exc:
                    failures += 1
                    app_elapsed = time.monotonic() - app_start
                    logger.error(
                        "[%d/%d] %s/%s: %s: %s (%.1fs)",
                        i,
                        len(candidates),
                        authority,
                        reference,
                        type(exc).__name__,
                        exc,
                        app_elapsed,
                    )
                    error_msg = f"{type(exc).__name__}: {exc}"
                    results.append(
                        {
                            "uid": uid,
                            "authority_name": authority,
                            "reference": reference,
                            "documentation_url": docs_url,
                            "documents_listed": 0,
                            "files_downloaded": 0,
                            "total_bytes": 0,
                            "status": "error",
                            "error": error_msg,
                            "timestamp": now,
                            "elapsed_s": round(app_elapsed, 1),
                        }
                    )
                    record_download_attempt(
                        conn,
                        scrape_log_id=log_id,
                        application_uid=uid,
                        status="error",
                        failure_code=classify_failure(error_msg),
                        failure_message=error_msg[:500],
                        host_name=host_name,
                        elapsed_s=round(app_elapsed, 1),
                    )
                    write_progress(
                        args.output_dir,
                        conn,
                        started_at=run_started_at,
                        processed=i,
                        total=len(candidates),
                        success=i - failures,
                        failed=failures,
                        files_downloaded=total_files,
                        bytes_downloaded=total_bytes,
                        elapsed=time.monotonic() - start_time,
                        last_app=f"{authority}/{reference}",
                        last_status="error",
                    )

                # Periodic sync via rclone (set SYNC_REMOTE env var to enable)
                if sync_remote and i % sync_every == 0:
                    rclone_sync(args.output_dir, sync_remote, clear_local=sync_clear)

            # Log scraper-level stats
            logger.info("Scraper stats: %s", scraper.stats)

        # Final sync via rclone
        if sync_remote:
            rclone_sync(args.output_dir, sync_remote, clear_local=sync_clear)

        # Write results CSV
        results_path = args.output_dir / "download_results.csv"
        with results_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=RESULT_FIELDNAMES)
            writer.writeheader()
            writer.writerows(results)

        elapsed = time.monotonic() - start_time
        summary = (
            f"Done: {len(results)} apps, {total_files} files, "
            f"{total_bytes / 1024 / 1024:.1f} MB, {failures} failures "
            f"in {elapsed:.0f}s"
        )
        logger.info(summary)
        logger.info("Results CSV: %s", results_path)

        log_scrape_end(
            conn,
            log_id,
            records_processed=len(results),
            records_new=total_files,
            records_failed=failures,
            status="completed",
        )
        return 0

    except Exception as exc:
        logger.exception("Fatal error during download run")
        log_scrape_end(
            conn,
            log_id,
            records_processed=len(results),
            records_new=total_files,
            records_failed=failures,
            error_log=f"{type(exc).__name__}: {exc}",
            status="failed",
        )
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_download(parse_args())))
