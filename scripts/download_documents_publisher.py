#!/usr/bin/env python3
"""Download all documents for Idox Publisher applications.

Publisher issues session-scoped document hashes, so each app must be listed
and downloaded in one pass: the URL returned by the AJAX endpoint becomes
invalid once the session rotates. This script runs listing + per-file
downloads in the same scraper session.

Optional rclone sync via environment variables:
    SYNC_REMOTE  rclone remote path (e.g. "myremote:path/to/docs/")
    SYNC_EVERY   sync every N apps (default: 50)
    SYNC_CLEAR   delete local files after sync ("1" to enable)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

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
from src.publisher_scraper import PublisherDocumentScraper

logger = logging.getLogger(__name__)


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


def classify_failure(error_str: str) -> str:
    e = error_str.lower()
    if "name or service not known" in e or "nodename nor servname" in e:
        return "dns_error"
    if "403" in e:
        return "http_403"
    if "404" in e:
        return "http_404"
    if "500" in e:
        return "http_500"
    if "server disconnected" in e or "connection reset" in e:
        return "connection_reset"
    if "timeout" in e or "timed out" in e:
        return "timeout"
    if "certificate" in e or "ssl" in e or "tls" in e:
        return "tls_error"
    return "unknown"


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


_FS_UNSAFE = re.compile(r"[^\w.-]+")


def sanitize_dirname(name: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*]', "_", name)
    safe = safe.strip(". ")
    return safe or "_unnamed"


def slugify(text: str, maxlen: int = 40) -> str:
    """Make a string safe for use in a filename fragment."""
    text = re.sub(r"\s+", " ", text).strip()
    text = _FS_UNSAFE.sub("_", text).strip("_")
    return text[:maxlen] if text else ""


def synthesize_filename(idx: int, doc_type: str, description: str, doc_url: str) -> str:
    """Build a readable filename from doc metadata, preserving the original extension.

    Publisher hashes rotate per session so using them in filenames is
    meaningless; synthesize from the metadata the scraper returned. Extension
    is taken from the URL path since Publisher serves PDFs, TIFFs, .msg, etc.
    """
    from urllib.parse import urlparse as _urlparse

    ext = Path(_urlparse(doc_url).path).suffix.lower() or ".pdf"
    parts = []
    type_slug = slugify(doc_type)
    desc_slug = slugify(description)
    if type_slug:
        parts.append(type_slug)
    if desc_slug and desc_slug != type_slug:
        parts.append(desc_slug)
    base = "_".join(parts) or "document"
    return f"{idx:03d}_{base}{ext}"


def rclone_sync(local_dir: Path, remote_path: str, clear_local: bool = False) -> bool:
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
        for child in local_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
        print(f"  Cleared local staging ({local_dir})")
    return True


def persist_documents(conn: sqlite3.Connection, app: dict, documents: list[dict]) -> int:
    """Insert document rows not already present."""
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
    doc_row = conn.execute(
        "SELECT"
        "  COUNT(*) AS total_docs,"
        "  SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) AS downloaded,"
        "  SUM(CASE WHEN download_status = 'pending' THEN 1 ELSE 0 END) AS pending,"
        "  SUM(CASE WHEN file_size_bytes IS NOT NULL THEN file_size_bytes ELSE 0 END) AS total_bytes"
        " FROM documents"
    ).fetchone()
    app_row = conn.execute(
        "SELECT COUNT(DISTINCT d.application_uid) AS apps_with_downloads"
        " FROM documents d WHERE d.download_status = 'downloaded'"
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
    path = output_dir / "progress_publisher.json"
    path.write_text(json.dumps(progress, indent=2) + "\n")


def setup_logging(output_dir: Path) -> None:
    log_path = output_dir / "download_publisher.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


async def process_app(
    scraper: PublisherDocumentScraper,
    row: sqlite3.Row,
    output_dir: Path,
) -> tuple[list[dict], dict[str, tuple[str, int]], str | None]:
    """List then download all documents for one app. Returns (documents, file_map, failure_reason)."""
    docs_url = row["documentation_url"]
    authority = row["authority_name"] or "unknown"
    reference = row["reference"] or row["uid"]

    documents = await scraper.scrape_documents(docs_url)
    if not documents:
        return [], {}, "no_documents_listed"

    target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
    file_map: dict[str, tuple[str, int]] = {}

    for idx, doc in enumerate(documents, start=1):
        doc_url = doc.get("document_url")
        if not doc_url:
            continue
        filename = synthesize_filename(
            idx,
            doc.get("document_type", ""),
            doc.get("description", ""),
            doc_url,
        )
        target_path = target_dir / filename
        bytes_written = await scraper.download_document(doc_url, target_path, referer=docs_url)
        if bytes_written > 0:
            rel_path = str(target_path.relative_to(output_dir))
            file_map[doc_url] = (rel_path, bytes_written)

    if not file_map:
        reason = "all_downloads_failed"
    elif len(file_map) < len(documents):
        reason = "partial"
    else:
        reason = None
    return documents, file_map, reason


async def run_download(args: argparse.Namespace) -> int:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(args.output_dir)

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
        portal_type="publisher",
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
        source="publisher",
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

    max_workers = int(os.environ.get("MAX_CONCURRENT_APPS", "2"))
    logger.info("Download workers: %d", max_workers)

    try:
        # Each worker owns its own scraper (and thus its own httpx session + cookie jar).
        # Publisher cookies (JSESSIONID) are domain-scoped, so sharing one client across
        # workers that hit the same domain would mean one worker's listing visit could
        # invalidate another's just-obtained hashes.
        result_queue: asyncio.Queue = asyncio.Queue(maxsize=2)
        work_queue: asyncio.Queue = asyncio.Queue()

        for row in candidates:
            await work_queue.put(row)
        for _ in range(max_workers):
            await work_queue.put(None)

        async def worker():
            async with PublisherDocumentScraper() as scraper:
                while True:
                    row = await work_queue.get()
                    if row is None:
                        break
                    t0 = time.monotonic()
                    try:
                        documents, file_map, reason = await process_app(scraper, row, args.output_dir)
                        elapsed = time.monotonic() - t0
                        await result_queue.put((row, documents, file_map, reason, None, elapsed))
                    except Exception as exc:
                        elapsed = time.monotonic() - t0
                        await result_queue.put((row, [], {}, None, exc, elapsed))

        workers = [asyncio.create_task(worker()) for _ in range(max_workers)]

        processed = 0
        while processed < len(candidates):
            row, documents, file_map, reason, exc, app_elapsed = await result_queue.get()
            processed += 1

            uid = row["uid"]
            authority = row["authority_name"] or "unknown"
            reference = row["reference"] or uid
            docs_url = row["documentation_url"]
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")

            if exc is not None:
                failures += 1
                error_msg = f"{type(exc).__name__}: {exc}"
                logger.error(
                    "[%d/%d] %s/%s: %s (%.1fs)",
                    processed,
                    len(candidates),
                    authority,
                    reference,
                    error_msg,
                    app_elapsed,
                )
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
                last_status = "error"
            elif not file_map:
                failures += 1
                logger.warning(
                    "[%d/%d] %s/%s: no files downloaded (listed %d docs, %.1fs) [%s]",
                    processed,
                    len(candidates),
                    authority,
                    reference,
                    len(documents),
                    app_elapsed,
                    reason or "unknown",
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
                        "status": "no_files",
                        "error": reason or "",
                        "timestamp": now,
                        "elapsed_s": round(app_elapsed, 1),
                    }
                )
                record_download_attempt(
                    conn,
                    scrape_log_id=log_id,
                    application_uid=uid,
                    status="no_files",
                    failure_code=reason or "no_files",
                    failure_message=reason,
                    host_name=host_name,
                    documents_listed=len(documents),
                    elapsed_s=round(app_elapsed, 1),
                )
                last_status = "no_files"
            elif reason == "partial":
                # Partial success: keep the files on disk but don't commit doc rows
                # or mark any as downloaded. Publisher hashes rotate per session so
                # the successful URLs we have are only valid right now anyway.
                # Leaving DB state clean makes this app a candidate for retry on the
                # next run, which will fetch a fresh listing + fresh hashes.
                failures += 1
                app_files = len(file_map)
                app_bytes = sum(s for _, s in file_map.values())
                logger.warning(
                    "[%d/%d] %s/%s: PARTIAL %d/%d files, %d KB, %.1fs -- will retry on next run",
                    processed,
                    len(candidates),
                    authority,
                    reference,
                    app_files,
                    len(documents),
                    app_bytes // 1024,
                    app_elapsed,
                )
                results.append(
                    {
                        "uid": uid,
                        "authority_name": authority,
                        "reference": reference,
                        "documentation_url": docs_url,
                        "documents_listed": len(documents),
                        "files_downloaded": app_files,
                        "total_bytes": app_bytes,
                        "status": "partial",
                        "error": "",
                        "timestamp": now,
                        "elapsed_s": round(app_elapsed, 1),
                    }
                )
                record_download_attempt(
                    conn,
                    scrape_log_id=log_id,
                    application_uid=uid,
                    status="partial",
                    failure_code="partial",
                    failure_message=f"{app_files}/{len(documents)} files",
                    host_name=host_name,
                    documents_listed=len(documents),
                    files_downloaded=app_files,
                    bytes_downloaded=app_bytes,
                    elapsed_s=round(app_elapsed, 1),
                )
                last_status = "partial"
            else:
                persist_documents(conn, dict(row), documents)
                with transaction(conn):
                    mark_documents_downloaded(conn, uid, file_map)

                app_files = len(file_map)
                app_bytes = sum(s for _, s in file_map.values())
                total_files += app_files
                total_bytes += app_bytes

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
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (len(candidates) - processed) / rate if rate > 0 else 0
                logger.info(
                    "[%d/%d] %s/%s: %d/%d files, %d KB, %.1fs (%.1f apps/min, ETA %ds)",
                    processed,
                    len(candidates),
                    authority,
                    reference,
                    app_files,
                    len(documents),
                    app_bytes // 1024,
                    app_elapsed,
                    rate * 60,
                    int(eta),
                )
                last_status = "success"

            write_progress(
                args.output_dir,
                conn,
                started_at=run_started_at,
                processed=processed,
                total=len(candidates),
                success=processed - failures,
                failed=failures,
                files_downloaded=total_files,
                bytes_downloaded=total_bytes,
                elapsed=time.monotonic() - start_time,
                last_app=f"{authority}/{reference}",
                last_status=last_status,
            )

            if sync_remote and processed % sync_every == 0:
                rclone_sync(args.output_dir, sync_remote, clear_local=sync_clear)

        await asyncio.gather(*workers)

        if sync_remote:
            rclone_sync(args.output_dir, sync_remote, clear_local=sync_clear)

        results_path = args.output_dir / "download_results_publisher.csv"
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
