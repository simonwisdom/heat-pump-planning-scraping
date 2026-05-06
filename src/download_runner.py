"""Shared runner for portal-specific document download scripts.

Each ``scripts/download_documents_<portal>.py`` defines two callables and
hands them to :func:`run_main`:

* ``make_scraper`` — returns an async-context-manager scraper instance.
* ``process_app(scraper, row, output_dir)`` — fetches one app's documents and
  files. Returns ``ProcessResult(documents, file_map, reason)`` where
  ``documents`` is the list of doc dicts to upsert, ``file_map`` is
  ``{document_url: (relative_path, bytes)}`` for files written to disk, and
  ``reason`` is ``None`` for full success, or one of
  ``"no_documents_listed" | "all_downloads_failed" | "partial" | <portal-specific>``.

The runner handles the boilerplate the four legacy downloaders
(``download_documents_{idox,northgate,publisher,planning_docs}.py``) duplicate:
CLI parsing, DB connection, candidate selection, worker pool, progress JSON,
results CSV, rclone sync, scrape-log bracketing.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import time
from collections import defaultdict
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

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


@dataclass
class ProcessResult:
    """Return value of a portal's ``process_app`` callback."""

    documents: list[dict]
    file_map: dict[str, tuple[str, int]]
    reason: Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)


ProcessAppFn = Callable[[Any, sqlite3.Row, Path], Awaitable[ProcessResult]]
MakeScraperFn = Callable[[], AbstractAsyncContextManager]


# ---------------------------------------------------------------------------
# Shared process_app factories
# ---------------------------------------------------------------------------


def make_url_process_app(default_ext: str = ".pdf") -> ProcessAppFn:
    """Build a ``process_app`` for scrapers exposing the common interface:

    * ``scrape_documents(docs_url) -> (list[dict], failure_code | None)``
    * ``download_document(doc_url, target_path, referer=...) -> (bytes_written, final_path)``

    Each ``doc`` dict is expected to carry ``document_url`` plus the optional
    metadata keys ``document_type``, ``description``, ``date_published``,
    ``drawing_number`` (used by ``persist_documents``).
    """

    async def process_app(scraper, row: sqlite3.Row, output_dir: Path) -> ProcessResult:
        docs_url = row["documentation_url"]
        authority = row["authority_name"] or "unknown"
        reference = row["reference"] or row["uid"]

        documents, failure_code = await scraper.scrape_documents(docs_url)
        if failure_code:
            return ProcessResult(documents=[], file_map={}, reason=failure_code)
        if not documents:
            return ProcessResult(documents=[], file_map={}, reason="no_documents_listed")

        target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
        file_map: dict[str, tuple[str, int]] = {}
        eligible = [d for d in documents if d.get("document_url")]

        for idx, doc in enumerate(eligible, start=1):
            target_name = synthesize_filename(
                idx,
                doc.get("document_type", ""),
                doc.get("description", ""),
                ext=default_ext,
            )
            bytes_written, final_path = await scraper.download_document(
                doc["document_url"],
                target_dir / target_name,
                referer=docs_url,
            )
            if bytes_written > 0:
                rel_path = str(Path(final_path).relative_to(output_dir))
                file_map[doc["document_url"]] = (rel_path, bytes_written)

        if not file_map:
            reason = "all_downloads_failed"
        elif len(file_map) < len(eligible):
            reason = "partial"
        else:
            reason = None
        return ProcessResult(documents=documents, file_map=file_map, reason=reason)

    return process_app


def make_doc_process_app(default_ext: str = ".pdf") -> ProcessAppFn:
    """Like :func:`make_url_process_app` but for scrapers whose
    ``download_document`` takes the full doc dict instead of just the URL.

    * ``download_document(doc, target_path) -> (bytes_written, final_path)``
    """

    async def process_app(scraper, row: sqlite3.Row, output_dir: Path) -> ProcessResult:
        docs_url = row["documentation_url"]
        authority = row["authority_name"] or "unknown"
        reference = row["reference"] or row["uid"]

        documents, failure_code = await scraper.scrape_documents(docs_url)
        if failure_code:
            return ProcessResult(documents=[], file_map={}, reason=failure_code)
        if not documents:
            return ProcessResult(documents=[], file_map={}, reason="no_documents_listed")

        target_dir = output_dir / sanitize_dirname(authority) / sanitize_dirname(reference)
        file_map: dict[str, tuple[str, int]] = {}
        eligible = [d for d in documents if d.get("document_url")]

        for idx, doc in enumerate(eligible, start=1):
            target_name = synthesize_filename(
                idx,
                doc.get("document_type", ""),
                doc.get("description", ""),
                ext=default_ext,
            )
            bytes_written, final_path = await scraper.download_document(
                doc,
                target_dir / target_name,
            )
            if bytes_written > 0:
                rel_path = str(Path(final_path).relative_to(output_dir))
                file_map[doc["document_url"]] = (rel_path, bytes_written)

        if not file_map:
            reason = "all_downloads_failed"
        elif len(file_map) < len(eligible):
            reason = "partial"
        else:
            reason = None
        return ProcessResult(documents=documents, file_map=file_map, reason=reason)

    return process_app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def classify_failure(error_str: str) -> str:
    e = (error_str or "").lower()
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
    if "captcha" in e or "block" in e or "access denied" in e:
        return "blocked"
    return "unknown"


_FS_UNSAFE = re.compile(r"[^\w.-]+")


def sanitize_dirname(name: str) -> str:
    """Make a string safe for use as a directory name."""
    safe = re.sub(r'[<>:"/\\|?*]', "_", name)
    safe = safe.strip(". ")
    return safe or "_unnamed"


def slugify(text: str, maxlen: int = 40) -> str:
    """Make a string safe for use as a filename fragment."""
    text = re.sub(r"\s+", " ", text).strip()
    text = _FS_UNSAFE.sub("_", text).strip("_")
    return text[:maxlen] if text else ""


def synthesize_filename(idx: int, doc_type: str, description: str, ext: str = ".pdf") -> str:
    """Build a readable filename from doc metadata. ``ext`` includes the leading dot."""
    parts: list[str] = []
    type_slug = slugify(doc_type)
    desc_slug = slugify(description)
    if type_slug:
        parts.append(type_slug)
    if desc_slug and desc_slug != type_slug:
        parts.append(desc_slug)
    base = "_".join(parts) or "document"
    if not ext.startswith("."):
        ext = "." + ext
    return f"{idx:03d}_{base}{ext}"


def interleave_by_authority(rows: list) -> list:
    """Round-robin across authorities so we don't hammer one portal."""
    by_authority: dict[str, list] = defaultdict(list)
    for row in rows:
        by_authority[row["authority_name"] or "unknown"].append(row)
    result: list = []
    while by_authority:
        empty: list[str] = []
        for authority in sorted(by_authority):
            result.append(by_authority[authority].pop(0))
            if not by_authority[authority]:
                empty.append(authority)
        for a in empty:
            del by_authority[a]
    return result


def rclone_sync(local_dir: Path, remote_path: str, *, clear_local: bool = False) -> bool:
    if not local_dir.exists() or not any(local_dir.iterdir()):
        return True
    cmd = ["rclone", "copy", str(local_dir), remote_path, "--transfers", "8", "--progress"]
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


def persist_documents(conn: sqlite3.Connection, app_row: sqlite3.Row, documents: list[dict]) -> int:
    """Insert document rows for an app. Skips rows without a document_url."""
    inserted = 0
    with transaction(conn):
        for doc in documents:
            if not (doc.get("document_url") or "").strip():
                continue
            created = upsert_document(
                conn,
                {
                    "application_uid": app_row["uid"],
                    "documentation_url": app_row["documentation_url"],
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
    portal: str,
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
    (output_dir / f"progress_{portal}.json").write_text(json.dumps(progress, indent=2) + "\n")


def setup_logging(output_dir: Path, portal: str) -> None:
    log_path = output_dir / f"download_{portal}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def build_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None, help="Max applications to process")
    parser.add_argument("--authority", type=str, default=None, help="Filter to single authority name")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PDF_DIR,
        help="Root directory for downloaded files",
    )
    parser.add_argument("--dry-run", action="store_true", help="List candidates without downloading")
    parser.add_argument(
        "--only-never-attempted",
        action="store_true",
        help="Skip apps that already have a row in download_attempts",
    )
    parser.add_argument(
        "--only-failure-codes",
        type=str,
        default=None,
        help="Comma-separated failure codes; retry only apps whose latest attempt failed with one of these.",
    )
    return parser


def select_failure_code_uids(conn: sqlite3.Connection, codes: list[str]) -> set[str]:
    if not codes:
        return set()
    placeholders = ",".join("?" * len(codes))
    rows = conn.execute(
        f"""
        WITH latest AS (
            SELECT application_uid, failure_code,
                   ROW_NUMBER() OVER (
                       PARTITION BY application_uid
                       ORDER BY attempted_at DESC, id DESC
                   ) AS rn
            FROM download_attempts
        )
        SELECT application_uid FROM latest
        WHERE rn = 1 AND failure_code IN ({placeholders})
        """,
        codes,
    )
    return {row[0] for row in rows}


def select_attempted_uids(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT DISTINCT application_uid FROM download_attempts")}


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


async def run_download(
    args: argparse.Namespace,
    *,
    portal: str,
    make_scraper: MakeScraperFn,
    process_app: ProcessAppFn,
    default_workers: int = 2,
) -> int:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(args.output_dir, portal)

    sync_remote = os.environ.get("SYNC_REMOTE")
    sync_every = max(1, int(os.environ.get("SYNC_EVERY", "50")))
    sync_clear = os.environ.get("SYNC_CLEAR", "").lower() in ("1", "true", "yes")
    if sync_remote:
        logger.info("Sync enabled: %s (every %d apps, clear=%s)", sync_remote, sync_every, sync_clear)

    host_name = socket.gethostname()
    db_path = args.db_path or DB_PATH
    conn = get_db(db_path)

    candidates = get_applications_needing_download(
        conn,
        portal_type=portal,
        authority=args.authority,
    )

    if getattr(args, "only_never_attempted", False):
        attempted = select_attempted_uids(conn)
        before = len(candidates)
        candidates = [c for c in candidates if c["uid"] not in attempted]
        logger.info("--only-never-attempted: %d -> %d candidates", before, len(candidates))

    if getattr(args, "only_failure_codes", None):
        codes = [c.strip() for c in args.only_failure_codes.split(",") if c.strip()]
        target_uids = select_failure_code_uids(conn, codes)
        before = len(candidates)
        candidates = [c for c in candidates if c["uid"] in target_uids]
        logger.info(
            "--only-failure-codes %s: %d -> %d candidates",
            codes,
            before,
            len(candidates),
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
        source=portal,
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
        "Starting %s download run: %d candidates, limit=%s, authority=%s",
        portal,
        len(candidates),
        args.limit,
        args.authority,
    )

    max_workers = max(1, int(os.environ.get("MAX_CONCURRENT_APPS", str(default_workers))))
    logger.info("Download workers: %d", max_workers)

    work_queue: asyncio.Queue = asyncio.Queue()
    result_queue: asyncio.Queue = asyncio.Queue(maxsize=2)
    for row in candidates:
        await work_queue.put(row)
    for _ in range(max_workers):
        await work_queue.put(None)

    async def worker() -> None:
        async with make_scraper() as scraper:
            while True:
                row = await work_queue.get()
                if row is None:
                    break
                t0 = time.monotonic()
                try:
                    result = await process_app(scraper, row, args.output_dir)
                    elapsed_one = time.monotonic() - t0
                    await result_queue.put((row, result, None, elapsed_one))
                except Exception as exc:  # noqa: BLE001
                    elapsed_one = time.monotonic() - t0
                    await result_queue.put((row, None, exc, elapsed_one))

    workers = [asyncio.create_task(worker()) for _ in range(max_workers)]

    async def watchdog() -> None:
        await asyncio.gather(*workers, return_exceptions=True)
        await result_queue.put(None)

    watchdog_task = asyncio.create_task(watchdog())

    try:
        processed = 0
        last_status = "pending"
        while processed < len(candidates):
            item = await result_queue.get()
            if item is None:
                worker_errors = [w.exception() for w in workers if w.done() and w.exception()]
                for err in worker_errors:
                    logger.error("Worker exited with error: %s", err)
                raise RuntimeError(f"All workers exited after {processed}/{len(candidates)} results; aborting")
            row, result, exc, app_elapsed = item
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
            elif not result.file_map:
                failures += 1
                docs_listed = len(result.documents)
                reason = result.reason or ("no_documents_listed" if docs_listed == 0 else "all_downloads_failed")
                logger.warning(
                    "[%d/%d] %s/%s: no files (listed %d, %.1fs) [%s]",
                    processed,
                    len(candidates),
                    authority,
                    reference,
                    docs_listed,
                    app_elapsed,
                    reason,
                )
                results.append(
                    {
                        "uid": uid,
                        "authority_name": authority,
                        "reference": reference,
                        "documentation_url": docs_url,
                        "documents_listed": docs_listed,
                        "files_downloaded": 0,
                        "total_bytes": 0,
                        "status": "no_files",
                        "error": reason,
                        "timestamp": now,
                        "elapsed_s": round(app_elapsed, 1),
                    }
                )
                record_download_attempt(
                    conn,
                    scrape_log_id=log_id,
                    application_uid=uid,
                    status="no_files",
                    failure_code=reason,
                    failure_message=reason,
                    host_name=host_name,
                    documents_listed=docs_listed,
                    elapsed_s=round(app_elapsed, 1),
                )
                last_status = "no_files"
            else:
                persist_documents(conn, row, result.documents)
                with transaction(conn):
                    mark_documents_downloaded(conn, uid, result.file_map)
                app_files = len(result.file_map)
                app_bytes = sum(s for _, s in result.file_map.values())
                total_files += app_files
                total_bytes += app_bytes
                if result.reason == "partial":
                    failures += 1
                    status = "partial"
                else:
                    status = "success"
                results.append(
                    {
                        "uid": uid,
                        "authority_name": authority,
                        "reference": reference,
                        "documentation_url": docs_url,
                        "documents_listed": len(result.documents),
                        "files_downloaded": app_files,
                        "total_bytes": app_bytes,
                        "status": status,
                        "error": result.reason or "",
                        "timestamp": now,
                        "elapsed_s": round(app_elapsed, 1),
                    }
                )
                record_download_attempt(
                    conn,
                    scrape_log_id=log_id,
                    application_uid=uid,
                    status=status,
                    failure_code=result.reason if status == "partial" else None,
                    failure_message=result.reason if status == "partial" else None,
                    host_name=host_name,
                    documents_listed=len(result.documents),
                    files_downloaded=app_files,
                    bytes_downloaded=app_bytes,
                    elapsed_s=round(app_elapsed, 1),
                )
                elapsed = time.monotonic() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (len(candidates) - processed) / rate if rate > 0 else 0
                logger.info(
                    "[%d/%d] %s/%s: %d/%d files, %d KB, %.1fs (%.1f apps/min, ETA %ds) [%s]",
                    processed,
                    len(candidates),
                    authority,
                    reference,
                    app_files,
                    len(result.documents),
                    app_bytes // 1024,
                    app_elapsed,
                    rate * 60,
                    int(eta),
                    status,
                )
                last_status = status

            write_progress(
                args.output_dir,
                portal,
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
        watchdog_task.cancel()
        try:
            await watchdog_task
        except asyncio.CancelledError:
            pass

        if sync_remote:
            rclone_sync(args.output_dir, sync_remote, clear_local=sync_clear)

        results_path = args.output_dir / f"download_results_{portal}.csv"
        with results_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=RESULT_FIELDNAMES)
            writer.writeheader()
            writer.writerows(results)

        elapsed = time.monotonic() - start_time
        logger.info(
            "Done: %d apps, %d files, %.1f MB, %d failures in %.0fs",
            len(results),
            total_files,
            total_bytes / 1024 / 1024,
            failures,
            elapsed,
        )
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
        logger.exception("Fatal error during %s download run", portal)
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


def run_main(
    *,
    portal: str,
    description: str,
    make_scraper: MakeScraperFn,
    process_app: ProcessAppFn,
    extra_args: Optional[Callable[[argparse.ArgumentParser], None]] = None,
    default_workers: int = 2,
) -> int:
    """Entry point for ``scripts/download_documents_<portal>.py`` modules."""
    parser = build_arg_parser(description)
    if extra_args:
        extra_args(parser)
    args = parser.parse_args()
    return asyncio.run(
        run_download(
            args,
            portal=portal,
            make_scraper=make_scraper,
            process_app=process_app,
            default_workers=default_workers,
        )
    )
