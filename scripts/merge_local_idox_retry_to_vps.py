#!/usr/bin/env python3
"""Merge the post-run local ashp.db (from a local-egress retry pass) into the VPS canonical DB.

Approach: SCP the local DB up to /tmp/local_post_run.db on the VPS, then ATTACH it
in the VPS sqlite session and INSERT/UPSERT the new rows that were added during the
local run.

Identifies "new" rows using markers captured before the local run kicked off:
    - download_attempts.id > VPS_MAX_ATTEMPT_ID_BEFORE
    - scrape_log.id        > VPS_MAX_SCRAPE_LOG_ID_BEFORE
    - documents rows whose download_status='downloaded' OR file_path IS NOT NULL
      where the corresponding VPS row is still pending (upsert by application_uid+document_url)

Usage:
    python3 scripts/merge_local_idox_retry_to_vps.py [--dry-run]

Markers are hard-coded at the top of this script for the 2026-05-13 run.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

LOCAL_DB = Path(__file__).resolve().parents[1] / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
VPS_HOST = "root@178.104.201.79"
VPS_DB = "/root/heat-pump-planning-scraping/_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
VPS_STAGING = "/tmp/local_post_run.db"

# Markers captured 2026-05-14, immediately before the second local retry pass started.
VPS_MAX_ATTEMPT_ID_BEFORE = 42044
VPS_MAX_SCRAPE_LOG_ID_BEFORE = 105


def run_ssh(cmd: str, *, check: bool = True) -> str:
    result = subprocess.run(
        ["ssh", VPS_HOST, cmd],
        capture_output=True,
        text=True,
        check=check,
    )
    return result.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report counts but do not write to VPS")
    args = parser.parse_args()

    if not LOCAL_DB.exists():
        print(f"Local DB not found at {LOCAL_DB}", file=sys.stderr)
        return 1

    print(f"Pushing {LOCAL_DB} → {VPS_HOST}:{VPS_STAGING} ...")
    subprocess.run(["scp", str(LOCAL_DB), f"{VPS_HOST}:{VPS_STAGING}"], check=True)

    pre_counts = run_ssh(
        f"sqlite3 {VPS_DB} '"
        'SELECT "vps_attempts=" || COUNT(*) FROM download_attempts;'
        'SELECT "vps_documents_downloaded=" || COUNT(*) FROM documents WHERE download_status="downloaded";'
        'SELECT "vps_scrape_log=" || COUNT(*) FROM scrape_log;'
        "'"
    )
    print("Before merge:")
    print(pre_counts)

    preview = run_ssh(
        f"sqlite3 {VPS_DB} \"ATTACH '{VPS_STAGING}' AS local; "
        f"SELECT 'local_new_attempts=' || COUNT(*) "
        f"  FROM local.download_attempts WHERE id > {VPS_MAX_ATTEMPT_ID_BEFORE}; "
        f"SELECT 'local_new_scrape_log=' || COUNT(*) FROM local.scrape_log WHERE id > {VPS_MAX_SCRAPE_LOG_ID_BEFORE}; "
        "SELECT 'local_documents_to_update=' || COUNT(*) "
        "  FROM local.documents l "
        "  JOIN documents v ON v.application_uid=l.application_uid AND v.document_url=l.document_url "
        " WHERE l.download_status='downloaded' AND COALESCE(v.download_status,'')!='downloaded';"
        '"'
    )
    print("Would write:")
    print(preview)

    if args.dry_run:
        print("Dry run — exiting before writes.")
        return 0

    merge_sql = f"""
    ATTACH '{VPS_STAGING}' AS local;
    BEGIN;
    -- 1. download_attempts: append new rows (id auto-generated on insert)
    INSERT INTO download_attempts
        (scrape_log_id, application_uid, attempted_at, status,
         failure_code, failure_message, host_name,
         documents_listed, files_downloaded, bytes_downloaded, elapsed_s)
    SELECT scrape_log_id, application_uid, attempted_at, status,
           failure_code, failure_message, host_name,
           documents_listed, files_downloaded, bytes_downloaded, elapsed_s
    FROM local.download_attempts
    WHERE id > {VPS_MAX_ATTEMPT_ID_BEFORE};

    -- 2. scrape_log: append new rows
    INSERT INTO scrape_log (stage, source, params_json, started_at, completed_at,
                            records_processed, records_new, records_failed, error_log, status)
    SELECT stage, source, params_json, started_at, completed_at,
           records_processed, records_new, records_failed, error_log, status
    FROM local.scrape_log
    WHERE id > {VPS_MAX_SCRAPE_LOG_ID_BEFORE};

    -- 3. documents: update VPS rows that local has marked downloaded
    UPDATE documents
    SET file_path = (SELECT l.file_path FROM local.documents l
                     WHERE l.application_uid=documents.application_uid
                       AND l.document_url=documents.document_url),
        file_size_bytes = (SELECT l.file_size_bytes FROM local.documents l
                          WHERE l.application_uid=documents.application_uid
                            AND l.document_url=documents.document_url),
        download_status = 'downloaded'
    WHERE EXISTS (
        SELECT 1 FROM local.documents l
        WHERE l.application_uid=documents.application_uid
          AND l.document_url=documents.document_url
          AND l.download_status='downloaded'
          AND COALESCE(documents.download_status,'') != 'downloaded'
    );

    -- 4. documents: insert any that exist locally but not on VPS (new doc metadata picked up during retry)
    INSERT INTO documents
        (application_uid, document_type, description, document_url, documentation_url,
         date_published, drawing_number, file_path, file_size_bytes, download_status, scraped_at)
    SELECT l.application_uid, l.document_type, l.description, l.document_url, l.documentation_url,
           l.date_published, l.drawing_number, l.file_path, l.file_size_bytes, l.download_status, l.scraped_at
    FROM local.documents l
    LEFT JOIN documents v ON v.application_uid=l.application_uid AND v.document_url=l.document_url
    WHERE v.id IS NULL;

    COMMIT;
    DETACH local;
    """

    print("Running merge on VPS ...")
    # -bail: stop at first error so a failing statement aborts the whole batch
    # before later statements (and the implicit auto-commit of earlier ones) can
    # leave the VPS in a half-merged state.
    proc = subprocess.run(
        ["ssh", VPS_HOST, f"sqlite3 -bail {VPS_DB}"],
        input=merge_sql,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        print("Merge failed:", proc.stderr, file=sys.stderr)
        return 1
    print(proc.stdout)

    post_counts = run_ssh(
        f"sqlite3 {VPS_DB} '"
        'SELECT "vps_attempts=" || COUNT(*) FROM download_attempts;'
        'SELECT "vps_documents_downloaded=" || COUNT(*) FROM documents WHERE download_status="downloaded";'
        'SELECT "vps_scrape_log=" || COUNT(*) FROM scrape_log;'
        "'"
    )
    print("After merge:")
    print(post_counts)

    print("Cleaning up staging file on VPS ...")
    run_ssh(f"rm {VPS_STAGING}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
