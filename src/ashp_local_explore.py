"""Utilities for pulling a consistent local copy of the canonical VPS ASHP database."""

from __future__ import annotations

import sqlite3
import subprocess
import uuid
from pathlib import Path

from .db import ensure_views

SSH_TARGET = "root@178.104.201.79"
REMOTE_DB = "/root/heat-pump-planning-scraping/_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
LOCAL_DB = Path("/tmp/ashp.db")
LOCAL_DOWNLOAD_DB = Path("/tmp/ashp.db.partial")
REMOTE_TMP_PREFIX = "/tmp/ashp-datasette-snapshot-"


def remote_snapshot_path() -> str:
    return f"{REMOTE_TMP_PREFIX}{uuid.uuid4().hex}.db"


def remote_snapshot_script() -> str:
    return """
import sqlite3
import sys

source_db = sys.argv[1]
snapshot_db = sys.argv[2]

src = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
dst = sqlite3.connect(snapshot_db)
try:
    src.backup(dst)
finally:
    dst.close()
    src.close()
"""


def create_remote_snapshot(snapshot_path: str) -> None:
    subprocess.run(
        ["ssh", SSH_TARGET, "python3", "-", REMOTE_DB, snapshot_path],
        input=remote_snapshot_script(),
        text=True,
        check=True,
    )


def scp_snapshot_command(snapshot_path: str) -> list[str]:
    return ["scp", "-p", f"{SSH_TARGET}:{snapshot_path}", str(LOCAL_DOWNLOAD_DB)]


def remove_remote_snapshot(snapshot_path: str) -> None:
    subprocess.run(["ssh", SSH_TARGET, "rm", "-f", snapshot_path], check=False)


def ensure_local_views(db_path: str | Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        ensure_views(conn)
    finally:
        conn.close()


def validate_local_db(db_path: str | Path) -> None:
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            result = conn.execute("PRAGMA integrity_check").fetchone()
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        raise RuntimeError(f"Integrity check failed for {db_path}: {exc}") from exc

    if not result or result[0] != "ok":
        raise RuntimeError(f"Integrity check failed for {db_path}: {result[0] if result else 'no result'}")


def refresh_local_db() -> Path:
    """Create a consistent VPS snapshot, copy it locally, validate it, and refresh derived views."""
    snapshot_path = remote_snapshot_path()
    try:
        print(f"Creating consistent snapshot on {SSH_TARGET}:{snapshot_path}")
        create_remote_snapshot(snapshot_path)

        print(f"Copying {SSH_TARGET}:{snapshot_path} -> {LOCAL_DOWNLOAD_DB}")
        subprocess.run(scp_snapshot_command(snapshot_path), check=True)
    finally:
        remove_remote_snapshot(snapshot_path)

    validate_local_db(LOCAL_DOWNLOAD_DB)
    LOCAL_DOWNLOAD_DB.replace(LOCAL_DB)
    ensure_local_views(LOCAL_DB)
    return LOCAL_DB
