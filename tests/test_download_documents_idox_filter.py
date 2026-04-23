"""Tests for the failure-code retry filter in download_documents_idox."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from src.db import get_db, upsert_application

# Load the script as a module so we can call its helpers directly.
_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "download_documents_idox.py"
_SPEC = importlib.util.spec_from_file_location("download_documents_idox", _SCRIPT)
download_documents_idox = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(download_documents_idox)
select_failure_code_uids = download_documents_idox.select_failure_code_uids


@pytest.fixture
def conn():
    connection = get_db(Path(":memory:"))
    try:
        yield connection
    finally:
        connection.close()


def _seed_app(conn, uid: str) -> None:
    upsert_application(
        conn,
        {
            "uid": uid,
            "reference": uid,
            "name": "x",
            "description": "x",
            "area_name": "Test Council",
            "area_id": "TC",
            "app_state": "Decided",
            "start_date": "2025-01-01",
            "link": f"https://www.planit.org.uk/p/{uid}/",
            "other_fields": {"docs_url": f"https://council.example/{uid}"},
        },
    )


def _record_attempt(conn, uid: str, attempted_at: str, failure_code: str | None, status: str = "no_zip") -> None:
    conn.execute(
        """
        INSERT INTO download_attempts (application_uid, attempted_at, status, failure_code, host_name)
        VALUES (?, ?, ?, ?, 'test-host')
        """,
        (uid, attempted_at, status, failure_code),
    )
    conn.commit()


def test_select_failure_code_uids_picks_latest_attempt_only(conn):
    _seed_app(conn, "A")
    # First attempt failed with get_failed; later attempt succeeded -- should NOT match.
    _record_attempt(conn, "A", "2026-04-20T10:00:00Z", "get_failed")
    _record_attempt(conn, "A", "2026-04-21T10:00:00Z", None, status="success")

    assert select_failure_code_uids(conn, ["get_failed"]) == set()


def test_select_failure_code_uids_returns_apps_whose_latest_matches(conn):
    for uid in ("A", "B", "C", "D"):
        _seed_app(conn, uid)
    _record_attempt(conn, "A", "2026-04-21T10:00:00Z", "get_failed")
    _record_attempt(conn, "B", "2026-04-21T10:00:00Z", "no_documents")
    _record_attempt(conn, "C", "2026-04-21T10:00:00Z", "http_403")
    _record_attempt(conn, "D", "2026-04-21T10:00:00Z", "no_zip")

    assert select_failure_code_uids(conn, ["get_failed", "no_zip"]) == {"A", "D"}


def test_select_failure_code_uids_handles_same_timestamp_via_id_tiebreaker(conn):
    _seed_app(conn, "A")
    # Same attempted_at; the higher id should win as the "latest".
    _record_attempt(conn, "A", "2026-04-21T10:00:00Z", "get_failed")
    _record_attempt(conn, "A", "2026-04-21T10:00:00Z", "no_documents")

    assert select_failure_code_uids(conn, ["no_documents"]) == {"A"}
    assert select_failure_code_uids(conn, ["get_failed"]) == set()


def test_select_failure_code_uids_empty_codes_returns_empty(conn):
    _seed_app(conn, "A")
    _record_attempt(conn, "A", "2026-04-21T10:00:00Z", "get_failed")

    assert select_failure_code_uids(conn, []) == set()
