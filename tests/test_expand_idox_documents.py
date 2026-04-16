import argparse
import asyncio
import csv
import importlib.util
from pathlib import Path

import pytest

from src.db import get_db, upsert_application

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "scrape_document_listings.py"
spec = importlib.util.spec_from_file_location("expand_idox_documents", SCRIPT_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)
load_candidates = module.load_candidates
run_scrape = module.run_scrape
select_applications = module.select_applications


class FakeRow(dict):
    def __getattr__(self, item):
        return self[item]


def _application_payload(uid: str, *, docs_url: str) -> dict:
    return {
        "uid": uid,
        "reference": f"REF-{uid}",
        "name": "ASHP install",
        "description": "Install air source heat pump",
        "address": "1 Example Street",
        "postcode": "AB1 2CD",
        "area_name": "Example Council",
        "area_id": 42,
        "location_y": 51.5,
        "location_x": -0.12,
        "app_type": "Full",
        "app_size": "Small",
        "app_state": "Undecided",
        "start_date": "2025-01-01",
        "consulted_date": "2025-01-05",
        "decided_date": "2025-02-01",
        "link": "https://planit.example/app",
        "_search_term": "ashp",
        "other_fields": {
            "decision": "Pending",
            "docs_url": docs_url,
            "n_documents": 2,
        },
    }


def _seed_idox_applications(db_path: Path, *apps: tuple[str, str]) -> None:
    conn = get_db(db_path)
    try:
        for uid, docs_url in apps:
            upsert_application(conn, _application_payload(uid, docs_url=docs_url))
            conn.execute(
                "UPDATE applications SET portal_type = 'idox' WHERE uid = ?",
                (uid,),
            )
        conn.commit()
    finally:
        conn.close()


def _make_fake_scraper(documents_by_url: dict[str, list[dict] | Exception]):
    class FakeScraper:
        def __init__(self):
            self.stats = {
                "success": 0,
                "failed": 0,
                "no_docs": 0,
                "captcha_blocked": 0,
                "tls_retry_used": 0,
            }

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def scrape_documents(self, docs_url: str) -> list[dict]:
            result = documents_by_url[docs_url]
            if isinstance(result, Exception):
                raise result
            if result:
                self.stats["success"] += 1
            else:
                self.stats["no_docs"] += 1
            return result

    return FakeScraper


def test_select_applications_respects_authority_cap():
    rows = [
        FakeRow(
            uid="a1",
            authority_name="Alpha",
            documentation_url="u1",
            start_date="2026-01-01",
        ),
        FakeRow(
            uid="a2",
            authority_name="Alpha",
            documentation_url="u2",
            start_date="2026-01-02",
        ),
        FakeRow(
            uid="a3",
            authority_name="Alpha",
            documentation_url="u3",
            start_date="2026-01-03",
        ),
        FakeRow(
            uid="b1",
            authority_name="Beta",
            documentation_url="u4",
            start_date="2026-01-01",
        ),
        FakeRow(
            uid="b2",
            authority_name="Beta",
            documentation_url="u5",
            start_date="2026-01-02",
        ),
        FakeRow(
            uid="c1",
            authority_name="Gamma",
            documentation_url="u6",
            start_date="2026-01-01",
        ),
    ]

    selected = select_applications(rows, sample_apps=5, max_per_authority=2, seed=42)

    assert len(selected) == 5
    counts = {}
    for row in selected:
        counts[row["authority_name"]] = counts.get(row["authority_name"], 0) + 1

    assert counts["Alpha"] <= 2
    assert counts["Beta"] <= 2
    assert counts["Gamma"] <= 2


def test_load_candidates_ignores_invalid_document_rows(tmp_path):
    db_path = tmp_path / "applications.sqlite"
    _seed_idox_applications(
        db_path,
        ("candidate-no-docs", "https://example.gov.uk/docs/no-docs"),
        ("candidate-invalid-doc", "https://example.gov.uk/docs/invalid"),
    )

    conn = get_db(db_path)
    try:
        conn.execute(
            """
            INSERT INTO documents (
                application_uid,
                document_url,
                documentation_url,
                scraped_at
            ) VALUES (?, ?, ?, ?)
            """,
            (
                "candidate-invalid-doc",
                None,
                "https://example.gov.uk/docs/invalid",
                "2026-04-16T00:00:00+00:00",
            ),
        )
        conn.commit()

        rows = load_candidates(conn)
    finally:
        conn.close()

    assert {row["uid"] for row in rows} == {
        "candidate-no-docs",
        "candidate-invalid-doc",
    }


def test_run_scrape_skips_missing_document_urls_and_logs_completion(monkeypatch, tmp_path):
    db_path = tmp_path / "applications.sqlite"
    output_dir = tmp_path / "output"
    _seed_idox_applications(
        db_path,
        ("app-with-docs", "https://example.gov.uk/docs/with-docs"),
        ("app-no-docs", "https://example.gov.uk/docs/no-docs"),
    )

    monkeypatch.setattr(
        module,
        "IdoxDocumentScraper",
        _make_fake_scraper(
            {
                "https://example.gov.uk/docs/with-docs": [
                    {
                        "document_type": "Decision Notice",
                        "description": "Valid document",
                        "document_url": "https://example.gov.uk/files/doc-1.pdf",
                        "date_published": "2026-04-01",
                        "drawing_number": "",
                    },
                    {
                        "document_type": "Decision Notice",
                        "description": "Missing URL",
                        "document_url": None,
                        "date_published": "2026-04-01",
                        "drawing_number": "",
                    },
                ],
                "https://example.gov.uk/docs/no-docs": [],
            }
        ),
    )

    args = argparse.Namespace(
        db_path=db_path,
        sample_apps=10,
        max_per_authority=4,
        seed=42,
        output_dir=output_dir,
    )

    assert asyncio.run(run_scrape(args)) == 0

    conn = get_db(db_path)
    try:
        docs_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        log_row = conn.execute(
            "SELECT status, records_processed, records_new, records_failed, error_log "
            "FROM scrape_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert docs_count == 1
    assert log_row["status"] == "completed"
    assert log_row["records_processed"] == 2
    assert log_row["records_new"] == 1
    assert log_row["records_failed"] == 0
    assert log_row["error_log"] is None

    with (output_dir / "idox_document_expansion_results.csv").open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    rows_by_uid = {row["uid"]: row for row in rows}
    assert rows_by_uid["app-with-docs"]["documents_found"] == "2"
    assert rows_by_uid["app-with-docs"]["documents_inserted"] == "1"
    assert rows_by_uid["app-with-docs"]["documents_skipped_missing_url"] == "1"
    assert rows_by_uid["app-with-docs"]["status"] == "success"
    assert rows_by_uid["app-no-docs"]["documents_found"] == "0"
    assert rows_by_uid["app-no-docs"]["status"] == "no_docs_or_failed"

    with (output_dir / "idox_document_expansion_summary.csv").open(encoding="utf-8", newline="") as fh:
        summary = next(csv.DictReader(fh))

    assert summary["applications_selected"] == "2"
    assert summary["applications_processed"] == "2"
    assert summary["new_documents_inserted"] == "1"
    assert summary["documents_skipped_missing_url"] == "1"
    assert summary["task_errors"] == "0"


def test_run_scrape_logs_failure_on_unhandled_error(monkeypatch, tmp_path):
    db_path = tmp_path / "applications.sqlite"
    output_dir = tmp_path / "output"
    _seed_idox_applications(
        db_path,
        ("app-crashes-on-insert", "https://example.gov.uk/docs/crash"),
    )

    monkeypatch.setattr(
        module,
        "IdoxDocumentScraper",
        _make_fake_scraper(
            {
                "https://example.gov.uk/docs/crash": [
                    {
                        "document_type": "Decision Notice",
                        "description": "Valid document",
                        "document_url": "https://example.gov.uk/files/doc-1.pdf",
                        "date_published": "2026-04-01",
                        "drawing_number": "",
                    }
                ]
            }
        ),
    )
    monkeypatch.setattr(
        module,
        "upsert_document",
        lambda conn, data: (_ for _ in ()).throw(RuntimeError("db broken")),
    )

    args = argparse.Namespace(
        db_path=db_path,
        sample_apps=10,
        max_per_authority=4,
        seed=42,
        output_dir=output_dir,
    )

    with pytest.raises(RuntimeError, match="db broken"):
        asyncio.run(run_scrape(args))

    conn = get_db(db_path)
    try:
        log_row = conn.execute(
            "SELECT status, completed_at, records_processed, records_new, error_log "
            "FROM scrape_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    assert log_row["status"] == "failed"
    assert log_row["completed_at"] is not None
    assert log_row["records_processed"] == 0
    assert log_row["records_new"] == 0
    assert log_row["error_log"] == "RuntimeError: db broken"
