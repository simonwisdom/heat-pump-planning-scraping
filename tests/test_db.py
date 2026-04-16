from pathlib import Path

import pytest

from src.db import (
    get_application_years,
    get_applications_needing_docs,
    get_applications_needing_download,
    get_db,
    get_resume_start_year,
    mark_documents_downloaded,
    upsert_application,
    upsert_document,
)


@pytest.fixture
def conn():
    connection = get_db(Path(":memory:"))
    try:
        yield connection
    finally:
        connection.close()


def _application_payload(uid: str, *, docs_url: str | None = None) -> dict:
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
            "n_documents": 2 if docs_url else 0,
        },
    }


def test_schema_creation_with_in_memory_sqlite(conn):
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

    assert {
        "applications",
        "documents",
        "appeals",
        "classifications",
        "authorities",
        "scrape_log",
    }.issubset(tables)


def test_upsert_application_insert_and_update(conn):
    created = upsert_application(conn, _application_payload("app-1", docs_url="https://docs/1"))
    assert created is True

    updated_payload = _application_payload("app-1", docs_url="https://docs/updated")
    updated_payload["description"] = "Updated description"

    created_again = upsert_application(conn, updated_payload)
    assert created_again is False

    row = conn.execute(
        "SELECT description, documentation_url FROM applications WHERE uid = ?",
        ("app-1",),
    ).fetchone()

    assert row["description"] == "Updated description"
    assert row["documentation_url"] == "https://docs/updated"


def test_upsert_document_insert_and_update(conn):
    upsert_application(conn, _application_payload("app-doc", docs_url="https://docs/source"))

    created = upsert_document(
        conn,
        {
            "application_uid": "app-doc",
            "document_type": "Decision Notice",
            "description": "Initial",
            "document_url": "https://files/doc-1.pdf",
            "documentation_url": "https://docs/source",
            "date_published": "2025-01-10",
            "drawing_number": "DWG-1",
        },
    )
    assert created is True

    created_again = upsert_document(
        conn,
        {
            "application_uid": "app-doc",
            "document_type": "Decision Notice",
            "description": "Updated description",
            "document_url": "https://files/doc-1.pdf",
            "documentation_url": "https://docs/source",
            "date_published": "2025-01-11",
            "drawing_number": "DWG-2",
        },
    )
    assert created_again is False

    row = conn.execute(
        "SELECT description, date_published, drawing_number FROM documents "
        "WHERE application_uid = ? AND document_url = ?",
        ("app-doc", "https://files/doc-1.pdf"),
    ).fetchone()

    assert row["description"] == "Updated description"
    assert row["date_published"] == "2025-01-11"
    assert row["drawing_number"] == "DWG-2"


def test_upsert_document_requires_document_url(conn):
    upsert_application(
        conn,
        _application_payload("app-doc-missing-url", docs_url="https://docs/source"),
    )

    with pytest.raises(ValueError, match="document_url is required"):
        upsert_document(
            conn,
            {
                "application_uid": "app-doc-missing-url",
                "document_type": "Decision Notice",
                "description": "Missing URL",
                "document_url": None,
                "documentation_url": "https://docs/source",
            },
        )


def test_deduplication_by_uid(conn):
    assert upsert_application(conn, _application_payload("dedup-1")) is True
    assert upsert_application(conn, _application_payload("dedup-1")) is False

    count = conn.execute(
        "SELECT COUNT(*) FROM applications WHERE uid = ?",
        ("dedup-1",),
    ).fetchone()[0]

    assert count == 1


def test_get_applications_needing_docs_query(conn):
    upsert_application(conn, _application_payload("needs-docs", docs_url="https://docs/needs"))
    upsert_application(conn, _application_payload("has-doc", docs_url="https://docs/has"))
    upsert_application(conn, _application_payload("no-doc-url", docs_url=None))
    upsert_application(conn, _application_payload("wrong-portal", docs_url="https://docs/wrong"))

    conn.execute("UPDATE applications SET portal_type = 'idox' WHERE uid IN ('needs-docs', 'has-doc', 'no-doc-url')")
    conn.execute("UPDATE applications SET portal_type = 'northgate' WHERE uid = 'wrong-portal'")

    upsert_document(
        conn,
        {
            "application_uid": "has-doc",
            "document_type": "Report",
            "description": "Already scraped",
            "document_url": "https://files/has-doc.pdf",
            "documentation_url": "https://docs/has",
        },
    )

    rows = get_applications_needing_docs(conn, portal_type="idox")

    assert [row["uid"] for row in rows] == ["needs-docs"]
    assert rows[0]["documentation_url"] == "https://docs/needs"
    assert rows[0]["authority_name"] == "Example Council"


def test_get_application_years_returns_sorted_distinct_years(conn):
    upsert_application(conn, _application_payload("year-2025-a"))
    payload_2024 = _application_payload("year-2024", docs_url="https://docs/2024")
    payload_2024["start_date"] = "2024-03-15"
    upsert_application(conn, payload_2024)
    payload_2025_b = _application_payload("year-2025-b", docs_url="https://docs/2025")
    payload_2025_b["start_date"] = "2025-06-20"
    upsert_application(conn, payload_2025_b)

    assert get_application_years(conn) == [2024, 2025]


def test_get_resume_start_year_returns_first_missing_year(conn):
    payload_2015 = _application_payload("resume-2015", docs_url="https://docs/2015")
    payload_2015["start_date"] = "2015-01-01"
    upsert_application(conn, payload_2015)

    payload_2016 = _application_payload("resume-2016", docs_url="https://docs/2016")
    payload_2016["start_date"] = "2016-01-01"
    upsert_application(conn, payload_2016)

    payload_2018 = _application_payload("resume-2018", docs_url="https://docs/2018")
    payload_2018["start_date"] = "2018-01-01"
    upsert_application(conn, payload_2018)

    assert get_resume_start_year(conn, min_year=2015, max_year=2020) == 2017


def test_get_resume_start_year_returns_none_when_range_is_filled(conn):
    for year in range(2015, 2018):
        payload = _application_payload(f"resume-complete-{year}", docs_url=f"https://docs/{year}")
        payload["start_date"] = f"{year}-01-01"
        upsert_application(conn, payload)

    assert get_resume_start_year(conn, min_year=2015, max_year=2017) is None


def test_get_applications_needing_download(conn):
    # App with pending documents
    upsert_application(conn, _application_payload("dl-pending", docs_url="https://docs/pending"))
    conn.execute("UPDATE applications SET portal_type = 'idox' WHERE uid = 'dl-pending'")
    upsert_document(
        conn,
        {
            "application_uid": "dl-pending",
            "document_type": "Decision Notice",
            "description": "DN",
            "document_url": "https://files/pending.pdf",
            "documentation_url": "https://docs/pending",
        },
    )

    # App with no documents listed yet (should also be included)
    upsert_application(conn, _application_payload("dl-no-docs", docs_url="https://docs/nodocs"))
    conn.execute("UPDATE applications SET portal_type = 'idox' WHERE uid = 'dl-no-docs'")

    # App with already-downloaded documents
    upsert_application(conn, _application_payload("dl-done", docs_url="https://docs/done"))
    conn.execute("UPDATE applications SET portal_type = 'idox' WHERE uid = 'dl-done'")
    upsert_document(
        conn,
        {
            "application_uid": "dl-done",
            "document_type": "Report",
            "description": "Report",
            "document_url": "https://files/done.pdf",
            "documentation_url": "https://docs/done",
        },
    )
    conn.execute("UPDATE documents SET download_status = 'downloaded' WHERE application_uid = 'dl-done'")

    rows = get_applications_needing_download(conn, portal_type="idox")
    uids = [row["uid"] for row in rows]
    assert "dl-pending" in uids
    assert "dl-no-docs" in uids
    assert "dl-done" not in uids


def test_get_applications_needing_download_authority_filter(conn):
    upsert_application(conn, _application_payload("dl-filter", docs_url="https://docs/f"))
    conn.execute("UPDATE applications SET portal_type = 'idox' WHERE uid = 'dl-filter'")
    upsert_document(
        conn,
        {
            "application_uid": "dl-filter",
            "document_type": "Decision",
            "description": "DN",
            "document_url": "https://files/filter.pdf",
            "documentation_url": "https://docs/f",
        },
    )

    rows = get_applications_needing_download(conn, portal_type="idox", authority="Example Council")
    assert len(rows) == 1

    rows = get_applications_needing_download(conn, portal_type="idox", authority="Nonexistent")
    assert len(rows) == 0


def test_mark_documents_downloaded(conn):
    upsert_application(conn, _application_payload("dl-mark", docs_url="https://docs/mark"))
    upsert_document(
        conn,
        {
            "application_uid": "dl-mark",
            "document_type": "Decision Notice",
            "description": "DN",
            "document_url": "https://files/mark.pdf",
            "documentation_url": "https://docs/mark",
        },
    )

    updated = mark_documents_downloaded(
        conn,
        "dl-mark",
        {
            "https://files/mark.pdf": ("/path/to/mark.pdf", 12345),
        },
    )
    assert updated == 1

    row = conn.execute(
        "SELECT download_status, file_path, file_size_bytes FROM documents WHERE application_uid = 'dl-mark'",
    ).fetchone()
    assert row["download_status"] == "downloaded"
    assert row["file_path"] == "/path/to/mark.pdf"
    assert row["file_size_bytes"] == 12345
