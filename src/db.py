"""SQLite database setup and CRUD operations.

Field names aligned with planning.data.gov.uk schema where applicable.
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .config import DB_PATH

SCHEMA = """
-- Planning applications from PlanIt API
CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Core fields (aligned with planning.data.gov.uk)
    uid TEXT UNIQUE NOT NULL,
    reference TEXT,
    name TEXT,
    description TEXT,
    address_text TEXT,
    postcode TEXT,

    -- Authority
    authority_name TEXT,
    authority_id INTEGER,

    -- Location
    lat REAL,
    lng REAL,

    -- Classification (planning.data.gov.uk: planning-application-type)
    planning_application_type TEXT,
    app_size TEXT,

    -- Status & decision (planning.data.gov.uk: planning-decision)
    planning_application_status TEXT,
    planning_decision TEXT,

    -- Dates
    start_date TEXT,
    consulted_date TEXT,
    decision_date TEXT,

    -- Document access
    documentation_url TEXT,
    n_documents INTEGER,

    -- Portal classification
    portal_type TEXT,
    portal_base_url TEXT,

    -- Raw data
    other_fields_json TEXT,
    search_term TEXT,
    planit_link TEXT,

    -- Metadata
    scraped_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_applications_authority ON applications(authority_name);
CREATE INDEX IF NOT EXISTS idx_applications_type ON applications(planning_application_type);
CREATE INDEX IF NOT EXISTS idx_applications_status ON applications(planning_application_status);
CREATE INDEX IF NOT EXISTS idx_applications_portal ON applications(portal_type);
CREATE INDEX IF NOT EXISTS idx_applications_start_date ON applications(start_date);


-- Document metadata scraped from council portals
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Link to application
    application_uid TEXT NOT NULL REFERENCES applications(uid),

    -- Document fields (aligned with planning-application-document)
    document_type TEXT,
    description TEXT,
    document_url TEXT,
    documentation_url TEXT,

    -- Extra metadata from portal
    date_published TEXT,
    drawing_number TEXT,

    -- Download tracking
    file_path TEXT,
    file_size_bytes INTEGER,
    download_status TEXT DEFAULT 'pending',

    -- Metadata
    scraped_at TEXT NOT NULL,

    UNIQUE(application_uid, document_url)
);

CREATE INDEX IF NOT EXISTS idx_documents_app ON documents(application_uid);
CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(document_type);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(download_status);


-- Appeals data from Planning Inspectorate bulk download
CREATE TABLE IF NOT EXISTS appeals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    case_reference TEXT UNIQUE,
    site_address TEXT,
    lpa_name TEXT,
    appeal_type TEXT,
    decision_date TEXT,
    appeal_decision TEXT,
    inspector_name TEXT,
    appellant_name TEXT,
    agent_name TEXT,
    procedure_type TEXT,
    description TEXT,

    -- ASHP matching
    is_ashp_related INTEGER DEFAULT 0,
    ashp_match_method TEXT,
    linked_application_uid TEXT,

    -- Raw data
    raw_data_json TEXT,
    imported_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_appeals_ashp ON appeals(is_ashp_related);
CREATE INDEX IF NOT EXISTS idx_appeals_lpa ON appeals(lpa_name);


-- AI classification results
CREATE TABLE IF NOT EXISTS classifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    document_id INTEGER REFERENCES documents(id),
    application_uid TEXT REFERENCES applications(uid),

    classification_type TEXT,
    classification_result TEXT,
    model_used TEXT,
    confidence REAL,

    classified_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_classifications_doc ON classifications(document_id);
CREATE INDEX IF NOT EXISTS idx_classifications_type ON classifications(classification_type);


-- Authority portal mapping
CREATE TABLE IF NOT EXISTS authorities (
    area_id INTEGER PRIMARY KEY,
    area_name TEXT NOT NULL,
    portal_type TEXT,
    portal_base_url TEXT,
    scraper_type TEXT,
    is_scrapeable INTEGER DEFAULT 1,
    notes TEXT
);


-- Scraper run log
CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    source TEXT,
    params_json TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    records_processed INTEGER DEFAULT 0,
    records_new INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_log TEXT,
    status TEXT DEFAULT 'running'
);
"""


def get_db(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a database connection, creating schema if needed."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Context manager for database transactions."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def now_iso() -> str:
    """Current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def upsert_application(conn: sqlite3.Connection, data: dict) -> bool:
    """Insert or update an application. Returns True if new, False if updated."""
    existing = conn.execute(
        "SELECT id FROM applications WHERE uid = ?", (data["uid"],)
    ).fetchone()

    other_fields = data.get("other_fields") or {}
    if isinstance(other_fields, str):
        other_fields = json.loads(other_fields)

    values = {
        "uid": data["uid"],
        "reference": data.get("reference"),
        "name": data.get("name"),
        "description": data.get("description"),
        "address_text": data.get("address"),
        "postcode": data.get("postcode"),
        "authority_name": data.get("area_name"),
        "authority_id": data.get("area_id"),
        "lat": data.get("location_y"),
        "lng": data.get("location_x"),
        "planning_application_type": data.get("app_type"),
        "app_size": data.get("app_size"),
        "planning_application_status": data.get("app_state"),
        "planning_decision": other_fields.get("decision"),
        "start_date": data.get("start_date"),
        "consulted_date": data.get("consulted_date"),
        "decision_date": data.get("decided_date"),
        "documentation_url": other_fields.get("docs_url"),
        "n_documents": other_fields.get("n_documents"),
        "other_fields_json": json.dumps(other_fields) if other_fields else None,
        "search_term": data.get("_search_term"),
        "planit_link": data.get("link"),
        "scraped_at": now_iso(),
    }

    if existing:
        set_clause = ", ".join(f"{k} = :{k}" for k in values if k != "uid")
        conn.execute(
            f"UPDATE applications SET {set_clause} WHERE uid = :uid", values
        )
        return False
    else:
        cols = ", ".join(values.keys())
        placeholders = ", ".join(f":{k}" for k in values.keys())
        conn.execute(
            f"INSERT INTO applications ({cols}) VALUES ({placeholders})", values
        )
        return True


def upsert_document(conn: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a document record. Returns True if new."""
    existing = conn.execute(
        "SELECT id FROM documents WHERE application_uid = ? AND document_url = ?",
        (data["application_uid"], data["document_url"]),
    ).fetchone()

    values = {
        "application_uid": data["application_uid"],
        "document_type": data.get("document_type"),
        "description": data.get("description"),
        "document_url": data.get("document_url"),
        "documentation_url": data.get("documentation_url"),
        "date_published": data.get("date_published"),
        "drawing_number": data.get("drawing_number"),
        "scraped_at": now_iso(),
    }

    if existing:
        set_clause = ", ".join(
            f"{k} = :{k}" for k in values if k != "application_uid"
        )
        conn.execute(
            f"UPDATE documents SET {set_clause} "
            f"WHERE application_uid = :application_uid AND document_url = :document_url",
            values,
        )
        return False
    else:
        cols = ", ".join(values.keys())
        placeholders = ", ".join(f":{k}" for k in values.keys())
        conn.execute(
            f"INSERT INTO documents ({cols}) VALUES ({placeholders})", values
        )
        return True


def log_scrape_start(conn: sqlite3.Connection, stage: str, source: str, params: dict | None = None) -> int:
    """Log the start of a scrape run. Returns the log ID."""
    cursor = conn.execute(
        "INSERT INTO scrape_log (stage, source, params_json, started_at, status) "
        "VALUES (?, ?, ?, ?, 'running')",
        (stage, source, json.dumps(params) if params else None, now_iso()),
    )
    conn.commit()
    return cursor.lastrowid


def log_scrape_end(
    conn: sqlite3.Connection,
    log_id: int,
    *,
    records_processed: int = 0,
    records_new: int = 0,
    records_failed: int = 0,
    error_log: str | None = None,
    status: str = "completed",
):
    """Log the end of a scrape run."""
    conn.execute(
        "UPDATE scrape_log SET completed_at = ?, records_processed = ?, "
        "records_new = ?, records_failed = ?, error_log = ?, status = ? "
        "WHERE id = ?",
        (now_iso(), records_processed, records_new, records_failed, error_log, status, log_id),
    )
    conn.commit()


def get_application_count(conn: sqlite3.Connection) -> int:
    """Get total number of applications in the database."""
    return conn.execute("SELECT COUNT(*) FROM applications").fetchone()[0]


def get_applications_needing_docs(conn: sqlite3.Connection, portal_type: str = "idox") -> list[sqlite3.Row]:
    """Get applications that have a docs URL but no scraped documents yet."""
    return conn.execute(
        """
        SELECT a.uid, a.documentation_url, a.authority_name
        FROM applications a
        LEFT JOIN documents d ON a.uid = d.application_uid
        WHERE a.portal_type = ?
          AND a.documentation_url IS NOT NULL
          AND d.id IS NULL
        ORDER BY a.authority_name, a.start_date DESC
        """,
        (portal_type,),
    ).fetchall()
