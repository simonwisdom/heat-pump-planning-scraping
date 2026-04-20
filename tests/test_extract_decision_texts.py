import importlib.util
import sys
from pathlib import Path

from src.db import get_db, upsert_application, upsert_document

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "extract_decision_texts.py"


def load_module():
    spec = importlib.util.spec_from_file_location("extract_decision_texts", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def application_payload(uid: str) -> dict:
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
        "app_state": "Decided",
        "start_date": "2025-01-01",
        "consulted_date": "2025-01-05",
        "decided_date": "2025-02-01",
        "link": "https://planit.example/app",
        "_search_term": "ashp",
        "other_fields": {
            "decision": "Granted",
            "docs_url": f"https://docs.example/{uid}",
            "n_documents": 5,
        },
    }


def insert_downloaded_document(
    conn,
    *,
    application_uid: str,
    document_type: str,
    description: str,
    filename: str,
    download_status: str = "downloaded",
) -> None:
    doc_url = f"https://files.example/{application_uid}/{filename}"
    upsert_document(
        conn,
        {
            "application_uid": application_uid,
            "document_type": document_type,
            "description": description,
            "document_url": doc_url,
            "documentation_url": f"https://docs.example/{application_uid}",
        },
    )
    conn.execute(
        """
        UPDATE documents
        SET download_status = ?, file_path = ?
        WHERE application_uid = ? AND document_url = ?
        """,
        (
            download_status,
            f"/stale/root/pdfs/Example Council/{application_uid}/{filename}",
            application_uid,
            doc_url,
        ),
    )


def test_classify_document_family_keeps_first_pass_docs_and_skips_low_value_noise():
    module = load_module()

    assert (
        module.classify_document_family(
            "Decision Notice",
            "Detailed Planning Permission APPROVE",
            "Detailed_Planning_Permission_APPROVE-1.pdf",
        )
        == "decision"
    )
    assert (
        module.classify_document_family(
            "Report of Handling",
            "Report of Handling",
            "Report_of_Handling-1.pdf",
        )
        == "officer_report"
    )
    assert (
        module.classify_document_family(
            "Report",
            "OFFICER'S RECOMMENDATION",
            "OFFICERS_RECOMMENDATION-1.pdf",
        )
        == "officer_report"
    )
    assert (
        module.classify_document_family(
            "Report",
            "REPORT - HANDLING - FULL",
            "REPORT_-_HANDLING_-_FULL-1.pdf",
        )
        == "officer_report"
    )
    assert (
        module.classify_document_family(
            "Decision",
            "Recommendation and reasons report",
            "recommendation_and_reasons_report-1.pdf",
        )
        == "decision"
    )
    assert (
        module.classify_document_family(
            "Other",
            "MCS 020 Manual Sound Calculator",
            "MANUAL_SOUND_CALCULATOR-1.pdf",
        )
        == "noise"
    )
    assert (
        module.classify_document_family(
            "Other",
            "Environmental Health Noise Response",
            "ENVIRONMENTAL_HEALTH_RESPONSE-1.pdf",
        )
        == "consultee"
    )
    assert (
        module.classify_document_family(
            "Supporting Documents",
            "Heritage Statement",
            "HERITAGE_STATEMENT-1.pdf",
        )
        == "statement"
    )
    assert (
        module.classify_document_family(
            "Document",
            "Acoustic planning compliance report",
            "acoustic_planning_compliance_report.pdf",
        )
        == "noise"
    )
    assert (
        module.classify_document_family(
            "Supporting information",
            "Air source heat pump technical specification",
            "AIR_SOURCE_HEAT_PUMP_TECHNICAL_SPECIFICATION.pdf",
        )
        == "spec_calc"
    )
    assert (
        module.classify_document_family(
            "Drawing",
            "APPROVED - Site Plan",
            "APPROVED_SITE_PLAN-1.pdf",
        )
        is None
    )
    assert (
        module.classify_document_family(
            "Other",
            "Acoustic Trickle Vent Datasheet",
            "ACOUSTIC_TRICKLE_VENT_DATASHEET-1.pdf",
        )
        is None
    )


def test_load_candidates_filters_to_priority_families_and_resolves_pdf_root(tmp_path):
    module = load_module()
    db_path = tmp_path / "ashp.db"
    pdf_root = tmp_path / "pdfs"

    conn = get_db(db_path)
    try:
        upsert_application(conn, application_payload("app-1"))

        insert_downloaded_document(
            conn,
            application_uid="app-1",
            document_type="Decision Notice",
            description="Decision Notice",
            filename="DECISION_NOTICE-1.pdf",
        )
        insert_downloaded_document(
            conn,
            application_uid="app-1",
            document_type="Report of Handling",
            description="Report of Handling",
            filename="REPORT_OF_HANDLING-1.pdf",
        )
        insert_downloaded_document(
            conn,
            application_uid="app-1",
            document_type="Other",
            description="MCS 020 Manual Sound Calculator",
            filename="MANUAL_SOUND_CALCULATOR-1.pdf",
        )
        insert_downloaded_document(
            conn,
            application_uid="app-1",
            document_type="Other",
            description="Environmental Health Noise Response",
            filename="ENVIRONMENTAL_HEALTH_RESPONSE-1.pdf",
        )
        insert_downloaded_document(
            conn,
            application_uid="app-1",
            document_type="Drawing",
            description="APPROVED - Site Plan",
            filename="APPROVED_SITE_PLAN-1.pdf",
        )
        insert_downloaded_document(
            conn,
            application_uid="app-1",
            document_type="Decision Notice",
            description="Pending download",
            filename="DECISION_NOTICE_PENDING-1.pdf",
            download_status="pending",
        )
        conn.commit()
    finally:
        conn.close()

    decision_path = pdf_root / "Example Council" / "app-1" / "DECISION_NOTICE-1.pdf"
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    decision_path.write_bytes(b"%PDF-1.4 placeholder")

    candidates = module.load_candidates(db_path, ("decision", "officer_report", "noise"))

    assert [candidate.document_family for candidate in candidates] == [
        "decision",
        "officer_report",
        "noise",
    ]
    assert [candidate.relative_pdf_path for candidate in candidates] == [
        "Example Council/app-1/DECISION_NOTICE-1.pdf",
        "Example Council/app-1/REPORT_OF_HANDLING-1.pdf",
        "Example Council/app-1/MANUAL_SOUND_CALCULATOR-1.pdf",
    ]

    resolved = module.resolve_local_pdf_path(
        "/stale/root/pdfs/Example Council/app-1/DECISION_NOTICE-1.pdf",
        pdf_root=pdf_root,
    )
    assert resolved == decision_path


def test_build_text_relative_path_uses_md_for_docling():
    module = load_module()
    candidate = module.CandidateRow(
        document_id=1,
        application_uid="uid",
        reference="ref",
        authority_name="Council",
        planning_decision="",
        document_type="",
        description="",
        date_published="",
        file_path="pdfs/Council/ref/NOISE_REPORT.pdf",
        relative_pdf_path="Council/ref/NOISE_REPORT.pdf",
        document_family="noise",
    )
    assert module.build_text_relative_path(candidate, extractor="pymupdf").suffix == ".txt"
    assert module.build_text_relative_path(candidate, extractor="docling").suffix == ".md"


def test_existing_row_reuse_depends_on_extractor_signature(tmp_path):
    module = load_module()
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    text_path = output_dir / "texts" / "Example Council" / "app-1" / "DECISION_NOTICE-1.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text("decision notice", encoding="utf-8")

    row = {
        "status": "extracted",
        "text_path": "texts/Example Council/app-1/DECISION_NOTICE-1.txt",
        "extractor_signature": "pymupdf|none|40|noise,spec_calc",
    }

    assert module.existing_row_is_reusable(
        row,
        output_dir,
        extractor_run_signature="pymupdf|none|40|noise,spec_calc",
    )
    assert not module.existing_row_is_reusable(
        row,
        output_dir,
        extractor_run_signature="pdfplumber|none|40|noise,spec_calc",
    )
