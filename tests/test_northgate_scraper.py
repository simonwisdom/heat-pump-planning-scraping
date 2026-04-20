import json
from pathlib import Path

import pytest

from src.northgate_scraper import (
    _detect_extension,
    _handler_for_url,
    extract_viewstate_fields,
    extract_wandsworth_doctypes,
    parse_camden_documents,
    parse_conwy_documents,
    parse_conwy_key_number,
    parse_publicaccess_documents,
    parse_wandsworth_postback,
    rewrite_legacy_url,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def camden_html() -> str:
    return (FIXTURES_DIR / "camden_northgate_landing.html").read_text(encoding="utf-8")


@pytest.fixture
def wandsworth_landing_html() -> str:
    return (FIXTURES_DIR / "wandsworth_northgate_landing.html").read_text(encoding="utf-8")


@pytest.fixture
def wandsworth_postback_html() -> str:
    return (FIXTURES_DIR / "wandsworth_northgate_postback_drawing.html").read_text(encoding="utf-8")


# ---------- Handler dispatch ----------


def test_handler_dispatch():
    assert _handler_for_url("https://camdocs.camden.gov.uk/CMWebDrawer/PlanRec?q=x") == "camden"
    assert (
        _handler_for_url('http://camdocs.camden.gov.uk/HPRMWebDrawer/PlanRec?q=recContainer:"2021/5895/P"') == "camden"
    )
    assert (
        _handler_for_url("https://planning2.wandsworth.gov.uk/planningcase/comments.aspx?case=2025/1361")
        == "wandsworth"
    )
    # Blackburn & Runnymede both use Idox PublicAccess_LIVE
    assert (
        _handler_for_url("http://planningdms-live.blackburn.gov.uk/Publicaccess_LIVE/ExternalEntryPoint.aspx?x=1")
        == "publicaccess"
    )
    assert (
        _handler_for_url("https://docs.runnymede.gov.uk/Publicaccess_LIVE/ExternalEntryPoint.aspx?x=1")
        == "publicaccess"
    )
    # Legacy Runnymede host also routes to publicaccess (rewritten before fetch)
    assert _handler_for_url("http://documents.runnymede.gov.uk/AniteIM.WebSearch/") == "publicaccess"
    # Conwy: new Civica EDM host + retired hosts all route to "conwy"
    assert _handler_for_url("https://edm.secure.conwy.gov.uk/planning/planning-documents?ref_no=0/49220") == "conwy"
    assert (
        _handler_for_url(
            "https://www.conwy.gov.uk/en/Resident/Planning-Building-Control-and-Conservation/Planning-Applications/Planning-Explorer-Docs-EDM.aspx?ref_no=0%2F49220"
        )
        == "conwy"
    )
    assert _handler_for_url("https://edm.conwy.gov.uk/Planning/lg/dialog.page?ref_no=0%2F41790") == "conwy"


# ---------- Legacy URL rewriting ----------


def test_rewrite_legacy_runnymede():
    old = (
        "http://documents.runnymede.gov.uk/AniteIM.WebSearch/"
        "ExternalEntryPoint.aspx?SEARCH_TYPE=1&DOC_CLASS_CODE=PL&FOLDER1_REF=PLN201895"
    )
    new = rewrite_legacy_url(old)
    assert new == (
        "https://docs.runnymede.gov.uk/Publicaccess_LIVE/"
        "ExternalEntryPoint.aspx?SEARCH_TYPE=1&DOC_CLASS_CODE=PL&FOLDER1_REF=PLN201895"
    )


def test_rewrite_legacy_passthrough():
    # Non-legacy URLs must pass through unchanged
    for url in [
        "https://docs.runnymede.gov.uk/Publicaccess_LIVE/ExternalEntryPoint.aspx?x=1",
        "http://planningdms-live.blackburn.gov.uk/Publicaccess_LIVE/x",
        "https://edm.secure.conwy.gov.uk/planning/planning-documents?ref_no=0/49220&viewdocs=true",
    ]:
        assert rewrite_legacy_url(url) == url


@pytest.mark.parametrize(
    "url, ref",
    [
        # www.conwy.gov.uk CMS wrapper
        (
            "https://www.conwy.gov.uk/en/Resident/Planning-Building-Control-and-Conservation/"
            "Planning-Applications/Planning-Explorer-Docs-EDM.aspx?ref_no=0%2F49220",
            "0/49220",
        ),
        # Retired edm.conwy.gov.uk dialog.page
        (
            "https://edm.conwy.gov.uk/Planning/lg/dialog.page?viewdocs=true&ref_no=0%2F41790",
            "0/41790",
        ),
    ],
)
def test_rewrite_legacy_conwy_migrates_to_new_host(url, ref):
    assert rewrite_legacy_url(url) == (
        f"https://edm.secure.conwy.gov.uk/planning/planning-documents?ref_no={ref}&viewdocs=true"
    )


# ---------- Conwy (Civica EDM) parsing ----------


@pytest.fixture
def conwy_pagedsearch() -> dict:
    return json.loads((FIXTURES_DIR / "conwy_keyobject_pagedsearch.json").read_text(encoding="utf-8"))


@pytest.fixture
def conwy_pagedsearch_empty() -> dict:
    return json.loads((FIXTURES_DIR / "conwy_keyobject_empty.json").read_text(encoding="utf-8"))


@pytest.fixture
def conwy_doc_list() -> dict:
    return json.loads((FIXTURES_DIR / "conwy_doc_list.json").read_text(encoding="utf-8"))


def test_conwy_key_number_from_search(conwy_pagedsearch):
    assert parse_conwy_key_number(conwy_pagedsearch) == "60775"


def test_conwy_key_number_missing(conwy_pagedsearch_empty):
    assert parse_conwy_key_number(conwy_pagedsearch_empty) is None
    assert parse_conwy_key_number({}) is None


def test_parse_conwy_documents(conwy_doc_list):
    docs = parse_conwy_documents(conwy_doc_list)
    assert len(docs) == 34
    first = docs[0]
    # Sanity-check shape
    assert first["document_url"].startswith(
        "https://edm.secure.conwy.gov.uk/w2webparts/Resource/Civica/Handler.ashx/doc/pagestream?DocNo="
    )
    assert first["document_url"].endswith("&pdf=true")
    assert first["document_type"]  # from DocDesc
    assert first["date_published"].count("-") == 2  # YYYY-MM-DD
    # Fall back to DocDesc if Title missing — all should have some description
    for d in docs:
        assert d["description"]


def test_parse_conwy_documents_empty():
    assert parse_conwy_documents({"CompleteDocument": []}) == []
    assert parse_conwy_documents({}) == []


# ---------- PublicAccess (Blackburn / Runnymede) parsing ----------


@pytest.fixture
def blackburn_html() -> str:
    return (FIXTURES_DIR / "blackburn_publicaccess_landing.html").read_text(encoding="utf-8")


@pytest.fixture
def runnymede_rows_html() -> str:
    return (FIXTURES_DIR / "runnymede_publicaccess_rows.html").read_text(encoding="utf-8")


def test_parse_publicaccess_blackburn(blackburn_html):
    docs = parse_publicaccess_documents(blackburn_html, "http://planningdms-live.blackburn.gov.uk")
    assert len(docs) == 10
    guids = [d["document_url"].split("id=")[1] for d in docs]
    assert "BE30A778BD694FEE9E8FA26C46A55669" in guids
    first = docs[0]
    assert first["document_type"] == "Decision notice"
    assert first["description"] == "9. DECISION NOTICE PERMITS"
    assert first["date_published"].startswith("10/22/2021")
    assert first["document_url"].startswith(
        "http://planningdms-live.blackburn.gov.uk/PublicAccess_LIVE/Document/ViewDocument?id="
    )


def test_parse_publicaccess_runnymede_preserves_path_case(runnymede_rows_html):
    # Runnymede uses /PublicAccess_Live/ (mixed case) where Blackburn uses
    # /PublicAccess_LIVE/ — must be read from the page, not hard-coded.
    docs = parse_publicaccess_documents(runnymede_rows_html, "https://docs.runnymede.gov.uk")
    assert docs
    for d in docs:
        assert d["document_url"].startswith("https://docs.runnymede.gov.uk/PublicAccess_Live/Document/ViewDocument?id=")


def test_parse_publicaccess_no_model_returns_empty():
    assert parse_publicaccess_documents("<html>no model here</html>", "https://x") == []


# ---------- Camden parsing ----------


def test_parse_camden(camden_html):
    docs = parse_camden_documents(camden_html, "https://camdocs.camden.gov.uk")
    # Fixture has 24 records; the parser must dedupe by record id.
    assert len(docs) == 24
    assert len({d["document_url"] for d in docs}) == 24

    # First row: Decision Notice from 31/03/2022
    first = docs[0]
    assert first["document_type"] == "Decision Notice"
    assert "Decision Notice" in first["description"]
    assert first["date_published"].startswith("31/03/2022")

    for d in docs:
        assert d["document_url"].startswith("https://camdocs.camden.gov.uk/CMWebDrawer/Record/")
        assert "/file/document" in d["document_url"]

    assert {"Decision Notice", "Revised Drawing", "Supporting Documents"} <= {d["document_type"] for d in docs}


# ---------- Wandsworth parsing ----------


def test_extract_viewstate_fields(wandsworth_landing_html):
    fields = extract_viewstate_fields(wandsworth_landing_html)
    assert fields["__VIEWSTATE"], "VIEWSTATE should be populated"
    assert fields["__VIEWSTATEGENERATOR"], "VIEWSTATEGENERATOR should be populated"
    assert fields["__EVENTVALIDATION"], "EVENTVALIDATION should be populated"


def test_extract_wandsworth_doctypes(wandsworth_landing_html):
    types = extract_wandsworth_doctypes(wandsworth_landing_html)
    names = [t[0] for t in types]
    assert names == [
        "Application Form",
        "Committee Report",
        "Decision Notice",
        "Drawing",
        "Report",
    ]
    targets = [t[1] for t in types]
    # Targets follow the ctl02..ctl06 convention
    assert targets[0] == "gvDocs$ctl02$lnkDShow"
    assert targets[-1] == "gvDocs$ctl06$lnkDShow"


def test_parse_wandsworth_postback_drawing(wandsworth_postback_html):
    docs = parse_wandsworth_postback(wandsworth_postback_html, "Drawing")
    # Drawing group for 2025/1361 has 16 docs per UI
    assert len(docs) == 16
    for d in docs:
        assert d["document_type"] == "Drawing"
        assert "IAMLink.aspx?docid=" in d["document_url"]
        assert d["date_published"]
        assert d["description"]


def test_parse_wandsworth_description_content(wandsworth_postback_html):
    docs = parse_wandsworth_postback(wandsworth_postback_html, "Drawing")
    descriptions = {d["description"] for d in docs}
    assert "DEMOLITION SET" in descriptions


# ---------- Extension detection ----------


def test_detect_extension_pdf_magic():
    assert _detect_extension(b"%PDF-1.5\n...", "application/pdf") == ".pdf"
    # magic takes priority over a (wrong) content-type
    assert _detect_extension(b"%PDF-1.5\n...", "application/octet-stream") == ".pdf"


def test_detect_extension_docx_via_content_type():
    zip_magic = b"PK\x03\x04rest..."
    ct = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    assert _detect_extension(zip_magic, ct) == ".docx"


def test_detect_extension_legacy_doc_magic():
    ole2 = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1rest..."
    assert _detect_extension(ole2, "application/msword") == ".doc"


def test_detect_extension_tiff():
    assert _detect_extension(b"II*\x00...", "image/tiff") == ".tif"
    assert _detect_extension(b"MM\x00*...", "image/tiff") == ".tif"


def test_detect_extension_fallback_to_pdf():
    assert _detect_extension(b"\x00\x00\x00\x00", "") == ".pdf"
