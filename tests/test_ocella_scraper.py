from pathlib import Path

import pytest

from src.ocella_scraper import parse_ocella_documents

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def hillingdon_html() -> str:
    return (FIXTURES_DIR / "hillingdon_ocella_documents.html").read_text(encoding="utf-8")


def test_hillingdon_parses_documents(hillingdon_html: str) -> None:
    page_url = "https://planning.hillingdon.gov.uk/OcellaWeb/showDocuments?module=pl&reference=16601%2FAPP%2F2021%2F412"
    docs = parse_ocella_documents(hillingdon_html, page_url)

    assert len(docs) >= 5
    for doc in docs:
        assert doc["document_url"].startswith("https://planning.hillingdon.gov.uk/OcellaWeb/viewDocument?file=")
        assert "module=pl" in doc["document_url"]


def test_hillingdon_assigns_section_headers(hillingdon_html: str) -> None:
    page_url = "https://planning.hillingdon.gov.uk/OcellaWeb/showDocuments?module=pl&reference=x"
    docs = parse_ocella_documents(hillingdon_html, page_url)

    sections = {d["section"] for d in docs}
    assert "Application Forms" in sections
    assert "Plans" in sections


def test_hillingdon_carries_metadata(hillingdon_html: str) -> None:
    page_url = "https://planning.hillingdon.gov.uk/OcellaWeb/showDocuments?module=pl&reference=x"
    docs = parse_ocella_documents(hillingdon_html, page_url)

    forms = [d for d in docs if d["section"] == "Application Forms"]
    assert forms, "expected Application Forms section"
    assert forms[0]["document_type"] == "APPLICATION FORM"
    assert forms[0]["date_published"]


def test_definition_list_layout_falls_back() -> None:
    """If a council's HTML uses h5/p instead of <table>, anchors still parse."""
    html = """
    <html><body>
      <h5>Application Forms</h5>
      <a href="viewDocument?file=dv_pl_files/abc.pdf&module=pl">APPLICATION FORM</a>
      <p>02-02-21</p>
      <hr>
      <h5>Plans</h5>
      <a href="viewDocument?file=dv_pl_files/plan.pdf&module=pl">LOCATION PLAN</a>
      <p>03-02-21 Site location</p>
    </body></html>
    """
    page_url = "https://example.gov.uk/OcellaWeb/showDocuments?reference=x&module=pl"
    docs = parse_ocella_documents(html, page_url)

    assert len(docs) == 2
    assert docs[0]["document_type"] == "APPLICATION FORM"
    assert docs[0]["section"] == "Application Forms"
    assert docs[0]["document_url"] == (
        "https://example.gov.uk/OcellaWeb/viewDocument?file=dv_pl_files/abc.pdf&module=pl"
    )
    assert docs[1]["section"] == "Plans"


def test_empty_listing_returns_no_documents() -> None:
    html = "<html><body><h1>No documents are available</h1></body></html>"
    docs = parse_ocella_documents(html, "https://example.gov.uk/OcellaWeb/showDocuments")
    assert docs == []


def test_dedupes_repeated_anchors() -> None:
    html = """
    <html><body>
      <a href="viewDocument?file=a.pdf&module=pl">FORM</a>
      <a href="viewDocument?file=a.pdf&module=pl">FORM (mirror)</a>
    </body></html>
    """
    docs = parse_ocella_documents(html, "https://example.gov.uk/OcellaWeb/showDocuments")
    assert len(docs) == 1
