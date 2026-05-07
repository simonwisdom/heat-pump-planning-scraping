from pathlib import Path

import pytest

from src.elmbridge_scraper import (
    documents_url_for_reference,
    parse_elmbridge_documents,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def documents_html() -> str:
    return (FIXTURES_DIR / "elmbridge_emaps_documents.html").read_text(encoding="utf-8")


def test_documents_url_for_reference_uses_emaps_path() -> None:
    url = documents_url_for_reference("2021/2264")
    assert url.startswith("https://emaps.elmbridge.gov.uk/ebc_planning.aspx")
    assert "PlanningPlansAndDocsTab" in url
    assert "2021%2F2264" in url


def test_parse_elmbridge_documents_returns_iam_links(documents_html: str) -> None:
    page_url = documents_url_for_reference("2021/2264")
    docs = parse_elmbridge_documents(documents_html, page_url)

    assert len(docs) == 7
    for doc in docs:
        assert doc["document_url"].startswith("https://edocs.elmbridge.gov.uk/IAM/IAMLink.aspx?docid=")
        assert doc["document_url"].count("docid=") == 1


def test_parse_extracts_metadata_from_row(documents_html: str) -> None:
    page_url = documents_url_for_reference("2021/2264")
    docs = parse_elmbridge_documents(documents_html, page_url)
    decision = next((d for d in docs if d["document_type"] == "Decision"), None)
    assert decision is not None
    assert decision["date_published"] == "13/10/2021"
    assert decision["description"].startswith("Decision")
    assert decision["_hint_ext"] == ".pdf"


def test_parse_extracts_filename_from_title(documents_html: str) -> None:
    page_url = documents_url_for_reference("2021/2264")
    docs = parse_elmbridge_documents(documents_html, page_url)
    plans = [d for d in docs if "Proposed" in d["description"]]
    assert plans, "expected proposed plans documents"
    assert all(d["_hint_ext"] == ".pdf" for d in plans)


def test_parse_dedupes_repeated_anchors() -> None:
    html = """
    <html><body>
      <table>
        <tr><td>Decision</td><td>01/01/2024</td>
            <td><a title="View or download 'Decision-1.pdf'"
                   href="//edocs.elmbridge.gov.uk/IAM/IAMLink.aspx?docid=1">View</a></td></tr>
        <tr><td>Decision</td><td>01/01/2024</td>
            <td><a title="View or download 'Decision-1.pdf'"
                   href="//edocs.elmbridge.gov.uk/IAM/IAMLink.aspx?docid=1">View</a></td></tr>
      </table>
    </body></html>
    """
    docs = parse_elmbridge_documents(html, "https://emaps.elmbridge.gov.uk/ebc_planning.aspx?ref=x")
    assert len(docs) == 1


def test_parse_handles_anchor_without_row() -> None:
    html = """
    <html><body>
      <h5>Plans</h5>
      <a title="View or download 'Plan-99.pdf'"
         href="//edocs.elmbridge.gov.uk/IAM/IAMLink.aspx?docid=99">View</a>
    </body></html>
    """
    docs = parse_elmbridge_documents(html, "https://emaps.elmbridge.gov.uk/ebc_planning.aspx?ref=x")
    assert len(docs) == 1
    assert docs[0]["description"] == "Plan"
    assert docs[0]["document_type"] == "Plan"
    assert docs[0]["_hint_ext"] == ".pdf"
