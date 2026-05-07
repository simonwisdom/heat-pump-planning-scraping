import json
from pathlib import Path

import pytest

from src.generic_route_recovery import (
    _pick_herefordshire_application_id,
    build_herefordshire_detail_url,
)
from src.herefordshire_scraper import parse_herefordshire_documents

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def detail_html() -> str:
    return (FIXTURES_DIR / "herefordshire_detail_page.html").read_text(encoding="utf-8")


@pytest.fixture
def search_api() -> dict:
    return json.loads((FIXTURES_DIR / "herefordshire_search_api.json").read_text())


def test_search_api_response_yields_application_id(search_api: dict) -> None:
    app_id = _pick_herefordshire_application_id(search_api)
    assert app_id == "214393"


def test_detail_url_uses_application_id_and_reference() -> None:
    url = build_herefordshire_detail_url("214393", "P214393/L")
    assert url.startswith(
        "https://www.herefordshire.gov.uk/info/200142/planning_services/planning_application_search/details"
    )
    assert "id=214393" in url
    assert "search=P214393%2FL" in url


def test_parse_documents_finds_all_links(detail_html: str) -> None:
    page_url = build_herefordshire_detail_url("214393", "P214393/L")
    docs = parse_herefordshire_documents(detail_html, page_url)
    assert len(docs) == 18
    for doc in docs:
        assert doc["document_url"].startswith("https://myaccount.herefordshire.gov.uk/documents?id=")


def test_parse_strips_view_prefix_from_title(detail_html: str) -> None:
    page_url = build_herefordshire_detail_url("214393", "P214393/L")
    docs = parse_herefordshire_documents(detail_html, page_url)
    decision = next((d for d in docs if d["description"] == "Decision Notice"), None)
    assert decision is not None
    assert decision["document_type"] == "Decision Notice"


def test_parse_carries_file_size(detail_html: str) -> None:
    page_url = build_herefordshire_detail_url("214393", "P214393/L")
    docs = parse_herefordshire_documents(detail_html, page_url)
    appform = next((d for d in docs if d["description"] == "R AppForm"), None)
    assert appform is not None
    assert appform["file_size_text"] == "178KB"


def test_parse_dedupes_repeats() -> None:
    html = """
    <html><body>
      <div id="planning-application-documents">
        <ul>
          <li><a href="https://myaccount.herefordshire.gov.uk/documents?id=abc"
                 title="view Plan">Plan</a><span class="fileSize">10KB</span></li>
          <li><a href="https://myaccount.herefordshire.gov.uk/documents?id=abc"
                 title="view Plan (duplicate)">Plan</a></li>
        </ul>
      </div>
    </body></html>
    """
    docs = parse_herefordshire_documents(html, "https://www.herefordshire.gov.uk/details?id=1")
    assert len(docs) == 1


def test_parse_returns_empty_when_section_missing() -> None:
    html = "<html><body><div>No documents</div></body></html>"
    docs = parse_herefordshire_documents(html, "https://www.herefordshire.gov.uk/details?id=1")
    assert docs == []
