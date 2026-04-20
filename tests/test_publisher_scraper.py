import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from src.publisher_scraper import (
    PublisherDocumentScraper,
    extract_ajax_url,
    parse_publisher_documents,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def birmingham_json() -> dict:
    return json.loads((FIXTURES_DIR / "birmingham_publisher_documents.json").read_text())


@pytest.fixture
def birmingham_html() -> str:
    return (FIXTURES_DIR / "birmingham_publisher_page.html").read_text(encoding="utf-8")


def test_parse_birmingham_fixture(birmingham_json):
    base_url = "http://eplanning.idox.birmingham.gov.uk"
    docs = parse_publisher_documents(birmingham_json, base_url)

    assert len(docs) == 28

    # Check a decision notice exists
    decision_notices = [d for d in docs if d["document_type"] == "Decision Notice"]
    assert len(decision_notices) >= 1
    dn = decision_notices[0]
    assert dn["document_url"].startswith("http://eplanning.idox.birmingham.gov.uk/publisher/docs/")
    assert dn["date_published"]


def test_parse_document_types(birmingham_json):
    docs = parse_publisher_documents(birmingham_json, "http://example.com")
    types = {d["document_type"] for d in docs}
    assert "Application Plans" in types
    assert "Decision Notice" in types
    assert "Supporting Statement" in types


def test_parse_document_urls_resolved(birmingham_json):
    base_url = "http://eplanning.idox.birmingham.gov.uk"
    docs = parse_publisher_documents(birmingham_json, base_url)
    for doc in docs:
        if doc["document_url"]:
            assert doc["document_url"].startswith("http://eplanning.idox.birmingham.gov.uk/publisher/docs/")


def test_parse_empty_data():
    docs = parse_publisher_documents({"data": []}, "http://example.com")
    assert docs == []


def test_parse_no_data_key():
    docs = parse_publisher_documents({}, "http://example.com")
    assert docs == []


def test_parse_service_error():
    docs = parse_publisher_documents(
        {"data": [], "serviceError": "Something went wrong"},
        "http://example.com",
    )
    assert docs == []


def test_parse_short_rows():
    docs = parse_publisher_documents(
        {"data": [["date", "desc"]]},  # Only 2 elements, need 4
        "http://example.com",
    )
    assert docs == []


def test_parse_skips_empty_rows():
    docs = parse_publisher_documents(
        {"data": [["", "", "", "/docs/abc.pdf", ""]]},
        "http://example.com",
    )
    assert docs == []


def test_extract_ajax_url(birmingham_html):
    ajax_url = extract_ajax_url(birmingham_html)
    assert ajax_url is not None
    assert "/publisher/mvc/getDocumentList" in ajax_url


def test_extract_ajax_url_missing():
    assert extract_ajax_url("<html><body>No AJAX here</body></html>") is None


@pytest.mark.asyncio
async def test_scraper_success(monkeypatch, birmingham_html, birmingham_json):
    """Full flow: page GET → extract AJAX → AJAX GET → parse JSON."""
    call_count = 0

    async def mock_get(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # First call: page HTML
            return SimpleNamespace(status_code=200, text=birmingham_html)
        else:
            # Second call: AJAX JSON
            return SimpleNamespace(
                status_code=200,
                json=lambda: birmingham_json,
            )

    async with PublisherDocumentScraper() as scraper:
        monkeypatch.setattr(scraper.client, "get", mock_get)
        docs = await scraper.scrape_documents(
            "http://eplanning.idox.birmingham.gov.uk/publisher/mvc/listDocuments"
            "?identifier=Planning&reference=2025/03490/PA"
        )

    assert len(docs) == 28
    assert scraper.stats["success"] == 1
    assert call_count == 2


@pytest.mark.asyncio
async def test_scraper_no_ajax_url(monkeypatch):
    """Page loads but has no AJAX endpoint."""
    async with PublisherDocumentScraper() as scraper:
        mock_get = AsyncMock(return_value=SimpleNamespace(status_code=200, text="<html>No AJAX</html>"))
        monkeypatch.setattr(scraper.client, "get", mock_get)
        docs = await scraper.scrape_documents("http://example.com/publisher/mvc/listDocuments")

    assert docs == []
    assert scraper.stats["ajax_missing"] == 1


@pytest.mark.asyncio
async def test_scraper_http_error(monkeypatch):
    async with PublisherDocumentScraper() as scraper:
        mock_get = AsyncMock(side_effect=httpx.HTTPError("connection failed"))
        monkeypatch.setattr(scraper.client, "get", mock_get)
        docs = await scraper.scrape_documents("http://example.com/publisher/mvc/listDocuments")

    assert docs == []
    assert scraper.stats["failed"] == 1


@pytest.mark.asyncio
async def test_scraper_page_404(monkeypatch):
    async with PublisherDocumentScraper() as scraper:
        mock_get = AsyncMock(return_value=SimpleNamespace(status_code=404, text=""))
        monkeypatch.setattr(scraper.client, "get", mock_get)
        docs = await scraper.scrape_documents("http://example.com/publisher/mvc/listDocuments")

    assert docs == []
    assert scraper.stats["failed"] == 1


@pytest.mark.asyncio
async def test_scrape_batch_skips_no_url(monkeypatch):
    applications = [
        {"uid": "app-no-url"},
        {
            "uid": "app-with-url",
            "documentation_url": "http://example.com/publisher/mvc/listDocuments?ref=X",
        },
    ]

    async with PublisherDocumentScraper() as scraper:
        mock_scrape = AsyncMock(return_value=[{"document_type": "Decision Notice"}])
        monkeypatch.setattr(scraper, "scrape_documents", mock_scrape)
        results = await scraper.scrape_batch(applications)

    assert "app-no-url" not in results
    assert "app-with-url" in results
    assert len(results["app-with-url"]) == 1
    mock_scrape.assert_awaited_once()
