import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from src.agile_scraper import (
    AGILE_AUTHORITIES,
    AGILE_BASE_URL,
    AgileDocumentScraper,
    _agile_headers,
    get_application_detail,
    get_application_documents,
    parse_agile_documents,
    search_applications,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def agile_documents_json() -> list[dict]:
    return json.loads((FIXTURES_DIR / "agile_middlesbrough_documents.json").read_text())


@pytest.fixture
def agile_search_json() -> dict:
    return json.loads((FIXTURES_DIR / "agile_middlesbrough_search.json").read_text())


def test_authority_list_has_expected_entries():
    assert len(AGILE_AUTHORITIES) == 17
    assert "MIDDLESBROUGH" in AGILE_AUTHORITIES
    assert "RUGBY" in AGILE_AUTHORITIES


def test_headers_normalise_client_name():
    headers = _agile_headers("middlesbrough")
    assert headers["x-client"] == "MIDDLESBROUGH"
    assert headers["x-product"] == "CITIZENPORTAL"
    assert headers["x-service"] == "PA"
    assert headers["Accept"] == "application/json"


def test_parse_middlesbrough_fixture(agile_documents_json):
    docs = parse_agile_documents(agile_documents_json, "MIDDLESBROUGH")

    assert len(docs) == 3
    first = docs[0]
    assert first["date_published"] == "2025-06-14"
    assert first["document_type"] == "Decision Notice"
    assert first["description"] == "Decision Notice"
    assert first["drawing_number"] == "901001"
    assert first["document_url"] == (
        "https://planningapi.agileapplications.co.uk/api/application/document/f4e6fa50-a3f5-4cbf-930e-75b786f43c2e"
    )


def test_parse_search_fixture_shape(agile_search_json):
    assert agile_search_json["total"] == 3
    assert len(agile_search_json["results"]) == 3
    assert agile_search_json["results"][0]["id"] == 38131


def test_parse_empty_documents_list():
    assert parse_agile_documents([], "MIDDLESBROUGH") == []


def test_parse_skips_rows_without_content():
    raw = [{"name": "", "mediaDescription": "", "documentHash": None}]
    assert parse_agile_documents(raw, "MIDDLESBROUGH") == []


def test_parse_all_document_urls_are_download_endpoints(agile_documents_json):
    docs = parse_agile_documents(agile_documents_json, "MIDDLESBROUGH")
    for doc in docs:
        assert doc["document_url"].startswith(f"{AGILE_BASE_URL}/api/application/document/")


@pytest.mark.asyncio
async def test_search_applications_uses_expected_endpoint(monkeypatch, agile_search_json):
    request_json = AsyncMock(return_value=agile_search_json)
    monkeypatch.setattr("src.agile_scraper._request_json", request_json)

    result = await search_applications("middlesbrough", "heat pump", size=3)

    assert result["total"] == 3
    assert request_json.await_count == 1
    _, url, client_name, params = request_json.await_args.args
    assert url.endswith("/api/application/search")
    assert client_name == "middlesbrough"
    assert params == {"proposal": "heat pump", "size": 3}


@pytest.mark.asyncio
async def test_get_application_detail_uses_expected_endpoint(monkeypatch):
    request_json = AsyncMock(return_value={"id": 38131, "reference": "23/0088/FUL"})
    monkeypatch.setattr("src.agile_scraper._request_json", request_json)

    result = await get_application_detail(38131, "MIDDLESBROUGH")

    assert result["id"] == 38131
    _, url, client_name = request_json.await_args.args
    assert url.endswith("/api/application/38131")
    assert client_name == "MIDDLESBROUGH"


@pytest.mark.asyncio
async def test_get_application_documents_non_list_response(monkeypatch):
    request_json = AsyncMock(return_value={"unexpected": "shape"})
    monkeypatch.setattr("src.agile_scraper._request_json", request_json)

    result = await get_application_documents(38131, "MIDDLESBROUGH")

    assert result == []


@pytest.mark.asyncio
async def test_scraper_success(monkeypatch, agile_documents_json):
    async with AgileDocumentScraper() as scraper:
        mock_get = AsyncMock(return_value=SimpleNamespace(status_code=200, json=lambda: agile_documents_json))
        monkeypatch.setattr(scraper.client, "get", mock_get)

        docs = await scraper.scrape_documents(38131, "MIDDLESBROUGH")

    assert len(docs) == 3
    assert scraper.stats["success"] == 1
    assert scraper.stats["failed"] == 0


@pytest.mark.asyncio
async def test_scraper_http_error(monkeypatch):
    async with AgileDocumentScraper() as scraper:
        mock_get = AsyncMock(side_effect=httpx.HTTPError("boom"))
        monkeypatch.setattr(scraper.client, "get", mock_get)

        docs = await scraper.scrape_documents(38131, "MIDDLESBROUGH")

    assert docs == []
    assert scraper.stats["failed"] == 1


@pytest.mark.asyncio
async def test_scraper_non_200(monkeypatch):
    async with AgileDocumentScraper() as scraper:
        mock_get = AsyncMock(return_value=SimpleNamespace(status_code=404, json=lambda: []))
        monkeypatch.setattr(scraper.client, "get", mock_get)

        docs = await scraper.scrape_documents(38131, "MIDDLESBROUGH")

    assert docs == []
    assert scraper.stats["failed"] == 1


@pytest.mark.asyncio
async def test_scrape_batch_skips_apps_without_ids(monkeypatch):
    applications = [
        {"uid": "missing-id", "client_name": "MIDDLESBROUGH"},
        {"uid": "has-id", "id": 38131, "client_name": "MIDDLESBROUGH"},
    ]

    async with AgileDocumentScraper() as scraper:
        mock_scrape = AsyncMock(return_value=[{"document_type": "Decision Notice"}])
        monkeypatch.setattr(scraper, "scrape_documents", mock_scrape)

        results = await scraper.scrape_batch(applications)

    assert "missing-id" not in results
    assert "has-id" in results
    assert len(results["has-id"]) == 1
    mock_scrape.assert_awaited_once()


@pytest.mark.asyncio
async def test_scrape_batch_skips_apps_without_client(monkeypatch):
    applications = [
        {"uid": "missing-client", "id": 38131},
        {"uid": "has-client", "app_id": 38131, "authority": "MIDDLESBROUGH"},
    ]

    async with AgileDocumentScraper() as scraper:
        mock_scrape = AsyncMock(return_value=[{"document_type": "Decision Notice"}])
        monkeypatch.setattr(scraper, "scrape_documents", mock_scrape)

        results = await scraper.scrape_batch(applications)

    assert "missing-client" not in results
    assert "has-client" in results
    mock_scrape.assert_awaited_once()
