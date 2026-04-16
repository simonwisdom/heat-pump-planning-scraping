import io
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from src.idox_scraper import (
    DomainRateLimiter,
    IdoxDocumentScraper,
    extract_application_ref,
    extract_case_number,
    extract_csrf_token,
    extract_download_action,
    looks_like_block_page,
    parse_idox_documents,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def cornwall_html() -> str:
    return (FIXTURES_DIR / "cornwall_documents.html").read_text(encoding="utf-8")


@pytest.fixture
def sheffield_html() -> str:
    return (FIXTURES_DIR / "sheffield_documents.html").read_text(encoding="utf-8")


@pytest.fixture
def manchester_html() -> str:
    return (FIXTURES_DIR / "manchester_documents.html").read_text(encoding="utf-8")


def test_parse_cornwall_fixture(cornwall_html):
    base_url = "https://planning.cornwall.gov.uk"
    docs = parse_idox_documents(cornwall_html, base_url)

    assert len(docs) == 6

    decision_notice = next(doc for doc in docs if doc["document_type"] == "Decision Notice")
    assert decision_notice["date_published"] == "11 Feb 2026"
    assert decision_notice["description"] == "ACFULZ - CONDITIONAL APPROVAL"
    assert decision_notice["drawing_number"] == ""
    assert (
        decision_notice["document_url"] == "https://planning.cornwall.gov.uk/online-applications/files/"
        "08F9809DE6571319F23595640A5E0345/pdf/"
        "PA25_09154-ACFULZ_-_CONDITIONAL_APPROVAL-9257966.pdf"
    )


def test_parse_sheffield_fixture(sheffield_html):
    docs = parse_idox_documents(sheffield_html, "https://planning.sheffield.gov.uk")

    assert len(docs) == 8

    decision_notice = next(doc for doc in docs if doc["document_type"] == "Decision Notice")
    assert decision_notice["date_published"] == "17 Apr 2024"
    assert decision_notice["description"] == ""
    assert decision_notice["drawing_number"] == ""
    assert decision_notice["document_url"].endswith("24_00236_FUL--2145889.pdf")


def test_parse_manchester_fixture(manchester_html):
    docs = parse_idox_documents(manchester_html, "https://pa.manchester.gov.uk")

    assert len(docs) == 9
    assert docs[0]["document_type"] == "Supporting Information"


def test_parse_empty_table():
    html = """
    <html>
      <body>
        <table id="Documents">
          <tr>
            <th>Date Published</th><th>Document Type</th><th>Description</th><th>View</th>
          </tr>
        </table>
      </body>
    </html>
    """
    assert parse_idox_documents(html, "https://example.gov.uk") == []


def test_parse_no_table():
    html = "<html><body><p>No documents listed</p></body></html>"
    assert parse_idox_documents(html, "https://example.gov.uk") == []


def test_parse_captcha_page():
    html = '<html><body><div class="g-recaptcha"></div></body></html>'
    assert parse_idox_documents(html, "https://example.gov.uk") == []


def test_looks_like_block_page_on_real_docs_fixture(cornwall_html):
    assert looks_like_block_page(cornwall_html) is False


def test_looks_like_block_page_on_captcha_page():
    html = '<html><body><div class="g-recaptcha"></div></body></html>'
    assert looks_like_block_page(html) is True


def test_dynamic_header_detection():
    cases = [
        ("cornwall_documents.html", "https://planning.cornwall.gov.uk", True),
        ("sheffield_documents.html", "https://planning.sheffield.gov.uk", False),
        ("manchester_documents.html", "https://pa.manchester.gov.uk", True),
    ]

    for fixture_name, base_url, expect_drawing_number_data in cases:
        html = (FIXTURES_DIR / fixture_name).read_text(encoding="utf-8")
        docs = parse_idox_documents(html, base_url)

        assert docs
        assert all(doc["document_type"] for doc in docs)

        has_drawing_number_values = any(doc["drawing_number"] for doc in docs)
        assert has_drawing_number_values is expect_drawing_number_data


def test_extract_application_ref():
    html = '<html><body><span id="applicationReference">PA25/09154</span></body></html>'
    assert extract_application_ref(html) == "PA25/09154"


def test_extract_application_ref_missing():
    html = "<html><body><span id='otherReference'>PA25/09154</span></body></html>"
    assert extract_application_ref(html) is None


def test_parse_checkbox_values(cornwall_html):
    docs = parse_idox_documents(cornwall_html, "https://planning.cornwall.gov.uk")
    assert len(docs) == 6
    assert all(doc["file_checkbox_value"] is not None for doc in docs)

    decision = next(d for d in docs if d["document_type"] == "Decision Notice")
    assert decision["file_checkbox_value"] == (
        "08F9809DE6571319F23595640A5E0345/ACFULZ_-_CONDITIONAL_APPROVAL-9257966.pdf"
    )


def test_parse_checkbox_values_sheffield(sheffield_html):
    docs = parse_idox_documents(sheffield_html, "https://planning.sheffield.gov.uk")
    assert all(doc["file_checkbox_value"] is not None for doc in docs)


def test_extract_csrf_token(cornwall_html):
    assert extract_csrf_token(cornwall_html) == "306333c0-f089-41d8-a1c8-14ea0b8c5919"


def test_extract_csrf_token_missing():
    html = "<html><body><p>No form here</p></body></html>"
    assert extract_csrf_token(html) is None


def test_extract_case_number(cornwall_html):
    assert extract_case_number(cornwall_html) == "PA25/09154"


def test_extract_case_number_missing():
    html = "<html><body><p>No form here</p></body></html>"
    assert extract_case_number(html) is None


def test_extract_download_action(cornwall_html):
    action = extract_download_action(cornwall_html, "https://planning.cornwall.gov.uk/online-applications/")
    assert action is not None
    assert "download" in action


@pytest.mark.asyncio
async def test_domain_rate_limiter_delays(monkeypatch):
    limiter = DomainRateLimiter(per_domain_delay=2.0, max_concurrent=1)

    current_time = 100.0
    monkeypatch.setattr("src.idox_scraper.time.monotonic", lambda: current_time)

    sleeps = []

    async def fake_sleep(delay: float):
        sleeps.append(delay)

    monkeypatch.setattr("src.idox_scraper.asyncio.sleep", fake_sleep)

    await limiter.acquire("example.gov.uk")
    assert sleeps == []
    limiter.release("example.gov.uk")

    current_time = 101.0
    await limiter.acquire("example.gov.uk")
    assert sleeps == [1.0]
    limiter.release("example.gov.uk")

    current_time = 101.1
    await limiter.acquire("other.gov.uk")
    assert sleeps == [1.0]
    limiter.release("other.gov.uk")


@pytest.mark.asyncio
async def test_scraper_documents_success(monkeypatch, cornwall_html):
    async with IdoxDocumentScraper() as scraper:
        mock_get = AsyncMock(return_value=SimpleNamespace(status_code=200, text=cornwall_html))
        monkeypatch.setattr(scraper.client, "get", mock_get)

        docs = await scraper.scrape_documents(
            "https://planning.cornwall.gov.uk/online-applications/applicationDetails.do?"
            "keyVal=T6QTZLFGFU500&activeTab=documents"
        )

    assert len(docs) == 6
    assert scraper.stats["success"] == 1
    assert scraper.stats["failed"] == 0


@pytest.mark.asyncio
async def test_scraper_captcha_detection(monkeypatch):
    captcha_html = '<html><body><div class="g-recaptcha"></div></body></html>'

    async with IdoxDocumentScraper() as scraper:
        mock_get = AsyncMock(return_value=SimpleNamespace(status_code=200, text=captcha_html))
        monkeypatch.setattr(scraper.client, "get", mock_get)

        docs = await scraper.scrape_documents(
            "https://planning.example.gov.uk/online-applications/applicationDetails.do?keyVal=ABC123"
        )

    assert docs == []
    assert scraper.stats["captcha_blocked"] == 1
    assert scraper.stats["success"] == 0
    assert scraper.stats["failed"] == 0


@pytest.mark.asyncio
async def test_scraper_http_error(monkeypatch):
    async with IdoxDocumentScraper() as scraper:
        mock_get = AsyncMock(side_effect=httpx.HTTPError("boom"))
        monkeypatch.setattr(scraper.client, "get", mock_get)

        docs = await scraper.scrape_documents(
            "https://planning.example.gov.uk/online-applications/applicationDetails.do?keyVal=ABC123"
        )

    assert docs == []
    assert scraper.stats["failed"] == 1


@pytest.mark.asyncio
async def test_scraper_tls_retry_uses_insecure_client(monkeypatch, cornwall_html):
    async with IdoxDocumentScraper() as scraper:
        secure_get = AsyncMock(
            side_effect=httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed")
        )
        insecure_get = AsyncMock(return_value=SimpleNamespace(status_code=200, text=cornwall_html))
        monkeypatch.setattr(scraper.client, "get", secure_get)
        monkeypatch.setattr(scraper.insecure_client, "get", insecure_get)

        docs = await scraper.scrape_documents(
            "https://planning.example.gov.uk/online-applications/applicationDetails.do?keyVal=ABC123"
        )

    assert len(docs) == 6
    assert scraper.stats["success"] == 1
    assert scraper.stats["tls_retry_used"] == 1


@pytest.mark.asyncio
async def test_scrape_batch_skips_no_url(monkeypatch):
    applications = [
        {"uid": "app-no-url"},
        {
            "uid": "app-with-url",
            "documentation_url": (
                "https://planning.cornwall.gov.uk/online-applications/applicationDetails.do?keyVal=T6QTZLFGFU500"
            ),
        },
    ]

    async with IdoxDocumentScraper() as scraper:
        mock_scrape_documents = AsyncMock(return_value=[{"document_type": "Decision Notice"}])
        monkeypatch.setattr(scraper, "scrape_documents", mock_scrape_documents)

        results = await scraper.scrape_batch(applications)

    assert "app-no-url" not in results
    assert "app-with-url" in results
    assert len(results["app-with-url"]) == 1
    mock_scrape_documents.assert_awaited_once()


def _make_zip(filenames: list[str]) -> bytes:
    """Create an in-memory zip with dummy content for each filename."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name in filenames:
            zf.writestr(name, f"content of {name}")
    return buf.getvalue()


@pytest.mark.asyncio
async def test_download_zip_success(monkeypatch, cornwall_html):
    docs = parse_idox_documents(cornwall_html, "https://planning.cornwall.gov.uk")
    filenames = [d["file_checkbox_value"] for d in docs if d.get("file_checkbox_value")]
    # Strip the hash prefix — zip members use just the filename part
    zip_names = [v.split("/", 1)[1] if "/" in v else v for v in filenames]
    fake_zip = _make_zip(zip_names)

    get_calls = []
    post_calls = []

    async def mock_get(url, **kw):
        get_calls.append(url)
        return SimpleNamespace(status_code=200, text=cornwall_html)

    async def mock_post(url, *, content=None, headers=None, **kw):
        post_calls.append(url)
        return SimpleNamespace(
            status_code=200,
            content=fake_zip,
            headers={"content-type": "application/force-download"},
        )

    async with IdoxDocumentScraper() as scraper:
        monkeypatch.setattr(scraper.client, "get", mock_get)
        monkeypatch.setattr(scraper.client, "post", mock_post)

        documents, zips, reason = await scraper.download_zip(
            "https://planning.cornwall.gov.uk/online-applications/"
            "applicationDetails.do?keyVal=T6QTZLFGFU500&activeTab=documents"
        )

    assert len(documents) == 6
    assert len(zips) == 1
    assert reason is None
    assert len(get_calls) == 1
    assert len(post_calls) == 1

    # Verify the zip is valid
    zf = zipfile.ZipFile(io.BytesIO(zips[0]))
    assert len(zf.namelist()) == len(zip_names)


@pytest.mark.asyncio
async def test_download_zip_no_form(monkeypatch):
    """When the page has documents but no download form, return docs but no zips."""
    html = """
    <html><body>
    <table id="Documents">
      <tr><th>Date Published</th><th>Document Type</th><th>Description</th><th>View</th></tr>
      <tr>
        <td>01 Jan 2025</td><td>Decision</td><td>GRANT</td>
        <td><a href="/files/ABC/test.pdf">View</a></td>
      </tr>
    </table>
    </body></html>
    """

    async def mock_get(url, **kw):
        return SimpleNamespace(status_code=200, text=html)

    async with IdoxDocumentScraper() as scraper:
        monkeypatch.setattr(scraper.client, "get", mock_get)

        documents, zips, reason = await scraper.download_zip(
            "https://example.gov.uk/online-applications/applicationDetails.do?keyVal=X"
        )

    assert len(documents) == 1
    assert zips == []
    assert reason == "no_download_form"
