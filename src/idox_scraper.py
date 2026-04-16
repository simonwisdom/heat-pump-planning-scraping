"""Idox planning portal document metadata scraper.

Fetches and parses document listings from Idox council planning portals.
Works across common Idox variants by dynamically parsing table headers rather
than using fixed column indices.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Callable
from urllib.parse import urlencode, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

# HTTP status codes worth retrying (transient failures, Cloudflare errors)
RETRYABLE_STATUS_CODES = {408, 421, 429, 500, 502, 503, 504, 520, 521, 522, 524}
MAX_RETRIES = 3
RETRY_DELAY = 5.0
RETRY_DELAY_429 = 30.0  # Longer backoff for rate-limit responses
IDOX_ZIP_BATCH_SIZE = 25  # Idox limits zip downloads to 25 files at a time
IDOX_ZIP_BATCH_DELAY = 5.0  # Seconds between batch POSTs within one app

logger = logging.getLogger(__name__)


_BLOCK_PAGE_PATTERNS = [
    re.compile(r'class=["\']g-recaptcha["\']', re.I),
    re.compile(r"cf-turnstile", re.I),
    re.compile(r"verify you are human", re.I),
    re.compile(r"please enable javascript(?: and cookies)? to continue", re.I),
    re.compile(r"access denied", re.I),
]


class DomainRateLimiter:
    """Rate limiter that tracks per-domain request timing."""

    def __init__(
        self,
        per_domain_delay: float = IDOX_RATE_LIMIT_PER_DOMAIN,
        max_concurrent: int = IDOX_MAX_CONCURRENT_DOMAINS,
    ):
        self.per_domain_delay = per_domain_delay
        self._last_request: dict[str, float] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def acquire(self, domain: str):
        await self._semaphore.acquire()
        last = self._last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.per_domain_delay:
            await asyncio.sleep(self.per_domain_delay - elapsed)

    def release(self, domain: str):
        self._last_request[domain] = time.monotonic()
        self._semaphore.release()


def parse_idox_documents(html: str, base_url: str) -> list[dict]:
    """Parse document metadata from an Idox portal documents-tab page."""
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table", id="Documents")
    if not table:
        logger.warning("No <table id='Documents'> found in HTML")
        return []

    headers = []
    header_row = table.find("tr")
    if header_row:
        for th in header_row.find_all("th"):
            headers.append(th.get_text(strip=True).lower())

    header_map = {h: i for i, h in enumerate(headers)}
    date_idx = header_map.get("date published")
    type_idx = header_map.get("document type")
    desc_idx = header_map.get("description")
    drawing_idx = header_map.get("drawing number")

    # Also locate the "View" column index for robust URL extraction
    view_idx = header_map.get("view")

    documents = []
    rows = table.find_all("tr")[1:]

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # Always capture checkbox value (used for zip download)
        checkbox = row.find("input", {"type": "checkbox", "name": "file"})
        file_checkbox_value = checkbox["value"] if checkbox and checkbox.get("value") else None

        # Strategy 1: "View" column anchor (most robust — catches all file types)
        doc_link = None
        if view_idx is not None and view_idx < len(cells):
            view_anchor = cells[view_idx].find("a", href=True)
            if view_anchor:
                doc_link = view_anchor["href"]

        # Strategy 2: any anchor with /files/ path (PDF or otherwise)
        if not doc_link:
            for link in row.find_all("a", href=True):
                href = link["href"]
                if "/files/" in href:
                    doc_link = href
                    break

        # Strategy 3: checkbox fallback for older Idox variants
        if not doc_link and file_checkbox_value:
            prefix_match = re.search(r"(/[^\"]+)/files/", html)
            if prefix_match:
                doc_link = f"{prefix_match.group(1)}/files/{file_checkbox_value}"

        def cell_text(idx: int | None) -> str:
            if idx is not None and idx < len(cells):
                return cells[idx].get_text(strip=True)
            return ""

        doc = {
            "date_published": cell_text(date_idx),
            "document_type": cell_text(type_idx),
            "description": cell_text(desc_idx),
            "drawing_number": cell_text(drawing_idx) if drawing_idx is not None else "",
            "document_url": urljoin(base_url, doc_link) if doc_link else None,
            "file_checkbox_value": file_checkbox_value,
        }

        if not doc["document_type"] and not doc["description"]:
            continue

        documents.append(doc)

    return documents


def extract_application_ref(html: str) -> str | None:
    """Extract the application reference when the page exposes one explicitly."""
    soup = _parse_html(html)
    ref_node = soup.find(id="applicationReference")
    if not ref_node:
        return None

    reference = ref_node.get_text(strip=True)
    return reference or None


def _parse_html(html: str) -> BeautifulSoup:
    """Parse HTML with lxml, falling back to html.parser."""
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def extract_csrf_token(html: str) -> str | None:
    """Extract the CSRF token from the caseDownloadForm."""
    soup = _parse_html(html)
    form = soup.find("form", id="caseDownloadForm")
    if not form:
        return None
    csrf_input = form.find("input", {"name": "_csrf"})
    if not csrf_input:
        return None
    return csrf_input.get("value") or None


def extract_case_number(html: str) -> str | None:
    """Extract the case number from the caseDownloadForm."""
    soup = _parse_html(html)
    form = soup.find("form", id="caseDownloadForm")
    if not form:
        return None
    case_input = form.find("input", {"name": "caseNumber"})
    if not case_input:
        return None
    return case_input.get("value") or None


def extract_download_action(html: str, base_url: str) -> str | None:
    """Extract the download form action URL, resolved against base_url."""
    soup = _parse_html(html)
    form = soup.find("form", id="caseDownloadForm")
    if not form:
        return None
    action = form.get("action")
    if not action:
        return None
    return urljoin(base_url, action)


def looks_like_block_page(html: str) -> bool:
    """Heuristic block-page detection for Idox responses.

    Ordinary Idox document pages often contain `recaptcha-link` and
    `recaptcha-submit` classes on document controls. Those markers are not, on
    their own, evidence of a CAPTCHA or WAF block.
    """
    lower = html.lower()

    # If the page exposes the documents table, treat it as a real document page.
    if '<table id="documents"' in lower:
        return False

    for pattern in _BLOCK_PAGE_PATTERNS:
        if pattern.search(html):
            return True
    return False


class IdoxDocumentScraper:
    """Async scraper for Idox planning portal document listings."""

    def __init__(self, rate_limiter: DomainRateLimiter | None = None):
        self.rate_limiter = rate_limiter or DomainRateLimiter()
        self._client: httpx.AsyncClient | None = None
        self._insecure_client: httpx.AsyncClient | None = None
        self.stats = {
            "success": 0,
            "failed": 0,
            "no_docs": 0,
            "captcha_blocked": 0,
            "tls_retry_used": 0,
        }

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": IDOX_USER_AGENT},
            follow_redirects=True,
        )
        self._insecure_client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": IDOX_USER_AGENT},
            follow_redirects=True,
            verify=False,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
        if self._insecure_client:
            await self._insecure_client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with IdoxDocumentScraper() as scraper:'")
        return self._client

    @property
    def insecure_client(self) -> httpx.AsyncClient:
        if not self._insecure_client:
            raise RuntimeError("Use 'async with IdoxDocumentScraper() as scraper:'")
        return self._insecure_client

    @staticmethod
    def _is_tls_verification_error(exc: Exception) -> bool:
        message = str(exc)
        return "CERTIFICATE_VERIFY_FAILED" in message or "certificate verify failed" in message

    async def _get_with_retry(self, url: str) -> httpx.Response | None:
        """GET with rate limiting, TLS fallback, and retry on transient errors."""
        domain = urlparse(url).netloc
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await self.rate_limiter.acquire(domain)
                try:
                    resp = await self.client.get(url)
                except Exception as exc:
                    if not self._is_tls_verification_error(exc):
                        raise
                    logger.warning("TLS verification failed for %s; retrying insecurely", domain)
                    self.stats["tls_retry_used"] += 1
                    resp = await self.insecure_client.get(url)
                finally:
                    self.rate_limiter.release(domain)

                if resp.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < MAX_RETRIES:
                        base = RETRY_DELAY_429 if resp.status_code == 429 else RETRY_DELAY
                        delay = base * attempt
                        logger.warning(
                            "GET %s for %s (attempt %s/%s), retrying in %.0fs",
                            resp.status_code,
                            url,
                            attempt,
                            MAX_RETRIES,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    logger.warning(
                        "GET %s for %s after %s attempts",
                        resp.status_code,
                        url,
                        MAX_RETRIES,
                    )
                    return None

                return resp

            except httpx.HTTPError as exc:
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "GET error for %s (attempt %s/%s): %s",
                        url,
                        attempt,
                        MAX_RETRIES,
                        exc,
                    )
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                logger.error("GET error for %s after %s attempts: %s", url, MAX_RETRIES, exc)
                return None
            except Exception as exc:
                logger.error("Unexpected GET error for %s: %s", url, exc)
                return None
        return None

    async def _post_with_retry(self, url: str, body: str) -> httpx.Response | None:
        """POST with rate limiting, TLS fallback, and retry on transient errors."""
        domain = urlparse(url).netloc
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await self.rate_limiter.acquire(domain)
                try:
                    resp = await self.client.post(url, content=body, headers=headers)
                except Exception as exc:
                    if not self._is_tls_verification_error(exc):
                        raise
                    logger.warning(
                        "TLS verification failed for POST %s; retrying insecurely",
                        domain,
                    )
                    self.stats["tls_retry_used"] += 1
                    resp = await self.insecure_client.post(url, content=body, headers=headers)
                finally:
                    self.rate_limiter.release(domain)

                if resp.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < MAX_RETRIES:
                        base = RETRY_DELAY_429 if resp.status_code == 429 else RETRY_DELAY
                        delay = base * attempt
                        logger.warning(
                            "POST %s for %s (attempt %s/%s), retrying in %.0fs",
                            resp.status_code,
                            url,
                            attempt,
                            MAX_RETRIES,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    logger.warning(
                        "POST %s for %s after %s attempts",
                        resp.status_code,
                        url,
                        MAX_RETRIES,
                    )
                    return None

                return resp

            except httpx.HTTPError as exc:
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "POST error for %s (attempt %s/%s): %s",
                        url,
                        attempt,
                        MAX_RETRIES,
                        exc,
                    )
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                logger.error("POST error for %s after %s attempts: %s", url, MAX_RETRIES, exc)
                return None
            except Exception as exc:
                logger.error("Unexpected POST error for %s: %s", url, exc)
                return None
        return None

    @staticmethod
    def _ensure_documents_tab(url: str) -> str:
        """Normalise a docs_url to ensure activeTab=documents."""
        if "activeTab=documents" not in url:
            if "activeTab=" in url:
                url = re.sub(r"activeTab=\w+", "activeTab=documents", url)
            elif "?" in url:
                url += "&activeTab=documents"
            else:
                url += "?activeTab=documents"
        return url

    async def scrape_documents(self, docs_url: str) -> list[dict]:
        parsed = urlparse(docs_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        url = self._ensure_documents_tab(docs_url)

        resp = await self._get_with_retry(url)
        if resp is None:
            self.stats["failed"] += 1
            return []

        if resp.status_code != 200:
            logger.warning("HTTP %s for %s", resp.status_code, url)
            self.stats["failed"] += 1
            return []

        html = resp.text
        documents = parse_idox_documents(html, base_url)
        if documents:
            self.stats["success"] += 1
            return documents

        if looks_like_block_page(html):
            logger.warning("Possible CAPTCHA/block page for %s", url)
            self.stats["captcha_blocked"] += 1
            return []

        self.stats["no_docs"] += 1
        return documents

    async def download_zip(
        self,
        docs_url: str,
    ) -> tuple[list[dict], list[bytes], str | None]:
        """GET documents page then POST to download all files as zip(s).

        Returns (documents_metadata, list_of_zip_bytes, failure_reason).
        failure_reason is None on success, or a short code string on failure.
        """
        parsed = urlparse(docs_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        url = self._ensure_documents_tab(docs_url)

        # Step 1: GET the documents page
        resp = await self._get_with_retry(url)
        if resp is None:
            self.stats["failed"] += 1
            return [], [], "get_failed"

        if resp.status_code != 200:
            logger.warning("HTTP %s for %s", resp.status_code, url)
            self.stats["failed"] += 1
            return [], [], f"http_{resp.status_code}"

        html = resp.text

        if looks_like_block_page(html):
            logger.warning("Possible CAPTCHA/block page for %s", url)
            self.stats["captcha_blocked"] += 1
            return [], [], "blocked"

        # Step 2: Parse document metadata
        documents = parse_idox_documents(html, base_url)
        if not documents:
            self.stats["no_docs"] += 1
            return [], [], "no_documents"

        # Step 3: Extract form data for zip download
        csrf = extract_csrf_token(html)
        case_number = extract_case_number(html)
        download_url = extract_download_action(html, url)

        if not csrf or not download_url:
            logger.warning("Missing CSRF or download URL for %s, cannot zip-download", url)
            self.stats["success"] += 1
            return documents, [], "no_download_form"

        file_values = [d["file_checkbox_value"] for d in documents if d.get("file_checkbox_value")]
        if not file_values:
            logger.warning("No checkbox values found for %s", url)
            self.stats["success"] += 1
            return documents, [], "no_checkboxes"

        # Step 4: Batch into chunks of IDOX_ZIP_BATCH_SIZE and POST each
        zips: list[bytes] = []
        for i in range(0, len(file_values), IDOX_ZIP_BATCH_SIZE):
            # Delay between batch POSTs to avoid rate limiting
            if i > 0:
                logger.debug(
                    "Waiting %.0fs between zip batches for %s",
                    IDOX_ZIP_BATCH_DELAY,
                    url,
                )
                await asyncio.sleep(IDOX_ZIP_BATCH_DELAY)

            batch = file_values[i : i + IDOX_ZIP_BATCH_SIZE]
            pairs = [("_csrf", csrf)]
            if case_number:
                pairs.append(("caseNumber", case_number))
            for fv in batch:
                pairs.append(("file", fv))
            body = urlencode(pairs)

            post_resp = await self._post_with_retry(download_url, body)
            if post_resp is None:
                logger.warning("ZIP POST failed for %s (batch %s)", url, i // IDOX_ZIP_BATCH_SIZE)
                continue

            if post_resp.status_code != 200:
                logger.warning("ZIP POST HTTP %s for %s", post_resp.status_code, url)
                continue

            content = post_resp.content
            # Validate zip magic bytes (PK\x03\x04)
            if content[:4] != b"PK\x03\x04":
                ct = post_resp.headers.get("content-type", "")
                if "zip" not in ct and "octet-stream" not in ct:
                    logger.warning("ZIP response is not a zip for %s (content-type: %s)", url, ct)
                    continue

            zips.append(content)

        self.stats["success"] += 1
        if zips:
            return documents, zips, None
        return documents, zips, "zip_post_failed"

    async def scrape_batch(
        self,
        applications: list[dict],
        on_result: Callable | None = None,
    ) -> dict[str, list[dict]]:
        results: dict[str, list[dict]] = {}

        for app in applications:
            uid = app["uid"]
            docs_url = app.get("documentation_url")
            if not docs_url:
                logger.debug("No docs_url for %s, skipping", uid)
                continue

            documents = await self.scrape_documents(docs_url)
            results[uid] = documents

            if on_result:
                on_result(uid, documents)

        return results
