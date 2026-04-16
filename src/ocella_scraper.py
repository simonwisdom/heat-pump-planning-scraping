"""OcellaWeb planning portal document metadata scraper.

Fetches and parses document listings from OcellaWeb council planning portals.
Tested on: Hillingdon, South Holland, Breckland.

OcellaWeb pages have a consistent structure:
- <strong> section headers (e.g. "Application Forms", "Plans", "Decision Documentation")
- <table> per section with rows: [link (doc type)] | [empty] | [date] | [empty] | [description]
- Document download URLs: viewDocument?file=...&module=pl (relative to OcellaWeb base)
"""

from __future__ import annotations

import asyncio
import logging
import time
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
MAX_RETRIES = 3
RETRY_DELAY = 5.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def parse_ocella_documents(html: str, page_url: str) -> list[dict]:
    """Parse document metadata from an OcellaWeb showDocuments page.

    Returns list of dicts with keys: document_type, description,
    date_published, document_url, section.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")

    # Derive base URL for resolving relative viewDocument links.
    # page_url is like: https://host/OcellaWeb/showDocuments?...
    # viewDocument links are relative: viewDocument?file=...
    # So base is: https://host/OcellaWeb/
    parsed = urlparse(page_url)
    path_parts = parsed.path.rsplit("/", 1)
    base_path = path_parts[0] + "/" if len(path_parts) > 1 else "/"
    base_url = f"{parsed.scheme}://{parsed.netloc}{base_path}"

    documents = []
    current_section = ""

    # Walk through the main content looking for <strong> headers and <table> elements
    # The structure is: <strong>Section Name</strong> ... <table>rows</table> ... <hr> ...
    main = soup.find("main") or soup.find("div", id=lambda x: x and "content" in x.lower()) or soup.body
    if not main:
        logger.warning("No main content found in HTML")
        return []

    for element in main.find_all(["strong", "table"]):
        if element.name == "strong":
            # Section header — but skip the "DOCUMENTS" heading
            text = element.get_text(strip=True)
            if text.upper() != "DOCUMENTS":
                current_section = text
            continue

        # It's a table — parse rows
        for row in element.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue

            # Skip "no documents" rows
            first_text = cells[0].get_text(strip=True)
            if "no documents" in first_text.lower():
                continue

            # First cell contains the link (doc type as link text, viewDocument URL)
            link = cells[0].find("a", href=True)
            if not link:
                continue

            doc_type = link.get_text(strip=True)
            href = link["href"]
            doc_url = urljoin(base_url, href)

            # Date is typically in cell index 2
            date_published = ""
            if len(cells) > 2:
                date_published = cells[2].get_text(strip=True)

            # Description is typically in cell index 4
            description = ""
            if len(cells) > 4:
                description = cells[4].get_text(strip=True)

            documents.append(
                {
                    "document_type": doc_type,
                    "description": description,
                    "date_published": date_published,
                    "document_url": doc_url,
                    "section": current_section,
                }
            )

    return documents


class OcellaDocumentScraper:
    """Async scraper for OcellaWeb planning portal document listings."""

    def __init__(self, per_domain_delay: float = 2.0):
        self.per_domain_delay = per_domain_delay
        self._client: httpx.AsyncClient | None = None
        self._last_request: dict[str, float] = {}
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with OcellaDocumentScraper() as scraper:'")
        return self._client

    async def _rate_limit(self, domain: str):
        last = self._last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.per_domain_delay:
            await asyncio.sleep(self.per_domain_delay - elapsed)

    def _record_request(self, domain: str):
        self._last_request[domain] = time.monotonic()

    async def scrape_documents(self, docs_url: str) -> list[dict]:
        domain = urlparse(docs_url).netloc

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await self._rate_limit(domain)
                resp = await self.client.get(docs_url)
                self._record_request(domain)

                if resp.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < MAX_RETRIES:
                        delay = RETRY_DELAY * attempt
                        logger.warning(
                            "HTTP %s for %s (attempt %s/%s), retrying in %.0fs",
                            resp.status_code,
                            docs_url,
                            attempt,
                            MAX_RETRIES,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    logger.warning(
                        "HTTP %s for %s after %s attempts",
                        resp.status_code,
                        docs_url,
                        MAX_RETRIES,
                    )
                    self.stats["failed"] += 1
                    return []

                if resp.status_code != 200:
                    logger.warning("HTTP %s for %s", resp.status_code, docs_url)
                    self.stats["failed"] += 1
                    return []

                documents = parse_ocella_documents(resp.text, docs_url)
                if documents:
                    self.stats["success"] += 1
                else:
                    self.stats["no_docs"] += 1
                return documents

            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                if attempt < MAX_RETRIES:
                    logger.warning(
                        "Error for %s (attempt %s/%s): %s",
                        docs_url,
                        attempt,
                        MAX_RETRIES,
                        exc,
                    )
                    await asyncio.sleep(RETRY_DELAY * attempt)
                    continue
                logger.error("Error for %s after %s attempts: %s", docs_url, MAX_RETRIES, exc)
                self.stats["failed"] += 1
                return []

        self.stats["failed"] += 1
        return []
