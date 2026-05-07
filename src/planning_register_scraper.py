"""Planning Register document scraper.

Covers councils running the ``planning-register.co.uk`` SaaS platform. Each
application has a direct deep-link URL:

    https://{authority}.planning-register.co.uk/Planning/Display/{uid}

The platform serves fully server-rendered HTML (no JS required) but redirects
first-time visitors to a disclaimer/cookie page:

    /Disclaimer?returnUrl=/Planning/Display/{uid}

We accept the disclaimer via a POST to /Disclaimer/Accept?returnUrl=... which
sets an ``AcceptedDisclaimer`` cookie. Subsequent requests within the same
session skip the disclaimer.

Document links use the pattern:

    /Document/Download?module=PLA&recordNumber=N&planId=N&imageId=N&isPlan=False&fileName=X.pdf

The display page renders each document in a table row with columns:
  [checkbox] | doc_type | date | description | file_size | drawing_number

Both the link anchor text (doc_type) and the table cells are parsed to
populate the standard document dict.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

logger = logging.getLogger(__name__)

# Pattern for disclaimer redirect URL
_DISCLAIMER_RE = re.compile(r"/Disclaimer\?returnUrl=(.+)$")


def _disclaimer_accept_url(disclaimer_url: str) -> str:
    """Convert a /Disclaimer?returnUrl=... URL to its POST /Disclaimer/Accept equivalent."""
    parsed = urlparse(disclaimer_url)
    return_url = parse_qs(parsed.query).get("returnUrl", [""])[0]
    return f"{parsed.scheme}://{parsed.netloc}/Disclaimer/Accept?returnUrl={return_url}"


def _is_disclaimer_page(url: str) -> bool:
    lower = url.lower()
    return "/disclaimer" in lower and "returnurl=" in lower


def _http_status_failure_code(status_code: int) -> str:
    if status_code == 403:
        return "http_403"
    if status_code == 404:
        return "http_404"
    if status_code == 429:
        return "http_429"
    if 500 <= status_code < 600:
        return "http_5xx"
    return f"http_{status_code}"


# Magic bytes → extension (borrowed from northgate_scraper pattern)
_MAGIC_EXTS: list[tuple[bytes, str]] = [
    (b"%PDF", ".pdf"),
    (b"PK\x03\x04", ".zip"),
    (b"\xd0\xcf\x11\xe0", ".doc"),
    (b"II*\x00", ".tif"),
    (b"MM\x00*", ".tif"),
    (b"\x89PNG", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"GIF8", ".gif"),
]

_CTYPE_EXTS = {
    "application/pdf": ".pdf",
    "image/tiff": ".tif",
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "text/plain": ".txt",
    "text/html": ".html",
}


def _detect_extension(content: bytes, content_type: str) -> str:
    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            if ext == ".zip" and content_type:
                ct = content_type.split(";")[0].strip().lower()
                office = _CTYPE_EXTS.get(ct)
                if office:
                    return office
            return ext
    ct = (content_type or "").split(";")[0].strip().lower()
    return _CTYPE_EXTS.get(ct, ".pdf")


def parse_planning_register_documents(html: str, base_url: str) -> list[dict]:
    """Parse document table rows from a planning-register.co.uk display page.

    Table columns (confirmed across wnc and vogonline authorities):
      td[0] - checkbox
      td[1] - link with doc_type as anchor text (contains /Document/Download href)
      td[2] - date (DD/MM/YYYY)
      td[3] - description / drawing title
      td[4] - file size
      td[5] - drawing/rev number

    We deduplicate on planId query parameter.
    """
    # Use a simple regex-based approach to avoid bs4 import at module level;
    # the caller imports bs4 but this function can be tested standalone.
    # We parse with a lightweight approach: find all <a> tags with Download hrefs,
    # then walk up to the parent <tr> to extract sibling <td> values.
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.error("beautifulsoup4 is required for planning_register_scraper")
        return []

    soup = BeautifulSoup(html, "html.parser")
    documents: list[dict] = []
    seen_plan_ids: set[str] = set()

    for a in soup.find_all("a", href=lambda h: h and "/Document/Download" in h):
        href = a["href"]
        qs = parse_qs(urlparse(href).query)
        plan_id = (qs.get("planId") or [""])[0]
        filename = (qs.get("fileName") or [""])[0]

        # Deduplicate — the same doc appears twice (once per tab)
        if plan_id and plan_id in seen_plan_ids:
            continue
        if plan_id:
            seen_plan_ids.add(plan_id)

        doc_type = a.get_text(strip=True)  # anchor text = doc type label
        date_str = ""
        description = ""
        drawing_number = ""

        tr = a.find_parent("tr")
        if tr:
            tds = tr.find_all("td")
            if len(tds) >= 3:
                date_str = tds[2].get_text(strip=True) if len(tds) > 2 else ""
            if len(tds) >= 4:
                description = tds[3].get_text(strip=True) if len(tds) > 3 else ""
            if len(tds) >= 6:
                drawing_number = tds[5].get_text(strip=True) if len(tds) > 5 else ""

        documents.append(
            {
                "date_published": date_str,
                "document_type": doc_type,
                "description": description,
                "drawing_number": drawing_number,
                "document_url": urljoin(base_url, href),
                "filename": filename,
            }
        )

    return documents


class PlanningRegisterDocumentScraper:
    """Async scraper for planning-register.co.uk document listings.

    Usage::

        async with PlanningRegisterDocumentScraper() as scraper:
            docs, failure_code = await scraper.scrape_documents(docs_url)
            if docs:
                bytes_written, final_path = await scraper.download_document(
                    docs[0]["document_url"], Path("/tmp/doc"), referer=docs_url
                )

    The scraper handles the disclaimer cookie automatically: it detects
    redirects to /Disclaimer and POSTs to /Disclaimer/Accept before
    retrying the original URL. The session cookie persists for the life
    of the scraper context, so the disclaimer is only accepted once per
    domain per session.
    """

    def __init__(
        self,
        per_domain_delay: float = IDOX_RATE_LIMIT_PER_DOMAIN,
        max_concurrent: int = IDOX_MAX_CONCURRENT_DOMAINS,
    ):
        self.per_domain_delay = per_domain_delay
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._last_request: dict[str, float] = {}
        self._client: httpx.AsyncClient | None = None
        self.stats = {"success": 0, "failed": 0, "no_docs": 0}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with PlanningRegisterDocumentScraper() as scraper:'")
        return self._client

    async def _rate_limit(self, domain: str):
        await self._semaphore.acquire()
        last = self._last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < self.per_domain_delay:
            await asyncio.sleep(self.per_domain_delay - elapsed)

    def _release(self, domain: str):
        self._last_request[domain] = time.monotonic()
        self._semaphore.release()

    async def _get(self, url: str, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            return await self.client.get(url, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def _post(self, url: str, data: dict | None = None, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            return await self.client.post(url, data=data or {}, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def _accept_disclaimer_if_needed(self, docs_url: str) -> tuple[httpx.Response | None, str | None]:
        """GET docs_url, accepting the disclaimer if redirected to it.

        Handles two planning-register.co.uk disclaimer variants:
        * Legacy (e.g. WNC): form action ``/Disclaimer/Accept?returnUrl=...``,
          no CSRF token. POST with empty body.
        * Newer (e.g. South Oxon, VoWH): form action ``/Disclaimer/AcceptDisclaimer``,
          ``__RequestVerificationToken`` CSRF input, hidden ``returnURL`` field.
          POST the parsed form fields.

        Returns (response, failure_code). failure_code is None on success.
        """
        resp = await self._get(docs_url)
        final_url = str(resp.url)

        if _is_disclaimer_page(final_url):
            try:
                from bs4 import BeautifulSoup
            except ImportError:
                logger.error("beautifulsoup4 is required for planning_register_scraper")
                return None, "parse_error"

            soup = BeautifulSoup(resp.text, "html.parser")
            form = soup.find(
                "form",
                action=lambda v: bool(v) and "/disclaimer" in v.lower() and "accept" in v.lower(),
            )
            parsed = urlparse(final_url)
            base = f"{parsed.scheme}://{parsed.netloc}"

            if form:
                action = form.get("action", "")
                accept_url = action if action.startswith("http") else f"{base}{action}"
                fields: dict[str, str] = {}
                for inp in form.find_all("input"):
                    name = inp.get("name")
                    if not name:
                        continue
                    fields[name] = inp.get("value", "") or ""
            else:
                # Fallback to legacy heuristic if form parsing fails
                accept_url = f"{base}/Disclaimer/Accept?{parsed.query}"
                fields = {}

            logger.debug("Accepting disclaimer at %s with %d fields", accept_url, len(fields))
            resp = await self._post(accept_url, data=fields, headers={"Referer": final_url})
            final_url = str(resp.url)
            if _is_disclaimer_page(final_url):
                logger.error("Still on disclaimer page after POST: %s", final_url)
                return None, "disclaimer_loop"

        if resp.status_code != 200:
            return None, _http_status_failure_code(resp.status_code)

        return resp, None

    async def scrape_documents(self, docs_url: str) -> tuple[list[dict], str | None]:
        """Scrape the document listing for a planning-register.co.uk application.

        Returns (documents, failure_code). failure_code is None on success
        (including empty listings). Possible failure codes: http_403, http_404,
        http_5xx, timeout, network_error, parse_error, no_display_page,
        disclaimer_loop, unexpected_error.
        """
        # Reject generic search URLs (e.g. Exmoor /Search/Results/)
        if "/Search/Results" in docs_url or "/Search/" in docs_url.split("?")[0]:
            logger.warning("Generic search URL — no document listing possible: %s", docs_url)
            return [], "no_display_page"

        parsed = urlparse(docs_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        try:
            resp, failure_code = await self._accept_disclaimer_if_needed(docs_url)
            if failure_code:
                self.stats["failed"] += 1
                return [], failure_code

            docs = parse_planning_register_documents(resp.text, base_url)

        except httpx.HTTPStatusError as exc:
            code = _http_status_failure_code(exc.response.status_code)
            logger.error("HTTP %s for %s: %s", exc.response.status_code, docs_url, exc)
            self.stats["failed"] += 1
            return [], code
        except httpx.TimeoutException as exc:
            logger.error("Timeout for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "timeout"
        except httpx.HTTPError as exc:
            logger.error("Network error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "network_error"
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return [], "unexpected_error"

        if not docs:
            self.stats["no_docs"] += 1
        else:
            self.stats["success"] += 1
        return docs, None

    async def download_document(
        self,
        doc_url: str,
        target_path: Path,
        referer: str = "",
        max_retries: int = 3,
    ) -> tuple[int, str]:
        """Download a single document. Returns (bytes_written, final_path_str).

        The path extension is fixed up based on magic bytes / Content-Type.
        Returns (0, str(target_path)) on failure.
        """
        domain = urlparse(doc_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                headers = {}
                if referer:
                    headers["Referer"] = referer
                resp = await self.client.get(doc_url, headers=headers)
                if resp.status_code == 200:
                    if not resp.content:
                        logger.warning("Empty 200 response for %s", doc_url)
                        return 0, str(target_path)
                    ext = _detect_extension(resp.content, resp.headers.get("Content-Type", ""))
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(resp.content)
                    return len(resp.content), str(final_path)
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                    backoff = 2**attempt
                    logger.info(
                        "HTTP %s for %s (attempt %d/%d), retrying in %ds",
                        resp.status_code,
                        doc_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning("HTTP %s for %s", resp.status_code, doc_url)
                return 0, str(target_path)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    backoff = 2**attempt
                    logger.info(
                        "%s for %s (attempt %d/%d), retrying in %ds",
                        type(exc).__name__,
                        doc_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error("Network error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            except Exception as exc:
                logger.error("Download error for %s: %s", doc_url, exc)
                return 0, str(target_path)
            finally:
                if acquired:
                    self._release(domain)
        return 0, str(target_path)
