"""Northgate-family document scrapers.

PlanIt classifies these councils as ``portal_type='northgate'`` because they
run Northgate Planning Explorer for search, but each bolts a different
document-management system onto it:

- Camden → CMWebDrawer (Micro Focus Content Manager). Static HTML table of
  records, direct GET of ``/CMWebDrawer/Record/{id}/file/document?inline``.
- Wandsworth → IAM. ASP.NET WebForms: ``comments.aspx`` shows a summary of
  document *types*; each type has a ``__doPostBack`` that must be fired
  (with ViewState + EventValidation) to reveal the list of documents for
  that type. Each doc is an ``IAMLink.aspx?docid=N`` URL that 302-redirects
  to the cached PDF.
- Blackburn & Runnymede → Idox PublicAccess_LIVE. The landing ``.aspx``
  redirects to ``/PublicAccess_LIVE/SearchResult/RunThirdPartySearch``, which
  embeds the document list as JSON in ``var model = {...}``. Each doc has a
  Guid; ViewDocument?id=<GUID> returns the PDF.
- Conwy → Civica EDM (edm.secure.conwy.gov.uk). The UI is a SPA but its API
  is callable directly: POST keyobject/pagedsearch to turn ``ref_no`` into a
  ``KeyNumber``, POST doc/list to get the ``CompleteDocument`` array, and
  GET doc/pagestream?DocNo=<N>&pdf=true to download a server-rendered PDF.

URLs in the DB dispatch to the right handler based on netloc. Some stored
URLs point to retired hosts and are rewritten to their current equivalents:

- ``documents.runnymede.gov.uk/AniteIM.WebSearch/...`` (dead) →
  ``docs.runnymede.gov.uk/Publicaccess_LIVE/...``
- ``www.conwy.gov.uk/.../Planning-Explorer-Docs-EDM.aspx?ref_no=...`` (CMS
  wrapper) and ``edm.conwy.gov.uk/Planning/lg/dialog.page?...ref_no=...``
  (retired) → ``edm.secure.conwy.gov.uk/planning/planning-documents?ref_no=...``
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import ssl
import time
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

import httpx
import truststore

from .config import (
    IDOX_MAX_CONCURRENT_DOMAINS,
    IDOX_RATE_LIMIT_PER_DOMAIN,
    IDOX_USER_AGENT,
)

logger = logging.getLogger(__name__)


# ---------- Camden (CMWebDrawer) parsing ----------

_CAMDEN_ROW_RE = re.compile(
    r"<tr>\s*"
    r'<td[^>]*class="nowrap"[^>]*>([^<]*)</td>\s*'  # date
    r'<td[^>]*>\s*<a[^>]+href="'
    r'(/CMWebDrawer/Record/(\d+)/file/document[^"]*)"'
    r"[^>]*>([^<]*)</a>\s*</td>\s*"  # title link
    r"<td[^>]*>\s*(?:<a[^>]*>([^<]*)</a>)?\s*</td>",  # doc type
    re.DOTALL,
)


def parse_camden_documents(html: str, base_url: str) -> list[dict]:
    """Parse the CMWebDrawer record table. Returns one entry per record id."""
    documents = []
    seen_ids: set[str] = set()
    for m in _CAMDEN_ROW_RE.finditer(html):
        date_str = m.group(1).strip()
        href = m.group(2)
        rec_id = m.group(3)
        title = (m.group(4) or "").strip()
        doc_type = (m.group(5) or "").strip()
        if rec_id in seen_ids:
            continue
        seen_ids.add(rec_id)
        documents.append(
            {
                "date_published": date_str,
                "document_type": doc_type,
                "description": title,
                "drawing_number": "",
                "document_url": urljoin(base_url, href),
            }
        )
    return documents


# ---------- Wandsworth (IAM) parsing ----------

_WANDS_FIELD_RE_TMPL = r'name="{name}"[^>]*value="([^"]*)"'
_WANDS_CHOICE_RE = re.compile(
    r'<span id="gvDocs_lblChoice_\d+">([^<]*)</span>.*?'
    r"__doPostBack\(&#39;(gvDocs\$ctl\d+\$lnkDShow)&#39;",
    re.DOTALL,
)
_WANDS_ROW_RE = re.compile(
    r'<span id="gvResults_Label1_\d+">([^<]*)</span>.*?'
    r'<span id="gvResults_Label2_\d+">([^<]*)</span>.*?'
    r'href="([^"]*IAMLink\.aspx\?docid=\d+)"',
    re.DOTALL,
)


def extract_viewstate_fields(html: str) -> dict[str, str]:
    fields = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
        m = re.search(_WANDS_FIELD_RE_TMPL.format(name=name), html)
        fields[name] = m.group(1) if m else ""
    return fields


def extract_wandsworth_doctypes(html: str) -> list[tuple[str, str]]:
    """Return [(type_name, postback_target), ...] from the landing page."""
    return [(m.group(1).strip(), m.group(2)) for m in _WANDS_CHOICE_RE.finditer(html)]


def parse_wandsworth_postback(html: str, doc_type: str) -> list[dict]:
    """Parse one postback response (all docs of a single type)."""
    documents = []
    for m in _WANDS_ROW_RE.finditer(html):
        date_str = m.group(1).strip()
        description = m.group(2).strip()
        href = m.group(3)
        documents.append(
            {
                "date_published": date_str,
                "document_type": doc_type,
                "description": description,
                "drawing_number": "",
                "document_url": href,
            }
        )
    return documents


# ---------- PublicAccess (Blackburn / Runnymede) parsing ----------

# The landing page embeds the doc list as `var model = {...};`. It's a
# single-line JSON blob terminated by `;` at end of statement.
_PA_MODEL_RE = re.compile(r"var\s+model\s*=\s*(\{.*?\})\s*;\s*\n", re.DOTALL)
_PA_VIEW_URL_RE = re.compile(r"var\s+viewDocumentUrl\s*=\s*'([^']+)'")


def parse_publicaccess_documents(html: str, base_url: str) -> list[dict]:
    """Parse the PublicAccess_LIVE document listing.

    The JSON in ``var model`` carries ``Rows`` with ``Guid``, ``Doc_Type``,
    ``Doc_Ref2`` (description) and ``Date_Received``. The download path comes
    from ``var viewDocumentUrl`` in the same script (Blackburn and Runnymede
    use different casings — ``/PublicAccess_LIVE`` vs ``/PublicAccess_Live``).
    """
    m = _PA_MODEL_RE.search(html)
    if not m:
        return []
    try:
        model = json.loads(m.group(1))
    except json.JSONDecodeError as exc:
        logger.warning("PublicAccess model JSON parse failed: %s", exc)
        return []

    view_match = _PA_VIEW_URL_RE.search(html)
    view_path = view_match.group(1) if view_match else "/PublicAccess_LIVE/Document/ViewDocument"

    documents: list[dict] = []
    for row in model.get("Rows", []):
        guid = (row.get("Guid") or "").strip()
        if not guid:
            continue
        doc_url = urljoin(base_url, f"{view_path}?id={guid}")
        documents.append(
            {
                "date_published": (row.get("Date_Received") or "").strip(),
                "document_type": (row.get("Doc_Type") or "").strip(),
                "description": (row.get("Doc_Ref2") or "").strip(),
                "drawing_number": "",
                "document_url": doc_url,
            }
        )
    return documents


# ---------- Conwy (Civica EDM) parsing ----------

CONWY_API_BASE = "https://edm.secure.conwy.gov.uk/w2webparts/Resource/Civica/Handler.ashx"
CONWY_LANDING_BASE = "https://edm.secure.conwy.gov.uk/planning/planning-documents"


def parse_conwy_key_number(body: dict) -> str | None:
    """Extract KeyNumber from the keyobject/pagedsearch JSON response."""
    ko = body.get("KeyObjects") or []
    if not ko:
        return None
    return ko[0].get("KeyNumber")


def parse_conwy_documents(body: dict) -> list[dict]:
    """Parse Civica doc/list JSON into our standard document shape.

    DocDate is ISO-like (``2025-10-27T16:24:00.0000000``); we trim to date.
    TypeCode maps to DocDesc (e.g. ``Revised Final Decision Notice (Web)``).
    Download URL is the server-rendered PDF endpoint.
    """
    documents: list[dict] = []
    for doc in body.get("CompleteDocument", []):
        doc_no = str(doc.get("DocNo") or "").strip()
        if not doc_no:
            continue
        date_raw = doc.get("ReceivedDate") or doc.get("DocDate") or ""
        date_str = date_raw.split("T", 1)[0] if date_raw else ""
        documents.append(
            {
                "date_published": date_str,
                "document_type": (doc.get("DocDesc") or "").strip(),
                "description": (doc.get("Title") or doc.get("DocDesc") or "").strip(),
                "drawing_number": "",
                "document_url": f"{CONWY_API_BASE}/doc/pagestream?DocNo={doc_no}&pdf=true",
            }
        )
    return documents


# ---------- URL rewriting ----------

# Legacy Runnymede AniteIM host (documents.runnymede.gov.uk) is NXDOMAIN.
# The council migrated to docs.runnymede.gov.uk running Idox PublicAccess.
# Folder references from the AniteIM era (e.g. PLN201895) may or may not
# exist in the new system; we still try — the parser gracefully returns
# an empty list if no rows are present.
_LEGACY_RUNNYMEDE_RE = re.compile(
    r"^https?://documents\.runnymede\.gov\.uk/AniteIM\.WebSearch/",
    re.IGNORECASE,
)

# Conwy retired ``edm.conwy.gov.uk`` and the ``www.conwy.gov.uk`` CMS
# wrapper that iframed it. Both carry the ``ref_no`` query param, which is
# what the new Civica EDM endpoint needs.
_CONWY_LEGACY_HOSTS = ("www.conwy.gov.uk", "edm.conwy.gov.uk")


def _rewrite_conwy(docs_url: str) -> str | None:
    parsed = urlparse(docs_url)
    host = parsed.netloc.lower()
    if host not in _CONWY_LEGACY_HOSTS:
        return None
    ref = (parse_qs(parsed.query).get("ref_no") or [""])[0]
    if not ref:
        return None
    return f"{CONWY_LANDING_BASE}?ref_no={ref}&viewdocs=true"


def rewrite_legacy_url(docs_url: str) -> str:
    if _LEGACY_RUNNYMEDE_RE.match(docs_url):
        return _LEGACY_RUNNYMEDE_RE.sub(
            "https://docs.runnymede.gov.uk/Publicaccess_LIVE/",
            docs_url,
        )
    conwy = _rewrite_conwy(docs_url)
    if conwy is not None:
        return conwy
    return docs_url


# ---------- Handler routing ----------


def _handler_for_url(docs_url: str) -> str:
    host = urlparse(docs_url).netloc.lower()
    if "camden.gov.uk" in host:
        return "camden"
    if "wandsworth.gov.uk" in host:
        return "wandsworth"
    if "planningdms-live.blackburn.gov.uk" in host:
        return "publicaccess"
    if "docs.runnymede.gov.uk" in host or "documents.runnymede.gov.uk" in host:
        return "publicaccess"
    if "edm.secure.conwy.gov.uk" in host or host in _CONWY_LEGACY_HOSTS:
        return "conwy"
    return "unsupported"


# ---------- Scraper ----------


class NorthgateDocumentScraper:
    """Async scraper dispatching to Camden / Wandsworth handlers by URL."""

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
        self.stats = {"success": 0, "failed": 0, "no_docs": 0, "unsupported": 0}

    async def __aenter__(self):
        # Camden (camdocs.camden.gov.uk) serves only the leaf cert, omitting
        # the Sectigo intermediate. certifi doesn't AIA-fetch, so we delegate
        # verification to the OS trust store via truststore.
        ssl_ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "User-Agent": IDOX_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            follow_redirects=True,
            verify=ssl_ctx,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with NorthgateDocumentScraper() as scraper:'")
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

    async def _post(self, url: str, data: dict, *, json_body: bool = False, **kwargs) -> httpx.Response:
        domain = urlparse(url).netloc
        acquired = False
        try:
            await self._rate_limit(domain)
            acquired = True
            if json_body:
                return await self.client.post(url, json=data, **kwargs)
            return await self.client.post(url, data=data, **kwargs)
        finally:
            if acquired:
                self._release(domain)

    async def scrape_documents(self, docs_url: str) -> list[dict]:
        docs_url = rewrite_legacy_url(docs_url)
        handler = _handler_for_url(docs_url)
        try:
            if handler == "camden":
                docs = await self._scrape_camden(docs_url)
            elif handler == "wandsworth":
                docs = await self._scrape_wandsworth(docs_url)
            elif handler == "publicaccess":
                docs = await self._scrape_publicaccess(docs_url)
            elif handler == "conwy":
                docs = await self._scrape_conwy(docs_url)
            else:
                logger.warning("Unsupported Northgate host: %s", docs_url)
                self.stats["unsupported"] += 1
                return []
        except httpx.HTTPError as exc:
            logger.error("HTTP error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return []
        except Exception as exc:
            logger.error("Unexpected error for %s: %s", docs_url, exc)
            self.stats["failed"] += 1
            return []

        if not docs:
            self.stats["no_docs"] += 1
        else:
            self.stats["success"] += 1
        return docs

    async def _scrape_camden(self, docs_url: str) -> list[dict]:
        # The old HPRMWebDrawer host in the DB redirects to CMWebDrawer;
        # follow_redirects handles that transparently.
        resp = await self._get(docs_url)
        if resp.status_code != 200:
            logger.warning("Camden landing HTTP %s for %s", resp.status_code, docs_url)
            return []
        parsed = urlparse(str(resp.url))
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        return parse_camden_documents(resp.text, base_url)

    async def _scrape_conwy(self, docs_url: str) -> list[dict]:
        ref = (parse_qs(urlparse(docs_url).query).get("ref_no") or [""])[0]
        if not ref:
            logger.warning("Conwy URL missing ref_no: %s", docs_url)
            return []

        # Override the default Accept header — with text/html preferred,
        # Conwy's API returns a debug HTML stub instead of JSON.
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }
        search = await self._post(
            f"{CONWY_API_BASE}/keyobject/pagedsearch",
            data={"refType": "PBDC", "searchFields": {"ref_no": ref}},
            headers=headers,
            json_body=True,
        )
        if search.status_code != 200:
            logger.warning("Conwy pagedsearch HTTP %s for ref %s", search.status_code, ref)
            return []
        key_number = parse_conwy_key_number(search.json())
        if not key_number:
            return []

        doc_list = await self._post(
            f"{CONWY_API_BASE}/doc/list",
            data={"KeyNumb": key_number, "RefType": "PBDC"},
            headers=headers,
            json_body=True,
        )
        if doc_list.status_code != 200:
            logger.warning(
                "Conwy doc/list HTTP %s for ref %s (key %s)",
                doc_list.status_code,
                ref,
                key_number,
            )
            return []
        return parse_conwy_documents(doc_list.json())

    async def _scrape_publicaccess(self, docs_url: str) -> list[dict]:
        # The ExternalEntryPoint.aspx 302s to /PublicAccess_*/SearchResult/
        # RunThirdPartySearch?FileSystemId=<code>&FOLDER1_REF=<ref>. httpx
        # follows redirects by default; we use the final URL to resolve
        # the view-document path to an absolute URL.
        resp = await self._get(docs_url)
        if resp.status_code != 200:
            logger.warning("PublicAccess landing HTTP %s for %s", resp.status_code, docs_url)
            return []
        parsed = urlparse(str(resp.url))
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        return parse_publicaccess_documents(resp.text, base_url)

    async def _scrape_wandsworth(self, docs_url: str) -> list[dict]:
        landing = await self._get(docs_url)
        if landing.status_code != 200:
            logger.warning("Wandsworth landing HTTP %s for %s", landing.status_code, docs_url)
            return []
        html = landing.text
        fields = extract_viewstate_fields(html)
        doctypes = extract_wandsworth_doctypes(html)
        if not doctypes:
            return []

        post_url = str(landing.url)  # the resolved URL is what postbacks POST to
        documents: list[dict] = []
        for doc_type, target in doctypes:
            body = {
                "__EVENTTARGET": target,
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": fields["__VIEWSTATE"],
                "__VIEWSTATEGENERATOR": fields["__VIEWSTATEGENERATOR"],
                "__EVENTVALIDATION": fields["__EVENTVALIDATION"],
            }
            try:
                resp = await self._post(
                    post_url,
                    body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
            except httpx.HTTPError as exc:
                logger.warning("Wandsworth postback failed for %s/%s: %s", docs_url, doc_type, exc)
                continue
            if resp.status_code != 200:
                logger.warning(
                    "Wandsworth postback HTTP %s for %s/%s",
                    resp.status_code,
                    docs_url,
                    doc_type,
                )
                continue
            documents.extend(parse_wandsworth_postback(resp.text, doc_type))

        # Dedupe by docid in case a doc appears under multiple types
        seen: set[str] = set()
        unique: list[dict] = []
        for d in documents:
            qs = parse_qs(urlparse(d["document_url"]).query)
            docid = (qs.get("docid") or [""])[0]
            if docid and docid in seen:
                continue
            if docid:
                seen.add(docid)
            unique.append(d)
        return unique

    async def download_document(
        self,
        document_url: str,
        target_path: Path,
        referer: str,
        max_retries: int = 3,
    ) -> tuple[int, Path]:
        """Download a single document. Returns (bytes_written, final_path).

        Camden in particular serves docs in their native format (.pdf, .docx,
        .tif, .msg, ...), so we fix up the extension based on Content-Type /
        magic bytes before writing to disk. Returns (0, target_path) on
        failure. Retries transient errors (timeout, 429, 5xx) with
        exponential backoff.
        """
        domain = urlparse(document_url).netloc
        for attempt in range(max_retries):
            acquired = False
            try:
                await self._rate_limit(domain)
                acquired = True
                resp = await self.client.get(document_url, headers={"Referer": referer})
                if resp.status_code == 200:
                    if not resp.content:
                        logger.warning("Empty 200 response for %s", document_url)
                        return 0, target_path
                    ext = _detect_extension(resp.content, resp.headers.get("Content-Type", ""))
                    final_path = target_path.with_suffix(ext)
                    final_path.parent.mkdir(parents=True, exist_ok=True)
                    final_path.write_bytes(resp.content)
                    return len(resp.content), final_path
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                    backoff = 2**attempt
                    logger.info(
                        "HTTP %s for %s (attempt %d/%d), retrying in %ds",
                        resp.status_code,
                        document_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.warning("HTTP %s for %s", resp.status_code, document_url)
                return 0, target_path
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt < max_retries - 1:
                    backoff = 2**attempt
                    logger.info(
                        "%s for %s (attempt %d/%d), retrying in %ds",
                        type(exc).__name__,
                        document_url,
                        attempt + 1,
                        max_retries,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error("Network error for %s: %s", document_url, exc)
                return 0, target_path
            except Exception as exc:
                logger.error("Download error for %s: %s", document_url, exc)
                return 0, target_path
            finally:
                if acquired:
                    self._release(domain)
        return 0, target_path


_MAGIC_EXTS: list[tuple[bytes, str]] = [
    (b"%PDF", ".pdf"),
    (b"PK\x03\x04", ".zip"),  # refined below for docx/xlsx/pptx
    (b"\xd0\xcf\x11\xe0", ".doc"),  # also xls/ppt (OLE2)
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
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.ms-outlook": ".msg",
    "text/plain": ".txt",
    "text/html": ".html",
}


def _detect_extension(content: bytes, content_type: str) -> str:
    """Pick an extension based on magic bytes, falling back to Content-Type."""
    head = content[:8]
    for sig, ext in _MAGIC_EXTS:
        if head.startswith(sig):
            # Distinguish zip-based Office formats by CT when possible
            if ext == ".zip" and content_type:
                ct = content_type.split(";")[0].strip().lower()
                office = _CTYPE_EXTS.get(ct)
                if office:
                    return office
            return ext
    ct = (content_type or "").split(";")[0].strip().lower()
    return _CTYPE_EXTS.get(ct, ".pdf")
