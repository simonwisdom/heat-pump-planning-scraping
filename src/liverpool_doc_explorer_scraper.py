"""Liverpool legacy DocumentExplorer (Northgate MVMPRD) recovery scraper.

Recon date: 2026-04-30
Author: portal-recon agent

# What's in the DB

`portal_type='liverpool_doc_explorer'` rows in ``ashp.db`` carry URLs like::

    http://northgate.liverpool.gov.uk/DocumentExplorer/Application/folderview.aspx
        ?type=MVMPRD_DC_PLANAPP&key=<numeric_doc_folder_id>

This is a legacy Northgate "Document Explorer" (MVMPRD) folder-view served
from ``northgate.liverpool.gov.uk`` (single IPv4: 213.105.41.215).
``key`` is a stable numeric folder id — the same key may appear for several
DB ``uid`` values when applications are linked (parent / amendment / listed
building consent share a folder).

# Why this isn't a normal Northgate handler

I added this as a separate module rather than extending
``src/northgate_scraper.py`` for two reasons:

1. The host is **dead from every IP we tried** (local + the VPS at
   178.104.201.79). Port 80 accepts the TCP connection but RSTs every HTTP
   request, regardless of UA / Accept / path. Port 443 is closed. So the
   legacy ``/DocumentExplorer/...`` URL is unreachable, and no amount of
   protocol coaxing fixes it. ``northgate_scraper.py`` cannot scrape this.
2. Liverpool has migrated to a **completely different system**:
   ``lar.liverpool.gov.uk`` (Idox-hosted SmartAdmin / iApps planning portal,
   the same family already handled by ``src/smartadmin_scraper.py``). The
   legacy Northgate ``key`` does **not** map to the new ``id`` — different
   numeric namespaces. Recovery has to go via search-by-application-reference.

So the recovery path is: take the row's ``uid`` (e.g. ``20F/0182``),
search the new LAR portal for it, follow the result's
``getApplication&id=<new_id>``, and parse the document table on the detail
page. That is exactly what ``SmartAdminDocumentScraper.scrape_documents``
already does for other Idox-SmartAdmin councils — so this module is a thin
shim that rewrites the dead Northgate URL to the live LAR search URL and
delegates.

# Caveats / status

- The LAR portal was returning ``Error (IDX002)`` (HTTP 406) on every URL
  — root, search, and ``getApplication`` — for both my IP and the VPS at
  the time of recon (2026-04-30). This is the same Idox edge-block that
  ``smartadmin_scraper.looks_like_idx002_block`` already detects. We
  cannot know whether the block is a transient outage, a wider IP block,
  or a permanent posture without a successful test against the live portal.
- Because the live portal was IDX002-blocked, **0/12 sample apps were
  actually downloaded** during recon. Sample manifests record the
  ``waf_idx002`` failure code only.
- The Wayback Machine confirms the structural shape of the new portal
  (form fields, document table, ``fa=downloadDocument&id=...`` URLs).
  Captured snapshots used to validate the parser:
  ``https://web.archive.org/web/20220702193854/https://lar.liverpool.gov.uk
   /planning/index.html?fa=getApplication&id=106428``.
- Two of the 12 DB rows share a ``key`` value. They have distinct ``uid``s
  so the search-by-reference path resolves them correctly.

# Recommended actions for the human reviewer

1. Reclassify these 12 rows from ``portal_type='liverpool_doc_explorer'``
   to ``portal_type='smartadmin'`` once the LAR portal is reachable, OR
2. Keep this dedicated module as the URL-rewrite seam (preferred — clearer
   audit trail for the migration) and let the runner dispatch on the rule
   ``host == 'northgate.liverpool.gov.uk'``.
3. Either way, the scraper itself is just SmartAdmin under the hood.
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

from .smartadmin_scraper import SmartAdminDocumentScraper

logger = logging.getLogger(__name__)


LAR_SEARCH_URL = "https://lar.liverpool.gov.uk/planning/index.html?fa=search"

# Hostnames we know belong to the legacy Liverpool Northgate DocumentExplorer.
# Only the literal host should match; we do not want to catch other
# ``northgate.*`` councils here.
_LEGACY_LIVERPOOL_HOSTS = frozenset({"northgate.liverpool.gov.uk"})


def is_liverpool_doc_explorer_url(docs_url: str) -> bool:
    """True for the legacy ``northgate.liverpool.gov.uk/DocumentExplorer/...`` shape."""
    if not docs_url:
        return False
    host = urlparse(docs_url).netloc.lower()
    return host in _LEGACY_LIVERPOOL_HOSTS


def rewrite_to_lar_search(_legacy_docs_url: str) -> str:
    """Map the dead Northgate folder URL to the live LAR search URL.

    The legacy ``key`` does not resolve on the new portal, so we discard
    it and fall back to search-by-reference (the caller passes the
    application ``uid`` to ``scrape_documents``).
    """
    return LAR_SEARCH_URL


class LiverpoolDocExplorerScraper:
    """Thin shim over ``SmartAdminDocumentScraper`` for legacy Liverpool URLs.

    Usage mirrors SmartAdmin: pass the row's ``application_reference`` (the
    DB ``uid``) alongside the original docs URL. The shim rewrites the URL
    to the LAR search endpoint before delegating.
    """

    def __init__(self, **kwargs) -> None:
        self._inner = SmartAdminDocumentScraper(**kwargs)

    async def __aenter__(self) -> "LiverpoolDocExplorerScraper":
        await self._inner.__aenter__()
        return self

    async def __aexit__(self, *args) -> None:
        await self._inner.__aexit__(*args)

    @property
    def stats(self) -> dict:
        return self._inner.stats

    async def scrape_documents(
        self,
        docs_url: str,
        application_reference: str = "",
    ) -> tuple[list[dict], str | None]:
        """Rewrite the legacy URL and delegate to the SmartAdmin flow."""
        if not is_liverpool_doc_explorer_url(docs_url):
            # Defensive: caller mis-routed. Still try, but log loudly.
            logger.warning("Non-Liverpool URL routed to LiverpoolDocExplorerScraper: %s", docs_url)

        rewritten = rewrite_to_lar_search(docs_url)
        return await self._inner.scrape_documents(rewritten, application_reference)

    async def download_document(
        self,
        doc_url: str,
        target_path: Path,
        referer: str = "",
        max_retries: int = 3,
    ) -> tuple[int, str]:
        return await self._inner.download_document(doc_url, target_path, referer, max_retries)
