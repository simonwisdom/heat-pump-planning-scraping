"""Helpers for recovering council/source links from PlanIt application pages."""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, FeatureNotFound

from .config import IDOX_USER_AGENT

_GENERIC_SOURCE_PATH_FRAGMENTS = (
    "/search/advanced",
    "/search/planning/advanced",
    "/search/generalsearch.aspx",
    "/searches/default.aspx",
)

# Matches "See source", "see  source", "See Source »", "See source (external)", etc.
# Anchored at the start; `\s+` (not `\s*`) requires real whitespace between the words.
_SEE_SOURCE_RE = re.compile(r"^\s*see\s+source\b", re.IGNORECASE)


def parse_other_fields(other_fields: dict[str, Any] | str | None) -> dict[str, Any]:
    """Normalise PlanIt ``other_fields`` payloads into a dict."""
    if not other_fields:
        return {}
    if isinstance(other_fields, dict):
        return other_fields
    if isinstance(other_fields, str):
        try:
            parsed = json.loads(other_fields)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def get_portal_hint_url(other_fields: dict[str, Any] | str | None) -> str | None:
    """Return the best URL hint available for portal-family classification."""
    payload = parse_other_fields(other_fields)
    docs_url = str(payload.get("docs_url") or "").strip()
    if docs_url:
        return docs_url
    source_url = str(payload.get("source_url") or "").strip()
    return source_url or None


def is_generic_source_url(url: str | None) -> bool:
    """Whether a PlanIt ``source_url`` points at a generic search entry page."""
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path.lower()
    fragment = (parsed.fragment or "").lower()
    return any(part in path for part in _GENERIC_SOURCE_PATH_FRAGMENTS) or fragment == "advancedsearch"


def _parse_html(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def extract_see_source_url(html: str, page_url: str) -> str | None:
    """Extract the council/source link from a PlanIt application page HTML."""
    soup = _parse_html(html)
    for anchor in soup.find_all("a", href=True):
        text = anchor.get_text(" ", strip=True)
        if _SEE_SOURCE_RE.match(text):
            return urljoin(page_url, anchor["href"])
    return None


def build_planit_recovery_client() -> httpx.AsyncClient:
    """Build an async client suitable for fetching PlanIt HTML pages."""
    return httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers={
            "User-Agent": IDOX_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )


def pick_usable_hint(other_fields: dict[str, Any] | str | None) -> tuple[str | None, str]:
    """Return (url, method) chosen from ``other_fields`` without any network I/O.

    ``method`` is one of ``"docs_url"``, ``"source_url"``, or ``"needs_fetch"``.
    """
    payload = parse_other_fields(other_fields)
    docs_url = str(payload.get("docs_url") or "").strip()
    if docs_url:
        return docs_url, "docs_url"
    source_url = str(payload.get("source_url") or "").strip()
    if source_url and not is_generic_source_url(source_url):
        return source_url, "source_url"
    return None, "needs_fetch"


async def fetch_see_source(client: httpx.AsyncClient, planit_link: str) -> str | None:
    """Fetch a PlanIt application page and extract its ``See source`` link."""
    response = await client.get(planit_link)
    response.raise_for_status()
    return extract_see_source_url(response.text, str(response.url))


async def recover_documentation_url(
    client: httpx.AsyncClient,
    *,
    planit_link: str | None,
    other_fields: dict[str, Any] | str | None,
) -> tuple[str | None, str]:
    """Recover a usable council/source URL for blank ``documentation_url`` rows."""
    url, method = pick_usable_hint(other_fields)
    if method != "needs_fetch":
        return url, method
    if not planit_link:
        return None, "no_planit_link"
    recovered = await fetch_see_source(client, planit_link)
    if recovered:
        return recovered, "see_source"
    return None, "see_source_missing"
