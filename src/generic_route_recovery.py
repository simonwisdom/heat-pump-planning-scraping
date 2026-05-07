"""Recover app-specific routes from generic planning-search URLs.

Some PlanIt records point only at a council search page even though the row has
positive document counts. These helpers turn known generic routes into
application-specific detail/document pages.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import quote

import httpx

from .config import IDOX_USER_AGENT
from .portal_classification import classify_url

SUPPORTED_AUTHORITIES = frozenset({"herefordshire", "elmbridge", "peak district", "kirklees"})


@dataclass(frozen=True)
class RouteRecoveryResult:
    uid: str
    authority_name: str
    original_url: str
    recovered_url: str | None
    portal_type: str | None
    method: str
    status: str
    note: str = ""


def _text(value: object | None) -> str:
    return str(value or "").strip()


def _positive_int(value: object | None) -> bool:
    try:
        return int(value or 0) > 0
    except (TypeError, ValueError):
        return False


def _application_reference(row: Mapping[str, object | None]) -> str:
    return _text(row.get("reference")) or _text(row.get("uid"))


async def _get_with_retry(client: httpx.AsyncClient, url: str) -> httpx.Response:
    last_exc: httpx.HTTPError | None = None
    for attempt in range(3):
        try:
            return await client.get(url)
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as exc:
            last_exc = exc
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("unreachable retry state")


def route_recovery_family(authority_name: str | None, documentation_url: str | None) -> str | None:
    """Return the recovery family for a known generic/blank route."""
    authority = _text(authority_name).lower()
    url = _text(documentation_url)
    lower_url = url.lower()

    if authority == "herefordshire" and lower_url.rstrip("/") == (
        "https://www.herefordshire.gov.uk/info/200142/planning_services/planning_application_search"
    ):
        return "herefordshire_direct"

    if authority == "elmbridge" and "emaps.elmbridge.gov.uk/ebc_planning.aspx" in lower_url:
        if "advancedsearchtab.tmplt" in lower_url:
            return "elmbridge_emaps"

    if authority == "peak district" and lower_url.rstrip("/") == "https://portal.peakdistrict.gov.uk":
        return "peak_district"

    if authority == "kirklees" and lower_url.rstrip("/") == (
        "https://www.kirklees.gov.uk/beta/planning-applications/search-for-planning-applications/default.aspx"
    ):
        return "kirklees_direct"

    return None


def is_route_recovery_candidate(
    row: Mapping[str, object | None],
    *,
    include_non_docs_positive: bool = False,
) -> bool:
    if not include_non_docs_positive and not _positive_int(row.get("n_documents")):
        return False
    return route_recovery_family(_text(row.get("authority_name")), _text(row.get("documentation_url"))) is not None


def build_herefordshire_search_api_url(reference: str) -> str:
    encoded = quote(reference, safe="")
    return (
        "https://restservices.herefordshire.gov.uk/search/planning"
        f"?query={encoded}&datefrom=&dateto=&status=all&format=json"
    )


def build_herefordshire_detail_url(application_id: str, reference: str) -> str:
    return (
        "https://www.herefordshire.gov.uk/info/200142/planning_services/planning_application_search/details"
        f"?id={quote(application_id, safe='')}&search={quote(reference, safe='')}"
    )


def build_peak_district_search_url(reference: str) -> str:
    return f"https://portal.peakdistrict.gov.uk/search?GeneralSearchTerm={quote(reference, safe='')}"


def build_elmbridge_documents_url(reference: str) -> str:
    encoded = quote(reference, safe="")
    return (
        "https://emaps.elmbridge.gov.uk/ebc_planning.aspx"
        "?requesttype=parseTemplate&template=PlanningPlansAndDocsTab.tmplt"
        f"&Filter=%5EAPPLICATION_NUMBER%5E%3D%27{encoded}%27"
        f"&appno:PARAM={encoded}&address:PARAM=&northing:PARAM=&easting:PARAM="
    )


def build_kirklees_detail_url(reference: str) -> str:
    return (
        "https://www.kirklees.gov.uk/beta/planning-applications/search-for-planning-applications/detail.aspx"
        f"?id={quote(reference, safe='')}"
    )


def build_route_recovery_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers={
            "User-Agent": IDOX_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        },
    )


def _result(
    row: Mapping[str, object | None],
    *,
    recovered_url: str | None,
    method: str,
    status: str,
    note: str = "",
) -> RouteRecoveryResult:
    return RouteRecoveryResult(
        uid=_text(row.get("uid")),
        authority_name=_text(row.get("authority_name")),
        original_url=_text(row.get("documentation_url")),
        recovered_url=recovered_url,
        portal_type=classify_url(recovered_url) if recovered_url else None,
        method=method,
        status=status,
        note=note,
    )


async def recover_application_route(
    row: Mapping[str, object | None],
    client: httpx.AsyncClient,
    *,
    verify: bool = True,
) -> RouteRecoveryResult:
    """Recover one app-specific route.

    Returns ``status='recovered'`` only when the route was built and either
    verified, or verification was explicitly skipped.
    """
    reference = _application_reference(row)
    if not reference:
        return _result(row, recovered_url=None, method="missing_reference", status="skipped")

    family = route_recovery_family(_text(row.get("authority_name")), _text(row.get("documentation_url")))
    if family is None:
        return _result(row, recovered_url=None, method="unsupported", status="skipped")

    try:
        if family == "herefordshire_direct":
            return await _recover_herefordshire(row, client, reference, verify=verify)
        if family == "peak_district":
            return await _recover_peak_district(row, client, reference, verify=verify)
        if family == "elmbridge_emaps":
            return await _recover_static_url(
                row,
                client,
                build_elmbridge_documents_url(reference),
                "elmbridge_documents_tab",
                verify=verify,
                expected_markers=(reference, "IAMLink.aspx"),
            )
        if family == "kirklees_direct":
            return await _recover_static_url(
                row,
                client,
                build_kirklees_detail_url(reference),
                "kirklees_detail",
                verify=verify,
                expected_markers=(reference, "downloaddocument.aspx"),
            )
    except httpx.HTTPError as exc:
        return _result(
            row,
            recovered_url=None,
            method=family,
            status="failed",
            note=f"{type(exc).__name__}: {exc}",
        )

    return _result(row, recovered_url=None, method=family, status="skipped", note="unsupported family")


async def _recover_herefordshire(
    row: Mapping[str, object | None],
    client: httpx.AsyncClient,
    reference: str,
    *,
    verify: bool,
) -> RouteRecoveryResult:
    response = await _get_with_retry(client, build_herefordshire_search_api_url(reference))
    response.raise_for_status()
    data = response.json()
    app_id = _pick_herefordshire_application_id(data)
    if not app_id:
        return _result(row, recovered_url=None, method="herefordshire_api", status="not_found")

    recovered_url = build_herefordshire_detail_url(app_id, reference)
    if not verify:
        return _result(row, recovered_url=recovered_url, method="herefordshire_api", status="recovered")

    detail = await _get_with_retry(client, recovered_url)
    detail.raise_for_status()
    markers = (reference, "planning-application-documents")
    if all(marker.lower() in detail.text.lower() for marker in markers):
        return _result(row, recovered_url=recovered_url, method="herefordshire_api", status="recovered")
    return _result(
        row,
        recovered_url=recovered_url,
        method="herefordshire_api",
        status="unverified",
        note="detail page did not contain expected document markers",
    )


def _pick_herefordshire_application_id(data: object) -> str | None:
    if not isinstance(data, dict):
        return None
    for result_set in data.get("resultSets", []):
        if not isinstance(result_set, dict):
            continue
        for result in result_set.get("results", []):
            if not isinstance(result, dict):
                continue
            if _text(result.get("service_name")).lower() != "application":
                continue
            app_id = _text(result.get("id"))
            if app_id:
                return app_id
    return None


async def _recover_peak_district(
    row: Mapping[str, object | None],
    client: httpx.AsyncClient,
    reference: str,
    *,
    verify: bool,
) -> RouteRecoveryResult:
    response = await _get_with_retry(client, build_peak_district_search_url(reference))
    response.raise_for_status()
    recovered_url = str(response.url)
    if "/result/" not in recovered_url:
        return _result(
            row,
            recovered_url=recovered_url,
            method="peak_district_search_redirect",
            status="not_found",
            note="search did not redirect to a result page",
        )
    if not verify:
        return _result(row, recovered_url=recovered_url, method="peak_district_search_redirect", status="recovered")
    if reference in response.text and "modules.PlanningSearch.partial.documents.documents" in response.text:
        return _result(row, recovered_url=recovered_url, method="peak_district_search_redirect", status="recovered")
    return _result(
        row,
        recovered_url=recovered_url,
        method="peak_district_search_redirect",
        status="unverified",
        note="detail page did not contain expected document markers",
    )


async def _recover_static_url(
    row: Mapping[str, object | None],
    client: httpx.AsyncClient,
    recovered_url: str,
    method: str,
    *,
    verify: bool,
    expected_markers: tuple[str, ...],
) -> RouteRecoveryResult:
    if not verify:
        return _result(row, recovered_url=recovered_url, method=method, status="recovered")

    for attempt in range(5):
        response = await _get_with_retry(client, recovered_url)
        response.raise_for_status()
        lower_text = response.text.lower()
        if all(marker.lower() in lower_text for marker in expected_markers):
            return _result(row, recovered_url=recovered_url, method=method, status="recovered")
        if attempt < 4:
            await asyncio.sleep(0.5 * (attempt + 1))
    return _result(
        row,
        recovered_url=recovered_url,
        method=method,
        status="unverified",
        note="detail page did not contain expected document markers",
    )
