from __future__ import annotations

import asyncio

import httpx

from src.generic_route_recovery import (
    build_elmbridge_documents_url,
    build_herefordshire_detail_url,
    build_kirklees_detail_url,
    build_peak_district_search_url,
    is_route_recovery_candidate,
    recover_application_route,
    route_recovery_family,
)


def test_route_recovery_family_recognises_known_generic_routes() -> None:
    assert (
        route_recovery_family(
            "Herefordshire",
            "https://www.herefordshire.gov.uk/info/200142/planning_services/planning_application_search",
        )
        == "herefordshire_direct"
    )
    assert (
        route_recovery_family(
            "Elmbridge",
            "https://emaps.elmbridge.gov.uk/ebc_planning.aspx?requesttype=parseTemplate&template=AdvancedSearchTab.tmplt",
        )
        == "elmbridge_emaps"
    )
    assert route_recovery_family("Peak District", "https://portal.peakdistrict.gov.uk/") == "peak_district"
    assert (
        route_recovery_family(
            "Kirklees",
            "https://www.kirklees.gov.uk/beta/planning-applications/search-for-planning-applications/default.aspx",
        )
        == "kirklees_direct"
    )


def test_route_recovery_family_ignores_specific_or_unknown_routes() -> None:
    assert route_recovery_family("Elmbridge", "https://emaps.elmbridge.gov.uk/details") is None
    assert route_recovery_family("St Albans", "") is None


def test_is_route_recovery_candidate_requires_positive_document_count_by_default() -> None:
    row = {
        "authority_name": "Kirklees",
        "documentation_url": "https://www.kirklees.gov.uk/beta/planning-applications/search-for-planning-applications/default.aspx",
        "n_documents": "0",
    }
    assert not is_route_recovery_candidate(row)
    assert is_route_recovery_candidate(row, include_non_docs_positive=True)


def test_route_builders_encode_slash_references() -> None:
    assert build_herefordshire_detail_url("214393", "P214393/L").endswith("id=214393&search=P214393%2FL")
    assert build_peak_district_search_url("NP/DDD/1121/1223").endswith("NP%2FDDD%2F1121%2F1223")
    assert build_kirklees_detail_url("2021/94597").endswith("id=2021%2F94597")

    elmbridge = build_elmbridge_documents_url("2021/2264")
    assert "PlanningPlansAndDocsTab.tmplt" in elmbridge
    assert "2021%2F2264" in elmbridge


def test_herefordshire_recovery_prefers_reference_field_when_present() -> None:
    row = {
        "uid": "P212480/V",
        "reference": "P212480/U",
        "authority_name": "Herefordshire",
        "documentation_url": "https://www.herefordshire.gov.uk/info/200142/planning_services/planning_application_search",
        "n_documents": "17",
    }
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        if request.url.host == "restservices.herefordshire.gov.uk":
            return httpx.Response(
                200,
                json={
                    "resultSets": [
                        {
                            "results": [
                                {
                                    "service_name": "application",
                                    "id": "212480",
                                }
                            ]
                        }
                    ]
                },
            )
        return httpx.Response(200, text="P212480/U planning-application-documents")

    async def run() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True) as client:
            return await recover_application_route(row, client)

    result = asyncio.run(run())

    assert result.recovered_url is not None
    assert result.recovered_url.endswith("id=212480&search=P212480%2FU")
    assert any("query=P212480%2FU" in url for url in requested_urls)
