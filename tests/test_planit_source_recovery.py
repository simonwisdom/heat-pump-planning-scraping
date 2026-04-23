from __future__ import annotations

import asyncio

import httpx
import pytest

from src.planit_source_recovery import (
    extract_see_source_url,
    get_portal_hint_url,
    is_generic_source_url,
    pick_usable_hint,
    recover_documentation_url,
)


def test_extract_see_source_url_picks_non_generic_external_anchor() -> None:
    html = """
    <html>
      <body>
        <a href="/internal">Internal</a>
        <a href="https://southhams.planning-register.co.uk/Search/Advanced">South West Devon</a>
        <a href="https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC">0628/26/ARC</a>
      </body>
    </html>
    """
    recovered = extract_see_source_url(html, "https://www.planit.org.uk/planapplic/SouthWestDevon/0628/26/ARC/")
    assert recovered == "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC"


def test_extract_see_source_url_skips_same_host_anchors() -> None:
    html = """
    <a href="/planarea/Wealden/">Wealden</a>
    <a href="https://planning.wealden.gov.uk/Planning/Display/WD/2023/2718/F">WD/2023/2718/F</a>
    """
    recovered = extract_see_source_url(html, "https://www.planit.org.uk/planapplic/Wealden/WD/2023/2718/F/")
    assert recovered == "https://planning.wealden.gov.uk/Planning/Display/WD/2023/2718/F"


def test_extract_see_source_url_ignores_js_template_strings_in_script_blocks() -> None:
    # PlanIt includes JS templates that look like <a>See source</a> inside <script>.
    # bs4 treats <script> contents as text, so these must not be picked up.
    html = """
    <script>var x = '<a href="https://bad.example/">See source</a>';</script>
    <a href="https://planning.wealden.gov.uk/Planning/Display/WD/2023/2718/F">WD/2023/2718/F</a>
    """
    recovered = extract_see_source_url(html, "https://www.planit.org.uk/planapplic/Wealden/WD/2023/2718/F/")
    assert recovered == "https://planning.wealden.gov.uk/Planning/Display/WD/2023/2718/F"


def test_extract_see_source_url_returns_none_when_only_generic_anchors() -> None:
    html = '<a href="https://southhams.planning-register.co.uk/Search/Advanced">South West Devon</a>'
    assert extract_see_source_url(html, "https://www.planit.org.uk/planapplic/SouthWestDevon/X/") is None


def test_extract_see_source_url_returns_none_when_no_external_anchors() -> None:
    html = '<a href="/internal">Internal</a><a href="#top">Top</a>'
    assert extract_see_source_url(html, "https://www.planit.org.uk/p/1") is None


def test_get_portal_hint_url_prefers_docs_url_then_source_url() -> None:
    assert (
        get_portal_hint_url({"docs_url": "https://docs.example/app", "source_url": "https://source.example/app"})
        == "https://docs.example/app"
    )
    assert get_portal_hint_url({"source_url": "https://source.example/app"}) == "https://source.example/app"
    assert get_portal_hint_url(None) is None


def test_pick_usable_hint_skips_generic_source_urls() -> None:
    url, method = pick_usable_hint({"source_url": "https://westdevon.planning-register.co.uk/Search/Advanced"})
    assert (url, method) == (None, "needs_fetch")


def test_pick_usable_hint_returns_specific_source_url() -> None:
    url, method = pick_usable_hint(
        {"source_url": "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC"}
    )
    assert method == "source_url"
    assert url == "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://westdevon.planning-register.co.uk/Search/Advanced", True),
        ("https://vogonline.planning-register.co.uk/Search/Planning/Advanced", True),
        ("https://www.rbkc.gov.uk/planning/searches/default.aspx?adv=1#advancedSearch", True),
        ("https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC", False),
        (None, False),
        # New: ASP-style search-entry pages
        ("https://planning.dorsetcouncil.gov.uk/advsearch.aspx", True),
        # New: bare /pr/s/ landing page (Wiltshire). Must NOT match /pr/s/register-view (Arcus detail page).
        ("https://development.wiltshire.gov.uk/pr/s/?tabset-167f1=3", True),
        ("https://development.wiltshire.gov.uk/pr/s/", True),
        ("https://publicregister.haringey.gov.uk/pr/s/register-view?c__r=Arcus_BE_Public_Register", False),
        # New: bare St Albans search landing page
        ("https://planningapplications.stalbans.gov.uk/planning/search-applications", True),
        ("https://planningapplications.stalbans.gov.uk/planning/search-applications/", True),
    ],
)
def test_is_generic_source_url(url: str | None, expected: bool) -> None:
    assert is_generic_source_url(url) is expected


def test_recover_documentation_url_uses_specific_source_url_without_fetching() -> None:
    async def _run() -> tuple[str | None, str]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: pytest.fail("network fetch not expected")),
        ) as client:
            return await recover_documentation_url(
                client,
                planit_link="https://www.planit.org.uk/planapplic/Example/1/",
                other_fields={"source_url": "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC"},
            )

    recovered, method = asyncio.run(_run())

    assert method == "source_url"
    assert recovered == "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC"


def test_recover_documentation_url_fetches_see_source_for_generic_source_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://www.planit.org.uk/planapplic/SouthWestDevon/0628/26/ARC/"
        return httpx.Response(
            200,
            text=(
                "<html><body>"
                '<a href="https://southhams.planning-register.co.uk/Search/Advanced">South West Devon</a>'
                '<a href="https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC">'
                "0628/26/ARC</a></body></html>"
            ),
        )

    async def _run() -> tuple[str | None, str]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await recover_documentation_url(
                client,
                planit_link="https://www.planit.org.uk/planapplic/SouthWestDevon/0628/26/ARC/",
                other_fields={"source_url": "https://westdevon.planning-register.co.uk/Search/Advanced"},
            )

    recovered, method = asyncio.run(_run())

    assert method == "see_source"
    assert recovered == "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC"


def test_recover_documentation_url_returns_no_planit_link_when_blank() -> None:
    async def _run() -> tuple[str | None, str]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: pytest.fail("network fetch not expected")),
        ) as client:
            return await recover_documentation_url(client, planit_link=None, other_fields=None)

    assert asyncio.run(_run()) == (None, "no_planit_link")
