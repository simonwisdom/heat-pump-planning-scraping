from unittest.mock import AsyncMock

import pytest

from src.planit_client import PlanItClient, RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_backoff_logic(monkeypatch):
    limiter = RateLimiter(
        max_requests=2,
        cooldown_base=10.0,
        cooldown_max=20.0,
        min_request_gap=2.0,
    )

    current_time = 100.0
    monkeypatch.setattr("src.planit_client.time.monotonic", lambda: current_time)

    sleeps: list[float] = []

    async def fake_sleep(delay: float):
        sleeps.append(delay)

    monkeypatch.setattr("src.planit_client.asyncio.sleep", fake_sleep)

    # First request recorded
    limiter.record_request()

    # Second call: only 1s elapsed (< 2s minimum), should sleep the gap
    current_time = 101.0
    await limiter.wait_if_needed()
    assert sleeps == [1.0]

    # After a 429, request_count is forced to max, so next wait triggers cooldown
    limiter.record_429()
    current_time = 105.0  # enough time elapsed for min gap
    await limiter.wait_if_needed()

    # Should have slept for cooldown: 10 * 1.5^1 = 15, capped at 20
    assert sleeps[1] == pytest.approx(15.0)
    assert limiter.request_count == 0

    # Another 429 escalates further
    limiter.record_request()
    limiter.record_request()
    limiter.record_429()
    current_time = 110.0
    await limiter.wait_if_needed()

    # cooldown: 10 * 1.5^2 = 22.5, capped at 20
    assert sleeps[2] == pytest.approx(20.0)


@pytest.mark.asyncio
async def test_search_applications_builds_correct_params(monkeypatch):
    client = PlanItClient()
    mock_request = AsyncMock(return_value={"records": [], "total": 0})
    monkeypatch.setattr(client, "_request", mock_request)

    await client.search_applications(
        search='"air source heat pump" OR ashp',
        start_date="2025-01-01",
        end_date="2025-01-31",
        app_type="Full",
        app_state="Undecided",
        auth="Example Council",
        pg_sz=123,
        index=9,
    )

    mock_request.assert_awaited_once_with(
        "applics/json",
        {
            "pg_sz": 123,
            "index": 9,
            "search": '"air source heat pump" OR ashp',
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
            "app_type": "Full",
            "app_state": "Undecided",
            "auth": "Example Council",
        },
    )


@pytest.mark.asyncio
async def test_search_applications_caps_page_size_to_configured_limit(monkeypatch):
    monkeypatch.setattr("src.planit_client.PLANIT_PAGE_SIZE", 300)

    client = PlanItClient()
    mock_request = AsyncMock(return_value={"records": [], "total": 0})
    monkeypatch.setattr(client, "_request", mock_request)

    await client.search_applications(
        search="ashp",
        start_date="2025-01-01",
        end_date="2025-01-31",
        pg_sz=5000,
    )

    mock_request.assert_awaited_once_with(
        "applics/json",
        {
            "pg_sz": 300,
            "index": 0,
            "search": "ashp",
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
        },
    )


@pytest.mark.asyncio
async def test_search_all_pages_paginates_with_indexes(monkeypatch):
    monkeypatch.setattr("src.planit_client.PLANIT_PAGE_SIZE", 2)
    monkeypatch.setattr("src.planit_client.PLANIT_MAX_RESULTS", 100)

    client = PlanItClient()
    mock_search = AsyncMock(
        side_effect=[
            # probe call (pg_sz=1)
            {"records": [{"uid": "0"}], "total": 5},
            # page fetches
            {"records": [{"uid": "1"}, {"uid": "2"}], "total": 5},
            {"records": [{"uid": "3"}, {"uid": "4"}], "total": 5},
            {"records": [{"uid": "5"}], "total": 5},
        ]
    )
    monkeypatch.setattr(client, "search_applications", mock_search)

    records = await client.search_all_pages(
        search="ashp",
        start_date="2025-01-01",
        end_date="2025-01-31",
        app_state="Permitted",
    )

    assert [record["uid"] for record in records] == ["1", "2", "3", "4", "5"]

    # First call is the probe (pg_sz=1), rest are page fetches
    calls = mock_search.await_args_list
    assert calls[0].kwargs["pg_sz"] == 1
    assert [call.kwargs["index"] for call in calls[1:]] == [0, 2, 4]
    for call in calls:
        assert call.kwargs["search"] == "ashp"
        assert call.kwargs["start_date"] == "2025-01-01"
        assert call.kwargs["end_date"] == "2025-01-31"
        assert call.kwargs["app_state"] == "Permitted"


@pytest.mark.asyncio
async def test_search_all_pages_auto_splits_when_over_max(monkeypatch):
    monkeypatch.setattr("src.planit_client.PLANIT_PAGE_SIZE", 2)
    monkeypatch.setattr("src.planit_client.PLANIT_MAX_RESULTS", 3)

    client = PlanItClient()
    mock_search = AsyncMock(
        side_effect=[
            # Initial probe: 5 results > max 3, triggers split
            {"records": [{"uid": "0"}], "total": 5},
            # First half probe (Jan 1 - Jan 16): 2 results, within limit
            {"records": [{"uid": "0"}], "total": 2},
            # First half page fetch
            {"records": [{"uid": "1"}, {"uid": "2"}], "total": 2},
            # Second half probe (Jan 17 - Jan 31): 3 results, within limit
            {"records": [{"uid": "0"}], "total": 3},
            # Second half page fetches
            {"records": [{"uid": "3"}, {"uid": "4"}], "total": 3},
            {"records": [{"uid": "5"}], "total": 3},
        ]
    )
    monkeypatch.setattr(client, "search_applications", mock_search)

    records = await client.search_all_pages(
        search="ashp",
        start_date="2025-01-01",
        end_date="2025-01-31",
    )

    assert [r["uid"] for r in records] == ["1", "2", "3", "4", "5"]

    # Verify the date split happened
    calls = mock_search.await_args_list
    # First call: probe for full range
    assert calls[0].kwargs["start_date"] == "2025-01-01"
    assert calls[0].kwargs["end_date"] == "2025-01-31"
    # Second call: probe for first half
    assert calls[1].kwargs["end_date"] == "2025-01-16"
    # Fourth call: probe for second half
    assert calls[3].kwargs["start_date"] == "2025-01-17"
