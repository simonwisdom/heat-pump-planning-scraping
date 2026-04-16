"""PlanIt.org.uk API client with rate limiting and pagination."""

import asyncio
import logging
import time
from collections.abc import Callable

import httpx

from .config import (
    PLANIT_BASE_URL,
    PLANIT_MAX_RESULTS,
    PLANIT_MIN_REQUEST_GAP,
    PLANIT_PAGE_SIZE,
    PLANIT_RATE_LIMIT_COOLDOWN_BASE,
    PLANIT_RATE_LIMIT_COOLDOWN_MAX,
    PLANIT_RATE_LIMIT_REQUESTS,
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """Adaptive rate limiter that backs off on 429 responses."""

    def __init__(
        self,
        max_requests: int = PLANIT_RATE_LIMIT_REQUESTS,
        cooldown_base: float = PLANIT_RATE_LIMIT_COOLDOWN_BASE,
        cooldown_max: float = PLANIT_RATE_LIMIT_COOLDOWN_MAX,
        min_request_gap: float = PLANIT_MIN_REQUEST_GAP,
    ):
        self.max_requests = max_requests
        self.cooldown_base = cooldown_base
        self.cooldown_max = cooldown_max
        self.min_request_gap = min_request_gap
        self.request_count = 0
        self.consecutive_429s = 0
        self._last_request_time = 0.0
        self._retry_after_override: float | None = None

    async def wait_if_needed(self):
        """Wait if we've hit the request limit."""
        # Enforce minimum gap between every request to avoid bursts
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.min_request_gap:
            await asyncio.sleep(self.min_request_gap - elapsed)

        if self.request_count >= self.max_requests:
            # Use one-time retry_after if set, otherwise normal backoff
            if self._retry_after_override is not None:
                wait = min(self._retry_after_override, self.cooldown_max)
                self._retry_after_override = None
            else:
                wait = self.cooldown_base * (1.5**self.consecutive_429s)
                wait = min(wait, self.cooldown_max)
            logger.info(f"Rate limit pause: {wait:.0f}s (request #{self.request_count})")
            await asyncio.sleep(wait)
            self.request_count = 0

    def record_request(self):
        self.request_count += 1
        self._last_request_time = time.monotonic()

    def record_429(self, retry_after: float | None = None):
        self.consecutive_429s += 1
        self.request_count = self.max_requests  # Force wait on next call
        if retry_after:
            self._retry_after_override = retry_after
        next_wait = retry_after or self.cooldown_base * (1.5**self.consecutive_429s)
        logger.warning(
            f"429 received (consecutive: {self.consecutive_429s}). "
            f"Next cooldown: {min(next_wait, self.cooldown_max):.0f}s"
        )

    def record_success(self):
        self.consecutive_429s = max(0, self.consecutive_429s - 1)


class PlanItClient:
    """Async client for the PlanIt planning applications API."""

    def __init__(self, rate_limiter: RateLimiter | None = None):
        self.base_url = PLANIT_BASE_URL
        self.rate_limiter = rate_limiter or RateLimiter()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    @property
    def client(self) -> httpx.AsyncClient:
        if not self._client:
            raise RuntimeError("Use 'async with PlanItClient() as client:'")
        return self._client

    async def _request(self, endpoint: str, params: dict) -> dict:
        """Make a rate-limited request to the PlanIt API."""
        import re

        url = f"{self.base_url}/{endpoint}"

        max_attempts = 10
        for attempt in range(max_attempts):
            await self.rate_limiter.wait_if_needed()

            try:
                resp = await self.client.get(url, params=params)
                self.rate_limiter.record_request()

                if resp.status_code == 429:
                    try:
                        error_data = resp.json()
                        error_msg = error_data.get("error", "")
                        match = re.search(r"(\d+)s", error_msg)
                        retry_after = float(match.group(1)) if match else None
                    except Exception:
                        retry_after = None

                    self.rate_limiter.record_429(retry_after)
                    logger.warning(f"429 on attempt {attempt + 1}/{max_attempts}, will retry")
                    continue

                resp.raise_for_status()
                data = resp.json()

                if "error" in data:
                    raise PlanItError(data["error"])

                self.rate_limiter.record_success()
                return data

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    self.rate_limiter.record_429()
                    continue
                raise

            except (httpx.TimeoutException, httpx.NetworkError) as e:
                logger.warning(f"Network/timeout error on attempt {attempt + 1}/{max_attempts}: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(15.0)
                    continue
                raise PlanItError(f"Network error after {max_attempts} attempts for {url}: {e}")

        raise PlanItError(f"Failed after {max_attempts} attempts for {url}")

    async def search_applications(
        self,
        search: str,
        start_date: str | None = None,
        end_date: str | None = None,
        app_type: str | None = None,
        app_state: str | None = None,
        auth: str | None = None,
        pg_sz: int = PLANIT_PAGE_SIZE,
        index: int = 0,
    ) -> dict:
        """Search for planning applications.

        Args:
            search: Text search on description field. Supports quoted phrases and OR.
            start_date: Filter by application start date (YYYY-MM-DD).
            end_date: Filter by application end date (YYYY-MM-DD).
            app_type: Filter by type (Full, Outline, Heritage, etc.).
            app_state: Filter by state (Permitted, Rejected, etc.).
            auth: Filter by authority name.
            pg_sz: Requested page size, capped to the configured PlanIt limit.
            index: Offset for pagination.

        Returns:
            Dict with 'records', 'total', 'from', 'to' keys.
        """
        safe_pg_sz = max(1, min(pg_sz, PLANIT_PAGE_SIZE))
        if safe_pg_sz != pg_sz:
            logger.warning(
                "Requested PlanIt page size %s exceeds configured limit %s; using %s instead",
                pg_sz,
                PLANIT_PAGE_SIZE,
                safe_pg_sz,
            )

        params = {"pg_sz": safe_pg_sz, "index": index}

        if search:
            params["search"] = search
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if app_type:
            params["app_type"] = app_type
        if app_state:
            params["app_state"] = app_state
        if auth:
            params["auth"] = auth

        return await self._request("applics/json", params)

    async def search_all_pages(
        self,
        search: str,
        start_date: str,
        end_date: str,
        on_page: "Callable[[list[dict]], None] | None" = None,
        **kwargs,
    ) -> list[dict]:
        """Fetch all pages for a search query.

        Paginates through results using index offset.
        If the total exceeds PLANIT_MAX_RESULTS, automatically splits the
        date range in half and recurses to get complete results.

        Args:
            on_page: Optional callback invoked with each page of records as
                     they arrive, enabling incremental persistence.

        Returns list of all application records.
        """
        # First check total count
        probe = await self.search_applications(
            search=search,
            start_date=start_date,
            end_date=end_date,
            pg_sz=1,
            **kwargs,
        )
        total = probe.get("total") or 0
        logger.info(f"Query total: {total} results for {start_date} to {end_date}")

        if not total:
            return []

        if total > PLANIT_MAX_RESULTS:
            return await self._split_and_fetch(search, start_date, end_date, total, on_page=on_page, **kwargs)

        return await self._fetch_all_pages(search, start_date, end_date, total, on_page=on_page, **kwargs)

    async def _split_and_fetch(
        self,
        search: str,
        start_date: str,
        end_date: str,
        total: int,
        on_page: "Callable[[list[dict]], None] | None" = None,
        **kwargs,
    ) -> list[dict]:
        """Split a date range in half and fetch both halves."""
        from datetime import date, timedelta

        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        mid = start + (end - start) // 2

        if mid <= start:
            logger.warning(
                f"Cannot split further ({start_date} to {end_date}, "
                f"{total} results). Fetching up to {PLANIT_MAX_RESULTS}."
            )
            return await self._fetch_all_pages(search, start_date, end_date, total, on_page=on_page, **kwargs)

        second_start = (mid + timedelta(days=1)).isoformat()
        logger.info(
            f"Splitting {start_date}..{end_date} ({total} results) "
            f"into {start_date}..{mid} + {second_start}..{end_date}"
        )

        first_half = await self.search_all_pages(
            search=search,
            start_date=start_date,
            end_date=mid.isoformat(),
            on_page=on_page,
            **kwargs,
        )

        second_half = await self.search_all_pages(
            search=search,
            start_date=second_start,
            end_date=end_date,
            on_page=on_page,
            **kwargs,
        )

        return first_half + second_half

    async def _fetch_all_pages(
        self,
        search: str,
        start_date: str,
        end_date: str,
        total: int,
        on_page: "Callable[[list[dict]], None] | None" = None,
        **kwargs,
    ) -> list[dict]:
        """Fetch all pages for a query known to be within limits."""
        all_records = []
        index = 0

        while True:
            data = await self.search_applications(
                search=search,
                start_date=start_date,
                end_date=end_date,
                index=index,
                **kwargs,
            )

            records = data.get("records", [])
            all_records.extend(records)
            logger.info(f"  Fetched {len(all_records)}/{total}")

            if on_page and records:
                on_page(records)

            if len(records) < PLANIT_PAGE_SIZE or len(all_records) >= total:
                break

            index += PLANIT_PAGE_SIZE

        return all_records

    async def count_applications(
        self,
        search: str,
        start_date: str | None = None,
        end_date: str | None = None,
        **kwargs,
    ) -> int | None:
        """Get the count of matching applications without fetching records."""
        data = await self.search_applications(
            search=search,
            start_date=start_date,
            end_date=end_date,
            pg_sz=1,
            **kwargs,
        )
        return data.get("total")


class PlanItError(Exception):
    """Error from PlanIt API."""

    pass
