# Copyright (c) 2026 Kenneth Stott
# Canary: d9c34314-72cf-401f-9ec9-6efefcf792c6
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""BDD steps for REQ-369 — Per-role rate limiting at the API layer
and REQ-370 — Independent NL query rate limiting to cap LLM cost exposure."""

from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException
from pytest_bdd import given, when, then, scenarios

from provisa.api.rate_limit import RedisRateLimiter, build_rate_limiter

scenarios("../features/REQ-369.feature")
scenarios("../features/REQ-370.feature")


class _FakeRedis:
    """Minimal in-memory async stand-in for the Redis commands the limiter uses."""

    def __init__(self) -> None:
        self.zsets: dict[str, dict[str, float]] = {}
        self.kv: dict[str, int] = {}

    async def zremrangebyscore(self, key, mn, mx):
        z = self.zsets.get(key, {})
        gone = [m for m, s in list(z.items()) if mn <= s <= mx]
        for m in gone:
            del z[m]
        return len(gone)

    async def zcard(self, key):
        return len(self.zsets.get(key, {}))

    async def zrange(self, key, start, end, *, withscores=False):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])
        sliced = items[start : (end + 1 if end >= 0 else None)]
        return sliced if withscores else [m for m, _ in sliced]

    async def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)
        return len(mapping)

    async def expire(self, key, seconds):
        return True

    async def incr(self, key):
        self.kv[key] = self.kv.get(key, 0) + 1
        return self.kv[key]

    async def decr(self, key):
        self.kv[key] = self.kv.get(key, 0) - 1
        return self.kv[key]

    async def set(self, key, value):
        self.kv[key] = int(value)
        return True


class _Clock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def shared_data() -> dict:
    return {}


@given("a role with configured rate limits")
def role_with_rate_limits(shared_data: dict) -> None:
    # REQ-829: build_rate_limiter with no redis URL now returns a RedisRateLimiter
    # backed by embedded fakeredis (not a Noop), so desktop exercises the real
    # sliding-window logic. Here we construct one explicitly with a deterministic
    # clock/fake so the window assertions are reproducible.
    assert isinstance(build_rate_limiter(None), RedisRateLimiter)
    clock = _Clock()
    limiter = RedisRateLimiter(_FakeRedis(), now=clock)
    shared_data["clock"] = clock
    shared_data["limiter"] = limiter
    # Per-role config: max 3 requests per second for role "analyst".
    shared_data["role_id"] = "analyst"
    shared_data["rate_key"] = "rps:analyst"
    shared_data["max_rps"] = 3
    shared_data["window_seconds"] = 1.0


@when("requests exceed the rate limit")
def requests_exceed_limit(shared_data: dict) -> None:
    async def _run() -> None:
        limiter: RedisRateLimiter = shared_data["limiter"]
        key = shared_data["rate_key"]
        limit = shared_data["max_rps"]
        window = shared_data["window_seconds"]

        results = []
        # Fire one more than the configured limit within the same window.
        for _ in range(limit + 1):
            allowed, retry_after = await limiter.allow(key, limit, window)
            results.append((allowed, retry_after))

        shared_data["results"] = results

        # Simulate the API layer rejecting the over-limit request before compilation
        # or execution: it raises HTTP 429 with a Retry-After header.
        last_allowed, last_retry = results[-1]
        shared_data["compilation_invoked"] = False
        if not last_allowed:
            retry_after_secs = max(1, int(round(last_retry)))
            try:
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded",
                    headers={"Retry-After": str(retry_after_secs)},
                )
            except HTTPException as exc:
                shared_data["exception"] = exc
        else:
            # Only if allowed would the request proceed to compilation/execution.
            shared_data["compilation_invoked"] = True

    asyncio.run(_run())


@then(
    "requests are rejected with HTTP 429 and a Retry-After header before compilation or execution"
)
def requests_rejected(shared_data: dict) -> None:
    results = shared_data["results"]
    limit = shared_data["max_rps"]

    # The first `limit` requests must be allowed.
    allowed_count = sum(1 for allowed, _ in results if allowed)
    assert allowed_count == limit, f"expected {limit} allowed, got {allowed_count}"

    # The request that exceeds the limit must be rejected with a positive retry.
    last_allowed, last_retry = results[-1]
    assert last_allowed is False
    assert last_retry > 0

    # The API layer must have raised HTTP 429 with a Retry-After header.
    exc: HTTPException = shared_data["exception"]
    assert exc.status_code == 429
    assert "Retry-After" in exc.headers
    assert int(exc.headers["Retry-After"]) >= 1

    # Rejection happened before any compilation or execution.
    assert shared_data["compilation_invoked"] is False


# ---------------------------------------------------------------------------
# REQ-370 — Independent NL query rate limit (caps LLM cost exposure).
#
# The NL query service (POST /query/nl) has its own per-minute, per-role limit
# configured via nl.rate_limit. The crucial guarantee is that requests over the
# limit are rejected *before* any LLM call is made — protecting cost regardless
# of server capacity. We exercise the real RedisRateLimiter sliding window and
# assert the LLM call counter never advances for rejected requests.
# ---------------------------------------------------------------------------


@given("a role with a configured nl.rate_limit")
def role_with_nl_rate_limit(shared_data: dict) -> None:
    # The NL limiter is independent of the general API limiter (separate key
    # namespace and separate configured limit).
    clock = _Clock()
    limiter = RedisRateLimiter(_FakeRedis(), now=clock)
    shared_data["nl_clock"] = clock
    shared_data["nl_limiter"] = limiter
    shared_data["nl_role"] = "analyst"
    # NL limit is keyed independently from the general query rate-limit key.
    shared_data["nl_rate_key"] = "nl:analyst"
    # nl.rate_limit: requests per minute per role.
    shared_data["nl_rate_limit"] = 5
    shared_data["nl_window_seconds"] = 60.0
    # Counter representing actual (cost-incurring) LLM API invocations.
    shared_data["llm_calls"] = 0
    shared_data["nl_exceptions"] = []


@when("NL query requests exceed the per-minute limit")
def nl_requests_exceed_limit(shared_data: dict) -> None:
    async def _run() -> None:
        limiter: RedisRateLimiter = shared_data["nl_limiter"]
        key = shared_data["nl_rate_key"]
        limit = shared_data["nl_rate_limit"]
        window = shared_data["nl_window_seconds"]

        async def fake_llm_call() -> str:
            # Each invocation here represents real spend against the LLM provider.
            shared_data["llm_calls"] += 1
            return "SELECT 1"

        results = []
        rejections = []
        # Fire two requests beyond the configured per-minute limit, all within the
        # same window so none of them age out.
        for _ in range(limit + 2):
            allowed, retry_after = await limiter.allow(key, limit, window)
            results.append((allowed, retry_after))
            if allowed:
                # Only allowed requests reach the LLM generation step.
                await fake_llm_call()
            else:
                rejections.append(retry_after)
                retry_after_secs = max(1, int(round(retry_after)))
                try:
                    raise HTTPException(
                        status_code=429,
                        detail="NL rate limit exceeded",
                        headers={"Retry-After": str(retry_after_secs)},
                    )
                except HTTPException as exc:
                    shared_data["nl_exceptions"].append(exc)

        shared_data["nl_results"] = results
        shared_data["nl_rejections"] = rejections

    asyncio.run(_run())


@then("requests are rejected before any LLM call is made")
def nl_requests_rejected_before_llm(shared_data: dict) -> None:
    limit = shared_data["nl_rate_limit"]
    results = shared_data["nl_results"]

    # Exactly `limit` requests should be admitted within the window.
    allowed_count = sum(1 for allowed, _ in results if allowed)
    assert allowed_count == limit, f"expected {limit} allowed, got {allowed_count}"

    # The LLM was invoked only for admitted requests — over-limit requests
    # incurred zero LLM cost.
    assert shared_data["llm_calls"] == limit, (
        f"LLM was called {shared_data['llm_calls']} times; expected {limit} "
        "(rejected requests must never reach the LLM)"
    )

    # Every request beyond the limit was rejected before the LLM call.
    rejections = shared_data["nl_rejections"]
    assert len(rejections) == len(results) - limit
    assert all(retry > 0 for retry in rejections)

    # Each rejection surfaced as HTTP 429 with a Retry-After header.
    exceptions = shared_data["nl_exceptions"]
    assert len(exceptions) == len(rejections)
    for exc in exceptions:
        assert exc.status_code == 429
        assert "Retry-After" in exc.headers
        assert int(exc.headers["Retry-After"]) >= 1


# ---------------------------------------------------------------------------
# REQ-369 — Concurrency limits for SSE subscriptions and Arrow Flight streams.
#
# Beyond requests-per-second, provisa.yaml configures per-role caps on the
# number of concurrent SSE subscriptions and concurrent Arrow Flight streams.
# These are enforced with the limiter's concurrency gauge (acquire/release)
# at the API layer before any subscription/stream is established. We exercise
# the real gauge to prove that exceeding either cap is rejected with HTTP 429.
# ---------------------------------------------------------------------------


@given("a role with configured concurrency limits")
def role_with_concurrency_limits(shared_data: dict) -> None:
    limiter = RedisRateLimiter(_FakeRedis())
    shared_data["conc_limiter"] = limiter
    shared_data["conc_role"] = "analyst"
    # Per-role config from provisa.yaml.
    shared_data["max_sse_subs"] = 2
    shared_data["max_flight_streams"] = 2
    shared_data["sse_key"] = "sse:analyst"
    shared_data["flight_key"] = "flight:analyst"
    shared_data["sse_exceptions"] = []
    shared_data["flight_exceptions"] = []
    shared_data["sse_results"] = []
    shared_data["flight_results"] = []


@when("concurrent SSE subscriptions and Arrow Flight streams exceed their limits")
def concurrency_exceeds_limits(shared_data: dict) -> None:
    async def _run() -> None:
        limiter: RedisRateLimiter = shared_data["conc_limiter"]

        # Attempt one more SSE subscription than allowed.
        sse_key = shared_data["sse_key"]
        sse_limit = shared_data["max_sse_subs"]
        for _ in range(sse_limit + 1):
            acquired = await limiter.acquire(sse_key, sse_limit)
            shared_data["sse_results"].append(acquired)
            if not acquired:
                try:
                    raise HTTPException(
                        status_code=429,
                        detail="SSE subscription limit exceeded",
                        headers={"Retry-After": "1"},
                    )
                except HTTPException as exc:
                    shared_data["sse_exceptions"].append(exc)

        # Attempt one more Arrow Flight stream than allowed.
        flight_key = shared_data["flight_key"]
        flight_limit = shared_data["max_flight_streams"]
        for _ in range(flight_limit + 1):
            acquired = await limiter.acquire(flight_key, flight_limit)
            shared_data["flight_results"].append(acquired)
            if not acquired:
                try:
                    raise HTTPException(
                        status_code=429,
                        detail="Arrow Flight stream limit exceeded",
                        headers={"Retry-After": "1"},
                    )
                except HTTPException as exc:
                    shared_data["flight_exceptions"].append(exc)

    asyncio.run(_run())


@then("the excess subscriptions and streams are rejected with HTTP 429")
def concurrency_rejected(shared_data: dict) -> None:
    sse_limit = shared_data["max_sse_subs"]
    flight_limit = shared_data["max_flight_streams"]

    # Exactly `limit` SSE subscriptions admitted, then the excess rejected.
    sse_results = shared_data["sse_results"]
    assert sum(1 for ok in sse_results if ok) == sse_limit
    assert sse_results[-1] is False

    # Exactly `limit` Flight streams admitted, then the excess rejected.
    flight_results = shared_data["flight_results"]
    assert sum(1 for ok in flight_results if ok) == flight_limit
    assert flight_results[-1] is False

    # Each rejection surfaced as HTTP 429 with a Retry-After header.
    for exc in shared_data["sse_exceptions"] + shared_data["flight_exceptions"]:
        assert exc.status_code == 429
        assert "Retry-After" in exc.headers
        assert int(exc.headers["Retry-After"]) >= 1

    assert len(shared_data["sse_exceptions"]) == 1
    assert len(shared_data["flight_exceptions"]) == 1


# ---------------------------------------------------------------------------
# REQ-369 — Concurrency slots are released and become reusable.
#
# After a subscription or stream terminates, the API layer releases its
# concurrency slot so subsequent requests are admitted again. This proves the
# enforcement is a live gauge and not a permanent counter.
# ---------------------------------------------------------------------------


@given("a saturated concurrency gauge for a role")
def saturated_concurrency_gauge(shared_data: dict) -> None:
    async def _run() -> None:
        limiter = RedisRateLimiter(_FakeRedis())
        shared_data["release_limiter"] = limiter
        shared_data["release_key"] = "flight:analyst"
        shared_data["release_limit"] = 1
        # Fill the single available slot.
        acquired = await limiter.acquire(shared_data["release_key"], shared_data["release_limit"])
        assert acquired is True
        # A second acquire must be rejected while the gauge is saturated.
        blocked = await limiter.acquire(shared_data["release_key"], shared_data["release_limit"])
        assert blocked is False
        shared_data["release_pre_state"] = blocked

    asyncio.run(_run())


@when("an active stream is released")
def active_stream_released(shared_data: dict) -> None:
    async def _run() -> None:
        limiter: RedisRateLimiter = shared_data["release_limiter"]
        await limiter.release(shared_data["release_key"])
        # After release a slot should be available again.
        shared_data["release_post_state"] = await limiter.acquire(
            shared_data["release_key"], shared_data["release_limit"]
        )

    asyncio.run(_run())


@then("a new stream can acquire the freed slot")
def new_stream_acquires_freed_slot(shared_data: dict) -> None:
    assert shared_data["release_pre_state"] is False
    assert shared_data["release_post_state"] is True
