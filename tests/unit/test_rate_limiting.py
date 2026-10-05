# Copyright (c) 2026 Kenneth Stott
# Canary: dad323fe-dd0c-4f52-b8f9-78beb986f9fa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for rate limiting (REQ-369–371). Redis is faked in-memory."""

from __future__ import annotations

import pytest

from provisa.api.rate_limit import (
    NoopRateLimiter,
    RedisRateLimiter,
    build_rate_limiter,
)


class FakeRedis:
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
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class TestNoopLimiter:
    @pytest.mark.asyncio
    async def test_allows_everything(self):
        lim = NoopRateLimiter()
        assert await lim.allow("k", 1, 1.0) == (True, 0.0)
        assert await lim.acquire("k", 1) is True
        assert await lim.release("k") is None

    def test_build_without_redis_uses_fakeredis(self):  # REQ-829
        # With no URL the limiter runs on embedded fakeredis (same code path as
        # production Redis), not a no-op — desktop exercises real rate limiting.
        assert isinstance(build_rate_limiter(None), RedisRateLimiter)
        assert isinstance(build_rate_limiter(""), RedisRateLimiter)


class TestSlidingWindow:
    @pytest.mark.asyncio
    async def test_allows_up_to_limit_then_rejects(self):
        clock = _Clock()
        lim = RedisRateLimiter(FakeRedis(), now=clock)
        assert (await lim.allow("k", 2, 1.0))[0] is True
        assert (await lim.allow("k", 2, 1.0))[0] is True
        allowed, retry_after = await lim.allow("k", 2, 1.0)
        assert allowed is False
        assert retry_after > 0

    @pytest.mark.asyncio
    async def test_window_slides(self):
        clock = _Clock()
        lim = RedisRateLimiter(FakeRedis(), now=clock)
        await lim.allow("k", 1, 1.0)
        assert (await lim.allow("k", 1, 1.0))[0] is False
        clock.t += 1.5  # advance past the window
        assert (await lim.allow("k", 1, 1.0))[0] is True

    @pytest.mark.asyncio
    async def test_zero_limit_is_unlimited(self):
        lim = RedisRateLimiter(FakeRedis())
        assert (await lim.allow("k", 0, 1.0))[0] is True


class TestConcurrencyGauge:
    @pytest.mark.asyncio
    async def test_acquire_up_to_limit(self):
        lim = RedisRateLimiter(FakeRedis())
        assert await lim.acquire("c", 2) is True
        assert await lim.acquire("c", 2) is True
        assert await lim.acquire("c", 2) is False  # over

    @pytest.mark.asyncio
    async def test_release_frees_a_slot(self):
        r = FakeRedis()
        lim = RedisRateLimiter(r)
        await lim.acquire("c", 1)
        assert await lim.acquire("c", 1) is False
        await lim.release("c")
        assert await lim.acquire("c", 1) is True

    @pytest.mark.asyncio
    async def test_release_never_goes_negative(self):
        r = FakeRedis()
        lim = RedisRateLimiter(r)
        await lim.release("c")
        assert r.kv["c"] == 0


class TestMiddleware:
    @pytest.mark.asyncio
    async def test_returns_429_when_over_limit(self, monkeypatch):
        from types import SimpleNamespace

        import provisa.api.app as app_module
        from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware

        class _OverLimiter:
            async def allow(self, key, limit, window_s):
                return False, 2.0

        fake_state = SimpleNamespace(
            rate_limiter=_OverLimiter(),
            roles={"analyst": {"rate_limit": {"requests_per_second": 1}}},
        )
        monkeypatch.setattr(app_module, "state", fake_state)

        mw = RateLimitMiddleware(app=lambda *a, **k: None)
        scope = {"type": "http", "state": {"role": "analyst", "active_org_id": "default"}}

        resp = await mw._process(scope)
        assert resp.status_code == 429
        assert resp.headers["Retry-After"] == "2"

    @pytest.mark.asyncio
    async def test_passes_through_when_no_limit(self, monkeypatch):
        from types import SimpleNamespace

        import provisa.api.app as app_module
        from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware

        fake_state = SimpleNamespace(rate_limiter=NoopRateLimiter(), roles={"analyst": {}})
        monkeypatch.setattr(app_module, "state", fake_state)

        mw = RateLimitMiddleware(app=lambda *a, **k: None)
        scope = {"type": "http", "state": {"role": "analyst", "active_org_id": "default"}}

        resp = await mw._process(scope)
        assert resp is None  # pass-through: __call__ falls through to self.app

    @pytest.mark.asyncio
    async def test_unknown_role_denied_when_roles_configured(self, monkeypatch):
        # A populated registry still rejects a role it does not know — never silent-unlimited.
        from types import SimpleNamespace

        import provisa.api.app as app_module
        from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware

        fake_state = SimpleNamespace(rate_limiter=NoopRateLimiter(), roles={"analyst": {}})
        monkeypatch.setattr(app_module, "state", fake_state)

        mw = RateLimitMiddleware(app=lambda *a, **k: None)
        scope = {"type": "http", "state": {"role": "admin", "active_org_id": "default"}}

        resp = await mw._process(scope)
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_empty_roles_registry_passes_through(self, monkeypatch):
        # Configless / unsecured native boot: no roles loaded. The unsecured default role must not
        # be rejected as "unknown" — that walled off the whole app (incl. /auth/me + setup flow).
        from types import SimpleNamespace

        import provisa.api.app as app_module
        from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware

        fake_state = SimpleNamespace(rate_limiter=NoopRateLimiter(), roles={})
        monkeypatch.setattr(app_module, "state", fake_state)

        mw = RateLimitMiddleware(app=lambda *a, **k: None)
        scope = {"type": "http", "state": {"role": "admin", "active_org_id": "default"}}

        resp = await mw._process(scope)
        assert resp is None  # pass-through: __call__ falls through to self.app


# --- the plane a request acts on (REQ-1266, REQ-1327) --------------------------------------------


@pytest.fixture
def two_planes(monkeypatch):
    """The deployment org 'root' holds the platform roles; tenant org 'acme' holds its own."""
    from provisa.api.app import AppState
    from provisa.api.org_runtime import OrgRuntime

    state = AppState()
    state.org_id = "root"
    root = state.org_registry.get("root")
    assert root is not None
    root.roles = {"platform_admin": {"id": "platform_admin"}}
    acme = OrgRuntime(org_id="acme")
    acme.roles = {"analyst": {"id": "analyst", "rate_limit": {"requests_per_second": 1}}}
    state.org_registry.set("acme", acme)
    monkeypatch.setattr("provisa.api.app.state", state)
    return state


class _Seen:
    def __init__(self) -> None:
        self.keys: list[str] = []

    async def allow(self, key, limit, window_s):
        self.keys.append(key)
        return True, 0.0


@pytest.mark.unbound
@pytest.mark.asyncio
async def test_a_request_acting_in_no_org_is_judged_on_the_platform_plane(two_planes):
    """No org named (or none held): nothing is bound, and the role is a platform role. Reading the
    tenant registry here refused every such request with a 500."""
    from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware

    two_planes.rate_limiter = NoopRateLimiter()
    mw = RateLimitMiddleware(app=lambda *a, **k: None)
    ok = {"type": "http", "state": {"role": "platform_admin", "active_org_id": None}}
    assert await mw._process(ok) is None
    # A tenant's role is not a platform role.
    other = {"type": "http", "state": {"role": "analyst", "active_org_id": None}}
    assert (await mw._process(other)).status_code == 403


@pytest.mark.unbound
@pytest.mark.asyncio
async def test_a_request_acting_in_an_org_is_judged_by_that_orgs_roles(two_planes):
    from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware
    from provisa.core.request_context import reset_current_org, set_current_org

    seen = _Seen()
    two_planes.rate_limiter = seen
    mw = RateLimitMiddleware(app=lambda *a, **k: None)
    token = set_current_org("acme")  # as the org-routing middleware binds it
    try:
        assert (
            await mw._process(
                {"type": "http", "state": {"role": "analyst", "active_org_id": "acme"}}
            )
            is None
        )
        denied = await mw._process(
            {"type": "http", "state": {"role": "platform_admin", "active_org_id": "acme"}}
        )
    finally:
        reset_current_org(token)
    assert seen.keys == ["rl:req:acme:analyst"]  # acme's limit, in acme's bucket
    assert denied.status_code == 403  # a platform role is not one of acme's


class _OnePerBucket:
    """Admits one request per bucket, then refuses: a limit of one, held per key."""

    def __init__(self) -> None:
        self.spent: set[str] = set()

    async def allow(self, key, limit, window_s):
        if key in self.spent:
            return False, 1.0
        self.spent.add(key)
        return True, 0.0


@pytest.mark.unbound
@pytest.mark.asyncio
async def test_one_orgs_role_cannot_spend_another_orgs_limit(two_planes):
    """Two orgs each define 'analyst' with a limit of one request. acme's analyst spending its
    limit leaves beta's analyst its own; the platform plane's buckets are apart from both."""
    from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware
    from provisa.api.org_runtime import OrgRuntime
    from provisa.core.request_context import reset_current_org, set_current_org

    beta = OrgRuntime(org_id="beta")
    beta.roles = {"analyst": {"id": "analyst", "rate_limit": {"requests_per_second": 1}}}
    two_planes.org_registry.set("beta", beta)
    root = two_planes.org_registry.get("root")
    assert root is not None
    root.roles = {"analyst": {"id": "analyst", "rate_limit": {"requests_per_second": 1}}}
    limiter = _OnePerBucket()
    two_planes.rate_limiter = limiter
    mw = RateLimitMiddleware(app=lambda *a, **k: None)

    async def _as(org: str | None):
        scope = {"type": "http", "state": {"role": "analyst", "active_org_id": org}}
        if org is None:
            return await mw._process(scope)
        token = set_current_org(org)
        try:
            return await mw._process(scope)
        finally:
            reset_current_org(token)

    assert await _as("acme") is None
    assert (await _as("acme")).status_code == 429  # acme's analyst has spent acme's limit
    assert await _as("beta") is None  # beta's analyst still has its own
    assert await _as(None) is None  # and the platform plane its own
    assert limiter.spent == {
        "rl:req:acme:analyst",
        "rl:req:beta:analyst",
        "rl:platform:req:analyst",
    }
