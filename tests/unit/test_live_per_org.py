# Copyright (c) 2026 Kenneth Stott
# Canary: 30631723-8816-4d57-bf97-61aa5d010ad7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live delivery is each org's own, governed per subscriber (REQ-286, REQ-1266).

Each org's prod runtime holds its own live engine. A subscription is served only by the bound
org's engine, as the subscriber's governance key; another org's live query is unknown to it, by
name. These run with nothing bound but what each test binds, as a request would.
"""

# Requirements: REQ-286, REQ-369, REQ-1266

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.api.errors import ApiError
from provisa.core.request_context import current_org, reset_current_org, set_current_org
from tests.unit.live_engine_doubles import governed, spec

pytestmark = pytest.mark.unbound


def _engine(org: str):
    from provisa.live.engine import LiveEngine

    return LiveEngine(
        tenant_db=None, org_id=org, scheduler=SimpleNamespace(add_job=lambda *a, **k: None)
    )


class _Orgs:
    """App state routed by the bound org, as the runtime registry serves it."""

    def __init__(self, engines: dict) -> None:
        self._engines = engines
        self.rate_limiter = None

    @property
    def live_engine(self):
        return self._engines[current_org.get()]

    @property
    def roles(self):
        return {"analyst": {"id": "analyst"}}


def _request(role: str = "analyst"):
    return cast("Any", SimpleNamespace(state=SimpleNamespace(role=role)))


async def _subscribe(org: str | None, query_id: str):
    from provisa.api.data.subscribe import subscribe

    token = set_current_org(org) if org is not None else None
    try:
        return await subscribe("orders", _request(), None, query_id)
    finally:
        if token is not None:
            reset_current_org(token)


@pytest.fixture
def two_orgs(monkeypatch):
    engines = {"root": _engine("root"), "acme": _engine("acme")}
    engines["root"].reconcile([spec("pg.orders")])  # only the deployment org has this live table
    monkeypatch.setattr("provisa.api.app.state", _Orgs(engines), raising=False)
    return engines


async def test_a_subscription_is_served_by_its_own_orgs_engine(two_orgs):
    with governed():
        response = await _subscribe("root", "pg.orders")
    assert response.media_type == "text/event-stream"
    # The subscriber's key is the bound org's: root's engine holds its poll, acme's holds none.
    assert [g.key.org_id for g in two_orgs["root"]._groups.values()] == ["root"]
    assert two_orgs["acme"]._groups == {}


async def test_another_orgs_live_query_is_unknown_by_name(two_orgs):
    """acme naming the deployment org's live query id is answered as an unknown id: its rows are
    never reachable from acme."""
    with governed() as reads, pytest.raises(ApiError) as err:
        await _subscribe("acme", "pg.orders")
    assert (err.value.status_code, err.value.code) == (404, "subscribe.live_query_not_registered")
    assert "pg.orders" in err.value.detail
    assert reads == []  # nothing was read, as any org
    assert two_orgs["root"]._groups == {}


async def test_a_subscription_with_no_org_bound_is_refused(two_orgs):
    with pytest.raises(RuntimeError, match="No active org bound"):
        await _subscribe(None, "pg.orders")


async def test_an_engine_refuses_a_key_of_another_org(two_orgs):
    from provisa.live.governed import GovernanceKey

    with governed(), pytest.raises(PermissionError, match="not served by org 'root'"):
        await two_orgs["root"].subscribe("pg.orders", GovernanceKey("acme", "analyst", ()))


async def test_a_refused_governed_check_starts_no_poll(two_orgs):
    """The first subscriber of a key is governed before its poll starts: a refusal reaches it and
    nothing is scheduled."""
    with governed(PermissionError("column 'ts' is not visible")), pytest.raises(PermissionError):
        await _subscribe("root", "pg.orders")
    assert two_orgs["root"]._groups == {}


async def test_the_sse_slot_is_the_orgs_role(monkeypatch):
    """REQ-369: one org's subscribers never take another org's slots for a role of the same id."""
    from provisa.api.data.subscribe import _acquire_sse_slot

    taken: list[str] = []

    class _Limiter:
        async def acquire(self, key, cap):
            taken.append(key)
            return True

    state = SimpleNamespace(
        rate_limiter=_Limiter(),
        roles={"analyst": {"rate_limit": {"max_sse_subscriptions": 1}}},
    )
    for org in ("acme", "beta"):
        token = set_current_org(org)
        try:
            await _acquire_sse_slot(state, "analyst")
        finally:
            reset_current_org(token)
    assert taken == ["rl:sse:acme:analyst", "rl:sse:beta:analyst"]


def test_a_replaced_runtime_stops_its_live_engine(monkeypatch):
    """A runtime no longer served stops its engine: its polls are its org's."""
    from provisa.api.org_runtime import OrgRegistry, OrgRuntime

    stopped: list[str] = []

    class _Engine:
        def __init__(self, org: str) -> None:
            self.org = org

        async def stop(self) -> None:
            stopped.append(self.org)

    def _run_now(coro, **_kw):
        asyncio.run(coro)

    monkeypatch.setattr("provisa.core.connection_loop.spawn_background", _run_now)
    registry = OrgRegistry()
    old = OrgRuntime(org_id="acme")
    old.live_engine = _Engine("acme-old")
    registry.set("acme", old)
    registry.set("acme", OrgRuntime(org_id="acme"))  # rebuilt
    other = OrgRuntime(org_id="beta")
    other.live_engine = _Engine("beta")
    registry.set("beta", other)
    registry.invalidate("beta")  # dropped
    assert stopped == ["acme-old", "beta"]
    assert old.live_engine is None


async def test_only_a_prod_runtime_starts_a_live_engine(monkeypatch):
    from provisa.api import app_rebuild
    from provisa.core.request_context import reset_current_env, set_current_env

    state = SimpleNamespace(live_engine=None, tenant_db=None, model_db=None)
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    token = set_current_org("acme")
    env_token = set_current_env("dev")
    try:
        await app_rebuild.start_org_live_engine(SimpleNamespace())
    finally:
        reset_current_env(env_token)
        reset_current_org(token)
    assert state.live_engine is None  # an environment's live config runs once promoted
