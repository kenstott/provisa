# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2f7a41-9c3e-4b86-a1f0-7e64c2d8b953
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A live poll is narrowed to its subscriber's entitlements, as a one-shot query is (REQ-2219,
REQ-286).

A poll runs on the scheduler, outside the request that subscribed, so nothing binds the person
there unless the subscription carries them. Each test reads the same table both ways -- a one-shot
query through the governed pipeline inside the person's request, and a live poll (append and
replace) outside it -- and requires the same narrowed rows. Both postures are covered: lists from
the identity provider's claim, and lists Provisa holds for the person, which can change between
polls.

The pipeline seam (``_govern_and_route`` / ``_execute_plan``) is stood in by a double that reads
the bound person's mask with the real :func:`provisa.entitlements.runtime.request_mask`, refusing a
table outside it and reading only the columns in it, as the entitlement rule does.
"""

# Requirements: REQ-2219, REQ-286

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.entitlements import runtime
from provisa.entitlements.lists import HeldEntitlements, HeldList
from provisa.live import governed
from provisa.pgwire import _pipeline
from tests.unit.live_engine_doubles import make_engine, make_pool_with_conn, spec
from tests.unit.test_entitlement_rule import _bind, _runtime_state

ORG = "default"
ROLE = "viewer"
REF = '"finance"."revenue"'
SQL = f"SELECT * FROM {REF}"
ROW = {"id": 1, "amount": 10}
REVENUE = 1  # the names table's finance.revenue


@pytest.fixture
def served(monkeypatch):
    """The org bound, the app state holding the role, and the pipeline seam reading the mask."""
    state = _runtime_state()
    monkeypatch.setattr("provisa.api.app.state", state)

    async def route(sql, role_id, *, session_vars=None, params=None):
        mask = runtime.request_mask(state, role_id)
        if mask is not None and not mask.has_table(REVENUE):
            raise PermissionError("finance.revenue is outside the entitlement mask")
        return [c for c in ROW if mask is None or mask.has_column(REVENUE, c)]

    async def execute(plan):
        return SimpleNamespace(column_names=plan, rows=[tuple(ROW[c] for c in plan)])

    monkeypatch.setattr(_pipeline, "_govern_and_route", route)
    monkeypatch.setattr(_pipeline, "_execute_plan", execute)
    monkeypatch.setattr(governed, "table_meta", lambda _tid: SimpleNamespace())
    monkeypatch.setattr(governed, "table_ref", lambda _meta: REF)
    token = set_current_org(ORG)
    try:
        yield state
    finally:
        reset_current_org(token)


async def _one_shot() -> list[dict]:
    """The normal query path, inside the person's request."""
    plan = await _pipeline._govern_and_route(SQL, ROLE, session_vars={}, params=None)
    result = await _pipeline._execute_plan(plan)
    return [dict(zip(result.column_names, row)) for row in result.rows]


async def _poll(key: governed.GovernanceKey, mode: str) -> list[dict]:
    """One scheduled poll of a live query on the table, as ``key``, with no request bound."""
    engine = make_engine(make_pool_with_conn(AsyncMock()), started=True)
    engine.reconcile([spec(watermark_column="id", mode=mode)])
    queue = await engine.subscribe("q1", key)
    with (
        patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
        patch("provisa.live.watermark.set_watermark", AsyncMock()),
    ):
        await engine._poll("q1", f"sse:{key.digest}")
    return queue.get_nowait()


# ── posture: the identity provider's lists ─────────────────────────────────────────────────────


@pytest.mark.parametrize("mode", ["append", "replace"])
@pytest.mark.asyncio
async def test_a_poll_reads_only_the_columns_the_claim_lists(served, monkeypatch, mode):
    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:columns:finance.revenue.id"]})
    with runtime.entitlements_scope(identity):
        key = governed.subscriber_key(ROLE)
        one_shot = await _one_shot()
    assert one_shot == [{"id": 1}]
    assert await _poll(key, mode) == one_shot


@pytest.mark.parametrize("mode", ["append", "replace"])
@pytest.mark.asyncio
async def test_a_poll_of_a_table_the_claim_leaves_out_is_refused(served, monkeypatch, mode):
    identity = _bind(monkeypatch, {"groups": ["provisa:viewer:tables:sales.orders"]})
    with runtime.entitlements_scope(identity):
        key = governed.subscriber_key(ROLE)
        with pytest.raises(PermissionError):
            await _one_shot()
    engine = make_engine(make_pool_with_conn(AsyncMock()), started=True)
    engine.reconcile([spec(watermark_column="id", mode=mode)])
    with pytest.raises(PermissionError):
        await engine.subscribe("q1", key)


# ── posture: the lists Provisa holds, changed between polls ────────────────────────────────────


def _hold(state, *identifiers: str) -> None:
    state.held_entitlements["alice"] = HeldEntitlements(
        lists=(HeldList(ROLE, "columns", identifiers),)
    )


@pytest.mark.parametrize("mode", ["append", "replace"])
@pytest.mark.asyncio
async def test_a_poll_reads_only_the_columns_provisa_holds(served, monkeypatch, mode):
    _hold(served, "finance.revenue.id")
    identity = _bind(monkeypatch, {})
    with runtime.entitlements_scope(identity):
        key = governed.subscriber_key(ROLE)
        one_shot = await _one_shot()
    assert one_shot == [{"id": 1}]
    assert await _poll(key, mode) == one_shot


@pytest.mark.parametrize("mode", ["append", "replace"])
@pytest.mark.asyncio
async def test_a_list_set_between_polls_narrows_the_next_poll(served, monkeypatch, mode):
    identity = _bind(monkeypatch, {})
    with runtime.entitlements_scope(identity):
        key = governed.subscriber_key(ROLE)
        assert await _one_shot() == [ROW]
    assert await _poll(key, mode) == [ROW]

    _hold(served, "finance.revenue.id")
    with runtime.entitlements_scope(identity):
        narrowed = await _one_shot()
    assert narrowed == [{"id": 1}]
    assert await _poll(key, mode) == narrowed


@pytest.mark.parametrize("mode", ["append", "replace"])
@pytest.mark.asyncio
async def test_a_list_widened_between_polls_widens_the_next_poll(served, monkeypatch, mode):
    _hold(served, "finance.revenue.id")
    identity = _bind(monkeypatch, {})
    with runtime.entitlements_scope(identity):
        key = governed.subscriber_key(ROLE)
    assert await _poll(key, mode) == [{"id": 1}]

    _hold(served, "finance.revenue.*")
    with runtime.entitlements_scope(identity):
        widened = await _one_shot()
    assert widened == [ROW]
    assert await _poll(key, mode) == widened


# ── the poll is the person's, never shared with another person of the role ─────────────────────


@pytest.mark.asyncio
async def test_two_people_of_one_role_do_not_share_a_poll(served, monkeypatch):
    narrow = _bind(monkeypatch, {"groups": ["provisa:viewer:columns:finance.revenue.id"]})
    whole = SimpleNamespace(user_id="bob", raw_claims={}, entitlements_claim="provisa_entitlements")
    with runtime.entitlements_scope(narrow):
        alice = governed.subscriber_key(ROLE)
    with runtime.entitlements_scope(whole):
        bob = governed.subscriber_key(ROLE)
    assert alice != bob
    assert alice.digest != bob.digest
    assert await _poll(alice, "replace") == [{"id": 1}]
    assert await _poll(bob, "replace") == [ROW]


@pytest.mark.asyncio
async def test_work_no_person_is_doing_binds_no_person(served, monkeypatch):
    """An output's key (a Kafka sink) names no person, and its reads bind none -- not a person
    from whatever context the poll happens to run in."""
    key = governed.GovernanceKey(ORG, ROLE, ())
    assert key.person is None
    seen = []

    async def route(_sql, _role_id, *, session_vars=None, params=None):
        seen.append(runtime.bound_request())
        return list(ROW)

    monkeypatch.setattr(_pipeline, "_govern_and_route", route)
    identity = SimpleNamespace(user_id="mallory", raw_claims={}, entitlements_claim=None)
    with runtime.entitlements_scope(identity):
        await governed.governed_rows(SQL, key)
    assert seen == [None]
