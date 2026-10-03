# Copyright (c) 2026 Kenneth Stott
# Canary: 8d2c5f17-3b6a-4e90-a7c4-1f9e6b0d3a58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement that reaches the chokepoint without a request deadline gets one (REQ-1905).

Only the GraphQL endpoint, the Cypher HTTP router and the pgwire/Flight connection loops bound a
request deadline. /data/sql, REST, JSON:API, MCP and Bolt reached ``_execute_plan`` with none, so a
statement that could not finish — observed: a replica land upserting a 20M-document collection
one row at a time — ran until the process was killed. ``_execute_plan`` is the one place they all
pass through: a user-initiated statement that arrives without a deadline is given its
transport's budget there."""

# Requirements: REQ-1905, REQ-1882, REQ-1174

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from provisa.audit.pipeline import PendingAudit
from provisa.cache.store import NoopCacheStore
from provisa.core import request_deadline
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from provisa.transpiler.router import Route

pytestmark = pytest.mark.asyncio


class _Engine:
    """A DIRECT terminal that issues short statements one after another for ``seconds`` — each
    registered with the request deadline exactly as a driver statement is."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.statements = 0
        self.deadline_seen: request_deadline.Deadline | None = None

    async def execute_native(self, pools, source_id, sql, params=None, span_attrs=None):
        self.deadline_seen = request_deadline.current()
        end = time.monotonic() + self.seconds
        while time.monotonic() < end:
            with request_deadline.cancel_on_deadline(lambda: None):
                time.sleep(0.01)
            self.statements += 1
        return QueryResult(rows=[(1,)], column_names=["id"])


def _state(engine, roles=None):
    return SimpleNamespace(
        federation_engine=engine,
        response_cache_store=NoopCacheStore(),
        source_pools=SimpleNamespace(has=lambda sid: True),
        source_types={"pg": "postgresql"},
        roles=roles or {"analyst": {"id": "analyst"}},
    )


def _plan(*, surface: str | None = "http"):
    audit = (
        None
        if surface is None
        else PendingAudit(
            user_id="u1",
            surface=surface,
            role_id="analyst",
            query_text="SELECT 1",
            table_ids=[],
            started=time.time(),
            model_stamp=1,
            enforced={},
        )
    )
    return _pipeline._Plan(
        route=Route.DIRECT,
        sql="SELECT 1",
        source_id="pg",
        dialect="postgres",
        audit=audit,
        role_id="analyst",
        stamp=_pipeline._mint_stamp(),
    )


@pytest.fixture(autouse=True)
def _no_audit_writes(monkeypatch):
    async def _noop(pending, status_code, state=None, **outcome):
        return None

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _noop)


async def test_a_statement_with_no_deadline_fails_at_its_transports_budget(monkeypatch):
    seen: list[str] = []

    def _budget(transport: str) -> float:
        seen.append(transport)
        return 0.2

    monkeypatch.setattr(_pipeline, "statement_budget", _budget)
    engine = _Engine(seconds=3.0)
    started = time.monotonic()
    with pytest.raises(TimeoutError) as failed:
        await _pipeline._execute_plan(_plan(surface="http"), _state(engine))
    assert time.monotonic() - started < 1.5, "the statement ran past its budget"
    assert seen == ["http"]
    message = str(failed.value)
    assert "http" in message and "0.2" in message and "limits.request_timeout" in message


async def test_the_roles_own_time_limit_tightens_the_budget(monkeypatch):
    monkeypatch.setattr(_pipeline, "statement_budget", lambda transport: 30.0)
    engine = _Engine(seconds=3.0)
    roles = {"analyst": {"id": "analyst", "rate_limit": {"max_query_time_ms": 200}}}
    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await _pipeline._execute_plan(_plan(), _state(engine, roles))
    assert time.monotonic() - started < 1.5


async def test_a_statement_within_its_budget_is_unaffected(monkeypatch):
    monkeypatch.setattr(_pipeline, "statement_budget", lambda transport: 5.0)
    engine = _Engine(seconds=0.05)
    result = await _pipeline._execute_plan(_plan(), _state(engine))
    assert result.rows == [(1,)]
    assert engine.deadline_seen is not None and engine.deadline_seen.timeout == 5.0
    assert request_deadline.current() is None, "the deadline outlived the statement"


async def test_a_surface_that_bound_its_own_deadline_keeps_it(monkeypatch):
    """pgwire and Flight bind theirs on the connection loop, GraphQL and Cypher in the endpoint:
    the chokepoint adds none on top."""
    called: list[str] = []
    monkeypatch.setattr(_pipeline, "statement_budget", lambda t: called.append(t) or 0.01)
    engine = _Engine(seconds=0.05)
    with request_deadline.within(30.0) as outer:
        await _pipeline._execute_plan(_plan(surface="pgwire"), _state(engine))
    assert engine.deadline_seen is outer and called == []


async def test_background_work_is_not_given_a_request_deadline(monkeypatch):
    """A plan with no audit record is not user-initiated (seeding, scheduled jobs, rebuilds): it
    runs unbounded by a request budget, as background work does."""
    monkeypatch.setattr(_pipeline, "statement_budget", lambda transport: 0.01)
    engine = _Engine(seconds=0.1)
    result = await _pipeline._execute_plan(_plan(surface=None), _state(engine))
    assert result.rows == [(1,)] and engine.deadline_seen is None


async def test_the_roles_limit_still_tightens_a_request_whose_transport_bound_its_deadline(
    monkeypatch,
):
    """Every transport now binds its request's deadline at its own boundary (REQ-1905), so the
    statement arrives here with one. The role's own limit (REQ-1174) is the tighter budget on
    top of it, and its expiry names the role's setting."""
    called: list[str] = []
    monkeypatch.setattr(_pipeline, "statement_budget", lambda t: called.append(t) or 30.0)
    engine = _Engine(seconds=3.0)
    roles = {"analyst": {"id": "analyst", "rate_limit": {"max_query_time_ms": 200}}}
    started = time.monotonic()
    with request_deadline.within(30.0):
        with pytest.raises(TimeoutError) as failed:
            await _pipeline._execute_plan(_plan(), _state(engine, roles))
    assert time.monotonic() - started < 1.5
    assert "max_query_time_ms" in str(failed.value) and "0.2" in str(failed.value)
    assert called == [], "the transport's budget is the bound deadline, not asked for again"
