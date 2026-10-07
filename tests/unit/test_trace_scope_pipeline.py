# Copyright (c) 2026 Kenneth Stott
# Canary: 7a1d4e96-3c58-4b27-8f60-2e9b5c0d7a34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one pipeline resolves each request's trace detail from the operator's debug-trace settings
(REQ-1910).

Both halves of the pipeline — the raw-SQL path (pgwire, /data/sql) and the compiled path (GraphQL
over Flight, Cypher, gRPC, MCP, REST) — are driven here against a real control-plane store. What
is pinned: a window covers its org and no other, an expired window covers nothing, a source window
takes effect once the request's sources are known, an unpermitted hint is rejected on both paths,
a debug request does not leave the level behind for the next request on the same context, and a
request no window covers gets the process default rather than a forced ``normal``.
"""

# Requirements: REQ-1910, REQ-030

from __future__ import annotations

import contextlib

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from provisa import otel_compat
from provisa.compiler.directives import NO_CACHE_HINT, CacheHint
from provisa.core import trace_scope as ts
from provisa.core.database import Database, create_engine_from_url
from provisa.core.operator_floor import OperatorFloorError
from provisa.core.schema_admin import debug_trace_hint_roles, debug_trace_windows, metadata
from provisa.pgwire import _pipeline
from tests.unit.test_governed_sql_engine_internals import _state as gov_state

_SQL = "SELECT o.id FROM sales.orders o"
_HINTED_SQL = f"-- @provisa trace=debug\n{_SQL}"
_DEBUG_HINT = CacheHint(opt_in=False, ttl=None, debug_trace=True)


@pytest.fixture
def served(tmp_path, monkeypatch):
    """A pipeline whose state has a real control plane and serves org ``acme``. ``at_routing``
    records the trace detail in force when the statement reached routing — after request entry,
    before its sources were known."""
    import provisa.api.app as app_mod

    db = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"), name="trace-scope"
    )
    with db.engine.begin() as conn:
        metadata.create_all(conn, tables=[debug_trace_windows, debug_trace_hint_roles])
    state = gov_state(["*"])
    state.admin_db = db
    state.org_id = "acme"
    state.source_dialects = {"pg": "postgres"}
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    ts.invalidate()
    otel_compat.clear_trace_detail()
    at_routing: list[str] = []

    async def _route(exec_sql, *_args, **_kwargs):
        from provisa.transpiler.router import Route, RouteDecision

        at_routing.append(otel_compat.trace_detail())
        return (
            exec_sql,
            RouteDecision(route=Route.DIRECT, source_id="pg", dialect="postgres", reason="t"),
            "pg",
            False,
            {"pg"},
            (),
        )

    # The routing stage itself (its cache included) is replaced: every execution reaches it.
    with patch.object(_pipeline, "_optimize_and_route_cached", new=AsyncMock(side_effect=_route)):
        yield db, state, at_routing
    otel_compat.clear_trace_detail()
    ts.invalidate()


@contextlib.contextmanager
def _request_of_the_served_org():
    """A request for the org the state serves, bound as the org-routing middleware binds it
    (REQ-1266). Read per request: a test re-points ``state.org_id`` to serve another org."""
    import provisa.api.app as app_mod
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(app_mod.state.org_id)
    try:
        yield
    finally:
        reset_current_org(token)


async def _raw(sql: str = _SQL) -> None:
    with _request_of_the_served_org():
        await _pipeline._govern_and_route(sql, "analyst")


async def _compiled(hint: CacheHint = NO_CACHE_HINT) -> None:
    with _request_of_the_served_org():
        await _pipeline._govern_and_route_compiled(_SQL, "analyst", cache_hint=hint, sdl_joins=True)


async def _window(db, scope: str, org_id: str, target: str | None = None, **kw) -> None:
    await ts.start_window(
        db, scope=scope, org_id=org_id, target=target, minutes=15, created_by=None, **kw
    )


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_a_request_no_window_covers_is_traced_in_normal_detail(served, run):
    _db, _state, at_routing = served
    await run()
    assert at_routing == ["normal"]
    assert otel_compat.trace_detail() == "normal"


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_an_org_window_puts_that_orgs_requests_in_debug_and_no_other_orgs(served, run):
    db, state, at_routing = served
    await _window(db, "org", "acme")
    await run()
    state.org_id = "globex"
    await run()
    assert at_routing == ["debug", "normal"]


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_a_role_window_covers_the_role(served, run):
    db, _state, at_routing = served
    await _window(db, "role", "acme", "steward")
    await run()
    await _window(db, "role", "acme", "analyst")
    await run()
    assert at_routing == ["normal", "debug"]


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_a_window_that_has_run_out_no_longer_covers_requests(served, run):
    db, _state, at_routing = served
    await _window(db, "org", "acme", now=datetime.now(timezone.utc) - timedelta(minutes=16))
    await run()
    assert at_routing == ["normal"]


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_a_source_window_takes_effect_once_the_requests_sources_are_known(served, run):
    db, _state, at_routing = served
    await _window(db, "source", "acme", "pg")
    await run()
    assert at_routing == ["normal"]  # at entry the sources were not known yet
    assert otel_compat.trace_detail() == "debug"  # from routing on, the request is a debug one


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_a_source_window_on_another_source_does_not_cover_the_request(served, run):
    db, _state, _at_routing = served
    await _window(db, "source", "acme", "crm")
    await run()
    assert otel_compat.trace_detail() == "normal"


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_a_debug_request_does_not_leave_the_level_for_the_next_one(served, run):
    # One pgwire connection serves many statements on one context: the statement after a debug
    # one resolves for itself.
    db, state, at_routing = served
    await _window(db, "source", "acme", "pg")
    await run()
    assert otel_compat.trace_detail() == "debug"
    state.org_id = "globex"
    await run()
    assert at_routing == ["normal", "normal"]
    assert otel_compat.trace_detail() == "normal"


@pytest.mark.parametrize("run", [_raw, _compiled], ids=["raw_sql", "compiled"])
@pytest.mark.asyncio
async def test_a_request_no_window_covers_gets_the_process_default(served, run, monkeypatch):
    # The scope decides who gets debug ON TOP of the deployment's default; it never forces an
    # uncovered request down to normal on a deployment configured for debug detail.
    monkeypatch.setattr(otel_compat, "_process_detail", "debug")
    _db, _state, at_routing = served
    await run()
    assert at_routing == ["debug"]


@pytest.mark.asyncio
async def test_a_hint_from_an_unpermitted_role_is_rejected_on_the_raw_sql_path(served):
    _db, _state, at_routing = served
    with pytest.raises(OperatorFloorError) as exc:
        await _raw(_HINTED_SQL)
    assert ts.HINT_SETTING in str(exc.value)
    assert at_routing == []  # rejected at entry, before anything ran


@pytest.mark.asyncio
async def test_a_hint_from_an_unpermitted_role_is_rejected_on_the_compiled_path(served):
    _db, _state, at_routing = served
    with pytest.raises(OperatorFloorError) as exc:
        await _compiled(_DEBUG_HINT)
    assert ts.HINT_SETTING in str(exc.value)
    assert at_routing == []


@pytest.mark.asyncio
async def test_a_hint_from_a_permitted_role_makes_the_request_a_debug_one(served):
    db, _state, at_routing = served
    await ts.set_hint_permission(db, "acme", "analyst", True, updated_by=None)
    await _raw(_HINTED_SQL)
    await _compiled(_DEBUG_HINT)
    await _raw()  # the same role, without the hint
    assert at_routing == ["debug", "debug", "normal"]


@pytest.mark.asyncio
async def test_the_permission_is_per_org(served):
    db, state, _at_routing = served
    await ts.set_hint_permission(db, "acme", "analyst", True, updated_by=None)
    state.org_id = "globex"
    with pytest.raises(ts.DebugTraceHintNotPermitted):
        await _raw(_HINTED_SQL)


@pytest.mark.asyncio
async def test_a_prepared_statement_resolves_at_each_execution(served):
    # pgwire governs a prepared statement once and routes it per Execute: a window opened between
    # two executions covers the second.
    db, _state, at_routing = served
    with _request_of_the_served_org():  # the pgwire session's org, bound for each statement
        governed = await _pipeline.govern_statement(_SQL, "analyst")
        await _pipeline.route_governed(governed)
        await _window(db, "org", "acme")
        await _pipeline.route_governed(governed)
    assert at_routing == ["normal", "debug"]


@pytest.mark.asyncio
async def test_the_level_is_the_requests_across_its_tasks_and_no_other_requests(served):
    # A transport serves one request as several tasks (pgwire: govern, execute, audit), each in a
    # copy of the connection's context. The level resolved inside the first is the request's: the
    # transport and the later tasks see it, and the connection's next request does not.
    import asyncio

    from opentelemetry.sdk.trace import TracerProvider

    db, _state, _at_routing = served
    await _window(db, "org", "acme")
    # A real SDK tracer: each request gets its own recording span, as in a running server.
    tracer = TracerProvider().get_tracer("test.trace_scope")

    async def _detail() -> str:
        return otel_compat.trace_detail()

    with otel_compat.request_span(tracer, "pgwire.query", transport="pgwire"):
        await asyncio.create_task(_raw())
        assert otel_compat.trace_detail() == "debug"
        assert await asyncio.create_task(_detail()) == "debug"
    with otel_compat.request_span(tracer, "pgwire.query", transport="pgwire"):
        assert otel_compat.trace_detail() == "normal"
        assert await asyncio.create_task(_detail()) == "normal"
