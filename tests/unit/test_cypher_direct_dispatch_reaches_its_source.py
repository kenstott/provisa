# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1a95-6b2d-4f80-a4e9-2d8b5f1c7e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher read (HTTP and Bolt) runs on its own source's connection, or not at all.

Cypher hands every governed plan to the one pipeline terminal (``_execute_plan``), whose DIRECT
terminal serves the provisa-admin source from the model store and refuses any other source it
holds no connection for (``tests/unit/test_direct_terminal_reaches_its_source.py``). A plan for a
source this node holds no connection for is routed to the engine, never run on the org's
control-plane store; the role reading it has no right to that store."""

# Requirements: REQ-825, REQ-1919, REQ-031

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.unit.test_governed_sql_engine_internals import _state as gov_state

pytestmark = pytest.mark.asyncio


class _ControlPlane:
    """The org's control-plane store: records every statement it is asked to run."""

    def __init__(self) -> None:
        self.ran: list[str] = []

    @asynccontextmanager
    async def acquire(self):
        store = self

        class _Conn:
            async def fetch_with_columns(self, sql):
                store.ran.append(sql)
                return ["id"], [(1,)]

        yield _Conn()


class _NoPools:
    """Connections this node holds: none."""

    source_ids: tuple[str, ...] = ()

    def has(self, _source_id: str) -> bool:
        return False


async def test_a_role_without_meta_reading_a_source_with_no_connection_never_reaches_the_control_plane(
    monkeypatch,
):
    import provisa.api.app as app_mod
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled
    from provisa.transpiler.router import Route

    store = _ControlPlane()
    state = gov_state(["sales"])  # analyst: the sales domain only, no meta
    state.source_pools = _NoPools()
    state.tenant_db = store
    state.source_dialects = {"pg": "postgres"}
    # The runtime the pipeline reads this environment's unbound sources from: none here.
    state._active_runtime = lambda: SimpleNamespace(unbound_sources=frozenset())
    # The engine the read is routed to: it plans the statement (nothing here executes it).
    state.federation_engine = SimpleNamespace(
        engine=SimpleNamespace(catalog_qualified=True),
        transpile_physical=lambda sql: sql,
        dialect="trino",
    )
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))

    # The statement Cypher's translator produces for MATCH (o:Orders) RETURN o.id, governed as
    # the Cypher router governs it: with no connection to its source here, the engine reads it.
    plan = await _govern_and_route_compiled(
        'SELECT "o"."id" FROM "sales"."orders" AS "o"',
        "analyst",
        state=state,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=False,  # as the Cypher router governs it
    )
    assert plan.route == Route.ENGINE, plan
    assert store.ran == [], f"the read ran on the control-plane store: {store.ran}"


async def test_cypher_runs_its_plan_through_the_one_pipeline_terminal(monkeypatch):
    """No Cypher-private dispatcher: the plan goes to ``_execute_plan``, where the DIRECT
    terminal's source rules hold for every surface."""
    from provisa.api.rest import cypher_router

    ran: list[object] = []

    async def _terminal(plan, state):
        ran.append(plan)
        return "result"

    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _terminal)
    monkeypatch.setattr("provisa.pgwire._pipeline.require_governed_plan", lambda _plan: None)
    plan = type("_Plan", (), {"physical_sql": "SELECT 1", "exec_sql": "SELECT 1"})()
    assert await cypher_router._run_plan(plan, object()) == "result"  # type: ignore[arg-type]
    assert ran == [plan]
    assert not hasattr(cypher_router, "_dispatch_execution_direct")
