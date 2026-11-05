# Copyright (c) 2026 Kenneth Stott
# Canary: d204a409-3c63-4fde-b4e6-d19f0b34faf9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A compiled statement (a Cypher or GraphQL read) that reads a temporary table of its session
goes to the engine, whatever else it reads (REQ-615, REQ-2176): the table is held in the engine's
store, which no source's driver reaches. The raw-SQL path routes it so already; a compiled read
joining a model table to a session table did not, and its source's driver was asked for a table
it does not hold."""

# Requirements: REQ-615, REQ-2176

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler import temp_tables
from provisa.compiler.directives import NO_CACHE_HINT
from provisa.transpiler.router import Route
from tests.unit.test_buffered_auto_threshold import _decision
from tests.unit.test_view_route import _state

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _org(bind_org):
    bind_org("org-a")


async def _plan(monkeypatch, sql: str, session: temp_tables.TempSession):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline, governed_plan

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    state = _state(kept=False)
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    token = temp_tables.bind(session)
    try:
        with patch.object(
            _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision("DIRECT"))
        ):
            return await _pipeline._govern_and_route_compiled(
                sql, "analyst", state=state, cache_hint=NO_CACHE_HINT, sdl_joins=True
            )
    finally:
        temp_tables.unbind(token)


async def test_a_compiled_read_of_a_model_table_and_a_temporary_table_goes_to_the_engine(
    monkeypatch,
):
    session = temp_tables.TempSession()
    session.tables["cypher__node_labels"] = temp_tables.TempTable(
        "cypher__node_labels", [("node_id", "text"), ("label", "text")]
    )
    plan = await _plan(
        monkeypatch,
        'SELECT o."id" FROM "sales"."orders" AS o JOIN cypher__node_labels AS l '
        'ON l.node_id = CAST(o."id" AS VARCHAR)',
        session,
    )
    assert plan.route == Route.ENGINE, plan.route_reason


async def test_a_compiled_read_of_model_tables_alone_keeps_its_route(monkeypatch):
    session = temp_tables.TempSession()
    session.tables["cypher__node_labels"] = temp_tables.TempTable(
        "cypher__node_labels", [("node_id", "text"), ("label", "text")]
    )
    plan = await _plan(monkeypatch, 'SELECT "id" FROM "sales"."orders"', session)
    assert plan.route == Route.DIRECT
