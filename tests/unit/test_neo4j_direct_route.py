# Copyright (c) 2026 Kenneth Stott
# Canary: 9ee69fe9-328f-4931-8c24-be310d13eb45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GitHub issue #119: a single-source neo4j pattern that decide_route would otherwise send
through the ENGINE materialize-then-join path is instead reverse-compiled to Cypher and routed
DIRECT — but ONLY when the reverse compiler (``best_effort_cypher_for_sql``) actually produces
Cypher text. Mocks ``_optimize_and_route`` to control the RouteDecision directly (same technique
as ``test_govern_and_route_nf_args.py``), so this exercises the override logic in
``_govern_and_route_planned`` without needing the full compiler/governance machinery."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta

pytestmark = pytest.mark.asyncio

SOURCE_ID = "bench-neo4j"
DOMAIN_ID = "perf_bench"
TABLE_ID = 1

_ORDER_COLUMNS = [{"column_name": "order_id", "data_type": "varchar", "visible_to": ["analyst"]}]
_ORDER_TABLE_DICT = {
    "id": TABLE_ID,
    "source_id": SOURCE_ID,
    "schema_name": "public",
    "table_name": "bench_order_node",
    "domain_id": DOMAIN_ID,
    "columns": _ORDER_COLUMNS,
}


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "bench_order_node": TableMeta(
            table_id=TABLE_ID,
            field_name="bench_order_node",
            type_name="Order",
            source_id=SOURCE_ID,
            catalog_name=SOURCE_ID,
            schema_name="public",
            table_name="bench_order_node",
            domain_id=DOMAIN_ID,
        )
    }
    return ctx


class _FakeEngineHandle:
    catalog_qualified = True


class _FakeFederationEngine:
    engine = _FakeEngineHandle()
    dialect = "duckdb"

    def transpile_physical(self, sql: str) -> str:
        return sql


def _fake_state(source_type: str = "neo4j"):
    return SimpleNamespace(
        contexts={"analyst": _ctx()},
        rls_contexts={},
        roles={"analyst": {"capabilities": [], "domain_access": ["*"]}},
        masking_rules={},
        tables=[_ORDER_TABLE_DICT],
        relationships=[],
        source_types={SOURCE_ID: source_type},
        source_catalogs={SOURCE_ID: SOURCE_ID},
        federation_engine=_FakeFederationEngine(),
        view_sql_map=None,
        security_high=False,
        metrics={},
    )


async def _fake_optimize_and_route_engine(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
    from provisa.transpiler.router import Route, RouteDecision

    return (
        exec_sql,
        RouteDecision(route=Route.ENGINE, source_id=None, dialect=None, reason="test"),
        SOURCE_ID,
        False,
        {SOURCE_ID},
        (),
    )


async def test_single_source_neo4j_pattern_routes_direct_when_cypher_translatable(monkeypatch):
    import provisa.api.app as app_mod
    import provisa.nl.runner as nl_runner
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    monkeypatch.setattr(app_mod, "state", _fake_state(), raising=False)
    monkeypatch.setattr(
        nl_runner,
        "best_effort_cypher_for_sql",
        lambda sql, ctx, role, state: "MATCH (o:Order) RETURN o",
    )

    with patch.object(
        _pipeline,
        "_optimize_and_route",
        new=AsyncMock(side_effect=_fake_optimize_and_route_engine),
    ):
        plan = await _pipeline._govern_and_route("SELECT order_id FROM bench_order_node", "analyst")

    assert plan.route == Route.DIRECT
    assert plan.dialect == "cypher"
    assert plan.sql == "MATCH (o:Order) RETURN o"
    assert plan.source_id == SOURCE_ID


async def test_single_source_neo4j_pattern_falls_back_to_engine_when_untranslatable(monkeypatch):
    import provisa.api.app as app_mod
    import provisa.nl.runner as nl_runner
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    monkeypatch.setattr(app_mod, "state", _fake_state(), raising=False)
    monkeypatch.setattr(nl_runner, "best_effort_cypher_for_sql", lambda sql, ctx, role, state: None)

    with patch.object(
        _pipeline,
        "_optimize_and_route",
        new=AsyncMock(side_effect=_fake_optimize_and_route_engine),
    ):
        plan = await _pipeline._govern_and_route("SELECT order_id FROM bench_order_node", "analyst")

    assert plan.route == Route.ENGINE
    assert plan.dialect != "cypher"


async def test_non_neo4j_single_source_never_attempts_cypher_translation(monkeypatch):
    """A single-source ENGINE-routed query against a non-neo4j VIRTUAL_SOURCES type (e.g. mongodb)
    must never even call the reverse compiler — the override is neo4j-specific."""
    import provisa.api.app as app_mod
    import provisa.nl.runner as nl_runner
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    monkeypatch.setattr(app_mod, "state", _fake_state(source_type="mongodb"), raising=False)
    called = []
    monkeypatch.setattr(
        nl_runner,
        "best_effort_cypher_for_sql",
        lambda sql, ctx, role, state: called.append(1) or "unused",
    )

    with patch.object(
        _pipeline,
        "_optimize_and_route",
        new=AsyncMock(side_effect=_fake_optimize_and_route_engine),
    ):
        plan = await _pipeline._govern_and_route("SELECT order_id FROM bench_order_node", "analyst")

    assert plan.route == Route.ENGINE
    assert called == []
