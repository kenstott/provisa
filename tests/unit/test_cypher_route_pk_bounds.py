# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: the Cypher-override _Plan sites (issue #119's single-source neo4j reverse-compile)
still populate pk_bounds from the ORIGINAL governed SQL text -- the override only changes the
EXECUTION dialect (SQL -> Cypher for the source's own HTTP endpoint), so the governed SQL handed
to best_effort_cypher_for_sql still has a normal relational predicate shape for extract_pk_bounds
to walk. Mirrors test_neo4j_direct_route.py's mocking technique (patch _optimize_and_route to
force the override, monkeypatch best_effort_cypher_for_sql to force a translatable result)."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta

pytestmark = pytest.mark.asyncio

SOURCE_ID = "bench-neo4j"
DOMAIN_ID = "perf_bench"
TABLE_ID = 1

_ORDER_COLUMNS = [
    {"column_name": "order_id", "data_type": "varchar", "visible_to": ["analyst"]},
]
_ORDER_TABLE_DICT = {
    "id": TABLE_ID,
    "source_id": SOURCE_ID,
    "schema_name": "public",
    "table_name": "bench_order_node",
    "domain_id": DOMAIN_ID,
    "columns": _ORDER_COLUMNS,
}


@dataclass
class _Col:
    name: str
    is_primary_key: bool = False


@dataclass
class _RowMatTable:
    source_id: str
    schema_name: str
    table_name: str
    columns: list = field(default_factory=list)
    row_materialize: bool = True


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


def _fake_state():
    return SimpleNamespace(
        contexts={"analyst": _ctx()},
        rls_contexts={},
        roles={"analyst": {"capabilities": [], "domain_access": ["*"]}},
        masking_rules={},
        tables=[_ORDER_TABLE_DICT],
        relationships=[],
        source_types={SOURCE_ID: "neo4j"},
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


async def test_cypher_override_plan_resolves_pk_bounds_from_governed_sql(monkeypatch):
    import provisa.api.app as app_mod
    import provisa.nl.runner as nl_runner
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    monkeypatch.setattr(app_mod, "state", _fake_state(), raising=False)
    monkeypatch.setattr(
        nl_runner,
        "best_effort_cypher_for_sql",
        lambda sql, ctx, role, state: "MATCH (o:Order) WHERE o.order_id IN $keys RETURN o",
    )

    row_mat_table = _RowMatTable(
        source_id=SOURCE_ID,
        schema_name="public",
        table_name="bench_order_node",
        columns=[_Col("order_id", is_primary_key=True)],
    )

    async def _fake_row_materialized_tables_by_name(state):
        return {"bench_order_node": row_mat_table}

    monkeypatch.setattr(
        "provisa.federation.query_residency.row_materialized_tables_by_name",
        _fake_row_materialized_tables_by_name,
    )

    with patch.object(
        _pipeline,
        "_optimize_and_route",
        new=AsyncMock(side_effect=_fake_optimize_and_route_engine),
    ):
        plan = await _pipeline._govern_and_route(
            "SELECT order_id FROM bench_order_node WHERE order_id = '42'", "analyst"
        )

    assert plan.route == Route.DIRECT
    assert plan.dialect == "cypher"
    # The override changed the EXECUTION text to Cypher, but pk_bounds still resolved from the
    # ORIGINAL governed SQL's WHERE clause -- proving the Cypher branch is not an untested gap.
    assert len(plan.pk_bounds) == 1
    bound = plan.pk_bounds[0]
    assert bound.table_name == "bench_order_node"
    assert bound.pk_columns == ("order_id",)
    assert bound.values == (("42",),)


async def test_cypher_override_plan_pk_bounds_empty_when_no_row_materialize_tables(monkeypatch):
    """No row_materialize table registered at all -- pk_bounds stays empty, never an error."""
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
    assert plan.pk_bounds == ()
