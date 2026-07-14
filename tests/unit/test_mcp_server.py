# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the MCP protocol adapter (REQ-1008, phase 1).

Covers deterministic drill-down (list_schemas / list_tables / describe_table),
governed run_sql / explain_sql routing through _govern_and_route, the row cap,
and the fail-loud role rules (no role / unknown role / PermissionError never
becomes a silent empty result or an admin default).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from provisa.api.flight.catalog import CatalogColumn, CatalogTable
from provisa.api.mcp import tools
from provisa.compiler.sql_types import JoinMeta, TableMeta
from provisa.executor.result import QueryResult
from provisa.transpiler.router import Route

pytestmark = pytest.mark.asyncio


# --- fixtures ---------------------------------------------------------------

_CATALOG = [
    CatalogTable(
        domain_id="sales",
        table_name="orders",
        description="Order records",
        columns=[
            CatalogColumn("id", "integer", False, "Primary key"),
            CatalogColumn("customer_id", "integer", True, "FK to customers"),
        ],
    ),
    CatalogTable(
        domain_id="sales",
        table_name="line_items",
        description="Order lines",
        columns=[CatalogColumn("id", "integer", False, "")],
    ),
    CatalogTable(
        domain_id="crm",
        table_name="customers",
        description="Customer master",
        columns=[CatalogColumn("id", "integer", False, "")],
    ),
]


def _make_state():
    """Fake AppState: a role context with one FK join + config domains."""
    orders_meta = TableMeta(
        table_id=1,
        field_name="orders",
        type_name="Orders",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="orders",
        domain_id="sales",
    )
    customers_meta = TableMeta(
        table_id=2,
        field_name="customers",
        type_name="Customers",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="customers",
        domain_id="crm",
    )
    ctx = SimpleNamespace(
        tables={"orders": orders_meta, "customers": customers_meta},
        joins={
            ("Orders", "customer"): JoinMeta(
                source_column="customer_id",
                target_column="id",
                source_column_type="integer",
                target_column_type="integer",
                target=customers_meta,
                cardinality="many-to-one",
            ),
        },
    )
    config = SimpleNamespace(
        domains=[
            SimpleNamespace(id="sales", description="Sales domain"),
            SimpleNamespace(id="crm", description="CRM domain"),
        ]
    )
    return SimpleNamespace(
        contexts={"analyst": ctx},
        roles={"analyst": {"id": "analyst"}},
        config=config,
    )


@pytest.fixture
def state():
    return _make_state()


@pytest.fixture(autouse=True)
def _patch_catalog(monkeypatch):
    """build_catalog_tables normally hits tenant_db; feed the fixed catalog."""
    monkeypatch.setattr(tools, "build_catalog_tables", lambda _state: list(_CATALOG))


# --- drill-down -------------------------------------------------------------


async def test_list_schemas(state):
    result = await tools.list_schemas(state, "analyst")
    assert result == [
        {"schema": "crm", "description": "CRM domain", "table_count": 1},
        {"schema": "sales", "description": "Sales domain", "table_count": 2},
    ]


async def test_list_tables(state):
    result = await tools.list_tables(state, "analyst", "sales")
    assert result == [
        {"table": "line_items", "description": "Order lines", "column_count": 1},
        {"table": "orders", "description": "Order records", "column_count": 2},
    ]


async def test_list_tables_unknown_schema(state):
    with pytest.raises(ValueError):
        await tools.list_tables(state, "analyst", "nope")


async def test_describe_table(state):
    result = await tools.describe_table(state, "analyst", "sales", "orders")
    assert result["schema"] == "sales"
    assert result["table"] == "orders"
    assert result["columns"] == [
        {"name": "id", "type": "integer", "description": "Primary key"},
        {"name": "customer_id", "type": "integer", "description": "FK to customers"},
    ]
    assert result["foreign_keys"] == [
        {
            "column": "customer_id",
            "references_schema": "crm",
            "references_table": "customers",
            "references_column": "id",
        }
    ]


async def test_describe_table_not_found(state):
    with pytest.raises(ValueError):
        await tools.describe_table(state, "analyst", "sales", "ghost")


# --- role rules -------------------------------------------------------------


async def test_missing_role_raises(state):
    with pytest.raises(ValueError):
        await tools.list_schemas(state, "")


async def test_unknown_role_raises(state):
    with pytest.raises(PermissionError):
        await tools.list_schemas(state, "intruder")


# --- run_sql ----------------------------------------------------------------


async def test_run_sql_routes_through_govern_and_route(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    plan = pipeline._Plan(
        route=Route.ENGINE,
        sql="SELECT 1",
        source_id="pg",
        dialect="trino",
        physical_sql="SELECT 1",
    )
    govern = AsyncMock(return_value=plan)
    execute = AsyncMock(
        return_value=QueryResult(rows=[(1, "a"), (2, "b")], column_names=["id", "name"])
    )
    monkeypatch.setattr(pipeline, "_govern_and_route", govern)
    monkeypatch.setattr(pipeline, "_execute_plan", execute)

    result = await tools.run_sql(state, "analyst", "SELECT * FROM sales.orders")

    govern.assert_awaited_once_with("SELECT * FROM sales.orders", "analyst")
    assert result["columns"] == ["id", "name"]
    assert result["rows"] == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert result["total_rows"] == 2
    assert result["truncated"] is False


async def test_run_sql_applies_row_cap(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    monkeypatch.setenv("PROVISA_MCP_MAX_ROWS", "2")
    plan = pipeline._Plan(route=Route.DIRECT, sql="SELECT 1", source_id="pg", dialect="postgres")
    rows = [(i,) for i in range(10)]
    monkeypatch.setattr(pipeline, "_govern_and_route", AsyncMock(return_value=plan))
    monkeypatch.setattr(
        pipeline,
        "_execute_plan",
        AsyncMock(return_value=QueryResult(rows=rows, column_names=["n"])),
    )

    result = await tools.run_sql(state, "analyst", "SELECT n FROM t")
    assert result["row_count"] == 2
    assert result["total_rows"] == 10
    assert result["truncated"] is True


async def test_run_sql_permission_error_propagates(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    monkeypatch.setattr(
        pipeline,
        "_govern_and_route",
        AsyncMock(side_effect=PermissionError("denied for role analyst")),
    )
    exec_mock = AsyncMock()
    monkeypatch.setattr(pipeline, "_execute_plan", exec_mock)

    with pytest.raises(PermissionError):
        await tools.run_sql(state, "analyst", "SELECT * FROM secret")
    exec_mock.assert_not_awaited()  # never a silent empty result


async def test_run_sql_missing_role(state):
    with pytest.raises(ValueError):
        await tools.run_sql(state, "", "SELECT 1")


# --- explain_sql ------------------------------------------------------------


async def test_explain_sql_returns_plan_without_executing(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    plan = pipeline._Plan(
        route=Route.ENGINE,
        sql="SELECT 1",
        source_id="pg",
        dialect="trino",
        physical_sql="SELECT 1 /*physical*/",
    )
    govern = AsyncMock(return_value=plan)
    execute = AsyncMock()
    monkeypatch.setattr(pipeline, "_govern_and_route", govern)
    monkeypatch.setattr(pipeline, "_execute_plan", execute)

    result = await tools.explain_sql(state, "analyst", "SELECT * FROM sales.orders")

    govern.assert_awaited_once()
    execute.assert_not_awaited()  # explain is plan-only
    assert result == {
        "route": "ENGINE",
        "physical_sql": "SELECT 1 /*physical*/",
        "source_id": "pg",
        "dialect": "trino",
    }


async def test_explain_sql_permission_error_propagates(state, monkeypatch):
    import provisa.pgwire._pipeline as pipeline

    monkeypatch.setattr(
        pipeline,
        "_govern_and_route",
        AsyncMock(side_effect=PermissionError("nope")),
    )
    with pytest.raises(PermissionError):
        await tools.explain_sql(state, "analyst", "SELECT 1")


# --- server wiring ----------------------------------------------------------


async def test_build_mcp_server_registers_tools():
    from provisa.api.mcp.server import build_mcp_server

    mcp = build_mcp_server(_make_state())
    names = {t.name for t in await mcp.list_tools()}
    assert {"list_schemas", "list_tables", "describe_table", "run_sql", "explain_sql"} <= names
    assert "search_catalog" not in names  # phase 2, deferred


async def test_pinned_stdio_role_requires_env(monkeypatch):
    from provisa.api.mcp import server

    monkeypatch.delenv("PROVISA_MCP_ROLE", raising=False)
    with pytest.raises(ValueError):
        server._pinned_stdio_role()
