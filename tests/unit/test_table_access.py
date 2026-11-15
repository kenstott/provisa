# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1f8b37-2c94-4a5d-9f73-0b8d4e6a2c15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ways to reach a table just registered (REQ-2234, SEE THE DATA RIGHT AFTER REGISTERING):
the GraphQL query, the SQL a pgwire client runs and its connection string, the REST read and the
MCP call, each named as the role's compiled schema names the table."""

# Requirements: REQ-2234

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.api.metadata_export.model import ServingEndpoints
from provisa.api.role_surface import RoleSurface
from provisa.compiler.sql_types import CompilationContext, TableMeta

_ORDERS = TableMeta(
    table_id=7,
    field_name="s__orders",
    type_name="S__Orders",
    source_id="pg",
    catalog_name="pg",
    schema_name="public",
    table_name="orders",
    domain_id="sales-ops",
)


@pytest.fixture
def served(monkeypatch):
    """A role whose schema serves the table, with the names the compiled schema gives it."""
    asked: list[tuple[str, str, str]] = []

    async def _names(_state, role, schema, table):
        asked.append((role, schema, table))
        return {
            "table_field": "s__orders",
            "openapi_path": "GET /data/rest/sales-ops/orders?limit=20",
            "columns": [
                {"name": "id", "graphql_name": "id"},
                {"name": "placed_at", "graphql_name": "placedAt"},
            ],
        }

    monkeypatch.setattr("provisa.api.mcp.tools.graphql_field_names", _names)
    ctx = CompilationContext(tables={"s__orders": _ORDERS})
    state = SimpleNamespace(
        contexts={"analyst": ctx}, surfaces={"analyst": RoleSurface(context=ctx)}
    )
    return state, asked


def _endpoints() -> ServingEndpoints:
    return ServingEndpoints(
        hostname="data.example.com",
        database="provisa",
        http_origin="https://data.example.com",
        pgwire_port=5433,
        http_paths={"GraphQL": "/data/graphql", "REST": "/data/rest"},
    )


async def test_each_access_method_names_the_table_as_the_role_is_served_it(served):
    from provisa.api.admin._table_access import table_access

    state, asked = served

    access = await table_access(state, "analyst", 7, _endpoints())

    assert asked == [("analyst", "sales_ops", "orders")]
    assert access.draft is False  # REQ-1921: a table in service
    assert access.graphql == "query {\n  s__orders(limit: 10) {\n    id\n    placedAt\n  }\n}"
    assert access.sql == 'SELECT * FROM "sales_ops"."orders" LIMIT 10'
    assert access.pgwire == (
        "postgresql://data.example.com:5433/provisa?options=-csearch_path%3Dsales_ops"
    )
    assert access.rest == "GET https://data.example.com/data/rest/sales-ops/orders?limit=20"
    assert json.loads(access.mcp) == {"tool": "run_sql", "arguments": {"sql": access.sql}}


async def test_a_listener_not_started_or_a_surface_not_mounted_is_not_offered(served):
    from provisa.api.admin._table_access import table_access

    state, _ = served
    endpoints = ServingEndpoints(
        hostname="data.example.com", database="provisa", http_origin="https://data.example.com"
    )

    access = await table_access(state, "analyst", 7, endpoints)

    assert access.pgwire is None
    assert access.rest is None


async def test_without_an_origin_only_what_needs_none_is_offered(served):
    from provisa.api.admin._table_access import table_access

    state, _ = served

    access = await table_access(state, "analyst", 7, None)

    assert (access.pgwire, access.rest) == (None, None)
    assert access.graphql.startswith("query {\n  s__orders(limit: 10)")


async def test_a_table_the_role_is_not_served_is_refused_by_name(served):
    from provisa.api.admin._table_access import table_access

    state, _ = served

    with pytest.raises(ValueError, match="table 8 is not served to role 'analyst'"):
        await table_access(state, "analyst", 8, _endpoints())


async def test_an_unauthorized_caller_is_refused_the_access_snippets(served, monkeypatch):
    from provisa.api.admin import schema_query

    state, asked = served

    def _require(_info, capability, domain_id=None):
        raise PermissionError(f"Missing capability: {capability!r}")

    monkeypatch.setattr(schema_query, "require_capability", _require)
    monkeypatch.setattr("provisa.api.app.state", state)

    with pytest.raises(PermissionError, match="Missing capability: 'table_registration'"):
        await schema_query.Query().table_access(  # pyright: ignore[reportCallIssue]
            SimpleNamespace(), 7, "analyst"
        )

    # Refused before the role's schema was read for the table.
    assert asked == []
