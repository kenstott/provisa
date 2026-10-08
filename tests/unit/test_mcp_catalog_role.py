# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e9a14-6b52-4d08-9f31-e8a0c4d6b275
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MCP catalog tools list what the caller's role is served (REQ-1105, REQ-273).

``list_schemas``, ``list_tables`` and ``describe_table`` checked that the role exists and then
answered from the whole registered catalog, so a role was shown the names of tables and columns
it may not read. They now answer from the same role-narrowed builder Arrow Flight's catalog uses
(``flight.catalog.role_visibility``): a role with one table in a model of two sees one.
"""

# Requirements: REQ-1105, REQ-273, REQ-127

from __future__ import annotations

import types

import pytest

from provisa.api.mcp import tools
from provisa.compiler.sql_types import TableMeta

# table id → (domain, table, columns)
_TABLES = {
    1: ("sales", "orders", ["id", "region", "margin"]),
    2: ("hr", "staff", ["id", "salary"]),
}
# role → table id → the columns it is served
_SERVED = {
    "seller": {1: ["id", "region"]},
    "hr_reader": {2: ["id", "salary"]},
    "org_admin": {1: ["id", "region", "margin"], 2: ["id", "salary"]},
}


class _Conn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def fetch(self, sql: str):
        if "FROM registered_tables" in sql:
            return [
                {
                    "id": table_id,
                    "domain_id": domain,
                    "table_name": name,
                    "description": "",
                    "modeling_role": None,
                    "modeling_history": None,
                }
                for table_id, (domain, name, _) in _TABLES.items()
            ]
        if "FROM relationships" in sql:
            return []
        return [
            {
                "table_id": table_id,
                "column_name": column,
                "description": "",
                "is_primary_key": False,
            }
            for table_id, (_, _, columns) in _TABLES.items()
            for column in columns
        ]


def _context(served: dict[int, list[str]]):
    metas = {}
    for table_id in served:
        domain, name, _ = _TABLES[table_id]
        metas[name] = TableMeta(
            table_id=table_id,
            field_name=name,
            type_name=name.capitalize(),
            source_id="pg",
            catalog_name="pg",
            schema_name="public",
            table_name=name,
            domain_id=domain,
        )
    return types.SimpleNamespace(
        tables=metas,
        physical_to_sql={(tid, col): col for tid, cols in served.items() for col in cols},
        joins={},
        unique_constraints={},
    )


@pytest.fixture
def state():
    db = types.SimpleNamespace(acquire=lambda: _Conn())
    return types.SimpleNamespace(
        model_db=db,
        tenant_db=db,
        engine_conn=None,
        config=None,
        contexts={role: _context(served) for role, served in _SERVED.items()},
    )


async def test_a_role_is_listed_only_the_schemas_and_tables_it_is_served(state):
    assert [s["schema"] for s in await tools.list_schemas(state, "seller")] == ["sales"]
    assert [t["table"] for t in await tools.list_tables(state, "seller", "sales")] == ["orders"]
    # The other role's schema is not a schema this role can name.
    with pytest.raises(ValueError, match="Unknown schema"):
        await tools.list_tables(state, "seller", "hr")
    assert [s["schema"] for s in await tools.list_schemas(state, "hr_reader")] == ["hr"]
    # A role served both sees both.
    assert [s["schema"] for s in await tools.list_schemas(state, "org_admin")] == ["hr", "sales"]


async def test_a_table_is_described_with_only_the_columns_the_role_is_served(state):
    described = await tools.describe_table(state, "seller", "sales", "orders")
    assert [c["name"] for c in described["columns"]] == ["id", "region"]  # not `margin`
    listed = await tools.list_tables(state, "seller", "sales")
    assert listed[0]["column_count"] == 2
    full = await tools.describe_table(state, "org_admin", "sales", "orders")
    assert [c["name"] for c in full["columns"]] == ["id", "region", "margin"]


async def test_a_table_the_role_is_not_served_is_not_found(state):
    with pytest.raises(ValueError, match="Table not found"):
        await tools.describe_table(state, "seller", "hr", "staff")


async def test_the_search_index_is_built_over_the_whole_catalog(state):
    whole = await tools._catalog(state, None)
    assert {(t.domain_id, t.table_name) for t in whole} == {("sales", "orders"), ("hr", "staff")}
    assert [c.name for t in whole if t.table_name == "orders" for c in t.columns] == [
        "id",
        "region",
        "margin",
    ]
