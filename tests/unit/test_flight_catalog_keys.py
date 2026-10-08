# Copyright (c) 2026 Kenneth Stott
# Canary: 2d6f0a95-8b34-4e71-9c58-a1e7b3f5d026
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The catalog carries key metadata, narrowed like the rest of it (REQ-127, REQ-128).

A client that draws a model from the catalog — the JDBC driver's ``getPrimaryKeys`` and
``getImportedKeys`` — reads a column's declared primary-key flag and the column it refers to
through a declared to-one relationship from the column's Arrow field metadata. A reference is
listed only where its target is listed: a role is not told of a table or column it is not served
by way of a key.
"""

# Requirements: REQ-127, REQ-128

from __future__ import annotations

import json
import types

from provisa.api.flight.catalog import (
    _build_catalog_tables_async,
    catalog_table_to_arrow_schema,
)

# table id → (domain, table, [(column, is_primary_key)])
_TABLES = {
    1: ("sales", "orders", [("id", True), ("customer_id", False), ("rep_id", False)]),
    2: ("sales", "customers", [("id", True), ("name", False)]),
    3: ("hr", "staff", [("id", True), ("salary", False)]),
}
# (source table, source column, target table, target column, cardinality)
_RELATIONSHIPS = [
    (1, "customer_id", 2, "id", "many-to-one"),
    (1, "rep_id", 3, "id", "many-to-one"),
    (2, "id", 1, "customer_id", "one-to-many"),  # the to-many side is not a reference
    (1, "id", None, None, "many-to-one"),  # defined by a condition: no target column
]
_SERVED = {
    # Sees orders and customers, not staff.
    "seller": {1: ["id", "customer_id", "rep_id"], 2: ["id", "name"]},
    # Sees orders, and customers without the key column the reference lands on.
    "clerk": {1: ["id", "customer_id"], 2: ["name"]},
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
            # The statement itself excludes relationships with no target and the to-many side.
            assert "target_column IS NOT NULL" in sql and "'many-to-one', 'one-to-one'" in sql
            return [
                {
                    "source_table_id": s,
                    "source_column": sc,
                    "target_table_id": t,
                    "target_column": tc,
                }
                for s, sc, t, tc, cardinality in _RELATIONSHIPS
                if t is not None and cardinality in ("many-to-one", "one-to-one")
            ]
        return [
            {"table_id": table_id, "column_name": col, "description": "", "is_primary_key": pk}
            for table_id, (_, _, columns) in _TABLES.items()
            for col, pk in columns
        ]


def _state():
    def _context(served):
        return types.SimpleNamespace(
            tables={f"t{i}": types.SimpleNamespace(table_id=i) for i in served},
            physical_to_sql={(i, c): c for i, cols in served.items() for c in cols},
        )

    return types.SimpleNamespace(
        model_db=types.SimpleNamespace(acquire=lambda: _Conn()),
        engine_conn=None,
        contexts={role: _context(served) for role, served in _SERVED.items()},
    )


async def _keys(role: str | None) -> dict[tuple[str, str], dict]:
    """(table, column) → {"pk": bool, "references": (domain, table, column) | None}."""
    out = {}
    for table in await _build_catalog_tables_async(_state(), role):
        for column in table.columns:
            out[(table.table_name, column.name)] = {
                "pk": column.is_primary_key,
                "references": column.references,
            }
    return out


async def test_the_whole_catalog_carries_every_declared_key():
    keys = await _keys(None)
    assert keys[("orders", "id")] == {"pk": True, "references": None}
    assert keys[("orders", "customer_id")]["references"] == ("sales", "customers", "id")
    assert keys[("orders", "rep_id")]["references"] == ("hr", "staff", "id")
    assert keys[("customers", "id")] == {"pk": True, "references": None}  # not the to-many side
    assert keys[("staff", "id")]["pk"] is True


async def test_a_reference_to_a_table_the_role_is_not_served_is_not_listed():
    keys = await _keys("seller")
    assert ("staff", "id") not in keys
    assert keys[("orders", "customer_id")]["references"] == ("sales", "customers", "id")
    assert keys[("orders", "rep_id")]["references"] is None, "staff is not in this role's catalog"


async def test_a_reference_to_a_column_the_role_is_not_served_is_not_listed():
    keys = await _keys("clerk")
    assert ("customers", "id") not in keys
    assert keys[("orders", "customer_id")]["references"] is None


async def test_the_keys_travel_in_the_arrow_field_metadata():
    (orders,) = [
        t for t in await _build_catalog_tables_async(_state(), "seller") if t.table_name == "orders"
    ]
    schema = catalog_table_to_arrow_schema(orders)
    assert schema.field("id").metadata == {b"primary_key": b"true"}
    assert json.loads(schema.field("customer_id").metadata[b"references"]) == {
        "domain": "sales",
        "table": "customers",
        "column": "id",
    }
    assert not schema.field("rep_id").metadata, "no key metadata for a reference not listed"
