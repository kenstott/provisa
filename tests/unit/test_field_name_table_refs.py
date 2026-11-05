# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5e7d41-8a2b-4f19-b3d6-5e1a9c7f2b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered table named in SQL by its field name -- the domain-prefixed ``sa__orders`` a
domain_prefix schema exposes, which the validator admits as naming the table -- is that table:
governance resolves it to the table, and the physical rewrite lowers it to the table's relation.
Before, the name was admitted but neither resolved nor lowered: the statement was read
ungoverned under ``sa__orders`` wherever its route found a relation of that name, and refused by
an engine that had none ("Schema 'org_…' does not exist")."""

# Requirements: REQ-641

from __future__ import annotations

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_rewrite import normalize_table_refs
from provisa.compiler.stage2 import build_governance_context

_ORDERS, _CUSTOMERS = 1, 2


def _meta(table_id: int, field_name: str, table_name: str) -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=table_name.title(),
        source_id="sales-pg",
        catalog_name="sales_pg",
        schema_name="public",
        table_name=table_name,
        domain_id="sales-analytics",
    )


def _context() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "sa__orders": _meta(_ORDERS, "sa__orders", "orders"),
        "sa__customers": _meta(_CUSTOMERS, "sa__customers", "customers"),
    }
    return ctx


def _tables() -> list[dict]:
    return [
        {
            "id": table_id,
            "source_id": "sales-pg",
            "domain_id": "sales-analytics",
            "schema_name": "public",
            "table_name": name,
            "columns": [{"column_name": "id", "data_type": "integer", "visible_to": ["analyst"]}],
        }
        for table_id, name in ((_ORDERS, "orders"), (_CUSTOMERS, "customers"))
    ]


def test_governance_resolves_a_field_name_to_its_table() -> None:
    role = {"id": "analyst", "domain_access": ["*"], "capabilities": ["query_development"]}
    gov = build_governance_context("analyst", RLSContext.empty(), {}, _context(), _tables(), role)
    assert gov.table_map["sa__orders"] == _ORDERS
    assert gov.table_map["sa__customers"] == _CUSTOMERS


def test_the_physical_rewrite_lowers_a_field_name_to_its_table() -> None:
    lowered = normalize_table_refs(
        "SELECT o.id FROM sa__orders o JOIN sa__customers c ON o.id = c.id", _context()
    )
    assert '"public"."orders" AS "o"' in lowered
    assert '"public"."customers" AS "c"' in lowered


def test_a_table_named_so_keeps_its_name() -> None:
    # A table whose own name is another's field name is the one that name reads.
    ctx = _context()
    ctx.tables["sa__orders_t"] = _meta(3, "sa__orders_t", "sa__orders")
    assert '"public"."sa__orders"' in normalize_table_refs("SELECT id FROM sa__orders", ctx)
