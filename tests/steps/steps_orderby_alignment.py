# Copyright (c) 2026 Kenneth Stott
# Canary: dd477e5f-91f7-48f6-ba91-07ff1b054990
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-202 — OrderBy Alignment (relationship ordering).

Verifies that a GraphQL order_by argument referencing a related object field
(e.g. order_by: {customer: {name: asc}}) compiles to a SQL ORDER BY clause that
references the related table's column — matching Hasura v2 default behavior.
"""

from __future__ import annotations

import pytest
from graphql import parse, validate
from pytest_bdd import given, when, then, parsers, scenario

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_schema_with_relationship():
    """Build a schema with orders -> customers object relationship 'customer'.

    The 'customer' relationship maps orders.customer_id to customers.id, so that
    order_by: {customer: {name: asc}} can sort orders by the related customer name.
    """
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [
            _col("id", "integer"),
            _col("customer_id", "integer"),
            _col("amount", "decimal(10,2)"),
        ],
        2: [
            _col("id", "integer"),
            _col("name", "varchar(100)"),
        ],
    }
    relationships = [
        {
            "id": 1,
            "name": "customer",
            "source_table_id": 1,
            "target_table_id": 2,
            "ref_table_id": 2,
            "kind": "object",
            "relationship_type": "object",
            "type": "object",
            "mapping": [{"source_column": "customer_id", "ref_column": "id"}],
            "column_mapping": {"customer_id": "id"},
        },
    ]
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"sales-pg": "postgresql"},
    )
    return generate_schema(si), build_context(si)


# ---------------------------------------------------------------------------
# Scenario binding
# ---------------------------------------------------------------------------


@scenario(
    "../features/REQ-202.feature",
    "REQ-202 default behaviour",
)
def test_req_202_default_behaviour():
    """REQ-202 — order by related object field."""


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@given("a query with order_by referencing a related object field")
def given_query_with_relationship_order_by(shared_data):
    schema, ctx = _build_schema_with_relationship()
    shared_data["schema"] = schema
    shared_data["ctx"] = ctx
    doc = parse(
        "{ orders(order_by: { customer: { name: asc } }) { id amount } }"
    )
    errors = validate(schema, doc)
    assert not errors, f"GraphQL validation failed: {errors}"
    shared_data["doc"] = doc


@when("the compiler processes it")
def when_compiler_processes(shared_data):
    doc = shared_data["doc"]
    ctx = shared_data["ctx"]
    results = compile_query(doc, ctx)
    assert results, "compile_query returned no results"
    result = results[0]
    assert result.sql, "compiled SQL is empty"
    shared_data["result"] = result
    shared_data["sql"] = result.sql


@then("the SQL ORDER BY clause references the related table's column")
def then_order_by_references_related_column(shared_data):
    sql = shared_data["sql"]
    sql_upper = sql.upper()
    assert "ORDER BY" in sql_upper, f"No ORDER BY in compiled SQL:\n{sql}"

    order_idx = sql_upper.index("ORDER BY")
    after_order = sql[order_idx:]

    # The related table's ordering column is customers.name — the ORDER BY
    # must reference the related column, not a parent (orders) column.
    assert '"name"' in after_order or "name" in after_order.lower(), (
        f"ORDER BY does not reference the related column 'name':\n{sql}"
    )
    assert "ASC" in after_order.upper(), (
        f"ORDER BY missing ASC direction:\n{sql}"
    )
