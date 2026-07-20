# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Property-based tests for GraphQL -> SQL compilation, Stage 1 (REQ-262, REQ-265).

compile_graphql lowers a GraphQL selection to SQL — the front of every read path. A
bug selects columns the client did not ask for (over-fetch / leak), references a
table the query never named (injection surface), drops the LIMIT, or emits invalid
SQL. Generate valid queries over a fixed two-table schema and assert, for each:

  * the SQL parses;
  * it references ONLY the root's table — nothing the query didn't name;
  * its projected columns are exactly the requested fields (no over/under-fetch);
  * a LIMIT/OFFSET argument appears iff the query carried one, with its value bound
    as a parameter (never inlined);
  * root_field echoes the query's root.
"""

from __future__ import annotations

import sqlglot
from graphql import parse as gql_parse
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlglot import exp

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage1 import compile_graphql

_ROOTS = {"orders": "Orders", "customers": "Customers"}
_COLS = ["id", "amount", "region", "name", "status", "created_at"]


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    for i, (field, type_name) in enumerate(_ROOTS.items(), start=1):
        ctx.tables[field] = TableMeta(
            table_id=i,
            field_name=field,
            type_name=type_name,
            source_id="test-pg",
            catalog_name="test_pg",
            schema_name="public",
            table_name=field,
        )
    return ctx


@st.composite
def _gql_query(draw):
    """A single-root GraphQL read over the fixed schema, optionally with limit/offset.
    Returns (query, root, fields, limit, offset)."""
    root = draw(st.sampled_from(sorted(_ROOTS)))
    fields = draw(st.lists(st.sampled_from(_COLS), min_size=1, max_size=5, unique=True))
    limit = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=1000)))
    offset = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=1000)))
    args = []
    if limit is not None:
        args.append(f"limit: {limit}")
    if offset is not None:
        args.append(f"offset: {offset}")
    arg_str = f"({', '.join(args)})" if args else ""
    query = f"{{ {root}{arg_str} {{ {' '.join(fields)} }} }}"
    return query, root, fields, limit, offset


@settings(max_examples=300, deadline=None)
@given(case=_gql_query())
def test_compile_is_valid_governed_and_faithful(case) -> None:
    query, root, fields, limit, offset = case
    results = compile_graphql(gql_parse(query), _ctx())
    assert len(results) == 1
    compiled = results[0]
    assert compiled.root_field == root

    tree = sqlglot.parse_one(compiled.sql, read="postgres")

    # References only the table the query named.
    assert {t.name for t in tree.find_all(exp.Table)} == {root}

    # Projects exactly the requested fields — no over- or under-fetch.
    projected = {c.name for c in tree.find_all(exp.Column)}
    assert projected == set(fields)

    # LIMIT/OFFSET appear iff requested; their values are bound as params, not inlined.
    assert (tree.args.get("limit") is not None) == (limit is not None)
    assert (tree.args.get("offset") is not None) == (offset is not None)
    expected_params = sum(x is not None for x in (limit, offset))
    assert len(compiled.params) == expected_params
    for literal in (limit, offset):
        if literal is not None:
            assert literal in compiled.params


# --------------------------------------------------------------------------- #
# Nested relationship compilation (sql_selection): orders -> customer.
# Reuses the governed schema builder (orders + customers + many-to-one relationship)
# already exercised by test_sql_gen — the relationship-aware context nested queries
# need.
# --------------------------------------------------------------------------- #
from tests.unit.test_sql_gen import _build_schema_and_ctx, _col  # noqa: E402

_ORDER_COLS = ["id", "customer_id", "amount", "region", "status"]
_CUST_COLS = ["id", "name", "email"]


@st.composite
def _nested_query(draw):
    order_fields = draw(st.lists(st.sampled_from(_ORDER_COLS), min_size=1, max_size=4, unique=True))
    cust_fields = draw(st.lists(st.sampled_from(_CUST_COLS), min_size=1, max_size=3, unique=True))
    query = f"{{ orders {{ {' '.join(order_fields)} customer {{ {' '.join(cust_fields)} }} }} }}"
    return query, order_fields, cust_fields


@settings(max_examples=200, deadline=None)
@given(case=_nested_query())
def test_nested_relationship_compiles_faithfully(case) -> None:
    """A nested `customer { ... }` compiles to SQL that references only orders and
    customers, projects every requested root field plus a `customer` relationship
    column, and carries every requested customer field into the nested subquery."""
    query, order_fields, cust_fields = case
    _, ctx = _build_schema_and_ctx()
    compiled = compile_graphql(gql_parse(query), ctx)[0]
    assert compiled.root_field == "orders"

    tree = sqlglot.parse_one(compiled.sql, read="postgres")
    assert {t.name for t in tree.find_all(exp.Table)} <= {"orders", "customers"}

    top_level = {c.field_name for c in compiled.columns if c.nested_in is None}
    assert set(order_fields) <= top_level, "a requested root field was dropped"
    assert "customer" in top_level, "the nested relationship column is missing"

    # Every requested customer field is carried into the (subquery) SQL.
    for f in cust_fields:
        assert f'"{f}"' in compiled.sql, f"customer.{f} did not reach the nested SQL"


# --------------------------------------------------------------------------- #
# One-to-many nesting (sql_selection array_agg): customers -> orders.
# --------------------------------------------------------------------------- #
def _o2m_ctx() -> CompilationContext:
    tables = [
        {
            "id": 1,
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
        {
            "id": 2,
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
    ]
    rels = [
        {
            "id": "cust-orders",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "id",
            "target_column": "customer_id",
            "cardinality": "one-to-many",
        }
    ]
    cts = {
        1: [_col("id", "integer"), _col("name")],
        2: [_col("id", "integer"), _col("customer_id", "integer"), _col("amount", "integer")],
    }
    _, ctx = _build_schema_and_ctx(tables=tables, relationships=rels, column_types=cts)
    return ctx


_O2M_CTX = _o2m_ctx()
_CUST2 = ["id", "name"]
_ORDER2 = ["id", "customer_id", "amount"]


@st.composite
def _o2m_query(draw):
    cust = draw(st.lists(st.sampled_from(_CUST2), min_size=1, max_size=2, unique=True))
    orders = draw(st.lists(st.sampled_from(_ORDER2), min_size=1, max_size=3, unique=True))
    return f"{{ customers {{ {' '.join(cust)} orders {{ {' '.join(orders)} }} }} }}", cust, orders


@settings(max_examples=200, deadline=None)
@given(case=_o2m_query())
def test_one_to_many_nesting_aggregates_children(case) -> None:
    """A one-to-many `orders { ... }` under customers compiles to an aggregating
    subquery (json_agg): only customers/orders tables, the parent fields projected
    plus an `orders` one-to-many column, every child field carried into the subquery."""
    query, cust_fields, order_fields = case
    compiled = compile_graphql(gql_parse(query), _O2M_CTX)[0]
    assert compiled.root_field == "customers"
    assert "json_agg" in compiled.sql, "one-to-many did not aggregate into an array"

    tree = sqlglot.parse_one(compiled.sql, read="postgres")
    assert {t.name for t in tree.find_all(exp.Table)} <= {"customers", "orders"}

    cols = {c.field_name: c for c in compiled.columns if c.nested_in is None}
    assert set(cust_fields) <= set(cols)
    assert cols.get("orders") is not None and cols["orders"].cardinality == "one-to-many"
    for f in order_fields:
        assert f'"{f}"' in compiled.sql, f"orders.{f} did not reach the aggregated subquery"
