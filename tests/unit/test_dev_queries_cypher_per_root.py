# Copyright (c) 2026 Kenneth Stott
# Canary: 611b72a1-09dc-4fd2-949d-866834eb5c10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL query of several root fields becomes several Cypher statements, one per root field
in the query's order, as it becomes several SQL ones (REQ-2136). Each statement runs to the rows
its own SQL statement does -- both run on DuckDB, the Cypher compiled the way a Cypher read is --
and a root that has no Cypher form says why in its place without taking the others' away."""

# Requirements: REQ-2136, REQ-345

from types import SimpleNamespace

import duckdb
import pytest
import sqlglot
from graphql import parse, validate

from provisa.api.admin import dev_queries
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import make_semantic_sql
from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from tests.unit.test_sql_gen import _build_schema_and_ctx
from tests.unit.test_where_relationships import RELATIONSHIPS

QUERY = "{ customers { id name } orders { id region } }"
ROLE = {"domain_access": ["*"]}


@pytest.fixture
def compiled():
    schema, ctx = _build_schema_and_ctx(relationships=RELATIONSHIPS)
    doc = parse(QUERY)
    assert not validate(schema, doc)
    return ctx, doc, compile_query(doc, ctx)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("CREATE SCHEMA sales")
    c.execute("CREATE TABLE sales.customers (id INTEGER, name VARCHAR, email VARCHAR)")
    c.execute("INSERT INTO sales.customers VALUES (1, 'Ann', 'a@x'), (2, 'Bo', 'b@x')")
    c.execute(
        "CREATE TABLE sales.orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), "
        "region VARCHAR, status VARCHAR, created_at TIMESTAMP)"
    )
    c.execute(
        "INSERT INTO sales.orders VALUES (10, 1, 5, 'us', 'new', NULL), "
        "(11, 2, 7, 'eu', 'new', NULL), (12, 1, 9, 'us', 'shipped', NULL)"
    )
    return c


def _per_root(
    ctx, doc, roots, *, flat: bool = False, node_only: bool = False
) -> list[tuple[str | None, str | None]]:
    state = SimpleNamespace(schema_build_cache={}, source_catalogs=None)
    return [
        dev_queries._compile_cypher_for_result(ctx, state, ROLE, doc, None, flat, node_only, i)
        for i in range(len(roots))
    ]


def _rows(con, sql: str) -> list[tuple]:
    return sorted(
        con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0]).fetchall()
    )


def _cypher_rows(con, ctx, cypher: str) -> list[tuple]:
    lm = CypherLabelMap.from_schema(ctx, domain_access=["*"])
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, lm)
    return _rows(con, make_semantic_sql(sql_ast.sql(dialect="postgres"), ctx))


def test_each_root_field_is_its_own_cypher_statement_running_to_its_sql_s_rows(compiled, con):
    ctx, doc, roots = compiled
    assert [r.root_field for r in roots] == ["customers", "orders"]
    statements = _per_root(ctx, doc, roots)
    assert [error for _, error in statements] == [None, None]
    customers, orders = (cypher for cypher, _ in statements)
    assert customers is not None and "Customers" in customers and "Orders" not in customers
    assert orders is not None and "Orders" in orders and "Customers" not in orders
    for root, (cypher, _) in zip(roots, statements, strict=True):
        assert cypher is not None
        sql_rows = _rows(con, make_semantic_sql(root.sql, ctx))
        assert sql_rows and _cypher_rows(con, ctx, cypher) == sql_rows


@pytest.mark.parametrize(("flat", "node_only"), [(True, False), (False, True)])
def test_each_projection_translates_each_root_not_the_first_twice(compiled, flat, node_only):
    ctx, doc, roots = compiled
    (customers, _), (orders, _) = _per_root(ctx, doc, roots, flat=flat, node_only=node_only)
    assert customers is not None and orders is not None
    assert "Customers" in customers and "Orders" in orders


def test_a_root_with_no_cypher_form_says_why_and_leaves_the_others_theirs(compiled, monkeypatch):
    ctx, doc, roots = compiled
    built = CypherLabelMap.from_schema

    def without_orders(*args, **kwargs):
        # A root whose table is no node label -- as a junction the relationships traverse is.
        lm = built(*args, **kwargs)
        for label in [k for k, n in lm.nodes.items() if n.table_name == "orders"]:
            del lm.nodes[label]
        return lm

    monkeypatch.setattr(CypherLabelMap, "from_schema", without_orders)
    (customers, customers_error), (orders, orders_error) = _per_root(ctx, doc, roots)
    assert customers is not None and customers_error is None
    assert orders is None and orders_error


def test_the_roots_are_not_folded_into_one_statement():
    assert not hasattr(dev_queries, "_combine_cypher_results")
