# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e3c1e-7d0a-4a52-9d1f-3f6f0c1c2a77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Comparison of the items of a list that holds a node, a relationship or a path beside
literals (openCypher TCK Comparison2 [3]). openCypher 9, "Comparability and orderability":
values of different types are not comparable, so an ordering comparison of them is null; nodes,
relationships and paths have equality but no order, so ordering them is null too. The only true
pair of the TCK's list is 1 < 3.14. Answered on DuckDB over the demo model (REQ-347, REQ-2167)."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import duckdb
import pytest

from provisa.compiler.sql_rewrite import lower_to_catalog_physical, make_semantic_sql
from provisa.compiler.view_expand import expand_view_refs
from provisa.cypher.parser import parse_cypher
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit import permits_on_duckdb as permits

pytestmark = pytest.mark.unit

_MATCH = "MATCH p = (n:Permits)-[r:DRAWS]->(c:Complaints) "
_PAIRS = (
    "UNWIND range(0, size(types) - 1) AS i UNWIND range(0, size(types) - 1) AS j "
    "WITH types[i] AS lhs, types[j] AS rhs WHERE i <> j "
)


@pytest.fixture(scope="module")
def model():
    return permits.model()


def _rows(model, query: str) -> list[tuple]:
    ctx, label_map, view_sql = model
    sql, _params, graph_vars = cypher_to_sql(parse_cypher(query), label_map, {})
    sql = apply_graph_rewrites(sql, graph_vars, label_map)
    physical = lower_to_catalog_physical(
        expand_view_refs(make_semantic_sql(sql.sql(dialect="postgres"), ctx), view_sql), ctx
    )
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS \"drop\"")
    con.execute('CREATE SCHEMA "drop"."drop"')
    con.execute(
        'CREATE TABLE "drop"."drop".building_permits (permit_id INTEGER, address VARCHAR, '
        "neighborhood VARCHAR, permit_type VARCHAR, issued_on DATE, expires_on DATE, "
        "status VARCHAR, valuation DOUBLE)"
    )
    con.execute(
        'CREATE TABLE "drop"."drop".complaints (complaint_id INTEGER, opened_at TIMESTAMP, '
        "status VARCHAR, closed_at TIMESTAMP, category VARCHAR, neighborhood VARCHAR, "
        "address VARCHAR)"
    )
    con.execute(
        "INSERT INTO \"drop\".\"drop\".building_permits VALUES (1,'a','n','t',NULL,NULL,'open',1.0)"
    )
    con.execute("INSERT INTO \"drop\".\"drop\".complaints VALUES (7,NULL,'open',NULL,'x','n','a')")
    try:
        return con.execute(transpile_to_duckdb(physical)).fetchall()
    finally:
        con.close()


def test_comparison2_3_orders_only_the_numbers(model):
    query = (
        _MATCH
        + "WITH [n, r, p, '', 1, 3.14, true, null, [], {}] AS types "
        + _PAIRS
        + "WITH lhs, rhs, lhs < rhs AS result WHERE result RETURN lhs, rhs"
    )
    assert [tuple(str(v) for v in row) for row in _rows(model, query)] == [("1", "3.14")]


@pytest.mark.parametrize("op", ["<", "<=", ">", ">="])
def test_a_graph_element_has_no_order_against_any_item(model, op):
    # Every ordering of a node, relationship or path against another item is null; the other
    # items here are of three kinds, none ordered against another either. (toString() of a
    # graph element is a type error, openCypher: it never picks the pairs out.)
    query = (
        _MATCH
        + "WITH [n, r, p, '', 1, {}] AS types "
        + _PAIRS
        + f"WITH lhs, rhs, lhs {op} rhs AS result "
        "WHERE lhs IS NOT NULL RETURN result"
    )
    assert {row[0] for row in _rows(model, query)} == {None}


def test_a_graph_element_equals_itself_in_a_mixed_list(model):
    query = (
        _MATCH + "WITH [n, r, p, 1, 'a'] AS types "
        "UNWIND range(0, size(types) - 1) AS i UNWIND range(0, size(types) - 1) AS j "
        "WITH types[i] AS lhs, types[j] AS rhs, i, j WHERE types[i] = types[j] "
        "RETURN i, j ORDER BY i, j"
    )
    assert _rows(model, query) == [(k, k) for k in range(5)]
