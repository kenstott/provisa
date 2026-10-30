# Copyright (c) 2026 Kenneth Stott
# Canary: 7d1c2a9e-3b64-4f0a-9a57-5e0d6c1b8f42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Ordering comparison (<, <=, >, >=) of two lists. openCypher 9, "Comparability and
orderability": lists compare element by element, a shorter prefix list is smaller, and an element
pair that cannot be compared (two kinds, a null) makes the result null. Plain lists and lists that
are items of a mixed list answer alike. Answered on DuckDB (REQ-347)."""

# Requirements: REQ-347

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
    try:
        return con.execute(transpile_to_duckdb(physical)).fetchall()
    finally:
        con.close()


def _plain(model, lhs: str, op: str, rhs: str):
    return _rows(model, f"RETURN {lhs} {op} {rhs} AS r")[0][0]


def _in_mixed(model, lhs: str, op: str, rhs: str):
    # Both lists are items of a list of several types (a string beside them makes it so).
    query = (
        f"WITH [{lhs}, {rhs}, 'x'] AS types WITH types[0] AS a, types[1] AS b RETURN a {op} b AS r"
    )
    return _rows(model, query)[0][0]


# (lhs, rhs, {op: expected}); None is null.
_CASES = [
    ("[1]", "[2]", {"<": True, "<=": True, ">": False, ">=": False}),
    ("[2]", "[1]", {"<": False, "<=": False, ">": True, ">=": True}),
    ("[1, 2]", "[1, 2]", {"<": False, "<=": True, ">": False, ">=": True}),
    ("[1, 2]", "[1, 3]", {"<": True, "<=": True, ">": False, ">=": False}),
    # a shorter list that is a prefix is smaller
    ("[1]", "[1, 0]", {"<": True, "<=": True, ">": False, ">=": False}),
    ("[1, 0]", "[1]", {"<": False, "<=": False, ">": True, ">=": True}),
    ("[]", "[1]", {"<": True, "<=": True, ">": False, ">=": False}),
    ("[]", "[]", {"<": False, "<=": True, ">": False, ">=": True}),
    # strings and booleans by value
    ("['a', 'b']", "['a', 'c']", {"<": True, "<=": True, ">": False, ">=": False}),
    ("[false]", "[true]", {"<": True, "<=": True, ">": False, ">=": False}),
    # an element pair of two kinds is incomparable: null, even beside a decided prefix of nothing
    ("['1']", "[1]", {"<": None, "<=": None, ">": None, ">=": None}),
    ("[1, 'a']", "[1, 2]", {"<": None, "<=": None, ">": None, ">=": None}),
    # an earlier differing element decides before a later incomparable one
    ("[1, 'a']", "[2, 3]", {"<": True, "<=": True, ">": False, ">=": False}),
    # nulls
    ("[null]", "[1]", {"<": None, "<=": None, ">": None, ">=": None}),
    ("[1, null]", "[1, 2]", {"<": None, "<=": None, ">": None, ">=": None}),
    ("[1, null]", "[2, 2]", {"<": True, "<=": True, ">": False, ">=": False}),
    # nested lists recurse
    ("[[1], [2]]", "[[1], [3]]", {"<": True, "<=": True, ">": False, ">=": False}),
    ("[[1, 2]]", "[[1]]", {"<": False, "<=": False, ">": True, ">=": True}),
    ("[[1], 'a']", "[['1'], 'a']", {"<": None, "<=": None, ">": None, ">=": None}),
]


@pytest.mark.parametrize("lhs,rhs,expected", _CASES)
@pytest.mark.parametrize("op", ["<", "<=", ">", ">="])
def test_plain_lists(model, lhs, rhs, expected, op):
    assert _plain(model, lhs, op, rhs) == expected[op]


@pytest.mark.parametrize("lhs,rhs,expected", _CASES)
@pytest.mark.parametrize("op", ["<", "<=", ">", ">="])
def test_lists_inside_a_mixed_list(model, lhs, rhs, expected, op):
    assert _in_mixed(model, lhs, op, rhs) == expected[op]


def test_a_list_against_a_non_list_in_a_mixed_list_is_null(model):
    query = "WITH [[1], 1, 'x'] AS types WITH types[0] AS a, types[1] AS b RETURN a < b AS r"
    assert _rows(model, query)[0][0] is None


def test_order_by_of_lists_in_a_mixed_list_puts_a_prefix_first(model):
    # ORDER BY (json_order_key, REQ-345) agrees with < on a prefix: the shorter list is first.
    query = (
        "WITH [[1, 0], [1], [0, 9], 'x'] AS types UNWIND types AS t "
        "WITH t WHERE t IS NOT NULL RETURN t ORDER BY t"
    )
    ordered = [str(r[0]).replace(" ", "") for r in _rows(model, query)]
    assert ordered.index("[1]") < ordered.index("[1,0]")
    assert ordered.index("[0,9]") < ordered.index("[1]")


def test_nesting_beyond_the_limit_is_refused_by_name(model):
    deep = "[[[[[1]]]]]"
    query = f"WITH [{deep}, {deep}, 'x'] AS types WITH types[0] AS a, types[1] AS b RETURN a < b"
    with pytest.raises(duckdb.InvalidInputException, match="nested deeper than"):
        _rows(model, query)


def test_nesting_at_the_limit_compares(model):
    query = "WITH [[[[1]]], [[[2]]], 'x'] AS types WITH types[0] AS a, types[1] AS b RETURN a < b"
    assert _rows(model, query)[0][0] is True


def test_trino_expansion_has_no_engine_function_left():
    import sqlglot

    from provisa.transpiler.transpile import transpile_to_trino

    sql = transpile_to_trino("SELECT json_list_compare(a, b) < 0 FROM t")
    assert "json_list_compare" not in sql.lower()
    sqlglot.parse_one(sql, read="trino")  # the expansion is Trino SQL
