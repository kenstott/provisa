# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit: a Cypher read's list functions on the pg engine. The Cypher read names them as Trino does
(gql/expr_visitor: element_at, slice, transform, any_match / all_match / none_match, reduce,
range_step, type_refused, json_kind, json_size, json_order_key, json_list_compare) and DuckDB's
transpile renders each as its own; PostgreSQL has none of them, nor lambdas, so each was refused
there ("function element_at(integer[], integer) does not exist", "column "x" does not exist").
On PostgreSQL each is its own SQL (transpile.postgres_list_functions); on DuckDB and Trino each
is as it was."""

# Requirements: REQ-345, REQ-347, REQ-066

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp
from sqlglot.optimizer.simplify import simplify

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import (
    LIST_COMPARE_TOO_DEEP,
    RANGE_STEP_ZERO,
    transpile,
    transpile_to_duckdb,
    transpile_to_trino,
)

pytestmark = pytest.mark.unit

#: The IR's engine functions PostgreSQL does not have.
_IR_NAMES = frozenset(
    {
        "ELEMENT_AT",
        "SLICE",
        "ANY_MATCH",
        "ALL_MATCH",
        "NONE_MATCH",
        "RANGE_STEP",
        "TYPE_REFUSED",
        "JSON_KIND",
        "JSON_SIZE",
        "JSON_ORDER_KEY",
        "JSON_LIST_COMPARE",
        "SEQUENCE",
    }
)

# (statement, DuckDB's rows): each statement's answer, which DuckDB's form keeps.
_ELEMENT_AT = (
    "WITH [1, 2, 3] AS l, 1 AS k "
    "RETURN l[0] AS a, l[-1] AS b, l[k] AS c, l[-k] AS d, head(l) AS h, last(l) AS z, l[5] AS o"
)
_SLICE = (
    "WITH [1, 2, 3, 4] AS l "
    "RETURN l[1..3] AS a, l[..2] AS b, l[2..] AS c, l[-3..-1] AS d, tail(l) AS t"
)
_COMPREHENSION = (
    "WITH [1, 2, 3] AS l RETURN [x IN l WHERE x > 1 | x * 10] AS a, [x IN l | x + 1] AS b, "
    "reverse(l) AS r"
)
_QUANTIFIERS = (
    "WITH [1, 2, 3] AS l, [1, null] AS m "
    "RETURN any(x IN l WHERE x > 2) AS a, all(x IN l WHERE x > 0) AS b, "
    "none(x IN l WHERE x > 5) AS c, single(x IN l WHERE x = 2) AS d, "
    "any(x IN m WHERE x > 1) AS e, all(x IN m WHERE x > 0) AS f, none(x IN m WHERE x > 0) AS g"
)
_REDUCE = (
    "RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS s, reduce(p = 1, x IN [2, 3, 4] | p * x) AS p"
)
_RANGE_STEP = "WITH -3 AS s UNWIND range(10, 1, s) AS i RETURN i"
_TYPE_REFUSED = "RETURN [x IN [1, 'a', [2]] | toString(x)] AS l"
_LIST_COMPARE = (
    "WITH [[1, 'a'], [1, 'b'], 'x'] AS types WITH types[0] AS a, types[1] AS b "
    "RETURN a < b AS r, a > b AS s"
)
_ORDER_KEY = "WITH [[1, 0], [1], [0, 9], 'x'] AS types UNWIND types AS t RETURN t ORDER BY t"
_JSON_SIZE = "RETURN [x IN [1, 'ab', [1, 2, 3]] | size(x)] AS l"

# (statement, DuckDB's rows, the Trino function the statement keeps on Trino)
_ANSWERS = [
    pytest.param(_ELEMENT_AT, [(1, 3, 2, 3, 1, 3, None)], "element_at", id="element_at"),
    pytest.param(_SLICE, [([2, 3], [1, 2], [3, 4], [2, 3], [2, 3, 4])], "slice", id="slice"),
    pytest.param(
        _COMPREHENSION, [([20, 30], [2, 3, 4], [3, 2, 1])], "transform", id="filter-transform"
    ),
    pytest.param(
        _QUANTIFIERS,
        [(True, True, True, True, None, None, False)],
        "all_match",
        id="quantifiers",
    ),
    pytest.param(_REDUCE, [(6, 24)], "reduce", id="reduce"),
    pytest.param(_LIST_COMPARE, [(True, False)], "json_format", id="json_list_compare"),
    pytest.param(_JSON_SIZE, [([None, 2, 3],)], "json_array_length", id="json_size"),
]


def _semantic_sql(statement: str) -> str:
    """The statement's SQL as the Cypher read writes it, before an engine's transpile."""
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher(statement), CypherLabelMap(nodes={}, relationships={}), {}
    )
    return sql_ast.sql(dialect="postgres")


def _postgres(statement: str) -> exp.Expr:
    """The statement as the pg engine runs it, parsed: PostgreSQL SQL with no IR function left."""
    sql = transpile(_semantic_sql(statement), "postgres")
    tree = sqlglot.parse_one(sql, read="postgres")
    left = {n.name.upper() for n in tree.find_all(exp.Anonymous) if n.name.upper() in _IR_NAMES}
    assert not left, sql
    assert not list(tree.find_all(exp.Lambda, exp.Transform, exp.Reduce)), sql
    return tree


def _calls(tree: exp.Expr, name: str) -> list[exp.Anonymous]:
    return [n for n in tree.find_all(exp.Anonymous) if n.name.upper() == name.upper()]


def test_an_index_on_postgres_is_a_subscript_from_either_end():
    tree = _postgres(_ELEMENT_AT)
    subscripts = list(tree.find_all(exp.Bracket))
    assert subscripts, tree.sql("postgres")
    # From the end: the list's cardinality plus one plus the (negative) index.
    assert _calls(tree, "cardinality"), tree.sql("postgres")
    # A computed index is read from the end where it is negative.
    assert any(
        isinstance(i.this, exp.LT) for c in tree.find_all(exp.Case) for i in c.args["ifs"]
    ), tree.sql("postgres")


def test_a_slice_on_postgres_is_an_inclusive_array_slice():
    tree = _postgres("WITH [1, 2, 3, 4] AS l RETURN l[1..3] AS a")
    (subscript,) = tree.find_all(exp.Bracket)
    (bounds,) = subscript.expressions
    assert isinstance(bounds, exp.Slice), tree.sql("postgres")
    # Cypher's l[1..3] (0-based, end excluded) is PostgreSQL's l[2:3] (1-based, both included).
    assert simplify(bounds.this).sql() == "2", tree.sql("postgres")
    assert simplify(bounds.expression).sql() == "3", tree.sql("postgres")


def test_a_projected_list_on_postgres_is_the_array_of_its_rows():
    tree = _postgres("WITH [1, 2, 3] AS l RETURN [x IN l | x + 1] AS b")
    (listed,) = [a for a in tree.find_all(exp.Array) if a.find(exp.Select)]
    assert isinstance(listed.expressions[0], exp.Select), tree.sql("postgres")
    assert listed.expressions[0].find(exp.Unnest) is not None, tree.sql("postgres")
    # A null list projects to null, as on Trino, never to an empty list.
    guard = listed.parent
    assert isinstance(guard, exp.Case), tree.sql("postgres")
    (null_list,) = guard.args["ifs"]
    assert isinstance(null_list.this, exp.Is), tree.sql("postgres")
    assert isinstance(null_list.args["true"], exp.Null), tree.sql("postgres")


@pytest.mark.parametrize(
    ("quantifier", "aggregate"),
    [("any", "bool_or"), ("all", "bool_and"), ("none", "bool_or")],
)
def test_a_quantifier_on_postgres_aggregates_its_predicate(quantifier, aggregate):
    tree = _postgres(f"WITH [1, 2] AS l RETURN {quantifier}(x IN l WHERE x > 1) AS q")
    assert f"{aggregate.upper()}(" in tree.sql("postgres"), tree.sql("postgres")
    # Three-valued: a null predicate value with none deciding answers null.
    assert any(isinstance(c.this, exp.Count) for c in tree.find_all(exp.GT)), tree.sql()


def test_a_fold_on_postgres_is_a_recursive_subquery():
    tree = _postgres(_REDUCE)
    folds = [w for w in tree.find_all(exp.With) if w.args.get("recursive")]
    assert len(folds) == 2, tree.sql("postgres")


def test_a_computed_step_of_zero_on_postgres_raises_its_error():
    tree = _postgres(_RANGE_STEP)
    raised = [
        c
        for c in tree.find_all(exp.Cast)
        if c.this.find(exp.Literal) and RANGE_STEP_ZERO in c.sql()
    ]
    # The message cast to an integer, from a subquery PostgreSQL does not fold when planning.
    assert raised and isinstance(raised[0].this, exp.Subquery), tree.sql("postgres")
    assert raised[0].to.is_type(exp.DataType.Type.INT), tree.sql("postgres")


def test_a_refused_kind_on_postgres_raises_where_its_branch_is_reached():
    tree = _postgres(_TYPE_REFUSED)
    (raised,) = [c for c in tree.find_all(exp.Cast) if "TypeError" in c.sql()]
    # The raise is the branch's test: the branch's value is null, of whatever type the CASE is.
    branch = raised.find_ancestor(exp.If)
    while branch is not None and not isinstance(branch.args.get("true"), exp.Null):
        branch = branch.find_ancestor(exp.If)
    assert branch is not None, tree.sql("postgres")


def test_lists_of_several_types_compared_on_postgres_read_their_items():
    tree = _postgres(_LIST_COMPARE)
    assert _calls(tree, "json_array_elements"), tree.sql("postgres")
    # Their strings compare by character code, as Trino's and DuckDB's do.
    assert list(tree.find_all(exp.Collate)), tree.sql("postgres")
    sql = tree.sql("postgres")
    deep = transpile(
        _semantic_sql(
            "WITH [[[[[1]]]], [[[[1]]]], 'x'] AS t WITH t[0] AS a, t[1] AS b RETURN a < b"
        ),
        "postgres",
    )
    assert LIST_COMPARE_TOO_DEEP in deep, sql


def test_an_order_of_values_of_several_types_on_postgres_is_its_key():
    tree = _postgres(_ORDER_KEY)
    (order,) = tree.find_all(exp.Order)
    (key,) = order.expressions
    assert isinstance(key.this, exp.Anonymous) and key.this.name.upper() == "ROW", tree.sql()
    assert "JSON_EXTRACT_PATH(" in tree.sql("postgres"), tree.sql("postgres")
    assert key.this.expressions[-1].find(exp.Collate) is not None, tree.sql("postgres")


def test_a_size_of_an_item_of_several_types_on_postgres_reads_its_json():
    tree = _postgres(_JSON_SIZE)
    assert _calls(tree, "json_array_length"), tree.sql("postgres")
    # An item that is a string is typed text for to_json, which PostgreSQL cannot infer.
    assert "TO_JSON(CAST('ab' AS TEXT))" in tree.sql("postgres"), tree.sql("postgres")


@pytest.mark.parametrize(("statement", "rows", "trino_function"), _ANSWERS)
def test_duckdb_answers_as_it_did(statement, rows, trino_function):
    del trino_function
    sql = transpile_to_duckdb(_semantic_sql(statement))
    assert duckdb.connect().execute(sql).fetchall() == rows


@pytest.mark.parametrize(("statement", "rows", "trino_function"), _ANSWERS)
def test_trino_keeps_its_own_list_functions(statement, rows, trino_function):
    del rows
    sql = transpile_to_trino(_semantic_sql(statement))
    tree = sqlglot.parse_one(sql, read="trino")
    assert trino_function.upper() in sql.upper(), sql
    # None of PostgreSQL's forms: no list of a subquery's rows, no collation, no recursion.
    assert "ARRAY(SELECT" not in sql.upper(), sql
    assert not list(tree.find_all(exp.Collate)), sql
    assert not [w for w in tree.find_all(exp.With) if w.args.get("recursive")], sql
