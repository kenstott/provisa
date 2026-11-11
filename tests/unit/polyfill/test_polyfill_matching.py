# Copyright (c) 2026 Kenneth Stott
# Canary: 9dd33761-c55a-40f5-b53e-235c29ec6507
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Applying a rule (REQ-2236): tree against tree, typed holes read as function_types reads a type,
and the matched subtrees grafted into the replacement. A node that does not match is untouched."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.polyfill.matching import fits, graft, match
from provisa.transpiler.polyfill.notation import RuleError, pattern, replacement


def _node(sql: str) -> exp.Expr:
    return sqlglot.parse_one(sql, read="postgres")


def _matched(rule: str, sql: str) -> dict[str, str] | None:
    bindings = match(pattern(rule), _node(sql))
    return None if bindings is None else {k: v.sql("postgres") for k, v in bindings.items()}


def test_a_call_matches_and_binds_its_arguments():
    assert _matched("trim_array($a::anyarray, $n::integer)", "trim_array(ARRAY[1, 2], 1)") == {
        "a": "ARRAY[1, 2]",
        "n": "1",
    }


def test_a_function_name_matches_in_any_case_and_quoted():
    assert _matched("array_ndims($a)", "ARRAY_NDIMS(x)") == {"a": "x"}
    assert _matched("array_ndims($a)", '"array_ndims"(x)') == {"a": "x"}


@pytest.mark.parametrize(
    "sql",
    [
        "array_dims(ARRAY[1])",  # another function
        "array_ndims(ARRAY[1], 2)",  # another arity
        "array_ndims()",
        "1 + array_ndims(ARRAY[1])",  # the call is below the node
    ],
)
def test_a_node_that_does_not_match_binds_nothing(sql: str):
    assert _matched("array_ndims($a::anyarray)", sql) is None


@pytest.mark.parametrize(
    ("argument", "fits_anyarray"),
    [
        ("ARRAY[1, 2]", True),  # its type is not said: it fits any
        ("x", True),  # a column of no said type
        ("NULL", True),
        ("NULL::integer[]", True),
        ("x::text[]", True),
        ("x::pg_catalog.int2vector", True),  # the catalog's vectors are arrays
        ("x::oidvector", True),
        ("5", False),
        ("'a'::text", False),
        ("true", False),
        ("x::integer", False),
    ],
)
def test_a_typed_hole_matches_what_fits_its_type(argument: str, fits_anyarray: bool):
    assert (_matched("array_ndims($a::anyarray)", f"array_ndims({argument})") is not None) is (
        fits_anyarray
    )


@pytest.mark.parametrize(
    ("argument", "parameter", "expected"),
    [
        ("1", "integer", True),
        ("-1", "integer", True),
        ("1", "bigint", True),  # an integer widens
        ("1", "double precision", True),
        ("1.5", "integer", False),  # a numeric does not narrow
        ("1.5", "double precision", True),
        ("x::bigint", "integer", False),
        ("x::smallint", "integer", True),
        ("'x'", "integer", True),  # an untyped literal fits any
        ("7", "anyelement", True),
        ("'x'::text", "anycompatible", True),
        ("true", "boolean", True),
        ("1", "boolean", False),
    ],
)
def test_fits_reads_types_as_postgresql_resolves_them(argument: str, parameter: str, expected):
    assert fits(_node(argument), parameter) is expected


def test_a_hole_written_twice_matches_the_same_subtree_twice():
    rule = "f($a, $a)"
    assert _matched(rule, "f(x + 1, x + 1)") == {"a": "x + 1"}
    assert _matched(rule, "f(x + 1, x + 2)") is None


def test_holes_match_nested_subtrees():
    rule = "array_upper($a::anyarray, $d::integer)"
    bound = _matched(rule, "array_upper(array_fill(1, ARRAY[2]), 1 + 0)")
    assert bound == {"a": "ARRAY_FILL(1, ARRAY[2])", "d": "1 + 0"}


def test_a_pattern_of_a_typed_node_matches_that_node():
    # sqlglot reads array_length as ArraySize: the pattern is the same node.
    assert _matched("array_length($a::anyarray, 1)", "array_length(x, 1)") == {"a": "x"}
    assert _matched("array_length($a::anyarray, 1)", "array_length(x, 2)") is None


def test_a_pattern_of_an_expression_matches_its_operands():
    assert _matched("$a || $b", "'x' || y") == {"a": "'x'", "b": "y"}
    assert _matched("$a || $b", "'x' + y") is None


def test_an_identifier_matches_folded_unless_quoted():
    assert _matched("f(t.$a, x)", "f(t.c, X)") is not None
    assert _matched('f($a, "X")', 'f(c, "x")') is None


def test_graft_puts_each_bound_subtree_in_the_replacement():
    rule = "trim_array($a::anyarray, $n::integer)"
    bindings = match(pattern(rule), _node("trim_array(ARRAY[1, 2, 3], 1)"))
    assert bindings is not None
    tree = graft(replacement("slice($a, 1, cardinality($a) - $n)", "trino", rule), bindings)
    assert tree.sql("trino") == "SLICE(ARRAY[1, 2, 3], 1, CARDINALITY(ARRAY[1, 2, 3]) - 1)"


def test_graft_copies_and_leaves_the_replacement_and_statement_untouched():
    rule = "f($a::anyarray)"
    form = replacement("len($a)", "duckdb", rule)
    statement = _node("f(ARRAY[1])")
    bindings = match(pattern(rule), statement)
    assert bindings is not None
    first = graft(form, bindings)
    second = graft(form, bindings)
    assert first == second and first is not second
    assert first.this is not bindings["a"]
    assert form.sql("duckdb") == "LENGTH($a)"
    assert statement.sql("postgres") == "F(ARRAY[1])"


def test_graft_does_not_rebind_a_hole_inside_what_it_put():
    # A statement's own parameter, inside a bound argument, is the statement's.
    rule = "f($a)"
    bindings = {"a": sqlglot.parse_one("g($a)", read="duckdb")}
    tree = graft(replacement("h($a)", "duckdb", rule), bindings)
    assert tree.sql("duckdb") == "H(G($a))"


def test_graft_of_an_unbound_hole_is_refused():
    with pytest.raises(RuleError, match=r"\$a"):
        graft(replacement("len($a)", "duckdb", "f($a)"), {})


@pytest.mark.parametrize(
    ("engine", "form", "expected"),
    [
        ("duckdb", "list_transform($a, _x -> _x + $n)", "LIST_TRANSFORM([1], LAMBDA _x : _x + 2)"),
        ("trino", "transform($a, _x -> _x + $n)", "TRANSFORM(ARRAY[1], _x -> _x + 2)"),
        ("clickhouse", "arrayMap(_x -> _x + $n, $a)", "arrayMap(_x -> _x + 2, [1])"),
    ],
)
def test_each_engines_replacement_is_written_in_its_dialect(engine: str, form: str, expected):
    rule = "f($a::anyarray, $n::integer)"
    bindings = match(pattern(rule), _node("f(ARRAY[1], 2)"))
    assert bindings is not None
    assert graft(replacement(form, engine, rule), bindings).sql(engine) == expected
