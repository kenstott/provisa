# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8bd4aa-2aaa-447f-a427-aae411bbb4c2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The polyfill notation (REQ-2236, A POLYFILL IS A RULE THE TRANSLATOR RUNS): a rule's sides are
parsed by sqlglot, each in its own dialect, into trees whose holes are parameter nodes."""

# Requirements: REQ-2236

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import Dialect

from provisa.transpiler.polyfill import notation
from provisa.transpiler.polyfill.notation import (
    Refusal,
    Rewrite,
    RuleError,
    hole_name,
    holes,
    pattern,
    replacement,
    rule_dialect,
    typed_hole,
)

_ENGINES = ("duckdb", "trino", "clickhouse")


@pytest.mark.parametrize("dialect", ("postgres", *_ENGINES))
@pytest.mark.parametrize(
    "text",
    [
        "f($a)",
        "$a[1:len($a) - $n]",  # a hole before another with no space: no dollar-quoted string
        "$a[$n]",
        "g(_x -> _x + $n, $lower_bounds)",
        "CASE WHEN $a IS NULL THEN NULL ELSE $b END",
    ],
)
def test_a_hole_is_a_parameter_node_in_each_dialect(dialect: str, text: str):
    tree = sqlglot.parse_one(text, dialect=rule_dialect(dialect))
    found = [hole_name(node) for node in tree.dfs() if hole_name(node) is not None]
    assert found
    assert all(isinstance(node, (exp.Parameter, exp.Placeholder)) for node in tree.dfs()
               if hole_name(node) is not None)  # fmt: skip


@pytest.mark.parametrize("dialect", _ENGINES)
def test_a_dollar_inside_a_string_is_text(dialect: str):
    tree = replacement("json_extract($a, '$[0]') || '$'", dialect, "f($a::text)")
    assert holes(tree) == ("a",)
    strings = {n.this for n in tree.find_all(exp.Literal) if n.is_string}
    assert strings == {"$[0]", "$"}


def test_holes_are_listed_in_the_order_written_each_once():
    assert holes(pattern("array_fill($v::anyelement, $dims::integer[], $v)")) == ("v", "dims")


@pytest.mark.parametrize(
    ("text", "said"),
    [
        ("f($a::anyarray)", "anyarray"),
        ("f($a::integer)", "integer"),
        ("f($a::integer[])", "integer[]"),
        ("f($a::boolean)", "boolean"),
        ("f($a::anycompatible)", "anycompatible"),
        ("f($a::text)", "text"),
    ],
)
def test_a_typed_hole_has_postgresqls_name_of_its_type(text: str, said: str):
    cast = pattern(text).expressions[0]
    assert typed_hole(cast) == ("a", said)


def test_a_hole_with_no_type_is_untyped():
    tree = pattern("f($a, 1)")
    assert typed_hole(tree.expressions[0]) is None
    assert hole_name(tree.expressions[0]) == "a"


def test_a_positional_parameter_is_no_hole():
    tree = sqlglot.parse_one("f($1)", read="postgres")
    assert hole_name(tree.expressions[0]) is None


def test_a_replacement_naming_a_hole_its_pattern_lacks_is_refused():
    with pytest.raises(RuleError, match=r"\['b'\]"):
        replacement("len($b)", "duckdb", "f($a::anyarray)")


def test_a_side_that_does_not_parse_is_refused():
    with pytest.raises(RuleError, match="does not parse"):
        replacement("CASE WHEN $a THEN", "trino", "f($a::boolean)")


def test_a_pattern_is_postgresql_and_a_replacement_its_engines_sql():
    # ClickHouse's own spelling of a lambda's function: arrayMap takes the lambda first.
    tree = replacement("arrayMap(_x -> _x = $v, $a)", "clickhouse", "f($a::anyarray, $v::integer)")
    assert isinstance(tree, exp.Transform)
    assert isinstance(pattern("array_length($a::anyarray, 1)"), exp.ArraySize)


def test_the_rule_dialects_are_not_registered_for_statements():
    for dialect in ("postgres", *_ENGINES):
        rule_dialect(dialect)
    assert not any(name.startswith("_polyfill_rule_") for name in Dialect.classes)


def test_a_rewrite_is_printed_as_its_name_and_purpose():
    def lowered(node: exp.Expr, dialect: str) -> exp.Expr | None:
        """Lowers a node for the test.

        More text the docs do not print."""
        return None

    rewrite = Rewrite(lowered)
    assert rewrite.name.endswith("lowered")
    assert rewrite.name.startswith(__name__)
    assert rewrite.purpose == "Lowers a node for the test."


def test_a_refusal_holds_its_sqlstate_and_message():
    refusal = Refusal("0A000", "x is refused")
    assert (refusal.sqlstate, refusal.message) == ("0A000", "x is refused")


def test_patterns_and_replacements_are_parsed_once():
    assert notation.pattern("f($a::text)") is notation.pattern("f($a::text)")
    assert replacement("g($a)", "duckdb", "f($a::text)") is replacement(
        "g($a)", "duckdb", "f($a::text)"
    )
