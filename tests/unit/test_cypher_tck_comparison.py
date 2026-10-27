# Copyright (c) 2026 Kenneth Stott
# Canary: 53389670-07b9-415a-befa-3da9f3ceb30c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A comparison of two values of different kinds, as the openCypher TCK's Comparison1 and 2
state it, answered on the DuckDB engine through the governed path: never equal, unequal, and
with no order (null) -- not the engine's implicit cast of one to the other. And a string + a value
is the two as one string, and toInteger() cuts toward zero (REQ-345, REQ-2167)."""

# Requirements: REQ-345, REQ-2167

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb

pytestmark = pytest.mark.unit

_EMPTY = CypherLabelMap(nodes={}, relationships={})


def _row(query: str) -> tuple:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    (row,) = duckdb.sql(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()
    return row


@pytest.mark.parametrize(
    ("query", "row"),
    [
        ("RETURN '1' = 1 AS a, '1.0' = 1.0 AS b, '1' <> 1 AS c", (False, False, True)),
        ("RETURN '1' < 1 AS a, '1.0' < 1.0 AS b, true >= 1 AS c", (None, None, None)),
        (
            "RETURN 0.0 / 0.0 = 'a' AS a, 0.0 / 0.0 <> 'a' AS b, 0.0 / 0.0 > 'a' AS c",
            (False, True, None),
        ),
        ("RETURN true = 1 AS a, 'true' = true AS b", (False, False)),
        # Values of one kind still compare: an integer and a float are numbers.
        ("RETURN 1 = 1.0 AS a, 1 < 2.5 AS b, 'a' < 'b' AS c", (True, True, True)),
    ],
)
def test_values_of_two_kinds_never_compare(query, row):
    assert _row(query) == row


@pytest.mark.parametrize(
    ("query", "row"),
    [
        ("RETURN 'a' + 'b' AS a, 'a' + null AS b", ("ab", None)),
        ("RETURN 'a' + 1 AS a, 1 + 'a' AS b, 'x' + 1.5 AS c", ("a1", "1a", "x1.5")),
        ("WITH 'a' AS s RETURN s + 'b' + s AS a", ("aba",)),
    ],
)
def test_a_string_plus_a_value_is_one_string(query, row):
    assert _row(query) == row


@pytest.mark.parametrize(
    ("query", "row"),
    [
        # TypeConversion2: toward zero, as Cypher truncates (an engine's cast rounds).
        ("WITH 82.9 AS weight RETURN toInteger(weight) AS a", (82,)),
        (
            "RETURN toInteger(-2.9) AS a, toInteger('2.9') AS b, toInteger('foo') AS c",
            (-2, 2, None),
        ),
        ("WITH [2, 2.9] AS numbers RETURN [n IN numbers | toInteger(n)] AS a", ([2, 2],)),
        ("RETURN toInteger(true) AS a, toIntegerOrNull('x') AS b", (1, None)),
    ],
)
def test_to_integer_cuts_toward_zero(query, row):
    assert _row(query) == row


@pytest.mark.parametrize(
    ("query", "row"),
    [
        # Conditional2: a WHEN of another kind than the subject never matches.
        ("RETURN CASE '0' WHEN 0 THEN 'zero' ELSE 'else' END AS a", ("else",)),
        ("RETURN CASE true WHEN 1 THEN 'one' WHEN true THEN 'yes' END AS a", ("yes",)),
        ("RETURN CASE 1 WHEN 1 THEN 'one' END AS a", ("one",)),
        # Precedence4 [4]: a string predicate of a value that is no string is null.
        ("RETURN 'abc' STARTS WITH (null OR true) AS a, 1 CONTAINS 'a' AS b", (None, None)),
        # Map2: null[k] and m[null] are null.
        ("RETURN null['x'] AS a, {name: 'Mats'}[null] AS b", (None, None)),
    ],
)
def test_values_of_another_kind_match_nothing(query, row):
    assert _row(query) == row


@pytest.mark.parametrize(
    ("query", "row"),
    [
        # Comparison1 [8], Comparison2 [5]: NaN is equal to nothing, itself included, and has no
        # order (DuckDB reads it as equal to itself and the largest value).
        ("RETURN 0.0 / 0.0 = 0.0 / 0.0 AS a, 0.0 / 0.0 <> 0.0 / 0.0 AS b", (False, True)),
        (
            "RETURN 0.0 / 0.0 > 1 AS a, 0.0 / 0.0 >= 1 AS b, 0.0 / 0.0 < 1 AS c, 0.0 / 0.0 <= 1 AS d",
            (False, False, False, False),
        ),
        ("RETURN 0.0 / 0.0 >= 0.0 / 0.0 AS a, 1.0 / 2.0 < 1 AS b", (False, True)),
        ("RETURN null > 0.0 / 0.0 AS a", (None,)),
    ],
)
def test_nan_is_equal_to_nothing_and_has_no_order(query, row):
    assert _row(query) == row


def test_a_comparison_of_floats_no_division_makes_is_not_guarded():
    # A column's own float is compared as its source holds it: the comparison may still go there.
    sql_ast, _, _ = cypher_to_sql(parse_cypher("RETURN 1.5 > 1.0 AS a"), _EMPTY, {})
    assert "IS_NAN" not in sql_ast.sql(dialect="postgres").upper()
