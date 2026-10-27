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
