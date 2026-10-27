# Copyright (c) 2026 Kenneth Stott
# Canary: 304bc835-30ba-404d-a88a-0cb9937687bb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's operator precedence, as the openCypher TCK's Precedence1-4 state it, answered on the
DuckDB engine through the governed path (cypher_to_sql, PostgreSQL SQL, transpile_to_duckdb):
XOR between AND and OR; IN, STARTS WITH, ENDS WITH, CONTAINS and IS [NOT] NULL tighter than a
comparison and looser than + and -; and an integer divided by an integer is an integer
(REQ-345, REQ-2167)."""

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
        # Precedence1 [1], [2]
        (
            "RETURN true OR true XOR true AS a, true OR (true XOR true) AS b,"
            " (true OR true) XOR true AS c",
            (True, True, False),
        ),
        (
            "RETURN true XOR false AND false AS a, true XOR (false AND false) AS b,"
            " (true XOR false) AND false AS c",
            (True, True, False),
        ),
        ("RETURN null XOR true AS a, false XOR false AS b", (None, False)),
        # Precedence1 [8]
        (
            "RETURN false = true IS NULL AS a, false = (true IS NULL) AS b,"
            " (false = true) IS NULL AS c",
            (True, True, False),
        ),
        # Precedence4 [1] #1
        (
            "RETURN null IS NOT NULL = null IS NULL AS a,"
            " (null IS NOT NULL) = (null IS NULL) AS b,"
            " (null IS NOT NULL = null) IS NULL AS c",
            (False, False, True),
        ),
        ("RETURN 1 + null IS NULL AS a", (True,)),
        # Precedence3 [5], [6], over lists of one type (mixed-type lists are their own matter)
        (
            "RETURN 1 + 2 IN [3] + [4] AS a, (1 + 2) IN ([3] + [4]) AS b",
            (True, True),
        ),
        (
            "RETURN false = 1 IN [2] AS a, false = (1 IN [2]) AS b, 3 IN [1, null] AS c",
            (True, True, None),
        ),
        # Precedence4 [4]
        (
            "RETURN (true OR null STARTS WITH 'abc') = (true OR (null STARTS WITH 'abc')) AS c",
            (True,),
        ),
        # Precedence2 [1], [2]
        (
            "RETURN 4 * 2 + 3 / 2 AS a, 4 * 2 + (3 / 2) AS b, 4 * (2 + 3) / 2 AS c",
            (9, 9, 10),
        ),
        ("RETURN 4 / 2 - 3 / 2 AS a, -7 / 2 AS b, 7.0 / 2 AS c", (1, -3, 3.5)),
        ("RETURN 4 ^ 3 / 2 ^ 3 AS a, 4 ^ (3 / 2) ^ 3 AS c", (8.0, 64.0)),
    ],
)
def test_cypher_operators_bind_as_the_tck_states(query, row):
    assert _row(query) == row
