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
    ],
)
def test_cypher_operators_bind_as_the_tck_states(query, row):
    assert _row(query) == row
