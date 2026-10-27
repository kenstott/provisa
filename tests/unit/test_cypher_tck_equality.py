# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Equality, IN and order of values the statement writes wholly, as the openCypher TCK's List3,
List5, Comparison1, Comparison2 and Precedence3 state them, answered on the DuckDB engine through
the governed path: across kinds (1 and [1] are never equal) and three-valued with a null inside a
list or map -- which an engine's typed comparison cannot answer (REQ-345, REQ-2167)."""

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


def _value(expression: str):
    sql_ast, _, _ = cypher_to_sql(parse_cypher(f"RETURN {expression} AS v"), _EMPTY, {})
    ((value,),) = duckdb.sql(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()
    return value


@pytest.mark.parametrize(
    ("expression", "value"),
    [
        # List5: membership across kinds and with nulls.
        ("1 IN ['1', 2]", False),
        ("[1, 2] IN [1, [1, '2']]", False),
        ("[1] IN [1, 2, [1]]", True),
        ("[1, 2] IN [1, [2, 1]]", False),
        ("[[1, 2], 3] IN [1, [[1, 2], 3]]", True),
        ("[null] IN [[null]]", None),
        ("[1, 2] IN [1, [1, 2], null]", True),
        ("[1, 2] IN [[null, 2]]", None),
        ("[1, 2] IN [[null, 2], [1, 3]]", None),
        ("[] IN []", False),
        ("[] IN [1, 2, null]", None),
        ("[[], []] IN [1, [[], []]]", True),
        # Comparison1 and List3: lists and maps with nulls, across kinds.
        ("[null] = [1]", None),
        ("[[1], [2]] = [[1], [null]]", None),
        ("[1, 2] = 'foo'", False),
        ("{k: null} = {k: null}", None),
        ("{k: 1, l: null} = {k: 1, l: 1}", None),
        ("{k: 1} = {l: 1}", False),
        ("[1, 2] <> [1, 2, 3]", True),
        # Comparison2 and Precedence3: an order across kinds is null.
        ("[1, 2] >= [1, null]", None),
        ("[1, 'a'] >= [1, null]", None),
        ("[1, 2] < [3, 4] IN [[3, 4], false]", None),
        ("[1, 2] = [3, 4] IN [[3, 4], false]", False),
        ("[1] + 2 IN [3] + 4", False),
        ("[1, 2] < [1, 3]", True),
    ],
)
def test_values_written_wholly_compare_as_cypher_says(expression, value):
    assert _value(expression) is value
