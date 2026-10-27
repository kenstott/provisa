# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's list + element, as the openCypher TCK writes it (Precedence3, Quantifier9-12),
answered on the DuckDB engine through the governed path: a list + an element appends it, an
element + a list prepends it, and a list + a list concatenates them -- whether the list is
written there or carried by a WITH or an UNWIND (REQ-345, REQ-2167)."""

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


def _rows(query: str) -> list[tuple]:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    return duckdb.sql(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()


@pytest.mark.parametrize(
    ("query", "rows"),
    [
        (
            "RETURN [1, 2] + 3 AS a, 0 + [1, 2] AS b, [1] + [2, 3] AS c",
            [([1, 2, 3], [0, 1, 2], [1, 2, 3])],
        ),
        ("RETURN [1] + 2 + 3 AS a, ([1] + 2) + [3] AS b", [([1, 2, 3], [1, 2, 3])]),
        ("WITH [1, 2] AS xs RETURN xs + 3 AS a, 0 + xs AS b", [([1, 2, 3], [0, 1, 2])]),
        ("RETURN [x IN [1, 2] | x * 10] + 3 AS a", [([10, 20, 3],)]),
        ("RETURN range(1, 2) + 3 AS a, [1, 2, 3][1..] + 4 AS b", [([1, 2, 3], [2, 3, 4])]),
        (
            "WITH [1, 2] AS xs WITH CASE WHEN true THEN xs ELSE [9] END + 3 AS ys RETURN ys",
            [([1, 2, 3],)],
        ),
        (
            "UNWIND [[1], [2]] AS xs RETURN xs + 0 AS ys ORDER BY ys",
            [([1, 0],), ([2, 0],)],
        ),
        # Quantifier11 [4]: any is the negation of none, over a list a WITH builds by appending.
        (
            "WITH [1, 2, 3] AS inputList UNWIND inputList AS x"
            " WITH inputList, x, [y IN inputList WHERE y > 1 | y] AS list"
            " WITH list + x AS list"
            " WITH any(x IN list WHERE x = 2) = (NOT none(x IN list WHERE x = 2)) AS result,"
            " count(*) AS cnt RETURN result",
            [(True,)],
        ),
        ("RETURN 1 + 2 AS a", [(3,)]),
    ],
)
def test_a_list_plus_an_element_appends_it(query, rows):
    assert _rows(query) == rows
