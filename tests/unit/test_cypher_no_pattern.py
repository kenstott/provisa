# Copyright (c) 2026 Kenneth Stott
# Canary: 5b2e8c47-1d93-4a6f-b0e5-7c3a9d2f6e18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher statement with no pattern -- RETURN 1 + 1, WITH 1 AS x RETURN x, UNWIND [1, 2] AS x
WITH x RETURN x -- is a SELECT with no FROM: it reads no table, so it needs no data source
(REQ-345; openCypher TCK)."""

# Requirements: REQ-345

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql

pytestmark = pytest.mark.unit

_EMPTY = CypherLabelMap(nodes={}, relationships={})


def _rows(query: str, params: dict | None = None) -> list[tuple]:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, params or {})
    sql = sqlglot.transpile(sql_ast.sql(dialect="postgres"), read="postgres", write="duckdb")[0]
    return duckdb.sql(sql).fetchall()


@pytest.mark.parametrize(
    ("query", "rows"),
    [
        ("RETURN 1 + 1 AS n", [(2,)]),
        ("RETURN 'a' AS s, true AS b", [("a", True)]),
        ("WITH 1 AS x RETURN x + 1 AS y", [(2,)]),
        ("WITH 1 AS x WITH x * 3 AS y RETURN y", [(3,)]),
        ("WITH 2 AS x WHERE x > 1 RETURN x", [(2,)]),
        ("WITH 2 AS x WHERE x > 5 RETURN x", []),
        ("UNWIND [1, 2] AS x WITH x RETURN x * 10 AS y ORDER BY y", [(10,), (20,)]),
        ("WITH [1, 2, 3] AS xs UNWIND xs AS x RETURN x ORDER BY x", [(1,), (2,), (3,)]),
    ],
)
def test_a_statement_with_no_pattern_reads_no_table(query, rows):
    assert _rows(query) == rows


@pytest.mark.parametrize(
    ("query", "rows"),
    [
        (
            "RETURN 0x1F AS a, 0o17 AS b, .5 AS c, 1e3 AS d, 2.5E-1 AS e",
            [(31, 15, 0.5, 1000.0, 0.25)],
        ),
        ("RETURN 2 ^ 3 AS a", [(8.0,)]),
        ("RETURN [[[[[[[]]]]]]] AS literal", [([[[[[[[]]]]]]],)]),
        ("WITH [1, 2] AS list UNWIND list AS x RETURN *", [([1, 2], 1), ([1, 2], 2)]),
        ("UNWIND [0, 2, 1, 2, 0, 1] AS x WITH DISTINCT x ORDER BY x LIMIT 1 RETURN x", [(0,)]),
    ],
)
def test_literals_operators_and_clauses_the_tck_reads(query, rows):
    assert _rows(query) == rows


def test_a_character_no_token_reads_is_refused_not_skipped():
    from provisa.cypher.parser import CypherParseError

    with pytest.raises(CypherParseError, match="Unexpected character '!'"):
        parse_cypher("RETURN 1 ! 2 AS a")


def test_an_expression_that_does_not_parse_is_a_parse_refusal():
    from provisa.cypher.parser import CypherParseError

    with pytest.raises(CypherParseError, match="cannot parse Cypher expression"):
        parse_cypher("RETURN 1 + AS a")
