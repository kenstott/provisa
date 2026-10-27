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


def test_a_list_of_values_of_several_types_is_one_list_in_the_answer():
    from provisa.gql.assembler import assemble_rows

    query = "RETURN [2E-01, 'a', null, 71034856, false] AS literal"
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    sql = sqlglot.transpile(sql_ast.sql(dialect="postgres"), read="postgres", write="duckdb")[0]
    raw = duckdb.sql(sql).fetchall()
    rows = assemble_rows([{"literal": raw[0][0]}], graph_vars)
    assert rows == [{"literal": [0.2, "a", None, 71034856, False]}]


def test_a_literal_nested_deeper_than_the_sql_parser_reads_in_time_is_refused_by_name():
    from provisa.cypher.parser import MAX_LITERAL_DEPTH, CypherParseError

    assert _rows("RETURN " + "[" * MAX_LITERAL_DEPTH + "]" * MAX_LITERAL_DEPTH + " AS l")
    deep = MAX_LITERAL_DEPTH + 1
    with pytest.raises(CypherParseError, match=f"nests {deep} deep"):
        parse_cypher("RETURN " + "[" * deep + "]" * deep + " AS l")


def test_a_time_value_is_written_in_the_answer_as_iso_8601():
    import datetime

    from provisa.gql.assembler import to_serializable

    assert to_serializable({"t": datetime.time(10, 35, 1, 500)}) == {"t": "10:35:01.000500"}


def test_a_temporal_value_built_from_a_map_is_refused_by_name():
    from provisa.gql.translator_types import CypherTranslateError

    with pytest.raises(CypherTranslateError, match=r"localtime\(\) built from a map"):
        _rows("RETURN localtime({hour: 10, minute: 35}) AS t")


@pytest.mark.parametrize(
    "query",
    [
        "CREATE ()",
        "CREATE (:A)",
        "CREATE (n:A {name: 'x'}) RETURN n",
        "UNWIND [1] AS i CREATE (:A)",
    ],
)
def test_a_write_in_a_form_not_read_is_refused_naming_the_forms_that_are(query):
    from provisa.cypher.parser import CypherParseError

    with pytest.raises(CypherParseError) as refused:
        parse_cypher(query)
    message = str(refused.value)
    assert "is not supported" not in message
    assert "CREATE (n:Label {key: value})" in message


def test_an_unaliased_expression_is_named_as_the_statement_writes_it():
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher("UNWIND [1, 3, 2] AS x RETURN min(x), count(x) > 1"), _EMPTY, {}
    )
    assert [s.alias_or_name for s in sql_ast.expressions] == ["min(x)", "count(x) > 1"]
