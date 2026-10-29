# Copyright (c) 2026 Kenneth Stott
# Canary: 766fb9d4-b0bb-40d3-a2e3-34e294a2f14e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A WITH's WHERE reads the variables in scope before the WITH as well as the names it projects:
it filters the rows the WITH reads (openCypher TCK WithWhere7, WithWhere1 [2], Comparison2 [3]).
A WHERE after an aggregating WITH filters the aggregated rows, and so reads only what the WITH
carries (WithWhere6). Answered on DuckDB through the governed path (REQ-347)."""

# Requirements: REQ-347

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit import permits_on_duckdb as permits

pytestmark = pytest.mark.unit

_EMPTY = CypherLabelMap(nodes={}, relationships={})


def _rows(query: str) -> list[tuple]:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    return duckdb.sql(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()


@pytest.mark.parametrize(
    ("query", "rows"),
    [
        # Comparison2 [3]'s shape: the WHERE reads i and j, which the WITH does not carry.
        (
            "UNWIND [1, 2] AS i UNWIND [1, 2] AS j WITH i * 10 + j AS a WHERE i <> j RETURN a",
            [(12,), (21,)],
        ),
        # WithWhere7 [3]: a name the WITH gives and a variable it drops, in one WHERE.
        (
            "UNWIND [1, 2, 3] AS i WITH i * 10 AS a WHERE a = 10 OR i = 3 RETURN a",
            [(10,), (30,)],
        ),
        # A WITH's name that shadows a variable before it is the WITH's value in its WHERE.
        (
            "UNWIND [1, 2, 3] AS i UNWIND [5] AS k WITH i * 10 AS k WHERE k > 15 AND i < 3 "
            "RETURN k",
            [(20,)],
        ),
        # WithWhere1 [2]: the WHERE filters the rows WITH DISTINCT reads, before they are made
        # distinct.
        ("UNWIND [1, 1, 2] AS i WITH DISTINCT i * 0 AS z WHERE i = 2 RETURN z", [(0,)]),
        # The WITH's ORDER BY orders the rows its WHERE keeps.
        (
            "UNWIND [3, 1, 2] AS i WITH i * 10 AS a ORDER BY a DESC WHERE i <> 2 RETURN a",
            [(30,), (10,)],
        ),
    ],
)
def test_a_with_where_reads_the_variables_before_the_with(query, rows):
    assert sorted(_rows(query)) == sorted(rows)


def test_comparison2_3_compares_the_pairs_of_other_positions():
    # Comparison2 [3] of a list of numbers and a string: only 1 < 3.14 is true.
    query = (
        "WITH [1, 3.14, 'x'] AS types "
        "UNWIND range(0, size(types) - 1) AS i "
        "UNWIND range(0, size(types) - 1) AS j "
        "WITH types[i] AS lhs, types[j] AS rhs "
        "WHERE i <> j "
        "WITH lhs, rhs, lhs < rhs AS result "
        "WHERE result "
        "RETURN lhs, rhs"
    )
    assert [tuple(str(v) for v in row) for row in _rows(query)] == [("1", "3.14")]


@pytest.mark.parametrize(
    ("query", "row"),
    [
        # Items of a list of several types are ordered by their values where of one kind, and
        # have no order (null) across kinds or where either is null -- never by their JSON text.
        (
            "WITH [1, 3.14, 10, 'a', 'b', true, false, null] AS t "
            "RETURN t[0] < t[1] AS a, t[1] < t[2] AS b, t[3] < t[4] AS c, t[6] < t[5] AS d, "
            "t[0] < t[3] AS e, t[0] < t[7] AS f, t[2] >= t[1] AS g",
            (True, True, True, True, None, None, True),
        ),
        # An item beside a value of a known kind.
        (
            "WITH [1, 3.14, 'a'] AS t RETURN t[1] < 10 AS a, t[2] < 1 AS b, 'b' > t[2] AS c",
            (True, None, True),
        ),
    ],
)
def test_items_of_a_list_of_several_types_order_by_their_values(query, row):
    assert _rows(query) == [row]


@pytest.mark.parametrize(
    ("query", "rows"),
    [
        # WithWhere6 [1]: the WHERE after an aggregating WITH filters the groups.
        ("UNWIND [1, 2, 2] AS i WITH i, count(*) AS c WHERE c > 1 RETURN i, c", [(2, 2)]),
        # A WHERE of only the WITH's names filters what the WITH carries, after its LIMIT.
        ("UNWIND [1, 2, 3] AS i WITH i AS a ORDER BY a LIMIT 2 WHERE a > 1 RETURN a", [(2,)]),
    ],
)
def test_a_where_of_the_with_names_filters_what_it_carries(query, rows):
    assert sorted(_rows(query)) == sorted(rows)


@pytest.fixture(scope="module")
def model():
    return permits.model()


_PERMITS = [
    {"status": "open", "valuation": 100.0},
    {"status": "closed", "valuation": 200.0},
    {"status": "open", "valuation": 300.0},
]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # WithWhere7 [1]: a node the WITH does not carry, read by its WHERE.
        (
            "MATCH (p:Permits) WITH p.status AS s WHERE p.valuation > 150 RETURN s",
            ["closed", "open"],
        ),
        # WithWhere7 [2] and [3].
        ("MATCH (p:Permits) WITH p.status AS s WHERE s = 'closed' RETURN s", ["closed"]),
        (
            "MATCH (p:Permits) WITH p.valuation AS v WHERE v = 100 OR p.status = 'closed' RETURN v",
            [100.0, 200.0],
        ),
    ],
)
def test_a_with_where_reads_a_node_the_with_drops(model, query, expected):
    assert sorted(permits.cypher_answer(query, model, _PERMITS)) == expected


def test_a_with_where_after_its_limit_reading_a_dropped_variable_is_refused():
    # Its WHERE keeps rows after the LIMIT picks them, but reads i, which only the rows before
    # the WITH hold: refused by name, never filtered before the LIMIT.
    from provisa.gql.translator_types import CypherTranslateError

    with pytest.raises(CypherTranslateError, match=r"SKIP or LIMIT .* \(i\)"):
        _rows("UNWIND [1, 2, 3] AS i WITH i * 10 AS a LIMIT 2 WHERE i > 1 RETURN a")
