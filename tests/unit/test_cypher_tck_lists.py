# Copyright (c) 2026 Kenneth Stott
# Canary: 074b29e7-160a-4b1f-9748-514f1fd6b526
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's list + element and reverse() of a list, as the openCypher TCK writes them
(Precedence3, Quantifier9-12), answered on the DuckDB engine through the governed path: a list +
an element appends it, an element + a list prepends it, a list + a list concatenates them, and
reverse() of a list reads it last to first -- whether the list is written there or carried by a
WITH or an UNWIND (REQ-345, REQ-2167)."""

# Requirements: REQ-345, REQ-2167

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
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
        # Aggregation8 [3], [4]: collect() skips nulls, and collects nothing as [].
        ("UNWIND [null, 1, null] AS x RETURN collect(DISTINCT x) AS c", [([1],)]),
        ("UNWIND [null, 1, null] AS x RETURN collect(x) AS c", [([1],)]),
        ("UNWIND [null, null] AS x RETURN collect(x) AS c", [([],)]),
        # List11 [3]: a function inside collect() is lowered too.
        (
            "WITH [1, 2] AS list, true AS e"
            " RETURN collect(CASE WHEN e THEN size(list) = 0 ELSE size(list) > 0 END) AS c",
            [([False],)],
        ),
        # reverse() of a list is its items last to first; of a string, its characters.
        (
            "RETURN reverse([1, 2, 3]) AS a, reverse('abc') AS b, reverse([]) AS c",
            [([3, 2, 1], "cba", [])],
        ),
        ("WITH [1, 2] AS xs RETURN reverse(xs) + 0 AS a", [([2, 1, 0],)]),
        (
            "WITH [1, 2] AS xs RETURN CASE WHEN true THEN reverse(xs) ELSE xs END + 3 AS a",
            [([2, 1, 3],)],
        ),
    ],
)
def test_a_list_plus_an_element_appends_it(query, rows):
    assert _rows(query) == rows


def _answer(query: str) -> list[dict]:
    """The rows as the endpoint answers them: graph rewrites, then each value decoded."""
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    sql = apply_graph_rewrites(sql_ast, graph_vars, _EMPTY)
    cur = duckdb.sql(transpile_to_duckdb(sql.sql(dialect="postgres")))
    names = [d[0] for d in cur.description]
    return assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)


@pytest.mark.parametrize(
    ("query", "row"),
    [
        # Precedence3 [1]-[5]: a list of several types built from lists answers its values.
        (
            "RETURN [[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10][3] AS a,"
            " ([[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10])[3] AS c",
            {"a": [[1], [2, 3], [4, 5], 10], "c": 5},
        ),
        (
            "RETURN ([[1], [2, 3], [4, 5]] + [5, [6, 7], [8, 9], 10])[2] AS c",
            {"c": [4, 5]},
        ),
        ("RETURN [1] + (2 IN [3]) + 4 AS c", {"c": [1, False, 4]}),
        ("RETURN [[1, 2, 3], ['a']] AS l", {"l": [[1, 2, 3], ["a"]]}),
    ],
)
def test_a_list_of_several_types_answers_its_values(query, row):
    assert _answer(query) == [row]


def test_unwind_null_is_no_rows():
    assert _rows("UNWIND null AS nil RETURN nil") == []


def test_return_star_returns_its_columns_by_name():
    # Unwind1 [13]: openCypher orders the columns RETURN * returns by name.
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher("WITH [1, 2] AS xs, [3, 4] AS ys UNWIND xs AS x UNWIND ys AS y RETURN *"),
        _EMPTY,
        {},
    )
    assert [s.alias_or_name for s in sql_ast.selects] == ["x", "xs", "y", "ys"]


@pytest.mark.parametrize(
    ("query", "rows"),
    [
        # An item of a list of several types answers its value, in openCypher's order.
        ("UNWIND [1, 'a', [1, 2]] AS x RETURN x", [{"x": 1}, {"x": "a"}, {"x": [1, 2]}]),
        (
            "UNWIND [1, 2.0, 5, null, 3.2, 0.1] AS x RETURN max(x) AS hi, min(x) AS lo",
            [{"hi": 5, "lo": 0.1}],
        ),
        ("UNWIND [9, 10.5, 'a'] AS x RETURN max(x) AS hi", [{"hi": 10.5}]),
        (
            "UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x RETURN max(x) AS hi, min(x) AS lo",
            [{"hi": 1, "lo": [1, 2]}],
        ),
        (
            "UNWIND [[], ['a'], ['a', 1], [1], [1, 'a'], [1, null], [null, 1], [null, 2]] AS lists"
            " RETURN lists ORDER BY lists",
            [
                {"lists": v}
                for v in ([], ["a"], ["a", 1], [1], [1, "a"], [1, None], [None, 1], [None, 2])
            ],
        ),
        ("WITH [1, 'a'] AS l UNWIND l AS x WITH x RETURN x", [{"x": 1}, {"x": "a"}]),
        # TypeConversion4 [5]: toString() of an item is its text, a string's unquoted.
        (
            "RETURN [x IN [1, 2.3, true, 'apa'] | toString(x)] AS list",
            [{"list": ["1", "2.3", "true", "apa"]}],
        ),
        ("UNWIND [1, 'a'] AS x RETURN toString(x) AS s", [{"s": "1"}, {"s": "a"}]),
        # With2 [2]: a key of a map literal inside a map a WITH carries.
        (
            "WITH {name: {name2: 'baz'}} AS nestedMap RETURN nestedMap.name.name2 AS n",
            [{"n": "baz"}],
        ),
    ],
)
def test_an_item_of_a_list_of_several_types_answers_its_value(query, rows):
    assert _answer(query) == rows


@pytest.mark.parametrize("args", ["0, 0, 0", "2, 8, 0", "-2, 8, 0", "2, -8, 0"])
def test_a_range_never_steps_by_zero(args):
    # List11 [4]: openCypher's ArgumentError, NumberOutOfRange.
    from provisa.gql.translator_types import CypherTranslateError

    with pytest.raises(CypherTranslateError, match="step other than 0"):
        _rows(f"RETURN range({args}) AS r")


def test_a_range_s_computed_step_is_refused_at_zero_by_the_engine():
    # The step a query computes: refused by the engine (range_step), never DuckDB's [].
    with pytest.raises(duckdb.InvalidInputException, match="step other than 0"):
        _rows("WITH 0 AS s RETURN range(2, 8, s) AS r")
    assert _rows("WITH 2 AS s RETURN range(2, 8, s) AS r") == [([2, 4, 6, 8],)]


@pytest.mark.parametrize(
    ("query", "row"),
    [
        # Path1 [1], Path2 [3]: an OPTIONAL MATCH from a variable bound to null matches nothing.
        (
            "WITH null AS a OPTIONAL MATCH p = (a)-[r]->() RETURN nodes(p) AS n, nodes(null) AS m",
            {"n": None, "m": None},
        ),
        (
            "WITH null AS a OPTIONAL MATCH p = (a)-[r]->()"
            " RETURN relationships(p) AS n, relationships(null) AS m, r",
            {"n": None, "m": None, "r": None},
        ),
    ],
)
def test_an_optional_match_from_null_matches_nothing(query, row):
    assert _answer(query) == [row]


def test_a_value_bound_where_a_pattern_reads_a_node_is_still_refused():
    from provisa.gql.translator_types import CypherMistake

    with pytest.raises(CypherMistake, match="value that is neither"):
        parse_cypher("WITH 1 AS a OPTIONAL MATCH (a)-[r]->() RETURN r")


@pytest.mark.parametrize(
    ("query", "row"),
    [
        # List2 [6]: a negative bound counts from the end; each is held to the list.
        (
            "WITH [1, 2, 3] AS list RETURN list[-3..-1] AS a, list[-2..] AS b, list[..-1] AS c,"
            " list[1..-5] AS d, list[-9..2] AS e",
            ([1, 2], [2, 3], [1, 2], [], [1, 2]),
        ),
        ("WITH [1, 2, 3] AS l, 1 AS i RETURN l[-i..] AS a, l[null..2] AS b", ([3], None)),
    ],
)
def test_a_slice_counts_a_negative_bound_from_the_end(query, row):
    assert _rows(query) == [row]
