# Copyright (c) 2026 Kenneth Stott
# Canary: 4d0cb655-c059-44a7-81f0-78cb884d8199
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""keys() of a map and a map's key read by name, as the openCypher TCK's Map2 and Map3 state
them, answered on the DuckDB engine through the governed path: keys() of a map literal, a
parameter, a variable bound to one or null is its keys in order (null for null), never a 500;
m['k'] is m.k's value, not its JSON (REQ-345, REQ-2167)."""

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


def _rows(query: str, params: dict | None = None) -> list[tuple]:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, params or {})
    return duckdb.sql(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()


@pytest.mark.parametrize(
    ("query", "rows"),
    [
        ("RETURN keys({k: 1, l: null}) AS keys", [(["k", "l"],)]),
        ("RETURN keys({}) AS keys", [([],)]),
        (
            "RETURN keys({name: 'Alice', age: 38, address: {city: 'London'}}) AS k",
            [(["name", "age", "address"],)],
        ),
        ("WITH null AS m RETURN keys(m), keys(null)", [(None, None)]),
        (
            "WITH {exists: 42, notMissing: null} AS map"
            " RETURN 'exists' IN keys(map) AS a, 'notMissing' IN keys(map) AS b,"
            " 'missing' IN keys(map) AS c",
            [(True, True, False)],
        ),
        ("WITH {name: 'Mats', nome: 'Pontus'} AS map RETURN map['name'] AS result", [("Mats",)]),
        ("WITH {null: 'Mats', NULL: 'Pontus'} AS map RETURN map['null'] AS result", [("Mats",)]),
        ("RETURN {a: 1}['a'] AS a, {a: 1}['b'] AS b", [(1, None)]),
        # Graph9 [4]: properties() of a map is the map.
        ("RETURN keys(properties({name: 'Popeye', level: 9001})) AS k", [(["name", "level"],)]),
        ("RETURN properties(null) AS p", [(None,)]),
        # Map2 [3], [4]: a map or key bound to null indexes to null.
        ("WITH null AS expr, 'x' AS idx RETURN expr[idx] AS value", [(None,)]),
        ("WITH {name: 'Mats'} AS expr, null AS idx RETURN expr[idx] AS value", [(None,)]),
        # Quantifier9-12 [5]: a key every unwound map gives a value of one type reads as it.
        (
            "UNWIND [{list: [2], fixed: true}, {list: [1, 2, 3], fixed: false}] AS input"
            " RETURN input.list AS l, input.fixed AS f",
            [([2], True), ([1, 2, 3], False)],
        ),
        (
            "UNWIND [{list: [2], fixed: true}] AS input"
            " RETURN CASE WHEN input.fixed THEN input.list ELSE [1] END AS l",
            [([2],)],
        ),
        # Map1: keys differing in case are two keys, carried through a WITH.
        (
            "WITH {name: 'Mats', Name: 'Pontus'} AS map RETURN map.Name AS a, map.name AS b",
            [("Pontus", "Mats")],
        ),
        ("WITH {null: 'Mats', NULL: 'Pontus'} AS map RETURN map.`NULL` AS r", [("Pontus",)]),
        (
            "WITH {name: 'Mats', Name: 'Pontus'} AS map WITH map AS m RETURN m.Name AS a",
            [("Pontus",)],
        ),
    ],
)
def test_a_map_answers_its_keys_and_its_values(query, rows):
    assert _rows(query) == rows


def _bound(query: str, params: dict) -> tuple[str, list[tuple]]:
    """The SQL and its rows with the client's values bound, as the executor binds them."""
    sql_ast, order, _ = cypher_to_sql(parse_cypher(query), _EMPTY, params)
    sql = transpile_to_duckdb(sql_ast.sql(dialect="postgres"))
    return sql, duckdb.execute(sql, [params[n] for n in order]).fetchall()


def test_keys_of_a_parameter_are_the_map_the_client_gives():
    param = {"name": "Alice", "age": 38, "address": {"city": "London", "residential": True}}
    sql, rows = _bound("RETURN keys($param) AS k", {"param": param})
    assert rows == [(["name", "age", "address"],)]
    # Read by the engine from the value, never written into the SQL: a translation is kept for
    # every request giving a map (cypher_plan), whatever its keys.
    other, rows = _bound("RETURN keys($param) AS k", {"param": {"z": 1}})
    assert rows == [(["z"],)] and other == sql


@pytest.mark.parametrize(
    ("query", "params", "value"),
    [
        # Map2 [1], [2]: a map the client gives read by a key the client gives (as its JSON).
        (
            "WITH $expr AS expr, $idx AS idx RETURN expr[idx] AS value",
            {"expr": {"name": "Apa"}, "idx": "name"},
            '"Apa"',
        ),
        (
            "WITH $expr AS expr, $idx AS idx RETURN expr[toString(idx)] AS value",
            {"expr": {"name": "Apa"}, "idx": "name"},
            '"Apa"',
        ),
        # List1 [3], [5]: a list the client gives is 0-indexed.
        (
            "WITH $expr AS expr, $idx AS idx RETURN expr[idx] AS value",
            {"expr": ["Apa"], "idx": 0},
            "Apa",
        ),
        (
            "WITH $expr AS expr, $idx AS idx RETURN expr[toInteger(idx)] AS value",
            {"expr": ["Apa"], "idx": 0},
            "Apa",
        ),
    ],
)
def test_a_value_the_client_gives_is_read_by_a_key_or_index_it_gives(query, params, value):
    assert _bound(query, params)[1] == [(value,)]


@pytest.mark.parametrize(
    ("elt", "coll", "result"),
    [
        (None, None, None),
        (None, [1, 2, 3], None),
        (None, [1, 2, 3, None], None),
        (None, [], False),
        (1, [1, 2, 3, None], True),
        (1, [None, 1], True),
        (5, [1, 2, 3, None], None),
    ],
)
def test_a_value_the_client_gives_in_a_list_it_gives_is_three_valued(elt, coll, result):
    # Null3 [4].
    assert _bound("RETURN $elt IN $coll AS result", {"elt": elt, "coll": coll})[1] == [(result,)]


def test_a_map_read_from_a_list_of_several_types_answers_its_keys():
    # Map1 [3]: (list[1]).key of a map inside a list of several types, null where it has none.
    from provisa.gql.assembler import assemble_rows
    from provisa.gql.graph_rewriter import apply_graph_rewrites

    query = (
        "WITH [123, {existing: 42, notMissing: null}] AS list"
        " RETURN (list[1]).missing AS m, (list[1]).notMissing AS n, (list[1]).existing AS e"
    )
    sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    sql = apply_graph_rewrites(sql_ast, graph_vars, _EMPTY)
    cur = duckdb.sql(transpile_to_duckdb(sql.sql(dialect="postgres")))
    names = [d[0] for d in cur.description]
    rows = assemble_rows([dict(zip(names, r)) for r in cur.fetchall()], graph_vars)
    assert rows == [{"m": None, "n": None, "e": 42}]
