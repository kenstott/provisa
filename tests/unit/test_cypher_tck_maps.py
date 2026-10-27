# Copyright (c) 2026 Kenneth Stott
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
from provisa.gql.translator_types import CypherTranslateError
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
    ],
)
def test_a_map_answers_its_keys_and_its_values(query, rows):
    assert _rows(query) == rows


def test_keys_of_a_parameter_are_the_map_the_client_gives():
    param = {"name": "Alice", "age": 38, "address": {"city": "London", "residential": True}}
    assert _rows("RETURN keys($param) AS k", {"param": param}) == [(["name", "age", "address"],)]


def test_keys_of_a_parameter_not_given_is_refused_by_name():
    with pytest.raises(CypherTranslateError, match=r"\$param was not supplied"):
        _rows("RETURN keys($param) AS k")
