# Copyright (c) 2026 Kenneth Stott
# Canary: d1d8f9f1-cd11-44cf-a943-f0db4bf4ea6d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's list quantifiers over a list holding null, as the openCypher TCK's Quantifier1-4 [10]
state them, answered on the DuckDB engine through the governed path: three-valued, so a null
predicate value decides the answer where no true or false one does (REQ-345, REQ-2167)."""

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

_LISTS = [
    ("[null]", "x = 2"),
    ("[null, null]", "x = 2"),
    ("[0, null]", "x = 2"),
    ("[2, null]", "x = 2"),
    ("[null, 2]", "x = 2"),
    ("[34, 0, null, 5, 900]", "x < 10"),
    ("[34, 10, null, 15, 900]", "x < 10"),
    ("[4, 0, null, -15, 9]", "x < 10"),
    ("[]", "x = 2"),
]

#: Each quantifier's answer over _LISTS, in order.
_ANSWERS = {
    "none": [None, None, None, False, False, False, None, False, True],
    "single": [None, None, None, None, None, False, None, False, False],
    "any": [None, None, None, True, True, True, None, True, False],
    "all": [None, None, False, None, None, False, False, None, True],
}


def _value(query: str):
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _EMPTY, {})
    ((value,),) = duckdb.sql(transpile_to_duckdb(sql_ast.sql(dialect="postgres"))).fetchall()
    return value


@pytest.mark.parametrize(
    ("quantifier", "items", "condition", "answer"),
    [
        (q, items, condition, answer)
        for q, answers in _ANSWERS.items()
        for (items, condition), answer in zip(_LISTS, answers, strict=True)
    ],
)
def test_a_quantifier_over_nulls_is_three_valued(quantifier, items, condition, answer):
    assert _value(f"RETURN {quantifier}(x IN {items} WHERE {condition}) AS result") is answer


@pytest.mark.parametrize(
    ("query", "answer"),
    [
        # Quantifier1-3 [6]: an item of a list of several types counts its own items.
        ("RETURN none(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3) AS result", False),
        ("RETURN single(x IN [['a'], [1, 2, 3]] WHERE size(x) = 3) AS result", True),
        ("RETURN any(x IN [['a'], [1, 2, 3], ['a']] WHERE size(x) = 3) AS result", True),
        ("RETURN all(x IN [[1, 2], ['abc', 'd']] WHERE size(x) = 2) AS result", True),
        ("RETURN [x IN [[1, 2, 3], 'ab'] | size(x)] AS sizes", [3, 2]),
    ],
)
def test_an_item_of_a_list_of_several_types_has_its_size(query, answer):
    assert _value(query) == answer
