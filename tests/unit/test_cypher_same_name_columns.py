# Copyright (c) 2026 Kenneth Stott
# Canary: 497843c1-88ff-4c84-afa5-a6e403c9d968
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Cypher RETURN of two variables' same-named properties is two columns (#259).

``RETURN n.name, c.name`` named both columns ``name``, and a row keeps one value a name, so one
was lost. Each is now named as Cypher clients name an unaliased item, ``n.name`` and ``c.name``:
every unaliased item is named as the statement writes it, and an item with its own name keeps it."""

# Requirements: REQ-345

from __future__ import annotations

from sqlglot import exp

from provisa.cypher.parser import parse_cypher
from provisa.gql.translator import cypher_to_sql
from tests.unit.test_cypher_translator import _make_label_map


def _names(cypher: str) -> list[str]:
    sql_ast, _params, _vars = cypher_to_sql(parse_cypher(cypher), _make_label_map(), {})
    return [e.alias_or_name for e in sql_ast.find(exp.Select).expressions]


def test_two_variables_same_named_properties_are_two_columns():
    names = _names("MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.name, c.name")
    assert names == ["n.name", "c.name"]


def test_a_property_no_other_item_shares_a_name_with_is_named_as_written():
    names = _names("MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.name, n.age, c.name")
    assert names == ["n.name", "n.age", "c.name"]


def test_an_item_with_its_own_name_keeps_it():
    names = _names("MATCH (n:Person)-[:WORKS_AT]->(c:Company) RETURN n.name AS person, c.name")
    assert names == ["person", "c.name"]
