# Copyright (c) 2026 Kenneth Stott
# Canary: a9d9460f-a21a-47a4-89a6-e763f3e573c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An inline property map matches as the WHERE it means (REQ-345, REQ-1586).

``MATCH (n:Person {name: 'Ann'})`` is ``MATCH (n:Person) WHERE n.name = 'Ann'``: the map on a
node or a relationship, in MATCH or OPTIONAL MATCH, a ``$param`` value included, is the same SQL as
the equivalent WHERE. Before this the map was parsed and dropped, so the read returned every row.
A DELETE or SET whose MATCH node carries a map writes only the rows it names."""

# Requirements: REQ-345, REQ-1586
from __future__ import annotations

import pytest

from provisa.cypher.parser import CypherParseError, parse_cypher
from provisa.cypher.translator import cypher_to_sql
from provisa.cypher.write_translator import (
    CypherWriteParseError,
    WriteTranslator,
    parse_cypher_write,
)
from tests.unit.test_cypher_junction_relationships import _label_map as _junction_map
from tests.unit.test_cypher_mutations import _person_map
from tests.unit.test_cypher_translator import _make_label_map

pytestmark = pytest.mark.unit


def _sql(query: str, lm=None) -> tuple[str, list[str]]:
    ast, params, _ = cypher_to_sql(parse_cypher(query), lm or _make_label_map(), {})
    return ast.sql(dialect="trino"), params


def test_a_node_map_filters_its_rows():
    # Fails before: the map was dropped and every person was returned.
    sql, _ = _sql("MATCH (n:Person {name: 'Ann'}) RETURN n.name")
    assert "'Ann'" in sql and "WHERE" in sql


def test_a_node_map_is_the_where_it_means():
    assert _sql("MATCH (n:Person {name: 'Ann', age: 30}) RETURN n.name") == _sql(
        "MATCH (n:Person) WHERE n.name = 'Ann' AND n.age = 30 RETURN n.name"
    )


def test_a_map_and_a_where_both_hold():
    assert _sql("MATCH (n:Person {name: 'Ann'}) WHERE n.age > 21 RETURN n.name") == _sql(
        "MATCH (n:Person) WHERE n.name = 'Ann' AND (n.age > 21) RETURN n.name"
    )


def test_a_parameter_in_a_map_is_bound_as_in_where():
    inline = _sql("MATCH (n:Person {name: $who}) RETURN n.name")
    assert inline == _sql("MATCH (n:Person) WHERE n.name = $who RETURN n.name")
    assert inline[1] == ["who"]


def test_a_string_with_a_quote_is_matched_as_written():
    assert _sql(r"MATCH (n:Person {name: 'O\'Brien'}) RETURN n.name") == _sql(
        r"MATCH (n:Person) WHERE n.name = 'O\'Brien' RETURN n.name"
    )


def test_an_anonymous_node_with_a_map_is_filtered():
    sql, _ = _sql("MATCH (p:Person)-[:WORKS_AT]->(:Company {name: 'Acme'}) RETURN p.name")
    assert "'Acme'" in sql and "WHERE" in sql


def test_an_optional_match_map_constrains_its_join_not_the_rows():
    sql, _ = _sql(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company {name: 'Acme'}) "
        "RETURN p.name, c.name"
    )
    on, _, where = sql.partition(" WHERE ")
    assert "LEFT JOIN" in on and "'Acme'" in on
    assert "'Acme'" not in where


def test_a_relationship_map_is_the_where_it_means():
    lm = _junction_map()
    assert _sql(
        "MATCH (t:GlossaryTerm)-[r:KIND_OF {note: 'x'}]->(b:GlossaryTerm) RETURN b.name", lm
    ) == _sql(
        "MATCH (t:GlossaryTerm)-[r:KIND_OF]->(b:GlossaryTerm) WHERE r.note = 'x' RETURN b.name", lm
    )


def test_a_map_on_a_variable_length_relationship_is_refused():
    with pytest.raises(CypherParseError, match="variable-length"):
        parse_cypher("MATCH (t:GlossaryTerm)-[r:KIND_OF*1..3 {note: 'x'}]->(b) RETURN b")


def _write(cypher: str) -> str:
    return WriteTranslator(_person_map()).translate(parse_cypher_write(cypher))


def test_a_set_whose_node_has_a_map_updates_only_those_rows():
    sql = _write("MATCH (n:Person {name: 'Ann'}) SET n.age = 31")
    assert sql.endswith('SET "age" = 31 WHERE "name" = \'Ann\'')


def test_a_delete_whose_node_has_a_map_and_a_where_holds_both():
    sql = _write("MATCH (n:Person {name: 'Ann'}) WHERE n.age > 21 DELETE n")
    assert sql.endswith('WHERE "name" = \'Ann\' AND ("age" > 21)')


def test_a_where_only_write_is_unchanged():
    assert _write("MATCH (n:Person) WHERE n.age > 21 DELETE n").endswith('WHERE "age" > 21')


def test_a_write_naming_no_rows_is_refused():
    with pytest.raises(CypherWriteParseError, match="which rows"):
        parse_cypher_write("MATCH (n:Person) DELETE n")


def test_merge_with_a_map_is_still_refused():
    with pytest.raises(CypherWriteParseError, match="MERGE"):
        parse_cypher_write("MERGE (n:Person {name: 'Ann'})")
