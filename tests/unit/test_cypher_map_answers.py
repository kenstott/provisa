# Copyright (c) 2026 Kenneth Stott
# Canary: 13268303-e928-48d0-aa03-4723f342b9df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A map a statement returns answers its values as they are (REQ-345, REQ-2136): a map literal is
built with each value JSON-encoded, so a map of several types is one engine type, and the answer
decodes them -- returned bare, nested in another map, in a list, collected, or in a COLLECT { }
subquery, as the Ontology View's query opened on the Cypher page returns its edges."""

# Requirements: REQ-345, REQ-2136

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import assemble_rows, to_serializable
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.gql.translator_types import GraphVarKind
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_junction_relationships import _term_node


@pytest.fixture
def answer():
    lm = CypherLabelMap(nodes={"GlossaryTerm": _term_node()}, relationships={})
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute("CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab')")

    def _answer(cypher: str) -> list[dict]:
        sql_ast, _, graph_vars = cypher_to_sql(parse_cypher(cypher), lm, {})
        cur = con.execute(transpile_to_duckdb(sql_ast.sql(dialect="postgres")))
        cols = [d[0] for d in cur.description]
        named = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        return [to_serializable(r) for r in assemble_rows(named, graph_vars)]

    return _answer


def test_a_returned_map_answers_its_values_as_they_are(answer):
    assert answer("MATCH (t:GlossaryTerm) RETURN {id: t.id, name: t.name, ok: true} AS m") == [
        {"m": {"id": 1, "name": "ab", "ok": True}}
    ]


def test_a_map_nested_in_a_map_answers_as_a_map(answer):
    assert answer("MATCH (t:GlossaryTerm) RETURN {term: {name: t.name}} AS m") == [
        {"m": {"term": {"name": "ab"}}}
    ]


def test_a_map_or_null_by_a_condition_answers_as_a_map(answer):
    cypher = "MATCH (t:GlossaryTerm) RETURN CASE WHEN t.name = 'ab' THEN {name: t.name} END AS m"
    assert answer(cypher) == [{"m": {"name": "ab"}}]


@pytest.mark.parametrize(
    "returned",
    [
        "[{id: t.id, name: t.name}]",
        "collect({id: t.id, name: t.name})",
        "COLLECT { MATCH (u:GlossaryTerm) WHERE u.name = t.name RETURN {id: u.id, name: u.name} AS row }",
    ],
)
def test_maps_in_a_list_answer_as_maps(answer, returned):
    assert answer(f"MATCH (t:GlossaryTerm) RETURN {returned} AS l") == [
        {"l": [{"id": 1, "name": "ab"}]}
    ]


def test_a_map_an_engine_hands_back_as_json_text_answers_the_same():
    """Snowflake's OBJECT_CONSTRUCT comes back as the object's JSON text."""
    rows = assemble_rows([{"m": '{"id": "1", "name": "\\"ab\\""}'}], {"m": GraphVarKind.JSON_MAP})
    assert to_serializable(rows[0]) == {"m": {"id": 1, "name": "ab"}}
