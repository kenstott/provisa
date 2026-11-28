# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b1d47-6c2a-4f95-a0d3-9b7e5c14f862
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ontology view's Cypher, opened as SPARQL, answers what it does (REQ-2136, REQ-2174): each
walk the view writes (provisa-ui ontologyCypher.ts, its text pinned by ontologyWalks.json, which
the UI's own test checks the writer against) is run as Cypher and as its SPARQL export -- read
back by the SPARQL front end -- on DuckDB over one small term graph whose links are rows of a
typed junction, as the ontology's are, and both give the same terms and links."""

# Requirements: REQ-2136, REQ-2174, REQ-2135

import json
from pathlib import Path

import duckdb
import pytest

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import (
    CypherLabelMap,
    JunctionMapping,
    NodeMapping,
    RelationshipMapping,
)
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import compile_sparql
from provisa.sparql_query.export import SparqlExportRefused, ir_to_sparql
from provisa.sparql_query.vocabulary import Grounding, build_vocabulary
from provisa.transpiler.transpile import transpile_to_duckdb

BASE = "https://data.example.com"
_WALKS = json.loads(
    (
        Path(__file__).resolve().parents[2]
        / "provisa-ui/src/components/graph3d/__tests__/ontologyWalks.json"
    ).read_text()
)
_TYPES = {("Term", "id"): "INTEGER", ("Term", "name"): "VARCHAR"}


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _model() -> CypherLabelMap:
    """The terms and their links, each link type a type of one junction, as the ontology's."""
    assert _WALKS["graph"]["label"] == "Term"
    term = NodeMapping(
        label="Term",
        type_name="Term",
        domain_label=None,
        table_label="Term",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="pg",
        schema_name="ontology",
        table_name="_terms",
        properties={"id": "id", "name": "name"},
        domain_id="ontology",
    )
    links = {
        t: RelationshipMapping(
            rel_type=t,
            source_label="Term",
            target_label="Term",
            join_source_column="id",
            join_target_column="id",
            field_name=t.lower(),
            via=JunctionMapping(
                catalog_name="pg",
                schema_name="ontology",
                table_name="_term_edges",
                type_name="TermEdge",
                source_columns=("from_term_id",),
                target_columns=("to_term_id",),
                type_column="rel_type",
                type_value=t,
            ),
            registered=True,
        )
        for t in _WALKS["graph"]["links"]
    }
    return CypherLabelMap(nodes={"Term": term}, relationships=links)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """1 is a kind of 2 and related to it, and 2 related back to 1; 2 is a kind of 3, 3 related
    to 4, 4 a kind of 5: 5 is four links from 1. 7 is related to 6; 8 has no link."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.ontology")
    c.execute("CREATE TABLE pg.ontology._terms (id INTEGER, name VARCHAR)")
    c.execute(
        "INSERT INTO pg.ontology._terms VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), "
        "(6, 'f'), (7, 'g'), (8, 'h')"
    )
    c.execute(
        "CREATE TABLE pg.ontology._term_edges (id INTEGER, from_term_id INTEGER, "
        "to_term_id INTEGER, rel_type VARCHAR)"
    )
    c.execute(
        "INSERT INTO pg.ontology._term_edges VALUES (1, 1, 2, 'KIND_OF'), (2, 2, 3, 'KIND_OF'), "
        "(3, 3, 4, 'RELATED_TO'), (4, 4, 5, 'KIND_OF'), (5, 1, 2, 'RELATED_TO'), "
        "(6, 2, 1, 'RELATED_TO'), (7, 7, 6, 'RELATED_TO')"
    )
    return c


def _rows(con, ast, lm) -> list[dict]:
    sql, _, graph_vars = cypher_to_sql(ast, lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    cur = con.execute(transpile_to_duckdb(sql.sql(dialect="postgres"), generated=True))
    names = [d[0] for d in cur.description]
    return assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)


def _cypher_value(v):
    """A Cypher answer's node as its term id, its relationship as its type."""
    if v is None:
        return None
    if hasattr(v, "type") and hasattr(v, "start_node"):
        return v.type
    return v.properties["id"]


def _sparql_value(v):
    """A SPARQL answer's subject as its term id, its predicate as its edge type."""
    if v is None:
        return None
    if "node" in v:
        assert v["node"]["table"] == "_terms"
        (key,) = v["node"]["key"]
        return key
    return v["iri"].removeprefix(f"{BASE}/vocab/rel/")


def _export(cypher: str) -> str:
    lm = _model()
    return ir_to_sparql(parse_cypher(cypher), build_vocabulary(lm, BASE, _TYPES, Grounding()), lm)


@pytest.mark.parametrize("walk", _WALKS["walks"], ids=[w["name"] for w in _WALKS["walks"]])
def test_the_view_s_sparql_answers_what_its_cypher_does(con, walk):
    lm = _model()
    sparql = _export(walk["cypher"])
    compiled = compile_sparql(sparql, build_vocabulary(lm, BASE, _TYPES, Grounding()), lm)
    keys = [i.name for i in parse_cypher(walk["cypher"]).return_clause.items]
    assert compiled.variables == keys, sparql
    if compiled.empty:
        got = []
    else:
        got = [tuple(_sparql_value(r[k]) for k in keys) for r in _rows(con, compiled.ast, lm)]
    expected = [
        tuple(_cypher_value(r[k]) for k in keys)
        for r in _rows(con, parse_cypher(walk["cypher"]), lm)
    ]
    assert sorted(got, key=repr) == sorted(expected, key=repr), sparql
    if walk["roots"]:
        assert expected, "a walk from a term answers it"


def test_each_hop_reaches_further_and_all_reaches_the_end_of_the_connected_part(con):
    """The fixture tells the walks apart: Hops 1, 2, 3 and All from term 1 reach more each time."""
    lm = _model()
    reached = {
        w["depth"]: {r["a"].properties["id"] for r in _rows(con, parse_cypher(w["cypher"]), lm)}
        for w in _WALKS["walks"]
        if w["roots"] == [1] and w["edgeTypes"] == [] and w["writer"] == "walk"
    }
    assert reached == {0: {1}, 1: {1, 2}, 2: {1, 2, 3}, 3: {1, 2, 3, 4}, 4: {1, 2, 3, 4, 5}}


def test_a_bounded_walk_is_a_nested_optional_path_and_a_link_of_several_types_a_branch_each():
    sparql = _export(
        "MATCH (t:Term {id: 1})-[:KIND_OF|RELATED_TO*0..3]-(a:Term) "
        "OPTIONAL MATCH (a)-[r:KIND_OF|RELATED_TO]->(b:Term) RETURN DISTINCT a, r, b"
    )
    step = "(rel:KIND_OF|^rel:KIND_OF|rel:RELATED_TO|^rel:RELATED_TO)"
    assert f"?t ({step}/({step}/{step}?)?)? ?a ." in sparql
    assert "PREFIX terms: <" in sparql  # a PREFIX name starts with a letter, though _terms does not
    assert sparql.count("UNION") == 2  # KIND_OF, RELATED_TO, and neither
    assert "BIND(rel:KIND_OF AS ?r)" in sparql and "BIND(rel:RELATED_TO AS ?r)" in sparql
    assert sparql.count("FILTER NOT EXISTS") == 2


@pytest.mark.parametrize(
    "cypher, why",
    [
        (
            "MATCH (t:Term)-[:KIND_OF*0..2]-(a:Term) RETURN a.name",
            "variable-length relationship in a statement whose rows are not DISTINCT",
        ),
        ("MATCH (t:Term)-[r:KIND_OF*0..2]-(a:Term) RETURN DISTINCT a", "relationship variable"),
        (
            "MATCH (a:Term)-[r:KIND_OF|RELATED_TO]->(b:Term) RETURN b.name",
            "A property of b, matched in a branch per relationship type",
        ),
        (
            "MATCH (a:Term) OPTIONAL MATCH (a)-[r:KIND_OF|RELATED_TO]->(b:Term) "
            "OPTIONAL MATCH (b)-[:KIND_OF]->(c:Term) RETURN a, r, b, c",
            "branched on before the statement's last step",
        ),
    ],
)
def test_what_the_export_cannot_say_exactly_is_refused_by_name(cypher, why):
    with pytest.raises(SparqlExportRefused, match=why):
        _export(cypher)
