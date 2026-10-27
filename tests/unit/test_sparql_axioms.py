# Copyright (c) 2026 Kenneth Stott
# Canary: 18427f44-4843-47fb-ac54-6c6708133b7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2163 step 3: SPARQL serves the ontology schema as RDFS/OWL/SKOS. A pattern whose predicate
is rdfs:subClassOf, rdfs:subPropertyOf, rdfs:domain, rdfs:range, skos:prefLabel or
skos:definition, or rdf:type with an OWL class, is lowered to ontology._axioms (a table SQL alone
serves, reached here through the label map's SQL-only nodes); a term is answered by its vocabulary
IRI, built from its name."""

from __future__ import annotations

import dataclasses

import duckdb
import pytest

from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.security.rights import ONTOLOGY_DOMAIN_ID
from provisa.sparql_query.compiler import SparqlRefused, compile_sparql
from provisa.sparql_query.vocabulary import (
    OWL,
    RDF,
    RDFS,
    SKOS,
    XSD,
    Grounding,
    build_vocabulary,
    term_iri,
)
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_cypher_min_hops import _node

BASE = "https://data.example.com"
PREFIXES = f"""
PREFIX rdf: <{RDF}>
PREFIX rdfs: <{RDFS}>
PREFIX owl: <{OWL}>
PREFIX skos: <{SKOS}>
PREFIX xsd: <{XSD}>
PREFIX term: <{BASE}/vocab/term/>
"""
_COLUMNS = ("id", "subject", "predicate", "object", "object_iri", "object_literal")
_AXIOMS = [
    ("Customer", f"{RDF}type", None, f"{OWL}Class", None),
    ("Order", f"{RDF}type", None, f"{OWL}Class", None),
    ("segment", f"{RDF}type", None, f"{OWL}DatatypeProperty", None),
    ("customer", f"{RDF}type", None, f"{OWL}ObjectProperty", None),
    ("Customer", f"{RDFS}subClassOf", "Party", None, None),
    ("segment", f"{RDFS}domain", "Customer", None, None),
    ("segment", f"{RDFS}range", None, f"{XSD}string", None),
    ("customer", f"{RDFS}range", "Customer", None, None),
    ("Customer", f"{SKOS}prefLabel", None, None, "Customer"),
    ("Party", f"{SKOS}prefLabel", None, None, "Party"),
    ("Customer", f"{SKOS}definition", None, None, "Someone who buys"),
]


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _maps() -> tuple[CypherLabelMap, CypherLabelMap]:
    """The role's graph (no SQL-only node) and SPARQL's (with ontology._axioms)."""
    axioms = dataclasses.replace(
        _node("Axioms", "_axioms_ontology"),
        domain_id=ONTOLOGY_DOMAIN_ID,
        properties={c: c for c in _COLUMNS},
    )
    graph = CypherLabelMap(
        nodes={},
        relationships={},
        sql_only_nodes={"Axioms": axioms},
        sql_only_tables={(ONTOLOGY_DOMAIN_ID, "_axioms"): "Axioms"},
    )
    return graph, graph.with_sql_only()


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    c.execute(
        "CREATE TABLE pg.public._axioms_ontology (id VARCHAR, subject VARCHAR, predicate VARCHAR, "
        "object VARCHAR, object_iri VARCHAR, object_literal VARCHAR)"
    )
    for i, row in enumerate(_AXIOMS):
        c.execute("INSERT INTO pg.public._axioms_ontology VALUES (?, ?, ?, ?, ?, ?)", [i, *row])
    return c


def _value(term):
    if term is None:
        return None
    if "term" in term:
        return term_iri(BASE, term["term"])
    if "iri" in term:
        return term["iri"]
    return term["literal"]


def _answer(con, query: str) -> list[tuple]:
    graph, sparql = _maps()
    vocab = build_vocabulary(graph, BASE, {}, Grounding())
    compiled = compile_sparql(PREFIXES + query, vocab, sparql)
    assert compiled.ast is not None
    sql, _, graph_vars = cypher_to_sql(compiled.ast, sparql, {})
    sql = apply_graph_rewrites(sql, graph_vars, sparql)
    cur = con.execute(transpile_to_duckdb(sql.sql(dialect="postgres")))
    names = [d[0] for d in cur.description]
    rows = assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)
    keep = compiled.variables or ["s", "p", "o"]
    return sorted(
        (tuple(_value(r[k]) for k in keep) for r in rows), key=lambda t: tuple(map(str, t))
    )


def t(name: str) -> str:
    return term_iri(BASE, name)


def test_the_classes(con):
    assert _answer(con, "SELECT ?c WHERE { ?c a owl:Class }") == [(t("Customer"),), (t("Order"),)]


def test_a_subclass(con):
    rows = _answer(con, "SELECT ?c ?d WHERE { ?c rdfs:subClassOf ?d }")
    assert rows == [(t("Customer"), t("Party"))]


def test_a_named_terms_label(con):
    assert _answer(con, "SELECT ?l WHERE { term:Customer skos:prefLabel ?l }") == [("Customer",)]


def test_a_datatype_property_and_its_range(con):
    rows = _answer(con, "SELECT ?p ?r WHERE { ?p a owl:DatatypeProperty ; rdfs:range ?r }")
    assert rows == [(t("segment"), f"{XSD}string")]


def test_an_object_propertys_range_is_a_class(con):
    rows = _answer(con, "SELECT ?r WHERE { ?p a owl:ObjectProperty ; rdfs:range ?r }")
    assert rows == [(t("Customer"),)]


def test_a_chain_through_a_shared_term(con):
    rows = _answer(con, "SELECT ?c ?l WHERE { ?c rdfs:subClassOf ?d . ?d skos:prefLabel ?l }")
    assert rows == [(t("Customer"), "Party")]


def test_a_construct_of_the_classes(con):
    rows = _answer(con, "CONSTRUCT { ?c a owl:Class } WHERE { ?c a owl:Class }")
    assert rows == [
        (t("Customer"), f"{RDF}type", f"{OWL}Class"),
        (t("Order"), f"{RDF}type", f"{OWL}Class"),
    ]


def test_axioms_joined_to_instance_data_are_refused_by_name():
    graph, sparql = _maps()
    vocab = build_vocabulary(graph, BASE, {}, Grounding())
    with pytest.raises(SparqlRefused, match="ontology's axioms"):
        compile_sparql(PREFIXES + "SELECT ?x WHERE { ?x a ?c . ?c a owl:Class }", vocab, sparql)
