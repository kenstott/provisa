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
from provisa.gql.iri import subject_iri
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.security.rights import ONTOLOGY_DOMAIN_ID
from provisa.sparql_query.compiler import compile_sparql
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
    ("Organisation", f"{RDFS}subClassOf", "Party", None, None),
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


#: The instance tables: persons grounded in Customer, companies in Organisation, each name bound
#: to the property term "legal name".
_DATA = {"Person": "persons", "Company": "companies"}
_CLASS_TERMS = {"Person": "Customer", "Company": "Organisation"}
#: The closure the ontology states: Customer and Organisation are kinds of Party; legal name is a
#: kind of name. Reflexive pairs are hops 0 (rdfs:subClassOf*), the rest hops 1 or more.
_CLOSURE = [
    ("Customer", "Customer", f"{RDFS}subClassOf", 0),
    ("Organisation", "Organisation", f"{RDFS}subClassOf", 0),
    ("Party", "Party", f"{RDFS}subClassOf", 0),
    ("Customer", "Party", f"{RDFS}subClassOf", 1),
    ("Organisation", "Party", f"{RDFS}subClassOf", 1),
    ("legal name", "legal name", f"{RDFS}subPropertyOf", 0),
    ("name", "name", f"{RDFS}subPropertyOf", 0),
    ("legal name", "name", f"{RDFS}subPropertyOf", 1),
]


def _maps(readable=tuple(_DATA)) -> tuple[CypherLabelMap, CypherLabelMap]:
    """The role's graph (its readable tables, no SQL-only node) and SPARQL's (with the ontology's
    SQL-only tables)."""
    sql_only = {
        "Axioms": dataclasses.replace(
            _node("Axioms", "_axioms_ontology"),
            domain_id=ONTOLOGY_DOMAIN_ID,
            properties={c: c for c in _COLUMNS},
        ),
        "Closure": dataclasses.replace(
            _node("Closure", "_closure_ontology"),
            domain_id=ONTOLOGY_DOMAIN_ID,
            properties={c: c for c in ("subject", "object", "predicate", "hops")},
        ),
    }
    nodes = {
        label: dataclasses.replace(
            _node(label, _DATA[label]), domain_id="hr", properties={"id": "id", "name": "name"}
        )
        for label in readable
    }
    graph = CypherLabelMap(
        nodes=nodes,
        relationships={},
        sql_only_nodes=sql_only,
        sql_only_tables={
            (ONTOLOGY_DOMAIN_ID, "_axioms"): "Axioms",
            (ONTOLOGY_DOMAIN_ID, "_closure"): "Closure",
        },
    )
    return graph, graph.with_sql_only()


def _grounding(readable) -> Grounding:
    return Grounding(
        class_terms={label: _CLASS_TERMS[label] for label in readable},
        property_terms={(label, "name"): ("legal name",) for label in readable},
    )


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
    c.execute(
        "CREATE TABLE pg.public._closure_ontology (subject VARCHAR, object VARCHAR, "
        "predicate VARCHAR, hops INTEGER)"
    )
    for row in _CLOSURE:
        c.execute("INSERT INTO pg.public._closure_ontology VALUES (?, ?, ?, ?)", list(row))
    c.execute("CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR)")
    c.execute("INSERT INTO pg.public.persons VALUES (1, 'Ann'), (2, 'Bo')")
    c.execute("CREATE TABLE pg.public.companies (id INTEGER, name VARCHAR)")
    c.execute("INSERT INTO pg.public.companies VALUES (1, 'Acme')")
    return c


def _value(term):
    if term is None:
        return None
    if "node" in term:
        n = term["node"]
        return subject_iri(BASE, n["domain"], n["table"], n["key"])
    if "term" in term:
        return term_iri(BASE, term["term"])
    if "iri" in term:
        return term["iri"]
    return term["literal"]


def _answer(con, query: str, readable=tuple(_DATA)) -> list[tuple]:
    graph, sparql = _maps(readable)
    vocab = build_vocabulary(graph, BASE, {}, _grounding(readable))
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
    assert rows == [(t("Customer"), t("Party")), (t("Organisation"), t("Party"))]


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
    assert rows == [(t("Customer"), "Party"), (t("Organisation"), "Party")]


def test_a_construct_of_the_classes(con):
    rows = _answer(con, "CONSTRUCT { ?c a owl:Class } WHERE { ?c a owl:Class }")
    assert rows == [
        (t("Customer"), f"{RDF}type", f"{OWL}Class"),
        (t("Order"), f"{RDF}type", f"{OWL}Class"),
    ]


def person(k: int) -> str:
    return subject_iri(BASE, "hr", "persons", [k])


def company(k: int) -> str:
    return subject_iri(BASE, "hr", "companies", [k])


def test_instances_of_a_superclass_include_its_subclasses_instances(con):
    rows = _answer(con, "SELECT ?x WHERE { ?x a ?c . ?c rdfs:subClassOf* term:Party }")
    assert rows == sorted([(person(1),), (person(2),), (company(1),)])


def test_a_role_that_cannot_read_a_subclasss_table_gets_none_of_its_instances(con):
    rows = _answer(
        con, "SELECT ?x WHERE { ?x a ?c . ?c rdfs:subClassOf* term:Party }", readable=("Person",)
    )
    assert rows == [(person(1),), (person(2),)]


def test_the_class_an_instance_is_found_by_is_answered(con):
    rows = _answer(con, "SELECT ?x ?c WHERE { ?x a ?c . ?c rdfs:subClassOf term:Party }")
    assert rows == sorted(
        [(company(1), t("Organisation")), (person(1), t("Customer")), (person(2), t("Customer"))]
    )


def test_values_of_a_superproperty_include_its_subproperties_values(con):
    rows = _answer(con, "SELECT ?x ?v WHERE { ?x ?p ?v . ?p rdfs:subPropertyOf* term:name }")
    assert rows == sorted([(person(1), "Ann"), (person(2), "Bo"), (company(1), "Acme")])


def test_a_one_or_more_path_leaves_the_class_itself_out(con):
    rows = _answer(con, "SELECT ?c WHERE { ?c rdfs:subClassOf+ term:Party }")
    assert rows == [(t("Customer"),), (t("Organisation"),)]
