# Copyright (c) 2026 Kenneth Stott
# Canary: 442b97cf-1987-4007-a710-0c24b1807569
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SPARQL front end (REQ-2135): a query parsed by rdflib, lowered to the graph IR, compiled by
the back end Cypher shares, and run on DuckDB over a small model, its answer read back as RDF
terms -- so each query is checked by what it returns."""

# Requirements: REQ-2135, REQ-2141

import dataclasses

import duckdb
import pytest

from provisa.gql.assembler import assemble_rows, take_cut
from provisa.gql.iri import subject_iri
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.sparql_query.compiler import SparqlRefused, compile_sparql
from provisa.sparql_query.vocabulary import (
    XSD,
    Grounding,
    build_vocabulary,
    column_iri,
    edge_iri,
    table_class_iri,
    term_iri,
)
from tests.unit.test_cypher_min_hops import _node, _rel

BASE = "https://data.example.com"
PERSON = table_class_iri(BASE, "hr", "persons")
COMPANY = table_class_iri(BASE, "hr", "companies")
NAME = column_iri(BASE, "hr", "persons", "name")
AGE = column_iri(BASE, "hr", "persons", "age")
CNAME = column_iri(BASE, "hr", "companies", "name")
KNOWS = edge_iri(BASE, "KNOWS")
WORKS_AT = edge_iri(BASE, "WORKS_AT")
PARENT = edge_iri(BASE, "PARENT")
EMPLOYEE = term_iri(BASE, "Employee")
FULL_NAME = term_iri(BASE, "Legal name")
PREFIXES = f"""
PREFIX p: <{PERSON}#>
PREFIX c: <{COMPANY}#>
PREFIX rel: <{BASE}/vocab/rel/>
PREFIX term: <{BASE}/vocab/term/>
PREFIX xsd: <{XSD}>
"""


@pytest.fixture(autouse=True)
def public_url(monkeypatch):
    monkeypatch.setenv("PROVISA_PUBLIC_URL", BASE)


def _model() -> CypherLabelMap:
    person = dataclasses.replace(
        _node("Person", "persons"),
        domain_id="hr",
        properties={"id": "id", "name": "name", "age": "age"},
    )
    company = dataclasses.replace(
        _node("Company", "companies"), domain_id="hr", properties={"id": "id", "name": "name"}
    )
    return CypherLabelMap(
        nodes={"Person": person, "Company": company},
        relationships={
            "KNOWS": _rel("KNOWS", "Person", "Person", "knows_id"),
            "WORKS_AT": _rel("WORKS_AT", "Person", "Company", "company_id"),
            "PARENT": _rel("PARENT", "Company", "Company", "parent_id"),
        },
    )


_TYPES = {
    ("Person", "id"): "INTEGER",
    ("Person", "name"): "VARCHAR",
    ("Person", "age"): "INTEGER",
    ("Company", "id"): "INTEGER",
    ("Company", "name"): "VARCHAR",
}
_GROUNDING = Grounding(
    class_terms={"Person": "Employee"},
    property_terms={("Person", "name"): ("Legal name",), ("Company", "name"): ("Legal name",)},
)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    """Ann (41) knows Bo; Bo knows Cy (29); Cy knows no one, and nor does Di (no age). Ann and Bo
    work at Acme, Cy at Beta; Acme's parent is Beta, Beta's is Corp."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS pg")
    c.execute("CREATE SCHEMA pg.public")
    c.execute(
        "CREATE TABLE pg.public.persons (id INTEGER, name VARCHAR, age INTEGER, knows_id INTEGER, "
        "company_id INTEGER)"
    )
    c.execute(
        "INSERT INTO pg.public.persons VALUES (1, 'Ann', 41, 2, 1), (2, 'Bo', 35, 3, 1), "
        "(3, 'Cy', 29, NULL, 2), (4, 'Di', NULL, NULL, NULL)"
    )
    c.execute("CREATE TABLE pg.public.companies (id INTEGER, name VARCHAR, parent_id INTEGER)")
    c.execute(
        "INSERT INTO pg.public.companies VALUES (1, 'Acme', 2), (2, 'Beta', 3), (3, 'Corp', NULL)"
    )
    return c


def _value(term):
    """An answer's term as a test reads it: a subject as its IRI, an IRI as itself, a literal as
    its value."""
    if term is None:
        return None
    if "node" in term:
        n = term["node"]
        return subject_iri(BASE, n["domain"], n["table"], n["key"])
    if "iri" in term:
        return term["iri"]
    return term["literal"]


def _answer(con, query: str) -> list[dict]:
    lm = _model()
    vocab = build_vocabulary(lm, BASE, _TYPES, _GROUNDING)
    compiled = compile_sparql(PREFIXES + query, vocab, lm)
    if compiled.empty:
        return []
    assert compiled.ast is not None
    sql, _, graph_vars = cypher_to_sql(compiled.ast, lm, {})
    # As the endpoint runs it (sparql_router): a term is its own value through the graph rewrite.
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    cur = con.execute(sql.sql(dialect="duckdb"))
    names = [d[0] for d in cur.description]
    rows = assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)
    if compiled.form == "ASK":
        return rows
    keep = compiled.variables or ["s", "p", "o"]
    return [{k: _value(r[k]) for k in keep} for r in rows]


def ann() -> str:
    return subject_iri(BASE, "hr", "persons", [1])


def test_a_class_and_a_column_predicate(con):
    rows = _answer(con, f"SELECT ?s ?n WHERE {{ ?s a <{PERSON}> ; p:name ?n }}")
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo", "Cy", "Di"]
    assert ann() in {r["s"] for r in rows}


def test_a_class_term_types_the_table_it_grounds(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s a term:Employee ; p:name ?n }")
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo", "Cy", "Di"]


def test_a_property_term_is_a_predicate_of_every_column_it_grounds(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s term:Legal%20name ?n }".replace("%20", "%20"))
    assert sorted(r["n"] for r in rows) == ["Acme", "Ann", "Beta", "Bo", "Corp", "Cy", "Di"]


def test_a_filter_on_a_typed_literal(con):
    rows = _answer(
        con, 'SELECT ?n WHERE { ?s p:name ?n ; p:age ?a FILTER(?a > "30"^^xsd:integer) }'
    )
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo"]


def test_a_required_property_drops_a_row_without_it_and_optional_keeps_it(con):
    required = _answer(con, "SELECT ?n ?a WHERE { ?s p:name ?n ; p:age ?a }")
    assert "Di" not in {r["n"] for r in required}
    optional = _answer(con, "SELECT ?n ?a WHERE { ?s p:name ?n OPTIONAL { ?s p:age ?a } }")
    assert {r["n"]: r["a"] for r in optional} == {"Ann": 41, "Bo": 35, "Cy": 29, "Di": None}


def test_an_edge_and_the_node_at_its_end(con):
    rows = _answer(con, "SELECT ?n ?cn WHERE { ?s p:name ?n ; rel:WORKS_AT ?c . ?c c:name ?cn }")
    assert sorted((r["n"], r["cn"]) for r in rows) == [
        ("Ann", "Acme"),
        ("Bo", "Acme"),
        ("Cy", "Beta"),
    ]


def test_an_edge_written_from_its_far_end(con):
    rows = _answer(con, "SELECT ?n WHERE { ?c c:name 'Beta' . ?s rel:WORKS_AT ?c ; p:name ?n }")
    assert [r["n"] for r in rows] == ["Cy"]


def test_a_property_path_reaches_each_pair_once(con):
    rows = _answer(con, "SELECT ?a ?b WHERE { ?x rel:PARENT+ ?y . ?x c:name ?a . ?y c:name ?b }")
    assert sorted((r["a"], r["b"]) for r in rows) == [
        ("Acme", "Beta"),
        ("Acme", "Corp"),
        ("Beta", "Corp"),
    ]


def test_a_sequence_path_with_an_inverse(con):
    # who works where someone Ann knows works: Ann knows Bo, Bo works at Acme, Acme employs Ann, Bo
    rows = _answer(
        con,
        "SELECT ?n WHERE { ?s p:name 'Ann' . ?s rel:KNOWS/rel:WORKS_AT/^rel:WORKS_AT ?o . "
        "?o p:name ?n }",
    )
    assert sorted(r["n"] for r in rows) == ["Ann", "Bo"]


def test_a_subject_named_by_its_iri_and_a_variable_predicate(con):
    rows = _answer(con, f"SELECT ?p ?o WHERE {{ <{ann()}> ?p ?o }}")
    got = {(r["p"], r["o"]) for r in rows}
    bo = subject_iri(BASE, "hr", "persons", [2])
    acme = subject_iri(BASE, "hr", "companies", [1])
    assert {
        ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", PERSON),
        ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", EMPLOYEE),
        (NAME, "Ann"),
        (FULL_NAME, "Ann"),
        (AGE, 41),
        (KNOWS, bo),
        (WORKS_AT, acme),
    } <= got


def test_filter_on_a_subject_iri(con):
    rows = _answer(con, f"SELECT ?n WHERE {{ ?s p:name ?n FILTER(?s = <{ann()}>) }}")
    assert [r["n"] for r in rows] == ["Ann"]


def test_an_untyped_subject_takes_the_class_of_its_predicates(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s c:name ?n }")
    assert sorted(r["n"] for r in rows) == ["Acme", "Beta", "Corp"]


def test_union(con):
    rows = _answer(con, "SELECT ?n WHERE { { ?s p:name ?n } UNION { ?s c:name ?n } }")
    assert len(rows) == 7


def test_order_by_and_limit(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s p:name ?n } ORDER BY DESC(?n) LIMIT 2")
    assert [r["n"] for r in rows] == ["Di", "Cy"]


def test_order_by_across_two_classes(con):
    rows = _answer(con, "SELECT ?n WHERE { ?s term:Legal%20name ?n } ORDER BY ?n LIMIT 3")
    assert [r["n"] for r in rows] == ["Acme", "Ann", "Beta"]


def test_distinct(con):
    rows = _answer(con, "SELECT DISTINCT ?c WHERE { ?s rel:WORKS_AT ?c }")
    assert len(rows) == 2


def test_ask(con):
    assert _answer(con, "ASK { ?s p:name 'Ann' }") != []
    assert _answer(con, "ASK { ?s p:name 'Zed' }") == []


def test_construct(con):
    rows = _answer(
        con,
        "CONSTRUCT { ?s <http://example.org/colleagueOf> ?o } "
        "WHERE { ?s rel:WORKS_AT ?c . ?o rel:WORKS_AT ?c FILTER(?s != ?o) }",
    )
    bo = subject_iri(BASE, "hr", "persons", [2])
    assert sorted((r["s"], r["o"]) for r in rows) == [(ann(), bo), (bo, ann())]


def test_a_predicate_the_vocabulary_lacks_matches_nothing(con):
    assert _answer(con, "SELECT ?o WHERE { ?s <http://example.org/nope> ?o }") == []


@pytest.mark.parametrize(
    "query, refusal",
    [
        ("SELECT (COUNT(?s) AS ?n) WHERE { ?s p:name ?x }", "aggregates"),
        ("SELECT ?s WHERE { VALUES ?s { 1 } }", "VALUES"),
        ("DESCRIBE ?s WHERE { ?s p:name ?n }", "DESCRIBE|Describe"),
        ("SELECT ?n WHERE { ?s p:name ?n FILTER(STRLEN(?n) > 2) }", "STRLEN"),
    ],
)
def test_what_is_not_lowered_is_refused_by_name(query, refusal):
    lm = _model()
    vocab = build_vocabulary(lm, BASE, _TYPES, _GROUNDING)
    with pytest.raises(SparqlRefused, match=refusal):
        compile_sparql(PREFIXES + query, vocab, lm)


def test_grounding_and_column_types_come_from_the_model_s_own_references():
    from types import SimpleNamespace

    from provisa.sparql_query.vocabulary import column_types_of, grounding_from_refs

    lm = _model()
    person_id = lm.nodes["Person"].table_id
    grounding = grounding_from_refs(
        lm, {person_id: "Employee", 99999: "Ghost"}, {(person_id, "name"): ["Legal name"]}
    )
    assert grounding.class_terms == {"Person": "Employee"}
    assert grounding.property_terms == {("Person", "name"): ("Legal name",)}
    ctx = SimpleNamespace(aggregate_columns={person_id: [("age", "INTEGER"), ("gone", "TEXT")]})
    assert column_types_of(lm, ctx) == {("Person", "age"): "INTEGER"}
