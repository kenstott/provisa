# Copyright (c) 2026 Kenneth Stott
# Canary: ea3b902a-3e80-4959-aaf8-398a6643f314
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2163 (A TERM-BOUND COLUMN'S PROPERTY IS THE TERM'S): a column bound to a glossary term is
published in SPARQL under that term's property IRI, one IRI shared by every column bound to the
term, so a query on it finds every class that has it; a column bound to no term keeps its
per-class property IRI; and the term's property is declared with its domains, one per class whose
column binds it.

The model (tests.unit.test_sparql_compiler): Person.name and Company.name are bound to the
Property term "Legal name"; Person.age is bound to no term."""

# Requirements: REQ-2163, REQ-2135

from __future__ import annotations

from provisa.cypher.parser import parse_cypher
from provisa.gql.iri import subject_iri
from provisa.sparql_query.export import ir_to_sparql
from provisa.sparql_query.vocabulary import XSD, build_vocabulary, vocabulary_document
from tests.unit import test_sparql_compiler as _sparql_tests
from tests.unit.test_sparql_compiler import (  # noqa: F401 -- fixtures
    AGE,
    BASE,
    CNAME,
    COMPANY,
    FULL_NAME,
    NAME,
    PERSON,
    _COLUMNS,
    _GROUNDING,
    _TYPES,
    _answer,
    _model,
    ann,
    public_url,
)
from tests.unit.test_sparql_export import _cypher_rows

con = _sparql_tests.con  # the model's DuckDB fixture

ACME = subject_iri(BASE, "hr", "companies", [1])


def _doc(grounding=_GROUNDING) -> dict:
    vocab = build_vocabulary(_model(), BASE, _TYPES, grounding)
    return vocabulary_document(vocab, [], {}, {})


def test_the_vocabulary_lists_a_term_property_once_with_a_domain_per_bound_class():
    by_term = [p for p in _doc()["predicates"] if p["iri"] == FULL_NAME]
    assert by_term == [
        {
            "iri": FULL_NAME,
            "kind": "term",
            "classes": [
                {
                    "class": PERSON,
                    "label": "Person",
                    "property": "name",
                    "datatype": f"{XSD}string",
                },
                {
                    "class": COMPANY,
                    "label": "Company",
                    "property": "name",
                    "datatype": f"{XSD}string",
                },
            ],
            "domains": [],
        }
    ]


def test_a_term_bound_column_has_no_per_class_property_of_its_own():
    iris = {p["iri"] for p in _doc()["predicates"]}
    assert NAME not in iris and CNAME not in iris


def test_an_unbound_column_keeps_its_per_class_property():
    predicates = _doc()["predicates"]
    assert {
        "iri": AGE,
        "kind": "column",
        "label": "Person",
        "property": "age",
        "datatype": f"{XSD}integer",
    } in predicates
    # Bound to no term, the two name columns are two properties, each of its own class.
    plain = {p["iri"]: p for p in _doc(_COLUMNS)["predicates"]}
    assert (plain[NAME]["label"], plain[CNAME]["label"]) == ("Person", "Company")
    assert FULL_NAME not in plain


def test_a_select_on_the_term_property_reads_every_class_bound_to_it(con):
    rows = _answer(con, "SELECT ?s ?n WHERE { ?s term:Legal%20name ?n }", _GROUNDING)
    got = {(r["s"], r["n"]) for r in rows}
    assert (ann(), "Ann") in got and (ACME, "Acme") in got
    assert len(rows) == 7  # four persons and three companies


def test_a_select_on_the_term_property_of_a_typed_subject_reads_that_class_only(con):
    rows = _answer(
        con, f"SELECT ?n WHERE {{ ?s a <{COMPANY}> ; term:Legal%20name ?n }}", _GROUNDING
    )
    assert sorted(r["n"] for r in rows) == ["Acme", "Beta", "Corp"]


def test_a_term_bound_columns_own_iri_matches_nothing(con):
    assert _answer(con, "SELECT ?n WHERE { ?s p:name ?n }", _GROUNDING) == []


def test_an_unbound_column_is_read_under_its_own_iri_beside_a_term_property(con):
    rows = _answer(con, "SELECT ?n ?a WHERE { ?s term:Legal%20name ?n ; p:age ?a }", _GROUNDING)
    assert sorted((r["n"], r["a"]) for r in rows) == [("Ann", 41), ("Bo", 35), ("Cy", 29)]


def test_a_construct_writes_the_term_property_for_every_class(con):
    rows = _answer(
        con,
        "CONSTRUCT { ?s term:Legal%20name ?n } WHERE { ?s term:Legal%20name ?n }",
        _GROUNDING,
    )
    subjects = {r["s"] for r in rows}
    assert ann() in subjects and ACME in subjects
    assert len(rows) == 7


def test_a_subjects_triples_carry_the_term_property_not_the_columns_own(con):
    rows = _answer(con, f"CONSTRUCT {{ <{ann()}> ?p ?o }} WHERE {{ <{ann()}> ?p ?o }}", _GROUNDING)
    got = {(r["p"], r["o"]) for r in rows}
    assert (FULL_NAME, "Ann") in got and (AGE, 41) in got
    assert all(p != NAME for p, _ in got)


def test_open_as_sparql_writes_a_term_bound_property_under_the_term(con):
    cypher = "MATCH (p:Person) RETURN p.name, p.age"
    lm = _model()
    sparql = ir_to_sparql(parse_cypher(cypher), build_vocabulary(lm, BASE, _TYPES, _GROUNDING), lm)
    assert f"<{FULL_NAME}>" in sparql and NAME not in sparql
    got = [tuple(r.values()) for r in _answer(con, sparql, _GROUNDING)]
    assert sorted(got, key=repr) == sorted(_cypher_rows(con, cypher), key=repr)
