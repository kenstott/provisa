# Copyright (c) 2026 Kenneth Stott
# Canary: e99862a3-8705-4640-a2fc-3311eb3122c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GET /data/sparql/vocabulary (REQ-2135, VOCABULARY ENDPOINT): the caller's role's vocabulary as
IRIs, for a client to offer as a query is written -- its base and prefixes; each class, a table's
own or a Class term, with the labels it types; each predicate, a column's own or a Property term,
with its label, property and datatype; each edge type with its source and target; and the live
Concept terms."""

# Requirements: REQ-2135

from __future__ import annotations

from provisa.sparql_query.vocabulary import (
    RDF,
    XSD,
    build_vocabulary,
    concepts_of,
    superclasses_of,
    term_iri,
    vocabulary_document,
)
from tests.unit.test_sparql_compiler import (
    AGE,
    BASE,
    COMPANY,
    EMPLOYEE,
    FULL_NAME,
    KNOWS,
    NAME,
    PERSON,
    WORKS_AT,
    _GROUNDING,
    _TYPES,
    _model,
)


def _doc(concepts: list[dict] | None = None, superclasses: dict | None = None) -> dict:
    vocab = build_vocabulary(_model(), BASE, _TYPES, _GROUNDING)
    return vocabulary_document(vocab, concepts or [], superclasses or {})


def test_the_vocabulary_names_its_base_and_prefixes():
    doc = _doc()
    assert doc["base"] == BASE
    assert doc["prefixes"]["rdf"] == RDF
    assert doc["prefixes"]["hr"] == f"{BASE}/vocab/hr/"
    assert doc["prefixes"]["rel"] == f"{BASE}/vocab/rel/"
    assert doc["prefixes"]["term"] == f"{BASE}/vocab/term/"


def test_each_class_is_a_tables_own_or_a_class_term_with_the_labels_it_types():
    classes = {c["iri"]: c for c in _doc()["classes"]}
    assert classes[PERSON] == {
        "iri": PERSON,
        "kind": "table",
        "labels": ["Person"],
        "superclasses": [],
    }
    assert classes[COMPANY] == {
        "iri": COMPANY,
        "kind": "table",
        "labels": ["Company"],
        "superclasses": [],
    }
    # A Class term with no KIND_OF edge has no superclass.
    assert classes[EMPLOYEE] == {
        "iri": EMPLOYEE,
        "kind": "term",
        "labels": ["Person"],
        "superclasses": [],
    }


def test_each_predicate_is_a_columns_own_or_a_property_term_with_its_datatype():
    predicates = _doc()["predicates"]
    assert {
        "iri": NAME,
        "kind": "column",
        "label": "Person",
        "property": "name",
        "datatype": f"{XSD}string",
    } in predicates
    assert {
        "iri": AGE,
        "kind": "column",
        "label": "Person",
        "property": "age",
        "datatype": f"{XSD}integer",
    } in predicates
    # A Property term is the predicate of every column it grounds.
    by_term = [p for p in predicates if p["iri"] == FULL_NAME]
    assert sorted((p["label"], p["property"], p["kind"]) for p in by_term) == [
        ("Company", "name", "term"),
        ("Person", "name", "term"),
    ]


def test_each_edge_type_with_its_source_and_target():
    edges = _doc()["edges"]
    assert {"iri": KNOWS, "type": "KNOWS", "source": "Person", "target": "Person"} in edges
    assert {"iri": WORKS_AT, "type": "WORKS_AT", "source": "Person", "target": "Company"} in edges


def test_the_live_concept_terms_are_offered_and_nothing_else_of_the_glossary():
    rows = [
        {
            "name": "customer lifetime",
            "definition": "How long a customer stays.",
            "is_abstract": True,
            "live": True,
        },
        {"name": "draft idea", "definition": None, "is_abstract": True, "live": False},
        {
            "name": "party",
            "definition": "Someone we deal with.",
            "is_abstract": False,
            "live": True,
        },
    ]
    concepts = concepts_of(BASE, rows)
    assert concepts == [
        {
            "iri": term_iri(BASE, "customer lifetime"),
            "name": "customer lifetime",
            "definition": "How long a customer stays.",
        }
    ]
    assert _doc(concepts)["concepts"] == concepts


def test_a_class_term_names_the_terms_it_is_a_kind_of_that_the_role_reads():
    # REQ-2135 (VISUALIZER VOCABULARY): a Class term is shown with the classes it is a KIND_OF.
    # An edge to a term the role does not read, or another edge type, is no superclass.
    readable = [{"id": 1, "name": "Employee"}, {"id": 2, "name": "Person"}]
    edges = [(1, 2, "KIND_OF"), (1, 3, "KIND_OF"), (2, 1, "PART_OF")]
    supers = superclasses_of(BASE, readable, edges)
    assert supers == {term_iri(BASE, "Employee"): [term_iri(BASE, "Person")]}
    classes = {c["iri"]: c for c in _doc(superclasses=supers)["classes"]}
    assert classes[EMPLOYEE]["superclasses"] == [term_iri(BASE, "Person")]
    # A table's own class has none.
    assert classes[PERSON]["superclasses"] == []
