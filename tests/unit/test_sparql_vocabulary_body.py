# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/sparql/vocabulary (REQ-2135, REQ-2141): the role's vocabulary as IRIs -- its prefixes,
each class (a table's own, a Class term), each predicate (a column's own, a Property term), each
edge type and each live Concept -- for a client to offer as it writes a query."""

# Requirements: REQ-2135, REQ-2141

from provisa.api.rest.sparql_router import vocabulary_body
from provisa.sparql_query.vocabulary import (
    XSD,
    build_vocabulary,
    column_iri,
    edge_iri,
    table_class_iri,
    term_iri,
)
from tests.unit.test_sparql_compiler import BASE, _GROUNDING, _TYPES, _model


def test_the_vocabulary_names_every_class_predicate_edge_and_concept_by_iri():
    body = vocabulary_body(build_vocabulary(_model(), BASE, _TYPES, _GROUNDING), ["Counterparty"])
    assert body["base"] == BASE
    assert body["prefixes"]["rel"] == f"{BASE}/vocab/rel/"
    assert body["prefixes"]["hr"] == f"{BASE}/vocab/hr/"
    classes = {c["iri"]: c for c in body["classes"]}
    assert classes[table_class_iri(BASE, "hr", "persons")] == {
        "iri": table_class_iri(BASE, "hr", "persons"),
        "kind": "table",
        "labels": ["Person"],
        "term": None,
    }
    assert classes[term_iri(BASE, "Employee")]["kind"] == "class"
    assert classes[term_iri(BASE, "Employee")]["term"] == "Employee"
    predicates = {(p["iri"], p["label"]): p for p in body["predicates"]}
    age = predicates[(column_iri(BASE, "hr", "persons", "age"), "Person")]
    assert (age["kind"], age["property"], age["datatype"]) == ("column", "age", f"{XSD}integer")
    legal = predicates[(term_iri(BASE, "Legal name"), "Company")]
    assert (legal["kind"], legal["term"], legal["property"]) == ("property", "Legal name", "name")
    assert {
        "iri": edge_iri(BASE, "WORKS_AT"),
        "rel_type": "WORKS_AT",
        "source": "Person",
        "target": "Company",
    } in body["edges"]
    assert body["concepts"] == [{"iri": term_iri(BASE, "Counterparty"), "term": "Counterparty"}]
