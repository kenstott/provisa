# Copyright (c) 2026 Kenneth Stott
# Canary: 71d4e9a3-0b6c-4f28-a5e1-3c8f2d7b9e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2237: an ontology file read into items and the structure between them (Apache Ossie YAML,
Turtle, RDF/XML, N-Triples, JSON-LD), fetching nothing; and the alignments proposed between each
item and the glossary's terms, by name, by the glossary's own normalization and by synonym."""

# Requirements: REQ-2237

from __future__ import annotations

import json

import pytest

from provisa.ontology_import import align, parse
from provisa.ontology_import.align import Proposal, Term
from provisa.ontology_import.parse import Edge, ImportRefused

OSSIE = b"""
version: 0.3.0.dev0
name: Banking
prefixes:
  fibo: https://spec.edmcouncil.org/fibo/ontology/
ontology:
- concept: Party
  type: EntityType
  iri: fibo:Party
  description: A person or organization that takes part in an agreement.
- concept: Customer
  type: EntityType
  extends: [ Party ]
  iri: https://spec.edmcouncil.org/fibo/ontology/Customer
  description: A party that receives goods or services.
  relationships:
  - name: email
    roles:
    - concept: EmailAddress
    verbalizes: [ '{Customer} has {EmailAddress}' ]
    description: The address a customer is written to at.
- concept: EmailAddress
  type: ValueType
  extends: [ String ]
- concept: CustomerEmail
  type: ValueType
  extends: [ EmailAddress ]
"""


def _names(parsed: parse.Parsed) -> list[str]:
    return [i.name for i in parsed.items]


def _edges(parsed: parse.Parsed) -> set[tuple[str, str, str]]:
    names = _names(parsed)
    return {(names[e.source], e.rel_type, names[e.target]) for e in parsed.edges}


# -- Apache Ossie ---------------------------------------------------------------------------------


def test_an_ossie_concept_is_an_item_and_its_relationship_a_property_of_it():
    parsed = parse.parse("banking.yaml", OSSIE)
    assert parsed.title == "Banking"
    assert parsed.format == parse.OSSIE
    by_name = {i.name: i for i in parsed.items}
    assert set(by_name) == {"party", "customer", "customer email", "email address"}
    assert by_name["party"].kind == parse.CLASS
    assert by_name["email address"].kind == parse.VALUE
    # A QName expands over the declared prefixes; a full IRI is kept as it is.
    assert by_name["party"].iri == "https://spec.edmcouncil.org/fibo/ontology/Party"
    assert by_name["customer"].iri == "https://spec.edmcouncil.org/fibo/ontology/Customer"
    assert by_name["party"].definition == (
        "A person or organization that takes part in an agreement."
    )
    # Customer.email and the value type CustomerEmail are one phrase, so one item.
    email = by_name["customer email"]
    assert email.kind == parse.PROPERTY
    assert email.keys == ["Customer.email", "CustomerEmail"]
    assert email.definition == "The address a customer is written to at."
    assert _edges(parsed) == {
        ("customer", "KIND_OF", "party"),
        ("customer email", "PART_OF", "customer"),
        ("customer email", "KIND_OF", "email address"),
    }
    # String is built in: extending it states nothing about the file's items.
    assert parsed.left_out == {}


def test_an_ossie_draft_that_nests_a_concepts_fields_under_concept_is_read_alike():
    nested = b"""
name: Old
ontology:
  - concept:
      name: SocialSecurityNr
      type: ValueType
      extends: [Integer]
  - concept:
      name: Employee
      type: EntityType
      extends: [Person]
  - concept:
      name: Person
      type: EntityType
"""
    parsed = parse.parse("old.yml", nested)
    assert _names(parsed) == ["social security nr", "employee", "person"]
    assert _edges(parsed) == {("employee", "KIND_OF", "person")}


def test_an_ossie_document_given_as_json_is_read_as_ossie():
    doc = {"name": "J", "ontology": [{"concept": "Account", "type": "EntityType"}]}
    parsed = parse.parse("j.json", json.dumps(doc).encode())
    assert parsed.format == parse.OSSIE
    assert _names(parsed) == ["account"]


def test_a_concept_of_no_known_type_is_refused_naming_it():
    with pytest.raises(ImportRefused, match="'Thing''s type is EntityType or ValueType"):
        parse.parse("x.yaml", b"name: X\nontology:\n- concept: Thing\n  type: Widget\n")


def test_a_file_that_is_no_ossie_ontology_is_refused():
    with pytest.raises(ImportRefused, match="not an Apache Ossie ontology"):
        parse.parse("x.yaml", b"name: X\ndatasets: []\n")


def test_a_file_that_declares_nothing_is_refused():
    with pytest.raises(ImportRefused, match="declares no class, property or concept"):
        parse.parse("x.yaml", b"name: X\nontology: []\n")


def test_an_extension_names_the_format_and_an_unknown_one_asks_for_it():
    assert parse.format_of("fibo.ttl", None) == parse.TURTLE
    assert parse.format_of("fibo.owl", None) == parse.RDF_XML
    assert parse.format_of("fibo.rdf", None) == parse.RDF_XML
    assert parse.format_of("fibo.nt", None) == parse.N_TRIPLES
    assert parse.format_of("fibo.jsonld", None) == parse.JSON_LD
    assert parse.format_of("anything.txt", parse.TURTLE) == parse.TURTLE
    with pytest.raises(ImportRefused, match="names no ontology format"):
        parse.format_of("fibo.txt", None)
    with pytest.raises(ImportRefused, match="format is one of"):
        parse.format_of("fibo.ttl", "owl-functional")


# -- RDF / OWL ------------------------------------------------------------------------------------

TURTLE = b"""
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix ex: <https://example.org/fibo#> .

ex:onto a owl:Ontology ; rdfs:label "Mini FIBO"@en .
ex:Party a owl:Class ; rdfs:label "Party"@en , "Partei"@de ;
    skos:definition "Eine Person."@de , "A person or organization."@en .
ex:Customer a owl:Class ; rdfs:subClassOf ex:Party , ex:Elsewhere ;
    skos:altLabel "client"@en ;
    rdfs:comment "Only a comment."@en ;
    skos:definition "A party that buys."@en .
ex:hasEmailAddress a owl:DatatypeProperty ; rdfs:domain ex:Customer .
ex:Account a owl:Class ; rdfs:subClassOf [ a owl:Restriction ] .
ex:Client a owl:Class ; owl:equivalentClass ex:Customer .
ex:Retail a skos:Concept ; skos:prefLabel "retail banking"@en ; skos:broader ex:Banking .
ex:Banking a skos:Concept ; skos:related ex:Retail .
owl:Thing a owl:Class .
"""


def test_turtle_classes_properties_and_concepts_are_items_with_their_own_text():
    parsed = parse.parse("mini.ttl", TURTLE)
    assert parsed.title == "Mini FIBO"
    by_name = {i.name: i for i in parsed.items}
    assert set(by_name) == {
        "party",
        "customer",
        "has email address",
        "account",
        "client",
        "retail banking",
        "banking",
    }
    # The English text is preferred; a definition over a comment.
    assert by_name["party"].definition == "A person or organization."
    assert by_name["customer"].definition == "A party that buys."
    assert by_name["customer"].labels == ["client"]
    assert by_name["customer"].iri == "https://example.org/fibo#Customer"
    assert by_name["has email address"].kind == parse.PROPERTY
    assert by_name["retail banking"].kind == parse.CONCEPT
    assert _edges(parsed) == {
        ("customer", "KIND_OF", "party"),
        ("has email address", "PART_OF", "customer"),
        ("client", "SYNONYM_OF", "customer"),
        ("retail banking", "KIND_OF", "banking"),
        ("banking", "RELATED_TO", "retail banking"),
    }
    # Customer ⊑ Elsewhere names a class the file does not declare; Account ⊑ [restriction] an
    # anonymous one. Both are counted, never guessed at.
    assert parsed.left_out == {
        "statement about something the file does not declare": 1,
        "statement about an anonymous class": 1,
    }


def _serialized(fmt: str) -> bytes:
    from rdflib import Graph

    graph = Graph()
    graph.parse(data=TURTLE.decode(), format="turtle")
    return graph.serialize(format=fmt).encode()


@pytest.mark.parametrize(
    "file_name, rdflib_format",
    [("mini.rdf", "xml"), ("mini.nt", "nt"), ("mini.jsonld", "json-ld")],
)
def test_rdf_xml_n_triples_and_json_ld_read_as_turtle_does(file_name, rdflib_format):
    from_turtle = parse.parse("mini.ttl", TURTLE)
    parsed = parse.parse(file_name, _serialized(rdflib_format))
    assert {(i.name, i.kind, i.iri, i.definition) for i in parsed.items} == {
        (i.name, i.kind, i.iri, i.definition) for i in from_turtle.items
    }
    assert _edges(parsed) == _edges(from_turtle)


@pytest.mark.parametrize(
    "context",
    [
        "https://example.org/context.jsonld",
        [{"@vocab": "https://example.org/"}, "https://example.org/more.jsonld"],
        {"@import": "https://example.org/base.jsonld"},
    ],
)
def test_a_json_ld_document_naming_a_remote_context_is_refused_before_anything_is_fetched(
    context,
):
    doc = {"@context": context, "@id": "https://example.org/x", "@type": "owl:Class"}
    with pytest.raises(ImportRefused, match="remote JSON-LD context .*fetches nothing"):
        parse.parse("x.jsonld", json.dumps(doc).encode())


def test_rdf_xml_resolves_no_external_entity(tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET")
    xml = f"""<?xml version="1.0"?>
<!DOCTYPE rdf:RDF [ <!ENTITY leak SYSTEM "file://{secret}"> ]>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#">
  <owl:Class rdf:about="https://example.org/Thing">
    <rdfs:comment>&leak;</rdfs:comment>
  </owl:Class>
</rdf:RDF>
"""
    parsed = parse.parse("x.rdf", xml.encode())
    assert all("TOP-SECRET" not in (i.definition or "") for i in parsed.items)


def test_text_that_is_not_the_format_named_is_refused_naming_the_file():
    with pytest.raises(ImportRefused, match="'broken.ttl' is not turtle RDF"):
        parse.parse("broken.ttl", b"this is not turtle .")


# -- the alignments proposed ------------------------------------------------------------------------


def _items(*specs: tuple[str, str]) -> list[parse.Item]:
    return [parse.Item(name, kind, [name]) for name, kind in specs]


def _proposed(items, terms, edges=()) -> set[tuple[str, str, bool, str, int | None]]:
    return {
        (items[p.item].name, p.rel_type, p.item_is_source, p.basis, p.term_id)
        for p in align.propose(items, list(edges), terms)
        if p.term_id is not None
    }


def test_an_item_named_as_a_term_is_proposed_as_the_same_entity_plural_or_not():
    items = _items(("customer", parse.CLASS), ("accounts", parse.CLASS))
    terms = [Term(1, "customer", "class", None), Term(2, "account", "property", None)]
    assert _proposed(items, terms) == {
        ("customer", "SAME_AS", True, align.NAME, 1),
        ("accounts", "SAME_AS", True, align.NAME, 2),
    }


def test_an_item_carrying_a_terms_iri_is_that_term_and_nothing_else_is_proposed():
    item = parse.Item("client", parse.CLASS, ["x"], iri="https://example.org/Customer")
    terms = [
        Term(1, "customer", "class", "https://example.org/Customer"),
        Term(2, "client", "concept", None),
    ]
    assert _proposed([item], terms) == {("client", "SAME_AS", True, align.IRI, 1)}


def test_the_glossarys_own_normalization_proposes_a_synonym():
    # cust_id derives the term "customer"; an imported CustomerIdentifier normalizes to it.
    items = _items(("customer identifier", parse.CLASS), ("txn", parse.CLASS))
    terms = [Term(1, "customer", "property", None), Term(2, "transaction", "class", None)]
    assert _proposed(items, terms) == {
        ("customer identifier", "SYNONYM_OF", True, align.NORMALIZED_NAME, 1),
        ("txn", "SYNONYM_OF", True, align.NORMALIZED_NAME, 2),
    }


def test_an_alternative_label_naming_a_term_proposes_a_synonym():
    item = parse.Item("client", parse.CLASS, ["x"], labels=["customer"])
    terms = [Term(1, "customer", "class", None)]
    assert _proposed([item], terms) == {("client", "SYNONYM_OF", True, align.SYNONYM, 1)}


def test_a_property_qualified_by_the_class_it_describes_is_the_term_the_glossary_derives():
    items = _items(("person", parse.CLASS), ("has name", parse.PROPERTY))
    edges = [Edge(1, "PART_OF", 0)]
    terms = [Term(1, "person name", "property", None)]
    assert ("has name", "SAME_AS", True, align.QUALIFIED_NAME, 1) in _proposed(items, terms, edges)


def test_a_head_noun_proposes_kind_of_in_the_direction_it_reads():
    items = _items(("customer", parse.CLASS), ("retail customer", parse.CLASS))
    terms = [Term(1, "enterprise customer", "class", None), Term(2, "customer", "class", None)]
    proposed = _proposed(items, terms)
    # An enterprise customer is a kind of the imported customer, which reaches its data.
    assert ("customer", "KIND_OF", False, align.HEAD_NOUN, 1) in proposed
    # An imported retail customer is a kind of the glossary's customer.
    assert ("retail customer", "KIND_OF", True, align.HEAD_NOUN, 2) in proposed


def test_a_property_term_a_class_qualifies_is_proposed_part_of_it():
    items = _items(("customer", parse.CLASS), ("email", parse.PROPERTY))
    terms = [
        Term(1, "customer email", "property", None),
        Term(2, "customer segment", "class", None),
    ]
    proposed = _proposed(items, terms)
    assert ("customer", "PART_OF", False, align.QUALIFIER, 1) in proposed
    # Only a Property describes the class it is qualified by.
    assert not any(p[4] == 2 and p[3] == align.QUALIFIER for p in proposed)
    # A property proposes no kind of by its head.
    assert not any(p[0] == "email" and p[3] == align.HEAD_NOUN for p in proposed)


def test_one_item_proposes_at_most_one_alignment_with_one_term_the_strongest():
    item = parse.Item("customer", parse.CLASS, ["x"], labels=["customers"])
    terms = [Term(1, "customer", "class", None)]
    (only,) = align.propose([item], [], terms)
    assert (only.rel_type, only.basis) == ("SAME_AS", align.NAME)


def test_the_files_own_structure_is_proposed_between_its_items():
    items = _items(("customer", parse.CLASS), ("party", parse.CLASS))
    (proposal,) = align.propose(items, [Edge(0, "KIND_OF", 1)], [])
    assert proposal == Proposal(0, "KIND_OF", True, align.ONTOLOGY, other_item=1)
