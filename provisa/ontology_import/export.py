# Copyright (c) 2026 Kenneth Stott
# Canary: b6f02d94-1e7a-4c58-8d3b-59a7e1c0f2d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An imported ontology written back out as RDF, so it round-trips (REQ-2237).

The terms an import applied, each by its IRI, as the glossary holds them now:

- every statement kept on the term (provisa.ontology_import.kept), as it was read -- its
  declarations, restrictions and cardinalities, disjointness, characteristics, property chains,
  individuals, ranges and annotations;
- each glossary edge between two of them, as the RDF it was read from: ``KIND_OF`` is
  ``rdfs:subClassOf`` (``rdfs:subPropertyOf`` from a property, ``skos:broader`` from a SKOS
  concept), ``SYNONYM_OF`` ``owl:equivalentClass`` (``owl:equivalentProperty``), ``PART_OF`` a
  property's ``rdfs:domain``, ``RELATED_TO`` ``skos:related`` and ``VALID_VALUE_OF`` the
  individual's ``rdf:type``. An edge of another type is no RDF an import reads, and is counted;
- its definition as ``skos:definition`` when no kept statement says it already, and a term that
  keeps no declaration is an ``owl:Class`` labelled with its name;
- REQ-2243: its calculation method as ``provisa:calculationMethod``, a plain literal.

A term with no IRI is no RDF resource and is counted, never given one here.
"""

# Requirements: REQ-2237, REQ-2243

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from rdflib import OWL, RDF, RDFS, SKOS, Graph, Literal, Namespace, URIRef

from provisa.ontology_import import kept
from provisa.ontology_import.parse import CALCULATION_METHOD, PROVISA

_PROVISA = Namespace(PROVISA)
_CALCULATION_METHOD = URIRef(CALCULATION_METHOD)

#: rdflib's name for each format an export writes, by the name an import reads it under.
FORMATS = {"turtle": "turtle", "xml": "xml", "nt": "nt", "json-ld": "json-ld"}
MEDIA_TYPES = {
    "turtle": "text/turtle",
    "xml": "application/rdf+xml",
    "nt": "application/n-triples",
    "json-ld": "application/ld+json",
}
_PROPERTY_TYPES = frozenset({str(OWL.ObjectProperty), str(OWL.DatatypeProperty), str(RDF.Property)})


@dataclass
class Exported:
    graph: Graph
    #: What could not be written as RDF, counted by why.
    left_out: dict[str, int] = field(default_factory=dict)


def _declared(statements: Iterable[dict[str, Any]]) -> set[str]:
    return {
        s["object"].get("iri", "")
        for s in statements
        if s.get("predicate") == str(RDF.type) and "object" in s
    }


def _predicate(rel_type: str, declared: set[str]) -> URIRef | None:
    is_property = bool(declared & _PROPERTY_TYPES)
    if rel_type == "KIND_OF":
        if is_property:
            return RDFS.subPropertyOf
        if str(SKOS.Concept) in declared:
            return SKOS.broader
        return RDFS.subClassOf
    if rel_type == "SYNONYM_OF":
        return OWL.equivalentProperty if is_property else OWL.equivalentClass
    return {
        "PART_OF": RDFS.domain,
        "RELATED_TO": SKOS.related,
        "VALID_VALUE_OF": RDF.type,
    }.get(rel_type)


def graph_of(
    terms: Sequence[Mapping[str, Any]],
    edges: Iterable[tuple[int, int, str]],
    statements: Mapping[int, list[dict[str, Any]]],
) -> Exported:
    """``terms`` (``id``, ``name``, ``iri``, ``definition``, ``calculation_method``), the ``edges``
    between them and the
    ``statements`` kept on each, as one RDF graph (module docstring)."""
    graph = Graph()
    for prefix, namespace in (("owl", OWL), ("rdfs", RDFS), ("skos", SKOS), ("provisa", _PROVISA)):
        graph.bind(prefix, namespace)
    out = Exported(graph)

    def leave_out(why: str) -> None:
        out.left_out[why] = out.left_out.get(why, 0) + 1

    iris: dict[int, URIRef] = {}
    for term in terms:
        if not term.get("iri"):
            leave_out("term with no IRI")
            continue
        iris[term["id"]] = URIRef(term["iri"])
    for term in terms:
        subject = iris.get(term["id"])
        if subject is None:
            continue
        held = statements.get(term["id"], [])
        kept.write(graph, held)
        if not any(
            s.get("subject") == str(subject) and s["kind"] == kept.DECLARATION for s in held
        ):
            graph.add((subject, RDF.type, OWL.Class))
            graph.add((subject, RDFS.label, Literal(term["name"])))
        definition = (term.get("definition") or "").strip()
        if definition and not any(
            s.get("subject") == str(subject) and s.get("object", {}).get("value") == definition
            for s in held
        ):
            graph.add((subject, SKOS.definition, Literal(definition)))
        method = (term.get("calculation_method") or "").strip()
        if method:
            graph.add((subject, _CALCULATION_METHOD, Literal(method)))
    for source, target, rel_type in edges:
        if source not in iris or target not in iris:
            continue
        predicate = _predicate(rel_type, _declared(statements.get(source, [])))
        if predicate is None:
            leave_out(f"{rel_type} edge")
            continue
        graph.add((iris[source], predicate, iris[target]))
    return out


def serialize(exported: Exported, fmt: str) -> bytes:
    if fmt not in FORMATS:
        raise ValueError(f"an ontology is exported as one of {', '.join(FORMATS)}")
    return exported.graph.serialize(format=FORMATS[fmt], encoding="utf-8")
