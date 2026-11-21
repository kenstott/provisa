# Copyright (c) 2026 Kenneth Stott
# Canary: 7d41c2e8-9b06-4f3a-a5d1-2e8c60b7f914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an imported ontology states that Provisa's terms do not model, kept on the term (REQ-2237).

A term models a name, a definition, an IRI and the glossary's edges. Everything else an
ontology states about the thing a term is -- restrictions and cardinalities, disjointness,
inverse and transitive properties, property chains, named individuals with properties of their
own, datatype ranges, an object property's domain and range, and annotations (sources,
editorial notes, labels by language) -- is kept as *statements* on the term, shown on its page
and written back out on export, so an imported ontology round-trips. Nothing here reasons over
them.

A statement is one RDF triple about an IRI, its object written as JSON so that a blank node
(a restriction, a datatype restriction) and an RDF list (a property chain, an enumeration) keep
their whole structure:

- ``{"iri": "..."}`` -- a named resource;
- ``{"value": "...", "lang": "fr"}`` or ``{"value": "1", "datatype": "..."}`` -- a literal;
- ``{"list": [<object>, ...]}`` -- an RDF list;
- ``{"node": [["<predicate>", <object>], ...]}`` -- a blank node with its own statements.

A statement is ``{"kind", "subject", "predicate", "object"}``; an axiom whose subject is itself a
blank node (``owl:AllDisjointClasses``, an annotated ``owl:Axiom``) is ``{"kind", "axiom":
<node>}``. ``kind`` says what it is for a reader: one of :data:`KINDS`.
"""

# Requirements: REQ-2237

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

from rdflib import OWL, RDF, RDFS, SKOS, XSD, BNode, Graph, Literal, URIRef
from rdflib.collection import Collection
from rdflib.namespace import DC, DCTERMS

RESTRICTION = "restriction"
CARDINALITY = "cardinality"
CLASS_EXPRESSION = "class_expression"
DISJOINTNESS = "disjointness"
INVERSE = "inverse"
CHARACTERISTIC = "characteristic"
PROPERTY_CHAIN = "property_chain"
INDIVIDUAL = "individual"
DOMAIN = "domain"
RANGE = "range"
DATATYPE_RANGE = "datatype_range"
ANNOTATION = "annotation"
DECLARATION = "declaration"
STATEMENT = "statement"
KINDS = (
    RESTRICTION,
    CARDINALITY,
    CLASS_EXPRESSION,
    DISJOINTNESS,
    INVERSE,
    CHARACTERISTIC,
    PROPERTY_CHAIN,
    INDIVIDUAL,
    DOMAIN,
    RANGE,
    DATATYPE_RANGE,
    ANNOTATION,
    DECLARATION,
    STATEMENT,
)

_CARDINALITIES = frozenset(
    {
        OWL.cardinality,
        OWL.minCardinality,
        OWL.maxCardinality,
        OWL.qualifiedCardinality,
        OWL.minQualifiedCardinality,
        OWL.maxQualifiedCardinality,
    }
)
_CHARACTERISTICS = frozenset(
    {
        OWL.TransitiveProperty,
        OWL.SymmetricProperty,
        OWL.AsymmetricProperty,
        OWL.FunctionalProperty,
        OWL.InverseFunctionalProperty,
        OWL.ReflexiveProperty,
        OWL.IrreflexiveProperty,
    }
)
_DISJOINT = frozenset(
    {OWL.disjointWith, OWL.disjointUnionOf, OWL.propertyDisjointWith, OWL.complementOf}
)
_DISJOINT_AXIOMS = frozenset({OWL.AllDisjointClasses, OWL.AllDisjointProperties})
#: Annotation vocabularies: a predicate in one of them annotates, whatever its object.
_ANNOTATION_NAMESPACES = (str(SKOS), str(DC), str(DCTERMS))
_ANNOTATION_PREDICATES = frozenset(
    {
        RDFS.label,
        RDFS.comment,
        RDFS.seeAlso,
        RDFS.isDefinedBy,
        OWL.versionInfo,
        OWL.deprecated,
        OWL.priorVersion,
        OWL.backwardCompatibleWith,
        OWL.incompatibleWith,
    }
)
_DATATYPE_NAMESPACE = str(XSD)
_LITERAL_RANGES = frozenset({RDFS.Literal, RDF.PlainLiteral, RDF.langString, RDF.XMLLiteral})


class BlankNodeCycle(ValueError):
    """A blank node that reaches itself: no OWL construct does, so it is no statement."""


def is_annotation(graph: Graph, predicate: Any) -> bool:
    """``predicate`` annotates: a known annotation vocabulary, or declared an annotation
    property by the ontology itself."""
    return (
        predicate in _ANNOTATION_PREDICATES
        or str(predicate).startswith(_ANNOTATION_NAMESPACES)
        or (predicate, RDF.type, OWL.AnnotationProperty) in graph
    )


# -- RDF to JSON -------------------------------------------------------------------------------


def _sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def _is_list(graph: Graph, node: Any) -> bool:
    return isinstance(node, BNode) and (node, RDF.first, None) in graph


def encode(graph: Graph, node: Any, seen: frozenset = frozenset()) -> dict[str, Any]:
    """``node`` as a statement's object (module docstring)."""
    if isinstance(node, URIRef):
        return {"iri": str(node)}
    if isinstance(node, Literal):
        out: dict[str, Any] = {"value": str(node)}
        if node.language:
            out["lang"] = node.language
        elif node.datatype is not None:
            out["datatype"] = str(node.datatype)
        return out
    if node in seen:
        raise BlankNodeCycle(str(node))
    inner = seen | {node}
    if _is_list(graph, node):
        return {"list": [encode(graph, member, inner) for member in Collection(graph, node)]}
    pairs = [
        [str(predicate), encode(graph, value, inner)]
        for predicate, value in graph.predicate_objects(node)
    ]
    return {"node": sorted(pairs, key=_sort_key)}


def iris_in(value: Any) -> Iterator[str]:
    """Every IRI a statement's object names, depth first."""
    if isinstance(value, dict):
        if "iri" in value:
            yield value["iri"]
        for member in value.get("list", []):
            yield from iris_in(member)
        for predicate, member in value.get("node", []):
            yield predicate
            yield from iris_in(member)


# -- kinds -------------------------------------------------------------------------------------


def _node_types(value: dict[str, Any]) -> set[str]:
    return {
        member.get("iri", "")
        for predicate, member in value.get("node", [])
        if predicate == str(RDF.type)
    }


def _node_predicates(value: dict[str, Any]) -> set[str]:
    return {predicate for predicate, _ in value.get("node", [])}


def kind_of(graph: Graph, predicate: Any, value: dict[str, Any]) -> str:
    """What the statement ``<subject> predicate value`` is, for a reader of the term."""
    if predicate == RDF.type:
        return CHARACTERISTIC if URIRef(value.get("iri", "")) in _CHARACTERISTICS else DECLARATION
    if predicate in (RDFS.subClassOf, OWL.equivalentClass) and "node" in value:
        if str(OWL.Restriction) in _node_types(value):
            cardinal = {str(p) for p in _CARDINALITIES}
            return CARDINALITY if _node_predicates(value) & cardinal else RESTRICTION
        return CLASS_EXPRESSION
    if predicate in _DISJOINT:
        return DISJOINTNESS
    if predicate == OWL.inverseOf:
        return INVERSE
    if predicate == OWL.propertyChainAxiom:
        return PROPERTY_CHAIN
    if predicate == RDFS.domain:
        return DOMAIN
    if predicate == RDFS.range:
        iri = value.get("iri")
        if iri is not None and (
            iri.startswith(_DATATYPE_NAMESPACE) or URIRef(iri) in _LITERAL_RANGES
        ):
            return DATATYPE_RANGE
        if str(RDFS.Datatype) in _node_types(value):
            return DATATYPE_RANGE
        return RANGE
    if is_annotation(graph, predicate) or "value" in value:
        return ANNOTATION
    return STATEMENT


def axiom_kind(value: dict[str, Any]) -> str:
    """What a blank-subject axiom is: disjointness among several, else an annotated axiom or
    another statement."""
    types = {URIRef(t) for t in _node_types(value)}
    if types & _DISJOINT_AXIOMS:
        return DISJOINTNESS
    if OWL.Axiom in types:
        return ANNOTATION
    return STATEMENT


def statement(graph: Graph, subject: Any, predicate: Any, value: Any) -> dict[str, Any]:
    """The triple ``subject predicate value`` as a kept statement."""
    obj = encode(graph, value)
    return {
        "kind": kind_of(graph, predicate, obj),
        "subject": str(subject),
        "predicate": str(predicate),
        "object": obj,
    }


def axiom(graph: Graph, node: Any) -> dict[str, Any]:
    """The blank-subject axiom ``node`` as a kept statement."""
    value = encode(graph, node)
    return {"kind": axiom_kind(value), "axiom": value}


# -- JSON to RDF -------------------------------------------------------------------------------


def decode(graph: Graph, value: dict[str, Any]) -> Any:
    """The RDF node ``value`` writes, its blank structure added to ``graph``."""
    if "iri" in value:
        return URIRef(value["iri"])
    if "value" in value:
        datatype = value.get("datatype")
        # Not normalized: the literal is written as the ontology spelled it ("0", not "0.0").
        return Literal(
            value["value"],
            lang=value.get("lang"),
            datatype=URIRef(datatype) if datatype else None,
            normalize=False,
        )
    if "list" in value:
        if not value["list"]:
            return RDF.nil
        head = BNode()
        Collection(graph, head, [decode(graph, member) for member in value["list"]])
        return head
    if "node" in value:
        node = BNode()
        for predicate, member in value["node"]:
            graph.add((node, URIRef(predicate), decode(graph, member)))
        return node
    raise ValueError(f"a kept statement's object is an iri, a value, a list or a node: {value!r}")


def write(graph: Graph, statements: Iterable[dict[str, Any]]) -> None:
    """Add every kept statement to ``graph``."""
    for kept in statements:
        if "axiom" in kept:
            decode(graph, kept["axiom"])
            continue
        graph.add(
            (URIRef(kept["subject"]), URIRef(kept["predicate"]), decode(graph, kept["object"]))
        )


def _valid_object(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if set(value) == {"iri"}:
        return isinstance(value["iri"], str) and bool(value["iri"])
    if "value" in value:
        return (
            isinstance(value["value"], str)
            and set(value) <= {"value", "lang", "datatype"}
            and not ("lang" in value and "datatype" in value)
            and all(isinstance(value[k], str) for k in value)
        )
    if set(value) == {"list"}:
        return isinstance(value["list"], list) and all(_valid_object(m) for m in value["list"])
    if set(value) == {"node"}:
        return isinstance(value["node"], list) and all(
            isinstance(pair, list)
            and len(pair) == 2
            and isinstance(pair[0], str)
            and _valid_object(pair[1])
            for pair in value["node"]
        )
    return False


def valid(kept: Any) -> bool:
    """``kept`` has a statement's shape (module docstring)."""
    if not isinstance(kept, dict) or kept.get("kind") not in KINDS:
        return False
    if set(kept) == {"kind", "axiom"}:
        return _valid_object(kept["axiom"]) and "node" in kept["axiom"]
    return (
        set(kept) == {"kind", "subject", "predicate", "object"}
        and isinstance(kept["subject"], str)
        and isinstance(kept["predicate"], str)
        and _valid_object(kept["object"])
    )
