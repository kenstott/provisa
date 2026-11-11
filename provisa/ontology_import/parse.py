# Copyright (c) 2026 Kenneth Stott
# Canary: 4f81a6d2-0c7e-4b95-a3d8-6e2b19f0c7a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Read an ontology file into items and the structure between them (REQ-2237).

Two families are read:

- an Apache Ossie ontology (YAML, or the same document as JSON): each concept is an item, an
  ``EntityType`` a class and a ``ValueType`` a value type; each relationship declared under a
  concept is a property of it, named ``<concept> <relationship>``. ``extends`` is KIND_OF its
  supertype, and a relationship is PART_OF the concept it is declared under. Built-in concepts
  (``Any``, ``String``, ...) are not items. Mapping documents and ``ontology_mappings`` are not
  read: Provisa grounds an imported concept through the terms it aligns with, never field by field.
- RDF/OWL (Turtle, RDF/XML, N-Triples, JSON-LD) through rdflib: every named ``owl:Class`` or
  ``rdfs:Class`` is a class, every ``owl:ObjectProperty``, ``owl:DatatypeProperty`` or
  ``rdf:Property`` a property, every ``skos:Concept`` a concept. ``rdfs:subClassOf``,
  ``rdfs:subPropertyOf`` and ``skos:broader`` are KIND_OF, ``rdfs:domain`` is PART_OF the domain
  class, ``owl:equivalentClass`` and ``owl:equivalentProperty`` are SYNONYM_OF and ``skos:related``
  is RELATED_TO -- each only between items of the same file; a statement about a class the file
  does not declare is left out and counted.

An item's name is its label as the glossary writes a term: lowercase words, a camelCase or
snake_case local name split into them, nothing expanded or dropped. Items of one file that share
a name are one item: one phrase, one term, as the glossary keeps it. Its definition is the
file's own text for it, never one written here.

Nothing is fetched while reading: Turtle, N-Triples and RDF/XML name no document to load (the
XML reader resolves no external entity), and a JSON-LD document that names a remote ``@context``
or ``@import`` is refused, naming it, rather than letting the parser fetch it.
"""

# Requirements: REQ-2237

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

#: The formats an import reads.
OSSIE = "ossie"
TURTLE = "turtle"
RDF_XML = "xml"
N_TRIPLES = "nt"
JSON_LD = "json-ld"
FORMATS = (OSSIE, TURTLE, RDF_XML, N_TRIPLES, JSON_LD)

_BY_EXTENSION = {
    ".yaml": OSSIE,
    ".yml": OSSIE,
    ".ttl": TURTLE,
    ".rdf": RDF_XML,
    ".owl": RDF_XML,
    ".xml": RDF_XML,
    ".nt": N_TRIPLES,
    ".jsonld": JSON_LD,
}

#: What an item is in the file it came from.
CLASS = "class"
PROPERTY = "property"
VALUE = "value"
CONCEPT = "concept"
KINDS = (CLASS, PROPERTY, VALUE, CONCEPT)

KIND_OF = "KIND_OF"
PART_OF = "PART_OF"
SYNONYM_OF = "SYNONYM_OF"
RELATED_TO = "RELATED_TO"

#: Ossie's built-in concepts: every ontology holds them, and they are no item of one.
OSSIE_BUILT_INS = frozenset(
    {"Any", "Boolean", "Date", "DateTime", "Decimal", "Float", "Integer", "String"}
)


class ImportRefused(ValueError):
    """A file that cannot be imported, said naming why."""


@dataclass
class Item:
    """One class, property, value type or concept of the file. ``keys`` are its identifiers in
    the file (IRIs, or Ossie names), several when items sharing a name fold into one."""

    name: str
    kind: str
    keys: list[str]
    definition: str | None = None
    iri: str | None = None
    labels: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Edge:
    """The file's own statement that item ``source`` relates to item ``target`` (ordinals)."""

    source: int
    rel_type: str
    target: int


@dataclass
class Parsed:
    title: str
    format: str
    items: list[Item]
    edges: list[Edge]
    #: What the file held that is no item or edge, counted by why.
    left_out: dict[str, int]


_CAMEL_1 = re.compile(r"([A-Z]+)([A-Z][a-z])")  # naming-ok: term words, as glossary._phrase splits
_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")  # naming-ok: term words, as glossary._phrase splits
_SEPARATORS = re.compile(r"[_\-.\s/]+")


def phrase(label: str) -> str:
    """``label`` as a term name: camelCase and snake_case split into lowercase words, single
    spaces, nothing expanded or dropped (``CustomerAccount`` → ``customer account``)."""
    spaced = _CAMEL_2.sub(r"\1 \2", _CAMEL_1.sub(r"\1 \2", label))
    return " ".join(_SEPARATORS.sub(" ", spaced).lower().split())


def format_of(file_name: str, declared: str | None) -> str:
    """The format of ``file_name``: ``declared`` when given, else its extension's. A ``.json``
    file is read by :func:`parse`, which tells an Ossie document from JSON-LD by its keys."""
    if declared is not None:
        if declared not in FORMATS:
            raise ImportRefused(f"an ontology's format is one of {', '.join(FORMATS)}")
        return declared
    name = file_name.lower()
    if name.endswith(".json"):
        return JSON_LD
    for extension, fmt in _BY_EXTENSION.items():
        if name.endswith(extension):
            return fmt
    raise ImportRefused(
        f"{file_name!r} names no ontology format: give its format, one of {', '.join(FORMATS)}"
    )


def _text(content: bytes, file_name: str) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ImportRefused(f"{file_name!r} is not UTF-8 text") from None


def parse(file_name: str, content: bytes, declared: str | None = None) -> Parsed:
    """The ontology ``content`` holds, read as ``declared`` or as its file name says."""
    fmt = format_of(file_name, declared)
    text = _text(content, file_name)
    if fmt == JSON_LD and declared is None and file_name.lower().endswith(".json"):
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ImportRefused(f"{file_name!r} is not JSON: {exc}") from None
        if isinstance(doc, dict) and "ontology" in doc:
            fmt = OSSIE
    parsed = _ossie(text, file_name) if fmt == OSSIE else _rdf(text, fmt, file_name)
    if not parsed.items:
        raise ImportRefused(f"{file_name!r} declares no class, property or concept to import")
    return parsed


# -- folding items that share a name ----------------------------------------------------------


class _Collector:
    """Items by name, and the file's edges between them by key."""

    def __init__(self) -> None:
        self.items: list[Item] = []
        self._by_name: dict[str, int] = {}
        self._by_key: dict[str, int] = {}
        self._edges: list[tuple[str, str, str]] = []
        self.left_out: dict[str, int] = {}

    def leave_out(self, why: str, n: int = 1) -> None:
        self.left_out[why] = self.left_out.get(why, 0) + n

    def add(
        self,
        key: str,
        name: str,
        kind: str,
        definition: str | None,
        iri: str | None,
        labels: Iterable[str] = (),
    ) -> None:
        if not name:
            self.leave_out("no name")
            return
        held = self._by_name.get(name)
        if held is None:
            self._by_name[name] = self._by_key[key] = len(self.items)
            self.items.append(Item(name, kind, [key], definition, iri, []))
        else:
            item = self.items[held]
            self._by_key[key] = held
            item.keys.append(key)
            item.definition = item.definition or definition
            item.iri = item.iri or iri
        item = self.items[self._by_name[name]]
        for label in labels:
            if label != name and label not in item.labels:
                item.labels.append(label)

    def relate(self, source: str, rel_type: str, target: str) -> None:
        self._edges.append((source, rel_type, target))

    def parsed(self, title: str, fmt: str) -> Parsed:
        edges: list[Edge] = []
        for source, rel_type, target in self._edges:
            a, b = self._by_key.get(source), self._by_key.get(target)
            if a is None or b is None:
                self.leave_out("statement about something the file does not declare")
                continue
            if a == b:
                continue
            edge = Edge(a, rel_type, b)
            if edge not in edges:
                edges.append(edge)
        return Parsed(title, fmt, self.items, edges, self.left_out)


# -- Apache Ossie --------------------------------------------------------------------------------


def _description(raw: Any) -> str | None:
    return raw.strip() or None if isinstance(raw, str) else None


def _expand(iri: Any, prefixes: Mapping[str, Any], where: str) -> str | None:
    """An Ossie ``iri``: a full IRI, or a ``prefix:local`` QName over the declared prefixes."""
    if iri is None:
        return None
    if not isinstance(iri, str) or not iri.strip():
        raise ImportRefused(f"{where}'s iri is a non-empty IRI")
    iri = iri.strip()
    prefix, colon, local = iri.partition(":")
    if colon and prefix in prefixes and not local.startswith("//"):
        return f"{prefixes[prefix]}{local}"
    return iri


def _ossie(text: str, file_name: str) -> Parsed:
    import yaml

    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ImportRefused(f"{file_name!r} is not YAML: {exc}") from None
    if not isinstance(doc, dict) or not isinstance(doc.get("ontology"), list):
        raise ImportRefused(
            f"{file_name!r} is not an Apache Ossie ontology: it has no ontology list"
        )
    prefixes = doc.get("prefixes") or {}
    if not isinstance(prefixes, dict):
        raise ImportRefused("an Ossie ontology's prefixes are a map of prefix to IRI")
    found = _Collector()
    for entry in doc["ontology"]:
        if not isinstance(entry, dict) or "concept" not in entry:
            found.leave_out("ontology entry that declares no concept")
            continue
        # 0.2.0 drafts nested a concept's fields under ``concept``; later ones declare them beside it.
        declared = entry["concept"]
        fields = {**entry, **declared} if isinstance(declared, dict) else entry
        concept = declared.get("name") if isinstance(declared, dict) else declared
        if not isinstance(concept, str) or not concept.strip():
            found.leave_out("ontology entry that declares no concept")
            continue
        declared_type = fields.get("type")
        kinds = {"EntityType": CLASS, "ValueType": VALUE}
        kind = kinds.get(declared_type) if isinstance(declared_type, str) else None
        if kind is None:
            raise ImportRefused(f"concept {concept!r}'s type is EntityType or ValueType")
        iri = _expand(fields.get("iri"), prefixes, f"concept {concept!r}")
        found.add(concept, phrase(concept), kind, _description(fields.get("description")), iri)
        for parent in fields.get("extends") or []:
            if isinstance(parent, str) and parent not in OSSIE_BUILT_INS:
                found.relate(concept, KIND_OF, parent)
        for rel in fields.get("relationships") or []:
            if not isinstance(rel, dict) or not isinstance(rel.get("name"), str):
                found.leave_out("relationship with no name")
                continue
            key = f"{concept}.{rel['name']}"
            rel_iri = _expand(rel.get("iri"), prefixes, f"relationship {key!r}")
            found.add(
                key,
                f"{phrase(concept)} {phrase(rel['name'])}",
                PROPERTY,
                _description(rel.get("description")),
                rel_iri,
            )
            found.relate(key, PART_OF, concept)
    if doc.get("ontology_mappings"):
        found.leave_out("ontology mapping", len(doc["ontology_mappings"]))
    name = doc.get("name")
    return found.parsed(name if isinstance(name, str) and name.strip() else file_name, OSSIE)


# -- RDF / OWL -----------------------------------------------------------------------------------


def _remote_context(node: Any) -> str | None:
    """The first remote ``@context`` or ``@import`` a JSON-LD document names, if any."""
    if isinstance(node, list):
        for value in node:
            found = _remote_context(value)
            if found is not None:
                return found
        return None
    if not isinstance(node, dict):
        return None
    for key, value in node.items():
        if key == "@context":
            contexts = value if isinstance(value, list) else [value]
            for context in contexts:
                if isinstance(context, str):
                    return context
                if isinstance(context, dict) and isinstance(context.get("@import"), str):
                    return context["@import"]
        found = _remote_context(value)
        if found is not None:
            return found
    return None


_RDF_FORMAT = {TURTLE: "turtle", RDF_XML: "xml", N_TRIPLES: "nt", JSON_LD: "json-ld"}


def _graph(text: str, fmt: str, file_name: str) -> Any:
    from rdflib import Graph

    if fmt == JSON_LD:
        try:
            remote = _remote_context(json.loads(text))
        except json.JSONDecodeError as exc:
            raise ImportRefused(f"{file_name!r} is not JSON: {exc}") from None
        if remote is not None:
            raise ImportRefused(
                f"{file_name!r} names the remote JSON-LD context {remote!r}; an import fetches "
                "nothing, so write the context into the document"
            )
    graph = Graph()
    try:
        graph.parse(data=text, format=_RDF_FORMAT[fmt])
    except Exception as exc:  # rdflib raises its parsers' own errors, of no common base
        raise ImportRefused(f"{file_name!r} is not {fmt} RDF: {exc}") from None
    return graph


def _preferred(literals: Iterable[Any]) -> str | None:
    """The English (or untagged) text among ``literals``, else any; None when there is none."""
    texts = [(lit.language, str(lit).strip()) for lit in literals if str(lit).strip()]
    for lang, text in texts:
        if lang is None or lang.lower().startswith("en"):
            return text
    return texts[0][1] if texts else None


def _local_name(iri: str) -> str:
    return re.split(r"[#/:]", iri.rstrip("/#"))[-1]


def _rdf(text: str, fmt: str, file_name: str) -> Parsed:
    from rdflib import OWL, RDF, RDFS, SKOS, URIRef
    from rdflib.namespace import DC, DCTERMS

    graph = _graph(text, fmt, file_name)
    reserved = tuple(str(ns) for ns in (RDF, RDFS, OWL, SKOS, "http://www.w3.org/2001/XMLSchema#"))
    declared: dict[str, str] = {}
    blank = 0
    for rdf_type, kind in (
        (OWL.Class, CLASS),
        (RDFS.Class, CLASS),
        (OWL.ObjectProperty, PROPERTY),
        (OWL.DatatypeProperty, PROPERTY),
        (RDF.Property, PROPERTY),
        (SKOS.Concept, CONCEPT),
    ):
        for subject in graph.subjects(RDF.type, rdf_type):
            if not isinstance(subject, URIRef):
                blank += 1
                continue
            iri = str(subject)
            if not iri.startswith(reserved):
                declared.setdefault(iri, kind)
    found = _Collector()
    if blank:
        found.leave_out("anonymous class or property", blank)

    def text_of(subject: Any, *predicates: Any) -> str | None:
        for predicate in predicates:
            said = _preferred(graph.objects(subject, predicate))
            if said is not None:
                return said
        return None

    for iri in sorted(declared):
        subject = URIRef(iri)
        label = text_of(subject, SKOS.prefLabel, RDFS.label) or _local_name(iri)
        labels = sorted(
            {phrase(str(a)) for a in graph.objects(subject, SKOS.altLabel) if str(a).strip()}
        )
        definition = text_of(
            subject, SKOS.definition, RDFS.comment, DCTERMS.description, DC.description
        )
        found.add(iri, phrase(label), declared[iri], definition, iri, labels)
    for predicate, rel_type in (
        (RDFS.subClassOf, KIND_OF),
        (RDFS.subPropertyOf, KIND_OF),
        (SKOS.broader, KIND_OF),
        (RDFS.domain, PART_OF),
        (OWL.equivalentClass, SYNONYM_OF),
        (OWL.equivalentProperty, SYNONYM_OF),
        (SKOS.related, RELATED_TO),
    ):
        for subject, target in graph.subject_objects(predicate):
            if str(subject) not in declared:
                continue
            if not isinstance(target, URIRef):
                found.leave_out("statement about an anonymous class")
                continue
            if str(target).startswith(reserved):
                continue
            found.relate(str(subject), rel_type, str(target))
    ontology = next(iter(graph.subjects(RDF.type, OWL.Ontology)), None)
    title = (
        text_of(ontology, DCTERMS.title, RDFS.label, DC.title) if ontology is not None else None
    ) or file_name
    return found.parsed(title, fmt)
