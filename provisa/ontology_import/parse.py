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
  ``rdfs:subPropertyOf`` and ``skos:broader`` are KIND_OF, a datatype property's ``rdfs:domain``
  is PART_OF the domain class, ``owl:equivalentClass`` and ``owl:equivalentProperty`` are
  SYNONYM_OF and ``skos:related`` is RELATED_TO -- each only between items of the same file. A
  named individual of a declared class that states nothing of its own beyond its types and
  annotations (a plain enumerated value) is an item, VALID_VALUE_OF its class.
- Nothing else about an item is lost (REQ-2237, amended): every other statement the file makes
  about it -- an object property's domain and range, restrictions, cardinalities, disjointness,
  characteristics, property chains, datatype ranges, annotations, a statement naming something
  the file does not declare -- is kept on the item (provisa.ontology_import.kept), and so is a
  named individual with properties of its own, on its class, and an axiom whose subject is a
  blank node, on the first item it names. What is about no item (an ontology's own header, a
  resource the file does not declare) is left out and counted.
- A whole release, as a zip archive of its module files, is read as one graph
  (provisa.ontology_import.release).

An item's name is its label as the glossary writes a term: lowercase words, a camelCase or
snake_case local name split into them, nothing expanded or dropped. Items of one file that share
a name are one item: one phrase, one term, as the glossary keeps it. Its definition is the
file's own text for it, never one written here.

Nothing is fetched while reading: ``owl:imports`` is never followed (a release's are resolved
inside its archive), Turtle, N-Triples and RDF/XML name no document to load (the
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
#: A whole release: every module file of it in one archive (provisa.ontology_import.release).
ARCHIVE = "archive"
FORMATS = (OSSIE, TURTLE, RDF_XML, N_TRIPLES, JSON_LD, ARCHIVE)

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
#: A named individual that is a plain enumerated value of a class: it states nothing of its own.
INDIVIDUAL = "individual"
KINDS = (CLASS, PROPERTY, VALUE, CONCEPT, INDIVIDUAL)

KIND_OF = "KIND_OF"
PART_OF = "PART_OF"
SYNONYM_OF = "SYNONYM_OF"
RELATED_TO = "RELATED_TO"
VALID_VALUE_OF = "VALID_VALUE_OF"

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
    #: What the ontology states about it that a term does not model (provisa.ontology_import.kept).
    statements: list[dict[str, Any]] = field(default_factory=list)


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
    #: What the file held that is no item, edge or kept statement, counted by why.
    left_out: dict[str, int]
    #: A release's module files, and each ``owl:imports`` it states with the module it resolved
    #: to (None: a module outside the archive). Empty for a single file.
    modules: list[str] = field(default_factory=list)
    imports: dict[str, str | None] = field(default_factory=dict)


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
    if name.endswith(".zip"):
        return ARCHIVE
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
    if fmt == ARCHIVE:
        parsed = _archive(content, file_name)
        if not parsed.items:
            raise ImportRefused(f"{file_name!r} declares no class, property or concept to import")
        return parsed
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
        statements: Iterable[dict[str, Any]] = (),
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
        item.statements.extend(statements)

    def holds(self, key: str) -> bool:
        return key in self._by_key

    def keep(self, key: str, statements: Iterable[dict[str, Any]]) -> None:
        """Keep ``statements`` on the item ``key`` names."""
        self.items[self._by_key[key]].statements.extend(statements)

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


#: Where an Ossie document a Provisa export wrote keeps its terms' statements: Ossie's concepts
#: and relationships take no key of a vendor's, and its ``ai_context`` takes any.
KEPT_STATEMENTS = "kept_statements"


def _kept(doc: Mapping[str, Any]) -> dict[str, tuple[str | None, list[dict[str, Any]]]]:
    """The IRI and the statements a Provisa export kept for each concept (or
    ``<concept>.<relationship>``) of an Ossie document, under ``ai_context.kept_statements``
    (provisa.ontology_import.kept). The IRI is the term's own, which Ossie's ``iri`` -- the
    Provisa URI of the term -- does not carry."""
    from provisa.ontology_import import kept

    context = doc.get("ai_context")
    entries = context.get(KEPT_STATEMENTS) if isinstance(context, dict) else None
    if entries is None:
        return {}
    out: dict[str, tuple[str | None, list[dict[str, Any]]]] = {}
    for entry in entries if isinstance(entries, list) else [None]:
        if not isinstance(entry, dict):
            raise ImportRefused(
                "ai_context.kept_statements is a list of a concept and its kept statements"
            )
        concept, statements = entry.get("concept"), entry.get("statements")
        if (
            not isinstance(concept, str)
            or not isinstance(statements, list)
            or not all(kept.valid(x) for x in statements)
        ):
            raise ImportRefused(
                "ai_context.kept_statements is a list of a concept and its kept statements"
            )
        relationship, iri = entry.get("relationship"), entry.get("iri")
        if iri is not None and (not isinstance(iri, str) or not iri.strip()):
            raise ImportRefused(f"ai_context.kept_statements' iri of {concept!r} is an IRI")
        key = concept if relationship is None else f"{concept}.{relationship}"
        out[key] = (iri, [*out.get(key, (None, []))[1], *statements])
    return out


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
    kept_by = _kept(doc)
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
        own_iri, statements = kept_by.get(concept, (None, []))
        iri = own_iri or _expand(fields.get("iri"), prefixes, f"concept {concept!r}")
        found.add(
            concept,
            phrase(concept),
            kind,
            _description(fields.get("description")),
            iri,
            statements=statements,
        )
        for parent in fields.get("extends") or []:
            if isinstance(parent, str) and parent not in OSSIE_BUILT_INS:
                found.relate(concept, KIND_OF, parent)
        for rel in fields.get("relationships") or []:
            if not isinstance(rel, dict) or not isinstance(rel.get("name"), str):
                found.leave_out("relationship with no name")
                continue
            key = f"{concept}.{rel['name']}"
            rel_own_iri, rel_statements = kept_by.get(key, (None, []))
            rel_iri = rel_own_iri or _expand(rel.get("iri"), prefixes, f"relationship {key!r}")
            found.add(
                key,
                f"{phrase(concept)} {phrase(rel['name'])}",
                PROPERTY,
                _description(rel.get("description")),
                rel_iri,
                statements=rel_statements,
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
    return _read(_graph(text, fmt, file_name), fmt, file_name, {})


def _module_format(member: str) -> str | None:
    """A release member's RDF format, or None for a member that is no module."""
    name = member.lower()
    if name.endswith("catalog-v001.xml"):
        return None
    if name.endswith(".json"):
        return JSON_LD
    for extension, fmt in _BY_EXTENSION.items():
        if name.endswith(extension):
            return None if fmt == OSSIE else fmt
    return None


def _archive(content: bytes, file_name: str) -> Parsed:
    from provisa.ontology_import import release

    found = release.read(file_name, content, _graph, _module_format, ImportRefused)
    left_out: dict[str, int] = {}
    outside = sum(1 for path in found.imports.values() if path is None)
    if outside:
        left_out["import of a module outside the archive"] = outside
    for why, n in found.skipped.items():
        left_out[f"archive {why}"] = n
    parsed = _read(found.graph, ARCHIVE, file_name, left_out)
    parsed.modules = found.modules
    parsed.imports = found.imports
    return parsed


#: The glossary edge a statement between two of the release's items is, by its predicate.
def _edge_predicates() -> dict[Any, str]:
    from rdflib import OWL, RDFS, SKOS

    return {
        RDFS.subClassOf: KIND_OF,
        RDFS.subPropertyOf: KIND_OF,
        SKOS.broader: KIND_OF,
        RDFS.domain: PART_OF,
        OWL.equivalentClass: SYNONYM_OF,
        OWL.equivalentProperty: SYNONYM_OF,
        SKOS.related: RELATED_TO,
    }


def _read(graph: Any, fmt: str, file_name: str, left_out: dict[str, int]) -> Parsed:
    """The items ``graph`` declares, the edges between them and, on each, what it states that a
    term does not model (provisa.ontology_import.kept)."""
    from rdflib import OWL, RDF, RDFS, SKOS, BNode, URIRef
    from rdflib.collection import Collection
    from rdflib.namespace import DC, DCTERMS

    from provisa.ontology_import import kept

    reserved = tuple(str(ns) for ns in (RDF, RDFS, OWL, SKOS, "http://www.w3.org/2001/XMLSchema#"))
    declared: dict[str, str] = {}
    object_properties: set[str] = set()
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
                if rdf_type in (OWL.Class, RDFS.Class):
                    continue  # a class expression: kept where it is used
                blank += 1
                continue
            iri = str(subject)
            if not iri.startswith(reserved):
                declared.setdefault(iri, kind)
                if rdf_type == OWL.ObjectProperty:
                    object_properties.add(iri)
    found = _Collector()
    for why, n in left_out.items():
        found.leave_out(why, n)
    if blank:
        found.leave_out("anonymous property", blank)

    def text_of(subject: Any, *predicates: Any) -> str | None:
        for predicate in predicates:
            said = _preferred(graph.objects(subject, predicate))
            if said is not None:
                return said
        return None

    def described(iri: str) -> tuple[str, str | None]:
        subject = URIRef(iri)
        label = text_of(subject, SKOS.prefLabel, RDFS.label) or _local_name(iri)
        definition = text_of(
            subject, SKOS.definition, RDFS.comment, DCTERMS.description, DC.description
        )
        return phrase(label), definition

    # Named individuals: those of a class the release declares, by their type or an enumeration.
    classes_of: dict[str, list[str]] = {}
    for subject, cls in graph.subject_objects(RDF.type):
        if (
            isinstance(subject, URIRef)
            and str(subject) not in declared
            and declared.get(str(cls)) == CLASS
        ):
            classes_of.setdefault(str(subject), []).append(str(cls))
    for owner, predicate, expression in graph.triples((None, None, None)):
        if predicate not in (OWL.equivalentClass, RDFS.subClassOf, OWL.oneOf):
            continue
        if declared.get(str(owner)) != CLASS:
            continue
        lists = (
            [expression] if predicate == OWL.oneOf else list(graph.objects(expression, OWL.oneOf))
        )
        for members in lists:
            for member in Collection(graph, members):
                if isinstance(member, URIRef) and str(member) not in declared:
                    held = classes_of.setdefault(str(member), [])
                    if str(owner) not in held:
                        held.append(str(owner))
    for subject in graph.subjects(RDF.type, OWL.NamedIndividual):
        if isinstance(subject, URIRef) and str(subject) not in declared:
            if str(subject) not in classes_of:
                found.leave_out("individual of no class the release declares")

    def plain(individual: str) -> bool:
        """An enumerated value: it states nothing of its own beyond its types and annotations."""
        return all(
            predicate == RDF.type or kept.is_annotation(graph, predicate)
            for predicate in graph.predicates(URIRef(individual))
        )

    def keep(subject: Any, skip: Any = None) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for predicate, value in sorted(graph.predicate_objects(subject)):
            if skip is not None and skip(predicate, value):
                continue
            try:
                out.append(kept.statement(graph, subject, predicate, value))
            except kept.BlankNodeCycle:
                found.leave_out("blank node that reaches itself")
        return out

    edge_predicates = _edge_predicates()

    def modeled(iri: str) -> Any:
        """Whether a statement about the item ``iri`` is one of the glossary's edges instead."""

        def skip(predicate: Any, value: Any) -> bool:
            rel_type = edge_predicates.get(predicate)
            if rel_type is None or not isinstance(value, URIRef) or str(value) not in declared:
                return False
            if rel_type == PART_OF and iri in object_properties:
                return False  # an object property keeps its domain, as it keeps its range
            return True

        return skip

    for iri in sorted(declared):
        name, definition = described(iri)
        subject = URIRef(iri)
        labels = sorted(
            {phrase(str(a)) for a in graph.objects(subject, SKOS.altLabel) if str(a).strip()}
        )
        found.add(iri, name, declared[iri], definition, iri, labels, keep(subject, modeled(iri)))
    for individual in sorted(classes_of):
        subject = URIRef(individual)
        classes = classes_of[individual]
        if plain(individual):
            name, definition = described(individual)
            found.add(
                individual,
                name,
                INDIVIDUAL,
                definition,
                individual,
                (),
                keep(
                    subject,
                    lambda p, v: p == RDF.type and str(v) in classes,
                ),
            )
            for cls in classes:
                found.relate(individual, VALID_VALUE_OF, cls)
            continue
        statements = keep(subject)
        for statement in statements:
            statement["kind"] = kept.INDIVIDUAL
        found.keep(classes[0], statements)
    for subject, predicate, target in graph.triples((None, None, None)):
        if str(subject) not in declared or not isinstance(target, URIRef):
            continue
        rel_type = edge_predicates.get(predicate)
        if rel_type is None or str(target) not in declared:
            continue
        if rel_type == PART_OF and str(subject) in object_properties:
            continue
        found.relate(str(subject), rel_type, str(target))
    # An axiom whose subject is itself a blank node (owl:AllDisjointClasses, an annotated
    # owl:Axiom) is kept on the first item it names.
    for node in sorted({s for s in graph.subjects() if isinstance(s, BNode)}):
        if next(iter(graph.subject_predicates(node)), None) is not None:
            continue
        if (node, RDF.first, None) in graph:
            continue
        try:
            statement = kept.axiom(graph, node)
        except kept.BlankNodeCycle:
            found.leave_out("blank node that reaches itself")
            continue
        owner = next(
            (iri for iri in kept.iris_in(statement["axiom"]) if found.holds(iri)),
            None,
        )
        if owner is None:
            found.leave_out("axiom about nothing the ontology declares")
            continue
        found.keep(owner, [statement])
    covered = set(declared) | set(classes_of)
    ontologies = {str(o) for o in graph.subjects(RDF.type, OWL.Ontology)}
    header = 0
    elsewhere = 0
    for subject in {s for s in graph.subjects() if isinstance(s, URIRef)}:
        iri = str(subject)
        if iri in covered or iri.startswith(reserved):
            continue
        n = len(list(graph.predicate_objects(subject)))
        if iri in ontologies:
            header += n
        else:
            elsewhere += n
    if header:
        found.leave_out("statement about an ontology module itself", header)
    if elsewhere:
        found.leave_out("statement about something the ontology does not declare", elsewhere)
    ontology = next(iter(sorted(graph.subjects(RDF.type, OWL.Ontology))), None)
    title = (
        text_of(ontology, DCTERMS.title, RDFS.label, DC.title)
        if ontology is not None and fmt != ARCHIVE
        else None
    ) or file_name
    return found.parsed(title, fmt)
