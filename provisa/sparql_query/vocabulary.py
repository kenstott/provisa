# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The vocabulary a SPARQL query is read against (REQ-2135, REQ-2141): which IRI names which
class, which property and which edge of the graph mapping Cypher uses.

Data and vocabulary are kept apart, instances under ``/id/`` (:mod:`provisa.gql.iri`) and the
vocabulary under ``/vocab/``:

- a table's class, ``{base}/vocab/{domain}/{table}``; every table is a class, its rows its
  instances (the W3C Direct Mapping);
- a column's predicate, ``{base}/vocab/{domain}/{table}#{column}``;
- a term's class or property, ``{base}/vocab/term/{name}``: a class term types the instances of
  the tables it grounds besides their table class, and a property term is the predicate of the
  columns it grounds besides their own;
- an edge type, ``{base}/vocab/rel/{relationship type}``.

Each segment is percent-encoded as an instance key is. The vocabulary is built for one role, from
the label map built for it, so a class or predicate the role cannot read is not in it.
"""

# Requirements: REQ-2135, REQ-2141

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from urllib.parse import quote

from provisa.gql.label_map import CypherLabelMap

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
SKOS = "http://www.w3.org/2004/02/skos/core#"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDF_TYPE = f"{RDF}type"

# A column's SQL type as the xsd datatype of its literals.
_XSD_OF_SQL = (
    (
        ("tinyint", "smallint", "integer", "int", "bigint", "hugeint", "int2", "int4", "int8"),
        "integer",
    ),
    (("decimal", "numeric", "number"), "decimal"),
    (("double", "float", "real", "float4", "float8"), "double"),
    (("boolean", "bool"), "boolean"),
    (("timestamp", "datetime", "timestamptz"), "dateTime"),
    (("date",), "date"),
    (("time",), "time"),
)


def xsd_of(sql_type: str) -> str:
    """The xsd datatype of a column of ``sql_type``; text, and anything else, is a string."""
    head = sql_type.strip().lower().split("(")[0].split(" ")[0]
    for names, xsd in _XSD_OF_SQL:
        if head in names:
            return f"{XSD}{xsd}"
    return f"{XSD}string"


def _seg(value: str) -> str:
    return quote(value, safe="")


@dataclass(frozen=True)
class ColumnPredicate:
    """A predicate read from a property of nodes of ``label``."""

    label: str
    prop: str
    datatype: str


@dataclass(frozen=True)
class EdgePredicate:
    """A predicate read from edges of ``rel_type``, ``source`` to ``target``."""

    rel_type: str
    source: str
    target: str


@dataclass(frozen=True)
class Grounding:
    """The curated terms (REQ-2141), by the labels and properties of the label map: the class term
    of a label's table, and the property terms of a label's properties."""

    class_terms: dict[str, str] = field(default_factory=dict)  # label → term name
    property_terms: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)


@dataclass
class Vocabulary:
    base: str
    #: class IRI → the labels whose nodes it types
    classes: dict[str, list[str]]
    #: label → every class IRI typing its nodes (its table class first)
    classes_of: dict[str, list[str]]
    columns: dict[str, list[ColumnPredicate]]
    edges: dict[str, list[EdgePredicate]]
    #: label → (predicate IRI, ColumnPredicate) for each predicate its nodes have
    columns_of: dict[str, list[tuple[str, ColumnPredicate]]]
    edges_of: dict[str, list[tuple[str, EdgePredicate]]]
    prefixes: dict[str, str]

    def labels_of_class(self, iri: str) -> list[str]:
        return self.classes.get(iri, [])


def table_class_iri(base: str, domain: str, table: str) -> str:
    return f"{base}/vocab/{_seg(domain)}/{_seg(table)}"


def column_iri(base: str, domain: str, table: str, column: str) -> str:
    return f"{table_class_iri(base, domain, table)}#{_seg(column)}"


def term_iri(base: str, name: str) -> str:
    return f"{base}/vocab/term/{_seg(name)}"


def edge_iri(base: str, rel_type: str) -> str:
    return f"{base}/vocab/rel/{_seg(rel_type)}"


def build_vocabulary(
    lm: CypherLabelMap,
    base: str,
    column_types: dict[tuple[str, str], str],
    grounding: Grounding,
) -> Vocabulary:
    """The vocabulary of ``lm``'s nodes and relationships. ``column_types`` is each
    (label, property)'s SQL type. A node with no domain has no IRI and so no vocabulary."""
    classes: dict[str, list[str]] = defaultdict(list)
    classes_of: dict[str, list[str]] = defaultdict(list)
    columns: dict[str, list[ColumnPredicate]] = defaultdict(list)
    columns_of: dict[str, list[tuple[str, ColumnPredicate]]] = defaultdict(list)
    edges: dict[str, list[EdgePredicate]] = defaultdict(list)
    edges_of: dict[str, list[tuple[str, EdgePredicate]]] = defaultdict(list)
    prefixes = {"rdf": RDF, "rdfs": RDFS, "owl": OWL, "skos": SKOS, "xsd": XSD}

    for label, nm in lm.nodes.items():
        if not nm.domain_id or nm.traversal_only:
            continue
        own = table_class_iri(base, nm.domain_id, nm.table_name)
        prefixes.setdefault(nm.domain_id, f"{base}/vocab/{_seg(nm.domain_id)}/")
        class_iris = [own]
        if label in grounding.class_terms:
            class_iris.append(term_iri(base, grounding.class_terms[label]))
        for c in class_iris:
            classes[c].append(label)
            classes_of[label].append(c)
        for prop in nm.properties:
            datatype = xsd_of(column_types.get((label, prop), "varchar"))
            cp = ColumnPredicate(label, prop, datatype)
            iris = [column_iri(base, nm.domain_id, nm.table_name, prop)]
            iris += [term_iri(base, t) for t in grounding.property_terms.get((label, prop), ())]
            for iri in iris:
                columns[iri].append(cp)
                columns_of[label].append((iri, cp))

    all_rels = [*lm.relationships.values(), *(r for rs in lm.aliases.values() for r in rs)]
    for rel in all_rels:
        if rel.source_label not in classes_of or rel.target_label not in classes_of:
            continue
        ep = EdgePredicate(rel.rel_type, rel.source_label, rel.target_label)
        iri = edge_iri(base, rel.rel_type)
        if ep not in edges[iri]:
            edges[iri].append(ep)
            edges_of[rel.source_label].append((iri, ep))
    prefixes.setdefault("rel", f"{base}/vocab/rel/")
    prefixes.setdefault("term", f"{base}/vocab/term/")
    return Vocabulary(
        base=base,
        classes=dict(classes),
        classes_of=dict(classes_of),
        columns=dict(columns),
        edges=dict(edges),
        columns_of=dict(columns_of),
        edges_of=dict(edges_of),
        prefixes=prefixes,
    )


def _props_by_column(nm) -> dict[str, str]:
    """A node's properties by the column each reads: its physical name, and its SQL one."""
    out = {col: prop for prop, col in nm.properties.items()}
    out.update({col: prop for prop, col in nm.physical_properties.items()})
    return out


def grounding_from_refs(
    lm: CypherLabelMap,
    table_terms: dict[int, str],
    column_terms: dict[tuple[int, str], list[str]],
) -> Grounding:
    """The grounding of ``lm`` from the glossary's references: ``table_terms`` the class term of
    each registered table by its id, ``column_terms`` the terms of each (table id, column) a term
    reference names. A reference to what ``lm`` does not hold (a table the role cannot read)
    grounds nothing here."""
    class_terms: dict[str, str] = {}
    property_terms: dict[tuple[str, str], tuple[str, ...]] = {}
    for label, nm in lm.nodes.items():
        if nm.table_id in table_terms:
            class_terms[label] = table_terms[nm.table_id]
        by_column = _props_by_column(nm)
        for (table_id, column), terms in column_terms.items():
            if table_id == nm.table_id and column in by_column:
                property_terms[(label, by_column[column])] = tuple(terms)
    return Grounding(class_terms, property_terms)


def column_types_of(lm: CypherLabelMap, ctx: object) -> dict[tuple[str, str], str]:
    """Each (label, property)'s SQL type, from the compilation context's columns."""
    aggregate_columns: dict[int, list[tuple[str, str]]] = getattr(ctx, "aggregate_columns")
    out: dict[tuple[str, str], str] = {}
    for label, nm in lm.nodes.items():
        by_column = _props_by_column(nm)
        for column, sql_type in aggregate_columns.get(nm.table_id, []):
            if column in by_column:
                out[(label, by_column[column])] = sql_type
    return out
