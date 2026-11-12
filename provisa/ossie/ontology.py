# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2e8f41-7a93-4d06-b1e5-9f0a3d6c2b78
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2238: the terms a data product reaches, as an Apache Ossie ontology document.

The document is Ossie's ontology specification (``ontology/ontology.json``) at the commit
``convert.py`` is written against, so its version is the same ``OSSIE_VERSION``. It names only
what an agent may see (``core.glossary.agent_view``, carried on the snapshot as its grounded
terms): every term on an agent's path down to one of the product's members' columns or tables.

* A Class term (it grounds tables) and a Concept are each an ``EntityType``. A Property term (it
  holds columns) is a ``ValueType`` extending the Ossie built-in its columns' type maps to, as Ossie
  requires of every value type; one whose columns map to no built-in (a time, or a type outside
  Ossie's vocabulary) is withheld, and the document's ``ai_context`` says so by name.
* Each concept carries its definition as its description and its Provisa URI as its ``iri``. A
  defined draft is named, and listed under ``ai_context.draft_terms``: Ossie has no status.
* An undefined term on such a path is a hop: a concept with no name of its own -- ``(hop N)`` in
  the order the document lists it -- and no description or iri, so the route keeps its shape.
* ``KIND_OF`` is ``extends`` between two concepts of one type. Every other edge, and a kind-of
  between an entity and a value type, is a relationship under its first term, with a second role
  played by the other and a verbalization saying the edge (``"{order} has {order amount}"``).
  ``PART_OF`` reads from the whole: the whole has the part.
* REQ-2237: what an imported ontology stated about a term that a term does not model (its kept
  statements, provisa/ontology_import/kept.py) is written back out: Ossie's concepts take no key
  of a vendor's, so the document's ``ai_context.kept_statements`` lists each such concept with
  its own IRI and its statements, which an import of the document reads back.
"""

# Requirements: REQ-2238, REQ-2237

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import yaml

from provisa.ontology_import.parse import KEPT_STATEMENTS
from provisa.ossie.convert import OSSIE_VERSION, OssieExportRefused, _map_datatype

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import (
        DataProductAsset,
        GlossaryTermEdge,
        GroundedTerm,
        MetadataSnapshot,
    )

#: Ossie's built-in value types (ontology.md, Built-in concepts). A ``DateTimeTz`` is a datetime.
_BUILT_IN = {
    "String": "String",
    "Integer": "Integer",
    "Decimal": "Decimal",
    "Float": "Float",
    "Boolean": "Boolean",
    "Date": "Date",
    "DateTime": "DateTime",
    "DateTimeTz": "DateTime",
}

#: Each edge type as a relationship: its name stem, the term it sits under (``from`` or ``to``),
#: and its verbalization with ``{a}`` the first role and ``{b}`` the second.
_RELATIONSHIPS: dict[str, tuple[str, str, str]] = {
    "KIND_OF": ("kind_of", "from", "{a} is a kind of {b}"),
    "PART_OF": ("has", "to", "{a} has {b}"),
    "VALID_VALUE_OF": ("valid_value_of", "from", "{a} is a valid value of {b}"),
    "SYNONYM_OF": ("synonym_of", "from", "{a} is a synonym of {b}"),
    "RELATED_TO": ("related_to", "from", "{a} is related to {b}"),
    "DERIVED_FROM": ("derived_from", "from", "{a} is derived from {b}"),
    "REPLACES": ("replaces", "from", "{a} replaces {b}"),
    "PREFERRED_TERM_FOR": ("preferred_term_for", "from", "{a} is the preferred term for {b}"),
    "TRANSLATION_OF": ("translation_of", "from", "{a} is a translation of {b}"),
    "ANTONYM_OF": ("antonym_of", "from", "{a} is an antonym of {b}"),
}

_NOT_WORD = re.compile(r"\W+")


def _slug(name: str) -> str:
    return _NOT_WORD.sub("_", name).strip("_").lower()


def _built_ins(snapshot: "MetadataSnapshot", term: "GroundedTerm") -> list[str]:
    """The Ossie built-ins a Property term's columns' types map to, sorted."""
    types = {column.ref.fqn(): column.data_type for column in snapshot.columns()}
    mapped = {_map_datatype(types[ref.fqn()]) for ref in term.refs}
    return sorted({_BUILT_IN[m] for m in mapped if m in _BUILT_IN})


def ossie_ontology(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> dict[str, Any]:
    """The terms ``product`` reaches, as an Ossie ontology document (module docstring)."""
    terms, edges = snapshot.terms_reached(product)
    names: dict[int, str] = {}
    hops = 0
    for term in terms:
        if term.named:
            names[term.term_id] = str(term.name)
        else:
            hops += 1
            names[term.term_id] = f"(hop {hops})"
    kinds: dict[int, str] = {}
    extends: dict[int, list[str]] = {}
    withheld: list[dict[str, str]] = []
    for term in terms:
        if term.role != "property":
            kinds[term.term_id] = "EntityType"
            continue
        built_ins = _built_ins(snapshot, term)
        if not built_ins:
            withheld.append(
                {
                    "term": names[term.term_id],
                    "reason": "its columns' type is none of Ossie's built-in value types",
                }
            )
            continue
        kinds[term.term_id] = "ValueType"
        extends[term.term_id] = built_ins
    relationships: dict[int, list[dict[str, Any]]] = {}
    for edge in sorted(edges, key=lambda e: (e.from_term_id, e.to_term_id, e.rel_type)):
        if edge.from_term_id not in kinds or edge.to_term_id not in kinds:
            continue  # an end withheld
        if edge.rel_type == "KIND_OF" and kinds[edge.from_term_id] == kinds[edge.to_term_id]:
            extends.setdefault(edge.from_term_id, []).append(names[edge.to_term_id])
            continue
        _relate(edge, names, relationships)
    if not kinds:
        raise OssieExportRefused(
            f"data product {product.id!r} reaches no term an agent may see that Ossie can state"
        )
    ontology = []
    for term in terms:
        if term.term_id not in kinds:
            continue
        component: dict[str, Any] = {"concept": names[term.term_id], "type": kinds[term.term_id]}
        if term.definition:
            component["description"] = term.definition
        if parents := extends.get(term.term_id):
            component["extends"] = parents
        if related := relationships.get(term.term_id):
            component["relationships"] = related
        if term.semantic_uri:
            component["iri"] = term.semantic_uri
        ontology.append(component)
    document: dict[str, Any] = {
        "version": OSSIE_VERSION,
        "name": product.id,
        "description": f"The terms of Provisa data product {product.name!r}, down to its data.",
        "ontology": ontology,
    }
    drafts = [names[t.term_id] for t in terms if t.status == "draft" and t.term_id in kinds]
    kept = [
        {"concept": names[t.term_id], "iri": t.iri, "statements": list(t.statements)}
        for t in terms
        if t.statements and t.term_id in kinds
    ]
    if drafts or withheld or kept:
        context: dict[str, Any] = {}
        if drafts:
            context["draft_terms"] = drafts
        if withheld:
            context["withheld_terms"] = withheld
        if kept:
            context[KEPT_STATEMENTS] = kept
        document["ai_context"] = context
    return document


def _relate(
    edge: "GlossaryTermEdge", names: dict[int, str], out: dict[int, list[dict[str, Any]]]
) -> None:
    """One edge as a relationship under its first term (module docstring)."""
    stem, under, verbalization = _RELATIONSHIPS[edge.rel_type]
    first, second = (
        (edge.from_term_id, edge.to_term_id)
        if under == "from"
        else (edge.to_term_id, edge.from_term_id)
    )
    name = f"{stem}_{_slug(names[second])}"
    taken = {r["name"] for r in out.get(first, [])}
    if name in taken:
        raise OssieExportRefused(
            f"term {names[first]!r} would hold two relationships named {name!r}"
        )
    out.setdefault(first, []).append(
        {
            "name": name,
            "roles": [{"concept": names[second]}],
            "verbalizes": [verbalization.format(a=f"{{{names[first]}}}", b=f"{{{names[second]}}}")],
        }
    )


def ossie_ontology_yaml(snapshot: "MetadataSnapshot", product: "DataProductAsset") -> str:
    return yaml.safe_dump(ossie_ontology(snapshot, product), sort_keys=False, allow_unicode=True)
