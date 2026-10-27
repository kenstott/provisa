# Copyright (c) 2026 Kenneth Stott
# Canary: b1b8584b-c743-49dc-a55b-461c12cd8bb4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bulk RDF export of the governed graph (REQ-2165): the counterpart of the Neo4j export.

An export is a list of SPARQL CONSTRUCT queries, each answered through the one pipeline as any
SPARQL query is (so it holds exactly what the exporting role reads, masks applied, at the request's
time travel), and the declarations of the role's own vocabulary:

- one query per class in scope: its instances, each typed by every class it has (its table's and
  its class term's), with each column's value -- the columns as OPTIONALs, one row each, so a
  wide table is one query and no union;
- one query per edge predicate leaving the class: its instances' edges;
- the ontology schema (REQ-2163), read from ontology._axioms: one query per RDFS/OWL/SKOS
  predicate it states;
- each table class in scope declared an owl:Class, each column predicate an
  owl:DatatypeProperty and each edge an owl:ObjectProperty, with their domains and ranges.

Scope is the whole graph, a domain, or one class.
"""

# Requirements: REQ-2165

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from provisa.sparql_query.vocabulary import OWL, RDF, RDF_TYPE, RDFS, SKOS, Vocabulary

Scope = Literal["graph", "domain", "class"]

#: The ontology schema's predicates, each read from ontology._axioms in a query of its own.
_AXIOM_PREDICATES = (
    f"{RDFS}subClassOf",
    f"{RDFS}subPropertyOf",
    f"{RDFS}domain",
    f"{RDFS}range",
    f"{SKOS}prefLabel",
    f"{SKOS}definition",
)
_OWL_KINDS = (f"{OWL}Class", f"{OWL}ObjectProperty", f"{OWL}DatatypeProperty")


@dataclass(frozen=True)
class ExportPlan:
    """The CONSTRUCT queries an export answers, and the vocabulary triples it declares."""

    queries: list[str]
    declarations: list[tuple[str, str, str]]


class ScopeRefused(ValueError):
    """The scope names no class or domain the role reads."""


def _labels_in_scope(
    vocab: Vocabulary, domains: dict[str, str | None], scope: Scope, value: str | None
) -> list[str]:
    """The labels whose instances the export holds. ``domains`` is each label's domain."""
    if scope == "graph":
        return sorted(vocab.classes_of)
    if scope == "domain":
        found = sorted(label for label in vocab.classes_of if domains.get(label) == value)
        if not found:
            raise ScopeRefused(f"The role reads no class in domain {value}")
        return found
    labels = sorted(vocab.classes.get(value or "", []))
    if not labels:
        raise ScopeRefused(f"The role reads no class <{value}>")
    return labels


def _instances_query(vocab: Vocabulary, label: str, typed_by: str) -> str:
    """A label's instances typed by every class they have, with every column's value."""
    template = [f"?s <{RDF_TYPE}> <{c}> ." for c in vocab.classes_of[label]]
    optionals = []
    for i, (iri, _cp) in enumerate(vocab.columns_of.get(label, [])):
        template.append(f"?s <{iri}> ?o{i} .")
        optionals.append(f"OPTIONAL {{ ?s <{iri}> ?o{i} }}")
    return (
        f"CONSTRUCT {{ {' '.join(template)} }} "
        f"WHERE {{ ?s <{RDF_TYPE}> <{typed_by}> . {' '.join(optionals)} }}"
    )


def _edge_queries(vocab: Vocabulary, label: str, typed_by: str) -> list[str]:
    return [
        f"CONSTRUCT {{ ?s <{iri}> ?o }} WHERE {{ ?s <{RDF_TYPE}> <{typed_by}> ; <{iri}> ?o }}"
        for iri in sorted({iri for iri, _ep in vocab.edges_of.get(label, [])})
    ]


def _axiom_queries() -> list[str]:
    queries = [f"CONSTRUCT {{ ?s <{p}> ?o }} WHERE {{ ?s <{p}> ?o }}" for p in _AXIOM_PREDICATES]
    queries += [
        f"CONSTRUCT {{ ?s <{RDF_TYPE}> <{k}> }} WHERE {{ ?s <{RDF_TYPE}> <{k}> }}" for k in _OWL_KINDS
    ]
    return queries


def _declarations(vocab: Vocabulary, labels: list[str]) -> list[tuple[str, str, str]]:
    """Each table class in scope an owl:Class, its column predicates owl:DatatypeProperty and its
    edges owl:ObjectProperty, with their domains and ranges."""
    out: set[tuple[str, str, str]] = set()
    for label in labels:
        own = vocab.classes_of[label][0]
        out.add((own, RDF_TYPE, f"{OWL}Class"))
        for iri, cp in vocab.columns_of.get(label, []):
            out.add((iri, RDF_TYPE, f"{OWL}DatatypeProperty"))
            out.add((iri, f"{RDFS}domain", own))
            out.add((iri, f"{RDFS}range", cp.datatype))
        for iri, ep in vocab.edges_of.get(label, []):
            out.add((iri, RDF_TYPE, f"{OWL}ObjectProperty"))
            out.add((iri, f"{RDFS}domain", own))
            out.add((iri, f"{RDFS}range", vocab.classes_of[ep.target][0]))
    return sorted(out)


def plan_export(
    vocab: Vocabulary, domains: dict[str, str | None], scope: Scope, value: str | None = None
) -> ExportPlan:
    """The export of ``scope`` (a domain id, or a class IRI, as ``value``) for the role whose
    vocabulary ``vocab`` is."""
    labels = _labels_in_scope(vocab, domains, scope, value)
    queries: list[str] = []
    for label in labels:
        # A label's instances are those of its own table class; a class term's, of every table
        # it grounds, each its own label.
        typed_by = vocab.classes_of[label][0]
        queries.append(_instances_query(vocab, label, typed_by))
        queries += _edge_queries(vocab, label, typed_by)
    queries += _axiom_queries()
    return ExportPlan(queries, _declarations(vocab, labels))


__all__ = ["RDF", "ExportPlan", "Scope", "ScopeRefused", "plan_export"]
