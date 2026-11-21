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

Scope is the whole graph, a domain, or one class. The triples are written as a Turtle file, as
N-Triples streamed one query's triples at a time, or loaded into a running store over the SPARQL
1.1 Graph Store HTTP Protocol, one query's triples per request, reporting the triples loaded and
each request the store refused.
"""

# Requirements: REQ-2165

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from rdflib import Graph, URIRef

from provisa.sparql_query.results import rdf_term
from provisa.sparql_query.vocabulary import OWL, RDF_TYPE, RDFS, SKOS, Vocabulary

Scope = Literal["graph", "domain", "class"]
Triple = tuple[Any, Any, Any]

#: The ontology schema's predicates, each read from ontology._axioms in a query of its own: the
#: compiler's axiom predicates (provisa.sparql_query.compiler._AXIOM_OBJECTS).
_AXIOM_PREDICATES = (
    f"{RDFS}subClassOf",
    f"{RDFS}subPropertyOf",
    f"{RDFS}domain",
    f"{RDFS}range",
    f"{SKOS}prefLabel",
    f"{SKOS}definition",
    f"{OWL}equivalentClass",
    f"{OWL}equivalentProperty",
    f"{SKOS}exactMatch",
)
_OWL_KINDS = (f"{OWL}Class", f"{OWL}ObjectProperty", f"{OWL}DatatypeProperty")

#: The Graph Store request body: N-Triples, which every store reads and which one request's
#: batch serializes to without prefixes.
NTRIPLES = "application/n-triples"


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
        f"CONSTRUCT {{ ?s <{RDF_TYPE}> <{k}> }} WHERE {{ ?s <{RDF_TYPE}> <{k}> }}"
        for k in _OWL_KINDS
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


#: One CONSTRUCT query answered through the one pipeline: its rows, each an ``s``/``p``/``o``.
Runner = Callable[[str], Awaitable[list[dict]]]


async def batches(plan: ExportPlan, run: Runner, base: str) -> AsyncIterator[list[Triple]]:
    """The export's triples, a batch at a time: the declarations, then each query's triples as
    ``run`` answers it."""
    yield [(URIRef(s), URIRef(p), URIRef(o)) for s, p, o in plan.declarations]
    for query in plan.queries:
        batch: list[Triple] = []
        for row in await run(query):
            s, p, o = (rdf_term(row.get(k), base) for k in ("s", "p", "o"))
            # A CONSTRUCT template triple an OPTIONAL left unbound is no triple (SPARQL 1.1 16.2).
            if s is not None and p is not None and o is not None:
                batch.append((s, p, o))
        yield batch


def _graph(triples: list[Triple], prefixes: dict[str, str]) -> Graph:
    graph = Graph(bind_namespaces="none")
    for prefix, iri in prefixes.items():
        graph.bind(prefix, iri)
    for triple in triples:
        graph.add(triple)
    return graph


async def turtle(plan: ExportPlan, run: Runner, base: str, prefixes: dict[str, str]) -> bytes:
    """The export as one Turtle document, under the vocabulary's prefixes."""
    triples: list[Triple] = []
    async for batch in batches(plan, run, base):
        triples += batch
    return _graph(triples, prefixes).serialize(format="turtle", encoding="utf-8")


def ntriples(batch: list[Triple]) -> bytes:
    """A batch as N-Triples lines: no prefixes, so batches written one after another are one
    document, and a large export is streamed a batch at a time."""
    return _graph(batch, {}).serialize(format="nt", encoding="utf-8")


@dataclass
class LoadReport:
    """A Graph Store load: the triples the store took, and why each request it refused was."""

    loaded: int = 0
    errors: list[str] = field(default_factory=list)


def graph_store_url(endpoint: str, graph: str | None) -> tuple[str, dict[str, str]]:
    """The Graph Store request's URL and query: the named graph ``graph``, else the default
    graph (SPARQL 1.1 Graph Store HTTP Protocol, 4.2 and 4.3)."""
    return endpoint, ({"graph": graph} if graph else {"default": ""})


async def load(
    plan: ExportPlan,
    run: Runner,
    base: str,
    client: Any,
    endpoint: str,
    graph: str | None,
    headers: dict[str, str],
    timeout: float,
) -> LoadReport:
    """The export POSTed to the Graph Store at ``endpoint`` (merged into ``graph``, or the default
    graph), one batch per request. A batch the store refuses is reported with its reason and the
    load goes on, as the Neo4j export reports each statement it could not import."""
    url, params = graph_store_url(endpoint, graph)
    report = LoadReport()
    async for batch in batches(plan, run, base):
        if not batch:
            continue
        resp = await client.post(
            url,
            params=params,
            content=ntriples(batch),
            headers={**headers, "Content-Type": NTRIPLES},
            timeout=timeout,
        )
        if resp.status_code // 100 == 2:
            report.loaded += len(batch)
        else:
            report.errors.append(
                f"{len(batch)} triples refused: HTTP {resp.status_code}: {resp.text[:200]}"
            )
    return report


__all__ = [
    "NTRIPLES",
    "ExportPlan",
    "LoadReport",
    "Runner",
    "Scope",
    "ScopeRefused",
    "batches",
    "graph_store_url",
    "load",
    "ntriples",
    "plan_export",
    "turtle",
]
