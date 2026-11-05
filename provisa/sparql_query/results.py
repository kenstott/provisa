# Copyright (c) 2026 Kenneth Stott
# Canary: 43c76bf2-c68b-4bd4-bb26-6cee509f1bbb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A SPARQL query's answer as the SPARQL 1.1 Protocol returns it (REQ-2135): the rows the
pipeline returned, each RDF term a JSON value (:mod:`provisa.gql.terms`), made RDF here -- a
subject's IRI built from its table and key values (:mod:`provisa.gql.iri`) -- and written by
rdflib's own serializers, so escaping and the formats are theirs.

A SELECT or ASK is written as SPARQL results (JSON, XML, CSV or TSV), a CONSTRUCT as Turtle,
N-Triples or JSON-LD. A CONSTRUCT's triples are a graph, so each is kept once.
"""

# Requirements: REQ-2135

from __future__ import annotations

import io
from typing import Any

from rdflib import Graph, Literal, URIRef, Variable
from rdflib.query import Result

from provisa.gql.iri import subject_iri
from provisa.sparql_query.vocabulary import term_iri

#: (media type, rdflib format), the first the form's default.
RESULT_FORMATS = (
    ("application/sparql-results+json", "json"),
    ("application/sparql-results+xml", "xml"),
    ("text/csv", "csv"),
    ("text/tab-separated-values", "tsv"),
)
GRAPH_FORMATS = (
    ("text/turtle", "turtle"),
    ("application/n-triples", "nt"),
    ("application/ld+json", "json-ld"),
)
_ALIASES = {
    "application/json": "application/sparql-results+json",
    "text/plain": "application/n-triples",
}


class NotAcceptable(ValueError):
    """No media type the request accepts is one this form of answer is written in."""


def negotiate(accept: str | None, form: str) -> tuple[str, str]:
    """The (media type, rdflib format) of the answer: the first of the request's Accept types,
    by quality, that the form is written in; the form's default when it accepts anything."""
    formats = GRAPH_FORMATS if form == "CONSTRUCT" else RESULT_FORMATS
    if form == "ASK":
        formats = RESULT_FORMATS[:2]  # a boolean has no CSV or TSV
    if not accept:
        return formats[0]
    ranked = []
    for i, part in enumerate(accept.split(",")):
        media, *params = (p.strip() for p in part.split(";"))
        q = 1.0
        for p in params:
            if p.startswith("q="):
                q = float(p[2:])
        ranked.append((-q, i, _ALIASES.get(media.lower(), media.lower())))
    by_type = dict(formats)
    for _, _, media in sorted(ranked):
        if media in ("*/*", "application/*", "text/*"):
            return formats[0]
        if media in by_type:
            return media, by_type[media]
    raise NotAcceptable(
        f"This answer is written as {', '.join(m for m, _ in formats)}; the request accepts "
        f"{accept}"
    )


def rdf_term(value: dict | None, base: str) -> Any:
    """One RDF term of the answer as rdflib's."""
    if value is None:
        return None
    if "node" in value:
        n = value["node"]
        return URIRef(subject_iri(base, n["domain"], n["table"], n["key"]))
    if "iri" in value:
        return URIRef(value["iri"])
    if "term" in value:
        # REQ-2163: a glossary term of ontology._axioms, named by its vocabulary IRI.
        return URIRef(term_iri(base, value["term"]))
    datatype = value.get("datatype")
    return Literal(
        value["literal"],
        lang=value.get("lang"),
        datatype=URIRef(datatype) if datatype and not value.get("lang") else None,
    )


def write_select(variables: list[str], rows: list[dict], base: str, fmt: str) -> bytes:
    result = Result("SELECT")
    result.vars = [Variable(v) for v in variables]
    result.bindings = [
        {Variable(v): t for v in variables if (t := rdf_term(row.get(v), base)) is not None}
        for row in rows
    ]
    return _serialize(result, fmt)


def write_ask(answer: bool, fmt: str) -> bytes:
    result = Result("ASK")
    result.askAnswer = answer
    return _serialize(result, fmt)


def _serialize(result: Result, fmt: str) -> bytes:
    buf = io.BytesIO()
    result.serialize(destination=buf, format=fmt)
    return buf.getvalue()


def write_construct(rows: list[dict], base: str, prefixes: dict[str, str], fmt: str) -> bytes:
    graph = Graph(bind_namespaces="none")
    for prefix, iri in prefixes.items():
        graph.bind(prefix, iri)
    for row in rows:
        s, p, o = (rdf_term(row.get(k), base) for k in ("s", "p", "o"))
        if s is not None and p is not None and o is not None:
            graph.add((s, p, o))
    return graph.serialize(format=fmt, encoding="utf-8")
