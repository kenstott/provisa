# Copyright (c) 2026 Kenneth Stott
# Canary: 2be84f06-d17c-4c3a-8a95-71c6e0f3b4d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the W3C SPARQL runner does to a query and an answer (REQ-2167): the query's constant IRIs
mapped onto the fixture's, the answer's mapped back, and the answer compared with the suite's
expected result. Nothing else of a query changes, so what runs is the suite's own query.

Solutions compare as multisets: blank nodes by a one-to-one mapping, numbers, booleans and dates
of one datatype by value, every other term exactly. ORDER BY is not compared, since the suite's
expected results do not separate ties."""

# Requirements: REQ-2167

from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from rdflib import BNode, Graph, Literal, URIRef, XSD
from rdflib.compare import isomorphic
from rdflib.query import Result

_TOKEN = re.compile(
    r"""
    (?P<comment>\#[^\n]*)
    |(?P<string>\"\"\"(?:[^"\\]|\\.|"(?!""))*\"\"\"
        |'''(?:[^'\\]|\\.|'(?!''))*'''
        |"(?:[^"\\\n]|\\.)*"
        |'(?:[^'\\\n]|\\.)*')
    |(?P<iri><[^<>"{}|^`\\\s]*>)
    |(?P<pname>(?P<prefix>[A-Za-z][\w.\-]*)?:(?P<local>(?:[\w\-.%:]|\\.)*))
    |(?P<word>[?$]?[A-Za-z_]\w*)
    """,
    re.VERBOSE,
)


def map_query(text: str, base: str, terms: dict[str, str]) -> str:
    """``text`` with each constant IRI ``terms`` holds written as the IRI it maps to, resolving
    its prologue as a SPARQL parser does. Strings, comments and every other token stay."""
    prefixes: dict[str, str] = {}
    pending: str | None = None  # the keyword whose IRI comes next
    prefix_name = ""
    out: list[str] = []
    at = 0
    for m in _TOKEN.finditer(text):
        kind = m.lastgroup if m.lastgroup not in ("prefix", "local") else "pname"
        if m.group("iri") is not None:
            kind = "iri"
        elif m.group("pname") is not None:
            kind = "pname"
        replacement = None
        if kind == "word":
            word = m.group().upper()
            pending = word if word in ("PREFIX", "BASE") else None
        elif kind == "pname":
            if pending == "PREFIX":
                prefix_name = m.group("prefix") or ""
                pending = "PREFIX-IRI"
            else:
                local = m.group("local")
                trailing = len(local) - len(local.rstrip("."))
                iri = prefixes.get(m.group("prefix") or "", None)
                if iri is not None:
                    name = iri + re.sub(r"\\(.)", r"\1", local.rstrip("."))
                    if name in terms:
                        replacement = f"<{terms[name]}>" + "." * trailing
        elif kind == "iri":
            iri = urljoin(base, m.group()[1:-1])
            if pending == "PREFIX-IRI":
                prefixes[prefix_name] = iri
                pending = None
            elif pending == "BASE":
                base = iri
                pending = None
            elif iri in terms:
                replacement = f"<{terms[iri]}>"
        if replacement is not None:
            out.append(text[at : m.start()])
            out.append(replacement)
            at = m.end()
    out.append(text[at:])
    return "".join(out)


def read_back(term: Any, reverse: dict[str, Any]) -> Any:
    """An answer's term as the dataset's: a model IRI as the IRI or blank node it stands for, and
    a string spelling one (STR of a subject) as that IRI's own spelling."""
    if isinstance(term, URIRef):
        return reverse.get(str(term), term)
    if isinstance(term, Literal) and str(term) in reverse:
        return Literal(str(reverse[str(term)]), datatype=term.datatype, lang=term.language)
    return term


def _value_key(lit: Literal) -> tuple:
    if lit.language:
        return ("lang", str(lit), lit.language.lower())
    datatype = str(lit.datatype or XSD.string)
    value = lit.value
    if lit.datatype in (XSD.decimal, XSD.integer, XSD.double, XSD.float, XSD.boolean, XSD.date):
        if value is not None and not lit.ill_typed:
            return ("value", datatype, float(value) if lit.datatype == XSD.double else value)
    if lit.datatype == XSD.dateTime and value is not None:
        return ("value", datatype, value)
    return ("lexical", datatype, str(lit))


def same_term(a: Any, b: Any) -> bool:
    if isinstance(a, Literal) and isinstance(b, Literal):
        return _value_key(a) == _value_key(b)
    return a == b


def _matches(a: dict, b: dict, bnodes: dict) -> dict | None:
    """``bnodes`` extended so solution ``a`` is solution ``b``, or None."""
    if set(a) != set(b):
        return None
    mapping = dict(bnodes)
    for var, term in a.items():
        other = b[var]
        if isinstance(term, BNode) or isinstance(other, BNode):
            if not (isinstance(term, BNode) and isinstance(other, BNode)):
                return None
            if mapping.get(term, other) != other:
                return None
            if term not in mapping and other in mapping.values():
                return None
            mapping[term] = other
        elif not same_term(term, other):
            return None
    return mapping


def same_solutions(actual: list[dict], expected: list[dict]) -> bool:
    """Whether the two lists of solutions are one multiset, under a one-to-one blank node map."""
    if len(actual) != len(expected):
        return False

    def assign(i: int, left: list[dict], bnodes: dict) -> bool:
        if i == len(actual):
            return True
        for n, candidate in enumerate(left):
            mapping = _matches(actual[i], candidate, bnodes)
            if mapping is not None and assign(i + 1, left[:n] + left[n + 1 :], mapping):
                return True
        return False

    return assign(0, list(expected), {})


def bindings(result: Result) -> list[dict]:
    return [
        {str(k): v for k, v in row.items() if v is not None}
        for row in (dict(b) for b in result.bindings)
    ]


_FORMATS = {
    ".srx": "xml",
    ".srj": "json",
    ".csv": "csv",
    ".tsv": "tsv",
}
_GRAPHS = {".ttl": "turtle", ".nt": "nt", ".rdf": "xml"}


def expected_of(path: Path, base: str) -> tuple[str, Any]:
    """(the form of the expected result, its value): ``boolean``, ``solutions`` or ``graph``."""
    if path.suffix in _GRAPHS:
        g = Graph()
        g.parse(path, format=_GRAPHS[path.suffix], publicID=base)
        return "graph", g
    result = Result.parse(io.BytesIO(path.read_bytes()), format=_FORMATS[path.suffix])
    if result.type == "ASK":
        return "boolean", bool(result.askAnswer)
    return "solutions", bindings(result)


def accept_of(path: Path) -> str:
    """The media type the suite's expected result is in, asked of the endpoint."""
    return {
        ".csv": "text/csv",
        ".tsv": "text/tab-separated-values",
        ".srx": "application/sparql-results+xml",
        ".srj": "application/sparql-results+json",
    }.get(path.suffix, "text/turtle")


def read_rows(
    rows: list[dict], reverse: dict[str, Any], scaffolding: tuple[str, ...]
) -> list[dict]:
    """Solutions as the dataset's: each term mapped back, and a solution holding what the fixture
    added (a key column read as a predicate, a junction row) left out, since the dataset has no
    such triple."""
    return [
        {k: read_back(v, reverse) for k, v in row.items()}
        for row in rows
        if not any(str(v).startswith(scaffolding) for v in row.values())
    ]


def answer(
    body: bytes, media: str, reverse: dict[str, Any], scaffolding: tuple[str, ...]
) -> tuple[str, Any]:
    """The endpoint's body read as (form, value), its terms mapped back to the dataset's."""
    kind = media.split(";")[0].strip().lower()
    if kind in ("text/turtle", "application/n-triples"):
        g = Graph()
        g.parse(data=body, format="turtle")
        out = Graph()
        for row in read_rows([{"s": s, "p": p, "o": o} for s, p, o in g], reverse, scaffolding):
            out.add((row["s"], row["p"], row["o"]))
        return "graph", out
    fmt = {
        "application/sparql-results+xml": "xml",
        "application/sparql-results+json": "json",
        "text/csv": "csv",
        "text/tab-separated-values": "tsv",
    }[kind]
    result = Result.parse(io.BytesIO(body), format=fmt)
    if result.type == "ASK":
        return "boolean", bool(result.askAnswer)
    return "solutions", read_rows(bindings(result), reverse, scaffolding)


def compare(actual: tuple[str, Any], expected: tuple[str, Any]) -> str | None:
    """None when the answer is the expected result, else what differs."""
    form, value = actual
    want_form, want = expected
    if form != want_form:
        return f"answered a {form}, expected a {want_form}"
    if form == "boolean":
        return None if value == want else f"answered {value}, expected {want}"
    if form == "graph":
        if isomorphic(value, want):
            return None
        return f"answered a graph of {len(value)} triples, not the expected {len(want)}"
    if same_solutions(value, want):
        return None
    return (
        f"answered {len(value)} solutions, expected {len(want)}: "
        f"{_shown(value)} expected {_shown(want)}"
    )


def _shown(rows: list[dict]) -> str:
    return json.dumps(
        [{k: v.n3() for k, v in sorted(r.items())} for r in rows[:3]], ensure_ascii=False
    )[:300]
