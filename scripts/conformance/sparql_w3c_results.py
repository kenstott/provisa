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
mapped onto the fixture's, a FILTER keeping a variable predicate from what the fixture added, the
answer's terms mapped back, and the answer compared with the suite's expected result. Nothing else
of a query changes, so what runs is the suite's own query.

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
from collections import defaultdict
from urllib.parse import urljoin

from rdflib import RDF, BNode, Graph, Literal, URIRef, Variable, XSD
from rdflib.compare import isomorphic
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
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


_ABSOLUTE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:")
#: What moves the guard's position: brackets, and a variable or the CONSTRUCT keyword.
_BRACKET = re.compile(
    r"""
    (?P<skip>\#[^\n]*
        |\"\"\"(?:[^"\\]|\\.|"(?!""))*\"\"\"
        |'''(?:[^'\\]|\\.|'(?!''))*'''
        |"(?:[^"\\\n]|\\.)*"
        |'(?:[^'\\\n]|\\.)*'
        |<[^<>"{}|^`\\\s]*>)
    |[{}()]
    |[?$]\w+
    |(?i:\bCONSTRUCT\b)
    """,
    re.VERBOSE,
)


def _resolved(base: str, iri: str) -> str:
    """``iri`` against ``base``. An absolute IRI stands as written: urljoin drops an empty
    fragment, the ``#`` that ends a namespace."""
    return iri if _ABSOLUTE.match(iri) else urljoin(base, iri)


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
            iri = _resolved(base, m.group()[1:-1])
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


def _variable_predicates(text: str) -> dict[str, str | None]:
    """Each variable a triple of ``text`` has as its predicate, with its object variable. A query
    this parser refuses has none here: the endpoint refuses it by name."""
    found: dict[str, str | None] = {}

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("triples") is not None and getattr(node, "name", "") == "BGP":
                for _s, p, o in node["triples"]:
                    if isinstance(p, Variable):
                        found[str(p)] = str(o) if isinstance(o, Variable) else None
            for value in node.values():
                visit(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                visit(value)

    try:
        visit(translateQuery(parseQuery(text)).algebra)
    except Exception:  # noqa: BLE001 -- rdflib raises pyparsing's and its own errors alike
        return {}
    return found


def guard_open_patterns(
    text: str, keys: tuple[str, ...], untyped: tuple[str, ...], *, typed: bool
) -> str:
    """``text`` with a FILTER in each group reading a variable predicate, keeping it from what the
    fixture adds to the dataset: each table's key column, read as a predicate, and the table class
    of an untyped subject, read as its ``rdf:type``. Filtering the answer afterwards cannot serve
    an aggregate over such a pattern. A dataset with no classes has no ``rdf:type`` at all; one
    with classes keeps its own. A CONSTRUCT WHERE takes no FILTER, and its answer's triples are
    left out by read_rows."""
    open_vars = _variable_predicates(text)
    if not open_vars or re.search(r"\bCONSTRUCT\s+WHERE\b", text, re.I):
        return text
    stack: list[int] = []  # the groups open at this point, by their number
    closes: dict[int, int] = {}  # a group's number -> where it closes
    wanted: dict[int, set[str]] = defaultdict(set)
    parens = 0
    groups = 0
    template_next = False
    template_depth: int | None = None
    for m in _BRACKET.finditer(text):
        token = m.group()
        if m.group("skip") is not None:
            continue
        if token.upper() == "CONSTRUCT":
            template_next = True
        elif token == "{":
            groups += 1
            stack.append(groups)
            if template_next:
                template_depth, template_next = len(stack), False
        elif token == "}":
            number = stack.pop()
            closes[number] = m.start()
            if template_depth == len(stack) + 1:
                template_depth = None
        elif token == "(":
            parens += 1
        elif token == ")":
            parens -= 1
        elif token[1:] in open_vars and stack and not parens and template_depth is None:
            wanted[stack[-1]].add(token[1:])
    out = text
    for number in sorted(wanted, key=lambda n: closes[n], reverse=True):
        guards = "".join(
            _guards(v, open_vars[v], keys, untyped, typed) for v in sorted(wanted[number])
        )
        at = closes[number]
        out = f"{out[:at]}{guards} {out[at:]}"
    return out


def _guards(
    var: str, obj: str | None, keys: tuple[str, ...], untyped: tuple[str, ...], typed: bool
) -> str:
    left_out = [f"<{k}>" for k in keys]
    if not typed:
        left_out.append(f"<{RDF.type}>")
    said = f" FILTER(?{var} NOT IN ({', '.join(left_out)}))" if left_out else ""
    if typed and untyped and obj is not None:
        classes = ", ".join(f"<{c}>" for c in untyped)
        said += f" FILTER(?{var} != <{RDF.type}> || ?{obj} NOT IN ({classes}))"
    return said


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
    added (a key column read as a predicate, an untyped table's class) left out, since the dataset
    has no such triple."""
    return [
        {k: read_back(v, reverse) for k, v in row.items()}
        for row in rows
        if not any(str(v) in scaffolding for v in row.values())
    ]


def answer(
    body: bytes, media: str, reverse: dict[str, Any], scaffolding: frozenset[str]
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
