# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1d2c84-3b7e-4a19-9e52-d40c7a8b1e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The W3C SPARQL 1.1 test suite's manifests, read into cases (REQ-2167).

The suite is w3c/rdf-tests at a pinned commit, fetched by the job and never vendored. Each test
group is a directory of ``sparql/sparql11`` with a ``manifest.ttl``. The three scopes of REQ-2167
are the groups the suite's own ``manifest-sparql11-query.ttl``, ``-update.ttl`` and
``-results.ttl`` include, so nothing here chooses which tests count."""

# Requirements: REQ-2167

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rdflib import RDF, Graph, Namespace, URIRef
from rdflib.collection import Collection

MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
QT = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
UT = Namespace("http://www.w3.org/2009/sparql/tests/test-update#")

#: The suite's manifests of REQ-2167's scope: query, update and result formats.
SCOPES = (
    "manifest-sparql11-query.ttl",
    "manifest-sparql11-update.ttl",
    "manifest-sparql11-results.ttl",
)

#: Each test type the run answers, by how a case of it is checked.
KINDS = {
    "QueryEvaluationTest": "query",
    "CSVResultFormatTest": "query",
    "PositiveSyntaxTest11": "positive-query",
    "NegativeSyntaxTest11": "negative-query",
    "PositiveUpdateSyntaxTest11": "positive-update",
    "NegativeUpdateSyntaxTest11": "negative-update",
    "UpdateEvaluationTest": "update",
}


@dataclass(frozen=True)
class Case:
    group: str  # the test group: its directory under sparql/sparql11
    name: str  # the suite's test id: the entry's fragment
    title: str
    kind: str  # a value of KINDS
    action: Path  # the query or update file
    data: tuple[Path, ...] = ()  # the default graph's files
    graph_data: tuple[Path, ...] = ()  # the named graphs' files
    result: Path | None = None  # the expected result's file


def _path(term: object) -> Path:
    return Path(str(term).removeprefix("file://"))


def included(root: Path) -> list[Path]:
    """Every group manifest of the three scopes, in the suite's order, each once."""
    out: dict[Path, None] = {}
    for scope in SCOPES:
        path = root / scope
        g = Graph()
        g.parse(path, format="turtle", publicID=path.resolve().as_uri())
        for _, _, head in g.triples((None, MF.include, None)):
            out.update((_path(i), None) for i in Collection(g, head))
    return list(out)


def cases_of(manifest: Path) -> list[Case]:
    """The cases of one group manifest, in its order. An entry of a type no scope has is refused:
    a suite update that adds one must be read, not dropped."""
    g = Graph()
    g.parse(manifest, format="turtle", publicID=manifest.resolve().as_uri())
    out: list[Case] = []
    for _, _, head in g.triples((None, MF.entries, None)):
        for entry in Collection(g, head):
            type_ = g.value(entry, RDF.type)
            short = str(type_).rsplit("#", 1)[-1]
            if short not in KINDS:
                raise ValueError(f"{manifest}: {entry} is a {short}, of no kind read here")
            out.append(_case(g, manifest, entry, KINDS[short]))
    return out


def _case(g: Graph, manifest: Path, entry: object, kind: str) -> Case:
    group = manifest.parent.name
    name = str(entry).rsplit("#", 1)[-1]
    title = str(g.value(entry, MF.name) or name)  # type: ignore[arg-type]
    action = g.value(entry, MF.action)  # type: ignore[arg-type]
    result = g.value(entry, MF.result)  # type: ignore[arg-type]
    expected = _path(result) if isinstance(result, URIRef) else None
    if isinstance(action, URIRef):  # a syntax test's action is its file
        return Case(group, name, title, kind, _path(action), result=expected)
    request = g.value(action, QT.query) or g.value(action, UT.request)  # type: ignore[arg-type]
    if request is None:
        raise ValueError(f"{manifest}: {entry} names no query or request")
    return Case(
        group,
        name,
        title,
        kind,
        _path(request),
        tuple(sorted(_path(d) for d in g.objects(action, QT.data))),  # type: ignore[arg-type]
        tuple(sorted(_path(d) for d in g.objects(action, QT.graphData))),  # type: ignore[arg-type]
        expected,
    )


def read(root: Path) -> list[Case]:
    """Every case of the three scopes."""
    return [c for manifest in included(root) for c in cases_of(manifest)]
