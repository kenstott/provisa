# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7a91d4-52be-4f08-9a16-e0d4b7c2a853
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One-hop pattern comprehensions with an unnamed end, a named relationship, several relationship
types, or a path variable, read inside functions and aggregates (REQ-347, REQ-2167; TCK Pattern2
and List6). Run on DuckDB over a session graph."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_GRAPH = (
    "CREATE (a:A {name: 'a'}), (b:B {name: 'b'}), (c:B {name: 'c'}), (d:C {name: 'd'}), "
    "(a)-[:T]->(b), (a)-[:T]->(c), (a)-[:U]->(d)"
)


def _run(monkeypatch, q):
    s = Session(monkeypatch, {})
    s.run(_GRAPH)
    return s.run(q)


def test_size_of_comprehension_with_unnamed_ends(monkeypatch):
    rows = _run(monkeypatch, "MATCH (n:A) RETURN size([(n)-->() | 1]) AS d")
    assert rows == [{"d": 3}]


def test_size_with_one_type(monkeypatch):
    rows = _run(monkeypatch, "MATCH (n:A) RETURN size([(n)-[:T]->() | 1]) AS d")
    assert rows == [{"d": 2}]


def test_size_with_several_types(monkeypatch):
    rows = _run(monkeypatch, "MATCH (n:A) RETURN size([(n)-[:T|U]->() | 1]) AS d")
    assert rows == [{"d": 3}]


def test_size_with_target_label(monkeypatch):
    rows = _run(monkeypatch, "MATCH (n:A) RETURN size([(n)-[:U]->(:C) | 1]) AS d")
    assert rows == [{"d": 1}]


def test_named_relationship(monkeypatch):
    rows = _run(monkeypatch, "MATCH (n:A) RETURN [(n)-[r:T]->(x) | type(r)] AS r")
    assert rows == [{"r": ["T", "T"]}]


def test_new_node_variable(monkeypatch):
    rows = _run(monkeypatch, "MATCH (n:A) RETURN [(n)-[:U]->(x:C) | x.name] AS r")
    assert rows == [{"r": ["d"]}]


def test_aggregate_on_comprehension(monkeypatch):
    rows = _run(monkeypatch, "MATCH (n:A) RETURN count([(n)-[:T]->() | 1]) AS c")
    assert rows == [{"c": 1}]


def test_comprehension_in_with_and_order_by(monkeypatch):
    rows = _run(
        monkeypatch,
        "MATCH (n:A) WITH n, size([(n)-->() | 1]) AS d RETURN n.name AS n, d ORDER BY d DESC, n",
    )
    assert rows == [{"n": "a", "d": 3}]


def test_comprehension_inside_list_comprehension(monkeypatch):
    rows = _run(
        monkeypatch,
        "MATCH (n:A) RETURN [x IN [(n)-[:T]->(m) | m.name] WHERE x <> 'b' | x] AS r",
    )
    assert rows == [{"r": ["c"]}]
