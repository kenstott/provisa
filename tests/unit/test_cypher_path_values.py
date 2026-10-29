# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1f0f8e-3c1b-4f53-9a8e-2b7a5e0c4d91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A path variable read as a value, not only returned bare (REQ-347): an item of a list, a
variable a WITH carries on, a pattern comprehension's projection -- and a path of no hops,
MATCH p = (a). Each had been the bare column ``p``, which no table holds ("Referenced column p
not found in FROM clause"). The scenarios are the openCypher TCK's (With1, ReturnOrderBy1,
WithOrderBy1, Comparison2, Pattern2), run on DuckDB over a session graph."""

# Requirements: REQ-347, REQ-2167

from __future__ import annotations

import json

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _session(monkeypatch, graph: str) -> Session:
    s = Session(monkeypatch, {})
    s.run(graph)
    return s


def _value(v):
    return json.loads(v) if isinstance(v, str) else v


def _labels(path: dict) -> list[str]:
    return [n["label"] for n in path["nodes"]]


def test_a_path_a_with_carries_is_returned(monkeypatch):
    # With1 [4]: a path of no hops, carried on by a WITH.
    s = _session(monkeypatch, "CREATE ()")
    (row,) = s.run("MATCH p = (a) WITH p RETURN p")
    assert len(row["p"]["nodes"]) == 1
    assert row["p"]["edges"] == []


def test_a_path_of_no_hops_is_its_one_node(monkeypatch):
    s = _session(monkeypatch, "CREATE (:A {num: 1})")
    (row,) = s.run("MATCH p = (a) RETURN p")
    assert _labels(row["p"]) == ["A"]
    assert row["p"]["edges"] == []


def test_a_path_a_with_renames_is_its_hops(monkeypatch):
    s = _session(monkeypatch, "CREATE (:A)-[:T]->(:B)")
    (row,) = s.run("MATCH p = (a:A)-[r:T]->(b:B) WITH p AS q RETURN q")
    assert _labels(row["q"]) == ["A", "B"]
    assert [e["type"] for e in row["q"]["edges"]] == ["T"]


def test_a_path_is_an_item_of_a_list(monkeypatch):
    # ReturnOrderBy1 [11], WithOrderBy1 [21]: a path beside its node and relationship in a list.
    s = _session(monkeypatch, "CREATE (:N)-[:REL]->()")
    rows = s.run("MATCH p = (n:N)-[r:REL]->() UNWIND [n, r, p] AS types RETURN types")
    values = [_value(r["types"]) for r in rows]
    (path,) = [v for v in values if "nodes" in v]
    assert _labels(path) == ["N", ""]
    assert [e["type"] for e in path["edges"]] == ["REL"]


def test_a_pattern_comprehension_answers_its_paths(monkeypatch):
    # Pattern2 [3]
    s = _session(monkeypatch, "CREATE (a:A), (b:B) CREATE (a)-[:T]->(b)")
    (row,) = s.run("MATCH (a:A), (b:B) RETURN [p = (a)-->(b) | p] AS list")
    (path,) = row["list"]
    assert _labels(path) == ["A", "B"]
    assert [e["type"] for e in path["edges"]] == ["T"]
