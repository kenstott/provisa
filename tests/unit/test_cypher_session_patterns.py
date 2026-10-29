# Copyright (c) 2026 Kenneth Stott
# Canary: 72d93003-102c-4481-ad60-04d875d65826
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Patterns read in a graph session (REQ-347, REQ-348, REQ-2176): openCypher TCK scenarios of
the Match*, MatchWhere*, Pattern*, Path*, Quantifier*, ExistentialSubquery* and
TriadicSelection* features, run on DuckDB through the session's own read path."""

# Requirements: REQ-347, REQ-348, REQ-2176

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _sorted(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


# --- a session's own node carries only its own properties ----------------------------------------


def test_a_session_node_returned_by_its_label_carries_no_key_property(monkeypatch):
    # TCK MatchWhere5 [1]: a node the session created has the properties it was created with;
    # the built-in _id is a model node's key (REQ-2136), which a session node has not.
    s = Session(monkeypatch, {})
    s.run("CREATE (root:Root {name: 'x'})-[:T]->(:TextNode {var: 'text'})")
    rows = s.run("MATCH (:Root {name: 'x'})-->(i:TextNode) RETURN i")
    assert [r["i"]["properties"] for r in rows] == [{"var": "text"}]


# --- a walk read in a WITH ------------------------------------------------------------------------


def test_a_walk_read_by_a_with_is_defined_ahead_of_it(monkeypatch):
    # TCK Quantifier1 [8]: the walk's recursive CTE comes before the WITH stage that reads it.
    s = Session(monkeypatch, {})
    s.run("CREATE (:SNodes)-[:RA]->(:A {name: 'a'})-[:RB]->(:B {name: 'b'})")
    rows = s.run(
        "MATCH p = (:SNodes)-[*0..2]->(x) WITH tail(nodes(p)) AS nodes "
        "RETURN size(nodes) AS n, none(x IN nodes WHERE x.name = 'b') AS result"
    )
    assert _sorted(rows) == [(0, True), (1, True), (2, False)]


def test_a_walks_node_list_carried_by_a_with_is_its_nodes(monkeypatch):
    # TCK Quantifier1 [8]: tail(nodes(p)) carried by a WITH is a list of nodes, each property of
    # which a quantifier reads, and RETURN gives those nodes.
    s = Session(monkeypatch, {})
    s.run("CREATE (:SNodes)-[:RA]->(:A {name: 'a'})")
    rows = s.run(
        "MATCH p = (:SNodes)-[*0..1]->(x) WITH tail(nodes(p)) AS nodes "
        "RETURN nodes, any(x IN nodes WHERE x.name = 'a') AS result"
    )
    got = sorted(([n["properties"] for n in r["nodes"]], r["result"]) for r in rows)
    assert got == [([], False), ([{"name": "a"}], True)]


def test_a_quantifier_reads_a_property_of_each_relationship_of_a_walk(monkeypatch):
    # TCK Quantifier3 [9]
    s = Session(monkeypatch, {})
    s.run("CREATE (:S)-[:RA {name: 'a'}]->()-[:RB {name: 'b'}]->()")
    rows = s.run(
        "MATCH p = (:S)-[*1..2]->(x) "
        "RETURN length(p) AS l, all(r IN relationships(p) WHERE r.name = 'a') AS result"
    )
    assert _sorted(rows) == [(1, True), (2, False)]
