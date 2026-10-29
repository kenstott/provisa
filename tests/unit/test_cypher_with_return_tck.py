# Copyright (c) 2026 Kenneth Stott
# Canary: 8cb0f845-2913-479e-aed2-e0207a5d07f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WITH, RETURN, ORDER BY, SKIP, LIMIT, UNION and UNWIND over a session's graph, as the openCypher
TCK's With*, Return*, Union* and Unwind* features run them (REQ-347, REQ-2167, REQ-2176). Each
test names the scenario it stands for."""

# Requirements: REQ-347, REQ-2167, REQ-2176

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _session(monkeypatch, setup: str | None = None) -> Session:
    s = Session(monkeypatch, {})
    if setup:
        s.run(setup)
    return s


def _props(rows, column):
    return [r[column]["properties"] for r in rows]


# --- a node only the session holds carries no _id ----------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:A) RETURN a",
        "MATCH (a:A) WITH a RETURN a",  # WithWhere3 [2], WithOrderBy4 [1]
        "MATCH (a:A) RETURN a AS a UNION MATCH (a:A) RETURN a AS a",  # Union1 [4]
    ],
)
def test_a_session_node_has_only_its_own_properties(monkeypatch, query):
    s = _session(monkeypatch, "CREATE (:A {num: 1})")
    assert _props(s.run(query), "a") == [{"num": 1}]


def test_a_session_node_read_by_label_is_still_found_by_a_later_create(monkeypatch):
    # The node's identity is its id (Label|e:N) once _id is not among its properties.
    s = _session(monkeypatch, "CREATE (:A {num: 1})")
    s.run("MATCH (a:A) CREATE (a)-[:R]->(:B {num: 2})")
    rows = s.run("MATCH (a:A)-[:R]->(b:B) RETURN a.num AS a, b.num AS b")
    assert rows == [{"a": 1, "b": 2}]


# --- a map of nodes keeps each node apart over Bolt -----------------------------------------


def test_nodes_inside_a_returned_map_get_their_own_ids():
    # Return2 [13]: {node1: n, rel: r, node2: m}. A node left with its text id packs as id 0,
    # and the driver reads two nodes of one id as one node with both labels.
    from provisa.gql.assembler import _apply_id_map

    a = {"id": "A|e:1", "label": "A", "tableLabel": "A", "properties": {}}
    b = {"id": "B|e:2", "label": "B", "tableLabel": "B", "properties": {}}
    rel = {"id": "r:3", "type": "T", "startNode": a, "endNode": b, "properties": {}}
    out = _apply_id_map({"node1": a, "rel": rel, "node2": b}, {"A|e:1": 7, "B|e:2": 8})
    assert out["node1"]["id"] == 7 and out["node2"]["id"] == 8
    assert out["rel"]["startNode"]["id"] == 7 and out["rel"]["endNode"]["id"] == 8
