# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2a6b1d-3c4f-4a5e-b7d8-9f0a1b2c3d4e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a graph-session write did (REQ-2176), counted as Neo4j counts it: the session's nodes,
relationships and labels it wrote, and each governed part as the governed path wrote it, once.
Run through the seam exactly as a surface runs it."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from provisa.cypher.counters import counting
from tests.unit.test_cypher_governed_merge import _rows, _run, _session

pytestmark = pytest.mark.unit


def _stats(s, statement: str, **params) -> dict:
    with counting() as counted:
        _run(s, statement, **params)
    assert counted.unknown == []
    return counted.stats()


def test_a_session_create_counts_its_nodes_relationships_labels_and_properties(monkeypatch):
    s = _session(monkeypatch)
    stats = _stats(s, "CREATE (:Temp:Note {k: 1, z: null})-[:LINKS {w: 2}]->(:Temp)")
    assert stats == {
        "nodes-created": 2,
        "relationships-created": 1,
        "labels-added": 3,
        "properties-set": 2,
        "contains-updates": True,
    }


def test_a_create_per_row_counts_each_row_it_made(monkeypatch):
    s = _session(monkeypatch)
    assert _stats(s, "UNWIND [1, 2, 3] AS x CREATE (:Temp {k: x})") == {
        "nodes-created": 3,
        "labels-added": 3,
        "properties-set": 3,
        "contains-updates": True,
    }


def test_a_create_of_a_session_node_and_a_model_row_counts_each_once(monkeypatch):
    s = _session(monkeypatch)
    stats = _stats(s, "CREATE (:Person {id: 7, name: 'Di'})-[:TAGGED]->(:Temp {k: 1})")
    assert len(_rows(s, "person")) == 4
    assert stats == {
        "nodes-created": 2,
        "relationships-created": 1,
        "labels-added": 2,
        "properties-set": 3,
        "contains-updates": True,
    }


def test_a_label_set_counts_each_label_a_node_did_not_carry(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "CREATE (:Temp {k: 1}), (:Temp {k: 2})")
    assert _stats(s, "MATCH (n:Temp) SET n:Seen") == {"labels-added": 2, "contains-updates": True}
    assert _stats(s, "MATCH (n:Temp) SET n:Seen") == {}


def test_a_label_on_a_model_node_is_counted_as_added(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "CREATE (:Temp {k: 1})")  # the session holds ephemeral objects
    assert _stats(s, "MATCH (n:Person {name: 'Ann'}) SET n:Star") == {
        "labels-added": 1,
        "contains-updates": True,
    }


def test_a_label_remove_counts_each_label_a_node_carried(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "CREATE (:Temp:Seen {k: 1}), (:Temp {k: 2})")
    assert _stats(s, "MATCH (n:Temp) REMOVE n:Seen") == {
        "labels-removed": 1,
        "contains-updates": True,
    }


def test_a_session_merge_that_matched_creates_nothing(monkeypatch):
    s = _session(monkeypatch)
    assert _stats(s, "MERGE (n:Temp {k: 1})") == {
        "nodes-created": 1,
        "labels-added": 1,
        "properties-set": 1,
        "contains-updates": True,
    }
    assert _stats(s, "MERGE (n:Temp {k: 1})") == {}


def test_a_session_merge_counts_its_on_match_labels(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "CREATE (:Temp {k: 1})")
    assert _stats(s, "MERGE (n:Temp {k: 1}) ON MATCH SET n:Again") == {
        "labels-added": 1,
        "contains-updates": True,
    }


def test_a_session_delete_counts_its_nodes_and_the_relationships_detached(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "CREATE (a:Temp {k: 1})-[:LINKS]->(:Temp {k: 2}), (a)-[:LINKS]->(:Temp {k: 3})")
    assert _stats(s, "MATCH (n:Temp {k: 1}) DETACH DELETE n") == {
        "nodes-deleted": 1,
        "relationships-deleted": 2,
        "contains-updates": True,
    }


def test_a_session_relationship_delete_counts_it(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "CREATE (:Temp {k: 1})-[:LINKS]->(:Temp {k: 2})")
    assert _stats(s, "MATCH (:Temp)-[r:LINKS]->(:Temp) DELETE r") == {
        "relationships-deleted": 1,
        "contains-updates": True,
    }


def test_a_delete_of_a_model_node_in_a_session_counts_it_once(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "CREATE (:Temp {k: 1})")
    assert _stats(s, "MATCH (n:Greeting {message: 'hello'}) DELETE n") == {
        "nodes-deleted": 1,
        "contains-updates": True,
    }


def test_a_session_property_set_and_remove_count_as_properties_set(monkeypatch):
    # A session property SET or REMOVE (then.py's write path) counts each property it writes.
    s = _session(monkeypatch)
    _run(s, "CREATE (:Temp {k: 1}), (:Temp {k: 2})")
    assert _stats(s, "MATCH (n:Temp) SET n.z = 5") == {
        "properties-set": 2,
        "contains-updates": True,
    }
    assert _stats(s, "MATCH (n:Temp {k: 1}) REMOVE n.z") == {
        "properties-set": 1,
        "contains-updates": True,
    }
