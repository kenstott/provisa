# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0c4f0e-1f7b-4b0e-9d4c-6f3a2b8e7c11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a governed Cypher write did, counted as Neo4j counts it (REQ-670): the rows each SQL
statement wrote, never the statement's shape. Run through the governed write path a surface runs
(written) over model tables in DuckDB, whose row counts are the source's own."""

# Requirements: REQ-670

from __future__ import annotations

import asyncio

import pytest

from provisa.cypher import counters
from provisa.cypher.counters import Counters, counting, counting_apart
from tests.unit.test_cypher_governed_merge import _rows, _run, _session

pytestmark = pytest.mark.unit


def _counted(s, statement: str, **params) -> Counters:
    with counting() as counted:
        _run(s, statement, **params)
    return counted


# --- the counters themselves ---


def test_only_the_counters_that_are_not_zero_are_stats_and_they_contain_updates():
    c = Counters(nodes_created=2, properties_set=3)
    assert c.stats() == {"nodes-created": 2, "properties-set": 3, "contains-updates": True}


def test_a_statement_that_changed_nothing_has_no_stats():
    assert Counters().stats() == {}


def test_a_collector_opened_inside_an_open_one_adds_to_it():
    with counting() as outer:
        counters.count(nodes_created=1)
        with counting() as inner:
            counters.count(nodes_created=1)
    assert inner is outer
    assert outer.nodes_created == 2


def test_a_collector_apart_counts_its_own_statement_alone():
    with counting() as outer:
        with counting_apart() as apart:
            counters.count(labels_added=1)
    assert apart.labels_added == 1 and outer.labels_added == 0


def test_a_write_no_surface_counts_is_told_to_no_one():
    counters.count(nodes_created=1)
    counters.unknown("nothing is open")


def _write(columns, rows, rowcount):
    async def write(sql, bound):
        assert sql and bound == []
        return columns, rows, rowcount

    return write


def test_the_rows_written_are_the_sources_row_count():
    with counting() as c:
        asyncio.run(
            counters.written_rows(
                _write([], [], 3), "DELETE FROM t WHERE a = 1", [], nodes_deleted=1
            )
        )
    assert c.nodes_deleted == 3


def test_with_no_row_count_the_rows_a_returning_gave_are_the_rows_written():
    with counting() as c:
        asyncio.run(
            counters.written_rows(
                _write(["_id"], [[1], [2]], None),
                "INSERT INTO t (a) VALUES (1), (2) RETURNING a AS _id",
                [],
                nodes_created=1,
                properties_set=1,
            )
        )
    assert (c.nodes_created, c.properties_set, c.unknown) == (2, 2, [])


def test_with_no_row_count_and_no_returning_the_count_is_unknown_never_guessed():
    with counting() as c:
        asyncio.run(
            counters.written_rows(
                _write([], [], None), "INSERT INTO t (a) VALUES (1)", [], nodes_created=1
            )
        )
    assert c.nodes_created == 0
    assert c.unknown == ["the source reported no row count for INSERT"]


# --- CREATE ---


def test_a_node_create_counts_its_node_label_and_each_property_given_a_value(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(s, "CREATE (n:Person {name: 'Di', age: $age})", age=None)
    assert c.stats() == {
        "nodes-created": 1,
        "labels-added": 1,
        "properties-set": 1,  # a null is no property
        "contains-updates": True,
    }


def test_a_relationship_create_counts_each_junction_row_it_wrote(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(
        s,
        "MATCH (a:Person {name: 'Ann'}) MATCH (b:Person) CREATE (a)-[:KNOWS {since: 2021}]->(b)",
    )
    assert len(_rows(s, "knows")) == 4
    assert c.stats() == {
        "relationships-created": 3,
        "properties-set": 3,
        "contains-updates": True,
    }


def test_a_create_of_rows_and_the_relationship_between_them_counts_each(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(
        s,
        "CREATE (a:Person {name: 'Di'}) CREATE (b:Person {name: 'Ed', age: 9}) "
        "CREATE (a)-[:KNOWS {since: 2022}]->(b)",
    )
    assert c.stats() == {
        "nodes-created": 2,
        "relationships-created": 1,
        "labels-added": 2,
        "properties-set": 4,
        "contains-updates": True,
    }


# --- SET ---


def test_a_set_counts_each_property_of_each_row_an_unchanged_value_included(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(s, "MATCH (n:Person) WHERE n.age >= 40 SET n.age = 40, n.name = 'X'")
    assert c.stats() == {"properties-set": 4, "contains-updates": True}


def test_a_set_that_matches_no_row_sets_nothing(monkeypatch):
    s = _session(monkeypatch)
    assert _counted(s, "MATCH (n:Person {name: 'Zed'}) SET n.age = 1").stats() == {}


def test_a_set_to_null_is_unknown_since_the_prior_value_is_not_read(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(s, "MATCH (n:Person {name: 'Ann'}) SET n.age = null")
    assert c.unknown == [
        "a SET to null counts a property only where the row held one, which is not read"
    ]


# --- DELETE ---


def test_a_delete_counts_each_row_it_deleted(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(s, "MATCH (n:Greeting {message: 'hello'}) DELETE n")
    assert c.stats() == {"nodes-deleted": 1, "contains-updates": True}


def test_a_detach_delete_counts_the_relationships_it_detached_and_the_node(monkeypatch):
    s = _session(monkeypatch)
    s.duck.execute("INSERT INTO s.knows VALUES (3, 1, 2019)")
    c = _counted(s, "MATCH (n:Person {name: 'Ann'}) DETACH DELETE n")
    assert c.stats() == {
        "nodes-deleted": 1,
        "relationships-deleted": 2,
        "contains-updates": True,
    }


def test_a_relationship_delete_counts_its_junction_row(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(s, "MATCH (:Person {name: 'Ann'})-[r:KNOWS]->(:Person {name: 'Bo'}) DELETE r")
    assert _rows(s, "knows") == []
    assert c.stats() == {"relationships-deleted": 1, "contains-updates": True}


# --- MERGE ---


def test_a_merge_that_matched_creates_nothing(monkeypatch):
    s = _session(monkeypatch)
    assert _counted(s, "MERGE (a:Person {name: 'Ann'}) MERGE (b:Person {name: 'Bo'})").stats() == {}


def test_a_merge_that_matched_counts_its_on_match_set(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(
        s,
        "MERGE (a:Person {name: 'Ann'}) MERGE (b:Person {name: 'Bo'}) ON MATCH SET b.age = 40",
    )
    assert c.stats() == {"properties-set": 1, "contains-updates": True}


def test_a_merge_that_matched_nothing_counts_what_it_made_and_its_on_create_set(monkeypatch):
    s = _session(monkeypatch)
    c = _counted(
        s,
        "MERGE (a:Person {name: 'Di'}) ON CREATE SET a.name = 'Di', a.age = 7 "
        "MERGE (b:Person {name: 'Ann'})",
    )
    # name set by the pattern, then again by ON CREATE SET, as Neo4j sets it; then age.
    assert c.stats() == {
        "nodes-created": 1,
        "labels-added": 1,
        "properties-set": 3,
        "contains-updates": True,
    }


def test_a_relationship_merge_counts_the_junction_row_only_when_it_made_one(monkeypatch):
    s = _session(monkeypatch)
    statement = "MATCH (a:Person {name: 'Ann'}), (c:Person {name: 'Cy'}) MERGE (a)-[:KNOWS]->(c)"
    assert _counted(s, statement).stats() == {
        "relationships-created": 1,
        "contains-updates": True,
    }
    assert _counted(s, statement).stats() == {}


# --- the HTTP answer: the same counters the Bolt summary's stats are ---


def test_the_http_answer_to_a_governed_write_carries_its_counters(monkeypatch):
    import json
    from types import SimpleNamespace

    import provisa.api.app as appmod
    from provisa.api.rest import cypher_router
    from provisa.executor.result import QueryResult
    from provisa.pgwire import _pipeline
    from tests.unit.test_cypher_write_return_and_junction import _map

    async def govern(sql, role_id, **_kwargs):
        assert sql.startswith("DELETE") and role_id == "analyst"
        return object()

    async def execute(plan, state):
        assert plan is not None and state is appmod.state
        return QueryResult(rows=[], column_names=[], rowcount=3)

    monkeypatch.setattr(appmod, "state", SimpleNamespace(contexts={"analyst": object()}))
    monkeypatch.setattr(cypher_router, "_resolve_role_id", lambda _r, _s: "analyst")
    monkeypatch.setattr(cypher_router, "_build_label_map", lambda *_a: _map())
    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", execute)
    answer = asyncio.run(
        cypher_router._answer(
            cypher_router.CypherRequest(query="MATCH (n:Person {name: 'Ann'}) DELETE n"),
            SimpleNamespace(),
            None,
        )
    )
    body = json.loads(bytes(answer.body))
    assert body["affected_rows"] == 3
    assert body["counters"] == {"nodes-deleted": 3, "contains-updates": True}
