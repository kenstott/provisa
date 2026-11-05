# Copyright (c) 2026 Kenneth Stott
# Canary: 014ba3c0-b6c3-4349-b035-a312a136fe0b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship between nodes a pattern names no type of, read where the statement already
holds one of its ends (REQ-345): a self-loop (n)-[r]->(n), a node an earlier MATCH, an earlier hop
or a WITH holds, a relationship variable already bound. Each had been refused ("A relationship
from a node a later MATCH starts at, or to a node the statement already holds, is ..."); each is
now its own union of every relationship, joined on the held node's key or identity -- never a
cross join -- and a self-loop the same row on both ends of its edge. Within one MATCH no edge is
matched twice, and an undirected hop reads each edge from either end, a self-loop once.

The scenarios are the openCypher TCK's (Match3, Match6, Match7, CountingSubgraphMatches1, With3,
Pattern1, Comparison1), run on DuckDB over a session graph. The joins are held to the
relationship guard (REQ-603) over a model of registered tables: a self-loop, a hop to a node of
one table and an undirected hop are equalities on the table's own key, which it admits; patterns
sharing no node are a cross join, which it still refuses."""

# Requirements: REQ-345, REQ-603

from __future__ import annotations

import json

import pytest

from provisa.compiler.sql_gen import CompilationContext, JoinMeta, TableMeta
from provisa.compiler.sql_validator import (
    approved_joins,
    computed_joins,
    tables_outside_relationships,
)
from provisa.compiler.stage2 import GovernanceContext
from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_LOOPS = "CREATE (a:A)-[:LOOP]->(a), ()-[:T]->()"
_KNOWS = (
    "CREATE (a {name: 'A'}), (b {name: 'B'}), (x1 {name: 'x1'}), (x2 {name: 'x2'}) "
    "CREATE (a)-[:KNOWS]->(x1), (a)-[:KNOWS]->(x2), (b)-[:KNOWS]->(x1), (b)-[:KNOWS]->(x2)"
)
_OPTIONAL = (
    "CREATE (s:Single), (a:A {num: 42}), (b:B {num: 46}), (c:C) "
    "CREATE (s)-[:REL]->(a), (s)-[:REL]->(b), (a)-[:REL]->(c), (b)-[:LOOP]->(b)"
)


def _session(monkeypatch, graph: str) -> Session:
    s = Session(monkeypatch, {})
    s.run(graph)
    return s


def _value(v) -> dict:
    return json.loads(v) if isinstance(v, str) else v


def _labels(rows, *names: str) -> list[tuple]:
    return sorted(tuple(_value(r[n])["label"] if r[n] else None for n in names) for r in rows)


def test_a_self_loop_is_the_same_row_on_both_ends(monkeypatch):
    s = _session(monkeypatch, _LOOPS)
    rows = s.run("MATCH (n)-[r]->(n) RETURN n, r")
    assert _labels(rows, "n") == [("A",)]
    assert _value(rows[0]["r"])["type"] == "LOOP"


def test_an_undirected_self_loop_is_matched_once(monkeypatch):
    s = _session(monkeypatch, _LOOPS)
    assert s.run("MATCH (n)--(n) RETURN count(*) AS c") == [{"c": 1}]
    assert s.run("MATCH (n)-[r]-(n) RETURN count(DISTINCT r) AS c") == [{"c": 1}]


def test_an_undirected_hop_reads_each_edge_from_either_end(monkeypatch):
    s = _session(monkeypatch, "CREATE (a {k: 1}), (b {k: 2}) CREATE (a)-[:T]->(b)")
    assert sorted(tuple(r.values()) for r in s.run("MATCH (x)--(y) RETURN x.k, y.k")) == [
        (1, 2),
        (2, 1),
    ]
    assert [tuple(r.values()) for r in s.run("MATCH (x)<--(y) RETURN x.k, y.k")] == [(2, 1)]


def test_a_chain_of_hops_joins_each_on_the_node_between(monkeypatch):
    s = _session(
        monkeypatch,
        "CREATE (a {k: 1}), (b {k: 2}), (c {k: 3}) CREATE (a)-[:T]->(b), (b)-[:T]->(c)",
    )
    rows = s.run("MATCH (x)-->(y)-->(z) RETURN x.k, y.k, z.k")
    assert [tuple(r.values()) for r in rows] == [(1, 2, 3)]


def test_a_hop_between_nodes_an_earlier_match_holds(monkeypatch):
    s = _session(monkeypatch, _KNOWS)
    rows = s.run("MATCH (a {name: 'A'}), (b {name: 'B'}) MATCH (a)-->(x)<-->(b) RETURN x.name")
    assert sorted(r["x.name"] for r in rows) == ["x1", "x2"]


def test_no_edge_is_matched_twice_in_one_match(monkeypatch):
    s = _session(monkeypatch, "CREATE (a), (b) CREATE (a)-[:T]->(b), (b)-[:T]->(a)")
    # Both hops would have to be the one edge from n to k.
    assert s.run("MATCH p = (n)-->(k)<--(n) RETURN p") == []
    s = _session(monkeypatch, "CREATE (a), (b), (c) CREATE (a)-[:T]->(b), (b)-[:T]->(c)")
    assert s.run("MATCH (a)-->(b), (b)-->(b) RETURN b") == []


def test_a_named_path_of_several_hops_holds_each_in_order(monkeypatch):
    s = _session(monkeypatch, "CREATE (a:A), (b:B) CREATE (a)-[:T1]->(b), (b)-[:T2]->(a)")
    rows = s.run("MATCH p = (n)<-->(k)<--(n) RETURN p")
    paths = sorted(
        (
            tuple(node["label"] for node in _value(r["p"])["nodes"]),
            tuple(edge["type"] for edge in _value(r["p"])["edges"]),
        )
        for r in rows
    )
    assert paths == [(("A", "B", "A"), ("T2", "T1")), (("B", "A", "B"), ("T1", "T2"))]


def test_patterns_sharing_no_node_are_each_row_of_both(monkeypatch):
    s = _session(monkeypatch, "CREATE (a:A), (b:B), (c:C) CREATE (a)-[:T]->(b), (a)-[:T]->(c)")
    rows = s.run("MATCH (a)-->(b) MATCH (c)-->(d) RETURN a, b, c, d")
    assert _labels(rows, "a", "b", "c", "d") == [
        ("A", "B", "A", "B"),
        ("A", "B", "A", "C"),
        ("A", "C", "A", "B"),
        ("A", "C", "A", "C"),
    ]


def test_an_optional_match_of_several_hops_is_matched_whole(monkeypatch):
    s = _session(monkeypatch, _OPTIONAL)
    assert s.run("MATCH (a:A), (c:C) OPTIONAL MATCH (a)-->(b)-->(c) RETURN b") == [{"b": None}]
    rows = s.run("MATCH (a:Single), (c:C) OPTIONAL MATCH (a)-->(b)-->(c) RETURN b")
    assert [_value(r["b"])["properties"]["num"] for r in rows] == [42]


def test_an_optional_self_loop_of_a_held_node(monkeypatch):
    s = _session(monkeypatch, _OPTIONAL)
    rows = s.run("MATCH (a) WHERE NOT (a:B) OPTIONAL MATCH (a)-[r]->(a) RETURN r")
    assert rows == [{"r": None}] * 3
    rows = s.run(
        "MATCH (a:A), (b:B) OPTIONAL MATCH (a)-->(x) OPTIONAL MATCH (x)-[r]->(b) RETURN x, r"
    )
    assert [(_value(r["x"])["label"], r["r"]) for r in rows] == [("C", None)]


def test_a_relationship_a_with_carries_is_matched_again(monkeypatch):
    s = _session(
        monkeypatch,
        "CREATE ()-[:T1 {id: 0}]->(:X), ()-[:T2 {id: 1}]->(:X), ()-[:T2 {id: 2}]->()",
    )
    rows = s.run(
        "MATCH (a)-[r]->(b:X) WITH a, r, b MATCH (a)-[r]->(b) RETURN r AS rel ORDER BY rel.id"
    )
    assert [_value(r["rel"])["properties"]["id"] for r in rows] == [0, 1]
    rows = s.run(
        "MATCH (a)-[r1]->(b:X) WITH a, r1 AS r2, b, count(*) AS c MATCH (a)-[r2]->(b) RETURN r2"
    )
    assert sorted(_value(r["r2"])["type"] for r in rows) == ["T1", "T2"]


def test_a_pattern_predicate_between_two_held_nodes(monkeypatch):
    s = _session(
        monkeypatch,
        "CREATE (a:A)-[:REL1]->(b:B), (b)-[:REL2]->(a), (a)-[:REL3]->(:C), (a)-[:REL1]->(:D)",
    )
    rows = s.run("MATCH (n), (m) WHERE (n)-[]->(m) RETURN n, m")
    assert _labels(rows, "n", "m") == [("A", "B"), ("A", "C"), ("A", "D"), ("B", "A")]


def test_two_paths_over_one_edge_are_equal_whichever_way_they_read_it(monkeypatch):
    s = _session(monkeypatch, "CREATE (n:A)-[:LOOP]->(n)")
    assert s.run("MATCH p1 = (:A)-->() MATCH p2 = (:A)<--() RETURN p1 = p2 AS same") == [
        {"same": True}
    ]


# --- the relationship guard over registered tables (REQ-603) ------------------------------------


def _persons():
    """One registered table, persons, and KNOWS, a relationship from it to itself."""
    meta = TableMeta(
        table_id=1,
        field_name="persons",
        type_name="Persons",
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name="persons",
        domain_id="sales",
        source_type="postgresql",
    )
    ctx = CompilationContext()
    ctx.tables = {"persons": meta}
    ctx.joins = {
        ("Persons", "knows"): JoinMeta(
            source_column="friend_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=meta,
            cardinality="many-to-one",
        )
    }
    gov = GovernanceContext()
    gov.table_map = {"public.persons": 1}
    gov.all_columns = {1: [("id", "integer"), ("name", "varchar"), ("friend_id", "integer")]}
    nm = NodeMapping(
        label="Persons",
        type_name="Persons",
        domain_label=None,
        table_label="Persons",
        table_id=1,
        source_id="pg",
        id_column="id",
        pk_columns=["id"],
        catalog_name="pg",
        schema_name="public",
        table_name="persons",
        properties={"id": "id", "name": "name", "friend_id": "friend_id"},
    )
    rm = RelationshipMapping(
        rel_type="KNOWS",
        source_label="Persons",
        target_label="Persons",
        join_source_column="friend_id",
        join_target_column="id",
        field_name="knows",
    )
    return ctx, gov, CypherLabelMap(nodes={"Persons": nm}, relationships={"KNOWS": rm})


def _guard(cypher: str) -> list[str]:
    ctx, gov, lm = _persons()
    computed, constants = computed_joins(ctx)
    sql, _, _ = cypher_to_sql(parse_cypher(cypher), lm, {})
    return [
        v.message
        for v in tables_outside_relationships(
            sql,
            gov,
            approved_joins(ctx),
            {1: ctx.tables["persons"]},
            computed=computed,
            constants=constants,
            primary_keys={1: frozenset({"id"})},
        )
    ]


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (n)-[r]->(n) RETURN n.name",
        "MATCH (n)--(n) RETURN count(*)",
        "MATCH (a)--(b) RETURN b.name",
        "MATCH (a:Persons) MATCH (a)-->(x) RETURN x.name",
        "MATCH (a:Persons) OPTIONAL MATCH (a)-[r]->(a) RETURN r",
    ],
)
def test_a_held_node_and_a_self_loop_are_joined_on_the_tables_key(cypher):
    assert _guard(cypher) == []


def test_patterns_sharing_no_node_are_still_a_cross_join_the_guard_refuses():
    refused = _guard("MATCH (a)-->(b) MATCH (c)-->(d) RETURN b.name, d.name")
    assert refused and all("persons and persons" in m for m in refused)
