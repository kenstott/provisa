# Copyright (c) 2026 Kenneth Stott
# Canary: dbddf7cf-10b8-4677-bbc1-d8c2968f7642
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Graph functions over a session's nodes and relationships (REQ-2176, REQ-2167): keys() lists
the properties an element holds set, properties() is the map of them, and labels(), type() and
property access read a node or relationship a list or map holds, as openCypher defines them."""

# Requirements: REQ-2176, REQ-2167

from __future__ import annotations

import json

import pytest

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def test_keys_of_a_session_node_are_the_properties_it_holds_set(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:L {num: 42, name: 'a', name2: 'B'}), (:L {other: 1})")
    s.run("MATCH (n:L) REMOVE n.num, n.name")
    rows = s.run("MATCH (n:L) RETURN keys(n) AS k")
    assert sorted(sorted(r["k"]) for r in rows) == [["name2"], ["other"]]


def test_keys_of_a_session_relationship_after_a_remove_in_the_same_statement(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a), (b), (a)-[:X {num: 42, a: 'a', b: 'B'}]->(b)")
    made = s.run("MATCH ()-[r]->() REMOVE r.num, r.a RETURN keys(r) AS k")
    assert made.rows == [{"k": ["b"]}]


def test_properties_of_a_node_and_a_relationship_are_maps_of_what_they_hold(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (n:Person {name: 'Popeye', level: 9001})-[:R {name: 'x', level: 1}]->(n)")
    s.run("CREATE (:Person {name: 'Olive'})")
    rows = s.run("MATCH (p:Person) RETURN properties(p) AS m")
    assert sorted(rows, key=repr) == sorted(
        [{"m": {"name": "Popeye", "level": 9001}}, {"m": {"name": "Olive"}}], key=repr
    )
    assert s.run("MATCH ()-[r:R]->() RETURN properties(r) AS m") == [
        {"m": {"name": "x", "level": 1}}
    ]


def test_labels_and_type_of_an_element_a_list_holds(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:Foo), (:Foo:Bar)")
    rows = s.run("MATCH (a) WITH [a, 1] AS list RETURN labels(list[0]) AS l")
    assert sorted(sorted(r["l"]) for r in rows) == [["Bar", "Foo"], ["Foo"]]
    s.run("CREATE ()-[:T]->()")
    assert s.run("MATCH (a)-[r]->() WITH [r, 1] AS list RETURN type(list[0]) AS t") == [{"t": "T"}]


def test_labels_of_a_value_that_is_no_node_is_a_type_error_when_read(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:Foo)")
    with pytest.raises(Exception, match=r"labels\(\) takes a node"):
        s.run("MATCH (a) WITH [a, 1] AS list RETURN labels(list[1]) AS l")


@pytest.mark.parametrize("element", ["n", "r"])
def test_a_property_of_an_element_a_list_holds(monkeypatch, element):
    s = Session(monkeypatch, {})
    s.run("CREATE ({existing: 42, missing: null})-[:REL {existing: 42, missing: null}]->()")
    match = "MATCH (n) WHERE n.existing = 42" if element == "n" else "MATCH ()-[r]->()"
    rows = s.run(
        f"{match} WITH [123, {element}] AS list "
        "RETURN (list[1]).missing AS a, (list[1]).missingToo AS b, (list[1]).existing AS c"
    )
    assert rows == [{"a": None, "b": None, "c": 42}]


def test_a_relationship_a_map_holds_is_its_value(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:User), (b:User) CREATE (a)-[:R {w: 1}]->(b)")
    rows = s.run("MATCH (:User)-[r]->(:User) WITH {key: r} AS rels RETURN rels.key AS x")
    [held] = [json.loads(r["x"]) if isinstance(r["x"], str) else r["x"] for r in rows]
    assert (held["type"], held["properties"]) == ("R", {"w": 1})
    assert held["identity"] in store.graph(s.session).rel_ends


def test_a_bound_node_given_a_property_map_again_in_a_create_is_refused(monkeypatch):
    s = Session(monkeypatch, {})
    with pytest.raises(CypherParseError, match="already declared"):
        s.run("CREATE (n:Foo) CREATE (n {})-[:OWNS]->(:Dog)")


def test_an_item_of_a_list_a_nested_carried_map_holds_is_the_item(monkeypatch):
    s = Session(monkeypatch, {})
    assert s.run("WITH {key: {key: [1, 2]}} AS m RETURN m.key.key[1] AS x") == [{"x": 2}]


def test_a_path_names_its_session_relationships_by_their_own_ids(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (a:User), (b:User) CREATE (a)-[:R]->(b)")
    rows = s.run("MATCH p = (:User)-[r]->(:User) WITH collect(p) AS ps RETURN ps[0] AS x")
    [held] = [json.loads(r["x"]) if isinstance(r["x"], str) else r["x"] for r in rows]
    assert [e["identity"] for e in held["edges"]] == list(store.graph(s.session).rel_ends)


@pytest.mark.parametrize("given", ["[]", "{}", "r", "p"])
def test_to_string_of_a_list_map_or_graph_element_a_list_holds_is_a_type_error(monkeypatch, given):
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:T]->()")
    # openCypher's TypeError, raised where the item is read (expr_visitor._checked_conversion).
    with pytest.raises(
        Exception, match=r"TypeError .*toString\(\) takes no (list|map|relationship|path)"
    ):
        s.run(f"MATCH p = (n)-[r:T]->() RETURN [x IN [1, '', {given}] | toString(x)] AS list")


def test_to_string_or_null_of_a_list_a_list_holds_is_null(monkeypatch):
    s = Session(monkeypatch, {})
    rows = s.run("RETURN [x IN [1, 'a', []] | toStringOrNull(x)] AS list")
    assert rows == [{"list": ["1", "a", None]}]


def test_an_unwind_over_an_undirected_hop_reads_each_direction_once_distinct(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ()-[:KNOWS {status: 'bad', year: '2015'}]->()")
    rows = s.run("MATCH ()-[r:KNOWS]-() UNWIND keys(r) AS x RETURN DISTINCT x AS theProps")
    assert sorted(r["theProps"] for r in rows) == ["status", "year"]
