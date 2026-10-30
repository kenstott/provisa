# Copyright (c) 2026 Kenneth Stott
# Canary: 9ff6b526-2825-4e9d-8512-334997947a56
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A property of a graph session's own node or relationship is written by SET n.p = v and
cleared by REMOVE n.p, exactly as SET n.p = null (REQ-2176, amended 2026-10-28). On a model node
REMOVE n.p stays refused by name, and SET or REMOVE of an ontology Class label is refused by name
(REQ-2163)."""

# Requirements: REQ-2176, REQ-2163

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import seam
from provisa.ephemeral.props import PropClause, Propertied, PropItem, prop_clause
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_MODEL = {
    "Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')"),
    "Customer": ({"id": "integer", "name": "text"}, "(1, 'Ann')"),
}


def _session(monkeypatch, *, in_place: bool = True) -> Session:
    return Session(monkeypatch, _MODEL, in_place=in_place)


@pytest.mark.parametrize(
    ("statement", "clause"),
    [
        (
            "MATCH (n:T) SET n.a = 1, n.`odd key` = n.a + 'x'",
            PropClause(
                "MATCH (n:T)",
                "SET",
                (PropItem("n", "a", "1"), PropItem("n", "odd key", "n.a + 'x'")),
            ),
        ),
        (
            "MATCH ()-[r:R]->() REMOVE r.a, r.b",
            PropClause(
                "MATCH ()-[r:R]->()", "REMOVE", (PropItem("r", "a", None), PropItem("r", "b", None))
            ),
        ),
        (
            "MATCH (n) SET n.xs = [1, 2], n.m = {a: 1, b: 2}",
            PropClause(
                "MATCH (n)",
                "SET",
                (PropItem("n", "xs", "[1, 2]"), PropItem("n", "m", "{a: 1, b: 2}")),
            ),
        ),
        (
            "MATCH (n) SET n += {a: 1}",
            PropClause("MATCH (n)", "SET", (PropItem("n", None, "{a: 1}", merge=True),)),
        ),
        (
            "MATCH (n), (m) SET n = m",
            PropClause("MATCH (n), (m)", "SET", (PropItem("n", None, "m"),)),
        ),
        (
            "MATCH (n) SET (n).a = 1",
            PropClause("MATCH (n)", "SET", (PropItem("n", "a", "1"),)),
        ),
    ],
)
def test_a_property_set_or_remove(statement, clause):
    assert prop_clause(statement) == clause


@pytest.mark.parametrize(
    "statement",
    [
        "MATCH (n) SET n:X",
        "MATCH (n) SET n:X, n.a = 1",
        "MATCH (n) SET n.a = 1 RETURN n",
        "CREATE (n)",
    ],
)
def test_anything_else_is_not_one(statement):
    assert prop_clause(statement) is None


@pytest.mark.parametrize("in_place", [True, False])
def test_remove_clears_a_session_nodes_property(monkeypatch, in_place):
    s = _session(monkeypatch, in_place=in_place)
    s.run("CREATE (:L {num: 42, name: 'a'}), (:L {num: 7, name: 'b'})")
    assert s.run("MATCH (n:L) WHERE n.name = 'a' REMOVE n.num") == Propertied(1)
    rows = s.run("MATCH (n:L) RETURN n.name AS name, n.num AS num ORDER BY name")
    assert rows == [{"name": "a", "num": None}, {"name": "b", "num": 7}]


def test_remove_is_set_to_null(monkeypatch):
    read = "MATCH (n:L) RETURN n.num IS NULL AS gone, n.name AS name"
    answers = []
    for clearing in ("MATCH (n:L) REMOVE n.num", "MATCH (n:L) SET n.num = null"):
        # One harness session at a time: each patches the store's one face.
        with monkeypatch.context() as patched:
            s = _session(patched)
            s.run("CREATE (:L {num: 42, name: 'a'})")
            s.run(clearing)
            answers.append(s.run(read))
    assert answers[0] == answers[1] == [{"gone": True, "name": "a"}]


def test_remove_then_a_read_sees_it(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:L {num: 42})")
    made = s.run("MATCH (n:L) REMOVE n.num RETURN n.num IS NOT NULL AS still_there")
    assert made.rows == [{"still_there": False}]
    assert made.counters == {"properties_set": 1}


def test_remove_clears_a_session_relationships_property(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (a), (b), (a)-[:X {num: 42, keep: 'k'}]->(b)")
    made = s.run(
        "MATCH ()-[r:X]->() REMOVE r.num RETURN r.num IS NOT NULL AS still, r.keep AS keep"
    )
    assert made.rows == [{"still": False, "keep": "k"}]


def test_set_writes_a_session_nodes_property_from_an_expression(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:L {num: 1}), (:L {num: 2})")
    assert s.run("MATCH (n:L) SET n.twice = n.num * 2") == Propertied(2)
    assert s.run("MATCH (n:L) RETURN n.twice AS t ORDER BY t") == [{"t": 2}, {"t": 4}]


def test_a_set_of_a_new_property_grows_the_anchor(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:L {num: 1})")
    s.run("MATCH (n:L) SET n.name = 'x', n.num = 1.5")
    assert s.run("MATCH (n:L) RETURN n.name AS name, n.num AS num") == [{"name": "x", "num": 1.5}]


def test_a_null_node_takes_no_property(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:L {num: 1}), (:M {num: 2})-[:R]->(:M {num: 3})")
    assert s.run("MATCH (n:L) OPTIONAL MATCH (n)-[:R]->(m:M) REMOVE m.num") == Propertied(0)
    assert s.run("MATCH (m:M) RETURN sum(m.num) AS s") == [{"s": 5}]


def test_remove_of_a_model_nodes_property_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(CypherParseError, match="A REMOVE of a property is not written") as err:
        s.run("MATCH (p:Person) REMOVE p.name")
    assert "governed column" in str(err.value)
    assert "REQ-" not in str(err.value)
    s.run("CREATE (:L {num: 1})")  # with the session's own beside it, the same
    with pytest.raises(CypherParseError, match="A REMOVE of a property is not written"):
        s.run("MATCH (p:Person) REMOVE p.name RETURN p")


def test_a_set_of_model_nodes_alone_is_the_governed_paths(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:L {num: 1})")
    made = asyncio.run(
        seam.write(
            "MATCH (p:Person) WHERE p.id = 1 SET p.name = 'Al'",
            {},
            s.model,
            s.session,
            s.state,
            s.read,
            s.write,
        )
    )
    assert made is None
    assert s.run("MATCH (n:L) RETURN n.num AS num") == [{"num": 1}]


def _with_class(s: Session, label: str) -> None:
    """Make ``label``'s table an ontology Class table (a negative table id, REQ-2163)."""
    s.model.nodes[label] = replace(s.model.nodes[label], table_id=-7)


def test_set_of_an_ontology_class_on_a_model_node_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    _with_class(s, "Customer")
    with pytest.raises(CypherParseError, match="Customer is an ontology Class") as err:
        s.run("MATCH (p:Person) SET p:Customer")
    assert "which no label write sets" in str(err.value)
    assert "REQ-" not in str(err.value)
    assert s.run("MATCH (c:Customer) RETURN count(c) AS c") == [{"c": 1}]  # no tag written


def test_a_class_label_set_or_remove_followed_by_a_read_is_refused_alike(monkeypatch):
    s = _session(monkeypatch)
    _with_class(s, "Customer")
    with pytest.raises(CypherParseError, match="Customer is an ontology Class"):
        s.run("MATCH (p:Person) SET p:Customer RETURN p.name")
    with pytest.raises(CypherParseError, match="which no label write removes"):
        s.run("MATCH (p:Person) REMOVE p:Customer RETURN p.name")


def test_a_merge_does_not_set_a_class_on_a_model_node(monkeypatch):
    s = _session(monkeypatch)
    _with_class(s, "Customer")
    with pytest.raises(CypherParseError, match="Customer is an ontology Class"):
        s.run("MATCH (p:Person) MERGE (p)-[:KNOWS]->(t:Pal) ON CREATE SET p:Customer")


def test_a_class_label_on_a_session_node_is_a_label_like_any(monkeypatch):
    s = _session(monkeypatch)
    _with_class(s, "Customer")
    s.run("CREATE (:Tmp {k: 1})")
    s.run("MATCH (t:Tmp) SET t:Customer")
    assert s.run("MATCH (c:Customer) RETURN count(c) AS c") == [{"c": 2}]


def _props_of(s, query):
    [row] = seam.answered(s.run(query), s.session)
    [value] = row.values()
    return value["properties"]


@pytest.mark.parametrize("in_place", [True, False])
def test_set_of_a_map_makes_the_properties_exactly_the_map(monkeypatch, in_place):
    # Set4 [2], [3]: a property the map does not hold, or holds as null, is cleared.
    s = _session(monkeypatch, in_place=in_place)
    s.run("CREATE (:X {name: 'A', name2: 'B'})")
    assert s.run("MATCH (n:X) SET n = {name: 'B', name2: null, baz: 'C'}") == Propertied(3)
    assert _props_of(s, "MATCH (n:X) RETURN n") == {"name": "B", "baz": "C"}


def test_set_of_an_empty_map_clears_every_property(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:X {name: 'A', name2: 'B'})")
    s.run("MATCH (n:X) SET n = { }")
    assert _props_of(s, "MATCH (n:X) RETURN n") == {}


def test_a_map_added_with_plus_equals_keeps_the_rest(monkeypatch):
    # Set5 [2], [4], [5]: += writes the map's properties, a null clears one, {} changes nothing.
    s = _session(monkeypatch)
    s.run("CREATE (:X {name: 'A', name2: 'B'})")
    s.run("MATCH (n:X) SET n += {name2: 'C', name: null}")
    assert _props_of(s, "MATCH (n:X) RETURN n") == {"name2": "C"}
    s.run("MATCH (n:X) SET n += { }")
    assert _props_of(s, "MATCH (n:X) RETURN n") == {"name2": "C"}


def test_a_relationship_takes_a_nodes_properties(monkeypatch):
    # Merge6 [6]'s SET r = a: a node's properties as a map.
    s = _session(monkeypatch)
    s.run("CREATE (:A {name: 'A', k: 1})-[:T {old: true}]->(:B)")
    s.run("MATCH (a:A)-[r:T]->() SET r = a")
    rels = s.run("MATCH ()-[r:T]->() RETURN r")
    # A cleared property reads null, which an answer leaves out.
    held = {k: v for k, v in rels[0]["r"]["properties"].items() if v is not None}
    assert held == {"name": "A", "k": 1}


def test_a_parenthesized_variable_is_the_variable(monkeypatch):
    # Set1 [3], [4].
    s = _session(monkeypatch)
    s.run("CREATE (:A)-[:REL]->()")
    s.run("MATCH (n:A) SET (n).name = 'neo4j'")
    s.run("MATCH ()-[r:REL]->() SET (r).name = 'neo4j'")
    assert _props_of(s, "MATCH (n:A) RETURN n") == {"name": "neo4j"}
    assert s.run("MATCH ()-[r:REL]->() RETURN r.name AS n") == [{"n": "neo4j"}]


@pytest.mark.parametrize("value", ["[{num: 1}]", "{num: 1}", "[[1], [2]]"])
def test_a_map_or_a_list_of_them_is_no_property_value(monkeypatch, value):
    # Set1 [10]: refused by name, as Cypher refuses it.
    s = _session(monkeypatch)
    s.run("CREATE (:A)")
    with pytest.raises(CypherParseError, match="never a map"):
        s.run(f"MATCH (a:A) SET a.p = {value}")


def test_a_set_of_a_label_no_one_has_is_refused_by_name(monkeypatch):
    # Set1 [8]: as a read of the label is (REQ-778), not taken by the governed path.
    from provisa.gql.translator_types import CypherMistake

    s = _session(monkeypatch)
    with pytest.raises(CypherMistake, match="There is no label :DoesNotExist"):
        s.run("OPTIONAL MATCH (a:DoesNotExist) SET a.num = 42 RETURN a")


def test_a_session_node_is_answered_with_every_label_and_no_built_in_id(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:A:B {k: 1})")
    s.run("MATCH (n:A) SET n:C")
    [row] = seam.answered(s.run("MATCH (n:B) RETURN n"), s.session)
    assert sorted(row["n"]["labels"]) == ["A", "B", "C"]
    assert row["n"]["properties"] == {"k": 1}
    assert row["n"]["id"] == "e:1"


def test_labels_of_a_session_node_are_every_label_it_carries(monkeypatch):
    # Set3 [6], Merge1 [10]: by its label, and matched by none.
    s = Session(monkeypatch, {})
    s.run("CREATE (:A:B), ()")
    s.run("MATCH (n) SET n:Foo")
    rows = s.run("MATCH (n:A) RETURN labels(n) AS l")
    assert [sorted(r["l"]) for r in rows] == [["A", "B", "Foo"]]
    rows = s.run("MATCH (n) RETURN labels(n) AS l")
    assert sorted(sorted(r["l"]) for r in rows) == [["A", "B", "Foo"], ["Foo"]]
