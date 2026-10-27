# Copyright (c) 2026 Kenneth Stott
# Canary: 292a9af1-7255-4d33-b6ad-912f55c0bd4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An ephemeral CREATE (REQ-2176): its values come from a governed read of its own leading
clauses, and each row of it makes the patterns' nodes and relationships in the session; the
governed write is left to its own path."""

# Requirements: REQ-2176

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from provisa.compiler.temp_tables import TempSession
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store, write
from tests.unit.test_ephemeral_classify import _model

pytestmark = pytest.mark.unit


@pytest.fixture
def written(monkeypatch):
    held: list[store.Writes] = []

    async def capture(state, session, writes):
        held.append(writes)

    monkeypatch.setattr(store, "write", capture)
    return held


def _run(statement: str, answer: list[dict[str, Any]], session=None, governed=None, **params):
    asked: list[str] = []

    async def read(query: str, values: dict[str, Any]) -> list[dict[str, Any]]:
        asked.append(query)
        assert values == params
        return answer

    async def governed_write(query: str, values: dict[str, Any]) -> None:
        assert governed is not None, query
        governed.append((query, values))

    made = asyncio.run(
        write.create(
            statement, params, _model(), session or TempSession(), None, read, governed_write
        )
    )
    return made, asked


def test_the_values_are_read_by_the_statements_own_clauses(written):
    made, asked = _run(
        "UNWIND $xs AS x CREATE (:Temp {n: x, d: x * 2})",
        [{"_n0_n": 1, "_n0_d": 2}, {"_n0_n": 3, "_n0_d": 6}],
        xs=[1, 3],
    )
    assert asked == ["UNWIND $xs AS x RETURN x AS _n0_n, x * 2 AS _n0_d"]
    assert made == write.Created(nodes=2, relationships=0, labels=2)
    [w] = written
    assert [(n.node_id, n.anchor, n.labels, n.properties) for n in w.nodes] == [
        ("e:1", "cypher__anchor__Temp", ("Temp",), {"n": 1, "d": 2}),
        ("e:2", "cypher__anchor__Temp", ("Temp",), {"n": 3, "d": 6}),
    ]


def test_a_create_with_no_leading_clause_reads_one_row(written):
    _, asked = _run("CREATE (), (:A:B {k: 1})", [{"_n1_k": 1}])
    assert asked == ["RETURN 1 AS _n1_k"]
    assert [(n.anchor, n.labels) for n in written[0].nodes] == [
        ("cypher__anchor", ()),
        ("cypher__anchor__A", ("A", "B")),
    ]


def test_relationships_join_new_nodes_and_matched_model_nodes(written):
    person = {"id": 4, "label": "Person", "properties": {"_id": 7, "id": 7}}
    _, asked = _run(
        "MATCH (p:Person) CREATE (p)-[:OWNS {since: 1}]->(r:Robot), (r)-[:NEXT]->(r)",
        [{"_r0_since": 1, "_v_p": person}],
    )
    assert asked == ["MATCH (p:Person) RETURN 1 AS _r0_since, p AS _v_p"]
    [w] = written
    assert [(r.rel_type, r.from_id, r.to_id, r.properties) for r in w.rels] == [
        ("OWNS", "Person|7", "e:1", {"since": 1}),
        ("NEXT", "e:1", "e:1", {}),
    ]


def test_a_model_node_is_tagged_by_its_labels_on_an_ephemeral_node(written):
    _run("CREATE (:Person:Temp {name: 'x'})", [{"_n0_name": "x"}])
    [node] = written[0].nodes
    assert (node.anchor, node.labels) == ("cypher__anchor__Temp", ("Person", "Temp"))


def test_the_anchor_a_free_label_creates_anchors_its_later_nodes(written):
    session = TempSession()
    _run("CREATE (:Scratch {k: 1})", [{"_n0_k": 1}], session)
    _run("CREATE (:Temp:Scratch {k: 2})", [{"_n0_k": 2}], session)
    assert [n.anchor for w in written for n in w.nodes] == [
        "cypher__anchor__Scratch",
        "cypher__anchor__Scratch",
    ]


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE (:Person {id: 1})",
        "CREATE (n:Person {id: 1}) RETURN n",
        "CREATE INDEX FOR (n:Temp) ON (n.k)",
        "MATCH (n:Person) RETURN n",
    ],
)
def test_the_governed_write_and_a_read_are_left_to_their_own_path(written, statement):
    made, asked = _run(statement, [])
    assert made is None and asked == [] and written == []


def test_a_relationship_from_a_value_that_is_no_node_is_refused(written):
    with pytest.raises(CypherParseError, match="is a node"):
        _run("UNWIND [1] AS p CREATE (p)-[:R]->(:X)", [{"_v_p": 1}])


def test_a_mixed_create_writes_its_governed_row_and_its_session_parts(written):
    governed: list[tuple[str, dict[str, Any]]] = []
    made, _ = _run(
        "CREATE (c:Person {id: 42}), (n:Note {text: 'x'}), (c)-[:ABOUT]->(n)",
        [{"_n0_id": 42, "_n1_text": "x"}],
        governed=governed,
    )
    assert governed == [("CREATE (n:Person {id: $id})", {"id": 42})]
    [w] = written
    assert [(n.anchor, n.labels, n.properties) for n in w.nodes] == [
        ("cypher__anchor__Note", ("Note",), {"text": "x"})
    ]
    # The session relationship ends on the model node the governed write made.
    assert [(r.rel_type, r.from_id, r.to_id, r.from_labels) for r in w.rels] == [
        ("ABOUT", "Person|42", "e:1", ("Person",))
    ]
    assert made == write.Created(nodes=2, relationships=1, labels=2)


def test_a_governed_row_a_session_relationship_ends_on_names_its_key(written):
    with pytest.raises(CypherParseError, match="names its id"):
        _run(
            "CREATE (c:Person {name: 'x'}), (c)-[:ABOUT]->(:Note)",
            [{"_n0_name": "x"}],
            governed=[],
        )


def test_a_model_relationship_of_a_split_goes_to_the_governed_path(written):
    governed: list[tuple[str, dict[str, Any]]] = []
    person = {"id": 4, "label": "Person", "properties": {"_id": 7, "id": 7}}
    _run(
        "MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b), (a)-[:LIKES]->(b)",
        [{"_v_a": person, "_v_b": person}],
        governed=governed,
    )
    assert governed == [("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)", {})]
    assert [(r.rel_type, r.from_id, r.to_id) for r in written[0].rels] == [
        ("LIKES", "Person|7", "Person|7")
    ]


def test_an_ephemeral_create_ends_its_statement(written):
    with pytest.raises(CypherParseError, match="followed by RETURN"):
        _run("CREATE (n:Temp {k: 1}) RETURN n", [])
