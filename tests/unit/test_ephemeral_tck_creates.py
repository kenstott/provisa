# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0e3f2a-91c4-4d7e-a5b8-2f6c8d1e7a93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The openCypher TCK's session writes that once answered 500 (REQ-2176): multi-hop patterns,
large multi-CREATE scripts and lists of durations are made in the session; a write past the
session's bounds (REQ-1926) is refused by name, never an internal error."""

# Requirements: REQ-2176, REQ-2173, REQ-1926

from __future__ import annotations

import pytest

from provisa.core import settings_registry
from provisa.cypher.parser import CypherParseError
from provisa.ephemeral.write import Created
from provisa.gql.temporal import Duration
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("statement", "made"),
    [
        # Create5 [1]..[5]
        ("CREATE (:A)-[:R]->(:B)-[:R]->(:C)", Created(3, 2, 3)),
        ("CREATE (:A)<-[:R]-(:B)<-[:R]-(:C)", Created(3, 2, 3)),
        ("CREATE (:A)-[:R]->(:B)<-[:R]-(:C)", Created(3, 2, 3)),
        ("CREATE ()-[:R1]->()<-[:R2]-()-[:R3]->()", Created(4, 3, 0)),
        ("CREATE (:A)<-[:R1]-(:B)-[:R2]->(:C)", Created(3, 2, 3)),
    ],
)
def test_a_pattern_of_several_hops_is_made_in_the_session(monkeypatch, statement, made):
    assert Session(monkeypatch, {}).run(statement) == made


def test_a_pattern_of_several_hops_is_read_back_along_its_directions(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {k: 1})<-[:R1]-(:B {k: 2})-[:R2]->(:C {k: 3})")
    rows = s.run("MATCH (a:A)<-[:R1]-(b:B)-[:R2]->(c:C) RETURN a.k, b.k, c.k")
    assert [tuple(r.values()) for r in rows] == [(1, 2, 3)]


def _movie_graph(people: int) -> str:
    """Create4 [1]'s shape: a node per CREATE clause, then one CREATE of many relationships, each
    with a list property, between the nodes the earlier clauses bound."""
    clauses = ["CREATE (m:Movie {title: 'The Matrix', released: 1999})"]
    clauses += [f"CREATE (p{i}:Person {{name: 'P{i}', born: {1950 + i}}})" for i in range(people)]
    rels = [f"(p{i})-[:ACTED_IN {{roles: ['R{i}']}}]->(m)" for i in range(people)]
    rels += [f"(p{i})-[:FOLLOWS]->(p{i + 1})" for i in range(people - 1)]
    return "\n".join([*clauses, "CREATE\n  " + ",\n  ".join(rels)])


def test_a_large_create_script_is_made_in_the_session(monkeypatch):
    s = Session(monkeypatch, {})
    assert s.run(_movie_graph(40)) == Created(41, 79, 41)
    rows = s.run("MATCH (p:Person)-[a:ACTED_IN]->(m:Movie) WHERE p.name = 'P7' RETURN a.roles")
    assert rows == [{"a.roles": ["R7"]}]


def test_many_create_clauses_on_one_bound_node_are_made_in_the_session(monkeypatch):
    # Create4 [2]'s shape: every clause after the first ends a relationship on the node it bound.
    s = Session(monkeypatch, {})
    clauses = ["CREATE (hf:School {name: 'Hilly Fields'})"]
    clauses += [f"CREATE (hf)-[:STUDENT]->(s{i}:Student {{name: 'S{i}'}})" for i in range(60)]
    assert s.run("\n".join(clauses)) == Created(61, 60, 61)
    assert s.run("MATCH (:School)-[:STUDENT]->(s:Student) RETURN count(s) AS n") == [{"n": 60}]


def _limited(monkeypatch, key: str, limit: int) -> None:
    held = settings_registry.value
    monkeypatch.setattr(settings_registry, "value", lambda k: limit if k == key else held(k))


def test_a_write_past_the_sessions_tables_is_refused_by_name(monkeypatch):
    s = Session(monkeypatch, {})
    _limited(monkeypatch, "temp_tables.max_per_session", 3)
    with pytest.raises(CypherParseError, match=r"temp_tables\.max_per_session = 3"):
        s.run("CREATE (:A)-[:R]->(:B)-[:R]->(:C)")


def test_a_write_past_a_tables_rows_is_refused_by_name(monkeypatch):
    s = Session(monkeypatch, {})
    _limited(monkeypatch, "temp_tables.max_rows", 5)
    with pytest.raises(CypherParseError, match=r"temp_tables\.max_rows = 5"):
        s.run(_movie_graph(6))


@pytest.mark.parametrize(
    ("written", "held"),
    [
        # Temporal4 [12] #1 and #2
        ("[duration({seconds: 13})]", [Duration(0, 0, 13_000_000)]),
        (
            "[duration({seconds: 14}), duration({seconds: 15}), duration({seconds: 16})]",
            [Duration(0, 0, 14_000_000), Duration(0, 0, 15_000_000), Duration(0, 0, 16_000_000)],
        ),
    ],
)
def test_a_list_of_durations_is_stored_and_read_back(monkeypatch, written, held):
    s = Session(monkeypatch, {})
    assert s.run(f"CREATE (:T {{dates: {written}}})") == Created(1, 0, 1)
    assert s.run("MATCH (n:T) RETURN n.dates") == [{"n.dates": held}]


def test_a_list_of_durations_keeps_its_months_and_its_nulls(monkeypatch):
    # A duration's months are its own, never read as 30 days; a null item stays null.
    s = Session(monkeypatch, {})
    rows = s.run("RETURN [duration({months: 2}), null, duration({days: 3})] AS d")
    assert rows == [{"d": [Duration(2, 0, 0), None, Duration(0, 3, 0)]}]
    s.run("CREATE (:T {dates: [duration({months: 16}), null]})")
    assert s.run("MATCH (n:T) RETURN n.dates") == [{"n.dates": [Duration(16, 0, 0), None]}]
