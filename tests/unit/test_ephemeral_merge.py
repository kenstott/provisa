# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MERGE (REQ-2176): MATCH, then CREATE under the same rules. Per row of its leading clauses, the
pattern is matched against the model and the session alike; a row it matches takes ON MATCH SET,
a row it does not makes the pattern (a governed row where it names one model table, else session
nodes) and takes ON CREATE SET. A later row sees what an earlier one made."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store
from provisa.ephemeral.merge import merge_of
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {
    "Person": ({"id": "integer", "name": "text", "age": "integer"}, "(1, 'Ann', 30), (2, 'Bo', 40)")
}


def _values(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


@pytest.mark.parametrize(
    ("statement", "read"),
    [
        (
            "MERGE (n:Temp {k: 1})",
            "OPTIONAL MATCH (n:Temp) WHERE n.k = 1 RETURN n AS _v_n, 1 AS _n0_k",
        ),
        (
            "MERGE (:Temp {k: 1, w: 'x'})",
            "OPTIONAL MATCH (_m0:Temp) WHERE _m0.k = 1 AND _m0.w = 'x' "
            "RETURN _m0 AS _v__m0, 1 AS _n0_k, 'x' AS _n0_w",
        ),
        (
            "UNWIND [1, 2] AS x MERGE (n:Temp {k: x})",
            "UNWIND [1, 2] AS x OPTIONAL MATCH (n:Temp) WHERE n.k = x RETURN n AS _v_n, x AS _n0_k",
        ),
        (
            "MATCH (a:Person), (b:Person) MERGE (a)-[r:LIKES]->(b)",
            "MATCH (a:Person), (b:Person) OPTIONAL MATCH (a)-[r:LIKES]->(b) "
            "RETURN r AS _v_r, a AS _v_a, b AS _v_b",
        ),
    ],
)
def test_a_merge_reads_its_pattern_as_an_optional_match_of_its_leading_clauses(statement, read):
    merge = merge_of(statement)
    assert merge is not None and merge.read == read


def test_a_statement_without_a_merge_is_none():
    assert merge_of("CREATE (n:Temp)") is None
    assert merge_of("MATCH (n) RETURN n") is None


def test_a_merge_creates_what_it_does_not_match_and_matches_it_after(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("MERGE (n:Temp {k: 1})")
    s.run("MERGE (n:Temp {k: 1})")
    s.run("MERGE (n:Temp {k: 2})")
    assert _values(s.run("MATCH (n:Temp) RETURN n.k")) == [(1,), (2,)]


def test_a_later_row_sees_what_an_earlier_row_made(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("UNWIND [1, 1, 2] AS x MERGE (n:Temp {k: x})")
    assert _values(s.run("MATCH (n:Temp) RETURN n.k")) == [(1,), (2,)]


def test_on_create_and_on_match_set_labels(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    merge = "MERGE (n:Temp {k: 1}) ON CREATE SET n:Fresh ON MATCH SET n:Seen"
    s.run(merge)
    assert _values(s.run("MATCH (n:Fresh) RETURN n.k")) == [(1,)]
    assert "Seen" not in store.graph(s.session).labels  # ON MATCH did not run
    s.run(merge)
    assert _values(s.run("MATCH (n:Seen) RETURN n.k")) == [(1,)]


def test_a_merge_matches_a_model_row_and_tags_it(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("MERGE (p:Person {name: 'Ann'}) ON MATCH SET p:Vip")
    assert _values(s.run("MATCH (n:Vip) RETURN n.name")) == [("Ann",)]
    assert len(s.run("MATCH (n:Person) RETURN n.name")) == 2


def test_a_merge_of_a_model_row_it_does_not_match_inserts_it(monkeypatch):
    # MATCH, else insert (REQ-2176, REQ-883): not an upsert; two concurrent MERGEs may both insert.
    s = Session(monkeypatch, _PEOPLE)
    s.run("MERGE (p:Person {id: 3, name: 'Cy'})")
    s.run("MERGE (p:Person {id: 3, name: 'Cy'})")
    assert s.duck.execute("SELECT id, name FROM s.person WHERE id = 3").fetchall() == [(3, "Cy")]


def test_a_relationship_merge_between_matched_nodes(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    statement = (
        "MATCH (a:Person), (b:Person) WHERE a.name = 'Ann' AND b.name = 'Bo' "
        "MERGE (a)-[:LIKES]->(b)"
    )
    s.run(statement)
    s.run(statement)
    rows = s.run("MATCH (a:Person)-[:LIKES]->(b:Person) RETURN a.name, b.name")
    assert _values(rows) == [("Ann", "Bo")]


def test_a_merge_followed_by_a_clause_is_refused_by_name(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    with pytest.raises(CypherParseError, match="followed by RETURN"):
        s.run("MERGE (n:Temp {k: 1}) RETURN n")
