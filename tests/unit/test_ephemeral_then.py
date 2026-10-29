# Copyright (c) 2026 Kenneth Stott
# Canary: 3db827cd-fc4b-4fb3-8899-2d0673c58229
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A write of session objects followed by reads in the same statement (REQ-2176): the write,
then the rest as a read over the session, which sees what it wrote -- one statement."""

# Requirements: REQ-2176

from __future__ import annotations

import asyncio

import pytest

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import seam
from provisa.ephemeral.then import Answered, clause_starts, split_of
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_MODEL = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}


def _session(monkeypatch) -> Session:
    return Session(monkeypatch, _MODEL)


# --------------------------------------------------------------------------- the split


@pytest.mark.parametrize(
    ("statement", "head", "rest", "kind"),
    [
        ("CREATE (n:T {a: 1}) RETURN n.a", "CREATE (n:T {a: 1})", "RETURN n.a", "create"),
        (
            "CREATE (a:T) CREATE (b:T) WITH a CREATE (a)-[:R]->(:T)",
            "CREATE (a:T) CREATE (b:T)",
            "WITH a CREATE (a)-[:R]->(:T)",
            "create",
        ),
        (
            "MERGE (a:T) ON CREATE SET a:New ON MATCH SET a:Old RETURN a",
            "MERGE (a:T) ON CREATE SET a:New ON MATCH SET a:Old",
            "RETURN a",
            "merge",
        ),
        ("MATCH (n:T) SET n:X RETURN n", "MATCH (n:T) SET n:X", "RETURN n", "labels"),
        (
            "MATCH (n:T) REMOVE n.a WITH n RETURN n",
            "MATCH (n:T) REMOVE n.a",
            "WITH n RETURN n",
            "props",
        ),
        # A DELETE followed by more (Merge1 [14]: MATCH (a:A) DELETE a MERGE (a2:A) ...).
        (
            "MATCH (n:T) DETACH DELETE n RETURN count(*)",
            "MATCH (n:T) DETACH DELETE n",
            "RETURN count(*)",
            "delete",
        ),
    ],
)
def test_a_write_followed_by_more_splits_at_its_end(statement, head, rest, kind):
    split = split_of(statement)
    assert split is not None
    assert (split.head, split.rest, split.kind) == (head, rest, kind)


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE (n:T)",
        "MATCH (n:T) RETURN n",
        "MATCH (n:T) SET n:X",
        "MATCH (n:T) DETACH DELETE n",
    ],
)
def test_a_write_ending_its_statement_is_not_split(statement):
    assert split_of(statement) is None


def test_a_clause_keyword_in_a_string_a_map_or_a_name_starts_no_clause():
    statement = "CREATE (n:T {set: 'RETURN x'}) SET n.create = 1 RETURN n.`with`"
    assert [statement[p : p + 6] for p in clause_starts(statement)] == [
        "CREATE",
        "SET n.",
        "RETURN",
    ]


def test_the_scope_after_a_create_is_its_leading_clauses_and_its_new_variables():
    split = split_of("UNWIND [1] AS x MATCH (p:Person) CREATE (p)-[r:R]->(m:T) RETURN m")
    assert split is not None
    assert split.scope == ("x", "p", "m", "r")


# --------------------------------------------------------------------------- CREATE


def test_a_create_returns_what_it_made(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("CREATE (n {name: 'foo'}) RETURN n.name AS p")
    assert isinstance(made, Answered)
    assert (made.columns, made.rows) == (["p"], [{"p": "foo"}])
    assert made.counters["nodes"] == 1


def test_each_row_of_its_leading_clauses_is_a_row_of_its_read(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("UNWIND [1, 2, 3, 4, 5] AS x CREATE (n:N {num: x}) RETURN sum(n.num) AS sum")
    assert made.rows == [{"sum": 15}]
    made = s.run(
        "UNWIND [1, 2, 3, 4, 5] AS x CREATE (n:M {num: x}) WITH n WHERE n.num % 2 = 0 "
        "RETURN n.num AS num"
    )
    assert sorted(r["num"] for r in made.rows) == [2, 4]


def test_skip_and_limit_count_the_rows_the_write_gave(monkeypatch):
    s = _session(monkeypatch)
    made = s.run(
        "UNWIND [42, 42, 42, 42, 42] AS x CREATE (n:N {num: x}) RETURN n.num AS num SKIP 2 LIMIT 2"
    )
    assert made.rows == [{"num": 42}, {"num": 42}]
    assert made.counters["nodes"] == 5
    made = s.run("CREATE (n:N {num: 42}) RETURN n LIMIT 0")
    assert (made.columns, made.rows) == (["n"], [])
    assert made.counters["nodes"] == 1


def test_a_created_relationship_is_found_again_by_its_id(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("UNWIND [1, 2, 3] AS x CREATE ()-[r:R {num: x}]->() RETURN sum(r.num) AS sum")
    assert made.rows == [{"sum": 6}]
    made = s.run("CREATE ()-[r:R {num: 42}]->() RETURN r")
    [row] = made.rows
    assert row["r"]["properties"] == {"num": 42}  # its id is no property of the answer


def test_a_value_of_the_leading_clauses_is_carried_to_the_read(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("UNWIND [1, 2] AS x CREATE (n:N {num: x * 10}) RETURN x, n.num AS num")
    assert sorted((r["x"], r["num"]) for r in made.rows) == [(1, 10), (2, 20)]


def test_a_model_node_the_create_reaches_back_to_is_read_again(monkeypatch):
    s = _session(monkeypatch)
    made = s.run(
        "MATCH (p:Person) WHERE p.id = 1 CREATE (p)-[:KNOWS]->(m:Pal {n: 1}) RETURN p.name AS name, m.n AS n"
    )
    assert made.rows == [{"name": "Ann", "n": 1}]


def test_creates_with_clauses_between_them_are_one_statement(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("CREATE (a:A) CREATE (b:B) WITH a CREATE (a)-[:R]->(:C) RETURN count(*) AS c")
    assert made.rows == [{"c": 1}]
    assert made.counters == {"nodes": 3, "relationships": 1, "labels": 3}
    assert s.run("MATCH (:A)-[:R]->(c:C) RETURN count(c) AS c") == [{"c": 1}]


def test_a_create_followed_by_a_set_writes_both(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("CREATE (n:N {a: 1}) SET n.b = 2 RETURN n.a + n.b AS s")
    assert made.rows == [{"s": 3}]
    assert made.counters["properties_set"] == 1
    made = s.run("CREATE (n:K) SET n:L RETURN count(n) AS c")
    assert made.rows == [{"c": 1}]
    assert s.run("MATCH (n:L) RETURN count(n) AS c") == [{"c": 1}]


def test_a_governed_create_followed_by_reads_is_the_governed_paths(monkeypatch):
    s = _session(monkeypatch)
    made = asyncio.run(
        seam.write(
            "CREATE (p:Person {id: 3, name: 'Cy'}) RETURN p.name",
            {},
            s.model,
            s.session,
            s.state,
            s.read,
            s.write,
        )
    )
    assert made is None
    assert s.sent == []


def test_a_variable_null_in_some_rows_and_not_others_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:T {a: 1})-[:R]->(:U)")
    s.run("CREATE (:T {a: 2})")
    with pytest.raises(CypherParseError, match="null in some and not in others"):
        s.run("MATCH (t:T) OPTIONAL MATCH (t)-[:R]->(u:U) SET t:X RETURN u")


# --------------------------------------------------------------------------- MERGE


def test_a_merge_returns_what_it_made_then_what_it_matched(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("MERGE (a:TheLabel {num: 43}) RETURN a.num AS num")
    assert made.rows == [{"num": 43}]
    assert made.counters["nodes"] == 1
    made = s.run("MERGE (a:TheLabel {num: 43}) RETURN a.num AS num")
    assert made.rows == [{"num": 43}]
    assert made.counters["nodes"] == 0


def test_a_merged_relationship_is_counted_once(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (a:A), (b:B) CREATE (a)-[:TYPE]->(b)")
    made = s.run("MATCH (a:A), (b:B) MERGE (a)-[r:TYPE]->(b) RETURN count(r) AS c")
    assert made.rows == [{"c": 1}]
    assert made.counters["relationships"] == 0


def test_merges_one_after_another_see_each_other(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("MERGE (a:A) MERGE (b:B) MERGE (a)-[:FOO]->(b)")
    assert isinstance(made, Answered)
    assert made.counters == {"nodes": 2, "relationships": 1, "labels": 2}
    assert s.run("MATCH (:A)-[:FOO]->(b:B) RETURN count(b) AS c") == [{"c": 1}]


def test_a_merge_then_a_create_then_a_read(monkeypatch):
    s = _session(monkeypatch)
    made = s.run(
        "CREATE (a:A), (b:B) MERGE (a)-[:KNOWS]->(b) CREATE (b)-[:KNOWS]->(c:C) RETURN count(*) AS n"
    )
    assert made.rows == [{"n": 1}]
    assert made.counters["relationships"] == 2


# --------------------------------------------------------------------------- SET n:L, REMOVE n:L


def test_a_label_remove_is_seen_by_the_read_after_it(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:N {num: 1}) CREATE (:N {num: 2}) CREATE (:N {num: 3})")
    made = s.run("MATCH (n:N) REMOVE n:N WITH n WHERE n.num % 2 = 1 RETURN sum(n.num) AS sum")
    assert made.rows == [{"sum": 4}]
    assert made.counters["removed"] == 3
    assert s.run("MATCH (n:N) RETURN count(n) AS c") == [{"c": 0}]


def test_a_label_set_on_a_model_node_then_a_read(monkeypatch):
    s = _session(monkeypatch)
    made = s.run("MATCH (p:Person) WHERE p.id = 2 SET p:Vip RETURN p.name AS name")
    assert made.rows == [{"name": "Bo"}]
    assert made.counters["added"] == 1
    assert s.run("MATCH (v:Vip) RETURN v.name AS name") == [{"name": "Bo"}]


def test_a_node_created_with_no_label_is_read_after_among_the_sessions_alone(monkeypatch):
    # The model's tables type ``id`` as text: a union of every label would read 12 as '12'.
    s = Session(monkeypatch, {"Person": ({"id": "text", "name": "text"}, "('p1', 'Ann')")})
    made = s.run("CREATE (n {id: 12, name: 'foo'}) RETURN n.id AS id, n.name AS p")
    assert made.rows == [{"id": 12, "p": "foo"}]
