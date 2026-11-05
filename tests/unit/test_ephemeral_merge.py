# Copyright (c) 2026 Kenneth Stott
# Canary: 3f72427c-6827-4547-be38-5388618ca5a1
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
            # The UNWIND's rows carried by a WITH: the OPTIONAL MATCH goes on from each of them.
            "UNWIND [1, 2] AS x WITH x OPTIONAL MATCH (n:Temp) WHERE n.k = x "
            "RETURN n AS _v_n, x AS _n0_k",
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


def test_a_merge_followed_by_a_clause_is_read_after_it(monkeypatch):
    # REQ-2176 (amended 2026-10-28): a MERGE of session objects may be followed by reads in the
    # same statement, which see what it wrote (then.py).
    s = Session(monkeypatch, _PEOPLE)
    made = s.run("MERGE (n:Temp {k: 1}) RETURN n.k AS k")
    assert (made.columns, made.rows) == (["k"], [{"k": 1}])
    assert s.run("MERGE (n:Temp {k: 1}) RETURN n.k AS k").counters["nodes"] == 0


# --- ON CREATE / ON MATCH SET of properties (REQ-2176) ------------------------------------------


def _empty(monkeypatch) -> Session:
    return Session(monkeypatch, {})


def test_on_create_set_writes_a_property_of_what_the_merge_made(monkeypatch):
    # Merge6 [1]: the made relationship's end takes it; a second MERGE matches and sets nothing.
    s = _empty(monkeypatch)
    s.run("CREATE (:A), (:B)")
    s.run("MATCH (a:A), (b:B) MERGE (a)-[:KNOWS]->(b) ON CREATE SET b.created = 1")
    s.run("MATCH (a:A), (b:B) MERGE (a)-[:KNOWS]->(b) ON CREATE SET b.created = 2")
    assert s.run("MATCH (b:B) RETURN b.created AS c") == [{"c": 1}]


def test_on_match_set_writes_only_what_the_merge_matched(monkeypatch):
    # Merge7 [1], [3]: a relationship's property, ON MATCH only.
    s = _empty(monkeypatch)
    s.run("CREATE (:A)-[:TYPE]->(:B)")
    s.run(
        "MATCH (a:A), (b:B) MERGE (a)-[r:TYPE]->(b) "
        "ON CREATE SET r.name = 'Lola' ON MATCH SET r.name = 'RUN'"
    )
    s.run(
        "MATCH (a:A), (b:B) MERGE (a)-[r:OTHER]->(b) "
        "ON CREATE SET r.name = 'Lola' ON MATCH SET r.name = 'RUN'"
    )
    assert s.run("MATCH ()-[r:TYPE]->() RETURN r.name AS n") == [{"n": "RUN"}]
    assert s.run("MATCH ()-[r:OTHER]->() RETURN r.name AS n") == [{"n": "Lola"}]


def test_on_create_set_copies_a_map_or_a_node(monkeypatch):
    # Merge6 [6], [7]: SET r = a, SET r += {...}.
    s = _empty(monkeypatch)
    s.run("CREATE (:A {name: 'A'}), (:B {name: 'B'})")
    s.run("MATCH (a:A), (b:B) MERGE (a)-[r:T]->(b) ON CREATE SET r = a")
    s.run("MATCH (a:A), (b:B) MERGE (a)-[r:U]->(b) ON CREATE SET r += {name: 'bar', x: 1}")
    assert s.run("MATCH ()-[r:T]->() RETURN r.name AS n") == [{"n": "A"}]
    assert s.run("MATCH ()-[r:U]->() RETURN r.name AS n, r.x AS x") == [{"n": "bar", "x": 1}]


def test_a_merge_makes_its_pattern_once_whatever_else_its_rows_carry(monkeypatch):
    # Merge2 [5]: two rows read for the SET's person make one city, not one each.
    s = _empty(monkeypatch)
    s.run("CREATE (:Person {bornIn: 'New York'}), (:Person {bornIn: 'Ohio'})")
    rows = s.run(
        "MATCH (person:Person) MERGE (city:City) ON CREATE SET city.name = person.bornIn "
        "RETURN person.bornIn AS b"
    )
    assert sorted(r["b"] for r in rows.rows) == ["New York", "Ohio"]
    assert len(s.run("MATCH (c:City) RETURN c.name AS n")) == 1


@pytest.mark.parametrize(
    "statement",
    [
        "MERGE ({num: null})",  # Merge1 [17]
        "CREATE (a), (b) MERGE (a)-[r:X {num: null}]->(b)",  # Merge5 [29]
    ],
)
def test_a_merge_on_a_null_property_is_refused(monkeypatch, statement):
    from provisa.cypher.parser import CypherParseError

    with pytest.raises(CypherParseError, match="null value"):
        _empty(monkeypatch).run(statement)


def test_a_set_naming_an_undefined_variable_is_refused_though_nothing_matches(monkeypatch):
    # Merge3 [5]: refused as Cypher refuses it when it compiles.
    from provisa.cypher.parser import CypherParseError

    with pytest.raises(CypherParseError, match="Variable `missing` not defined"):
        _empty(monkeypatch).run("MERGE (a) ON MATCH SET a.num = missing RETURN a")


def test_a_variable_length_merge_is_refused(monkeypatch):
    from provisa.cypher.parser import CypherParseError

    with pytest.raises(CypherParseError, match="one hop"):
        _empty(monkeypatch).run("CREATE (a), (b) MERGE (a)-[:FOO*2]->(b)")


def test_an_undirected_merge_matches_either_direction():
    m = merge_of("MATCH (a {id: 2}), (b {id: 1}) MERGE (a)-[r:KNOWS]-(b)")
    assert m is not None
    assert "OPTIONAL MATCH (a)-[r:KNOWS]-(b)" in m.read
    [rel] = m.pattern.rels
    assert (rel.source, rel.target, rel.undirected) == (0, 1, True)


def test_a_merge_after_a_delete_does_not_match_what_it_deleted(monkeypatch):
    # Merge1 [14]: the first row's MERGE makes one node, the second's matches it.
    s = _empty(monkeypatch)
    s.run("CREATE (:A {num: 1}), (:A {num: 2})")
    rows = s.run("MATCH (a:A) DELETE a MERGE (a2:A) RETURN a2.num AS n")
    assert [r["n"] for r in rows.rows] == [None, None]
    assert s.run("MATCH (a:A) RETURN count(a) AS c") == [{"c": 1}]


def test_a_merge_after_with_reads_every_row_the_with_carries(monkeypatch):
    # Merge1 [9]: an OPTIONAL MATCH after a WITH keeps the rows it matches nothing for.
    s = _empty(monkeypatch)
    s.run("UNWIND [0, 1, 2] AS x UNWIND [0, 1, 2] AS y CREATE ({x: x, y: y})")
    rows = s.run(
        "MATCH (foo) WITH foo.x AS x, foo.y AS y "
        "MERGE (:N {x: x, y: y + 1}) MERGE (:N {x: x, y: y}) MERGE (:N {x: x + 1, y: y}) "
        "RETURN x, y"
    )
    assert len(rows.rows) == 9
    assert s.run("MATCH (n:N) RETURN count(n) AS c") == [{"c": 15}]


def test_a_merge_after_unwind_makes_each_value_once(monkeypatch):
    # Merge9 [1].
    s = _empty(monkeypatch)
    s.run("CREATE ({id: 3})")
    s.run("UNWIND [1, 2, 3, 4] AS int MERGE (n {id: int})")
    assert s.run("MATCH (n) RETURN count(n) AS c") == [{"c": 4}]


def test_a_merge_after_a_merge_matches_the_loop_on_a_node_with_no_label(monkeypatch):
    # TCK Merge5 [18] [19]: a node with no label found again after a MERGE (then.UNLABELED) is an
    # end of the session's relationships; filtered by a label '' it never was, the MERGE made a
    # second relationship.
    s = _empty(monkeypatch)
    s.run("CREATE ({id: 0})")
    made = s.run(
        "MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[:T]->(b) WITH a AS x, b AS y "
        "MERGE (a) MERGE (b) MERGE (a)-[:T]->(b) RETURN x.id AS x, y.id AS y"
    )
    assert made.rows == [{"x": 0, "y": 0}]
    assert made.counters["relationships"] == 1


def test_the_ends_of_a_relationship_merged_with_no_direction_are_read(monkeypatch):
    # TCK Merge5 [11]: startNode(r)/endNode(r) of a session relationship found again with
    # unlabelled ends ("endpoint of 'r' unresolved"); made with no direction, it goes outward.
    s = _empty(monkeypatch)
    made = s.run(
        "CREATE (a {id: 2}), (b {id: 1}) MERGE (a)-[r:KNOWS]-(b) "
        "RETURN startNode(r).id AS s, endNode(r).id AS e"
    )
    assert made.rows == [{"s": 2, "e": 1}]


def test_a_match_maps_value_may_be_any_expression(monkeypatch):
    # TCK Unwind1 [6]: {year: event.year} in a MATCH is the equality y.year = event.year; only a
    # literal or a parameter was read ("Unexpected value token 'event'").
    s = _empty(monkeypatch)
    s.run("CREATE (:Year {year: 2016})")
    made = s.run(
        "UNWIND $events AS event MATCH (y:Year {year: event.year}) "
        "MERGE (e:Event {id: event.id}) MERGE (y)<-[:IN]-(e) RETURN e.id AS x ORDER BY x",
        events=[{"year": 2016, "id": 1}, {"year": 2016, "id": 2}],
    )
    assert made.rows == [{"x": 1}, {"x": 2}]
    assert made.counters["nodes"] == 2 and made.counters["relationships"] == 2


def test_a_merge_binds_the_path_of_one_node_it_made(monkeypatch):
    # TCK Merge1 [13]: MERGE p = (a {num: 1}) binds p ("Expected 'LPAREN', got 'IDENT'").
    from provisa.ephemeral import seam

    s = _empty(monkeypatch)
    made = s.run("MERGE p = (a {num: 1}) RETURN p")
    assert made.counters["nodes"] == 1
    [row] = seam.answered(made.rows, s.session)
    assert [n["properties"] for n in row["p"]["nodes"]] == [{"num": 1}]
    assert row["p"]["edges"] == []


def test_a_merge_binds_the_path_of_a_relationship_it_made(monkeypatch):
    # TCK Merge5 [10]: MERGE p = (a)-[:R]->(b), its relationship anonymous, binds p.
    from provisa.ephemeral import seam

    s = _empty(monkeypatch)
    made = s.run("MERGE (a {num: 1}) MERGE (b {num: 2}) MERGE p = (a)-[:R]->(b) RETURN p")
    assert made.counters == {"nodes": 2, "relationships": 1, "labels": 0}
    [row] = seam.answered(made.rows, s.session)
    assert [n["properties"] for n in row["p"]["nodes"]] == [{"num": 1}, {"num": 2}]
    assert [e["type"] for e in row["p"]["edges"]] == ["R"]
    again = s.run("MERGE p = (a {num: 1})-[:R]->(b {num: 2}) RETURN p")
    assert again.counters["relationships"] == 0 and len(again.rows) == 1


def test_a_merge_of_a_node_already_bound_is_refused(monkeypatch):
    # TCK Merge1 [15]: VariableAlreadyBound. Refused only by accident until MATCH (a) MATCH (a)
    # read its table twice; the read now runs, so the MERGE refuses it by name.
    s = _empty(monkeypatch)
    s.run("CREATE (:A)")
    with pytest.raises(CypherParseError, match="Variable `a` already declared"):
        s.run("MATCH (a) MERGE (a)")
