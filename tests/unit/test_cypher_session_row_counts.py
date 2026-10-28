# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""openCypher TCK scenarios read over a session's graph (REQ-2176, REQ-2167): each scenario's setup
CREATE builds the session's graph, its query reads it, and its rows are the TCK's.

Each was answered with the wrong number of rows. The causes, each covering the scenarios below it:

* an end's label held only where the pattern names the end -- (:A)-[r]->(:B) read every
  relationship into a B (Match2 [2]);
* a pattern predicate or EXISTS { } over an outer node drawn from several labels matched the
  pattern anew, tied to no outer row (Pattern1 [1], [4], [6]; ExistentialSubquery1 [1], [2];
  ExistentialSubquery2 [1]);
* a hop read from every relationship replaced the rows read before it instead of going on from
  them (Match3 [9]; ReturnSkipLimit2 [8]; Graph4 [4]), read (a)<--(b) as (a)-->(b) (Match6 [4]),
  and read (a)--(b) from one end only (Match3 [5]);
* a relationship a WITH carries, matched again, was matched anew (With1 [3], With6 [2]);
* an item of a list of several types compared with, or tested as a string against, a value of one
  kind by the engine's cast of its JSON (Comparison1 [2], [3]; String8 [8], String10 [8]).
"""

# Requirements: REQ-2176, REQ-2167

from __future__ import annotations

from collections import Counter
from typing import Any

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _node(*labels: str, **props: Any) -> tuple:
    return ("node", frozenset(labels), tuple(sorted((k, str(v)) for k, v in props.items())))


def _rel(rel_type: str) -> tuple:
    return ("relationship", rel_type)


def _value(got: Any) -> Any:
    """An answer's value as the scenario names it: a node by its labels and properties (each as
    text, as a node drawn from several labels carries them), a relationship by its type."""
    if isinstance(got, dict) and "type" in got and "startNode" in got:
        return _rel(got["type"])
    if isinstance(got, dict) and "label" in got and "properties" in got:
        labels = frozenset(label for label in got["label"].split(":") if label)
        props = tuple(
            sorted((k, str(v)) for k, v in got["properties"].items() if not k.startswith("_"))
        )
        return ("node", labels, props)
    return got


def _rows(rows: list[dict[str, Any]]) -> Counter:
    return Counter(tuple((k, _value(v)) for k, v in sorted(r.items())) for r in rows)


def _expected(*rows: dict[str, Any]) -> Counter:
    return Counter(tuple(sorted(r.items())) for r in rows)


_PATTERN1 = "CREATE (a:A)-[:REL1]->(b:B), (b)-[:REL2]->(a), (a)-[:REL3]->(:C), (a)-[:REL1]->(:D)"
_EXISTS1 = (
    "CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}), "
    "(a)-[:R]->(:C {prop: 2}), (a)-[:R]->(:D {prop: 3})"
)
_FORWARDED = "CREATE ()-[:T1]->(:X), ()-[:T2]->(:X), ()-[:T3]->()"
_STRINGS = (
    "CREATE (:TheLabel {name: 'ABCDEF'}), (:TheLabel {name: 'AB'}), "
    "(:TheLabel {name: 'abcdef'}), (:TheLabel {name: 'ab'}), "
    "(:TheLabel {name: ''}), (:TheLabel)"
)

#: (scenario, its setup, its query, its rows -- in any order)
_SCENARIOS: list[tuple[str, list[str], str, list[dict[str, Any]]]] = [
    (
        "Match2 [2] a label predicate on both sides",
        ["CREATE (:A)-[:T1]->(:B), (:B)-[:T2]->(:A), (:B)-[:T3]->(:B), (:A)-[:T4]->(:A)"],
        "MATCH (:A)-[r]->(:B) RETURN r",
        [{"r": _rel("T1")}],
    ),
    (
        "Match3 [5] two subgraphs with a bound undirected relationship",
        ["CREATE (a:A {num: 1})-[:REL {name: 'r'}]->(b:B {num: 2})"],
        "MATCH (a)-[r {name: 'r'}]-(b) RETURN a, b",
        [
            {"a": _node("B", num=2), "b": _node("A", num=1)},
            {"a": _node("A", num=1), "b": _node("B", num=2)},
        ],
    ),
    (
        "Match3 [9] related to related to",
        ["CREATE (a:A {num: 1})-[:KNOWS]->(b:B {num: 2})-[:FRIEND]->(c:C {num: 3})"],
        "MATCH (n)-->(a)-->(b) RETURN b",
        [{"b": _node("C", num=3)}],
    ),
    (
        "Match6 [4] direction respected when matching a non-existent path",
        ["CREATE (a {name: 'a'}), (b {name: 'b'}) CREATE (a)-[:T]->(b)"],
        "MATCH p = ({name: 'a'})<--({name: 'b'}) RETURN p",
        [],
    ),
    (
        "ReturnSkipLimit2 [8] limit to more rows than there are",
        [
            "CREATE (a:A), (n1 {num: 1}), (n2 {num: 2}), (m1), (m2) "
            "CREATE (a)-[:T]->(n1), (n1)-[:T]->(m1), (a)-[:T]->(n2), (n2)-[:T]->(m2)"
        ],
        "MATCH (a:A)-->(n)-->(m) RETURN n.num, count(*) ORDER BY n.num LIMIT 1000",
        [{"n.num": 1, "count(*)": 1}, {"n.num": 2, "count(*)": 1}],
    ),
    (
        "With1 [3] forwarding a relationship variable",
        [_FORWARDED],
        "MATCH ()-[r1]->(:X) WITH r1 AS r2 MATCH ()-[r2]->() RETURN r2 AS rel",
        [{"rel": _rel("T1")}, {"rel": _rel("T2")}],
    ),
    (
        "With6 [2] a relationship variable as a grouping key",
        [_FORWARDED],
        "MATCH ()-[r1]->(:X) WITH r1 AS r2, count(*) AS c MATCH ()-[r2]->() RETURN r2 AS rel",
        [{"rel": _rel("T1")}, {"rel": _rel("T2")}],
    ),
    (
        "Comparison1 [2] number-typed float comparison",
        ["CREATE ({id: 0})"],
        "WITH collect([0.5, 0]) AS numbers UNWIND numbers AS arr WITH arr[0] AS expected "
        "MATCH (n) WHERE toInteger(n.id) = expected RETURN n",
        [],
    ),
    (
        "Comparison1 [3] any-typed string comparison",
        ["CREATE ({id: 0})"],
        "WITH collect(['0', 0]) AS things UNWIND things AS arr WITH arr[0] AS expected "
        "MATCH (n) WHERE toInteger(n.id) = expected RETURN n",
        [],
    ),
    (
        "ExistentialSubquery1 [1] a subquery without WHERE",
        [_EXISTS1],
        "MATCH (n) WHERE exists { (n)-->() } RETURN n",
        [{"n": _node("A", prop=1)}],
    ),
    (
        "ExistentialSubquery1 [2] a subquery with WHERE",
        [_EXISTS1 + ", (b)-[:R]->(d)"],
        "MATCH (n) WHERE exists { (n)-->(m) WHERE n.prop = m.prop } RETURN n",
        [{"n": _node("A", prop=1)}],
    ),
    (
        "ExistentialSubquery2 [1] a full existential subquery",
        [_EXISTS1],
        "MATCH (n) WHERE exists { MATCH (n)-->() RETURN true } RETURN n",
        [{"n": _node("A", prop=1)}],
    ),
    (
        "Graph4 [4] type() of null and non-null relationships",
        ["CREATE ()-[:T]->()"],
        "MATCH (a) OPTIONAL MATCH (a)-[r:T]->() RETURN type(r)",
        [{"type(r)": "T"}, {"type(r)": None}],
    ),
    (
        "Pattern1 [1] any single outgoing directed connection",
        [_PATTERN1],
        "MATCH (n) WHERE (n)-[]->() RETURN n",
        [{"n": _node("A")}, {"n": _node("B")}],
    ),
    (
        "Pattern1 [4] a specific type of single outgoing directed connection",
        [_PATTERN1],
        "MATCH (n) WHERE (n)-[:REL1]->() RETURN n",
        [{"n": _node("A")}],
    ),
    (
        "Pattern1 [6] a specific type of single incoming directed connection",
        [_PATTERN1],
        "MATCH (n) WHERE (n)<-[:REL1]-() RETURN n",
        [{"n": _node("B")}, {"n": _node("D")}],
    ),
    (
        "String10 [8] non-string operands of CONTAINS",
        [_STRINGS],
        "WITH [1, 3.14, true, [], {}, null] AS operands UNWIND operands AS op1 "
        "UNWIND operands AS op2 WITH op1 CONTAINS op2 AS v RETURN v, count(*)",
        [{"v": None, "count(*)": 36}],
    ),
    (
        "String8 [8] non-string operands of STARTS WITH",
        [_STRINGS],
        "WITH [1, 3.14, true, [], {}, null] AS operands UNWIND operands AS op1 "
        "UNWIND operands AS op2 WITH op1 STARTS WITH op2 AS v RETURN v, count(*)",
        [{"v": None, "count(*)": 36}],
    ),
]


@pytest.mark.parametrize(
    ("setup", "query", "rows"),
    [pytest.param(s, q, r, id=name) for name, s, q, r in _SCENARIOS],
)
def test_a_tck_scenario_over_a_session_answers_its_rows(monkeypatch, setup, query, rows):
    s = Session(monkeypatch, {})
    for statement in setup:
        s.run(statement)
    assert _rows(s.run(query)) == _expected(*rows)


def test_a_self_loop_read_undirected_is_one_row(monkeypatch):
    # Match2 [3]: either end of a relationship is read, a self-loop's once.
    s = Session(monkeypatch, {})
    s.run("CREATE (a) CREATE (a)-[:T]->(a)")
    assert _rows(s.run("MATCH ()-[r]-() RETURN type(r) AS r")) == _expected({"r": "T"})


def test_a_relationship_read_undirected_is_a_row_from_each_end(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {k: 1})-[:T]->(:B {k: 2})")
    assert _rows(s.run("MATCH (x)--(y) RETURN x.k, y.k")) == _expected(
        {"x.k": 1, "y.k": 2}, {"x.k": 2, "y.k": 1}
    )


def test_a_hop_of_no_type_goes_on_from_a_typed_one(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE (:A {k: 1})-[:T]->(:B {k: 2})-[:U]->(:C {k: 3}), (:B {k: 4})")
    rows = s.run("MATCH (a:A)-[:T]->(b:B)-->(c) RETURN b.k, c.k")
    assert _rows(rows) == _expected({"b.k": 2, "c.k": 3})


def test_a_pattern_predicate_negated_keeps_the_nodes_it_does_not_hold_for(monkeypatch):
    s = Session(monkeypatch, {})
    s.run(_PATTERN1)
    rows = s.run("MATCH (n) WHERE NOT (n)-[:REL1]->() RETURN n")
    assert _rows(rows) == _expected({"n": _node("B")}, {"n": _node("C")}, {"n": _node("D")})


def test_an_item_of_a_mixed_list_equals_a_number_of_its_kind(monkeypatch):
    s = Session(monkeypatch, {})
    s.run("CREATE ({id: 0})")
    rows = s.run(
        "WITH collect([0, 'x']) AS pairs UNWIND pairs AS arr WITH arr[0] AS expected "
        "MATCH (n) WHERE toInteger(n.id) = expected RETURN n.id"
    )
    assert _rows(rows) == _expected({"n.id": 0})
