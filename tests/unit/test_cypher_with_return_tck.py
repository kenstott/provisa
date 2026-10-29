# Copyright (c) 2026 Kenneth Stott
# Canary: 8cb0f845-2913-479e-aed2-e0207a5d07f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""WITH, RETURN, ORDER BY, SKIP, LIMIT, UNION and UNWIND over a session's graph, as the openCypher
TCK's With*, Return*, Union* and Unwind* features run them (REQ-347, REQ-2167, REQ-2176). Each
test names the scenario it stands for."""

# Requirements: REQ-347, REQ-2167, REQ-2176

from __future__ import annotations

import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _session(monkeypatch, setup: str | None = None) -> Session:
    s = Session(monkeypatch, {})
    if setup:
        s.run(setup)
    return s


def _props(rows, column):
    return [r[column]["properties"] for r in rows]


# --- a node only the session holds carries no _id ----------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:A) RETURN a",
        "MATCH (a:A) WITH a RETURN a",  # WithWhere3 [2], WithOrderBy4 [1]
        "MATCH (a:A) RETURN a AS a UNION MATCH (a:A) RETURN a AS a",  # Union1 [4]
    ],
)
def test_a_session_node_has_only_its_own_properties(monkeypatch, query):
    s = _session(monkeypatch, "CREATE (:A {num: 1})")
    assert _props(s.run(query), "a") == [{"num": 1}]


def test_a_session_node_read_by_label_is_still_found_by_a_later_create(monkeypatch):
    # The node's identity is its id (Label|e:N) once _id is not among its properties.
    s = _session(monkeypatch, "CREATE (:A {num: 1})")
    s.run("MATCH (a:A) CREATE (a)-[:R]->(:B {num: 2})")
    rows = s.run("MATCH (a:A)-[:R]->(b:B) RETURN a.num AS a, b.num AS b")
    assert rows == [{"a": 1, "b": 2}]


# --- a map of nodes keeps each node apart over Bolt -----------------------------------------


def test_nodes_inside_a_returned_map_get_their_own_ids():
    # Return2 [13]: {node1: n, rel: r, node2: m}. A node left with its text id packs as id 0,
    # and the driver reads two nodes of one id as one node with both labels.
    from provisa.gql.assembler import _apply_id_map

    a = {"id": "A|e:1", "label": "A", "tableLabel": "A", "properties": {}}
    b = {"id": "B|e:2", "label": "B", "tableLabel": "B", "properties": {}}
    rel = {"id": "r:3", "type": "T", "startNode": a, "endNode": b, "properties": {}}
    out = _apply_id_map({"node1": a, "rel": rel, "node2": b}, {"A|e:1": 7, "B|e:2": 8})
    assert out["node1"]["id"] == 7 and out["node2"]["id"] == 8
    assert out["rel"]["startNode"]["id"] == 7 and out["rel"]["endNode"]["id"] == 8


# --- a variable named as a SQL keyword ---------------------------------------------------------


def test_a_variable_named_like_a_sql_keyword_is_read(monkeypatch):
    # Return4 [11], With4 [6]: `<-[like]-` is read as the variable, never SQL's LIKE.
    s = _session(monkeypatch, "CREATE (:P {n: 1})<-[:T {d: 7}]-(:M)<-[:L {at: 3}]-(:P {n: 2})")
    rows = s.run(
        "MATCH (person:P)<--(message)<-[like]-(:P) "
        "WITH like.at AS likeTime, person AS person ORDER BY likeTime, message.id "
        "RETURN likeTime"
    )
    assert rows == [{"likeTime": 3}]


# --- a CREATE reads a node it made before -------------------------------------------------------


def test_a_create_reads_a_property_of_a_node_it_made_before(monkeypatch):
    # With2 [1], WithSkipLimit1 [1], WithSkipLimit2 [2]: their setups.
    s = _session(
        monkeypatch, "CREATE (a:End {num: 42, id: 0}), (:End {num: 3}), (:Begin {num: a.id})"
    )
    assert s.run("MATCH (b:Begin) RETURN b.num AS n") == [{"n": 0}]
    s.run("CREATE (a:X {k: 1}), (:Y {k: a.nope, s: 'a.k'})")
    assert s.run("MATCH (y:Y) RETURN y.k AS k, y.s AS s") == [{"k": None, "s": "a.k"}]


# --- SKIP and LIMIT -----------------------------------------------------------------------------

_FIVE = "CREATE ({name: 'A'}), ({name: 'B'}), ({name: 'C'}), ({name: 'D'}), ({name: 'E'})"


def _names(rows):
    return [r["n"]["properties"]["name"] for r in rows]


def test_skip_and_limit_take_parameters(monkeypatch):
    # ReturnSkipLimit1 [2], ReturnSkipLimit3 [2], WithSkipLimit3 [2]
    s = _session(monkeypatch, _FIVE)
    rows = s.run("MATCH (n) RETURN n ORDER BY n.name ASC SKIP $s LIMIT $l", s=2, l=2)
    assert _names(rows) == ["C", "D"]
    rows = s.run("MATCH (n) WITH n ORDER BY n.name ASC SKIP $s LIMIT $l RETURN n", s=2, l=2)
    assert _names(rows) == ["C", "D"]


def test_skip_and_limit_take_an_expression_that_reads_no_variable(monkeypatch):
    # ReturnSkipLimit1 [3], ReturnSkipLimit2 [6]
    s = _session(monkeypatch, _FIVE)
    assert s.run("MATCH (n) WITH n LIMIT toInteger(ceil(1.7)) RETURN count(*) AS count") == [
        {"count": 2}
    ]
    rows = s.run(
        "MATCH (n) WITH n SKIP toInteger(rand()*3) WITH count(*) AS count RETURN count > 0 AS ne"
    )
    assert rows == [{"ne": True}]


@pytest.mark.parametrize(
    ("query", "params"),
    [
        ("MATCH (n) RETURN n SKIP n.count", {}),  # ReturnSkipLimit1 [5]: reads a variable
        ("MATCH (n) RETURN n LIMIT n.count", {}),  # ReturnSkipLimit2 [9]
        ("MATCH (n) RETURN n SKIP $s", {"s": 1.5}),  # ReturnSkipLimit1 [8]: not an integer
        ("MATCH (n) RETURN n LIMIT $l", {"l": -1}),  # ReturnSkipLimit2 [10]: negative
        ("MATCH (n) RETURN n LIMIT 1.5", {}),  # ReturnSkipLimit2 [17]
        ("MATCH (n) RETURN n SKIP -1", {}),  # ReturnSkipLimit1 [7]
    ],
)
def test_a_count_that_is_no_whole_number_of_rows_is_refused(monkeypatch, query, params):
    s = _session(monkeypatch, _FIVE)
    with pytest.raises(Exception, match="SKIP|LIMIT"):
        s.run(query, **params)


def test_no_rows_still_answer_the_statements_columns():
    # ReturnSkipLimit2 [3]: LIMIT 0 answers the column n over Bolt.
    from provisa.bolt.session import _answer_columns

    assert _answer_columns([], ["n", "__cut"]) == ["n"]
    assert _answer_columns([{"n": 1}], ["n", "__cut"]) == ["n"]


# --- a node of no one label a WITH carries ------------------------------------------------------

_UNLABELLED = "CREATE ({name: 'A'}), ({name: 'B', num: 42})"


def test_a_property_of_an_unlabelled_node_a_with_carries_is_read_from_the_with(monkeypatch):
    s = _session(monkeypatch, _UNLABELLED)
    # WithWhere1 [1]
    rows = s.run("MATCH (a) WITH a WHERE a.name = 'B' RETURN a")
    assert _props(rows, "a") == [{"name": "B", "num": 42}]
    # Return8 [1]
    assert s.run("MATCH (n) WITH n WHERE n.num = 42 RETURN count(*)") == [{"count(*)": 1}]
    rows = s.run("MATCH (a) WITH a RETURN a.name AS name, a.nope AS nope ORDER BY name")
    assert rows == [{"name": "A", "nope": None}, {"name": "B", "nope": None}]


def test_a_skip_parameter_prints_as_written():
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.printer import print_cypher
    from provisa.gql.label_map import CypherLabelMap

    from tests.unit.ephemeral_harness import model_node

    lm = CypherLabelMap(nodes={"A": model_node("A", {"id": "bigint"})}, relationships={})
    text = print_cypher(parse_cypher("MATCH (a:A) RETURN a.id SKIP $s LIMIT 2", lm))
    assert text.endswith("SKIP $s\nLIMIT 2")


# --- a WITH's alias shadowing an earlier one ----------------------------------------------------


def test_an_alias_a_with_gives_again_is_its_new_value(monkeypatch):
    # WithOrderBy4 [7]: the second x is a.num + a.num2, not the first WITH's a.num2 % 3.
    s = _session(monkeypatch, "CREATE (:A {num: 1, num2: 4}), (:A {num: 5, num2: 2})")
    rows = s.run(
        "MATCH (a:A) WITH a, a.num2 % 3 AS x WITH a, a.num + a.num2 AS x ORDER BY x RETURN a.num, x"
    )
    assert rows == [{"a.num": 1, "x": 5}, {"a.num": 5, "x": 7}]


# --- what a projection keeps --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("query", "code"),
    [
        # With4 [5]
        ("MATCH (a) WITH a, count(*) RETURN a", "with_needs_alias"),
        # ReturnOrderBy2 [13]
        ("MATCH (a) RETURN DISTINCT a.name ORDER BY a.age", "order_by_not_projected"),
        # WithOrderBy4 [14]
        (
            "MATCH (a:A) WITH a.num2 % 3 AS mod, min(a.num + a.num2) AS min "
            "ORDER BY sum(a.num + a.num2) RETURN mod, min",
            "order_by_not_projected",
        ),
    ],
)
def test_a_projection_is_read_only_by_what_it_keeps(query, code):
    from provisa.cypher.parser import CypherParseMistake, parse_cypher
    from provisa.gql.label_map import CypherLabelMap
    from tests.unit.ephemeral_harness import model_node

    lm = CypherLabelMap(
        nodes={"A": model_node("A", {"num": "bigint", "num2": "bigint"})}, relationships={}
    )
    with pytest.raises(CypherParseMistake) as caught:
        parse_cypher(query, lm)
    assert caught.value.code == f"cypher.{code}"


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a) RETURN DISTINCT a.name ORDER BY a.name",
        "MATCH (a) RETURN a.name AS n, count(*) AS c ORDER BY n, c + count(*)",
        "MATCH (a) WITH a, a.name AS n ORDER BY a.num RETURN n",  # no DISTINCT: a is in scope
    ],
)
def test_a_projection_may_be_ordered_by_what_it_keeps(query):
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.label_map import CypherLabelMap

    parse_cypher(query, CypherLabelMap(nodes={}, relationships={}))


def test_an_aggregate_a_returned_map_takes_orders_its_rows():
    # The GraphQL front's lowered reads order by an aggregate their map item computes.
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.label_map import CypherLabelMap

    parse_cypher(
        "MATCH (n0) RETURN {k: n0.k} AS key, {count: count(*), avg: avg(n0.s)} AS agg "
        "ORDER BY count(*) DESC, avg(n0.s) DESC",
        CypherLabelMap(nodes={}, relationships={}),
    )


# --- a node a WITH carries, read on after it ----------------------------------------------------


def test_a_label_test_of_a_node_a_with_carries(monkeypatch):
    # WithWhere5 [2]
    s = _session(monkeypatch, "CREATE (:TextNode {var: 'text'}), ({var: 'x'})")
    rows = s.run("MATCH (i:TextNode) WITH i WHERE i.var > 'te' AND i:TextNode RETURN i.var")
    assert rows == [{"i.var": "text"}]
    assert s.run("MATCH (i) WITH i WHERE i:TextNode RETURN i.var") == [{"i.var": "text"}]


def test_a_node_a_with_names_anew_is_its_row(monkeypatch):
    # With1 [6]: WITH a AS a after an OPTIONAL MATCH; WITH i AS j renames it.
    s = _session(monkeypatch, "CREATE (:A {num: 42}), (:B {num: 46})")
    assert s.run("MATCH (i:A) WITH i AS j RETURN j.num") == [{"j.num": 42}]
    rows = s.run("OPTIONAL MATCH (a:A) WITH a AS a MATCH (b:B) RETURN a, b")
    assert _props(rows, "a") == [{"num": 42}] and _props(rows, "b") == [{"num": 46}]


_TWO_A = "CREATE (:A {k: 1})-[:REL]->(:B), (:A {k: 2})-[:REL]->(:B), (:X)"


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:A) WHERE a.k = 1 WITH a MATCH (a)-->(b) RETURN a.k",
        "MATCH (a:A) WHERE a.k = 1 WITH a MATCH (b)<--(a) RETURN a.k",
        "MATCH (a:A) WHERE a.k = 1 WITH a MATCH (a)-->(b), (x:X) RETURN a.k",
        # With1 [2], WithSkipLimit2 [3]: the carried rows are joined before a hop reads them.
        "MATCH (a:A) WHERE a.k = 1 WITH a MATCH (x:X), (a)-->(b) RETURN a.k",
    ],
)
def test_a_hop_of_no_type_from_a_node_a_with_carries_starts_at_that_node(monkeypatch, query):
    s = _session(monkeypatch, _TWO_A)
    assert s.run(query) == [{"a.k": 1}]
