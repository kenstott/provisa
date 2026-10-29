# Copyright (c) 2026 Kenneth Stott
# Canary: 9a41c6e2-07d3-4b8f-b1e5-3f62d8a0c947
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""openCypher's value semantics in a session's graph (REQ-347, REQ-2176, REQ-2167), as the TCK's
expression features state them: conversions that raise a TypeError on a value of a kind they do
not take (TypeConversion1-4), exact percentiles (Aggregation6), chained comparisons
(Comparison4), a property of several kinds compared and returned by its value (Comparison2),
labels() of an end of an unlabelled hop, count(DISTINCT) of an unmatched node (Aggregation8),
and a pattern comprehension tied to its row (List6)."""

# Requirements: REQ-347, REQ-2176, REQ-2167

from __future__ import annotations

import duckdb
import pytest

from provisa.cypher.expr_parser import parse_expression
from provisa.gql.expr_ast import Binary
from provisa.gql.translator_types import CypherTranslateError
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_EDGE = "CREATE ()-[:T]->()"


def _session(monkeypatch, *setup: str) -> Session:
    s = Session(monkeypatch, {})
    for statement in setup:
        s.run(statement)
    return s


# -- TypeConversion1-4: a TypeError, never null --------------------------------------------- #


@pytest.mark.parametrize(
    ("call", "first", "invalid"),
    [
        ("toBoolean", "true", "[]"),
        ("toBoolean", "true", "{}"),
        ("toBoolean", "true", "1.0"),
        ("toBoolean", "true", "n"),
        ("toBoolean", "true", "r"),
        ("toBoolean", "true", "p"),
        ("toInteger", "1", "[]"),
        ("toInteger", "1", "{}"),
        ("toInteger", "1", "n"),
        ("toInteger", "1", "r"),
        ("toInteger", "1", "p"),
        ("toFloat", "1.0", "true"),
        ("toFloat", "1.0", "[]"),
        ("toFloat", "1.0", "{}"),
        ("toFloat", "1.0", "n"),
        ("toFloat", "1.0", "r"),
        ("toFloat", "1.0", "p"),
        ("toString", "1, ''", "[]"),
        ("toString", "1, ''", "{}"),
        ("toString", "1, ''", "n"),
        ("toString", "1, ''", "r"),
        ("toString", "1, ''", "p"),
    ],
)
def test_a_conversion_of_a_kind_it_does_not_take_is_a_type_error(monkeypatch, call, first, invalid):
    s = _session(monkeypatch, _EDGE)
    query = f"MATCH p = (n)-[r:T]->() RETURN [x IN [{first}, {invalid}] | {call}(x)] AS list"
    with pytest.raises(duckdb.InvalidInputException, match="TypeError"):
        s.run(query)


def test_a_conversion_over_no_row_raises_nothing(monkeypatch):
    # The TypeError is the row's: a read of no row converts nothing.
    s = _session(monkeypatch, "CREATE (:Z)")
    assert s.run("MATCH (n:Z) WHERE 1 = 2 RETURN [x IN [1, n] | toInteger(x)] AS l") == []


def test_a_conversion_of_a_kind_it_takes_converts(monkeypatch):
    s = _session(monkeypatch)
    assert s.run("RETURN [x IN [1, 2.3, true, 'apa'] | toString(x)] AS l") == [
        {"l": ["1", "2.3", "true", "apa"]}
    ]
    assert s.run("WITH [2, 2.9, '1.7'] AS t RETURN [n IN t | toInteger(n)] AS l") == [
        {"l": [2, 2, 1]}
    ]
    assert s.run("WITH [3.4, 3, '5'] AS t RETURN [n IN t | toFloat(n)] AS l") == [
        {"l": [3.4, 3.0, 5.0]}
    ]
    assert s.run("UNWIND [null, '', ' tru '] AS v RETURN toBoolean(v) AS b") == [
        {"b": None},
        {"b": None},
        {"b": None},
    ]


@pytest.mark.parametrize("query", ["RETURN toInteger([1]) AS v", "RETURN toFloat(true) AS v"])
def test_a_conversion_the_statement_types_wrong_is_refused_when_read(monkeypatch, query):
    s = _session(monkeypatch)
    with pytest.raises(CypherTranslateError, match="TypeError"):
        s.run(query)


# -- Aggregation6: exact percentiles --------------------------------------------------------- #

_PRICES = "CREATE ({price: 10.0}), ({price: 20.0}), ({price: 30.0}), ({price: 40.0})"


@pytest.mark.parametrize(
    ("call", "p", "expected"),
    [
        ("percentileDisc", 0.0, 10.0),
        ("percentileDisc", 0.3, 20.0),
        ("percentileDisc", 0.6, 30.0),
        ("percentileDisc", 1.0, 40.0),
        ("percentileCont", 0.0, 10.0),
        ("percentileCont", 0.5, 25.0),
        ("percentileCont", 1.0, 40.0),
    ],
)
def test_a_percentile_is_exact(monkeypatch, call, p, expected):
    s = _session(monkeypatch, _PRICES)
    rows = s.run(f"MATCH (n) RETURN {call}(n.price, $p) AS v", p=p)
    assert [float(r["v"]) for r in rows] == [expected]


@pytest.mark.parametrize("p", [-1, 1.1, 1000])
def test_a_percentile_outside_zero_to_one_is_refused(monkeypatch, p):
    s = _session(monkeypatch, _PRICES)
    with pytest.raises(duckdb.Error):
        s.run("MATCH (n) RETURN percentileDisc(n.price, $p) AS v", p=p)


# -- Comparison2, Comparison4 ---------------------------------------------------------------- #

_CHILDREN = (
    "CREATE (root:Root)-[:T]->(:Child {var: 0}), (root)-[:T]->(:Child {var: 'xx'}), "
    "(root)-[:T]->(:Child)"
)


def test_a_property_of_several_kinds_orders_against_a_string_by_its_value(monkeypatch):
    s = _session(monkeypatch, _CHILDREN)
    rows = s.run("MATCH (:Root)-->(i:Child) WHERE i.var IS NOT NULL AND i.var > 'x' RETURN i.var")
    assert rows == [{"i.var": "xx"}]
    rows = s.run("MATCH (:Root)-->(i:Child) WHERE i.var IS NULL OR i.var > 'x' RETURN i.var")
    assert sorted(rows, key=repr) == [{"i.var": "xx"}, {"i.var": None}]


def test_comparisons_chain():
    chained = parse_expression("a < b = c <> d")
    assert isinstance(chained, Binary) and chained.op == "AND"
    assert parse_expression("(a < b) = c").op == "="


def test_a_chain_of_comparisons_is_each_pair(monkeypatch):
    s = _session(
        monkeypatch,
        "CREATE (a:A {prop1: 3, prop2: 4}), (b:B {prop1: 4, prop2: 5}), (c:C {prop1: 4, prop2: 4})",
        "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)",
        "MATCH (b:B), (c:C) CREATE (b)-[:R]->(c)",
        "MATCH (a:A), (c:C) CREATE (c)-[:R]->(a)",
    )
    rows = s.run("MATCH (n)-->(m) WHERE n.prop1 < m.prop1 = n.prop2 <> m.prop2 RETURN labels(m)")
    assert rows == [{"labels(m)": ["B"]}]


# -- labels(), count(DISTINCT), a pattern comprehension -------------------------------------- #


def test_labels_of_an_end_of_an_unlabelled_hop(monkeypatch):
    s = _session(monkeypatch, "CREATE (a:A {name: 'c'})", "MATCH (a:A) CREATE (a)-[:T]->()")
    assert s.run("MATCH (n)-->(m) RETURN labels(n) AS a, labels(m) AS b") == [{"a": ["A"], "b": []}]


def test_count_distinct_of_an_unmatched_node_is_zero(monkeypatch):
    s = _session(monkeypatch)
    assert s.run("OPTIONAL MATCH (a) RETURN count(DISTINCT a)") == [{"count(DISTINCT a)": 0}]


def test_a_pattern_comprehension_reads_its_own_rows_node(monkeypatch):
    s = _session(
        monkeypatch,
        "CREATE (:X {num: 42}), (:X {num: 43})",
        "MATCH (a:X {num: 42}) CREATE (a)-[:T]->()",
    )
    rows = s.run("MATCH (n:X) RETURN n.num, size([(n)--() | 1]) > 0 AS b")
    assert sorted(rows, key=repr) == [{"n.num": 42, "b": True}, {"n.num": 43, "b": False}]
