# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2f5d3c-7e41-4b86-a0c9-1d8e6f3b2a57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Functions and operators handed an argument of a type the engine has no overload for (REQ-347,
REQ-751, REQ-2173), each refused "No function matches the given name and argument types":

- avg() of a property no table holds, typed text by the union (avg(VARCHAR));
- an ORDER BY of a name the RETURN gives a value a node variable already has (n + 2 of the node);
- keys(n) and n[key] of a node of no one label (map_keys / list_extract of its row);
- a duration multiplied or divided by a number (the dividend cast to a float).

The scenarios are the openCypher TCK's (Return6 [17], ReturnOrderBy6 [1], With6 [5],
WithOrderBy4 [16], ReturnOrderBy5 [1], Graph7 [1]-[3], Graph8 [1]-[4], [8], Temporal8 [7]), run on
DuckDB over a session graph."""

# Requirements: REQ-347, REQ-751, REQ-2173, REQ-2167

from __future__ import annotations

import pytest

from provisa.gql.temporal import Duration
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _session(monkeypatch, graph: str | None = None) -> Session:
    s = Session(monkeypatch, {})
    if graph is not None:
        s.run(graph)
    return s


@pytest.mark.parametrize(
    ("statement", "column"),
    [
        ("MATCH (person) RETURN $age + avg(person.age) - 1000 AS v", "v"),  # Return6 [17]
        # ReturnOrderBy6 [1]
        ("MATCH (person) RETURN avg(person.age) AS v ORDER BY $age + avg(person.age) - 1000", "v"),
        ("MATCH (person) WITH $age + avg(person.age) - 1000 AS v RETURN *", "v"),  # With6 [5]
        # WithOrderBy4 [16]
        (
            "MATCH (person) WITH avg(person.age) AS v ORDER BY $age + avg(person.age) - 1000 "
            "RETURN v",
            "v",
        ),
    ],
)
def test_an_aggregate_of_a_property_no_table_holds_is_null(monkeypatch, statement, column):
    assert _session(monkeypatch).run(statement, age=38) == [{column: None}]


def test_an_average_of_a_property_no_node_has_is_null(monkeypatch):
    s = _session(monkeypatch, "CREATE ({name: 'x'})")
    assert s.run("MATCH (n) RETURN avg(n.age) AS a") == [{"a": None}]


def test_an_order_by_reads_the_value_its_name_shadows(monkeypatch):
    # ReturnOrderBy5 [1]
    s = _session(monkeypatch, "CREATE (n1 {num: 1}), (n2 {num: 3}), (n3 {num: -5})")
    rows = s.run("MATCH (n) RETURN n.num AS n ORDER BY n + 2")
    assert [r["n"] for r in rows] == [-5, 1, 3]


def test_a_with_order_by_reads_the_value_its_name_shadows(monkeypatch):
    s = _session(monkeypatch, "CREATE (n1 {num: 1}), (n2 {num: 3}), (n3 {num: -5})")
    rows = s.run("MATCH (n) WITH n.num AS n ORDER BY n + 2 DESC RETURN n")
    assert [r["n"] for r in rows] == [3, 1, -5]


@pytest.mark.parametrize(
    ("graph", "props"),
    [
        ("CREATE ({name: 'Andres', surname: 'Lopez'})", {"name", "surname"}),  # Graph8 [1]
        (  # Graph8 [2]
            "CREATE ({name: 'Andres', surname: 'Lopez'}), "
            "({otherName: 'Andres', otherSurname: 'Lopez'})",
            {"name", "surname", "otherName", "otherSurname"},
        ),
        ("CREATE ()", set()),  # Graph8 [3]
    ],
)
def test_the_keys_of_a_node_of_no_one_label(monkeypatch, graph, props):
    s = _session(monkeypatch, graph)
    rows = s.run("MATCH (n) UNWIND keys(n) AS x RETURN DISTINCT x AS theProps")
    assert {r["theProps"] for r in rows} == props


def test_the_keys_of_an_optional_node_that_matched_none(monkeypatch):
    # Graph8 [4]
    s = _session(monkeypatch, "CREATE ()")
    assert s.run("OPTIONAL MATCH (n) UNWIND keys(n) AS x RETURN DISTINCT x AS theProps") == []


def test_a_property_set_to_null_is_no_key(monkeypatch):
    # Graph8 [8]
    s = _session(monkeypatch, "CREATE ({exists: 42, missing: null})")
    rows = s.run(
        "MATCH (n) RETURN 'exists' IN keys(n) AS a, 'missing' IN keys(n) AS b, "
        "'missingToo' IN keys(n) AS c"
    )
    assert rows == [{"a": True, "b": False, "c": False}]


def test_a_node_property_read_by_a_computed_key(monkeypatch):
    # Graph7 [1]
    s = _session(monkeypatch, "CREATE ({name: 'Apa'})")
    rows = s.run("MATCH (n {name: 'Apa'}) RETURN n['nam' + 'e'] AS value")
    assert rows == [{"value": "Apa"}]


@pytest.mark.parametrize(
    ("statement", "params"),
    [
        ("CREATE (n {name: 'Apa'}) RETURN n['nam' + 'e'] AS value", {}),  # Graph7 [2]
        ("CREATE (n {name: 'Apa'}) RETURN n[$idx] AS value", {"idx": "name"}),  # Graph7 [3]
    ],
)
def test_a_created_node_property_read_by_a_computed_key(monkeypatch, statement, params):
    answered = _session(monkeypatch).run(statement, **params)
    assert answered.rows == [{"value": "Apa"}]


_DURATION = (
    "CREATE (:Duration {date: duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, "
    "seconds: 70})})"
)
_WHOLE = Duration(149, 14, 58_390_000_000)  # P12Y5M14DT16H13M10S
_DOUBLE = Duration(298, 28, 116_780_000_000)  # P24Y10M28DT32H26M20S
_HALF = Duration(74, 22, 48_068_000_000)  # P6Y2M22DT13H21M8S


@pytest.mark.parametrize(
    ("num1", "num2", "prod", "div"),
    [(1, 1, _WHOLE, _WHOLE), (2, 2, _DOUBLE, _HALF), (0.5, 0.5, _HALF, _DOUBLE)],
)
def test_a_duration_multiplied_or_divided_by_a_number(monkeypatch, num1, num2, prod, div):
    # Temporal8 [7]: a fraction of a month is carried into days, a fraction of a day into time.
    s = _session(monkeypatch, _DURATION)
    rows = s.run(f"MATCH (d:Duration) RETURN d.date * {num1} AS prod, d.date / {num2} AS div")
    assert rows == [{"prod": prod, "div": div}]


def test_a_number_times_a_duration(monkeypatch):
    # Its days and its time are scaled each on its own: 24 hours are no day.
    assert _session(monkeypatch).run("RETURN 2 * duration({days: 1, hours: 12}) AS a") == [
        {"a": Duration(0, 2, 86_400_000_000)}
    ]
