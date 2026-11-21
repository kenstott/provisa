# Copyright (c) 2026 Kenneth Stott
# Canary: b8e1d4a7-3f20-4c69-9d5b-6a72e0c3f914
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A field of a value a WITH carries, and a session node read by the name the statement gives it
(REQ-039, REQ-2176).

J609's unknown-qualifier rule (62cd63bcef) refuses a column qualified by a name that matches no
relation in scope. Two Cypher shapes were refused by it though the statement binds the name:

- A field of a value a derived relation gives: ``WITH null AS m RETURN m.missing`` is
  ``SELECT "m"."missing" FROM _w0`` where ``_w0``'s SELECT gives ``m``; an unwound parameter of
  maps (``"prop"."login"``) and the rows a write gave (``"__b"."__live"``) are the same. The
  qualifier is a value column of the derived relation, checked where its own SELECT is.
- A session node read by an alias with a capital in it (``MERGE (newB:B ...)``): the read named
  the session's relation ``AS "newB"``, quoted, while the statement reads it as ``newB."num"``,
  which PostgreSQL folds to ``newb`` -- another name. The relation keeps the alias as the
  statement spells it.

A qualifier bound nowhere, and a field of a governed table's own column, are still refused."""

# Requirements: REQ-039, REQ-2176

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from tests.unit.governed_session_harness import GovernedSession
from tests.unit.test_v003_identifier_case import _ROLE, _gov

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        pytest.param("WITH null AS m RETURN m.missing", [{"m.missing": None}], id="Map1-2"),
        pytest.param(
            "WITH null AS map RETURN map.name IS NULL AS result",
            [{"result": True}],
            id="Null1-5-null",
        ),
        pytest.param(
            "WITH null AS map RETURN map.name IS NOT NULL AS result",
            [{"result": False}],
            id="Null2-5-null",
        ),
    ],
)
def test_a_field_of_a_null_a_with_carries_is_null(monkeypatch, query, expected):
    s = GovernedSession(monkeypatch)
    assert s.run(query) == expected, s.judged


def test_a_field_of_an_unwound_map_parameter(monkeypatch):
    """Unwind1 [14]."""
    s = GovernedSession(monkeypatch)
    rows = s.run(
        "UNWIND $props AS prop MERGE (p:Person {login: prop.login}) SET p.name = prop.name "
        "RETURN p.name, p.login",
        props=[{"login": "login1", "name": "name1"}, {"login": "login2", "name": "name2"}],
    )
    assert sorted(rows.rows, key=lambda r: r["p.login"]) == [
        {"p.name": "name1", "p.login": "login1"},
        {"p.name": "name2", "p.login": "login2"},
    ], s.judged


def test_a_write_that_gave_no_row_reads_none(monkeypatch):
    """Match5 [27]'s setup: a write that gave no row is followed by ``WITH __b WHERE
    __b.__live`` (ephemeral.then), the field of the row the WITH carries."""
    s = GovernedSession(monkeypatch)
    rows = s.run(
        "UNWIND $__bound AS __b WITH __b WHERE __b.__live RETURN 1 AS one",
        __bound=[{"__live": False}],
    )
    assert rows == [], s.judged


def test_a_session_node_is_read_by_the_alias_the_statement_spells(monkeypatch):
    """Merge5 [20]: ``newB`` is read as ``newB."__node_id"``; its relation is ``AS newB``."""
    s = GovernedSession(monkeypatch)
    s.run(
        "CREATE (a:A) CREATE (b1:B {num: 0}), (b2:B {num: 1}) CREATE (c1:C), (c2:C) "
        "CREATE (a)-[:REL]->(b1), (a)-[:REL]->(b2), (b1)-[:REL]->(c1), (b2)-[:REL]->(c2)"
    )
    s.run(
        "MATCH (a:A)-[ab]->(b:B)-[bc]->(c:C) DELETE ab, bc, b, c "
        "MERGE (newB:B {num: 1}) MERGE (a)-[:REL]->(newB) "
        "MERGE (newC:C) MERGE (newB)-[:REL]->(newC)"
    )
    assert s.run("MATCH (:A)-[:REL]->(b:B)-[:REL]->(:C) RETURN b.num AS num, count(*) AS n") == [
        {"num": 1, "n": 1}
    ], s.judged


def test_an_unwound_event_s_fields_after_a_merge(monkeypatch):
    """Unwind1 [6]: ``y`` is read by its own alias beside the unwound ``event``."""
    s = GovernedSession(monkeypatch)
    s.run("CREATE (:Year {year: 2016})")
    rows = s.run(
        "UNWIND $events AS event MATCH (y:Year {year: event.year}) "
        "MERGE (e:Event {id: event.id}) MERGE (y)<-[:IN]-(e) RETURN e.id AS x ORDER BY x",
        events=[{"year": 2016, "id": 1}, {"year": 2016, "id": 2}],
    )
    assert rows.rows == [{"x": 1}, {"x": 2}], s.judged


# -- the validator: what is a value column, and what is not ----------------------------------------


def _codes(sql: str) -> list[str]:
    found = validate_sql(
        sql, CompilationContext(), _gov(), _ROLE, [], bypass_relationship_guard=True
    )
    return [f"{v.code}: {v.message}" for v in found]


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param('WITH _w0 AS (SELECT NULL AS m) SELECT "m"."missing" FROM _w0', id="cte"),
        pytest.param('SELECT "m"."k" FROM (SELECT NULL AS m) AS _inner', id="derived-table"),
        pytest.param(
            'WITH _w0 AS (SELECT * FROM (SELECT _uw0."__b" AS "__b" FROM UNNEST($1) AS '
            '_uw0(__b)) AS _inner WHERE "__b"."__live") SELECT 1 AS one FROM _w0',
            id="unwound-row-in-a-derived-table",
        ),
    ],
)
def test_a_field_of_a_derived_value_column_is_read(sql):
    assert _codes(sql) == []


@pytest.mark.parametrize(
    "sql",
    [
        # A table's own column is not a value a qualifier reads a field of.
        pytest.param("SELECT name.x FROM bank.customer AS c", id="table-column"),
        # A name bound nowhere.
        pytest.param('WITH _w0 AS (SELECT NULL AS m) SELECT "q"."missing" FROM _w0', id="unbound"),
        # The CTE's SELECT is judged: a hidden column is refused there.
        pytest.param(
            "WITH _w0 AS (SELECT ssn AS m FROM bank.customer) SELECT m.k FROM _w0",
            id="cte-reads-hidden",
        ),
    ],
)
def test_what_is_not_a_derived_value_column_is_still_refused(sql):
    assert any(c.startswith("V003") for c in _codes(sql)), _codes(sql)
