# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session relation is one CTE (REQ-2176, "A LABEL'S UNION IS ONE CTE"): each node pattern
instance reads a CTE of its label's union carrying the conditions on that variable alone, which
filter before the union is joined; instances whose definitions come out the same share one; a
relationship type is one CTE. Each is defined once, however often the statement reads it."""

# Requirements: REQ-2176

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp
import pytest

from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}


def _ctes(sql: str) -> dict[str, str]:
    tree = sqlglot.parse_one(sql, read="postgres")
    return {cte.alias: cte.this.sql(dialect="postgres") for cte in tree.find_all(exp.CTE)}


def _session(monkeypatch) -> Session:
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:T {k: 1})-[:R]->(:T {k: 2}), (:T {k: 3})")
    return s


def test_instances_without_conditions_share_one_cte(monkeypatch):
    s = _session(monkeypatch)
    sql = s.sql("MATCH (a:T), (c:T) RETURN a.k, c.k")
    nodes = [name for name in _ctes(sql) if name.startswith("__session_node__T")]
    assert len(nodes) == 1
    assert sql.count("cypher__anchor__T") == 1  # the union is written once


def test_instances_whose_conditions_differ_have_their_own_ctes(monkeypatch):
    s = _session(monkeypatch)
    sql = s.sql("MATCH (a:T {k: 1}), (b:T) RETURN a.k, b.k")
    nodes = {n: body for n, body in _ctes(sql).items() if n.startswith("__session_node__T")}
    assert len(nodes) == 2
    assert sum("= 1" in body for body in nodes.values()) == 1  # the condition is inside one
    assert _values(s.run("MATCH (a:T {k: 1}), (b:T) RETURN a.k, b.k")) == [
        (1, 1),
        (1, 2),
        (1, 3),
    ]


def test_instances_with_the_same_conditions_share_one_cte(monkeypatch):
    s = _session(monkeypatch)
    sql = s.sql("MATCH (a:T {k: 1}), (b:T {k: 1}) RETURN a.k, b.k")
    assert len([n for n in _ctes(sql) if n.startswith("__session_node__T")]) == 1


def test_a_where_condition_on_one_variable_sits_inside_its_cte(monkeypatch):
    s = _session(monkeypatch)
    sql = s.sql("MATCH (a:T)-[:R]->(b:T) WHERE a.k = 1 AND b.k > a.k RETURN b.k")
    bodies = [b for n, b in _ctes(sql).items() if n.startswith("__session_node__T")]
    assert any("= 1" in b for b in bodies)
    main = sqlglot.parse_one(sql, read="postgres")
    assert "> " in main.args["where"].sql()  # the condition naming both stays outside
    assert _values(s.run("MATCH (a:T)-[:R]->(b:T) WHERE a.k = 1 AND b.k > a.k RETURN b.k")) == [
        (2,)
    ]


def test_a_relationship_type_is_one_cte(monkeypatch):
    s = _session(monkeypatch)
    sql = s.sql("MATCH (a:T)-[:R]->(b:T), (c:T)-[:R]->(d:T) RETURN a.k")
    assert len([n for n in _ctes(sql) if n.startswith("__session_rel__R")]) == 1


def test_an_optional_matchs_condition_stays_where_it_is(monkeypatch):
    s = _session(monkeypatch)
    rows = s.run("MATCH (a:T) OPTIONAL MATCH (a)-[:R]->(b:T {k: 2}) RETURN a.k, b.k")
    assert _values(rows) == [(1, 2), (2, None), (3, None)]


def test_a_model_label_with_session_matches_reads_its_table_inside_the_cte(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:Person:Temp {name: 'Cy'})")
    sql = s.sql("MATCH (p:Person), (q:Person) RETURN p.name")
    [body] = [b for n, b in _ctes(sql).items() if n.startswith("__session_node__Person")]
    assert '"s"."person"' in body


def _values(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


def test_a_variable_length_path_reads_the_ctes_from_its_recursive_one(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:T {k: 1})-[:R]->(:T {k: 2})-[:R]->(:T {k: 3})")
    sql = s.sql("MATCH (a:T {k: 1})-[:R*1..2]->(b:T) RETURN b.k")
    names = list(_ctes(sql))
    assert len([n for n in names if n.startswith("__session_rel__R")]) == 1
    rows = s.run("MATCH (a:T {k: 1})-[:R*1..2]->(b:T) RETURN b.k")
    assert _values(rows) == [(2,), (3,)]
