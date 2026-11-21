# Copyright (c) 2026 Kenneth Stott
# Canary: 0f6b3c9e-2d71-4a85-b4e3-8c1a7f5d2e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The rest of a statement after its write reads the rows the write gave (REQ-2176, REQ-039).

The rest runs as ``UNWIND $__bound AS __b ...`` (ephemeral.then): translated, the rows are
``UNNEST($1) AS _uw0(__b)`` and a value a row carries is ``"__b"."num"``. ``__b`` is a column
the FROM item binds, whose field the column reads -- a derived value, not a table. The column
check (V003) resolved a qualifier only by a FROM item's own name (84bb0f577d), found none for
``__b`` and refused ``"__b"."num"`` as naming no governed relation: every statement whose rest
carries a value past its write (the openCypher TCK's Delete6, List12, Merge1 [9], Merge9 [2],
Return2 [14], Unwind1 [6], and Match4 [4]'s setup) was refused. A client's UNWIND of a parameter
of maps (``UNWIND $events AS event ... event.id``) is the same shape."""

# Requirements: REQ-2176, REQ-039

from __future__ import annotations

import pytest

from provisa.compiler import temp_tables
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import validate_sql
from tests.unit.governed_session_harness import _ROLE, GovernedSession, _empty_model

pytestmark = pytest.mark.unit

_NODES = "\n".join(f"CREATE (:N {{num: {i}}})" for i in range(1, 6))
_RELS = "\n".join(f"CREATE ()-[:R {{num: {i}}}]->()" for i in range(1, 6))


@pytest.mark.parametrize(
    ("setup", "query", "expected"),
    [
        pytest.param(
            _NODES,
            "MATCH (n:N) WITH n, n.num AS num DELETE n WITH num WHERE num % 2 = 0 RETURN num",
            [{"num": 2}, {"num": 4}],
            id="Delete6-5",
        ),
        pytest.param(
            _NODES,
            "MATCH (n:N) WITH n, n.num AS num DELETE n RETURN sum(num) AS sum",
            [{"sum": 15}],
            id="Delete6-6",
        ),
        pytest.param(
            _NODES,
            "MATCH (n:N) WITH n, n.num AS num DELETE n WITH sum(num) AS sum RETURN sum",
            [{"sum": 15}],
            id="Delete6-7",
        ),
        pytest.param(
            _RELS,
            "MATCH ()-[r:R]->() WITH r, r.num AS num DELETE r WITH num WHERE num % 2 = 0 "
            "RETURN num",
            [{"num": 2}, {"num": 4}],
            id="Delete6-12",
        ),
        pytest.param(
            _RELS,
            "MATCH ()-[r:R]->() WITH r, r.num AS num DELETE r RETURN sum(num) AS sum",
            [{"sum": 15}],
            id="Delete6-13",
        ),
        pytest.param(
            "CREATE ()-[:T]->()",
            "MATCH ()-[r]->() DELETE r RETURN type(r)",
            [{"type(r)": "T"}],
            id="Return2-14",
        ),
    ],
)
def test_the_rest_after_a_delete_reads_the_values_it_carried(monkeypatch, setup, query, expected):
    s = GovernedSession(monkeypatch)
    s.run(setup)
    rows = s.run(query)
    assert sorted(map(sorted, (r.items() for r in rows.rows))) == sorted(
        map(sorted, (e.items() for e in expected))
    ), s.judged


def test_the_rest_after_a_set_reads_the_list_it_carried(monkeypatch):
    """List12 [1]: the comprehension's list is carried past the SET as ``__b.oldNames``."""
    s = GovernedSession(monkeypatch)
    s.run("CREATE (:Label1 {name: 'original'})")
    made = s.run(
        "MATCH (a:Label1) WITH collect(a) AS nodes "
        "WITH nodes, [x IN nodes | x.name] AS oldNames "
        "UNWIND nodes AS n SET n.name = 'newName' RETURN n.name, oldNames"
    )
    assert made.rows == [{"n.name": "newName", "oldNames": ["original"]}], s.judged


def test_the_rest_after_a_merge_reads_the_values_it_carried(monkeypatch):
    """Merge9 [2]: ``actor`` is carried past the first MERGE as ``__b.actor``."""
    s = GovernedSession(monkeypatch)
    s.run(
        "UNWIND ['Keanu Reeves', 'Hugo Weaving', 'Carrie-Anne Moss', 'Laurence Fishburne'] "
        "AS actor MERGE (m:Movie {name: 'The Matrix'}) MERGE (p:Person {name: actor}) "
        "MERGE (p)-[:ACTED_IN]->(m)"
    )
    people = s.run("MATCH (p:Person)-[:ACTED_IN]->(:Movie) RETURN count(p) AS n")
    assert people == [{"n": 4}], s.judged


def test_an_unwound_value_s_field_is_a_derived_value():
    """The translated shape itself: ``__b`` is the UNNEST's column, its field read in the select
    list -- not a relation governance must resolve to a table."""
    sql = 'SELECT "__b"."num" AS num FROM UNNEST($1) AS _uw0(__b)'
    token = temp_tables.bind(temp_tables.TempSession())
    try:
        assert validate_sql(sql, CompilationContext(), _empty_model(), _ROLE, []) == []
    finally:
        temp_tables.unbind(token)


def test_a_qualifier_bound_nowhere_is_still_refused():
    sql = 'SELECT "__x"."num" AS num FROM UNNEST($1) AS _uw0(__b)'
    found = validate_sql(sql, CompilationContext(), _empty_model(), _ROLE, [])
    assert [v.code for v in found] == ["V003"], found


def test_a_chain_built_after_a_create_is_walked(monkeypatch):
    """Match4 [4]: the setup carries ``i`` and the node list past its CREATEs as ``__b``'s
    fields; the walk with no upper bound then finds the chain's end."""
    s = GovernedSession(monkeypatch)
    s.run(
        "CREATE (a {var: 'start'}), (b {var: 'end'}) WITH * UNWIND range(1, 20) AS i "
        "CREATE (n {var: i}) WITH a, b, [a] + collect(n) + [b] AS nodeList "
        "UNWIND range(0, size(nodeList) - 2, 1) AS i "
        "WITH nodeList[i] AS n1, nodeList[i+1] AS n2 CREATE (n1)-[:T]->(n2)"
    )
    rows = s.run("MATCH (n {var: 'start'})-[:T*]->(m {var: 'end'}) RETURN m.var AS var")
    assert rows == [{"var": "end"}], s.judged


def test_merges_after_a_with_read_the_values_it_carried(monkeypatch):
    """Merge1 [9]: ``x`` and ``y`` are carried past each MERGE as ``__b.x``, ``__b.y``."""
    s = GovernedSession(monkeypatch)
    s.run("UNWIND [0, 1, 2] AS x UNWIND [0, 1, 2] AS y CREATE ({x: x, y: y})")
    made = s.run(
        "MATCH (foo) WITH foo.x AS x, foo.y AS y MERGE (:N {x: x, y: y + 1}) "
        "MERGE (:N {x: x, y: y}) MERGE (:N {x: x + 1, y: y}) RETURN x, y"
    )
    rows = made.rows if hasattr(made, "rows") else made
    assert sorted((r["x"], r["y"]) for r in rows) == [(x, y) for x in range(3) for y in range(3)]
    assert s.run("MATCH (n:N) RETURN count(n) AS c") == [{"c": 15}], s.judged
