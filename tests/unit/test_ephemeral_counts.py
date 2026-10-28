# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session graph is indexed and counted (REQ-2176, "SESSION GRAPHS ARE INDEXED AND COUNTED"):
its tables are indexed where a read looks rows up -- a relationship by either end and its type,
a node's labels by the label -- and store.Graph keeps, at write time, the nodes of each label,
the relationships of each type and of each (type, source label, target label). Each count is
what the session's own tables hold after every write."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from provisa.ephemeral import store
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}


def _held(s: Session) -> tuple[dict, dict]:
    """The counts the session's tables hold: each label's nodes and tags (unlabeled nodes as
    ""), each type's relationships."""
    duck = s.duck
    labels = dict(
        duck.execute('SELECT label, count(*) FROM "cypher__node_labels" GROUP BY label').fetchall()
    )
    unlabeled = duck.execute(
        'SELECT count(*) FROM "cypher__nodes" n WHERE NOT EXISTS '
        '(SELECT 1 FROM "cypher__node_labels" l WHERE l.node_id = n.node_id)'
    ).fetchone()[0]
    if unlabeled:
        labels[""] = unlabeled
    tables = {r[0] for r in duck.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
    types = (
        dict(duck.execute('SELECT type, count(*) FROM "cypher__rels" GROUP BY type').fetchall())
        if "cypher__rels" in tables
        else {}
    )
    return labels, types


@pytest.mark.parametrize(
    "statements",
    [
        [
            "CREATE (:A {k: 1})-[:R]->(:B), (:A {k: 2})-[:R]->(:B), ()-[:S]->(:A)",
            "MATCH (n:A) WHERE n.k = 1 SET n:Hot",
            "MATCH (n:Hot) REMOVE n:Hot",
            "MATCH (p:Person) WHERE p.name = 'Ann' SET p:Vip",
            "MATCH (n:A) WHERE n.k = 2 DETACH DELETE n",
            "MATCH (:A)-[r:R]->(:B) DELETE r",
        ]
    ],
)
def test_the_counts_are_what_the_tables_hold_after_every_write(monkeypatch, statements):
    s = Session(monkeypatch, _PEOPLE)
    for statement in statements:
        s.run(statement)
        g = store.graph(s.session)
        labels, types = _held(s)
        assert (g.label_counts, g.type_counts) == (labels, types), statement


def test_a_relationship_is_counted_by_its_ends_labels(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:A)-[:R]->(:B), (:A)-[:R]->(:B), (:A)-[:R]->(:C)")
    g = store.graph(s.session)
    assert g.pair_counts == {("R", "A", "B"): 2, ("R", "A", "C"): 1}


def test_the_tables_are_indexed_where_a_read_looks_rows_up(monkeypatch):
    s = Session(monkeypatch, _PEOPLE)
    s.run("CREATE (:A)-[:R]->(:B)")
    names = {r[0] for r in s.duck.execute("SELECT index_name FROM duckdb_indexes()").fetchall()}
    for expected in (
        "on_cypher__rels_from_id",
        "on_cypher__rels_to_id",
        "on_cypher__rels_type",
        "on_cypher__node_labels_label_node_id",
    ):
        assert expected in names
