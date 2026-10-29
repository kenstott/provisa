# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher's index statements are the graph session's own indexes (REQ-2176, amended
2026-10-28): CREATE INDEX on a session label's anchor table or a relationship type's property
table in the scratch store, living with the session; refused on a model label; SHOW INDEXES
lists them and the built-in ones, marked; DROP INDEX drops one and refuses a built-in."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest

from provisa.cypher.parser import CypherParseError
from provisa.ephemeral import store
from provisa.ephemeral.indexes import Indexed, is_index_statement
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_MODEL = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann')")}


def _session(monkeypatch, *, in_place: bool = True) -> Session:
    return Session(monkeypatch, _MODEL, in_place=in_place)


def _calls(s: Session) -> list[str]:
    """What the store's in-place face was asked to do, in order."""
    assert s.rows is not None
    return s.rows.calls


def _indexed(s: Session, table: str) -> list[str]:
    """The columns of each non-unique index DuckDB holds on ``table``."""
    rows = s.duck.execute(
        "SELECT expressions FROM duckdb_indexes() WHERE table_name = ? AND NOT is_unique", [table]
    ).fetchall()
    return sorted(str(r[0]) for r in rows)


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE INDEX FOR (n:T) ON (n.a)",
        "create range index ix if not exists for (n:T) on (n.a, n.b)",
        "SHOW INDEXES",
        "SHOW INDEX",
        "DROP INDEX ix",
    ],
)
def test_index_statements_are_known(statement):
    assert is_index_statement(statement)


def test_create_index_indexes_a_session_labels_anchor(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:T {a: 1, b: 'x'})")
    assert s.run("CREATE INDEX ix FOR (n:T) ON (n.a, n.b)") == Indexed(1, 0)
    assert "lookup cypher__anchor__T a,b" in _calls(s)
    assert len(_indexed(s, "cypher__anchor__T")) == 1
    assert s.run("MATCH (n:T) WHERE n.a = 1 RETURN n.b AS b") == [{"b": "x"}]


def test_an_index_made_before_its_label_has_nodes_is_built_when_it_does(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE INDEX FOR (n:T) ON (n.a)")
    assert not any(c.startswith("lookup cypher__anchor__T") for c in _calls(s))
    s.run("CREATE (:T {b: 1})")  # no column a yet: no index
    assert not any(c.startswith("lookup cypher__anchor__T") for c in _calls(s))
    s.run("CREATE (:T {a: 2})")
    assert "lookup cypher__anchor__T a" in _calls(s)
    assert len(_indexed(s, "cypher__anchor__T")) == 1


def test_create_index_on_a_relationship_type(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:T)-[:R {w: 2}]->(:T)")
    assert s.run("CREATE INDEX FOR ()-[r:R]-() ON (r.w)") == Indexed(1, 0)
    assert "lookup cypher__rel__R w" in _calls(s)


def test_create_index_on_a_model_label_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(CypherParseError, match="Person is a model label") as err:
        s.run("CREATE INDEX FOR (p:Person) ON (p.name)")
    assert "scratch store" in str(err.value)
    assert "REQ-" not in str(err.value)


def test_a_second_index_of_one_name_or_definition_is_refused_unless_if_not_exists(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE INDEX ix FOR (n:T) ON (n.a)")
    with pytest.raises(CypherParseError, match="An index ix already exists"):
        s.run("CREATE INDEX ix FOR (n:U) ON (n.a)")
    with pytest.raises(CypherParseError, match="An index ix already exists"):
        s.run("CREATE INDEX other FOR (n:T) ON (n.a)")
    assert s.run("CREATE INDEX ix IF NOT EXISTS FOR (n:T) ON (n.a)") == Indexed(0, 0)


def test_a_text_index_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(CypherParseError, match="A TEXT index is not made"):
        s.run("CREATE TEXT INDEX FOR (n:T) ON (n.a)")


def test_show_indexes_lists_the_sessions_and_the_built_in_ones(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:T {a: 1})-[:R]->(:T)")
    s.run("CREATE INDEX ix FOR (n:T) ON (n.a)")
    shown = s.run("SHOW INDEXES")
    assert shown.columns[:5] == ["name", "type", "entityType", "labelsOrTypes", "properties"]
    own = [r for r in shown.rows if not r["builtIn"]]
    assert own == [
        {
            "name": "ix",
            "type": "RANGE",
            "entityType": "NODE",
            "labelsOrTypes": ["T"],
            "properties": ["a"],
            "table": "cypher__anchor__T",
            "builtIn": False,
        }
    ]
    built_in = {(r["table"], tuple(r["properties"])) for r in shown.rows if r["builtIn"]}
    assert {
        (store.NODES, ("node_id",)),
        (store.RELS, ("rel_id",)),
        (store.RELS, ("from_id",)),
        (store.RELS, ("to_id",)),
        (store.RELS, ("type",)),
        (store.NODE_LABELS, ("node_id", "label")),
        (store.NODE_LABELS, ("label", "node_id")),
        ("cypher__anchor__T", ("node_id",)),
    } <= built_in


def test_drop_index_drops_a_session_index(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:T {a: 1})")
    s.run("CREATE INDEX ix FOR (n:T) ON (n.a)")
    assert s.run("DROP INDEX ix") == Indexed(0, 1)
    assert "drop cypher__anchor__T a" in _calls(s)
    assert _indexed(s, "cypher__anchor__T") == []
    assert [r for r in s.run("SHOW INDEXES").rows if not r["builtIn"]] == []
    s.run("CREATE (:T {a: 2})")  # a dropped index is not made again
    assert _indexed(s, "cypher__anchor__T") == []


def test_drop_index_refuses_a_built_in_one_and_an_unknown_name(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE (:T {a: 1})")
    built_in = next(r["name"] for r in s.run("SHOW INDEXES").rows if r["builtIn"])
    with pytest.raises(CypherParseError, match="is a built-in index"):
        s.run(f"DROP INDEX {built_in}")
    with pytest.raises(CypherParseError, match="No session index is named nope"):
        s.run("DROP INDEX nope")
    assert s.run("DROP INDEX nope IF EXISTS") == Indexed(0, 0)


def test_a_session_index_lives_with_the_session(monkeypatch):
    s = _session(monkeypatch)
    s.run("CREATE INDEX ix FOR (n:T) ON (n.a)")
    held = store.graph_from_json(store.graph_to_json(store.graph(s.session)))
    assert held.indexes == {"ix": ("node", "T", ("a",))}
    # A graph kept before sessions held indexes is read with none.
    kept = store.graph_to_json(store.Graph())
    del kept["indexes"]
    assert store.graph_from_json(kept).indexes == {}


def test_an_index_on_a_store_landing_tables_whole_is_held_and_listed(monkeypatch):
    s = _session(monkeypatch, in_place=False)
    s.run("CREATE (:T {a: 1})")
    assert s.run("CREATE INDEX ix FOR (n:T) ON (n.a)") == Indexed(1, 0)
    assert [r["name"] for r in s.run("SHOW INDEXES").rows if not r["builtIn"]] == ["ix"]
    assert s.run("DROP INDEX ix") == Indexed(0, 1)
