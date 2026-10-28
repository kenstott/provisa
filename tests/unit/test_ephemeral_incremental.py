# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's ephemeral graph written in place (REQ-2176): rows added and removed by key, a
column added or a bigint widened in place, each table indexed on its key when it lands -- and the
graph held exactly as landing each table again whole holds it."""

# Requirements: REQ-2176

from __future__ import annotations

import asyncio

import pytest

from provisa.ephemeral import store
from provisa.ephemeral.store import NewLabel, NewNode, NewRel, Tag, Writes
from provisa.pgwire import temp_exec
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit


def _node(n: int, labels: tuple[str, ...] = ("Person",), **props) -> NewNode:
    anchor = store.anchor_table(labels[0]) if labels else store.DEFAULT_ANCHOR
    return NewNode(f"e:{n}", anchor, labels, props)


def _rel(n: int, a: int, b: int, **props) -> NewRel:
    return NewRel(f"r:{n}", "KNOWS", f"e:{a}", f"e:{b}", props, ("Person",), ("Person",))


async def _series(s: Session) -> None:
    """Writes and removals touching every table, a column grown and widened."""
    st, ss = s.state, s.session
    await store.write(st, ss, Writes(nodes=[_node(1, name="a", age=1), _node(2, name="b")]))
    await store.write(
        st, ss, Writes(nodes=[_node(3, name="c", age=2.5, city="x")])
    )  # grows, widens
    await store.write(st, ss, Writes(nodes=[_node(4, ())], tags=[Tag("Movie|7", "Movie", "Seen")]))
    await store.write(st, ss, Writes(labels=[NewLabel("e:4", "Extra")]))
    await store.write(
        st, ss, Writes(rels=[_rel(1, 1, 2, since=2020), _rel(2, 2, 3), _rel(3, 1, 3)])
    )
    await store.write(st, ss, Writes(nodes=[_node(5, name="e", age="old")]))  # widens age to JSON
    await store.remove_labels(st, ss, {("e:4", "Extra"), ("Movie|7", "Seen")})
    await store.delete_rels(st, ss, ["r:2"])
    await store.delete_nodes(st, ss, ["e:2"])


def _held(s: Session) -> dict[str, tuple[list, list]]:
    """Each session table's columns and rows, as the store holds them."""
    out = {}
    for name, table in s.session.tables.items():
        rows = s.duck.execute(f'SELECT * FROM "{name}"').fetchall()
        out[name] = (table.columns, sorted(rows, key=repr))
    return out


def test_in_place_the_graph_is_held_as_landing_it_whole_holds_it(monkeypatch):
    in_place = Session(monkeypatch, {})
    asyncio.run(_series(in_place))
    whole = Session(monkeypatch, {}, in_place=False)
    asyncio.run(_series(whole))
    assert _held(in_place) == _held(whole)
    g, h = store.graph(in_place.session), store.graph(whole.session)
    assert (g.node_keys, g.node_anchors, g.label_anchors) == (
        h.node_keys,
        h.node_anchors,
        h.label_anchors,
    )
    # A deleted node's rows are gone from its anchor, nodes and node_labels (its relationships
    # are the caller's to delete).
    node_tables = {store.anchor_table("Person"), store.NODES, store.NODE_LABELS}
    held = _held(in_place)
    assert "e:2" not in g.node_anchors
    assert all("'e:2'" not in repr(held[t][1]) for t in node_tables)


def test_a_write_in_the_tables_shape_adds_its_rows_without_reading_the_table(monkeypatch):
    s = Session(monkeypatch, {})
    asyncio.run(store.write(s.state, s.session, Writes(nodes=[_node(1, name="a")])))
    read: list[str] = []
    real = temp_exec.held_rows

    async def held_rows(state, session, name):
        read.append(name)
        return await real(state, session, name)

    monkeypatch.setattr(temp_exec, "held_rows", held_rows)
    s.rows.calls.clear()
    asyncio.run(store.write(s.state, s.session, Writes(nodes=[_node(2, name="b")])))
    assert read == []
    assert s.rows.calls == [
        f"insert {store.anchor_table('Person')}",
        f"insert {store.NODES}",
        f"insert {store.NODE_LABELS}",
    ]


def test_a_new_column_is_added_and_a_bigint_widened_in_place(monkeypatch):
    s = Session(monkeypatch, {})
    anchor = store.anchor_table("Person")
    asyncio.run(store.write(s.state, s.session, Writes(nodes=[_node(1, age=1)])))
    s.rows.calls.clear()
    asyncio.run(store.write(s.state, s.session, Writes(nodes=[_node(2, age=2.5, city="x")])))
    assert s.rows.calls[:3] == [f"retype {anchor}.age", f"add {anchor}.city", f"insert {anchor}"]
    assert sorted(s.duck.execute(f'SELECT * FROM "{anchor}"').fetchall()) == [
        ("e:1", 1.0, None),
        ("e:2", 2.5, "x"),
    ]


def test_a_widening_only_landing_it_whole_expresses_relands_it_and_indexes_it_again(monkeypatch):
    s = Session(monkeypatch, {})
    anchor = store.anchor_table("Person")
    asyncio.run(store.write(s.state, s.session, Writes(nodes=[_node(1, age=1)])))
    s.rows.calls.clear()
    asyncio.run(store.write(s.state, s.session, Writes(nodes=[_node(2, age="old")])))
    assert f"insert {anchor}" not in s.rows.calls  # landed again whole, as JSON
    assert f"index {anchor}" in s.rows.calls
    assert sorted(s.duck.execute(f'SELECT * FROM "{anchor}"').fetchall()) == [
        ("e:1", "1"),
        ("e:2", '"old"'),
    ]


def test_each_table_is_indexed_on_its_key_when_it_lands(monkeypatch):
    s = Session(monkeypatch, {})
    asyncio.run(_series(s))
    indexed = {
        r[0]: r[1]
        for r in s.duck.execute("SELECT table_name, expressions FROM duckdb_indexes()").fetchall()
    }
    assert set(indexed) == set(s.session.tables)
    assert indexed[store.NODE_LABELS].replace('"', "").replace("'", "") == "[node_id, label]"
    assert indexed[store.RELS] == "[rel_id]"


def test_rows_added_in_place_are_held_to_the_sessions_rows(monkeypatch):
    s = Session(monkeypatch, {})
    asyncio.run(store.write(s.state, s.session, Writes(nodes=[_node(n) for n in range(1, 900)])))
    with pytest.raises(ValueError, match="cypher__anchor__Person"):
        asyncio.run(
            store.write(s.state, s.session, Writes(nodes=[_node(n) for n in range(900, 1200)]))
        )


@pytest.mark.parametrize("in_place", [True, False])
def test_a_session_whose_first_nodes_carry_no_label_still_holds_node_labels(monkeypatch, in_place):
    """Every read reads node_labels: it stands, indexed, from the session's first node on."""
    s = Session(monkeypatch, {}, in_place=in_place)
    s.run("CREATE ({name: 'bar'})")
    assert store.NODE_LABELS in s.session.tables
    assert s.run("MATCH (n) RETURN n.name AS x") == [{"x": "bar"}]
