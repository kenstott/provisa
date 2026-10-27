# Copyright (c) 2026 Kenneth Stott
# Canary: 8a868a06-3e49-43df-885d-26bfa6af95e1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's ephemeral graph is held in its temporary tables (REQ-2176): nodes, node_labels,
rels and the anchor tables, each landed again whole as the graph grows, and gone with the
session."""

# Requirements: REQ-2176

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace

import pytest

from provisa.compiler.temp_tables import TempSession
from provisa.ephemeral import store
from provisa.ephemeral.store import NewNode, NewRel, Writes
from provisa.pgwire import temp_exec

pytestmark = pytest.mark.unit


def _state(engine: str = "duckdb") -> SimpleNamespace:
    return SimpleNamespace(federation_engine=SimpleNamespace(engine=SimpleNamespace(name=engine)))


@pytest.fixture
def landed(monkeypatch):
    """The store: what each landing wrote, read back as the store holds it."""
    tables: dict[str, tuple[list, list]] = {}

    async def land(state, session, name, batch, columns):
        tables[name] = (columns, [tuple(r.values()) for r in batch.to_pylist()])
        session.stored = True

    async def address(state, session, name):
        return name

    def system_rows(state, sql, name):
        return [c for c, _ in tables[name][0]], tables[name][1]

    monkeypatch.setattr(temp_exec, "_land", land)
    monkeypatch.setattr(temp_exec, "_store_address", address)
    monkeypatch.setattr(temp_exec, "_system_rows", system_rows)
    from provisa.core import settings_registry

    limits = {"temp_tables.max_per_session": 20, "temp_tables.max_rows": 10}
    monkeypatch.setattr(settings_registry, "value", lambda key: limits[key])
    return tables


def _write(session, writes, engine="duckdb"):
    asyncio.run(store.write(_state(engine), session, writes))


def test_a_node_lands_in_its_anchor_with_its_labels(landed):
    session = TempSession()
    anchor = store.anchor_table("Person")
    _write(session, Writes(nodes=[NewNode("e:1", anchor, ("Person", "Temp"), {"name": "Ann"})]))
    assert landed[anchor] == ([("node_id", "text"), ("name", "text")], [("e:1", "Ann")])
    assert landed[store.NODES][1] == [("e:1", anchor)]
    assert landed[store.NODE_LABELS][1] == [("e:1", "Person"), ("e:1", "Temp")]
    assert store.graph(session).labels == {"Person", "Temp"}


def test_a_later_node_grows_and_widens_its_anchor(landed):
    session = TempSession()
    anchor = store.anchor_table("T")
    _write(session, Writes(nodes=[NewNode("e:1", anchor, ("T",), {"v": 1})]))
    _write(session, Writes(nodes=[NewNode("e:2", anchor, ("T",), {"v": 2.5, "w": [1, 2]})]))
    columns, rows = landed[anchor]
    assert columns == [("node_id", "text"), ("v", "double"), ("w", "list<bigint>")]
    assert rows == [("e:1", 1.0, None), ("e:2", 2.5, [1, 2])]
    assert landed[store.NODES][1] == [("e:1", anchor), ("e:2", anchor)]


def test_on_trino_a_list_lands_as_json(landed):
    session = TempSession()
    anchor = store.anchor_table("T")
    _write(session, Writes(nodes=[NewNode("e:1", anchor, ("T",), {"w": [1, 2]})]), "trino")
    assert landed[anchor] == ([("node_id", "text"), ("w", "json")], [("e:1", "[1, 2]")])


def test_a_duration_and_a_time_with_its_offset_land_as_the_text_the_store_reads(landed):
    from provisa.gql.temporal import Duration

    session = TempSession()
    anchor = store.anchor_table("T")
    at = dt.time(12, 30, tzinfo=dt.timezone(dt.timedelta(hours=1)))
    _write(
        session, Writes(nodes=[NewNode("e:1", anchor, ("T",), {"d": Duration(1, 2, 3), "t": at})])
    )
    assert landed[anchor] == (
        [("node_id", "text"), ("d", "interval"), ("t", "zoned_time")],
        [("e:1", "1 months 2 days 3 microseconds", "12:30:00+01:00")],
    )


def test_a_tag_on_a_model_node_is_a_label_row_under_its_identity(landed):
    session = TempSession()
    _write(session, Writes(tags=[("Person|7", "Temp")]))
    assert landed[store.NODE_LABELS][1] == [("Person|7", "Temp")]
    assert store.NODES not in landed


def test_a_relationship_lands_its_ends_and_its_properties(landed):
    session = TempSession()
    _write(session, Writes(rels=[NewRel("r:1", "KNOWS", "e:1", "Person|7", {"since": 2019})]))
    assert landed[store.RELS][1] == [("r:1", "KNOWS", "e:1", "Person|7", store.rel_table("KNOWS"))]
    assert landed[store.rel_table("KNOWS")] == (
        [("rel_id", "text"), ("since", "bigint")],
        [("r:1", 2019)],
    )
    assert store.graph(session).types == {"KNOWS"}


def test_a_graph_past_the_sessions_rows_is_refused_by_name(landed):
    session = TempSession()
    anchor = store.anchor_table("T")
    nodes = [NewNode(f"e:{i}", anchor, ("T",), {}) for i in range(11)]
    with pytest.raises(ValueError, match="temp_tables.max_rows = 10"):
        _write(session, Writes(nodes=nodes))


def test_the_end_of_a_session_ends_its_graph(landed, monkeypatch):
    session = TempSession()
    _write(session, Writes(tags=[("Person|7", "Temp")]))

    async def dropped(*_args):
        return None

    import provisa.federation.store_scope as scope
    import provisa.pgwire.temp_keeper as keeper

    monkeypatch.setattr(scope, "drop_temp_schema", dropped)
    monkeypatch.setattr(keeper, "note_ended", dropped)
    monkeypatch.setattr(temp_exec, "_schema", lambda s: "schema")
    state = SimpleNamespace(
        federation_engine=SimpleNamespace(
            engine=SimpleNamespace(name="duckdb", materialize_store=lambda: "dsn")
        )
    )
    asyncio.run(temp_exec.end(session, state))
    assert store.graph(session).empty and session.tables == {}
