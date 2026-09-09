# Copyright (c) 2026 Kenneth Stott
# Canary: 2e8b6d41-7c3f-4a95-b1d6-9f0e4a27c5b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1661: the query path lands a MATERIALIZED source it reads when that source has never
landed or has gone stale, through the engine's own materialize_pending, and stamps the node
freshness state the event loop reads."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.query_residency import ensure_resident, is_stale_of, stale_sources

pytestmark = pytest.mark.unit


def _source(sid, **kw):
    base = dict(
        id=sid,
        type=SimpleNamespace(value="sqlite"),
        change_signal="ttl",
        cache_ttl=None,
        freshness_gate=False,
        prefer_materialized=False,
        load_protected=False,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _table(sid, name, schema="pet_store"):
    return SimpleNamespace(source_id=sid, schema_name=schema, table_name=name)


def test_stale_sources_reports_never_landed_and_failed_lands():
    sources = [_source("a"), _source("b"), _source("c")]
    tables = {"a": [_table("a", "pets"), _table("a", "vets")], "b": [_table("b", "x")], "c": []}
    states = {
        "pet_store.pets": {"last_refresh_at": 100.0, "last_refresh_ok": True},
        "pet_store.vets": {"last_refresh_at": 50.0, "last_refresh_ok": False},
        "pet_store.x": None,
    }
    stamps, oks = stale_sources(sources, tables, states)
    assert stamps == {"a": 50.0, "b": None, "c": None}
    assert oks == {"a": False, "b": True, "c": True}


def test_is_stale_of_honours_never_landed_failure_and_ttl():
    sources = [_source("fresh"), _source("ttl", cache_ttl=60), _source("bad"), _source("never")]
    stamps = {"fresh": 1000.0, "ttl": 900.0, "bad": 1000.0, "never": None}
    oks = {"fresh": True, "ttl": True, "bad": False, "never": True}
    is_stale = is_stale_of(sources, stamps, oks, now=1000.0)
    assert not is_stale("fresh")
    assert is_stale("ttl")  # 100s old against a 60s ttl
    assert is_stale("bad")
    assert is_stale("never")


class _Conn:
    def __init__(self, states, recorded):
        self._states = states
        self._recorded = recorded


class _Db:
    def __init__(self, states):
        self.states = states
        self.recorded: list[tuple[str, bool]] = []

    def acquire(self):
        db = self

        class _Ctx:
            async def __aenter__(self):
                return _Conn(db.states, db.recorded)

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class _Backend:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    async def materialize_pending(self, state, *, loader, is_stale, source_ids, **kw):
        self.calls.append((set(source_ids), kw["now"]))
        if self.fail:
            raise RuntimeError("adapter down")
        landed = []
        for sid in source_ids:
            if is_stale(sid):
                landed += [(sid, t.table_name) for t in state.config.tables if t.source_id == sid]
        return landed


def _state(sources, tables, backend):
    engine = SimpleNamespace(engine=SimpleNamespace(backend=backend, native_store="snowflake"))
    return SimpleNamespace(
        federation_engine=engine,
        config=SimpleNamespace(sources=sources, tables=tables),
        tenant_db=_Db({}),
    )


@pytest.fixture
def wiring(monkeypatch):
    async def get_node_state(conn, node):
        return conn._states.get(node)

    async def record_refresh(conn, node, *, at, ok):
        conn._recorded.append((node, ok))

    monkeypatch.setattr("provisa.events.queue.get_node_state", get_node_state)
    monkeypatch.setattr("provisa.events.queue.record_refresh", record_refresh)
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda state, engine: {})
    monkeypatch.setattr(
        "provisa.events.source_loader.SourceRowLoader", lambda engine, adapter_loaders: object()
    )
    monkeypatch.setattr("provisa.events.land_lock._locks", {})


@pytest.mark.asyncio
async def test_a_never_landed_source_is_landed_and_stamped(wiring):
    backend = _Backend()
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    landed = await ensure_resident(state, {"pets-db"})
    assert landed == [("pets-db", "pets")]
    assert backend.calls and backend.calls[0][0] == {"pets-db"}
    assert state.tenant_db.recorded == [("pet_store.pets", True)]


@pytest.mark.asyncio
async def test_a_resident_source_is_left_alone(wiring):
    backend = _Backend()
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    state.tenant_db.states["pet_store.pets"] = {"last_refresh_at": 1.0, "last_refresh_ok": True}
    assert await ensure_resident(state, {"pets-db"}) == []
    assert state.tenant_db.recorded == []


@pytest.mark.asyncio
async def test_only_the_sources_the_plan_names_are_considered(wiring):
    backend = _Backend()
    state = _state(
        [_source("a"), _source("b")], [_table("a", "pets"), _table("b", "vets")], backend
    )
    assert await ensure_resident(state, {"b"}) == [("b", "vets")]
    assert await ensure_resident(state, set()) == []
    assert await ensure_resident(state, {"unknown"}) == []


@pytest.mark.asyncio
async def test_a_failed_land_is_logged_stamped_not_ok_and_does_not_block_the_read(wiring, caplog):
    backend = _Backend(fail=True)
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    assert await ensure_resident(state, {"pets-db"}) == []
    assert state.tenant_db.recorded == [("pet_store.pets", False)]
    assert "landing pets-db failed" in caplog.text


@pytest.mark.asyncio
async def test_without_an_engine_or_store_nothing_happens():
    assert await ensure_resident(SimpleNamespace(), {"pets-db"}) == []
