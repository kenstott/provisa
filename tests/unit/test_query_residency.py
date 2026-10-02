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

import itertools
from types import SimpleNamespace

import pytest

from provisa.federation.query_residency import ensure_resident, is_stale_of, stale_sources
from provisa.federation.replica_address import ReplicaRoutes
from provisa.federation.replica_routing import table_floor

pytestmark = pytest.mark.unit


def _source(sid, **kw):
    base = dict(
        id=sid,
        type=SimpleNamespace(value="sqlite"),
        change_signal="ttl",
        cache_ttl=None,
        freshness_gate=False,
        replicate=None,
        load_protected=False,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _recent() -> float:
    """A refresh stamp inside every fixture table's TTL: REQ-1907 judges a replica's age against
    the reader's effective TTL, so a stamp that is to read as fresh has to be a recent one."""
    import time

    return time.time() - 1.0


_IDS = itertools.count(1)


def _table(sid, name, schema="pet_store", row_materialize=False, columns=None, cache_ttl=300):
    return SimpleNamespace(
        id=next(_IDS),
        source_id=sid,
        schema_name=schema,
        table_name=name,
        row_materialize=row_materialize,
        columns=columns or [],
        cache_ttl=cache_ttl,
        role_ttl={},  # REQ-1907
        change_signal=None,
        replicate=None,
        load_protected=None,
    )


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
    # REQ-1907 (amended 2026-09-30): a ttl-signal source needs a cache_ttl -- none is an error.
    sources = [_source(sid, cache_ttl=60) for sid in ("fresh", "ttl", "bad", "never")]
    tables = {sid: [_table(sid, sid, cache_ttl=None)] for sid in ("fresh", "ttl", "bad", "never")}
    states = {
        "pet_store.fresh": {"last_refresh_at": 1000.0, "last_refresh_ok": True},
        "pet_store.ttl": {"last_refresh_at": 900.0, "last_refresh_ok": True},
        "pet_store.bad": {"last_refresh_at": 1000.0, "last_refresh_ok": False},
        "pet_store.never": None,
    }
    is_stale = is_stale_of(sources, tables, states, 1000.0, reader_role=None)
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
        self.acquires = 0  # control-plane round trips

    def acquire(self):
        db = self
        db.acquires += 1

        class _Ctx:
            async def __aenter__(self):
                return _Conn(db.states, db.recorded)

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class _Backend:
    def __init__(self, fail=False, live=()):
        self.calls = []
        self.fail = fail
        self.dialect = "postgres"
        self._landed_this_process: set[str] = set()
        self.live = set(live)  # sources this engine reads in place: never landed
        self.replicated: dict[str, bool] = {}  # what the last plan was told, per source

    def pending_lands(self, sources, *, is_stale, **kw):
        """As EngineBackend.pending_lands: the sources a read must land first. A source the engine
        reads live never is; a landed one is when its staleness oracle says so."""
        self.replicated = {s.id: kw["replicated_of"](s.id) for s in sources}
        return [s.id for s in sources if s.id not in self.live and is_stale(s.id)]

    def is_first_touch(self, source_id: str) -> bool:
        return source_id not in self._landed_this_process

    def mark_landed(self, source_id: str) -> None:
        self._landed_this_process.add(source_id)

    async def materialize_pending(self, state, *, loader, is_stale, source_ids, **kw):
        self.calls.append((set(source_ids), kw["now"]))
        if self.fail:
            raise RuntimeError("adapter down")
        landed = []
        for sid in source_ids:
            if is_stale(sid):
                landed += [
                    (sid, t.table_name)
                    for t in state.config.tables
                    if t.source_id == sid and not getattr(t, "row_materialize", False)
                ]
        return landed

    def replica_address(self, state, *, source_id, schema_name, table_name):
        """As EngineBackend.replica_address: the replicas schema, under the one replica name."""
        from provisa.federation.replica_address import ReplicaAddress, replica_table_name

        del state
        return ReplicaAddress(
            "org_test_replicas", replica_table_name(source_id, schema_name, table_name)
        )


def _state(sources, tables, backend):
    engine = SimpleNamespace(
        engine=SimpleNamespace(
            backend=backend,
            native_store="snowflake",
            replica_store_backend=lambda: "snowflake",  # as FederationEngine: its native store
            materialize_store=lambda: "postgresql://localhost/materialize",
        )
    )
    return SimpleNamespace(
        federation_engine=engine,
        config=SimpleNamespace(sources=sources, tables=tables),
        tenant_db=_Db({}),
        replica_routes=ReplicaRoutes(floored=_floored(sources, tables)),
    )


def _floored(sources, tables) -> dict[int, tuple[str, str]]:
    """The floored tables as the schema build publishes them: each table the operator's
    settings put on its replica, by the one decision."""
    by_id = {s.id: s for s in sources}
    out: dict[int, tuple[str, str]] = {}
    for t in tables:
        setting = table_floor(by_id[t.source_id], t, promoted=False)
        if setting is not None:
            out[t.id] = (t.source_id, setting)
    return out


def _read(state) -> frozenset[int]:
    """A statement that reads every registered table of the fixture."""
    return frozenset(t.id for t in state.config.tables)


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
        "provisa.events.app_wiring.build_keyed_adapter_loaders", lambda state, engine=None: {}
    )
    monkeypatch.setattr(
        "provisa.events.source_loader.SourceRowLoader",
        lambda engine, adapter_loaders=None, keyed_adapter_loaders=None: object(),
    )
    monkeypatch.setattr("provisa.events.land_lock._locks", {})
    # REQ-1674: ensure_resident reads the registry view; here the config IS the registry
    # (these tests are about residency, not about where the rows come from).

    async def _sources(state, conn=None):
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)


@pytest.mark.asyncio
async def test_a_never_landed_source_is_landed_and_stamped(wiring):
    backend = _Backend()
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    landed = await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
    assert landed == [("pets-db", "pets")]
    assert backend.calls and backend.calls[0][0] == {"pets-db"}
    assert state.tenant_db.recorded == [("pet_store.pets", True)]


@pytest.mark.asyncio
async def test_row_materialize_table_never_swept_into_the_whole_source_land(wiring):
    """REQ-1865: a row_materialize=True table's residency is governed exclusively by
    ensure_rows_resident (called alongside this function at every real call site) -- it must
    never also be landed here. Confirmed live: this whole-source sweep landing a row_materialize
    table before ensure_rows_resident got a chance to help negated the entire point of the row
    cache (a keyed lookup paid the same full-table land cost row_materialize exists to avoid)."""
    backend = _Backend()
    state = _state(
        [_source("bench-neo4j")],
        [
            _table("bench-neo4j", "bench_order_node", schema="neo4j", row_materialize=True),
            _table("bench-neo4j", "bench_placed_edge", schema="neo4j"),
        ],
        backend,
    )
    landed = await ensure_resident(state, {"bench-neo4j"}, reader_role=None, table_ids=_read(state))
    assert landed == [("bench-neo4j", "bench_placed_edge")]
    assert ("bench-neo4j", "bench_order_node") not in landed


@pytest.mark.asyncio
async def test_a_row_level_table_is_never_landed_whole(wiring, monkeypatch):
    """REQ-1915: a table replicated row by row is read by key only — a statement that does not
    bind its key is refused at planning — so ``ensure_resident`` has no whole-table path for it.
    The one that used to be here (load the entire source table into the worker, upsert it one row
    at a time; `neo4j_materialize_cold`'s unfiltered count) is gone: nothing is loaded, nothing is
    landed, no lock is taken and no control-plane statement is issued for a source whose tables
    are all row-level, and the function no longer takes the statement's table names."""
    loaded: list = []

    class _Loader:
        async def load(self, source, table):
            loaded.append(table.table_name)
            return [{"order_id": 1}]

    monkeypatch.setattr(
        "provisa.events.source_loader.SourceRowLoader",
        lambda engine, adapter_loaders=None, keyed_adapter_loaders=None: _Loader(),
    )
    backend = _Backend()
    state = _state(
        [_source("bench-neo4j")],
        [
            _table(
                "bench-neo4j",
                "bench_order_node",
                schema="neo4j",
                row_materialize=True,
                columns=[SimpleNamespace(name="order_id", is_primary_key=True)],
            ),
        ],
        backend,
    )
    assert (
        await ensure_resident(state, {"bench-neo4j"}, reader_role=None, table_ids=_read(state))
        == []
    )
    assert loaded == [] and backend.calls == []
    assert state.tenant_db.acquires == 0 and state.tenant_db.recorded == []
    with pytest.raises(TypeError):
        await ensure_resident(
            state, {"bench-neo4j"}, unbound_targets={"bench_order_node"}, reader_role=None
        )  # type: ignore[call-arg]


@pytest.mark.asyncio
async def test_a_resident_source_is_left_alone(wiring):
    backend = _Backend()
    # REQ-1730: staleness is the persisted stamp OR this backend INSTANCE's own first-touch
    # signal (a genuine reboot reads as stale even with a fresh stamp, since THIS engine has
    # never held the row) — "already resident" here means both: a fresh stamp AND this process
    # has already landed it once.
    backend.mark_landed("pets-db")
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    # REQ-1907: staleness is judged per table against the table's own cache_ttl (300 here), so a
    # "fresh stamp" is one inside it.
    import time

    state.tenant_db.states["pet_store.pets"] = {
        "last_refresh_at": time.time(),
        "last_refresh_ok": True,
    }
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == []
    assert state.tenant_db.recorded == []


@pytest.mark.asyncio
async def test_only_the_sources_the_plan_names_are_considered(wiring):
    backend = _Backend()
    state = _state(
        [_source("a"), _source("b")], [_table("a", "pets"), _table("b", "vets")], backend
    )
    assert await ensure_resident(state, {"b"}, reader_role=None, table_ids=_read(state)) == [
        ("b", "vets")
    ]
    assert await ensure_resident(state, set(), reader_role=None, table_ids=_read(state)) == []
    assert await ensure_resident(state, {"unknown"}, reader_role=None, table_ids=_read(state)) == []


@pytest.mark.asyncio
async def test_only_the_tables_the_statement_reads_are_judged_and_stamped(wiring):
    """REQ-826: a statement that reads one table of a source neither lands nor stamps the
    source's other tables."""
    backend = _Backend()
    pets, vets = _table("pets-db", "pets"), _table("pets-db", "vets")
    state = _state([_source("pets-db")], [pets, vets], backend)
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids={pets.id})
    assert state.tenant_db.recorded == [("pet_store.pets", True)]


@pytest.mark.asyncio
async def test_a_setting_on_a_table_the_statement_does_not_read_does_not_move_its_read(wiring):
    """REQ-826: ``vets`` is set to always; a statement that reads only ``pets`` is not put on a
    replica by it, and one that reads ``vets`` is."""
    backend = _Backend(live={"pets-db"})
    pets, vets = _table("pets-db", "pets"), _table("pets-db", "vets")
    vets.replicate = 0
    state = _state([_source("pets-db")], [pets, vets], backend)
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids={pets.id})
    assert backend.replicated == {"pets-db": False}
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids={vets.id})
    assert backend.replicated == {"pets-db": True}


@pytest.mark.asyncio
async def test_a_live_table_read_beside_its_replica_served_sibling_is_left_alone(wiring):
    """REQ-826, per table: the engine reads the source in place, ``vets`` is set to always and
    ``pets`` is not. One statement reads both: only ``vets`` is landed and stamped. ``pets`` is
    read live through the engine's attach, so it is neither landed nor asked for a replication
    clock (it has none here)."""
    backend = _Backend()
    pets = _table("pets-db", "pets", cache_ttl=None)
    vets = _table("pets-db", "vets")
    vets.replicate = 0
    state = _state([_source("pets-db")], [pets, vets], backend)
    # this engine reads the source's type in place
    state.federation_engine.engine.connectors = {"sqlite": SimpleNamespace(reads_in_place=True)}
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids={pets.id, vets.id})
    assert backend.replicated == {"pets-db": True}
    assert state.tenant_db.recorded == [("pet_store.vets", True)]
    # and a statement that reads only the live table plans no land at all
    backend.replicated = {}
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids={pets.id})
    assert backend.replicated == {}
    assert state.tenant_db.recorded == [("pet_store.vets", True)]


@pytest.mark.asyncio
async def test_a_failed_land_is_stamped_not_ok_and_fails_the_read(wiring):
    """REQ-1661 (amended 2026-09-30): a failed land raises its own cause -- the query never reads
    the stale replica. The node is still stamped not ok, so the next query retries the land."""
    backend = _Backend(fail=True)
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    with pytest.raises(RuntimeError, match="adapter down"):
        await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
    assert state.tenant_db.recorded == [("pet_store.pets", False)]


@pytest.mark.asyncio
async def test_without_an_engine_or_store_nothing_happens():
    assert (
        await ensure_resident(SimpleNamespace(), {"pets-db"}, reader_role=None, table_ids=()) == []
    )


# -- one land per stale table, shared by the requests that need it (REQ-1661, REQ-1882) ---------


class _SlowBackend(_Backend):
    """Lands like _Backend, but holds each land open and counts the lands that did real work."""

    def __init__(self, hold: float = 0.3, both_inside=None):
        super().__init__()
        self.hold = hold
        self.both_inside = both_inside
        self.landed_calls: list[tuple[str, int]] = []

    async def materialize_pending(self, state, *, loader, is_stale, source_ids, **kw):
        import threading
        import time as _time

        landed = await super().materialize_pending(
            state, loader=loader, is_stale=is_stale, source_ids=source_ids, **kw
        )
        if landed:
            self.landed_calls += [(sid, threading.get_ident()) for sid, _ in landed]
            if self.both_inside is not None:
                self.both_inside.wait(timeout=10)
            else:
                _time.sleep(self.hold)  # blocks this request's thread, as a real land does
        return landed


def _stamping_refresh(monkeypatch):
    """record_refresh that writes the freshness state get_node_state reads, as the real one does."""

    async def record_refresh(conn, node, *, at, ok):
        conn._recorded.append((node, ok))
        conn._states[node] = {"last_refresh_at": at.timestamp(), "last_refresh_ok": ok}

    monkeypatch.setattr("provisa.events.queue.record_refresh", record_refresh)


def _on_request_threads(calls):
    """Run each call on its own thread and connection loop, started together; return results."""
    import threading

    from provisa.core.connection_loop import connection_loop

    start = threading.Barrier(len(calls))
    results: list = [None] * len(calls)
    errors: list[BaseException] = []

    def _request(i, make_coro):
        try:
            start.wait(timeout=10)
            with connection_loop() as cl:
                results[i] = cl.run(make_coro())
        except BaseException as exc:  # reported by the caller's assertion with the real cause
            errors.append(exc)

    threads = [threading.Thread(target=_request, args=(i, c)) for i, c in enumerate(calls)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors, errors
    return results


def test_two_requests_reading_one_stale_table_share_one_land(wiring, monkeypatch):
    """Both requests find the table stale. One lands it; the other waits for that land and then
    reads the fresh copy — it does not land the table a second time."""
    _stamping_refresh(monkeypatch)
    backend = _SlowBackend()
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)

    results = _on_request_threads(
        [
            lambda: ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)),
            lambda: ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)),
        ]
    )

    assert len(backend.landed_calls) == 1, f"the table was landed {len(backend.landed_calls)} times"
    assert sorted(results, key=len) == [[], [("pets-db", "pets")]]


def test_two_requests_reading_different_stale_tables_land_at_the_same_time(wiring, monkeypatch):
    import threading

    _stamping_refresh(monkeypatch)
    both_inside = threading.Barrier(2)
    backend = _SlowBackend(both_inside=both_inside)
    state = _state(
        [_source("pets-db"), _source("vets-db")],
        [_table("pets-db", "pets"), _table("vets-db", "vets")],
        backend,
    )

    results = _on_request_threads(
        [
            lambda: ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)),
            lambda: ensure_resident(state, {"vets-db"}, reader_role=None, table_ids=_read(state)),
        ]
    )

    assert both_inside.broken is False, "the two lands did not overlap"
    assert sorted(results) == [[("pets-db", "pets")], [("vets-db", "vets")]]
    assert len({ident for _, ident in backend.landed_calls}) == 2


# -- the staleness decision is made in memory (REQ-1661 amended 2026-10-01) ----------------------
#
# ensure_resident runs before every statement on every surface. It used to take a land lock and
# read the control plane's node_freshness_state once per table of every source the plan reads —
# for a source the engine reads in place, which never lands, and for a landed source that was
# fresh. Measured on /data/sql: 2.9 ms of a 10.4 ms cached request.


@pytest.mark.asyncio
async def test_a_source_the_engine_reads_live_issues_no_control_plane_statement(wiring):
    backend = _Backend(live={"pg"})
    state = _state([_source("pg")], [_table("pg", "orders"), _table("pg", "customers")], backend)
    for _ in range(3):
        assert await ensure_resident(state, {"pg"}, reader_role=None, table_ids=_read(state)) == []
    assert state.tenant_db.acquires == 0, "a read that lands nothing read the control plane"
    assert backend.calls == []
    from provisa.events import land_lock

    assert land_lock._locks == {}, "a read that lands nothing took a land lock"


@pytest.mark.asyncio
async def test_a_fresh_landed_source_is_decided_from_memory(wiring, monkeypatch):
    _stamping_refresh(monkeypatch)
    backend = _Backend()
    state = _state([_source("pets-db", cache_ttl=300)], [_table("pets-db", "pets")], backend)
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == [
        ("pets-db", "pets")
    ]
    read_and_stamp = state.tenant_db.acquires
    assert read_and_stamp >= 1
    for _ in range(5):
        assert (
            await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
            == []
        )
    assert state.tenant_db.acquires == read_and_stamp, "a fresh landed source re-read its state"
    assert len(backend.calls) == 1


@pytest.mark.asyncio
async def test_a_source_another_process_landed_is_read_once_then_decided_from_memory(wiring):
    backend = _Backend()
    backend.mark_landed("pets-db")
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    state.tenant_db.states["pet_store.pets"] = {
        "last_refresh_at": _recent(),
        "last_refresh_ok": True,
    }
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == []
    assert state.tenant_db.acquires == 1
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == []
    assert state.tenant_db.acquires == 1


@pytest.mark.asyncio
async def test_the_in_memory_state_is_a_bounded_snapshot(wiring, monkeypatch):
    """Another process's land — or its failed land — changes the persisted state. The snapshot is
    re-read once its backstop lifetime is over, so that is seen within it (as the registry
    caches' own backstop does for the registry)."""
    from provisa.federation import node_freshness_view as view

    backend = _Backend()
    backend.mark_landed("pets-db")
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    state.tenant_db.states["pet_store.pets"] = {
        "last_refresh_at": _recent(),
        "last_refresh_ok": True,
    }
    clock = {"now": 1000.0}
    monkeypatch.setattr(view.time, "monotonic", lambda: clock["now"])
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == []
    clock["now"] += view.BACKSTOP_SECONDS / 2
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == []
    assert state.tenant_db.acquires == 1
    # another process's land of this table failed
    state.tenant_db.states["pet_store.pets"] = {
        "last_refresh_at": _recent(),
        "last_refresh_ok": False,
    }
    clock["now"] += view.BACKSTOP_SECONDS
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == [
        ("pets-db", "pets")
    ], "the failed land was not retried once the snapshot expired"


@pytest.mark.asyncio
async def test_a_ttl_outrun_in_memory_goes_back_to_the_control_plane_and_lands(wiring, monkeypatch):
    _stamping_refresh(monkeypatch)
    backend = _Backend()
    backend.mark_landed("pets-db")
    # the table declares no TTL of its own, so it is held to its source's 60 s (REQ-1907)
    state = _state(
        [_source("pets-db", cache_ttl=60)], [_table("pets-db", "pets", cache_ttl=None)], backend
    )
    import time as _time

    fresh_at = _time.time() - 10
    state.tenant_db.states["pet_store.pets"] = {
        "last_refresh_at": fresh_at,
        "last_refresh_ok": True,
    }
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == []
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == []
    assert state.tenant_db.acquires == 1 and backend.calls == [({"pets-db"}, backend.calls[0][1])]
    # the same snapshot, 100 s later: the ttl is outrun, so the truth is read and the table landed
    real_time = _time.time
    monkeypatch.setattr("provisa.federation.query_residency.time.time", lambda: real_time() + 100)
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == [
        ("pets-db", "pets")
    ]
    assert state.tenant_db.recorded == [("pet_store.pets", True)]


@pytest.mark.asyncio
async def test_a_failed_land_is_retried_by_the_next_read(wiring, monkeypatch):
    """REQ-1661: the failed land is stamped not ok in the persisted state AND in memory, so the
    next read does not take the in-memory state for a fresh replica."""
    _stamping_refresh(monkeypatch)
    backend = _Backend(fail=True)
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    with pytest.raises(RuntimeError, match="adapter down"):
        await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
    with pytest.raises(RuntimeError, match="adapter down"):
        await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
    assert len(backend.calls) == 2
    backend.fail = False
    assert await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state)) == [
        ("pets-db", "pets")
    ]


@pytest.mark.asyncio
async def test_a_new_schema_generation_drops_the_in_memory_state(wiring):
    backend = _Backend()
    backend.mark_landed("pets-db")
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    state.schema_boot_id, state.schema_version = "boot", 1
    state.tenant_db.states["pet_store.pets"] = {
        "last_refresh_at": _recent(),
        "last_refresh_ok": True,
    }
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
    assert state.tenant_db.acquires == 1
    state.schema_version = 2  # a rebuild: a landed table may have been recreated
    await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=_read(state))
    assert state.tenant_db.acquires == 2


# -- row_materialize is ignored when the engine attaches the source (SETTLED, REQ-1865) -----------


@pytest.mark.asyncio
async def test_a_row_materialize_flag_is_ignored_when_the_engine_attaches_the_source(
    wiring, monkeypatch
):
    """The flag is the reach for a source the engine CANNOT attach. On an engine that reads the
    source in place, a flagged table the statement names with no key bound is read through the
    attach: no whole-table land into a row cache, no lock, no control-plane statement — on the
    attach: no row-level handling, no lock, no control-plane statement, on every surface."""
    landed_calls: list = []

    async def _land(*args, **kwargs):
        landed_calls.append(args)

    monkeypatch.setattr("provisa.federation.query_residency._land_row_cache", _land)
    backend = _Backend(live={"mongo"})
    state = _state(
        [_source("mongo", type=SimpleNamespace(value="mongodb"))],
        [
            _table(
                "mongo",
                "order_docs",
                schema="provisa_bench",
                row_materialize=True,
                columns=[SimpleNamespace(name="order_id", is_primary_key=True)],
            )
        ],
        backend,
    )
    # the bound engine declares a connector that reads mongodb in place
    state.federation_engine.engine.connectors = {"mongodb": SimpleNamespace(reads_in_place=True)}
    for _ in range(2):
        assert (
            await ensure_resident(state, {"mongo"}, reader_role=None, table_ids=_read(state)) == []
        )
    assert landed_calls == [], "a table the engine attaches was landed into a row cache"
    assert state.tenant_db.acquires == 0 and backend.calls == []


@pytest.mark.asyncio
async def test_the_flag_applies_when_the_engine_cannot_attach_the_source(wiring):
    """The same table on an engine with no connector for its source type: the flag is its reach,
    so it is left to the keyed paths while a sibling table of the source still lands whole."""
    backend = _Backend()
    state = _state(
        [_source("mongo", type=SimpleNamespace(value="mongodb"))],
        [
            _table(
                "mongo",
                "order_docs",
                schema="provisa_bench",
                row_materialize=True,
                columns=[SimpleNamespace(name="order_id", is_primary_key=True)],
            ),
            _table("mongo", "order_tags", schema="provisa_bench"),
        ],
        backend,
    )
    state.federation_engine.engine.connectors = {}
    assert await ensure_resident(state, {"mongo"}, reader_role=None, table_ids=_read(state)) == [
        ("mongo", "order_tags")
    ]
