# Copyright (c) 2026 Kenneth Stott
# Canary: 8f2a6c19-4e7b-4d31-9a05-b3c8e1d74f62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1907: replica freshness is judged per TABLE and per reader — a read asks for a replica's
build only when its age exceeds that reader's effective TTL, max(cache_ttl, role_ttl(role));
concurrent stale reads of one table share one build; the row cache applies the same per-reader age check."""

# Requirements: REQ-1907, REQ-1661, REQ-1865

from __future__ import annotations

import asyncio
import types
from datetime import UTC, datetime, timedelta
import itertools
from types import SimpleNamespace

import pytest

from provisa.compiler.pk_bounds import PkBound
from provisa.core.models import Column, Source, Table
from provisa.federation.query_residency import ensure_resident, ensure_rows_resident, is_stale_of
from provisa.federation.replica_address import ReplicaRoutes
from provisa.federation.replica_routing import table_floor

pytestmark = pytest.mark.unit

NOW = 10_000.0


def _src(sid="s", cache_ttl=None, **kw):
    base = dict(
        id=sid,
        type=SimpleNamespace(value="sqlite"),
        change_signal="ttl",
        cache_ttl=cache_ttl,
        freshness_gate=False,
        replicate=None,
        load_protected=False,
        region=None,  # REQ-1921
    )
    base.update(kw)
    return SimpleNamespace(**base)


_IDS = itertools.count(1)


def _tbl(name, sid="s", cache_ttl=60, role_ttl=None, **kw):
    base = dict(
        id=next(_IDS),
        source_id=sid,
        schema_name="sch",
        table_name=name,
        row_materialize=False,
        columns=[],
        cache_ttl=cache_ttl,
        role_ttl=role_ttl if role_ttl is not None else {"analyst": 360, "trader": 0},
        change_signal=None,
        replicate=None,
        load_protected=None,
        region=None,  # REQ-1921
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _st(age, ok=True):
    return {"last_refresh_at": NOW - age, "last_refresh_ok": ok}


def _check(verdict):
    """A freshness check that always answers ``verdict`` (True fresh / False not / None none)."""
    return lambda source, table, stamp, ok, now: verdict


def _stale(tables, states, role, verdict=None, src=None):
    source = src or _src()
    return is_stale_of(
        [source], {"s": tables}, states, NOW, reader_role=role, fresh_of=_check(verdict)
    )("s")


# --- the gate: effective TTL AND freshness check -------------------------------------------


def test_an_analyst_serves_a_200s_old_replica_a_trader_does_not():
    t, states = _tbl("orders"), {"s/sch.orders": _st(200)}
    assert not _stale([t], states, "analyst")
    assert _stale([t], states, "trader")
    assert _stale([t], states, None)


def test_a_trader_with_role_ttl_0_is_held_to_the_cache_ttl_floor():
    t = _tbl("orders")
    assert not _stale([t], {"s/sch.orders": _st(30)}, "trader")
    assert _stale([t], {"s/sch.orders": _st(61)}, "trader")


def test_check_says_stale_but_ttl_not_passed_does_not_land():
    assert not _stale([_tbl("orders")], {"s/sch.orders": _st(30)}, "trader", verdict=False)


def test_ttl_passed_but_check_says_fresh_does_not_land():
    assert not _stale([_tbl("orders")], {"s/sch.orders": _st(200)}, "trader", verdict=True)


def test_ttl_passed_and_check_says_stale_lands():
    assert _stale([_tbl("orders")], {"s/sch.orders": _st(200)}, "trader", verdict=False)


def test_no_check_lands_on_the_ttl_alone():
    assert _stale([_tbl("orders")], {"s/sch.orders": _st(200)}, "trader", verdict=None)


def _real_stale(tables, states, role, src=None):
    """The oracle with the default freshness check (``freshness_verdict``), not an injected one."""
    return is_stale_of([src or _src()], {"s": tables}, states, NOW, reader_role=role)("s")


@pytest.mark.parametrize("signal", ["native", "debezium", "kafka", "probe"])
def test_a_change_fed_table_is_fresh_for_every_role_role_ttl_cannot_hold_back_the_feed(signal):
    """REQ-1907 (amended 2026-09-30): a pushed-change or probed table is kept current by the
    event loop's change path, so on the read path its replica counts as fresh for every reader --
    role_ttl only limits read-triggered refreshes; it never holds back the change feed, and a
    change-fed table is never re-landed by a read on the clock."""
    t = _tbl("feed", cache_ttl=None, role_ttl={"analyst": 360}, change_signal=signal)
    for role in ("analyst", "trader", None):
        for age in (1, 200, 361, 100_000):
            assert not _real_stale([t], {"s/sch.feed": _st(age)}, role)
    # never-landed / failed still lands, for every reader
    assert _real_stale([t], {"s/sch.feed": None}, "analyst")
    assert _real_stale([t], {"s/sch.feed": _st(1, ok=False)}, "analyst")


@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
def test_a_ttl_signal_table_with_no_table_or_source_cache_ttl_fails_the_read(signal):
    """REQ-1907 (amended 2026-09-30): change_signal ttl / ttl_probe needs a cache_ttl on the
    table or its source; one that slipped past config load and admin save fails the read with an
    error naming the table -- it is never treated as fresh."""
    from provisa.federation.query_residency import freshness_verdict

    on_table = _tbl("orders", cache_ttl=None, role_ttl={"analyst": 360}, change_signal=signal)
    on_source = _tbl("orders", cache_ttl=None, role_ttl={"analyst": 360})
    for t, src in (
        (on_table, _src(change_signal="kafka")),
        (on_source, _src(change_signal=signal)),
    ):
        for states in ({"s/sch.orders": _st(200)}, {"s/sch.orders": None}):
            with pytest.raises(ValueError, match=r"orders.*add a cache_ttl"):
                _real_stale([t], states, "analyst", src=src)
        with pytest.raises(ValueError, match=r"orders.*add a cache_ttl"):
            freshness_verdict(src, t, NOW - 500, True, NOW)
    # a source-level cache_ttl satisfies it
    assert not _real_stale(
        [on_source],
        {"s/sch.orders": _st(200)},
        "analyst",
        src=_src(change_signal=signal, cache_ttl=3600),
    )


def test_staleness_is_judged_per_table_against_that_tables_own_ttl():
    src = _src(cache_ttl=3600)
    slow = _tbl("slow", cache_ttl=None, role_ttl={})  # inherits the source's 3600
    fast = _tbl("fast", cache_ttl=10, role_ttl={})
    states = {"s/sch.slow": _st(100), "s/sch.fast": _st(5)}
    assert not _stale([slow, fast], states, None, src=src)
    states["s/sch.fast"] = _st(11)
    assert _stale([slow, fast], states, None, src=src)


def test_never_landed_or_failed_is_stale_for_every_reader_whatever_the_check():
    t = _tbl("orders")
    assert _stale([t], {"s/sch.orders": None}, "analyst", verdict=True)
    assert _stale([t], {"s/sch.orders": _st(1, ok=False)}, "analyst", verdict=True)


def test_push_and_probe_tables_are_kept_fresh_by_their_change_path():
    from provisa.federation.query_residency import freshness_verdict

    for signal in ("debezium", "kafka", "native", "probe", "ttl_probe"):
        t = _tbl("orders", change_signal=signal)
        assert freshness_verdict(_src(), t, NOW - 500, True, NOW) is True
    assert freshness_verdict(_src(), _tbl("orders"), NOW - 500, True, NOW) is None
    # a table inherits its source's signal
    assert freshness_verdict(_src(change_signal="kafka"), _tbl("orders"), NOW, True, NOW) is True


def test_a_freshness_gated_source_is_judged_by_its_own_predicate():
    from provisa.federation.query_residency import freshness_verdict

    gated = _src(freshness_gate=True, change_signal="ttl", cache_ttl=100)
    assert freshness_verdict(gated, _tbl("orders"), NOW - 50, True, NOW) is True
    assert freshness_verdict(gated, _tbl("orders"), NOW - 150, True, NOW) is False


# --- ensure_resident: per-reader land decision, single-flight, attach, protected ---------


class _Db:
    """The state store as these tests need it: each table's replica state by node
    (``{last_refresh_at, last_refresh_ok}``, None: never built), the builds a read has asked
    for, and how many records were read."""

    def __init__(self, states):
        self.states = states
        self.reads = 0
        self.requested: set[str] = set()
        self.building: set[str] = set()

    def acquire(self):
        db = self

        class _Ctx:
            async def __aenter__(self):
                return db

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class _Backend:
    dialect = "postgres"

    def __init__(self):
        self.lands = 0
        self.plans = 0
        self.attaches = False  # set by _state: the engine reads the source in place

    def pending_lands(
        self, sources, *, is_stale, replicated_of, load_protected_of, resident_of, **kw
    ):
        """As EngineBackend.pending_lands (plan.build_execution_plan): a source the engine reads
        in place lands only when a setting puts its read on the replica; a load-protected source
        preps only when not resident; any other lands when its staleness oracle says so."""
        del kw
        due = []
        for s in sources:
            protected = load_protected_of(s.id)
            if self.attaches and not (protected or replicated_of(s.id)):
                continue
            if protected:
                if resident_of is None or not resident_of(s.id):
                    due.append(s.id)
            elif is_stale(s.id):
                due.append(s.id)
        return due

    def replica_address(self, state, *, source_id, schema_name, table_name):
        """As EngineBackend.replica_address: the replicas schema, under the one replica name."""
        from provisa.federation.replica_address import ReplicaAddress, replica_table_name

        del state
        return ReplicaAddress(
            "org_test_replicas", replica_table_name(source_id, schema_name, table_name)
        )


def _state(tables, backend, states, *, source=None, attaches=False):
    from provisa.federation.connector import Mechanism

    attach = SimpleNamespace(reads_in_place=True, reach_modes=frozenset({Mechanism.ATTACH_R}))
    connectors = {"sqlite": attach} if attaches else {}
    backend.attaches = attaches
    engine = SimpleNamespace(
        engine=SimpleNamespace(
            backend=backend,
            name="postgres",
            native_store="postgres",
            replica_store_backend=lambda: "postgres",  # as FederationEngine: its native store
            connectors=connectors,
            materialize_store=lambda: "postgresql://localhost/materialize",
        )
    )
    src = source or _src()
    floored = {}
    for t in tables:
        setting = table_floor(src, t, promoted=False)
        if setting is not None:
            floored[t.id] = (src.id, setting)
    return SimpleNamespace(
        federation_engine=engine,
        config=SimpleNamespace(sources=[src], tables=tables),
        model_db=(_one_db := _Db(states)),
        tenant_db=_one_db,
        # as the schema build publishes it: the tables the operator's settings put on a replica
        replica_routes=ReplicaRoutes(floored=floored),
    )


def _read(state) -> frozenset[int]:
    """A statement that reads every registered table of the fixture."""
    return frozenset(t.id for t in state.config.tables)


@pytest.fixture
def wiring(monkeypatch):
    """The read backstop's collaborators: a fixed clock, the registry, the replica records read
    from ``_Db.states``, and a stand-in for the build runner that builds what a read asked for."""
    import time as _time

    from provisa.federation import replica_state

    monkeypatch.setattr(_time, "time", lambda: NOW)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", lambda state: "store")
    monkeypatch.setattr("provisa.federation.query_residency._BUILD_POLL_S", 0.01)
    monkeypatch.setattr(
        "provisa.core.settings_registry.value",
        lambda key: {"replication.retry_interval": 60}[key],
    )
    seen: dict = {}

    def _node(key) -> str:
        from provisa.events.nodes import source_node

        return source_node(*key)

    async def read(conn, key):
        seen["db"] = conn
        conn.reads += 1
        held = conn.states.get(_node(key))
        building = _node(key) in conn.requested | conn.building
        if held is None and not building:
            return None
        at = held["last_refresh_at"] if held else None
        failed = held is not None and not held.get("last_refresh_ok", True)
        return SimpleNamespace(
            build_state="requested" if building else "failed" if failed else "idle",
            completed_at=datetime.fromtimestamp(at, UTC) if at is not None else None,
            last_error=None,
            exists_in=lambda store: at is not None,
        )

    async def request_build(conn, key, reason, **kw):
        del reason, kw
        seen["backend"].plans += 1
        conn.requested.add(_node(key))
        return True

    async def _build() -> None:
        db, backend = seen["db"], seen["backend"]
        # As the runner: a requested build is claimed by one pass, so two kicks build it once.
        claimed, db.requested = sorted(db.requested), set()
        db.building.update(claimed)
        for node in claimed:
            backend.lands += 1
            await asyncio.sleep(0.05)  # a real build takes time; a second reader waits on it
            db.states[node] = {"last_refresh_at": _time.time(), "last_refresh_ok": True}
            db.building.discard(node)

    def kick(org_id) -> None:
        del org_id
        asyncio.get_running_loop().create_task(_build())

    monkeypatch.setattr(replica_state, "read", read)
    monkeypatch.setattr(replica_state, "request_build", request_build)
    monkeypatch.setattr("provisa.federation.replica_builds.kick", kick)

    async def _sources(state, conn=None):
        seen["backend"] = state.federation_engine.engine.backend
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)


@pytest.mark.asyncio
async def test_an_analyst_read_of_a_200s_old_replica_does_not_land(wiring):
    backend = _Backend()
    state = _state([_tbl("orders")], backend, {"s/sch.orders": _st(200)})
    assert (
        await ensure_resident(state, {"s"}, reader_role="analyst", table_ids=_read(state))
    ).built == []
    assert backend.lands == 0


@pytest.mark.asyncio
async def test_a_trader_read_lands_only_past_the_cache_ttl_floor(wiring, monkeypatch):
    backend = _Backend()
    state = _state([_tbl("orders")], backend, {"s/sch.orders": _st(30)})
    assert (
        await ensure_resident(state, {"s"}, reader_role="trader", table_ids=_read(state))
    ).built == []
    # 170 s later the same replica is 200 s old: past the cache_ttl floor the trader is held to.
    # The clock moves, not the persisted stamp: the freshness state is decided from memory first
    # (REQ-1661 amended 2026-10-01), and a stamp ages there exactly as it does in the control plane.
    import time as _time

    monkeypatch.setattr(_time, "time", lambda: NOW + 170)
    assert (
        await ensure_resident(state, {"s"}, reader_role="trader", table_ids=_read(state))
    ).built == [("s", "orders")]
    assert backend.lands == 1


@pytest.mark.asyncio
async def test_two_concurrent_trader_reads_share_one_land(wiring):
    backend = _Backend()
    state = _state([_tbl("orders")], backend, {"s/sch.orders": _st(200)})
    first, second = await asyncio.gather(
        ensure_resident(state, {"s"}, reader_role="trader", table_ids=_read(state)),
        ensure_resident(state, {"s"}, reader_role="trader", table_ids=_read(state)),
    )
    assert backend.lands == 1 and backend.plans == 1  # one request, one build
    # both readers waited for that one build
    assert first.built == second.built == [("s", "orders")]


@pytest.mark.asyncio
async def test_a_direct_attached_table_is_read_live_with_no_staleness_evaluation(wiring):
    """REQ-1907 (direct attach is live): role_ttl and cache_ttl are set, the replica is stale for
    a trader -- but the engine attaches the source, so nothing is evaluated and nothing lands."""
    backend = _Backend()
    state = _state([_tbl("orders")], backend, {"s/sch.orders": _st(10_000)}, attaches=True)
    assert (
        await ensure_resident(state, {"s"}, reader_role="trader", table_ids=_read(state))
    ).built == []
    assert backend.plans == 0 and backend.lands == 0
    assert state.tenant_db.reads == 0


@pytest.mark.asyncio
async def test_attach_capable_but_replicate_goes_through_the_replica_gate(wiring):
    backend = _Backend()
    t = _tbl("orders", replicate=0)
    state = _state([t], backend, {"s/sch.orders": _st(200)}, attaches=True)
    assert (
        await ensure_resident(state, {"s"}, reader_role="analyst", table_ids=_read(state))
    ).built == []  # 200 < 360
    assert (
        await ensure_resident(state, {"s"}, reader_role="trader", table_ids=_read(state))
    ).built == [("s", "orders")]
    assert backend.lands == 1


@pytest.mark.asyncio
async def test_a_read_of_a_ttl_table_with_no_cache_ttl_fails_before_any_land(wiring):
    """REQ-1907 (amended 2026-09-30): the read fails with a clear error naming the table; it is
    rejected before the land plan runs, so no land is attempted and no node is stamped."""
    backend = _Backend()
    t = _tbl("orders", cache_ttl=None, role_ttl={"analyst": 360})
    state = _state([t], backend, {"s/sch.orders": _st(200)})
    with pytest.raises(ValueError, match=r"orders.*add a cache_ttl"):
        await ensure_resident(state, {"s"}, reader_role="analyst", table_ids=_read(state))
    assert backend.plans == 0 and backend.lands == 0
    assert state.tenant_db.states == {"s/sch.orders": _st(200)}


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["ttl", "ttl_probe"])
async def test_a_directly_attached_ttl_table_with_no_cache_ttl_reads_live(wiring, signal):
    """REQ-1907 (option B): the table never lands -- the engine reads it in place -- so it needs
    no landing clock and the read succeeds with nothing evaluated."""
    backend = _Backend()
    t = _tbl("orders", cache_ttl=None, change_signal=signal)
    state = _state([t], backend, {"s/sch.orders": _st(10_000)}, attaches=True)
    assert (
        await ensure_resident(state, {"s"}, reader_role="analyst", table_ids=_read(state))
    ).built == []
    assert backend.plans == 0 and backend.lands == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["probe", "native", "debezium", "kafka"])
async def test_a_landed_no_ttl_freshness_signal_table_reads_without_a_cache_ttl(wiring, signal):
    backend = _Backend()
    t = _tbl("orders", cache_ttl=None, change_signal=signal)
    state = _state([t], backend, {"s/sch.orders": _st(10_000)})
    assert (
        await ensure_resident(state, {"s"}, reader_role="analyst", table_ids=_read(state))
    ).built == []
    assert backend.lands == 0


def test_a_freshness_gated_probe_source_with_no_cache_ttl_is_judged_by_its_gate():
    from provisa.federation.query_residency import freshness_verdict

    """No error: the gate's own predicate decides (REQ-860), held back only by the reader's TTL."""
    gated = _src(freshness_gate=True, change_signal="probe", cache_ttl=None)
    t = _tbl("orders", cache_ttl=None, role_ttl={"analyst": 360})
    # no probe verdict has been recorded, so the gate reports the replica not fresh
    assert freshness_verdict(gated, t, NOW - 500, True, NOW) is False
    assert not _real_stale([t], {"s/sch.orders": _st(1)}, "analyst", src=gated)
    assert _real_stale([t], {"s/sch.orders": _st(400)}, "analyst", src=gated)


@pytest.mark.asyncio
async def test_a_load_protected_table_is_never_landed_by_a_read_even_for_ttl_0(wiring):
    """REQ-1141: once resident, only the scheduler refreshes a load-protected table; every
    reader, a TTL-0 trader included, gets the scheduled snapshot."""
    backend = _Backend()
    t = _tbl("orders", load_protected=True, role_ttl={"trader": 0})
    state = _state([t], backend, {"s/sch.orders": _st(10_000)}, attaches=True)
    assert (
        await ensure_resident(state, {"s"}, reader_role="trader", table_ids=_read(state))
    ).built == []
    assert backend.lands == 0


# --- the row cache: per-reader age against the landed-at stamp ----------------------------


def _rm_table(role_ttl=None) -> Table:
    return Table(
        source_id="pg1",
        domain_id="dom1",
        schema_name="public",
        table_name="orders",
        columns=[
            Column(name="id", visible_to=["public"], data_type="integer", is_primary_key=True),
            Column(name="status", visible_to=["public"], data_type="text"),
        ],
        row_materialize=True,
        cache_ttl=60,
        role_ttl=role_ttl if role_ttl is not None else {"analyst": 360, "trader": 0},
    )


class _RowBackend:
    dialect = "postgresql"

    def replica_address(self, state, *, source_id, schema_name, table_name):
        """As EngineBackend.replica_address: the one replica name. The schema is the test store's
        own (a SQLite file stands in for the store here, and ``main`` is the schema it has): what
        is under test is the row cache, not where the replicas schema lives."""
        from provisa.federation.replica_address import ReplicaAddress, replica_table_name

        del state
        return ReplicaAddress("main", replica_table_name(source_id, schema_name, table_name))


class _Loader:
    def __init__(self):
        self.calls: list[list[tuple]] = []

    async def load_keys(self, source, table, pk_columns, keys, *, admit=None):
        self.calls.append(list(keys))
        return [{"id": k[0], "status": "new"} for k in keys]


@pytest.fixture
def row_env(monkeypatch, tmp_path):
    ns = types.SimpleNamespace(table=_rm_table(), loader=_Loader())
    source = Source(id="pg1", type="postgresql", cache_ttl=None)

    async def _sources(state):
        return [source]

    async def _tables(state):
        return [ns.table]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr(
        "provisa.events.source_loader.SourceRowLoader",
        lambda engine, adapter_loaders=None, keyed_adapter_loaders=None: ns.loader,
    )
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda s, e: {})
    monkeypatch.setattr(
        "provisa.events.app_wiring.build_keyed_adapter_loaders", lambda s, e=None: {}
    )
    dsn = f"sqlite:///{tmp_path}/row_cache.db"
    ns.dsn = dsn
    ns.state = types.SimpleNamespace(
        federation_engine=types.SimpleNamespace(
            engine=types.SimpleNamespace(
                backend=_RowBackend(), dialect="postgresql", materialize_store=lambda: dsn
            )
        )
    )
    return ns


_BOUND = PkBound(
    source_id="pg1", schema_name="public", table_name="orders", pk_columns=("id",), values=((1,),)
)


async def _backdate(dsn: str, seconds: float) -> None:
    from sqlalchemy import text

    from provisa.federation import store_writer

    stamp = datetime.now(UTC) - timedelta(seconds=seconds)
    async with store_writer.store_connection(dsn) as conn:
        await conn.execute_core(
            text('UPDATE "pg1__public__orders" SET _row_cached_at = :at').bindparams(at=stamp)
        )


@pytest.mark.asyncio
async def test_row_cache_serves_a_200s_old_row_to_an_analyst_and_refetches_for_a_trader(row_env):
    await ensure_rows_resident(row_env.state, [_BOUND], reader_role="trader")
    assert row_env.loader.calls == [[(1,)]]
    await _backdate(row_env.dsn, 200)

    assert await ensure_rows_resident(row_env.state, [_BOUND], reader_role="analyst") == [
        ("pg1", "orders", 0)
    ]
    assert row_env.loader.calls == [[(1,)]]

    assert await ensure_rows_resident(row_env.state, [_BOUND], reader_role="trader") == [
        ("pg1", "orders", 1)
    ]
    assert row_env.loader.calls == [[(1,)], [(1,)]]


@pytest.mark.asyncio
async def test_row_cache_trader_is_held_to_the_cache_ttl_floor(row_env):
    await ensure_rows_resident(row_env.state, [_BOUND], reader_role="trader")
    await _backdate(row_env.dsn, 30)
    assert await ensure_rows_resident(row_env.state, [_BOUND], reader_role="trader") == [
        ("pg1", "orders", 0)
    ]
    assert row_env.loader.calls == [[(1,)]]


@pytest.mark.asyncio
async def test_row_cache_expiry_stamp_covers_the_longest_role_ttl_for_the_reaper(row_env):
    """``_row_expires_at`` is the reaper's horizon: the landed-at stamp plus the longest TTL any
    reader accepts, so a cold-row sweep never deletes a row a long-TTL class still serves."""
    from sqlalchemy import text

    from provisa.federation import store_writer

    await ensure_rows_resident(row_env.state, [_BOUND], reader_role="trader")
    async with store_writer.store_connection(row_env.dsn) as conn:
        row = (
            await conn.execute_core(
                text('SELECT _row_cached_at, _row_expires_at FROM "pg1__public__orders"')
            )
        ).fetchone()
    cached_at, expires_at = row
    if isinstance(cached_at, str):
        cached_at, expires_at = (
            datetime.fromisoformat(cached_at),
            datetime.fromisoformat(expires_at),
        )
    assert (expires_at - cached_at).total_seconds() == pytest.approx(360, abs=1)


@pytest.mark.asyncio
async def test_a_pushed_change_row_is_kept_current_by_cdc_not_refetched_on_the_clock(row_env):
    """A row of a pushed-change table is refreshed by the CDC background path (REQ-1865 section 5),
    which is its freshness check: a read never re-fetches it on the TTL, even for a TTL-0 role."""
    row_env.table = row_env.table.model_copy(update={"change_signal": "debezium"})
    await ensure_rows_resident(row_env.state, [_BOUND], reader_role="trader")
    await _backdate(row_env.dsn, 10_000)
    assert await ensure_rows_resident(row_env.state, [_BOUND], reader_role="trader") == [
        ("pg1", "orders", 0)
    ]
    assert row_env.loader.calls == [[(1,)]]
