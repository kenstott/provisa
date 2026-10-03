# Copyright (c) 2026 Kenneth Stott
# Canary: 2e8b6d41-7c3f-4a95-b1d6-9f0e4a27c5b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1661, REQ-1915: the read backstop. A statement that reads a table from a whole-table
replica has that replica built and fresh for its reader before it reads: the build is asked for
in the state store and awaited on its record; a read never copies a table itself. A replica
this process's copy says is fresh costs no control-plane read."""

from __future__ import annotations

import asyncio
import itertools
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import metadata, replica_state as replica_state_table
from provisa.federation import replica_state
from provisa.federation.query_residency import ensure_resident, is_stale_of
from provisa.federation.replica_address import ReplicaRoutes
from provisa.federation.replica_routing import table_floor
from provisa.federation.replica_state import ReplicaBuildFailed, ReplicaBuilding

pytestmark = pytest.mark.unit

STORE = "store-a"


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


def test_is_stale_of_honours_never_landed_failure_and_ttl():
    # REQ-1907 (amended 2026-09-30): a ttl-signal source needs a cache_ttl -- none is an error.
    sources = [_source(sid, cache_ttl=60) for sid in ("fresh", "ttl", "bad", "never")]
    tables = {sid: [_table(sid, sid, cache_ttl=None)] for sid in ("fresh", "ttl", "bad", "never")}
    # Keyed by each table's event-graph node: its registered identity (events.nodes.source_node).
    states = {
        "fresh/pet_store.fresh": {"last_refresh_at": 1000.0, "last_refresh_ok": True},
        "ttl/pet_store.ttl": {"last_refresh_at": 900.0, "last_refresh_ok": True},
        "bad/pet_store.bad": {"last_refresh_at": 1000.0, "last_refresh_ok": False},
        "never/pet_store.never": None,
    }
    is_stale = is_stale_of(sources, tables, states, 1000.0, reader_role=None)
    assert not is_stale("fresh")
    assert is_stale("ttl")  # 100s old against a 60s ttl
    assert is_stale("bad")
    assert is_stale("never")


def _attach():
    """A connector that reads its source type in place, as a live attach."""
    from provisa.federation.connector import Mechanism

    return SimpleNamespace(reads_in_place=True, reach_modes=frozenset({Mechanism.ATTACH_R}))


class _Db:
    """The state store, counting control-plane round trips."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self.acquires = 0

    def acquire(self):
        self.acquires += 1
        return self._db.acquire()


class _Backend:
    def __init__(self, live=()):
        self.dialect = "postgres"
        self.live = set(live)  # sources this engine reads in place: never served from a replica
        self.replicated: dict[str, bool] = {}  # what the last plan was told, per source

    def pending_lands(self, sources, *, is_stale, **kw):
        """As EngineBackend.pending_lands: the sources a read must have a replica built for. A
        source the engine reads live never is; a replica-served one is when its staleness oracle
        says so."""
        self.replicated = {s.id: kw["replicated_of"](s.id) for s in sources}
        return [s.id for s in sources if s.id not in self.live and is_stale(s.id)]


class _Runner:
    """Stands in for the build runner: when kicked it completes (or fails) every requested
    build, as another task, and counts what it built."""

    def __init__(self, db: Database, *, fail: str | None = None, hold: bool = False) -> None:
        self._db = db
        self.fail = fail
        self.hold = hold  # never finishes a build: the request's deadline passes first
        self.built: list[replica_state.ReplicaKey] = []
        self.kicks = 0

    def kick(self, org_id) -> None:
        del org_id
        self.kicks += 1
        if not self.hold:
            asyncio.get_running_loop().create_task(self.run())

    async def run(self) -> None:
        now = datetime.now(UTC)
        async with self._db.acquire() as conn:
            for key in await replica_state.candidates(conn, retry_interval=60, now=now, limit=50):
                if not await replica_state.claim(
                    conn, key, holder="test:1", retry_interval=60, now=now
                ):
                    continue
                if self.fail is not None:
                    await replica_state.record_failed(conn, key, error=self.fail, now=now)
                    continue
                await replica_state.record_completed(
                    conn,
                    key,
                    rows_copied=1,
                    method="stream_batches",
                    content_hash=None,
                    store=STORE,
                    next_refresh_at=None,
                    now=datetime.now(UTC),
                )
                self.built.append(key)


@pytest.fixture
def plane(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    with engine.begin() as conn:
        metadata.create_all(conn, tables=[replica_state_table])
    yield Database(engine, "test")
    engine.dispose()


def _state(sources, tables, backend, plane):
    engine = SimpleNamespace(
        engine=SimpleNamespace(
            backend=backend,
            name="snowflake",
            native_store="snowflake",
            connectors={},  # no connector reads any source in place, unless a test declares one
            replica_store_backend=lambda: "snowflake",  # as FederationEngine: its native store
            materialize_store=lambda: "postgresql://localhost/materialize",
        )
    )
    return SimpleNamespace(
        federation_engine=engine,
        config=SimpleNamespace(sources=sources, tables=tables),
        model_db=(_one_db := _Db(plane)),
        tenant_db=_one_db,
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
def wiring(monkeypatch, plane):
    """The registry (here the config IS the registry: these tests are about residency, not
    about where the rows come from), the store's identity, and a runner that builds on a kick."""

    async def _sources(state, conn=None):
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", lambda state: STORE)
    monkeypatch.setattr("provisa.federation.query_residency._BUILD_POLL_S", 0.01)
    runner = _Runner(plane)
    monkeypatch.setattr("provisa.federation.replica_builds.kick", runner.kick)
    return runner


def _key(table) -> replica_state.ReplicaKey:
    return (table.source_id, table.schema_name, table.table_name)


async def _built(plane, table, *, at: datetime | None = None, store: str = STORE) -> None:
    """Record ``table``'s replica as built at ``at`` (now by default) in ``store``."""
    when = at if at is not None else datetime.now(UTC)
    async with plane.acquire() as conn:
        await replica_state.request_build(conn, _key(table), "model", now=when)
        await replica_state.claim(conn, _key(table), holder="test:1", retry_interval=60, now=when)
        await replica_state.record_completed(
            conn,
            _key(table),
            rows_copied=1,
            method="stream_batches",
            content_hash=None,
            store=store,
            next_refresh_at=None,
            now=when,
        )


async def _record(plane, table):
    async with plane.acquire() as conn:
        return await replica_state.read(conn, _key(table))


async def _residency(state, sources, tables=None, **kw):
    return await ensure_resident(
        state,
        sources,
        reader_role=kw.get("reader_role"),
        table_ids=_read(state) if tables is None else tables,
    )


async def _ensure(state, sources, tables=None, **kw):
    """The builds the read waited for."""
    return (await _residency(state, sources, tables, **kw)).built


@pytest.mark.asyncio
async def test_a_replica_never_built_is_requested_awaited_and_then_read(wiring, plane):
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    assert await _ensure(state, {"pets-db"}) == [("pets-db", "pets")]
    assert wiring.built == [_key(pets)] and wiring.kicks == 1
    record = await _record(plane, pets)
    assert record.requested_reason == "read" and record.exists_in(STORE)


@pytest.mark.asyncio
async def test_row_materialize_table_is_never_built_whole(wiring, plane):
    """REQ-1865: a row_materialize table's residency is governed exclusively by the keyed paths
    (ensure_rows_resident, called alongside this function) — it is never also built whole here."""
    order = _table("bench-neo4j", "bench_order_node", schema="neo4j", row_materialize=True)
    edge = _table("bench-neo4j", "bench_placed_edge", schema="neo4j")
    state = _state([_source("bench-neo4j")], [order, edge], _Backend(), plane)
    assert await _ensure(state, {"bench-neo4j"}) == [("bench-neo4j", "bench_placed_edge")]
    assert await _record(plane, order) is None


@pytest.mark.asyncio
async def test_a_table_with_a_parameter_column_is_never_built_whole(wiring, plane):
    """A table with a parameter column is a function of its arguments: there is no whole to
    copy. A statement that reaches it asks for no build — the runner would call the endpoint
    with no arguments — while its plain sibling is built as ever."""
    by_id = _table(
        "api",
        "get_pet_by_id",
        columns=[
            SimpleNamespace(name="id", native_filter_type=None),
            SimpleNamespace(name="pet_id", native_filter_type="path_param"),
        ],
    )
    pets = _table("api", "list_pets", columns=[SimpleNamespace(name="id", native_filter_type=None)])
    state = _state([_source("api", cache_ttl=60)], [by_id, pets], _Backend(), plane)
    built = await _ensure(state, ["api"])
    assert built == [("api", "list_pets")] and wiring.built == [_key(pets)]
    assert await _record(plane, by_id) is None


async def test_a_source_whose_tables_are_all_row_level_costs_nothing(wiring, plane):
    """REQ-1915: a table replicated row by row is read by key only, so there is no whole-table
    path for it: nothing is requested and no control-plane statement is issued, and the function
    does not take the statement's table names."""
    order = _table(
        "bench-neo4j",
        "bench_order_node",
        schema="neo4j",
        row_materialize=True,
        columns=[SimpleNamespace(name="order_id", is_primary_key=True, native_filter_type=None)],
    )
    state = _state([_source("bench-neo4j")], [order], _Backend(), plane)
    assert await _ensure(state, {"bench-neo4j"}) == []
    assert state.tenant_db.acquires == 0 and wiring.kicks == 0
    with pytest.raises(TypeError):
        await ensure_resident(
            state, {"bench-neo4j"}, unbound_targets={"bench_order_node"}, reader_role=None
        )  # type: ignore[call-arg]


@pytest.mark.asyncio
async def test_a_fresh_replica_is_left_alone(wiring, plane):
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    await _built(plane, pets)
    assert await _ensure(state, {"pets-db"}) == []
    assert wiring.kicks == 0 and (await _record(plane, pets)).build_state == "idle"


@pytest.mark.asyncio
async def test_only_the_sources_the_plan_names_are_considered(wiring, plane):
    state = _state(
        [_source("a"), _source("b")], [_table("a", "pets"), _table("b", "vets")], _Backend(), plane
    )
    assert await _ensure(state, {"b"}) == [("b", "vets")]
    assert await _ensure(state, set()) == []
    assert await _ensure(state, {"unknown"}) == []


@pytest.mark.asyncio
async def test_only_the_tables_the_statement_reads_are_judged_and_built(wiring, plane):
    """REQ-826: a statement that reads one table of a source neither builds nor waits on the
    source's other tables."""
    pets, vets = _table("pets-db", "pets"), _table("pets-db", "vets")
    state = _state([_source("pets-db")], [pets, vets], _Backend(), plane)
    await _ensure(state, {"pets-db"}, {pets.id})
    assert wiring.built == [_key(pets)] and await _record(plane, vets) is None


@pytest.mark.asyncio
async def test_a_setting_on_a_table_the_statement_does_not_read_does_not_move_its_read(
    wiring, plane
):
    """REQ-826: ``vets`` is set to always; a statement that reads only ``pets`` is not put on a
    replica by it, and one that reads ``vets`` is."""
    backend = _Backend(live={"pets-db"})
    pets, vets = _table("pets-db", "pets"), _table("pets-db", "vets")
    vets.replicate = 0
    state = _state([_source("pets-db")], [pets, vets], backend, plane)
    await _ensure(state, {"pets-db"}, {pets.id})
    assert backend.replicated == {"pets-db": False}
    await _ensure(state, {"pets-db"}, {vets.id})
    assert backend.replicated == {"pets-db": True}


@pytest.mark.asyncio
async def test_a_live_table_read_beside_its_replica_served_sibling_is_left_alone(wiring, plane):
    """REQ-826, per table: the engine reads the source in place, ``vets`` is set to always and
    ``pets`` is not. One statement reads both: only ``vets`` is built. ``pets`` is read live
    through the engine's attach, so it is neither built nor asked for a replication clock (it
    has none here)."""
    backend = _Backend()
    pets = _table("pets-db", "pets", cache_ttl=None)
    vets = _table("pets-db", "vets")
    vets.replicate = 0
    state = _state([_source("pets-db")], [pets, vets], backend, plane)
    # this engine reads the source's type in place
    state.federation_engine.engine.connectors = {"sqlite": _attach()}
    await _ensure(state, {"pets-db"}, {pets.id, vets.id})
    assert backend.replicated == {"pets-db": True}
    assert wiring.built == [_key(vets)] and await _record(plane, pets) is None
    # and a statement that reads only the live table asks the plan nothing at all
    backend.replicated = {}
    await _ensure(state, {"pets-db"}, {pets.id})
    assert backend.replicated == {} and wiring.built == [_key(vets)]


@pytest.mark.asyncio
async def test_a_failed_build_fails_the_read_with_the_builds_own_cause(wiring, plane):
    """REQ-1661 (amended 2026-09-30): a failed build fails the query — it never reads what the
    failed build left. The failure is on the replica's record, so the next read sees it."""
    wiring.fail = "adapter down"
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    with pytest.raises(ReplicaBuildFailed, match="pets-db.pet_store.pets.*adapter down"):
        await _ensure(state, {"pets-db"})
    assert (await _record(plane, pets)).build_state == "failed"


@pytest.mark.asyncio
async def test_a_failed_build_is_not_asked_for_again_within_the_retry_interval(
    wiring, plane, monkeypatch
):
    """replication.retry_interval: a read that finds a build failed less than that long ago
    fails with the recorded cause and asks for nothing; after it, the next read asks again."""
    wiring.fail = "adapter down"
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    with pytest.raises(ReplicaBuildFailed):
        await _ensure(state, {"pets-db"})
    wiring.fail = None  # the source is back, but the interval has not passed
    with pytest.raises(ReplicaBuildFailed, match="adapter down"):
        await _ensure(state, {"pets-db"})
    assert wiring.kicks == 1 and wiring.built == []
    monkeypatch.setattr(
        "provisa.core.settings_registry.value",
        lambda key: 0 if key == "replication.retry_interval" else None,
    )
    assert await _ensure(state, {"pets-db"}) == [("pets-db", "pets")]
    assert wiring.built == [_key(pets)]


@pytest.mark.asyncio
async def test_a_read_whose_deadline_passes_while_the_build_runs_says_so(wiring, plane):
    from provisa.core import request_deadline

    wiring.hold = True
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    # The build is held, so the deadline passes while the read waits on it, whatever the budget;
    # the budget must only outlast making the request (a control-plane write), which under a
    # loaded machine takes more than a few tens of milliseconds.
    with request_deadline.within(1.0):
        with pytest.raises(ReplicaBuilding, match="pets-db.pet_store.pets is still being built"):
            await _ensure(state, {"pets-db"})
    # the request stands: the build goes on without the reader
    assert (await _record(plane, pets)).build_state == "requested"


@pytest.mark.asyncio
async def test_without_an_engine_or_store_nothing_happens():
    state = SimpleNamespace(federation_engine=None, config=None, model_db=None, tenant_db=None)
    assert (
        await ensure_resident(state, {"pets-db"}, reader_role=None, table_ids=set())
    ).built == []


@pytest.mark.asyncio
async def test_a_burst_of_reads_of_one_stale_table_makes_one_request_and_one_build(
    wiring, plane, monkeypatch
):
    """Single-flight per replica per process: every reader waits on the same record."""
    requests: list = []
    real = replica_state.request_build

    async def counting(conn, key, reason, **kw):
        made = await real(conn, key, reason, **kw)
        requests.append((key, made))
        return made

    monkeypatch.setattr("provisa.federation.replica_state.request_build", counting)
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    results = await asyncio.gather(*[_ensure(state, {"pets-db"}) for _ in range(8)])
    assert wiring.built == [_key(pets)]
    assert [made for _key_, made in requests].count(True) == 1
    # one reader re-read the record and asked; the rest found its answer in the process's copy
    assert len(requests) == 1
    assert all(r in ([("pets-db", "pets")], []) for r in results)


@pytest.mark.asyncio
async def test_a_source_the_engine_reads_live_issues_no_control_plane_statement(wiring, plane):
    state = _state(
        [_source("live-db")], [_table("live-db", "pets")], _Backend(live={"live-db"}), plane
    )
    for _ in range(3):
        assert await _ensure(state, {"live-db"}) == []
    assert state.tenant_db.acquires == 0 and wiring.kicks == 0


@pytest.mark.asyncio
async def test_a_fresh_replica_is_decided_from_this_processs_copy(wiring, plane):
    """REQ-1661 (amended 2026-10-01): the first read of a replica reads its record; while the
    copy says it is fresh for the reader, later reads issue no control-plane statement."""
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    await _built(plane, pets)
    assert await _ensure(state, {"pets-db"}) == []
    after_first = state.tenant_db.acquires
    assert after_first == 1
    for _ in range(5):
        assert await _ensure(state, {"pets-db"}) == []
    assert state.tenant_db.acquires == after_first


@pytest.mark.asyncio
async def test_a_copy_that_says_stale_is_checked_against_the_record_before_a_build_is_asked(
    wiring, plane
):
    """The copy can only be older than the truth: a replica it says is stale may have been
    rebuilt by another node since. One record is re-read; fresh there, nothing is requested."""
    pets = _table("pets-db", "pets", cache_ttl=60)
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    await _built(plane, pets, at=datetime.now(UTC) - timedelta(seconds=50))
    assert await _ensure(state, {"pets-db"}) == []  # the copy now holds a 50 s old build
    # another node rebuilds it; this process's copy still holds the old completion
    await _built(plane, pets, at=datetime.now(UTC))
    import time

    real_time = time.time
    try:
        time.time = lambda: real_time() + 30  # the copy's build is now 80 s old: stale by it
        before = state.tenant_db.acquires
        assert await _ensure(state, {"pets-db"}) == []
        assert state.tenant_db.acquires == before + 1  # one re-read
    finally:
        time.time = real_time
    assert wiring.kicks == 0 and (await _record(plane, pets)).build_state == "idle"


@pytest.mark.asyncio
async def test_a_ttl_outrun_asks_for_a_rebuild_and_waits_for_it(wiring, plane):
    pets = _table("pets-db", "pets", cache_ttl=60)
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    await _built(plane, pets, at=datetime.now(UTC) - timedelta(seconds=600))
    assert await _ensure(state, {"pets-db"}) == [("pets-db", "pets")]
    assert wiring.built == [_key(pets)]
    assert await _ensure(state, {"pets-db"}) == []  # fresh again, from the copy


@pytest.mark.asyncio
async def test_a_replica_built_in_another_store_is_one_never_built_here(wiring, plane):
    """REQ-1730: the record is per table, not per engine. A deployment moved to another engine
    or store finds the old record and no table: the read asks for a build in this store."""
    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _Backend(), plane)
    await _built(plane, pets, store="some-other-store")
    assert await _ensure(state, {"pets-db"}) == [("pets-db", "pets")]
    assert (await _record(plane, pets)).built_store == STORE


@pytest.mark.asyncio
async def test_a_row_materialize_flag_is_ignored_when_the_engine_attaches_the_source(wiring, plane):
    """The flag is the reach for a source the engine CANNOT attach. On an engine that reads the
    source in place, a flagged table the statement names with no key bound is read through the
    attach: no build, no lock, no control-plane statement, on every surface."""
    backend = _Backend(live={"mongo"})
    state = _state(
        [_source("mongo", type=SimpleNamespace(value="mongodb"))],
        [
            _table(
                "mongo",
                "order_docs",
                schema="provisa_bench",
                row_materialize=True,
                columns=[
                    SimpleNamespace(name="order_id", is_primary_key=True, native_filter_type=None)
                ],
            )
        ],
        backend,
        plane,
    )
    # the bound engine declares a connector that reads mongodb in place
    state.federation_engine.engine.connectors = {"mongodb": _attach()}
    for _ in range(2):
        assert await _ensure(state, {"mongo"}) == []
    assert state.tenant_db.acquires == 0 and wiring.kicks == 0


@pytest.mark.asyncio
async def test_the_flag_applies_when_the_engine_cannot_attach_the_source(wiring, plane):
    """The same table on an engine with no connector for its source type: the flag is its reach,
    so it is left to the keyed paths while a sibling table of the source is still built whole."""
    state = _state(
        [_source("mongo", type=SimpleNamespace(value="mongodb"))],
        [
            _table(
                "mongo",
                "order_docs",
                schema="provisa_bench",
                row_materialize=True,
                columns=[
                    SimpleNamespace(name="order_id", is_primary_key=True, native_filter_type=None)
                ],
            ),
            _table("mongo", "order_tags", schema="provisa_bench"),
        ],
        _Backend(),
        plane,
    )
    state.federation_engine.engine.connectors = {}
    assert await _ensure(state, {"mongo"}) == [("mongo", "order_tags")]


# -- what the statement read, for the audit record's data age (REQ-1915) ------------------------


@pytest.mark.asyncio
async def test_every_replica_read_is_returned_with_the_completion_of_the_build_it_is_read_from(
    wiring, plane
):
    """A replica found fresh and a replica waited for are both read, each with the completion
    time of the build its read is answered from; a fresh one costs no control-plane read."""
    at = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=30)
    pets = _table("pets-db", "pets")
    owners = _table("pets-db", "owners")
    state = _state([_source("pets-db")], [pets, owners], _Backend(), plane)
    await _built(plane, pets, at=at)
    first = await _residency(state, {"pets-db"})
    assert first.built == [("pets-db", "owners")]
    owners_at = (await _record(plane, owners)).completed_at
    assert first.replicas_read == {_key(pets): at, _key(owners): owners_at}
    assert all(t.tzinfo is not None for t in first.replicas_read.values())

    before = state.tenant_db.acquires
    again = await _residency(state, {"pets-db"})
    assert again.built == [] and again.replicas_read == first.replicas_read
    assert state.tenant_db.acquires == before  # decided from this process's copy


@pytest.mark.asyncio
async def test_a_table_read_live_is_not_in_what_was_read_from_a_replica(wiring, plane):
    pets = _table("live-db", "pets")
    state = _state([_source("live-db")], [pets], _Backend(live={"live-db"}), plane)
    assert (await _residency(state, {"live-db"})).replicas_read == {}
    order = _table("bench-neo4j", "bench_order_node", schema="neo4j", row_materialize=True)
    state = _state([_source("bench-neo4j")], [order], _Backend(), plane)
    assert (await _residency(state, {"bench-neo4j"})).replicas_read == {}


@pytest.mark.asyncio
async def test_a_replica_read_with_no_build_in_this_store_is_refused_not_left_out(wiring, plane):
    """The plan may decide a replica needs no build (a load-protected table is never rebuilt
    by a read). If that replica has no completed build in this store, the read is refused: the
    audit record must not say live data for a replica read."""
    from provisa.federation.query_residency import ReplicaAgeUnknown

    class _NoReadBuilds(_Backend):
        def pending_lands(self, sources, *, is_stale, **kw):
            # Served from a replica (asked with nothing built), never built by a read.
            return [s.id for s in sources] if kw["resident_of"] is None else []

    pets = _table("pets-db", "pets")
    state = _state([_source("pets-db")], [pets], _NoReadBuilds(), plane)
    await _built(plane, pets, store="another-store")
    with pytest.raises(ReplicaAgeUnknown, match="pets-db.pet_store.pets"):
        await _residency(state, {"pets-db"})


@pytest.mark.asyncio
async def test_the_engine_residency_step_puts_what_was_read_on_the_plan(monkeypatch):
    """pgwire and Flight SQL share this step: what the statement read from replicas goes on the
    plan, where the audit record takes its data age from."""
    from provisa.federation import query_residency
    from provisa.federation.query_residency import Residency, prepare_engine_residency

    read = {("s", "public", "t"): datetime(2026, 10, 2, tzinfo=UTC)}

    async def nothing(*args, **kwargs):
        return None

    async def resident(*args, **kwargs):
        return Residency(built=[], replicas_read=read)

    monkeypatch.setattr(query_residency, "ensure_rows_resident", nothing)
    monkeypatch.setattr(query_residency, "pushdown_row_materialize", nothing)
    monkeypatch.setattr(query_residency, "ensure_resident", resident)
    plan = SimpleNamespace(
        pk_bounds={},
        role_id="r",
        physical_sql="SELECT 1",
        exec_params=[],
        sources={"s"},
        table_ids=(1,),
        replicas_read={},
    )
    state = SimpleNamespace(federation_engine=SimpleNamespace(dialect="postgres"))
    await prepare_engine_residency(state, plan)
    assert plan.replicas_read == read
