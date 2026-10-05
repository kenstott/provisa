# Copyright (c) 2026 Kenneth Stott
# Canary: 6a9e778c-9038-42d5-a9ef-d520f7b72697
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: one replica's build goes through the data replicator — the table's reader, the
engine's part and the store's write face, at the replica's address — and the runner that runs
builds is a per-worker scheduled pass for every org of a process that does background work."""

from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest

from provisa.core import process_mode
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.federation import replica_builds
from provisa.federation.data_replicator import (
    EngineCaps,
    SourceCaps,
    SourceRead,
    TargetCaps,
    TargetLoad,
    TargetWrite,
)
from provisa.federation.replica_address import ReplicaAddress, replica_table_name
from provisa.scheduler.executor import PER_WORKER_JOB_IDS, runs_in_every_worker

pytestmark = pytest.mark.unit


def _col(name, data_type="text", pk=False):
    return SimpleNamespace(
        name=name, data_type=data_type, is_primary_key=pk, native_filter_type=None
    )


def _table(source_id="s1"):
    return SimpleNamespace(
        source_id=source_id,
        schema_name="public",
        table_name="events",
        change_signal=None,
        watermark_column=None,
        probe_type=None,
        live=None,
        row_materialize=False,
        columns=[_col("id", "bigint", pk=True), _col("status", "text")],
    )


def _source(source_id="s1"):
    return SimpleNamespace(
        id=source_id,
        type="openapi",
        change_signal="ttl",
        freshness_gate=False,
        cache_ttl=None,
        replicate=None,
        load_protected=False,
    )


class _Target:
    caps = TargetCaps(frozenset({TargetWrite.BULK_BATCH}), True, TargetLoad.BULK_STREAM)

    def __init__(self, log, address, args):
        self.log, self.address, self.args = log, address, args

    async def begin(self):
        self.log.append("begin")

    async def write(self, batch, rows):
        self.log.append(("write", rows))

    async def swap(self):
        self.log.append("swap")

    async def abort(self):
        self.log.append("abort")


class _Backend:
    dialect = "trino"

    def __init__(self):
        self.log: list = []
        self.target: _Target | None = None

    def replica_address(self, state, *, source_id, schema_name, table_name):
        return ReplicaAddress(
            "org_acme_replicas", replica_table_name(source_id, schema_name, table_name)
        )

    def replica_engine(self, state, source, table, *, address, args):
        log = self.log

        class _Engine:
            caps = EngineCaps(reaches_source=False, runs=frozenset())

            async def after_swap(self):
                log.append("after_swap")

        return _Engine()

    def replica_target(self, state, *, address, args, engine):
        self.target = _Target(self.log, address, args)
        return self.target


class _Reader:
    caps = SourceCaps(frozenset({SourceRead.CURSOR}))

    def __init__(self, rows):
        self._rows = rows

    async def batches(self, batch_rows):
        yield pa.RecordBatch.from_pylist(self._rows)


class _Db:
    def acquire(self):
        class _Ctx:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.fixture
def wiring(monkeypatch):
    """The registry (the config IS the registry here), the vault, the adapter loaders, the land
    lock and the state store's record read, so ``build_replica`` runs over the fakes above."""
    from contextlib import asynccontextmanager

    rows = [{"id": 1, "status": "new"}, {"id": 2, "status": "old"}]
    seen: dict = {"record": None, "read_columns": None}

    async def _sources(state, conn=None):
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    @asynccontextmanager
    async def _vault(state, sources):
        yield

    class _Loader:
        def __init__(self, engine, **kw):
            pass

        def replica_source(self, state, source, table, columns):
            seen["read_columns"] = columns
            return _Reader(rows)

    async def _read(conn, key):
        return seen["record"]

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.federation.source_vault.org_vault", _vault)
    monkeypatch.setattr("provisa.events.source_loader.SourceRowLoader", _Loader)
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda s, e: {})
    monkeypatch.setattr(
        "provisa.events.app_wiring.build_keyed_adapter_loaders", lambda s, e=None: {}
    )
    monkeypatch.setattr("provisa.events.land_lock._locks", {})
    monkeypatch.setattr("provisa.federation.replica_state.read", _read)

    async def _started(conn, key, *, method, load_kind):
        seen["started"] = (key, method, load_kind)

    monkeypatch.setattr("provisa.federation.replica_state.record_started", _started)
    monkeypatch.setattr(replica_builds, "store_identity", lambda state: "store-a")
    return seen


def _state(backend, sources=None, tables=None):
    return SimpleNamespace(
        config=SimpleNamespace(
            sources=[_source()] if sources is None else sources,
            tables=[_table()] if tables is None else tables,
        ),
        federation_engine=SimpleNamespace(engine=SimpleNamespace(backend=backend, name="fake")),
        tenant_db=_Db(),
    )


async def _noop(rows_copied):
    return None


async def test_a_build_of_a_table_with_no_whole_copy_is_refused_before_anything_is_read(wiring):
    """Nothing asks for one (convergence and the read backstop apply the same rule); a build
    that ran would call a function with no arguments."""
    table = _table()
    table.columns.append(
        SimpleNamespace(
            name="pet_id", data_type="bigint", is_primary_key=False, native_filter_type="path_param"
        )
    )
    backend = _Backend()
    with pytest.raises(replica_builds.NoWholeCopy, match="public.events of source s1"):
        await replica_builds.build_replica(
            _state(backend, tables=[table]), ("s1", "public", "events"), _noop
        )
    assert backend.log == []


async def test_a_build_of_a_push_source_table_is_refused_before_anything_is_read(wiring):
    """REQ-1907/REQ-929: a push source (ingest/websocket/kafka) is fed by its push path, has no
    whole copy to build, and would hit the "no engine-scannable table and no adapter row-fetch"
    refusal — convergence and the read backstop never ask for its build either."""
    for stype in ("ingest", "websocket", "kafka"):
        table = _table()
        source = _source()
        source.type = stype
        backend = _Backend()
        with pytest.raises(replica_builds.NoWholeCopy, match="public.events of source s1"):
            await replica_builds.build_replica(
                _state(backend, sources=[source], tables=[table]),
                ("s1", "public", "events"),
                _noop,
            )
        assert backend.log == []


async def test_a_build_reads_the_table_and_replaces_its_replica_at_the_replicas_address(wiring):
    backend = _Backend()
    outcome = await replica_builds.build_replica(_state(backend), ("s1", "public", "events"), _noop)
    assert (outcome.rows_copied, outcome.method, outcome.changed) == (2, "stream_batches", True)
    # how it copies is recorded as it starts; what it was built from goes with its completion
    assert wiring["started"] == (("s1", "public", "events"), "stream_batches", "bulk_stream")
    assert outcome.built_columns == [["id", "bigint"], ["status", "text"]]
    assert outcome.definition_hash and len(outcome.definition_hash) == 64
    # REQ-1912: the org's replicas schema, under the one replica name — on every engine
    target = backend.target
    assert (target.address.schema, target.address.table) == (
        "org_acme_replicas",
        "s1__public__events",
    )
    assert target.args.columns == wiring["read_columns"] == [("id", "bigint"), ("status", "text")]
    assert target.args.pk_columns == ["id"]
    assert backend.log == [
        "begin",
        ("write", [{"id": 1, "status": "new"}, {"id": 2, "status": "old"}]),
        "swap",
        "after_swap",
    ]


async def test_a_build_of_a_table_the_model_no_longer_has_is_refused_by_name(wiring):
    backend = _Backend()
    with pytest.raises(replica_builds.ReplicaTableGone, match="public.events of source s1"):
        await replica_builds.build_replica(
            _state(backend, tables=[]), ("s1", "public", "events"), _noop
        )
    with pytest.raises(replica_builds.ReplicaTableGone):
        await replica_builds.build_replica(
            _state(backend, sources=[]), ("s1", "public", "events"), _noop
        )
    assert backend.log == []


async def test_unchanged_content_is_discarded_only_against_a_replica_in_this_store(wiring):
    """The last build's hash says "unchanged" only of the replica standing in THIS store: a
    record left by another engine's store has no table here, so the build must swap."""
    backend = _Backend()
    state = _state(backend)
    first = await replica_builds.build_replica(state, ("s1", "public", "events"), _noop)

    def record(store):
        return SimpleNamespace(
            content_hash=first.content_hash, exists_in=lambda asked: asked == store
        )

    wiring["record"] = record("store-a")
    backend.log.clear()
    same = await replica_builds.build_replica(state, ("s1", "public", "events"), _noop)
    assert same.changed is False and backend.log[-1] == "abort" and "swap" not in backend.log

    wiring["record"] = record("another-store")
    backend.log.clear()
    moved = await replica_builds.build_replica(state, ("s1", "public", "events"), _noop)
    assert moved.changed is True and "swap" in backend.log


def test_the_build_pass_runs_in_every_worker_for_every_org():
    assert replica_builds.RUNNER_JOB_ID in PER_WORKER_JOB_IDS
    assert runs_in_every_worker("replica:builds")
    assert runs_in_every_worker("replica:builds:org_acme")
    assert runs_in_every_worker("egress_drain")
    assert not runs_in_every_worker("events:tick")
    assert not runs_in_every_worker("events:tick:org_acme")


class _Scheduler:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def add_job(self, func, **kw):
        self.jobs[kw["id"]] = {"func": func, **kw}


def test_a_process_that_does_background_work_registers_its_build_pass(monkeypatch, tmp_path):
    made: list = []
    monkeypatch.setattr(
        replica_builds, "make_runner", lambda state, org, url: made.append((org, url)) or object()
    )
    monkeypatch.setattr(replica_builds, "_runners", {})
    scheduler = _Scheduler()
    url = f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    process_mode.set_mode(process_mode.EVERY)
    # The boot wires it with the org it serves bound (REQ-1266); the pass is that org's.
    org_token = set_current_org("acme")
    try:
        replica_builds.wire_replica_runner(scheduler, state=object(), platform_url=url)
        assert list(scheduler.jobs) == ["replica:builds:org_acme"] and made == [("acme", url)]
        assert scheduler.jobs["replica:builds:org_acme"]["replace_existing"] is True
        # a process that only answers queries builds nothing (REQ-1916)
        process_mode.set_mode(process_mode.QUERY)
        other = _Scheduler()
        replica_builds.wire_replica_runner(other, state=object(), platform_url=url)
        assert other.jobs == {} and len(made) == 1
    finally:
        reset_current_org(org_token)
        process_mode.set_mode(process_mode.EVERY)


def test_a_kick_with_no_runner_for_the_org_does_nothing(monkeypatch):
    monkeypatch.setattr(replica_builds, "_runners", {})
    replica_builds.kick("an-org-this-process-has-no-runner-for")


def test_the_base_engine_names_its_write_face_by_its_stores_kind_not_its_own():
    """A DuckDB or Trino engine whose replicas are in a PostgreSQL store writes them through the
    PostgreSQL face: the store decides, whatever the engine's own native store is called."""
    from provisa.federation.backend import EngineBackend
    from provisa.federation.data_replicator import EngineRun
    from provisa.federation.replica_target import PostgresStoreTarget

    backend = SimpleNamespace(
        engine=SimpleNamespace(
            native_store="duckdb",
            materialize_store=lambda: "postgresql+psycopg://u:p@store:5432/db",
        )
    )
    args = SimpleNamespace(columns=[("id", "bigint")], pk_columns=["id"])
    address = ReplicaAddress("org_acme_replicas", "s1__public__events")
    for runs, writes in (
        (frozenset(), {TargetWrite.COPY_STREAM}),
        (frozenset({EngineRun.STATEMENT}), {TargetWrite.COPY_STREAM, TargetWrite.STATEMENT_COPY}),
    ):
        engine = SimpleNamespace(caps=EngineCaps(reaches_source=False, runs=runs))
        target = EngineBackend.replica_target(
            backend,  # type: ignore[arg-type]
            object(),
            address=address,
            args=args,
            engine=engine,
        )
        assert isinstance(target, PostgresStoreTarget) and target.caps.writes == writes


def test_the_postgresql_engine_writes_replicas_into_its_own_database():
    """The PostgreSQL engine is its own store ("postgres"): its replicas go through the one
    PostgreSQL write face, at the engine's own address."""
    from provisa.federation.data_replicator import EngineRun
    from provisa.federation.pg_backend import PgBackend
    from provisa.federation.replica_target import PostgresStoreTarget

    backend = SimpleNamespace(
        _runtime_for=lambda state: SimpleNamespace(_engine_dsn="postgresql://u:p@engine:5432/db")
    )
    target = PgBackend.replica_target(
        backend,  # type: ignore[arg-type]
        object(),
        address=ReplicaAddress("org_acme_replicas", "s1__public__events"),
        args=SimpleNamespace(columns=[("id", "bigint")], pk_columns=None),
        engine=SimpleNamespace(
            caps=EngineCaps(reaches_source=True, runs=frozenset({EngineRun.STATEMENT}))
        ),
    )
    assert isinstance(target, PostgresStoreTarget)
    assert target._dsn == "postgresql://u:p@engine:5432/db"
    assert TargetWrite.STATEMENT_COPY in target.caps.writes


async def test_the_engines_own_copy_is_chosen_when_the_engine_reaches_the_table(wiring):
    """Whether the engine reaches a table itself is the engine's fact. The PostgreSQL engine
    copies a floored source through its own foreign table although the source has no live
    attach: the table's reader declares only a stream, the engine's part says it reaches the
    table, and the build is one statement on the engine — no row passes through this process."""
    from provisa.federation.data_replicator import BuildOutcome, EngineRun

    backend = _Backend()
    copied: list = []

    class _CopyingEngine:
        caps = EngineCaps(reaches_source=True, runs=frozenset({EngineRun.STATEMENT}))

        async def copy(self, prior_hash):
            copied.append(prior_hash)
            return BuildOutcome(rows_copied=150_000, method="engine_statement")

        async def after_swap(self):
            copied.append("after_swap")

    class _StatementTarget(_Target):
        caps = TargetCaps(
            frozenset({TargetWrite.COPY_STREAM, TargetWrite.STATEMENT_COPY}),
            True,
            TargetLoad.BULK_STREAM,
        )

    backend.replica_engine = lambda state, source, table, *, address, args: _CopyingEngine()
    backend.replica_target = lambda state, *, address, args, engine: _StatementTarget(
        backend.log, address, args
    )
    outcome = await replica_builds.build_replica(_state(backend), ("s1", "public", "events"), _noop)
    assert (outcome.method, outcome.rows_copied) == ("engine_statement", 150_000)
    assert copied == [None, "after_swap"]
    assert backend.log == []  # the stream's target was never opened


async def test_a_table_deleted_while_its_build_runs_is_not_swapped_in(wiring):
    """The build ends, finds no model row before the swap, and removes its build table."""
    backend = _Backend()
    state = _state(backend)
    real_write = _Target.write

    async def write_then_delete(self, batch, rows):
        await real_write(self, batch, rows)
        state.config.tables.clear()  # the table leaves the model mid-build

    _Target.write = write_then_delete  # type: ignore[method-assign]
    try:
        with pytest.raises(replica_builds.ReplicaTableGone):
            await replica_builds.build_replica(state, ("s1", "public", "events"), _noop)
    finally:
        _Target.write = real_write  # type: ignore[method-assign]
    assert "swap" not in backend.log and backend.log[-1] == "abort"


class _Queue:
    def __init__(self):
        self.refreshes: list = []
        self.events: list = []
        self.fanned: list = []

    async def record_refresh(self, conn, node, *, at, ok):
        self.refreshes.append((node, ok))

    async def post_event(self, conn, *, source_table, event_type, payload=None):
        self.events.append((source_table, event_type, payload))
        return len(self.events)

    async def fan_out(self, conn, event_id, dependent_tables):
        self.fanned.append((event_id, dependent_tables))
        return len(dependent_tables)


@pytest.fixture
def queue(monkeypatch):
    held = _Queue()
    for name in ("record_refresh", "post_event", "fan_out"):
        monkeypatch.setattr(f"provisa.events.queue.{name}", getattr(held, name))
    return held


def _built(monkeypatch, outcome):
    async def build(state, key, progress):
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(replica_builds, "build_replica", build)


async def test_a_changed_build_is_posted_to_its_tables_node_so_dependents_ripple(
    queue, monkeypatch
):
    from provisa.federation.data_replicator import BuildOutcome

    key = ("s1", "public", "events")
    state = SimpleNamespace(tenant_db=_Db(), replica_nodes={key: "s1/public.events"})
    _built(monkeypatch, BuildOutcome(rows_copied=9, method="stream_batches", changed=True))
    await replica_builds.run_build(state, key, _noop)
    assert queue.refreshes == [("s1/public.events", True)]
    assert queue.events == [("s1/public.events", "replace", {"built": True, "rows": 9})]
    assert queue.fanned == [(1, ["s1/public.events"])]  # to the node itself: it re-posts onward


async def test_an_unchanged_build_ripples_nothing(queue, monkeypatch):
    from provisa.federation.data_replicator import BuildOutcome

    key = ("s1", "public", "events")
    state = SimpleNamespace(tenant_db=_Db(), replica_nodes={key: "s1/public.events"})
    _built(monkeypatch, BuildOutcome(rows_copied=9, method="stream_batches", changed=False))
    await replica_builds.run_build(state, key, _noop)
    assert queue.refreshes == [("s1/public.events", True)] and queue.events == []


async def test_a_build_of_a_table_with_no_event_loop_node_posts_nothing(queue, monkeypatch):
    from provisa.federation.data_replicator import BuildOutcome

    state = SimpleNamespace(tenant_db=_Db())  # the event loop is not wired, or has no such node
    _built(monkeypatch, BuildOutcome(rows_copied=9, method="stream_batches"))
    await replica_builds.run_build(state, ("s1", "public", "events"), _noop)
    assert queue.events == [] and queue.refreshes == [("s1/public.events", True)]


async def test_a_failed_build_is_stamped_not_fresh_and_ripples_nothing(queue, monkeypatch):
    key = ("s1", "public", "events")
    state = SimpleNamespace(tenant_db=_Db(), replica_nodes={key: "s1/public.events"})
    _built(monkeypatch, RuntimeError("source down"))
    with pytest.raises(RuntimeError, match="source down"):
        await replica_builds.run_build(state, key, _noop)
    assert queue.refreshes == [("s1/public.events", False)] and queue.events == []


# -- REQ-874: build_replica branches a delta table into an incremental apply, or a whole rebuild
# that records why. These drive the real build_replica over the fakes above, with the delta read +
# apply (delta.apply_sql_delta, covered end-to-end against a real store in test_delta_apply_store)
# stubbed, so what is asserted is build_replica's OWN wiring: the decision, the store DSN it applies
# into, and the cursor/skip bookkeeping.


def _delta_table():
    from provisa.core.models import DeltaConfig

    t = _table()
    t.watermark_column = "updated_at"
    t.delta = DeltaConfig(apply="upsert")
    return t


def _delta_state(backend):
    source = _source()
    source.type = "postgresql"  # a SQL source: delta is defined for it
    eng = SimpleNamespace(
        backend=backend,
        name="fake",
        native_store=None,  # not its own store -> applies into the materialize store
        materialize_store=lambda: "duckdb:///x.duckdb",
    )
    return SimpleNamespace(
        config=SimpleNamespace(sources=[source], tables=[_delta_table()]),
        federation_engine=SimpleNamespace(engine=eng),
        tenant_db=_Db(),
        source_pools=SimpleNamespace(has=lambda _id: True),
    )


async def test_a_delta_table_with_a_cursor_applies_the_delta_and_advances_it(wiring, monkeypatch):
    captured: dict = {}

    async def _apply(state, engine, source, table, args, address, cursor, store_dsn):
        captured["cursor"] = cursor
        captured["store_dsn"] = store_dsn
        captured["address"] = (address.schema, address.table)
        return 3, 42

    async def _record_delta(conn, key, *, cursor):
        captured["recorded_cursor"] = cursor

    monkeypatch.setattr("provisa.federation.delta.apply_sql_delta", _apply)
    monkeypatch.setattr("provisa.federation.replica_state.record_delta_applied", _record_delta)
    wiring["record"] = SimpleNamespace(
        delta_cursor="7", definition_hash=None, requested_reason="refresh",
        exists_in=lambda store: store == "store-a",
    )  # fmt: skip

    backend = _Backend()
    outcome = await replica_builds.build_replica(
        _delta_state(backend), ("s1", "public", "events"), _noop
    )
    # The delta path ran: no whole copy was read/written through the target.
    assert backend.log == [] and backend.target is None
    assert (outcome.rows_copied, outcome.method, outcome.changed) == (3, "delta", True)
    assert captured["cursor"] == 7  # the stored JSON cursor, decoded and bound as $1
    assert captured["store_dsn"] == "duckdb:///x.duckdb"
    assert captured["address"] == ("org_acme_replicas", "s1__public__events")
    assert captured["recorded_cursor"] == 42  # advanced
    assert wiring["started"][1] == "delta"


async def test_a_delta_table_without_a_cursor_whole_rebuilds_and_records_the_skip(
    wiring, monkeypatch
):
    captured: dict = {}

    async def _max_wm(state, engine, source, table):
        return 99

    async def _record_whole(conn, key, *, skipped, cursor):
        captured["skipped"] = skipped
        captured["cursor"] = cursor

    monkeypatch.setattr("provisa.federation.delta.source_max_watermark", _max_wm)
    monkeypatch.setattr("provisa.federation.replica_state.record_whole_rebuild", _record_whole)
    wiring["record"] = None  # no prior build -> no cursor -> first_build

    backend = _Backend()
    outcome = await replica_builds.build_replica(
        _delta_state(backend), ("s1", "public", "events"), _noop
    )
    # The whole copy ran through the target, and the skip + the fresh cursor were recorded.
    assert outcome.method == "stream_batches"
    assert "swap" in backend.log
    from provisa.federation import delta as _delta

    assert captured["skipped"] == _delta.SKIP_FIRST_BUILD
    assert captured["cursor"] == 99
