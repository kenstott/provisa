# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0d3e8f-2a19-4c57-8e4d-f1a7c2b93e05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration (REQ-1907, REQ-1915): per-reader replica freshness over the REAL state store.

The real ``ensure_resident`` reads and requests against the real ``replica_state`` rows of a real
Postgres control plane, and a real ``ReplicaRunner`` (real build locks on that control plane)
claims and records the builds. Only the copy itself is a recording stand-in, so what is asserted
is the decision the query path makes against persisted state and the one build the runner makes
of it: a tolerant role serves a 200s-old replica, a TTL-0 role asks for a build only past the
table's cache_ttl floor, and two concurrent stale reads share one build.
"""

# Requirements: REQ-1907, REQ-1661, REQ-1915

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import replica_state as replica_state_table
from provisa.federation import replica_state
from provisa.federation.data_replicator import BuildOutcome
from provisa.federation.query_residency import ensure_resident
from provisa.federation.replica_address import ReplicaRoutes
from provisa.federation.replica_locks import BuildLocks
from provisa.federation.replica_runner import ReplicaRunner

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

KEY = ("s", "sch", "orders")
STORE = "store-a"


def _async_dsn(pg_dsn: str) -> str:
    return pg_dsn.replace("postgresql://", "postgresql+psycopg://", 1)


@pytest_asyncio.fixture
async def db(pg_dsn):
    """A real PG control-plane Database scoped to a throwaway schema (isolated per test)."""
    schema = f"rttl_{uuid.uuid4().hex[:12]}"
    engine = create_engine_from_url(_async_dsn(pg_dsn))
    with engine.begin() as c:
        c.execute(text(f'CREATE SCHEMA "{schema}"'))
        c.execute(text(f'SET search_path TO "{schema}"'))
        replica_state_table.metadata.create_all(c, tables=[replica_state_table])
    try:
        yield Database(engine, name="rttl", search_path=schema)
    finally:
        with engine.begin() as c:
            c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()


class _Backend:
    """An engine that cannot read an rss source in place and has neither operator setting on:
    the source is served from its replica when the query path's oracle says it is stale."""

    dialect = "postgres"

    def pending_lands(self, sources, *, is_stale, **kw):
        del kw
        return [s.id for s in sources if is_stale(s.id)]


class _NoCap:
    @staticmethod
    def key(org_id, source_id):
        return f"{org_id}:{source_id}"


def _async(fn):
    async def call(*args):
        return fn(*args)

    return call


class _Builds:
    """The real runner over the real state store and locks; the copy is a recording stand-in
    that takes time, so a concurrent reader waits on the same record."""

    def __init__(self, db: Database, platform_url: str, tmp_path) -> None:
        self.count = 0
        self.tasks: list[asyncio.Future] = []
        locks = BuildLocks(platform_url)
        locks._slots = tmp_path / "slots"  # a host of its own
        self.runner = ReplicaRunner(
            db=db,
            org_id="org1",
            locks=locks,
            engine_key=lambda: f"postgres@{uuid.uuid4().hex}",
            build=self._build,
            source_cap=_async(lambda _key: None),
            permits=_NoCap(),
            next_refresh_at=_async(lambda _key, _now: None),
            store=lambda: STORE,
            retry_interval=lambda: 60.0,
            builds_per_node=lambda: 4,
            engine_jobs=lambda: 4,
            spawn=lambda coro, name: self.tasks.append(asyncio.ensure_future(coro)),
        )

    async def _build(self, key, progress) -> BuildOutcome:
        del key, progress
        self.count += 1
        await asyncio.sleep(0.2)
        return BuildOutcome(rows_copied=1, method="stream_batches")

    def kick(self, org_id) -> None:
        del org_id
        self.tasks.append(asyncio.ensure_future(self.runner.run_pass()))

    async def drain(self) -> None:
        while self.tasks:
            done, self.tasks = self.tasks, []
            await asyncio.gather(*done)


# The registered id of the one table; a statement that reads it carries this id (REQ-826).
_TABLE_ID = 1
_READ = frozenset({_TABLE_ID})


def _state(db: Database, backend: _Backend) -> SimpleNamespace:
    source = SimpleNamespace(
        id="s",
        type=SimpleNamespace(value="rss"),
        change_signal="ttl",
        cache_ttl=None,
        freshness_gate=False,
        replicate=None,
        load_protected=False,
        region=None,  # REQ-1921: a source and a table each carry their region
    )
    table = SimpleNamespace(
        id=_TABLE_ID,
        source_id="s",
        schema_name="sch",
        table_name="orders",
        row_materialize=False,
        columns=[],
        cache_ttl=60,
        role_ttl={"analyst": 360, "trader": 0},
        change_signal=None,
        replicate=None,
        load_protected=None,
        region=None,
    )
    engine = SimpleNamespace(
        engine=SimpleNamespace(
            backend=backend,
            name="postgres",
            native_store="postgres",
            connectors={},
            materialize_store=lambda: "postgresql://unused/materialize",
        )
    )
    return SimpleNamespace(
        federation_engine=engine,
        config=SimpleNamespace(sources=[source], tables=[table]),
        model_db=db,
        tenant_db=db,
        # as the schema build publishes it: no operator setting puts the table on its replica
        # (it is replica-served because the engine cannot read an rss source in place)
        replica_routes=ReplicaRoutes(),
    )


@pytest.fixture
def builds(monkeypatch, db, pg_dsn, tmp_path):
    """The registry view reads the control plane's registered tables; this test registers its one
    table in-process (the decision under test is the freshness judgement, not the registry). A
    read's kick starts the real runner's pass."""

    async def _sources(state, conn=None):
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.federation.replica_builds.store_identity", lambda state: STORE)
    held = _Builds(db, _async_dsn(pg_dsn), tmp_path)
    monkeypatch.setattr("provisa.federation.replica_builds.kick", held.kick)
    return held


async def _built(db: Database, age_seconds: float) -> None:
    """Record the replica as built ``age_seconds`` ago in this store."""
    at = datetime.now(UTC) - timedelta(seconds=age_seconds)
    async with db.acquire() as conn:
        await replica_state.request_build(conn, KEY, replica_state.REASON_MODEL, now=at)
        await replica_state.claim(conn, KEY, holder="test:1", retry_interval=60, now=at)
        await replica_state.record_completed(
            conn,
            KEY,
            rows_copied=1,
            method="stream_batches",
            content_hash=None,
            store=STORE,
            next_refresh_at=None,
            now=at,
        )


async def _age(db: Database) -> float:
    async with db.acquire() as conn:
        record = await replica_state.read(conn, KEY)
    assert record is not None and record.completed_at is not None
    return (datetime.now(UTC) - record.completed_at).total_seconds()


async def test_an_analyst_serves_a_200s_old_replica(db, builds):
    await _built(db, 200)
    assert (
        await ensure_resident(_state(db, _Backend()), {"s"}, reader_role="analyst", table_ids=_READ)
    ).built == []
    await builds.drain()
    assert builds.count == 0
    assert await _age(db) >= 199  # the persisted record was not touched


async def test_a_trader_asks_for_a_build_only_past_the_cache_ttl_floor(db, builds):
    await _built(db, 30)
    assert (
        await ensure_resident(_state(db, _Backend()), {"s"}, reader_role="trader", table_ids=_READ)
    ).built == []
    await _built(db, 200)
    assert (
        await ensure_resident(_state(db, _Backend()), {"s"}, reader_role="trader", table_ids=_READ)
    ).built == [("s", "orders")]
    await builds.drain()
    assert builds.count == 1
    assert await _age(db) < 5  # the build's completion is the persisted state now


async def test_two_concurrent_trader_reads_share_one_build(db, builds):
    await _built(db, 200)
    state = _state(db, _Backend())
    first, second = await asyncio.gather(
        ensure_resident(state, {"s"}, reader_role="trader", table_ids=_READ),
        ensure_resident(state, {"s"}, reader_role="trader", table_ids=_READ),
    )
    await builds.drain()
    assert builds.count == 1
    assert first.built == second.built == [("s", "orders")]  # both waited for that one build
