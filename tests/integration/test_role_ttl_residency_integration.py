# Copyright (c) 2026 Kenneth Stott
# Canary: 6b0d3e8f-2a19-4c57-8e4d-f1a7c2b93e05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration (REQ-1907): per-reader landing freshness over the REAL control-plane freshness state.

The real ``ensure_resident`` reads and stamps the real ``node_freshness_state`` rows (through the
real ``queue.get_node_state`` / ``queue.record_refresh``) of a real Postgres control plane, under
the real per-node ``land_lock``. Only the engine's land itself is a recording stand-in, so what is
asserted is the decision the query path makes against persisted state: a tolerant role serves a
200s-old replica, a TTL-0 role lands only past the table's cache_ttl floor, and two concurrent
stale reads share one land because the second re-reads the state the first stamped.
"""

# Requirements: REQ-1907, REQ-1661

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import node_freshness_state
from provisa.events import queue
from provisa.federation.query_residency import ensure_resident

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

NODE = "sch.orders"


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
        node_freshness_state.metadata.create_all(c, tables=[node_freshness_state])
    try:
        yield Database(engine, name="rttl", search_path=schema)
    finally:
        with engine.begin() as c:
            c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()


class _RecordingBackend:
    """The engine's land, recorded: ``materialize_pending`` lands a source iff the query path's
    oracle says so (a real land takes time, so a concurrent reader queues on the node lock)."""

    dialect = "postgres"

    def __init__(self) -> None:
        self.lands = 0

    def is_first_touch(self, sid: str) -> bool:
        return False  # this process already holds the replica; only the persisted clock decides

    def mark_landed(self, sid: str) -> None:
        pass

    def landing_target(self, *, store_schema, source_id, source_type, schema_name, table_name):
        return schema_name, table_name

    async def materialize_pending(
        self, state, *, loader, is_stale, source_ids, load_protected_of, resident_of, **kw
    ):
        out = []
        for sid in source_ids:
            if is_stale(sid):
                self.lands += 1
                await asyncio.sleep(0.2)
                out.append((sid, "orders"))
        return out


def _state(db: Database, backend: _RecordingBackend) -> SimpleNamespace:
    source = SimpleNamespace(
        id="s",
        type=SimpleNamespace(value="rss"),
        change_signal="ttl",
        cache_ttl=None,
        freshness_gate=False,
        prefer_materialized=False,
        load_protected=False,
    )
    table = SimpleNamespace(
        source_id="s",
        schema_name="sch",
        table_name="orders",
        row_materialize=False,
        columns=[],
        cache_ttl=60,
        role_ttl={"analyst": 360, "trader": 0},
        change_signal=None,
        prefer_materialized=None,
        load_protected=None,
    )
    engine = SimpleNamespace(
        engine=SimpleNamespace(
            backend=backend,
            native_store="postgres",
            connectors={},
            materialize_store=lambda: "postgresql://unused/materialize",
        )
    )
    return SimpleNamespace(
        federation_engine=engine,
        config=SimpleNamespace(sources=[source], tables=[table]),
        tenant_db=db,
    )


@pytest.fixture
def registry(monkeypatch):
    """The registry view reads the control plane's registered tables; this test registers its one
    table in-process (the decision under test is the freshness judgement, not the registry)."""

    async def _sources(state, conn=None):
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda s, e: {})
    monkeypatch.setattr(
        "provisa.events.app_wiring.build_keyed_adapter_loaders", lambda s, e=None: {}
    )
    monkeypatch.setattr("provisa.events.land_lock._locks", {})


async def _stamp(db: Database, age_seconds: float) -> None:
    async with db.acquire() as conn:
        await queue.record_refresh(
            conn, NODE, at=datetime.now(UTC) - timedelta(seconds=age_seconds), ok=True
        )


async def _age(db: Database) -> float:
    async with db.acquire() as conn:
        state = await queue.get_node_state(conn, NODE)
    assert state is not None and state["last_refresh_at"] is not None
    return datetime.now(UTC).timestamp() - state["last_refresh_at"]


async def test_an_analyst_serves_a_200s_old_replica(db, registry):
    backend = _RecordingBackend()
    await _stamp(db, 200)
    assert await ensure_resident(_state(db, backend), {"s"}, reader_role="analyst") == []
    assert backend.lands == 0
    assert await _age(db) >= 199  # the persisted stamp was not touched


async def test_a_trader_lands_only_past_the_cache_ttl_floor(db, registry):
    backend = _RecordingBackend()
    await _stamp(db, 30)
    assert await ensure_resident(_state(db, backend), {"s"}, reader_role="trader") == []
    await _stamp(db, 200)
    assert await ensure_resident(_state(db, backend), {"s"}, reader_role="trader") == [
        ("s", "orders")
    ]
    assert backend.lands == 1
    assert await _age(db) < 5  # the land re-stamped the persisted state


async def test_two_concurrent_trader_reads_share_one_land(db, registry):
    backend = _RecordingBackend()
    await _stamp(db, 200)
    state = _state(db, backend)
    first, second = await asyncio.gather(
        ensure_resident(state, {"s"}, reader_role="trader"),
        ensure_resident(state, {"s"}, reader_role="trader"),
    )
    assert backend.lands == 1
    assert sorted([first, second]) == [[], [("s", "orders")]]
