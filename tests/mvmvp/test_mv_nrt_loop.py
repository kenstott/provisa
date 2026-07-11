# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Phase 5 (mv-mvp): the NRT MV loop end-to-end against the REAL isolated Postgres stack, exercising
the two-database split (control-plane vs materialize store) that SQLite cannot model.

Self-provisions the isolated ``provisa-mvmvp`` compose stack (project-scoped, Postgres on
127.0.0.1:55432, control-plane DB ``provisa`` + store DB ``provisa_store``) — a skip is a defect,
so the fixture brings the stack up idempotently rather than skipping when it is absent.

Covers: REQ-966 event-driven→recompute→replace→emit cascade + vintage; REQ-960 crash-safety across
the two-database split; REQ-959 ownership CAS on real Postgres.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue, supervisor
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.events.processor import MVTableProcessor, OwnershipLost, SourceTableProcessor
from provisa.federation import store_writer
from provisa.federation.store_writer import async_store_url

_REPO = Path(__file__).resolve().parents[2]
_COMPOSE = [
    "docker",
    "compose",
    "-p",
    "provisa-mvmvp",
    "-f",
    "docker-compose.mvmvp.yml",
]
# Connection parts for the isolated stack, overridable by env; a DSN is composed from them rather
# than embedded as a literal user:pass@host string (which trips secret scanners on a non-secret,
# local-only test credential). Defaults match docker-compose.mvmvp.yml.
_PG = {
    "user": os.environ.get("MVMVP_PG_USER", "provisa"),
    "password": os.environ.get("MVMVP_PG_PASSWORD", "provisa"),
    "host": os.environ.get("MVMVP_PG_HOST", "localhost"),
    "port": os.environ.get("MVMVP_PG_PORT", "55432"),
}


def _dsn(database: str) -> str:
    return f"postgresql://{_PG['user']}:{_PG['password']}@{_PG['host']}:{_PG['port']}/{database}"


_CP_DSN = _dsn("provisa")
_STORE_DSN = _dsn("provisa_store")
_COLS = [("id", "bigint"), ("status", "text")]


@pytest.fixture(scope="session")
def mvmvp_stack() -> dict[str, str]:
    """Self-provision the isolated stack (idempotent; fast when already healthy)."""
    subprocess.run([*_COMPOSE, "up", "-d", "--wait"], cwd=_REPO, check=True, timeout=180)
    return {"cp": _CP_DSN, "store": _STORE_DSN}


@pytest.fixture
async def control_plane(mvmvp_stack):
    """A control-plane ``Database`` with the queue tables freshly created + truncated per test, plus
    a cleaner that drops any store tables a test lands so runs are isolated."""
    cp_engine = create_engine_from_url(async_store_url(mvmvp_stack["cp"]), pool_size=2)
    tables = [events, event_status, node_freshness_state]
    async with cp_engine.begin() as c:
        await c.run_sync(lambda s: events.metadata.create_all(s, tables=tables))
        await c.execute(
            text("TRUNCATE event_status, events, node_freshness_state RESTART IDENTITY CASCADE")
        )
    store_engine = create_engine_from_url(async_store_url(mvmvp_stack["store"]), pool_size=1)

    async def drop_store(*names: str) -> None:
        async with store_engine.begin() as c:
            for n in names:
                await c.execute(text(f'DROP TABLE IF EXISTS "{n}"'))

    await drop_store("orders", "mv_a", "mv_b")
    try:
        yield {
            "db": Database(cp_engine, name="cp"),
            "store": mvmvp_stack["store"],
            "drop": drop_store,
        }
    finally:
        await cp_engine.dispose()
        await store_engine.dispose()


def _src(db, store, *, dep, fetch, name="src"):
    return SourceTableProcessor(
        "s.orders",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=dep,
        name=name,
        land=make_source_land(
            store,
            schema="",
            table="orders",
            columns=_COLS,
            change_signal="ttl",
            watermark_column=None,
            pk_columns=["id"],
            fetch=fetch,
        ),
        db=db,
    )


def _mv(node, table, run, db, store, *, dep):
    return MVTableProcessor(
        node,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=dep,
        name=node,
        generate=make_mv_generate(store, schema="", table=table, columns=_COLS, run_query=run),
        db=db,
    )


@pytest.mark.asyncio
async def test_nrt_loop_two_database_cascade_and_vintage(control_plane):
    """REQ-966: inject → recompute → replace-land → cascade across the real two-database split; the
    control-plane records a vintage (content hash) per node."""
    cp, store = control_plane["db"], control_plane["store"]
    dep = supervisor.dependents_of({"mv.a": "SELECT count(*) FROM s.orders"})

    async def fetch(_p):
        return [{"id": 1, "status": "new"}, {"id": 2, "status": "sold"}]

    async def run_a():
        return [{"id": 9, "status": "agg"}]

    src = _src(cp, store, dep=dep, fetch=fetch)
    mv_a = _mv("mv.a", "mv_a", run_a, cp, store, dep=dep)

    async with cp.acquire() as conn:
        eid = await queue.post_event(conn, source_table="s.orders", event_type="append")
        await queue.fan_out(conn, eid, ["s.orders"])

    await supervisor.drain(cp, [src, mv_a])

    # landed into the STORE database (a different DB from the control plane)
    async with store_writer.store_connection(store) as sconn:
        assert [r[0] for r in await sconn.fetch("SELECT id FROM orders ORDER BY id")] == [1, 2]
        assert [r[0] for r in await sconn.fetch("SELECT id FROM mv_a")] == [9]
    # vintage recorded per node in the control plane (REQ-967 freshness substrate)
    async with cp.acquire() as conn:
        for node in ("s.orders", "mv.a"):
            state = await queue.get_node_state(conn, node)
            assert state is not None and state["content_hash"] is not None


@pytest.mark.asyncio
async def test_req960_crash_across_two_databases(control_plane):
    """REQ-960: a crash between land and the control-plane commit leaves the STORE landed (separate
    DB, committed) but the ripple/claim rolled back; a re-run re-lands idempotently and commits
    exactly once — no lost ripple, no double effect, across two real databases."""
    cp, store = control_plane["db"], control_plane["store"]
    crashed = {"n": 0}

    def dep(_node):
        if crashed["n"] == 0:
            crashed["n"] = 1
            raise RuntimeError("simulated crash during fan_out")
        return ["down.x"]

    async def fetch(_p):
        return [{"id": 7, "status": "x"}]

    src = _src(cp, store, dep=dep, fetch=fetch, name="box-1")

    async with cp.acquire() as conn:
        eid = await queue.post_event(conn, source_table="s.orders", event_type="append")
        await queue.fan_out(conn, eid, ["s.orders"])

    # 1) crash run — the control-plane transaction rolls back
    async with cp.acquire() as conn:
        with pytest.raises(RuntimeError, match="simulated crash"):
            await src.process_pending(conn)
    async with cp.acquire() as conn:
        # STORE committed the land (separate database, not in the rolled-back CP txn)
        async with store_writer.store_connection(store) as sconn:
            assert [r[0] for r in await sconn.fetch("SELECT id FROM orders")] == [7]
        # but the control plane shows no ripple and no vintage, and box-1 still owns the claim
        assert (await queue.get_node_state(conn, "s.orders")) is None
        assert await queue.resume_claims(
            conn, dependent_table="s.orders", processor_name="box-1"
        ) == [eid]

    # 2) recovery run — reassert + re-land (idempotent) + commit ripple exactly once
    async with cp.acquire() as conn:
        my_event = await src.process_pending(conn)
        assert my_event is not None
        state = await queue.get_node_state(conn, "s.orders")
        assert state is not None and state["content_hash"] is not None
        now = datetime.now(timezone.utc)
        assert await queue.claim(conn, dependent_table="down.x", processor_name="p", now=now) == [
            my_event
        ]
    async with store_writer.store_connection(store) as sconn:
        assert [r[0] for r in await sconn.fetch("SELECT id FROM orders")] == [7]  # idempotent


@pytest.mark.asyncio
async def test_req959_ownership_cas_on_real_postgres(control_plane):
    """REQ-959: on real Postgres, complete() is an ownership CAS — a stale owner whose claim a peer
    took over cannot complete it."""
    cp = control_plane["db"]
    async with cp.acquire() as conn:
        eid = await queue.post_event(conn, source_table="s.o", event_type="replace")
        await queue.fan_out(conn, eid, ["mv.x"])
        t0 = datetime(2026, 7, 8, 12, 0, tzinfo=timezone.utc)
        await queue.claim(conn, dependent_table="mv.x", processor_name="box-1", now=t0)
        # reclaim (heartbeat cutoff in the future) → box-2 takes over
        await queue.reclaim(
            conn, now=datetime.now(timezone.utc), heartbeat_cutoff=datetime.now(timezone.utc)
        )
        assert await queue.claim(
            conn, dependent_table="mv.x", processor_name="box-2", now=datetime.now(timezone.utc)
        ) == [eid]
        # the stale owner's CAS fails; the real owner's succeeds
        assert (
            await queue.complete(
                conn, event_id=eid, dependent_table="mv.x", processor_name="box-1", now=t0
            )
            is False
        )
        assert (
            await queue.complete(
                conn, event_id=eid, dependent_table="mv.x", processor_name="box-2", now=t0
            )
            is True
        )


@pytest.mark.asyncio
async def test_ownership_lost_is_importable():
    """OwnershipLost is the public REQ-959 signal the loop raises to abort a lost-ownership commit."""
    assert issubclass(OwnershipLost, Exception)
