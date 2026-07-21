# Copyright (c) 2026 Kenneth Stott
# Canary: 3c8b1f26-9d47-4a53-b1e0-6f2a9c4d7e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""End-to-end materialization pipeline for the freshness contract (REQ-961), source → consumption.

Drives the WHOLE event loop over real collaborators — no fakes: a real ``SourceTableProcessor`` lands
rows through the real ``store_writer`` (stamping ``node_freshness_state`` via ``record_refresh``), the
change fans out through the real ``supervisor.drain`` to a real periodic ``MVTableProcessor`` whose
contract PULLs the source's freshness through the real ``make_db_freshness_of`` reader, and — when the
input is fresh-through the window boundary — SEALS the day's partition into the MV's own store table.
The consumption point asserted is the sealed store table a query would read.

Two flows:
- FRESH: the source lands at/after the boundary → the MV seals; the store table holds the partition.
- STALE: the source last landed before the boundary → the MV HOLDS (no partition sealed), never a
  silent skip.

(An HTTP-level e2e that declares the calendar + periodic MV through config is blocked until the
calendar config/admin surface exists; this drives the same pipeline in-process to the store — the
materialization consumption point.)
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue, supervisor
from provisa.events.calendars import Calendar
from provisa.events.deadlines import PeriodicCalendar
from provisa.events.freshness_reader import make_db_freshness_of
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.events.processor import MVTableProcessor, SourceTableProcessor

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

UTC = timezone.utc
SRC = "s.transactions"
MV = "mat.daily_sales"
IN_WINDOW = datetime(2026, 7, 9, 12, tzinfo=UTC)  # inside the 2026-07-09 window (before its end)
AFTER_BOUNDARY = datetime(2026, 7, 10, 1, 30, tzinfo=UTC)  # past the 2026-07-09 window's close
_TABLES = [events, event_status, node_freshness_state]
_SRC_COLS = [("id", "INTEGER"), ("amount", "INTEGER")]
_MV_COLS = [("day", "TEXT"), ("total", "INTEGER")]


@pytest_asyncio.fixture
async def store(tmp_path):
    """A real sqlite materialization store (the write face target)."""
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


@pytest_asyncio.fixture
async def db(tmp_path):
    """A real control-plane Database (event queue + freshness state)."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cp.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: events.metadata.create_all(s, tables=_TABLES))
    try:
        yield Database(engine, name="cp")
    finally:
        await engine.dispose()


def _now_at(monkeypatch, instant):
    import provisa.events.processor as pm

    monkeypatch.setattr(pm, "_now", lambda: instant)


def _procs(db, store, *, mv_rows):
    async def _fetch(_pending):
        return [{"id": 1, "amount": 10}, {"id": 2, "amount": 5}]

    async def _run_query():
        return mv_rows

    src = SourceTableProcessor(
        SRC,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: [MV] if n == SRC else [],
        db=db,
        name="src-box",
        land=make_source_land(
            store,
            schema="main",
            table="s_transactions",
            columns=_SRC_COLS,
            change_signal="ttl",
            watermark_column=None,
            pk_columns=["id"],
            fetch=_fetch,
            probe_type="none",  # replace shape
        ),
    )
    mv = MVTableProcessor(
        MV,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: [],
        db=db,
        name="mv-box",
        generate=make_mv_generate(
            store,
            schema="main",
            table="mat_daily_sales",
            columns=_MV_COLS,
            run_query=_run_query,
            persist="replace",
            pk_columns=["day"],
        ),
        deadline_source=PeriodicCalendar(
            Calendar(name="g", version="v1"), "daily", allowed_lateness=0.0
        ),
        expected_events=[SRC],
        freshness_of=make_db_freshness_of(db),
    )
    return src, mv


async def _seed_source_change(db):
    async with db.acquire() as conn:
        e = await queue.post_event(conn, source_table=SRC, event_type="replace", payload={})
        await queue.fan_out(conn, e, [SRC])  # the source node is the first dependent of its own change


async def _store_rows(store, table):
    engine = create_async_engine(store)
    try:
        async with engine.begin() as c:
            return (await c.execute(text(f"SELECT * FROM main.{table}"))).fetchall()
    finally:
        await engine.dispose()


async def _mv_freshness(db):
    async with db.acquire() as conn:
        return await queue.get_node_state(conn, MV)


# ---------------------------------------------------------------------------


async def test_fresh_source_seals_partition_to_store(db, store, monkeypatch):
    """Source lands at/after the boundary → fresh-through window.end → the MV seals its partition and
    the sealed rows are readable at the store (the consumption point)."""
    src, mv = _procs(db, store, mv_rows=[{"day": "2026-07-09", "total": 15}])
    _now_at(monkeypatch, AFTER_BOUNDARY)
    await _seed_source_change(db)
    await supervisor.drain(db, [src, mv])

    # source landed → its replica table exists and freshness was stamped fresh-through the boundary
    src_rows = await _store_rows(store, "s_transactions")
    assert len(src_rows) == 2
    async with db.acquire() as conn:
        src_state = await queue.get_node_state(conn, SRC)
    assert src_state["last_refresh_ok"] is True
    assert src_state["last_refresh_at"] >= datetime(2026, 7, 10, tzinfo=UTC).timestamp()

    # MV contract passed → the day's partition is sealed into the MV store table (consumption point)
    mv_rows = await _store_rows(store, "mat_daily_sales")
    assert [tuple(r) for r in mv_rows] == [("2026-07-09", 15)]


async def test_stale_source_holds_no_partition(db, store, monkeypatch):
    """Source last landed BEFORE the window boundary → the contract sees it not fresh-through → the MV
    HOLDS: no partition is sealed to the store, and a warn (not a silent skip) is on the log."""
    src, mv = _procs(db, store, mv_rows=[{"day": "2026-07-09", "total": 15}])

    # land the source INSIDE the 2026-07-09 window (before its end) → stale w.r.t. the boundary
    _now_at(monkeypatch, IN_WINDOW)
    await _seed_source_change(db)
    async with db.acquire() as conn:
        assert await src.process_pending(conn) is not None  # source lands, stamps freshness (stale)

    # now advance past the boundary and fan a fresh trigger at the MV; the source is unchanged (stale)
    _now_at(monkeypatch, AFTER_BOUNDARY)
    async with db.acquire() as conn:
        e = await queue.post_event(conn, source_table=SRC, event_type="replace", payload={})
        await queue.fan_out(conn, e, [MV])
        assert await mv.process_pending(conn) is None  # contract outage → hold
        warn = [
            r
            for r in await queue.read_since(conn, cursor=0)
            if r["source_table"] == MV and r["event_type"] == "warn"
        ]
    assert len(warn) == 1  # warn/hold, not a silent skip

    # the MV store table has NO sealed partition and the MV never recorded a successful refresh
    with pytest.raises(Exception):  # table never created — nothing was sealed
        await _store_rows(store, "mat_daily_sales")
    assert await _mv_freshness(db) is None
