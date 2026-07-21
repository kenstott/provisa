# Copyright (c) 2026 Kenneth Stott
# Canary: 1a7c9e34-8b2d-4f16-9c0a-5e3d7b1f4a28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the periodic MV freshness CONTRACT on real inputs against real Postgres (REQ-961).

Unlike the unit tests (which hand the processor a fake ``freshness_of`` lambda), these exercise the
REAL collaborators end-to-end at the control-plane boundary:

- the real :class:`MVTableProcessor.process_pending` claim/coalesce/gate/seal loop,
- the real ``node_freshness_state`` persistence stamped by ``queue.record_refresh`` on every land,
- the real DB-backed reader ``make_db_freshness_of`` the app wires into each periodic MV,

all over a real Postgres engine (its own isolated schema). The reader PULLs each input's persisted
refresh state and the contract decides: fresh-through window.end → SEAL; not fresh-through (stale /
failed / never-refreshed) → warn/HOLD, never a silent skip.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.calendars import Calendar
from provisa.events.deadlines import PeriodicCalendar
from provisa.events.freshness_reader import make_db_freshness_of
from provisa.events.processor import MVTableProcessor, SourceTableProcessor

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

UTC = timezone.utc
_TABLES = [events, event_status, node_freshness_state]
# The just-closed daily window when _now sits at 2026-07-10 01:30 is 2026-07-09 → end 2026-07-10 00:00.
WIN_END = datetime(2026, 7, 10, tzinfo=UTC)
AFTER_BOUNDARY = datetime(2026, 7, 10, 1, 30, tzinfo=UTC)
MV_NODE = "mat.daily_sales"
INPUT = "s.transactions"


def _async_dsn(pg_dsn: str) -> str:
    return pg_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)


@pytest_asyncio.fixture
async def db(pg_dsn):
    """A real PG-backed control-plane Database scoped to a throwaway schema (isolated per test)."""
    schema = f"frx_{uuid.uuid4().hex[:12]}"
    engine = create_async_engine(_async_dsn(pg_dsn))
    async with engine.begin() as c:
        await c.execute(text(f'CREATE SCHEMA "{schema}"'))
        await c.execute(text(f'SET search_path TO "{schema}"'))
        await c.run_sync(lambda s: events.metadata.create_all(s, tables=_TABLES))
    try:
        yield Database(engine, name="frx", search_path=schema)
    finally:
        async with engine.begin() as c:
            await c.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()


async def _fan_in(db, node, source, n=1):
    async with db.acquire() as conn:
        for _ in range(n):
            e = await queue.post_event(conn, source_table=source, event_type="append")
            await queue.fan_out(conn, e, [node])


def _mv(db, *, expected, generate, lateness=0.0, business_day=False, holidays=frozenset()):
    cal = Calendar(name="g", version="v1", holidays=holidays)

    async def _generate(pending, *, prior_hash, ctx=None, forced=False):
        return generate

    return MVTableProcessor(
        MV_NODE,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: ["down.x"],
        db=db,
        name="box-1",
        generate=_generate,
        deadline_source=PeriodicCalendar(cal, "daily", allowed_lateness=lateness,
                                         business_day=business_day),
        expected_events=expected,
        freshness_of=make_db_freshness_of(db),  # the REAL DB-backed reader
    )


async def _posted(conn, node):
    return [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == node]


@asynccontextmanager
async def _at(monkeypatch, instant):
    import provisa.events.processor as pm

    monkeypatch.setattr(pm, "_now", lambda: instant)
    yield


# ---------------------------------------------------------------------------


async def test_fresh_input_seals(db, monkeypatch):
    """Input persisted fresh-through window.end → the contract passes and the window seals."""
    await _fan_in(db, MV_NODE, INPUT)
    async with db.acquire() as conn:
        await queue.record_refresh(conn, INPUT, at=WIN_END, ok=True)
    proc = _mv(db, expected=[INPUT], generate=("replace", {"rows": 5}, "h1"))
    async with _at(monkeypatch, AFTER_BOUNDARY):
        async with db.acquire() as conn:
            fired = await proc.process_pending(conn)
            assert fired is not None  # contract passed on the real reader → sealed
            posted = await _posted(conn, MV_NODE)
            assert [p["event_type"] for p in posted] == ["replace"]


async def test_trustworthy_zero_seals(db, monkeypatch):
    """A cleanly-refreshed input with zero rows is a trustworthy zero — still seals (REQ-961)."""
    await _fan_in(db, MV_NODE, INPUT)
    async with db.acquire() as conn:
        await queue.record_refresh(conn, INPUT, at=WIN_END, ok=True)
    proc = _mv(db, expected=[INPUT], generate=("replace", {"rows": 0}, "h0"))
    async with _at(monkeypatch, AFTER_BOUNDARY):
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is not None
            assert [p["event_type"] for p in await _posted(conn, MV_NODE)] == ["replace"]


async def test_stale_input_is_outage_warn_hold(db, monkeypatch):
    """Input last refreshed BEFORE window.end → outage: warn/hold, no seal, no ripple."""
    await _fan_in(db, MV_NODE, INPUT)
    async with db.acquire() as conn:
        await queue.record_refresh(conn, INPUT, at=WIN_END - timedelta(minutes=10), ok=True)
    proc = _mv(db, expected=[INPUT], generate=("replace", {"rows": 5}, "h1"))
    async with _at(monkeypatch, AFTER_BOUNDARY):
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None
            kinds = {r["source_table"]: r["event_type"] for r in await queue.read_since(conn, cursor=0)}
            assert kinds.get(MV_NODE) == "warn"  # not a silent skip
            # the claim is completed, not orphaned — the loop does not spin
            assert await queue.resume_claims(
                conn, dependent_table=MV_NODE, processor_name="box-1"
            ) == []


async def test_failed_input_is_outage(db, monkeypatch):
    """Input whose last refresh FAILED (ok=False) is not fresh → outage, even if timestamped late."""
    await _fan_in(db, MV_NODE, INPUT)
    async with db.acquire() as conn:
        await queue.record_refresh(conn, INPUT, at=WIN_END + timedelta(hours=1), ok=False)
    proc = _mv(db, expected=[INPUT], generate=("replace", {"rows": 5}, "h1"))
    async with _at(monkeypatch, AFTER_BOUNDARY):
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None
            kinds = {r["source_table"]: r["event_type"] for r in await queue.read_since(conn, cursor=0)}
            assert kinds.get(MV_NODE) == "warn"


async def test_never_refreshed_input_is_outage(db, monkeypatch):
    """An input with NO persisted refresh state is itself an outage — never assumed fresh (REQ-961)."""
    await _fan_in(db, MV_NODE, INPUT)  # no record_refresh for INPUT
    proc = _mv(db, expected=[INPUT], generate=("replace", {"rows": 5}, "h1"))
    async with _at(monkeypatch, AFTER_BOUNDARY):
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None
            kinds = {r["source_table"]: r["event_type"] for r in await queue.read_since(conn, cursor=0)}
            assert kinds.get(MV_NODE) == "warn"


async def test_multi_input_partial_outage_lists_only_stale(db, monkeypatch):
    """With several inputs, only those not fresh-through window.end are named in the outage warn."""
    await _fan_in(db, MV_NODE, "s.a")
    async with db.acquire() as conn:
        await queue.record_refresh(conn, "s.a", at=WIN_END, ok=True)  # fresh
        await queue.record_refresh(conn, "s.b", at=WIN_END - timedelta(hours=2), ok=True)  # stale
        # s.c never refreshed → also an outage
    proc = _mv(db, expected=["s.a", "s.b", "s.c"], generate=("replace", {"rows": 5}, "h1"))
    async with _at(monkeypatch, AFTER_BOUNDARY):
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is None
            warn = next(r for r in await queue.read_since(conn, cursor=0)
                        if r["source_table"] == MV_NODE and r["event_type"] == "warn")
            reasons = " ".join(warn["payload"]["reasons"])
            assert "s.b" in reasons and "s.c" in reasons and "s.a" not in reasons


async def test_source_land_stamps_freshness_state(db, monkeypatch):
    """The REAL source-land path stamps node_freshness_state.last_refresh_at — the reader then reads
    it. Closes the loop: land → persisted freshness → make_db_freshness_of subject fresh-through."""
    await _fan_in(db, "mat.src_replica", "upstream.raw")

    async def _land(pending, *, prior_hash, forced):
        return ("replace", {"rows": 3}, "sh1")

    src = SourceTableProcessor(
        "mat.src_replica",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: [],
        db=db,
        name="src-box",
        land=_land,
    )
    async with _at(monkeypatch, WIN_END):
        async with db.acquire() as conn:
            assert await src.process_pending(conn) is not None
    # the land wrote real freshness state; the real reader now sees it fresh-through WIN_END
    reader = make_db_freshness_of(db)
    subject = await reader("mat.src_replica")
    assert subject.last_refresh_ok() is True
    assert subject.last_refresh_at() == WIN_END.timestamp()
