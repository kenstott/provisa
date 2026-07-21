# Copyright (c) 2026 Kenneth Stott
# Canary: 2e9d7b41-6c38-4a15-9f02-8b1e5c3d7a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the ASSEMBLED periodic-snapshot pipeline over real DuckDB (REQ-961/1162/1166/1167).

Every collaborator is real — no fakes but the tiny DuckDB engine shim (which runs real SQL): the
event-loop ``MVTableProcessor`` claims/coalesces the fire, the calendar ``PeriodicCalendar`` peg it
seals, ``make_mv_bitemporal_generate`` → ``apply_bitemporal_append`` → the REQ-1162 append SQL land a
version stamped by ``window.end``, and ``reconstruct_as_of_sql`` reads each sealed period back. This
proves the runtime binding the plumbing just wired: a calendar boundary cuts an addressable,
time-travel-able snapshot.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import duckdb
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.calendars import Calendar
from provisa.events.deadlines import PeriodicCalendar
from provisa.events.handlers import make_mv_bitemporal_generate
from provisa.events.processor import MVTableProcessor
from provisa.mv.bitemporal import BitemporalSpec, reconstruct_as_of_sql, system_ts_literal
from provisa.mv.models import MVDefinition
from provisa.mv.refresh import apply_bitemporal_append

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

UTC = timezone.utc
MV_NODE = "mat.month_snap"
COLS = ["id", "region", "amount"]
TARGET = '"memory"."main"."mv_month_snap"'


class _DuckEngine:
    """A minimal engine shim over an in-memory DuckDB connection (execute_engine + dialect)."""

    dialect = "duckdb"

    def __init__(self, con):
        self.con = con

    async def execute_engine(self, sql: str):
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return SimpleNamespace(column_names=cols, rows=cur.fetchall())


@asynccontextmanager
async def _control_plane(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cp.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    try:
        yield Database(engine, name="cp")
    finally:
        await engine.dispose()


def _mv() -> MVDefinition:
    return MVDefinition(
        id="view-month_snap",
        source_tables=["base"],
        target_catalog="memory",
        target_schema="main",
        target_table="mv_month_snap",
        sql="SELECT id, region, amount FROM base",
        bitemporal=BitemporalSpec(key=("id",), mode="snapshot"),
        calendar="fy",
        grain="monthly",
    )


def _processor(db, engine, mv):
    async def _append(system_ts):
        await apply_bitemporal_append(engine, mv, system_ts=system_ts)

    return MVTableProcessor(
        MV_NODE,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: ["down.x"],
        db=db,
        name="snap-box",
        generate=make_mv_bitemporal_generate(_append),
        deadline_source=PeriodicCalendar(
            Calendar(name="fy", version="v1"), "monthly", allowed_lateness=0.0
        ),
        expected_events=[],  # calendar-only preflight: isolate the append binding
        freshness_of=lambda _i: None,
    )


async def _fire(db, proc, engine, con, *, base_rows, now, monkeypatch):
    """Set the base data, fan a trigger into the MV, and process one fire at ``now``."""
    import provisa.events.processor as pm

    con.execute("DELETE FROM base")
    for r in base_rows:
        con.execute("INSERT INTO base VALUES (?, ?, ?)", r)
    async with db.acquire() as conn:
        e = await queue.post_event(conn, source_table="base", event_type="replace", payload={})
        await queue.fan_out(conn, e, [MV_NODE])
    monkeypatch.setattr(pm, "_now", lambda: now)
    async with db.acquire() as conn:
        return await proc.process_pending(conn)


def _as_of(con, spec, ts: datetime) -> set:
    sql = reconstruct_as_of_sql(TARGET, spec, COLS, system_ts_literal(ts))
    return set(con.execute(sql).fetchall())


async def test_calendar_boundary_seals_addressable_snapshots(tmp_path, monkeypatch):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    engine = _DuckEngine(con)
    mv = _mv()
    async with _control_plane(tmp_path) as db:
        proc = _processor(db, engine, mv)

        # January closes at Feb 1 → seal the January snapshot, stamped 2026-02-01.
        fired = await _fire(
            db, proc, engine, con,
            base_rows=[(1, "west", 10), (2, "east", 20)],
            now=datetime(2026, 2, 10, tzinfo=UTC), monkeypatch=monkeypatch,
        )
        assert fired is not None  # the periodic fire sealed a version

        # February closes at Mar 1 → seal a second, different snapshot.
        fired2 = await _fire(
            db, proc, engine, con,
            base_rows=[(1, "west", 15), (3, "north", 30)],
            now=datetime(2026, 3, 10, tzinfo=UTC), monkeypatch=monkeypatch,
        )
        assert fired2 is not None

    spec = mv.bitemporal
    # as-of each boundary → exactly that month's sealed dataset (calendar-addressed time travel)
    assert _as_of(con, spec, datetime(2026, 2, 1, tzinfo=UTC)) == {(1, "west", 10), (2, "east", 20)}
    assert _as_of(con, spec, datetime(2026, 3, 1, tzinfo=UTC)) == {(1, "west", 15), (3, "north", 30)}
    # history is preserved (append-only): two distinct versions live in the log
    versions = con.execute(f"SELECT COUNT(DISTINCT sys_recorded_at) FROM {TARGET}").fetchone()[0]
    assert versions == 2


async def test_downstream_ripple_on_each_seal(tmp_path, monkeypatch):
    """Each sealed boundary re-posts a replace so dependents recompute (REQ-965 fan-out)."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    engine = _DuckEngine(con)
    mv = _mv()
    async with _control_plane(tmp_path) as db:
        proc = _processor(db, engine, mv)
        await _fire(
            db, proc, engine, con, base_rows=[(1, "west", 10)],
            now=datetime(2026, 2, 10, tzinfo=UTC), monkeypatch=monkeypatch,
        )
        async with db.acquire() as conn:
            posted = [
                r for r in await queue.read_since(conn, cursor=0)
                if r["source_table"] == MV_NODE
            ]
    assert [p["event_type"] for p in posted] == ["replace"]  # sealed → ripples once
