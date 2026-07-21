# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3c1a72-4d69-4b28-9e15-3a7d0f2c6b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""pytest-bdd steps for REQ-1166 — repeating-calendar snapshot MVs.

Drives the REAL assembled pipeline (MVTableProcessor + make_mv_bitemporal_generate +
apply_bitemporal_append + the REQ-1162 append SQL) over an in-memory DuckDB store and a sqlite
control plane, on one event loop, so each boundary seal is real end to end.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import duckdb
import pytest
from pytest_bdd import given, when, then, scenarios
from sqlalchemy.ext.asyncio import create_async_engine
from types import SimpleNamespace

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

scenarios("../features/REQ-1166.feature")

UTC = timezone.utc
MV_NODE = "mat.snap"
COLS = ["id", "region", "amount"]
TARGET = '"memory"."main"."mv_snap"'


class _DuckEngine:
    dialect = "duckdb"

    def __init__(self, con):
        self.con = con

    async def execute_engine(self, sql: str):
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return SimpleNamespace(column_names=cols, rows=cur.fetchall())


@pytest.fixture
def loop():
    lp = asyncio.new_event_loop()
    yield lp
    lp.close()


@pytest.fixture
def ctx(loop, tmp_path) -> dict:
    return {"loop": loop, "tmp_path": tmp_path}


async def _setup(tmp_path, grain: str):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / (uuid.uuid4().hex + '.db')}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    db = Database(engine, name="cp")
    mv = MVDefinition(
        id="view-snap",
        source_tables=["base"],
        target_catalog="memory",
        target_schema="main",
        target_table="mv_snap",
        sql="SELECT id, region, amount FROM base",
        bitemporal=BitemporalSpec(key=("id",), mode="snapshot"),
        calendar="fy",
        grain=grain,
    )
    duck = _DuckEngine(con)

    async def _append(system_ts):
        await apply_bitemporal_append(duck, mv, system_ts=system_ts)

    proc = MVTableProcessor(
        MV_NODE,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: ["down.x"],
        db=db,
        name="snap-box",
        generate=make_mv_bitemporal_generate(_append),
        deadline_source=PeriodicCalendar(Calendar(name="fy", version="v1"), grain, allowed_lateness=0.0),
        expected_events=[],
        freshness_of=lambda _i: None,
    )
    return con, db, mv, proc, engine


async def _fire(con, db, proc, rows, now):
    import provisa.events.processor as pm

    con.execute("DELETE FROM base")
    for r in rows:
        con.execute("INSERT INTO base VALUES (?, ?, ?)", r)
    async with db.acquire() as conn:
        e = await queue.post_event(conn, source_table="base", event_type="replace", payload={})
        await queue.fan_out(conn, e, [MV_NODE])
    pm._now = lambda: now  # noqa: SLF001 — deterministic clock for the scenario
    async with db.acquire() as conn:
        return await proc.process_pending(conn)


def _run(ctx, coro):
    return ctx["loop"].run_until_complete(coro)


# -- monthly scenario --------------------------------------------------------


@given("a bitemporal snapshot MV on a monthly calendar")
def _given_monthly(ctx):
    con, db, mv, proc, engine = _run(ctx, _setup(ctx["tmp_path"], "monthly"))
    ctx.update(con=con, db=db, mv=mv, proc=proc, engine=engine)


@when("the January window closes with the month's data")
def _jan(ctx):
    ctx["jan_fired"] = _run(
        ctx, _fire(ctx["con"], ctx["db"], ctx["proc"], [(1, "west", 10), (2, "east", 20)],
                   datetime(2026, 2, 10, tzinfo=UTC))
    )


@when("the February window closes with changed data")
def _feb(ctx):
    ctx["feb_fired"] = _run(
        ctx, _fire(ctx["con"], ctx["db"], ctx["proc"], [(1, "west", 15), (3, "north", 30)],
                   datetime(2026, 3, 10, tzinfo=UTC))
    )


@then("each closed window sealed a distinct version stamped at its boundary")
def _distinct(ctx):
    assert ctx["jan_fired"] is not None and ctx["feb_fired"] is not None
    n = ctx["con"].execute(f"SELECT COUNT(DISTINCT sys_recorded_at) FROM {TARGET}").fetchone()[0]
    assert n == 2


def _asof(ctx, dt):
    sql = reconstruct_as_of_sql(TARGET, ctx["mv"].bitemporal, COLS, system_ts_literal(dt))
    return set(ctx["con"].execute(sql).fetchall())


@then("reading as-of the January boundary returns January's data")
def _asof_jan(ctx):
    assert _asof(ctx, datetime(2026, 2, 1, tzinfo=UTC)) == {(1, "west", 10), (2, "east", 20)}


@then("reading as-of the February boundary returns February's data")
def _asof_feb(ctx):
    assert _asof(ctx, datetime(2026, 3, 1, tzinfo=UTC)) == {(1, "west", 15), (3, "north", 30)}


# -- nth-weekday scenario ----------------------------------------------------


@given('a bitemporal snapshot MV on a "3rd Wednesday of month" calendar')
def _given_3we(ctx):
    con, db, mv, proc, engine = _run(ctx, _setup(ctx["tmp_path"], "3WE"))
    ctx.update(con=con, db=db, mv=mv, proc=proc, engine=engine)


@when("the third-Wednesday window closes")
def _3we_close(ctx):
    # 3rd Wed of Feb 2026 = Feb 18; firing on Feb 20 seals the [Jan 21, Feb 18) tile at Feb 18.
    ctx["fired"] = _run(
        ctx, _fire(ctx["con"], ctx["db"], ctx["proc"], [(1, "west", 10)],
                   datetime(2026, 2, 20, tzinfo=UTC))
    )


@then("a version is sealed addressed by that occurrence")
def _3we_sealed(ctx):
    assert ctx["fired"] is not None
    stamps = ctx["con"].execute(f"SELECT DISTINCT sys_recorded_at FROM {TARGET}").fetchall()
    assert stamps == [(datetime(2026, 2, 18),)]  # the 3rd-Wednesday boundary
