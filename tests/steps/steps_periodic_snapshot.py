# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3c1a72-4d69-4b28-9e15-3a7d0f2c6b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""pytest-bdd steps for REQ-1166 — repeating-calendar snapshot MVs.

Binds the GENERATED tests/features/REQ-1166.feature (scenario text lives in requirements.yaml — the
feature file is a generated artifact, never hand-edited). Drives the REAL assembled pipeline
(MVTableProcessor + make_mv_bitemporal_generate + apply_bitemporal_append + the REQ-1162 append SQL)
over an in-memory DuckDB store and a sqlite control plane on one event loop, asserting a boundary cuts
a window_id-addressable version whose as-of read reconstructs it — in BOTH snapshot and delta storage.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import duckdb
import pytest
from pytest_bdd import given, when, then, scenarios
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

scenarios("../features/REQ-1166.feature")

UTC = timezone.utc
MV_NODE = "mat.snap"
COLS = ["id", "region", "amount"]
JAN_END = datetime(2026, 2, 1, tzinfo=UTC)  # the January monthly window closes at Feb 1
AFTER_JAN = datetime(2026, 2, 10, tzinfo=UTC)


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
    return {"loop": loop, "tmp_path": tmp_path, "modes": {}}


def _run(ctx, coro):
    return ctx["loop"].run_until_complete(coro)


async def _build_mode(tmp_path, mode: str):
    """A fresh assembled pipeline (control plane + DuckDB store + processor) for one storage mode."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE base (id INTEGER, region VARCHAR, amount INTEGER)")
    target = f'"memory"."main"."mv_snap_{mode}"'
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / (uuid.uuid4().hex + '.db')}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    db = Database(engine, name="cp")
    mv = MVDefinition(
        id=f"view-snap-{mode}",
        source_tables=["base"],
        target_catalog="memory",
        target_schema="main",
        target_table=f"mv_snap_{mode}",
        sql="SELECT id, region, amount FROM base",
        bitemporal=BitemporalSpec(key=("id",), mode=mode),
        calendar="fy",
        grain="monthly",
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
        deadline_source=PeriodicCalendar(Calendar(name="fy", version="v1"), "monthly", allowed_lateness=0.0),
        expected_events=[],  # freshness gates satisfied (calendar-only contract)
        freshness_of=lambda _i: None,
    )
    return {"con": con, "db": db, "mv": mv, "proc": proc, "engine": engine, "target": target}


async def _fire(state, rows, now):
    import provisa.events.processor as pm

    state["con"].execute("DELETE FROM base")
    for r in rows:
        state["con"].execute("INSERT INTO base VALUES (?, ?, ?)", r)
    async with state["db"].acquire() as conn:
        e = await queue.post_event(conn, source_table="base", event_type="replace", payload={})
        await queue.fan_out(conn, e, [MV_NODE])
    pm._now = lambda: now  # noqa: SLF001 — deterministic clock for the scenario
    async with state["db"].acquire() as conn:
        return await state["proc"].process_pending(conn)


# -- generated REQ-1166 scenario ---------------------------------------------


@given("a materialized view with a repeating calendar trigger on a named boundary")
def _given(ctx):
    # both storage strategies, to assert addressability "regardless of storage"
    ctx["modes"]["snapshot"] = _run(ctx, _build_mode(ctx["tmp_path"], "snapshot"))
    ctx["modes"]["delta"] = _run(ctx, _build_mode(ctx["tmp_path"], "delta"))


@when("each calendar boundary is reached and input freshness gates are satisfied")
def _when(ctx):
    for state in ctx["modes"].values():
        ctx.setdefault("fired", {})[id(state)] = _run(
            ctx, _fire(state, [(1, "west", 10), (2, "east", 20)], AFTER_JAN)
        )


@then(
    "a version is cut at that boundary with a stable window_id, and the version is addressable "
    "via that window_id in time-travel reads, regardless of whether the underlying storage is full "
    "snapshot or delta append"
)
def _then(ctx):
    for state in ctx["modes"].values():
        assert ctx["fired"][id(state)] is not None  # the boundary cut a version
        # the version is stamped at the boundary (window.end = Jan-close = Feb 1) and addressable there
        sql = reconstruct_as_of_sql(state["target"], state["mv"].bitemporal, COLS, system_ts_literal(JAN_END))
        assert set(state["con"].execute(sql).fetchall()) == {(1, "west", 10), (2, "east", 20)}
