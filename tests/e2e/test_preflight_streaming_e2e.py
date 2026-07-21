# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the per-input streaming preflight gate through the WHOLE event-loop path (REQ-1165).

From source data to landed effect, every collaborator is real: a real DuckDB engine holds the MV's
input, ``make_streams_evaluator`` binds the gate at wiring, ``MVTableProcessor`` claims the fire and
runs ``make_mv_generate`` → the gate streams the input node's Arrow batches → CONTINUE lands the
recomputed rows in the real sqlite store, ABORT blocks the land and fans an ``error`` to dependents.
This exercises the exact chain the boot wiring assembles — no fakes, no gate stubs.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import duckdb
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.handlers import make_mv_generate
from provisa.events.processor import MVTableProcessor
from provisa.executor.result import QueryResult
from provisa.federation import store_writer
from provisa.federation.runtime import EngineCapability, UnsupportedCapabilityError
from provisa.mv.preflight_eval import make_streams_evaluator

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_COLS = [("id", "bigint"), ("region", "text")]
MV_NODE = "mat.summary"
_INPUT = "orders"


class _DuckEngine:
    dialect = "duckdb"

    def __init__(self, con, caps=frozenset({EngineCapability.ARROW_STREAM})):
        self.con = con
        self._caps = caps

    async def execute_engine(self, sql, *a, **k):
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return QueryResult(rows=[tuple(r) for r in cur.fetchall()], column_names=cols)

    def execute_engine_stream(self, sql, *a, **k):
        reader = self.con.execute(sql).to_arrow_reader(1)
        return reader.schema, reader

    def supports(self, cap):
        return cap in self._caps

    def require(self, cap):
        if cap not in self._caps:
            raise UnsupportedCapabilityError("duckdb", cap)


@asynccontextmanager
async def _cp(tmp_path):
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


# The gate inspects the INPUT (orders): abort if any order has a negative amount — even though the
# recomputed OUTPUT rows are perfectly valid. This is the whole point of a per-input gate.
_GATE_SRC = (
    "def preflight(streams, ctx):\n"
    "    if any(r['amount'] < 0 for r in streams['orders']):\n"
    "        return ctx.abort('negative amount in orders')\n"
    "    return ctx.ok()"
)


async def _run_query():
    # The MV's recomputed output — small, valid, independent of the input's row count.
    return [{"id": 1, "region": "east"}, {"id": 2, "region": "west"}]


def _processor(db, engine, store_dsn):
    generate = make_mv_generate(
        store_dsn,
        schema="",
        table="summary",
        columns=_COLS,
        run_query=_run_query,
        persist="replace",
        pk_columns=["id"],
    )
    evaluator = make_streams_evaluator(engine, _GATE_SRC, [_INPUT])
    return MVTableProcessor(
        MV_NODE,
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: ["down.x"],
        db=db,
        name="e2e-box",
        generate=generate,
        preprocess=evaluator,
    )


async def _fire(db, node):
    async with db.acquire() as conn:
        e = await queue.post_event(conn, source_table="up", event_type="replace")
        await queue.fan_out(conn, e, [node])


async def _events_for(conn, node):
    return [r for r in await queue.read_since(conn, cursor=0) if r["source_table"] == node]


async def test_continue_lands_output_when_input_is_clean(tmp_path):
    store_dsn = f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10),(2,5)) AS v(id, amount)")
    async with _cp(tmp_path) as db:
        await _fire(db, MV_NODE)
        proc = _processor(db, _DuckEngine(con), store_dsn)
        async with db.acquire() as conn:
            assert await proc.process_pending(conn) is not None
        async with store_writer.store_connection(store_dsn) as sc:
            rows = await sc.fetch("SELECT id, region FROM summary ORDER BY id")
        assert [(r[0], r[1]) for r in rows] == [(1, "east"), (2, "west")]  # gate passed → landed


async def test_abort_blocks_land_when_input_has_negative(tmp_path):
    store_dsn = f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1,10),(2,-4)) AS v(id, amount)")
    async with _cp(tmp_path) as db:
        await _fire(db, MV_NODE)
        proc = _processor(db, _DuckEngine(con), store_dsn)
        async with db.acquire() as conn:
            ev = await proc.process_pending(conn)
            assert ev is not None  # the error event id
            evs = await _events_for(conn, MV_NODE)
            assert [e["event_type"] for e in evs] == ["error"]  # gate aborted → no landed change
            assert "negative amount" in str(evs[0]["payload"])
            # fanned the error forward to dependents (poison propagation)
            claimed = await queue.claim(
                conn, dependent_table="down.x", processor_name="p", now=datetime.now(timezone.utc)
            )
            assert claimed  # dependent sees the poison
        # the store table was never created (nothing landed)
        async with store_writer.store_connection(store_dsn) as sc:
            from sqlalchemy.exc import OperationalError

            try:
                rows = await sc.fetch("SELECT id FROM summary")
            except OperationalError:
                rows = []
        assert rows == []
