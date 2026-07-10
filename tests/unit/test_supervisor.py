# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-941: the event-loop supervisor — a change propagates through the whole DAG in one drain,
end-to-end through real processors + queue + write face; plus reaper + cycle rejection."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue, supervisor
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.events.processor import MVTableProcessor, SourceTableProcessor

_COLS = [("id", "bigint"), ("status", "text")]


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


@pytest.mark.asyncio
async def test_change_propagates_through_dag_in_one_drain(tmp_path):
    store = f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"
    # lineage: mv.daily depends on s.orders  ->  dependents_of(s.orders) == [mv.daily]
    dep = supervisor.dependents_of({"mv.daily": "SELECT count(*) FROM s.orders"})

    async def fetch(_pending):
        return [{"id": 1, "status": "new"}, {"id": 2, "status": "sold"}]

    async def run_query():  # the engine's MV SELECT result (aggregate)
        return [{"id": 2, "status": "count"}]

    src = SourceTableProcessor(
        "s.orders",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=dep,
        name="src",
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
        db=None,  # set below
    )
    mv = MVTableProcessor(
        "mv.daily",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=dep,
        name="mv",
        generate=make_mv_generate(
            store, schema="", table="mv_daily", columns=_COLS, run_query=run_query
        ),
        db=None,
    )

    from provisa.federation import store_writer

    async with _cp(tmp_path) as cp:
        src._db = mv._db = cp
        # injector: s.orders changed → its landing work item (own acquire, committed + released)
        async with cp.acquire() as conn:
            eid = await queue.post_event(conn, source_table="s.orders", event_type="append")
            await queue.fan_out(conn, eid, ["s.orders"])

        # drain acquires its own connections per tick — propagates through the whole DAG
        rounds = await supervisor.drain(cp, [src, mv])
        assert rounds >= 1  # src lands + re-posts → mv generates (both within the catch-up)

        # both nodes landed through the write face
        async with store_writer.store_connection(store) as sconn:
            assert [r[0] for r in await sconn.fetch("SELECT id FROM orders ORDER BY id")] == [1, 2]
            assert [r[0] for r in await sconn.fetch("SELECT id FROM mv_daily")] == [2]
        # the DAG re-posted at each level (s.orders landed event, mv.daily generated event)
        async with cp.acquire() as conn:
            posted = {r["source_table"] for r in await queue.read_since(conn, cursor=0)}
        assert {"s.orders", "mv.daily"} <= posted


@pytest.mark.asyncio
async def test_reap_reclaims_stale(tmp_path):
    async with _cp(tmp_path) as cp, cp.acquire() as conn:
        eid = await queue.post_event(conn, source_table="s.a", event_type="replace")
        await queue.fan_out(conn, eid, ["mv.a"])
        t0 = datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc)
        await queue.claim(conn, dependent_table="mv.a", processor_name="dead", now=t0)
    # reaper with a short lease, evaluated well after t0 → reclaims
    assert await supervisor.reap(cp, lease_seconds=60, now=datetime.now(timezone.utc)) == 1


def test_dependents_of_rejects_cycle():
    with pytest.raises(ValueError, match="cycle"):
        supervisor.dependents_of({"mv.a": "SELECT * FROM mv.b", "mv.b": "SELECT * FROM mv.a"})
