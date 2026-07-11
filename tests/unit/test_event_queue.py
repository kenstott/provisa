# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-940/941: the event-substrate queue — post/fan-out/claim/heartbeat/complete/reclaim/read,
end-to-end against a real SQLite control plane."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events
from provisa.events import queue

_T0 = datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc)


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'q.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: events.metadata.create_all(s, tables=[events, event_status]))
    try:
        async with Database(engine, name="q").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_post_validates_event_type(tmp_path):
    async with _conn(tmp_path) as conn:
        with pytest.raises(ValueError, match="invalid event_type"):
            await queue.post_event(conn, source_table="s.t", event_type="bogus")


@pytest.mark.asyncio
async def test_full_flow_post_fanout_claim_complete(tmp_path):
    async with _conn(tmp_path) as conn:
        # injector posts a change on source s.orders
        eid = await queue.post_event(
            conn, source_table="s.orders", event_type="append", payload={"cursor": 42}
        )
        assert isinstance(eid, int)
        # dispatcher fans out to the two MVs that depend on s.orders (from lineage)
        assert await queue.fan_out(conn, eid, ["mv.daily", "mv.by_customer"]) == 2
        # fan_out is idempotent — a retry doesn't double-insert
        await queue.fan_out(conn, eid, ["mv.daily", "mv.by_customer"])

        # two DIFFERENT table processors claim in parallel — each gets its own work item
        a = await queue.claim(conn, dependent_table="mv.daily", processor_name="box-A", now=_T0)
        b = await queue.claim(
            conn, dependent_table="mv.by_customer", processor_name="box-B", now=_T0
        )
        assert a == [eid] and b == [eid]
        # same table, second claimant gets nothing (already claimed)
        assert (
            await queue.claim(conn, dependent_table="mv.daily", processor_name="box-C", now=_T0)
            == []
        )

        await queue.heartbeat(conn, dependent_table="mv.daily", processor_name="box-A", now=_T0)
        assert await queue.complete(
            conn, event_id=eid, dependent_table="mv.daily", processor_name="box-A", now=_T0
        )  # REQ-959: owner CAS succeeds
        # completed work is not re-claimable
        assert (
            await queue.claim(conn, dependent_table="mv.daily", processor_name="box-A", now=_T0)
            == []
        )


@pytest.mark.asyncio
async def test_reclaim_stale_lease(tmp_path):
    async with _conn(tmp_path) as conn:
        eid = await queue.post_event(conn, source_table="s.orders", event_type="replace")
        await queue.fan_out(conn, eid, ["mv.daily"])
        # claim, then let the lease go stale (heartbeat at _T0, reaper cutoff later)
        await queue.claim(conn, dependent_table="mv.daily", processor_name="dead-box", now=_T0)
        assert (
            await queue.reclaim(
                conn, now=_T0 + timedelta(minutes=5), heartbeat_cutoff=_T0 + timedelta(minutes=5)
            )
            == 1
        )
        # reclaimed → any processor can pick it up again
        assert await queue.claim(
            conn, dependent_table="mv.daily", processor_name="box-Z", now=_T0
        ) == [eid]


@pytest.mark.asyncio
async def test_repeater_reads_by_cursor(tmp_path):
    async with _conn(tmp_path) as conn:
        e1 = await queue.post_event(conn, source_table="s.a", event_type="delta")
        e2 = await queue.post_event(
            conn, source_table="s.b", event_type="warn", payload={"msg": "x"}
        )
        rows = await queue.read_since(conn, cursor=0)
        assert [r["id"] for r in rows] == [e1, e2]
        assert rows[1]["event_type"] == "warn" and rows[1]["payload"] == {"msg": "x"}
        # cursor advances — a repeater only sees new events
        assert await queue.read_since(conn, cursor=e2) == []


@pytest.mark.asyncio
async def test_req959_ownership_cas_blocks_stale_owner(tmp_path):
    """A returning owner whose claim was reclaimed by a peer cannot complete it: the ownership CAS
    in complete() matches only while THIS processor still owns the row (REQ-959)."""
    async with _conn(tmp_path) as conn:
        eid = await queue.post_event(conn, source_table="s.o", event_type="replace")
        await queue.fan_out(conn, eid, ["mv.x"])
        # box-1 claims, then its lease is reclaimed (crash) and box-2 takes over.
        await queue.claim(conn, dependent_table="mv.x", processor_name="box-1", now=_T0)
        await queue.reclaim(
            conn, now=_T0 + timedelta(minutes=5), heartbeat_cutoff=_T0 + timedelta(minutes=5)
        )
        assert await queue.claim(
            conn, dependent_table="mv.x", processor_name="box-2", now=_T0 + timedelta(minutes=6)
        ) == [eid]
        # The stale owner's completion CAS FAILS (box-2 owns it now) → False, must abort.
        assert (
            await queue.complete(
                conn, event_id=eid, dependent_table="mv.x", processor_name="box-1", now=_T0
            )
            is False
        )
        # The real owner completes it.
        assert (
            await queue.complete(
                conn, event_id=eid, dependent_table="mv.x", processor_name="box-2", now=_T0
            )
            is True
        )


@pytest.mark.asyncio
async def test_req959_deadline_reclaims_stuck_but_alive(tmp_path):
    """A per-claim deadline reclaims a stuck-but-alive owner whose HEARTBEAT is still fresh — the
    failure a heartbeat structurally cannot catch (REQ-959)."""
    async with _conn(tmp_path) as conn:
        eid = await queue.post_event(conn, source_table="s.o", event_type="replace")
        await queue.fan_out(conn, eid, ["mv.x"])
        # claim at _T0 with a deadline at _T0; heartbeat_at = _T0 (fresh).
        await queue.claim(
            conn, dependent_table="mv.x", processor_name="wedged", now=_T0, deadline=_T0
        )
        # heartbeat_cutoff far in the PAST → the heartbeat branch cannot fire (lease still valid);
        # only the deadline+grace branch can reclaim.
        reclaimed = await queue.reclaim(
            conn,
            now=_T0 + timedelta(minutes=10),
            heartbeat_cutoff=_T0 - timedelta(minutes=10),
            grace_seconds=0.0,
        )
        assert reclaimed == 1  # deadline path reclaimed the stuck-but-alive owner
        assert await queue.claim(
            conn, dependent_table="mv.x", processor_name="peer", now=_T0 + timedelta(minutes=11)
        ) == [eid]
