# Copyright (c) 2026 Kenneth Stott
# Canary: 8197b593-f9d7-4e77-93a8-9080a4a13cb4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: row_refresh's claim key must be namespaced away from the bare physical node.

A row_materialize table's ordinary whole-table federation strategy can independently resolve to
Strategy.MATERIALIZED (design doc section 3a), in which case boot.build_source_node_spec registers
a plain SourceTableProcessor claiming the bare node string. queue.claim has no event_type filter,
so a row_refresh event fanned to that SAME bare node would be claimable by that unrelated
processor. row_refresh_claim_node's namespacing (f"{node}::row_refresh") must keep the two claim
lanes disjoint against a real SQLite control plane -- this is the mechanical proof for the
justification in row_materialize_lifecycle.py's module docstring."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events
from provisa.events import queue
from provisa.federation.row_materialize_cdc import row_refresh_claim_node

_NOW = datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc)


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
async def test_row_refresh_claim_node_is_namespaced_not_bare():
    node = "public.orders"
    assert row_refresh_claim_node(node) != node
    assert node in row_refresh_claim_node(node)


@pytest.mark.asyncio
async def test_ordinary_processor_cannot_claim_a_row_refresh_event(tmp_path):
    node = "public.orders"
    async with _conn(tmp_path) as conn:
        eid = await queue.post_event(
            conn, source_table=node, event_type="row_refresh", payload={"keys": [[1]]}
        )
        await queue.fan_out(conn, eid, [row_refresh_claim_node(node)])

        # The table's own ordinary whole-table processor claims the BARE node -- it must get
        # nothing, because the row_refresh work item was never fanned there.
        ordinary_claim = await queue.claim(
            conn, dependent_table=node, processor_name="ordinary-source-processor", now=_NOW
        )
        assert ordinary_claim == []

        # The row_refresh processor claims its OWN namespaced lane and gets exactly this event.
        refresh_claim = await queue.claim(
            conn,
            dependent_table=row_refresh_claim_node(node),
            processor_name="row_refresh",
            now=_NOW,
        )
        assert refresh_claim == [eid]


@pytest.mark.asyncio
async def test_row_refresh_event_never_appears_on_bare_node_pending(tmp_path):
    node = "public.orders"
    async with _conn(tmp_path) as conn:
        eid = await queue.post_event(
            conn, source_table=node, event_type="delta", payload={"cursor": 1}
        )
        await queue.fan_out(conn, eid, [node])  # an ORDINARY refresh event, fanned to the bare node

        r_eid = await queue.post_event(
            conn, source_table=node, event_type="row_refresh", payload={"keys": [[2]]}
        )
        await queue.fan_out(conn, r_eid, [row_refresh_claim_node(node)])

        # The ordinary node's pending set contains only the ordinary event -- never the row_refresh
        # one, even though both events describe the same physical table.
        pending = await queue.peek_pending(conn, dependent_table=node)
        assert [p["event_id"] for p in pending] == [eid]

        refresh_pending = await queue.peek_pending(
            conn, dependent_table=row_refresh_claim_node(node)
        )
        assert [p["event_id"] for p in refresh_pending] == [r_eid]
