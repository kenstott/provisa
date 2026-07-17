# Copyright (c) 2026 Kenneth Stott
# Canary: 73455bd5-0963-4355-bb86-0b2aca759e81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-940: the injector action — probe → token-gate → post the change event + fan out."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.schema_org import event_status, events, node_freshness_state
from provisa.events import queue
from provisa.events.injector import check_node


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'q.db'}")
    async with engine.begin() as c:
        await c.run_sync(
            lambda s: events.metadata.create_all(
                s, tables=[events, event_status, node_freshness_state]
            )
        )
    try:
        async with Database(engine, name="q").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


def _probe(token: str | None = "tok"):
    """A transport that returns a fixed current token (str | None)."""

    async def p():
        return token

    return p


@pytest.mark.asyncio
async def test_unchanged_posts_nothing(tmp_path):
    async with _conn(tmp_path) as conn:
        # baseline token already persisted; the probe returns the same token → unchanged
        await queue.set_node_state(conn, "s.orders", probe_token="tok")
        eid = await check_node(
            conn,
            node="s.orders",
            change_signal="ttl",
            watermark_column=None,
            probe=_probe("tok"),
            dependents=["mv.daily"],
        )
        assert eid is None  # token-gated: no event, no wasted downstream work
        assert await queue.read_since(conn, cursor=0) == []


@pytest.mark.asyncio
async def test_none_token_degrades_to_ttl_and_posts(tmp_path):
    # a source with no token capability (None) is not gated — it re-posts every cadence (TTL degrade)
    async with _conn(tmp_path) as conn:
        eid = await check_node(
            conn,
            node="s.orders",
            change_signal="ttl",
            watermark_column=None,
            probe=_probe(None),
            dependents=[],
        )
        assert eid is not None
        rows = await queue.read_since(conn, cursor=0)
        assert len(rows) == 1 and rows[0]["event_type"] == "replace"


@pytest.mark.asyncio
async def test_changed_poll_watermark_posts_append_and_fans_out(tmp_path):
    async with _conn(tmp_path) as conn:
        eid = await check_node(
            conn,
            node="s.orders",
            change_signal="ttl_probe",
            watermark_column="updated_at",
            probe=_probe("t2"),
            dependents=["mv.daily", "mv.by_cust"],
        )
        rows = await queue.read_since(conn, cursor=0)
        assert len(rows) == 1 and rows[0]["event_type"] == "append"  # poll+watermark → append
        assert rows[0]["payload"] == {"token": "t2"}
        # the new token is persisted as the baseline for the next comparison
        assert (await queue.get_node_state(conn, "s.orders"))["probe_token"] == "t2"
        # fanned out to both dependents (claimable)
        assert await queue.claim(
            conn, dependent_table="mv.daily", processor_name="A", now=datetime.now(timezone.utc)
        ) == [eid]


@pytest.mark.asyncio
async def test_probe_type_watermark_drives_append_shape(tmp_path):
    # probe_type is the shape source of truth: watermark → append regardless of change_signal hints
    async with _conn(tmp_path) as conn:
        await check_node(
            conn,
            node="s.wm",
            change_signal="probe",
            watermark_column=None,
            probe=_probe("v1"),
            dependents=[],
            probe_type="watermark",
        )
        rows = await queue.read_since(conn, cursor=0)
        assert rows[0]["event_type"] == "append"


@pytest.mark.asyncio
async def test_event_kind_from_change_signal(tmp_path):
    async with _conn(tmp_path) as conn:
        # ttl, no watermark → replace
        await check_node(
            conn,
            node="s.a",
            change_signal="ttl",
            watermark_column=None,
            probe=_probe("tok"),
            dependents=[],
        )
        # push (kafka) → delta (upsert by PK)
        await check_node(
            conn,
            node="s.b",
            change_signal="kafka",
            watermark_column=None,
            probe=_probe("tok"),
            dependents=[],
        )
        kinds = {r["source_table"]: r["event_type"] for r in await queue.read_since(conn, cursor=0)}
        assert kinds == {"s.a": "replace", "s.b": "delta"}
