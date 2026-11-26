# Copyright (c) 2026 Kenneth Stott
# Canary: 1a2b3c4d-5e6f-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for Live Query Engine (Phase AM)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.live.engine import _build_incremental_sql
from provisa.live.outputs.sse import SSEFanout
from tests.unit.live_engine_doubles import (
    KEY,
    governed,
    make_engine,
    make_pool_with_conn,
    spec,
    sse_type,
)


# ---------------------------------------------------------------------------
# _build_incremental_sql
# ---------------------------------------------------------------------------


class TestBuildIncrementalSql:
    def test_no_watermark_adds_is_not_null(self):
        sql = "SELECT id, updated_at FROM orders"
        result = _build_incremental_sql(sql, "updated_at", None)
        assert "WHERE updated_at IS NOT NULL" in result

    def test_with_watermark_adds_gt_filter(self):
        sql = "SELECT id, updated_at FROM orders"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01'" in result

    def test_existing_where_ands_filter(self):
        sql = "SELECT id FROM orders WHERE status = 'active'"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE status = 'active'" in result
        assert "AND updated_at > '2026-01-01'" in result

    def test_strips_trailing_semicolon(self):
        sql = "SELECT id FROM orders;"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert not result.rstrip().endswith(";")

    def test_inserts_before_order_by(self):
        sql = "SELECT id FROM orders ORDER BY id"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01' ORDER BY id" in result

    def test_inserts_before_limit(self):
        sql = "SELECT id FROM orders LIMIT 100"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01' LIMIT 100" in result


# ---------------------------------------------------------------------------
# SSEFanout
# ---------------------------------------------------------------------------


class TestSSEFanout:
    @pytest.mark.asyncio
    async def test_subscribe_returns_queue(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        assert fanout.subscriber_count == 1
        assert isinstance(q, asyncio.Queue)

    @pytest.mark.asyncio
    async def test_send_delivers_to_subscribers(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        rows = [{"id": 1, "val": "x"}]
        await fanout.send(rows)
        received = q.get_nowait()
        assert received == rows

    @pytest.mark.asyncio
    async def test_send_empty_does_nothing(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        await fanout.send([])
        assert q.empty()

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_queue(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        fanout.unsubscribe(q)
        assert fanout.subscriber_count == 0

    @pytest.mark.asyncio
    async def test_close_sends_sentinel(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        await fanout.close()
        sentinel = q.get_nowait()
        assert sentinel is None

    @pytest.mark.asyncio
    async def test_multiple_subscribers_all_receive(self):
        fanout = SSEFanout("q1")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()
        rows = [{"id": 2}]
        await fanout.send(rows)
        assert q1.get_nowait() == rows
        assert q2.get_nowait() == rows


# ---------------------------------------------------------------------------
# LiveEngine
# ---------------------------------------------------------------------------


class TestLiveEngine:
    @pytest.mark.asyncio
    async def test_register_and_is_registered(self):
        engine = make_engine(started=True)
        engine.reconcile([spec()])
        assert engine.is_registered("q1")

    @pytest.mark.asyncio
    async def test_subscribe_returns_queue(self):
        engine = make_engine(started=True)
        engine.reconcile([spec()])
        with governed():
            q = await engine.subscribe("q1", KEY)
        assert isinstance(q, asyncio.Queue)

    @pytest.mark.asyncio
    async def test_subscribe_unknown_raises(self):
        engine = make_engine()
        with pytest.raises(KeyError, match="q_unknown"):
            await engine.subscribe("q_unknown", KEY)

    @pytest.mark.asyncio
    async def test_unregister_removes_job(self):
        engine = make_engine(started=True)
        engine.reconcile([spec()])
        engine.reconcile([])
        assert not engine.is_registered("q1")

    @pytest.mark.asyncio
    async def test_double_register_is_idempotent(self):
        sched = MagicMock()
        engine = make_engine(scheduler=sched, started=True)
        engine.reconcile([spec()])
        engine.reconcile([spec()])
        with governed():
            await engine.subscribe("q1", KEY)
            await engine.subscribe("q1", KEY)
        assert sched.add_job.call_count == 1

    @pytest.mark.asyncio
    async def test_poll_routes_through_trino_and_delivers(self):
        """A poll is a read through the one governed pipeline, as the subscriber's key; the
        org's store is used only for watermark bookkeeping."""
        conn_mock = AsyncMock()
        engine = make_engine(make_pool_with_conn(conn_mock), started=True)
        engine.reconcile([spec(watermark_column="updated_at")])
        with governed():
            q = await engine.subscribe("q1", KEY)
        with (
            governed([{"id": 1, "updated_at": "2026-01-02"}]) as reads,
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
        ):
            await engine._poll("q1", sse_type())
        assert [key for _sql, key, _p in reads] == [KEY]
        conn_mock.fetch.assert_not_called()
        assert q.get_nowait() == [{"id": 1, "updated_at": "2026-01-02"}]

    @pytest.mark.asyncio
    async def test_poll_without_trino_conn_raises_and_is_caught(self):
        # A failed read -> the poll logs and swallows (never crashes the scheduler).
        engine = make_engine(make_pool_with_conn(AsyncMock()), started=True)
        engine.reconcile([spec()])
        with governed():
            q = await engine.subscribe("q1", KEY)
        with (
            governed(RuntimeError("no engine bound")),
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
        ):
            await engine._poll("q1", sse_type())  # must not raise
        assert q.empty()


class TestReconcile:
    @pytest.mark.asyncio
    async def test_reconcile_registers_and_unregisters(self):
        engine = make_engine(started=True)
        engine.reconcile([spec("a", table_id=1)])
        assert engine.is_registered("a")

        # 'a' dropped, 'b' added
        engine.reconcile([spec("b", table_id=2)])
        assert not engine.is_registered("a")
        assert engine.is_registered("b")

    @pytest.mark.asyncio
    async def test_reconcile_unchanged_preserves_job_and_subscribers(self):
        engine = make_engine(started=True)
        engine.reconcile([spec("a")])
        with governed():
            q = await engine.subscribe("a", KEY)
        engine.reconcile([spec("a")])  # identical signature → no churn
        fanout = engine._groups[("a", sse_type())].output
        assert q in [sq for _, sq in fanout._queues]  # subscriber preserved

    @pytest.mark.asyncio
    async def test_reconcile_changed_signature_reregisters(self):
        engine = make_engine(started=True)
        engine.reconcile([spec("a", poll_interval=10)])
        with governed():
            q = await engine.subscribe("a", KEY)
        spawned: list = []
        with patch(
            "provisa.core.connection_loop.spawn_background", lambda coro, **_k: spawned.append(coro)
        ):
            engine.reconcile([spec("a", poll_interval=30)])
        for coro in spawned:
            await coro
        assert engine._specs["a"].poll_interval == 30
        # The changed spec ends the old subscribers' stream; a new subscriber starts afresh.
        assert ("a", sse_type()) not in engine._groups
        assert q.get_nowait() is None
