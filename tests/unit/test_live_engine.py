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

from provisa.live.engine import LiveEngine, _build_incremental_sql
from provisa.live.outputs.sse import SSEFanout


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
    def _make_engine(self) -> LiveEngine:
        pool = AsyncMock()
        return LiveEngine(pg_pool=pool)

    @pytest.mark.asyncio
    async def test_register_and_is_registered(self):
        engine = self._make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        assert engine.is_registered("q1")

    @pytest.mark.asyncio
    async def test_subscribe_returns_queue(self):
        engine = self._make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        q = engine.subscribe("q1")
        assert isinstance(q, asyncio.Queue)

    @pytest.mark.asyncio
    async def test_subscribe_unknown_raises(self):
        engine = self._make_engine()
        with pytest.raises(KeyError, match="q_unknown"):
            engine.subscribe("q_unknown")

    @pytest.mark.asyncio
    async def test_unregister_removes_job(self):
        engine = self._make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        engine.unregister("q1")
        assert not engine.is_registered("q1")

    @pytest.mark.asyncio
    async def test_double_register_is_idempotent(self):
        engine = self._make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        assert mock_sched.add_job.call_count == 1

    @pytest.mark.asyncio
    async def test_poll_delivers_to_fanout(self):
        mock_rows = [{"id": 1, "updated_at": "2026-01-02"}]

        # Build a proper async context manager mock for pool.acquire()
        conn_mock = AsyncMock()
        conn_mock.fetch = AsyncMock(return_value=mock_rows)

        pool = MagicMock()
        pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn_mock),
            __aexit__=AsyncMock(return_value=False),
        ))

        engine = LiveEngine(pg_pool=pool)

        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        q = engine.subscribe("q1")

        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)), \
             patch("provisa.live.watermark.set_watermark", AsyncMock()), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value={
                 "query_text": "SELECT id, updated_at FROM orders",
                 "compiled_sql": "SELECT id, updated_at FROM orders",
             })):
            await engine._poll("q1")

        received = q.get_nowait()
        assert received == [dict(r) for r in mock_rows]
