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
        return LiveEngine(tenant_db=pool)

    @pytest.mark.asyncio
    async def test_register_and_is_registered(self):
        engine = self._make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register(
            "q1",
            sql="SELECT id, updated_at FROM orders",
            watermark_column="updated_at",
            poll_interval=5,
        )
        assert engine.is_registered("q1")

    @pytest.mark.asyncio
    async def test_subscribe_returns_queue(self):
        engine = self._make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register(
            "q1",
            sql="SELECT id, updated_at FROM orders",
            watermark_column="updated_at",
            poll_interval=5,
        )
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

        engine.register(
            "q1",
            sql="SELECT id, updated_at FROM orders",
            watermark_column="updated_at",
            poll_interval=5,
        )
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

        engine.register(
            "q1",
            sql="SELECT id, updated_at FROM orders",
            watermark_column="updated_at",
            poll_interval=5,
        )
        engine.register(
            "q1",
            sql="SELECT id, updated_at FROM orders",
            watermark_column="updated_at",
            poll_interval=5,
        )
        assert mock_sched.add_job.call_count == 1

    @pytest.mark.asyncio
    async def test_poll_routes_through_trino_and_delivers(self):
        # Poll data comes from Trino (federated), not the PG pool. PG is used
        # only for watermark bookkeeping.
        from provisa.executor.trino import QueryResult

        # PG pool: watermark bookkeeping only.
        conn_mock = AsyncMock()
        pool = MagicMock()
        pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=conn_mock),
                __aexit__=AsyncMock(return_value=False),
            )
        )
        trino_conn = MagicMock()
        engine = LiveEngine(tenant_db=pool, trino_conn=trino_conn)

        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()

        engine.register(
            "q1",
            sql='SELECT * FROM cat."public"."orders"',
            watermark_column="updated_at",
            poll_interval=5,
        )
        q = engine.subscribe("q1")

        trino_result = QueryResult(rows=[(1, "2026-01-02")], column_names=["id", "updated_at"])
        exec_mock = MagicMock(return_value=trino_result)
        with (
            patch("provisa.executor.trino.execute_trino", exec_mock),
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
        ):
            await engine._poll("q1")

        # Data query ran against the Trino connection, never the PG pool.
        assert exec_mock.call_args.args[0] is trino_conn
        conn_mock.fetch.assert_not_called()
        received = q.get_nowait()
        assert received == [{"id": 1, "updated_at": "2026-01-02"}]

    @pytest.mark.asyncio
    async def test_poll_without_trino_conn_raises_and_is_caught(self):
        # No Trino connection → poll logs and swallows (never crashes scheduler).
        pool = MagicMock()
        pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=AsyncMock()),
                __aexit__=AsyncMock(return_value=False),
            )
        )
        engine = LiveEngine(tenant_db=pool, trino_conn=None)
        with patch("provisa.live.engine.AsyncIOScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_sched_cls.return_value = mock_sched
            await engine.start()
        engine.register(
            "q1",
            sql="SELECT * FROM cat.public.orders",
            watermark_column="updated_at",
            poll_interval=5,
        )
        q = engine.subscribe("q1")
        with (
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
        ):
            await engine._poll("q1")  # must not raise
        assert q.empty()


class TestReconcile:
    def _started_engine(self, stack) -> LiveEngine:
        engine = LiveEngine(tenant_db=AsyncMock(), trino_conn=MagicMock())
        mock_sched = MagicMock()
        mock_sched.add_job.return_value = MagicMock(id="live_x")
        stack.enter_context(patch("provisa.live.engine.AsyncIOScheduler", return_value=mock_sched))
        return engine

    @pytest.mark.asyncio
    async def test_reconcile_registers_and_unregisters(self):
        from contextlib import ExitStack
        from provisa.live.engine import LiveSpec

        with ExitStack() as stack:
            engine = self._started_engine(stack)
            await engine.start()

            engine.reconcile(
                [LiveSpec(query_id="a", sql="SELECT * FROM cat.s.a", watermark_column="ts")]
            )
            assert engine.is_registered("a")

            # 'a' dropped, 'b' added
            engine.reconcile(
                [LiveSpec(query_id="b", sql="SELECT * FROM cat.s.b", watermark_column="ts")]
            )
            assert not engine.is_registered("a")
            assert engine.is_registered("b")

    @pytest.mark.asyncio
    async def test_reconcile_unchanged_preserves_job_and_subscribers(self):
        from contextlib import ExitStack
        from provisa.live.engine import LiveSpec

        with ExitStack() as stack:
            engine = self._started_engine(stack)
            await engine.start()
            spec = LiveSpec(query_id="a", sql="SELECT * FROM cat.s.a", watermark_column="ts")
            engine.reconcile([spec])
            q = engine.subscribe("a")
            engine.reconcile([spec])  # identical signature → no churn
            # Same fanout queue survived (subscriber preserved).
            assert q in engine._jobs["a"].fanout._queues

    @pytest.mark.asyncio
    async def test_reconcile_changed_signature_reregisters(self):
        from contextlib import ExitStack
        from provisa.live.engine import LiveSpec

        with ExitStack() as stack:
            engine = self._started_engine(stack)
            await engine.start()
            engine.reconcile(
                [
                    LiveSpec(
                        query_id="a",
                        sql="SELECT * FROM cat.s.a",
                        watermark_column="ts",
                        poll_interval=10,
                    )
                ]
            )
            first_fanout = engine._jobs["a"].fanout
            engine.reconcile(
                [
                    LiveSpec(
                        query_id="a",
                        sql="SELECT * FROM cat.s.a",
                        watermark_column="ts",
                        poll_interval=30,
                    )
                ]
            )
            assert engine._jobs["a"].poll_interval == 30
            assert engine._jobs["a"].fanout is not first_fanout
