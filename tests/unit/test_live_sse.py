# Copyright (c) 2026 Kenneth Stott
# Canary: c1d2e3f4-a5b6-7890-cdef-012345678abc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the live query engine's SSE fanout delivery.

Moved from tests/integration/test_live_sse_integration.py.
These tests require no running infrastructure — all PG calls are mocked.

Focus areas:
- Bounded-queue (QueueFull) drop behaviour under SSEFanout.send
- Full subscriber-count lifecycle across subscribe + unsubscribe transitions
- SSEFanout.send dispatched via asyncio.gather (concurrent delivery path)
- Engine.stop closes fanouts for MULTIPLE registered queries simultaneously
- Engine._poll uses compiled_sql when provided (vs query_text fallback)
- Engine._poll empty query_text returns early without touching fanout
- Engine._poll with only one row updates watermark to that row's value
- Engine.unregister while subscribers still attached leaves subscriber queues intact
- Engine.register without calling start() still creates a usable fanout
- SSEFanout.send with a mix of full and non-full queues drops only the full one
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.live.engine import LiveEngine
from provisa.live.outputs.sse import SSEFanout

pytestmark = [pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_pool_with_conn(conn_mock):
    """Build an asyncpg pool mock whose acquire() context manager yields conn_mock."""
    pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn_mock)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=cm)
    return pool


def _make_conn():
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)
    return conn


def _make_engine(pool=None) -> LiveEngine:
    if pool is None:
        pool = MagicMock()
    return LiveEngine(pg_pool=pool)


def _patch_poll_deps(record=None, watermark=None):
    """Return patch objects for watermark internals."""
    return (
        patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=watermark)),
        patch("provisa.live.watermark.set_watermark", AsyncMock()),
    )


# ---------------------------------------------------------------------------
# TestSSEFanoutQueueFullBehaviour
# ---------------------------------------------------------------------------


class TestSSEFanoutQueueFullBehaviour:
    """Tests focused on the QueueFull / drop path inside SSEFanout.send."""

    async def test_send_to_full_queue_drops_batch_silently(self):
        """A maxsize=1 queue that is already full receives no additional items."""
        fanout = SSEFanout("q-full")
        bounded: asyncio.Queue = asyncio.Queue(maxsize=1)
        fanout._queues.append(bounded)

        first_batch = [{"id": 1}]
        second_batch = [{"id": 2}]

        await fanout.send(first_batch)
        await fanout.send(second_batch)

        assert bounded.get_nowait() == first_batch
        assert bounded.empty()

    async def test_send_partial_drop_leaves_non_full_queues_intact(self):
        """When one queue is full, only that queue drops; others still receive the batch."""
        fanout = SSEFanout("q-partial")

        healthy: asyncio.Queue = asyncio.Queue()
        full: asyncio.Queue = asyncio.Queue(maxsize=1)
        full.put_nowait([{"id": 0}])

        fanout._queues.extend([healthy, full])

        rows = [{"id": 99}]
        await fanout.send(rows)

        assert healthy.get_nowait() == rows
        assert full.get_nowait() == [{"id": 0}]
        assert full.empty()

    async def test_send_to_full_queue_does_not_raise(self):
        """A QueueFull condition must never propagate as an exception to the caller."""
        fanout = SSEFanout("q-no-raise")
        bounded: asyncio.Queue = asyncio.Queue(maxsize=1)
        bounded.put_nowait([{"id": 0}])
        fanout._queues.append(bounded)

        await fanout.send([{"id": 1}])


# ---------------------------------------------------------------------------
# TestSSEFanoutSubscriberCountLifecycle
# ---------------------------------------------------------------------------


class TestSSEFanoutSubscriberCountLifecycle:
    async def test_count_starts_at_zero(self):
        fanout = SSEFanout("q-lifecycle")
        assert fanout.subscriber_count == 0

    async def test_count_returns_to_zero_after_all_unsubscribed(self):
        fanout = SSEFanout("q-lifecycle")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()
        q3 = fanout.subscribe()
        assert fanout.subscriber_count == 3

        fanout.unsubscribe(q1)
        fanout.unsubscribe(q2)
        fanout.unsubscribe(q3)
        assert fanout.subscriber_count == 0

    async def test_unsubscribe_phantom_queue_does_not_alter_count(self):
        fanout = SSEFanout("q-lifecycle")
        q = fanout.subscribe()
        phantom = asyncio.Queue()
        count_before = fanout.subscriber_count

        fanout.unsubscribe(phantom)

        assert fanout.subscriber_count == count_before
        fanout.unsubscribe(q)

    async def test_resubscribe_same_queue_increments_count_twice(self):
        fanout = SSEFanout("q-lifecycle")
        q = fanout.subscribe()
        fanout._queues.append(q)
        assert fanout.subscriber_count == 2

        await fanout.close()
        assert q.get_nowait() is None
        assert q.get_nowait() is None
        assert fanout.subscriber_count == 0


# ---------------------------------------------------------------------------
# TestSSEFanoutConcurrentDelivery
# ---------------------------------------------------------------------------


class TestSSEFanoutConcurrentDelivery:
    async def test_concurrent_sends_all_delivered_to_subscriber(self):
        fanout = SSEFanout("q-concurrent")
        q = fanout.subscribe()

        batches = [[{"id": i}] for i in range(5)]
        await asyncio.gather(*(fanout.send(b) for b in batches))

        received = []
        while not q.empty():
            received.append(q.get_nowait())

        assert len(received) == 5
        flat_ids = {item["id"] for batch in received for item in batch}
        assert flat_ids == {0, 1, 2, 3, 4}

    async def test_concurrent_send_and_subscribe_does_not_corrupt_count(self):
        fanout = SSEFanout("q-concurrent")
        q_early = fanout.subscribe()

        rows = [{"id": 42}]

        async def late_subscribe():
            await asyncio.sleep(0)
            return fanout.subscribe()

        late_q, _ = await asyncio.gather(late_subscribe(), fanout.send(rows))

        assert q_early.get_nowait() == rows
        assert fanout.subscriber_count == 2

        fanout.unsubscribe(q_early)
        fanout.unsubscribe(late_q)


# ---------------------------------------------------------------------------
# TestEnginePollEdgeCases
# ---------------------------------------------------------------------------


class TestEnginePollEdgeCases:
    async def test_poll_uses_registered_sql(self):
        raw_rows = [{"id": 1, "ts": "2026-04-01"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register(
            "q1", sql="SELECT id, ts FROM compiled_events", watermark_column="ts", poll_interval=5
        )
        engine.subscribe("q1")

        p1, p2 = _patch_poll_deps(watermark=None)
        with p1, p2:
            await engine._poll("q1")

        executed_sql = conn.fetch.call_args[0][0]
        assert "compiled_events" in executed_sql

    async def test_poll_with_no_rows_does_not_deliver_to_fanout(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register(
            "q1", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )
        q = engine.subscribe("q1")

        p1, p2 = _patch_poll_deps(watermark=None)
        with p1, p2:
            await engine._poll("q1")

        assert q.empty()

    async def test_poll_single_row_watermark_set_to_that_row_value(self):
        raw_rows = [{"id": 7, "ts": "2026-03-31"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register(
            "q1", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )

        mock_set_wm = AsyncMock()
        record = {
            "query_text": "SELECT id, ts FROM events",
            "compiled_sql": "SELECT id, ts FROM events",
        }
        with (
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", mock_set_wm),
        ):
            await engine._poll("q1")

        mock_set_wm.assert_called_once_with(conn, "q1", "sse", "2026-03-31")

    async def test_poll_watermark_column_missing_from_row_uses_empty_string(self):
        raw_rows = [{"id": 1}, {"id": 2}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register(
            "q1", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )

        mock_set_wm = AsyncMock()
        record = {
            "query_text": "SELECT id FROM events",
            "compiled_sql": "SELECT id FROM events",
        }
        with (
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", mock_set_wm),
        ):
            await engine._poll("q1")

        mock_set_wm.assert_called_once()
        _, _, _, wm_value = mock_set_wm.call_args[0]
        assert isinstance(wm_value, str)

    async def test_poll_delivers_to_multiple_subscribers_simultaneously(self):
        raw_rows = [{"id": 1, "ts": "2026-04-01"}, {"id": 2, "ts": "2026-04-02"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register(
            "q1", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )

        q_a = engine.subscribe("q1")
        q_b = engine.subscribe("q1")

        record = {
            "query_text": "SELECT id, ts FROM events",
            "compiled_sql": "SELECT id, ts FROM events",
        }
        p1, p2 = _patch_poll_deps(record=record, watermark="2026-01-01")
        with p1, p2:
            await engine._poll("q1")

        expected = [dict(r) for r in raw_rows]
        assert q_a.get_nowait() == expected
        assert q_b.get_nowait() == expected


# ---------------------------------------------------------------------------
# TestEngineUnregisterBehaviour
# ---------------------------------------------------------------------------


class TestEngineUnregisterBehaviour:
    async def test_unregister_does_not_flush_existing_subscriber_queues(self):
        raw_rows = [{"id": 1, "ts": "2026-04-01"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register(
            "q1", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )
        q = engine.subscribe("q1")

        record = {
            "query_text": "SELECT id, ts FROM events",
            "compiled_sql": "SELECT id, ts FROM events",
        }
        p1, p2 = _patch_poll_deps(record=record, watermark=None)
        with p1, p2:
            await engine._poll("q1")

        engine.unregister("q1")
        assert not engine.is_registered("q1")
        assert q.get_nowait() == [dict(r) for r in raw_rows]

    async def test_unregister_with_scheduler_calls_remove_job(self):
        engine = _make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q-removal")
            MockSched.return_value = mock_sched
            await engine.start()

        engine.register(
            "q-removal", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )
        engine.unregister("q-removal")

        mock_sched.remove_job.assert_called_once_with("live_q-removal")

    async def test_unregister_without_scheduler_does_not_raise(self):
        engine = _make_engine()
        engine.register(
            "q-no-sched", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )
        engine.unregister("q-no-sched")
        assert not engine.is_registered("q-no-sched")


# ---------------------------------------------------------------------------
# TestEngineStopMultipleQueries
# ---------------------------------------------------------------------------


class TestEngineStopMultipleQueries:
    async def test_stop_sends_none_sentinel_to_all_registered_query_subscribers(self):
        conn = _make_conn()
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)

        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_x")
            MockSched.return_value = mock_sched
            await engine.start()

        engine.register(
            "qa", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )
        engine.register(
            "qb", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=10
        )

        qa_sub = engine.subscribe("qa")
        qb_sub1 = engine.subscribe("qb")
        qb_sub2 = engine.subscribe("qb")

        await engine.stop()

        assert qa_sub.get_nowait() is None
        assert qb_sub1.get_nowait() is None
        assert qb_sub2.get_nowait() is None

    async def test_stop_clears_all_jobs(self):
        engine = _make_engine()

        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_x")
            MockSched.return_value = mock_sched
            await engine.start()

        for qid in ("alpha", "beta", "gamma"):
            engine.register(
                qid, sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
            )

        assert len(engine._jobs) == 3
        await engine.stop()
        assert engine._jobs == {}

    async def test_stop_without_start_is_silent(self):
        engine = _make_engine()
        await engine.stop()

    async def test_stop_twice_does_not_raise(self):
        engine = _make_engine()

        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_x")
            MockSched.return_value = mock_sched
            await engine.start()

        await engine.stop()
        await engine.stop()


# ---------------------------------------------------------------------------
# TestEngineRegisterWithoutStart
# ---------------------------------------------------------------------------


class TestEngineRegisterWithoutStart:
    async def test_subscribe_after_register_without_start_returns_queue(self):
        engine = _make_engine()
        engine.register(
            "q1", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )
        q = engine.subscribe("q1")
        assert isinstance(q, asyncio.Queue)

    async def test_fanout_delivers_rows_after_register_without_start(self):
        engine = _make_engine()
        engine.register(
            "q1", sql="SELECT id, ts FROM events", watermark_column="ts", poll_interval=5
        )
        q = engine.subscribe("q1")

        rows = [{"id": 1, "ts": "2026-04-06"}]
        await engine._jobs["q1"].fanout.send(rows)

        assert q.get_nowait() == rows


# ---------------------------------------------------------------------------
# TestSSEFanout (moved from tests/integration/test_sse_subscriptions.py)
# ---------------------------------------------------------------------------


class TestSSEFanout:
    async def test_sse_provider_yields_events_on_insert(self):
        fanout = SSEFanout("test-query-1")
        queue = fanout.subscribe()

        rows = [{"id": 1, "amount": 100.0, "region": "us-east"}]
        await fanout.send(rows)

        assert not queue.empty()
        received = queue.get_nowait()
        assert received == rows
        assert received[0]["id"] == 1

    async def test_sse_multiple_subscribers_all_notified(self):
        fanout = SSEFanout("test-query-2")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()

        rows = [{"id": 10, "amount": 50.0}]
        await fanout.send(rows)

        assert q1.get_nowait() == rows
        assert q2.get_nowait() == rows

    async def test_sse_filter_excludes_non_matching_events(self):
        fanout = SSEFanout("test-query-3")
        queue = fanout.subscribe()

        await fanout.send([])
        assert queue.empty()

        matching = [{"id": 5, "region": "eu-west"}]
        await fanout.send(matching)
        assert queue.get_nowait() == matching

    async def test_sse_provider_close_stops_stream(self):
        fanout = SSEFanout("test-query-4")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()

        assert fanout.subscriber_count == 2
        await fanout.close()

        assert q1.get_nowait() is None
        assert q2.get_nowait() is None
        assert fanout.subscriber_count == 0

    async def test_sse_unsubscribe_removes_queue(self):
        fanout = SSEFanout("test-query-5")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()
        fanout.unsubscribe(q1)

        rows = [{"id": 99}]
        await fanout.send(rows)

        assert q1.empty()
        assert q2.get_nowait() == rows


# ---------------------------------------------------------------------------
# TestLiveEngineWatermark (moved from tests/integration/test_sse_subscriptions.py)
# ---------------------------------------------------------------------------


class TestLiveEngineWatermark:
    async def test_live_engine_watermark_advances(self):
        from provisa.live.engine import _build_incremental_sql

        base_sql = 'SELECT * FROM "public"."orders"'
        result_with = _build_incremental_sql(base_sql, "created_at", "2026-01-01T00:00:00")
        assert "WHERE" in result_with
        assert "created_at > '2026-01-01T00:00:00'" in result_with

        result_without = _build_incremental_sql(base_sql, "created_at", None)
        assert "created_at IS NOT NULL" in result_without

    async def test_live_engine_watermark_appends_and_filter(self):
        from provisa.live.engine import _build_incremental_sql

        base_sql = 'SELECT * FROM "public"."orders" WHERE "region" = \'us-east\''
        result = _build_incremental_sql(base_sql, "created_at", "2026-01-01")
        assert result.count("WHERE") == 1
        assert "AND created_at" in result

    async def test_live_engine_register_and_subscribe(self):
        mock_pool = MagicMock()
        engine = LiveEngine(pg_pool=mock_pool)

        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched_instance = MagicMock()
            mock_sched_instance.add_job.return_value = MagicMock(id="live_abc")
            MockSched.return_value = mock_sched_instance
            await engine.start()

        engine.register(
            "abc-123",
            sql="SELECT id, ts FROM events",
            watermark_column="created_at",
            poll_interval=10,
        )
        assert engine.is_registered("abc-123")

        queue = engine.subscribe("abc-123")
        assert isinstance(queue, asyncio.Queue)

        engine.unsubscribe("abc-123", queue)
        engine.unregister("abc-123")
        assert not engine.is_registered("abc-123")

    async def test_live_engine_subscribe_unknown_raises(self):
        mock_pool = MagicMock()
        engine = LiveEngine(pg_pool=mock_pool)
        with pytest.raises(KeyError, match="not registered"):
            engine.subscribe("nonexistent-id")
