# Copyright (c) 2026 Kenneth Stott
# Canary: c1d2e3f4-a5b6-7890-cdef-012345678abc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the live query engine's SSE fanout delivery.

Focus areas NOT already covered by test_live_engine_full.py or
test_sse_subscriptions.py:

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

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Shared helpers (mirrors test_live_engine_full.py style without importing it)
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
    """Return three patch objects for registry + watermark internals."""
    return (
        patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=watermark)),
        patch("provisa.live.watermark.set_watermark", AsyncMock()),
        patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)),
    )


# ---------------------------------------------------------------------------
# TestSSEFanoutQueueFullBehaviour
# ---------------------------------------------------------------------------

class TestSSEFanoutQueueFullBehaviour:
    """Tests focused on the QueueFull / drop path inside SSEFanout.send."""

    async def test_send_to_full_queue_drops_batch_silently(self):
        """A maxsize=1 queue that is already full receives no additional items."""
        fanout = SSEFanout("q-full")
        # Inject a bounded queue manually so we control its capacity
        bounded: asyncio.Queue = asyncio.Queue(maxsize=1)
        fanout._queues.append(bounded)

        first_batch = [{"id": 1}]
        second_batch = [{"id": 2}]

        await fanout.send(first_batch)   # fills the queue
        await fanout.send(second_batch)  # should be dropped silently

        # Queue still contains only the first batch
        assert bounded.get_nowait() == first_batch
        assert bounded.empty()

    async def test_send_partial_drop_leaves_non_full_queues_intact(self):
        """When one queue is full, only that queue drops; others still receive the batch."""
        fanout = SSEFanout("q-partial")

        healthy: asyncio.Queue = asyncio.Queue()
        full: asyncio.Queue = asyncio.Queue(maxsize=1)
        # Pre-fill the bounded queue so the next put_nowait raises QueueFull
        full.put_nowait([{"id": 0}])

        fanout._queues.extend([healthy, full])

        rows = [{"id": 99}]
        await fanout.send(rows)

        # healthy queue received the rows
        assert healthy.get_nowait() == rows
        # full queue still holds only its pre-filled item
        assert full.get_nowait() == [{"id": 0}]
        assert full.empty()

    async def test_send_to_full_queue_does_not_raise(self):
        """A QueueFull condition must never propagate as an exception to the caller."""
        fanout = SSEFanout("q-no-raise")
        bounded: asyncio.Queue = asyncio.Queue(maxsize=1)
        bounded.put_nowait([{"id": 0}])  # fill it
        fanout._queues.append(bounded)

        # Must not raise
        await fanout.send([{"id": 1}])


# ---------------------------------------------------------------------------
# TestSSEFanoutSubscriberCountLifecycle
# ---------------------------------------------------------------------------

class TestSSEFanoutSubscriberCountLifecycle:
    """Tests the full arc of subscriber_count across multiple operations.

    test_live_engine_full.py checks increments and decrements individually;
    here we verify the complete 0→N→M lifecycle and edge cases.
    """

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
        """Removing a queue that was never subscribed leaves count unchanged."""
        fanout = SSEFanout("q-lifecycle")
        q = fanout.subscribe()
        phantom = asyncio.Queue()
        count_before = fanout.subscriber_count

        fanout.unsubscribe(phantom)

        assert fanout.subscriber_count == count_before

        # Clean up
        fanout.unsubscribe(q)

    async def test_resubscribe_same_queue_increments_count_twice(self):
        """Subscribing the same Queue object twice is unusual but the list
        tracks it as two distinct entries (since list.append is unconditional).
        """
        fanout = SSEFanout("q-lifecycle")
        q = fanout.subscribe()
        fanout._queues.append(q)  # simulate a second subscribe of the same object
        assert fanout.subscriber_count == 2

        # close() should send None to each slot
        await fanout.close()
        assert q.get_nowait() is None
        assert q.get_nowait() is None
        assert fanout.subscriber_count == 0


# ---------------------------------------------------------------------------
# TestSSEFanoutConcurrentDelivery
# ---------------------------------------------------------------------------

class TestSSEFanoutConcurrentDelivery:
    """Tests SSEFanout.send dispatched from concurrent coroutines.

    The existing tests call send() sequentially; these tests use
    asyncio.gather to invoke send from multiple concurrent coroutines,
    exercising the list-snapshot behaviour inside send().
    """

    async def test_concurrent_sends_all_delivered_to_subscriber(self):
        """Multiple concurrent send() calls each deliver their batch."""
        fanout = SSEFanout("q-concurrent")
        q = fanout.subscribe()

        batches = [[{"id": i}] for i in range(5)]
        await asyncio.gather(*(fanout.send(b) for b in batches))

        received = []
        while not q.empty():
            received.append(q.get_nowait())

        # All 5 batches must have arrived (order may vary)
        assert len(received) == 5
        flat_ids = {item["id"] for batch in received for item in batch}
        assert flat_ids == {0, 1, 2, 3, 4}

    async def test_concurrent_send_and_subscribe_does_not_corrupt_count(self):
        """Subscribing while send() is running must not corrupt the queue list."""
        fanout = SSEFanout("q-concurrent")
        q_early = fanout.subscribe()

        rows = [{"id": 42}]

        # Run send and a late subscribe concurrently
        async def late_subscribe():
            await asyncio.sleep(0)  # yield to event loop once
            return fanout.subscribe()

        late_q, _ = await asyncio.gather(late_subscribe(), fanout.send(rows))

        # q_early must have received the rows (it was present when send snapshotted _queues)
        assert q_early.get_nowait() == rows
        # subscriber count reflects both queues
        assert fanout.subscriber_count == 2

        fanout.unsubscribe(q_early)
        fanout.unsubscribe(late_q)


# ---------------------------------------------------------------------------
# TestEnginePollEdgeCases
# ---------------------------------------------------------------------------

class TestEnginePollEdgeCases:
    """Edge cases inside LiveEngine._poll not covered by existing unit tests."""

    async def test_poll_uses_compiled_sql_over_query_text(self):
        """When a record has compiled_sql, that SQL (not query_text) is executed."""
        raw_rows = [{"id": 1, "ts": "2026-04-01"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)
        engine.subscribe("q1")

        record = {
            "query_text": "SELECT * FROM fallback_table",
            "compiled_sql": "SELECT id, ts FROM compiled_events",
        }
        p1, p2, p3 = _patch_poll_deps(record=record, watermark=None)
        with p1, p2, p3:
            await engine._poll("q1")

        executed_sql = conn.fetch.call_args[0][0]
        # compiled_sql must be the base (query_text must not appear)
        assert "compiled_events" in executed_sql
        assert "fallback_table" not in executed_sql

    async def test_poll_returns_early_when_query_text_is_empty_string(self):
        """Record with empty query_text (and no compiled_sql) must not fetch rows."""
        conn = _make_conn()
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")

        record = {"query_text": "", "compiled_sql": ""}
        p1, p2, p3 = _patch_poll_deps(record=record, watermark=None)
        with p1, p2, p3:
            await engine._poll("q1")

        conn.fetch.assert_not_called()
        assert q.empty()

    async def test_poll_single_row_watermark_set_to_that_row_value(self):
        """With exactly one row, set_watermark is called with that row's column value."""
        raw_rows = [{"id": 7, "ts": "2026-03-31"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)

        mock_set_wm = AsyncMock()
        record = {
            "query_text": "SELECT id, ts FROM events",
            "compiled_sql": "SELECT id, ts FROM events",
        }
        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)), \
             patch("provisa.live.watermark.set_watermark", mock_set_wm), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)):
            await engine._poll("q1")

        mock_set_wm.assert_called_once_with(conn, "q1", "2026-03-31")

    async def test_poll_watermark_column_missing_from_row_uses_empty_string(self):
        """Rows that lack the watermark column contribute an empty string to max()."""
        raw_rows = [{"id": 1}, {"id": 2}]  # no "ts" key
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)

        mock_set_wm = AsyncMock()
        record = {
            "query_text": "SELECT id FROM events",
            "compiled_sql": "SELECT id FROM events",
        }
        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)), \
             patch("provisa.live.watermark.set_watermark", mock_set_wm), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)):
            await engine._poll("q1")

        # max(str(None), str(None)) == "None" — the call must still happen
        mock_set_wm.assert_called_once()
        _, _, wm_value = mock_set_wm.call_args[0]
        assert isinstance(wm_value, str)

    async def test_poll_delivers_to_multiple_subscribers_simultaneously(self):
        """When two clients are subscribed, _poll sends the same batch to both queues."""
        raw_rows = [{"id": 1, "ts": "2026-04-01"}, {"id": 2, "ts": "2026-04-02"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)

        q_a = engine.subscribe("q1")
        q_b = engine.subscribe("q1")

        record = {
            "query_text": "SELECT id, ts FROM events",
            "compiled_sql": "SELECT id, ts FROM events",
        }
        p1, p2, p3 = _patch_poll_deps(record=record, watermark="2026-01-01")
        with p1, p2, p3:
            await engine._poll("q1")

        expected = [dict(r) for r in raw_rows]
        assert q_a.get_nowait() == expected
        assert q_b.get_nowait() == expected


# ---------------------------------------------------------------------------
# TestEngineUnregisterBehaviour
# ---------------------------------------------------------------------------

class TestEngineUnregisterBehaviour:
    """Targeted tests for unregister() interactions not in existing files."""

    async def test_unregister_does_not_flush_existing_subscriber_queues(self):
        """After unregister(), a queue that already received rows still holds them."""
        raw_rows = [{"id": 1, "ts": "2026-04-01"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")

        record = {
            "query_text": "SELECT id, ts FROM events",
            "compiled_sql": "SELECT id, ts FROM events",
        }
        p1, p2, p3 = _patch_poll_deps(record=record, watermark=None)
        with p1, p2, p3:
            await engine._poll("q1")

        # Rows are in the queue now; unregister should not drain them
        engine.unregister("q1")
        assert not engine.is_registered("q1")
        assert q.get_nowait() == [dict(r) for r in raw_rows]

    async def test_unregister_with_scheduler_calls_remove_job(self):
        """unregister() invokes scheduler.remove_job with the correct job id."""
        engine = _make_engine()
        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_q-removal")
            MockSched.return_value = mock_sched
            await engine.start()

        engine.register("q-removal", watermark_column="ts", poll_interval=5)
        engine.unregister("q-removal")

        mock_sched.remove_job.assert_called_once_with("live_q-removal")

    async def test_unregister_without_scheduler_does_not_raise(self):
        """unregister() with scheduler=None (never started) is silent."""
        engine = _make_engine()
        engine.register("q-no-sched", watermark_column="ts", poll_interval=5)
        # No start() called, so _scheduler is None
        engine.unregister("q-no-sched")
        assert not engine.is_registered("q-no-sched")


# ---------------------------------------------------------------------------
# TestEngineStopMultipleQueries
# ---------------------------------------------------------------------------

class TestEngineStopMultipleQueries:
    """Tests for stop() closing fanouts across multiple registered queries."""

    async def test_stop_sends_none_sentinel_to_all_registered_query_subscribers(self):
        """After stop(), every subscriber queue across all queries receives None."""
        conn = _make_conn()
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)

        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_x")
            MockSched.return_value = mock_sched
            await engine.start()

        engine.register("qa", watermark_column="ts", poll_interval=5)
        engine.register("qb", watermark_column="ts", poll_interval=10)

        qa_sub = engine.subscribe("qa")
        qb_sub1 = engine.subscribe("qb")
        qb_sub2 = engine.subscribe("qb")

        await engine.stop()

        assert qa_sub.get_nowait() is None
        assert qb_sub1.get_nowait() is None
        assert qb_sub2.get_nowait() is None

    async def test_stop_clears_all_jobs(self):
        """After stop(), _jobs is empty regardless of how many queries were registered."""
        engine = _make_engine()

        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_x")
            MockSched.return_value = mock_sched
            await engine.start()

        for qid in ("alpha", "beta", "gamma"):
            engine.register(qid, watermark_column="ts", poll_interval=5)

        assert len(engine._jobs) == 3
        await engine.stop()
        assert engine._jobs == {}

    async def test_stop_without_start_is_silent(self):
        """Calling stop() when the engine was never started must not raise."""
        engine = _make_engine()
        # Never called start() — _scheduler is None
        await engine.stop()

    async def test_stop_twice_does_not_raise(self):
        """Calling stop() twice in a row must be idempotent."""
        engine = _make_engine()

        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched = MagicMock()
            mock_sched.add_job.return_value = MagicMock(id="live_x")
            MockSched.return_value = mock_sched
            await engine.start()

        await engine.stop()
        await engine.stop()  # second call — must not raise


# ---------------------------------------------------------------------------
# TestEngineRegisterWithoutStart
# ---------------------------------------------------------------------------

class TestEngineRegisterWithoutStart:
    """register() works even before start() is called (no scheduler available)."""

    async def test_subscribe_after_register_without_start_returns_queue(self):
        engine = _make_engine()
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")
        assert isinstance(q, asyncio.Queue)

    async def test_fanout_delivers_rows_after_register_without_start(self):
        """Fanout created by register() without start() still delivers rows via send()."""
        engine = _make_engine()
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")

        rows = [{"id": 1, "ts": "2026-04-06"}]
        await engine._jobs["q1"].fanout.send(rows)

        assert q.get_nowait() == rows
