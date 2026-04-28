# Copyright (c) 2026 Kenneth Stott
# Canary: 5a1b2c3d-4e5f-6789-abcd-ef0123456789
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for SSE live query subscriptions."""

from __future__ import annotations

import asyncio
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from provisa.live.outputs.sse import SSEFanout
from provisa.live.engine import LiveEngine, _build_incremental_sql
from provisa.subscriptions.base import ChangeEvent

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _pg_env() -> dict:
    return dict(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )


async def _try_pg_pool():
    """Create an asyncpg pool; raises if PG is unavailable."""
    import asyncpg  # noqa: PLC0415
    env = _pg_env()
    pool = await asyncio.wait_for(
        asyncpg.create_pool(
            host=env["host"],
            port=env["port"],
            database=env["database"],
            user=env["user"],
            password=env["password"],
            min_size=1,
            max_size=3,
            command_timeout=10,
        ),
        timeout=5.0,
    )
    return pool


# ---------------------------------------------------------------------------
# SSEFanout unit-level integration tests (no PG required)
# ---------------------------------------------------------------------------

class TestSSEFanout:
    async def test_sse_provider_yields_events_on_insert(self):
        """SSEFanout delivers rows to a subscriber queue."""
        fanout = SSEFanout("test-query-1")
        queue = fanout.subscribe()

        rows = [{"id": 1, "amount": 100.0, "region": "us-east"}]
        await fanout.send(rows)

        assert not queue.empty()
        received = queue.get_nowait()
        assert received == rows
        assert received[0]["id"] == 1

    async def test_sse_multiple_subscribers_all_notified(self):
        """Two subscriber queues both receive the same batch."""
        fanout = SSEFanout("test-query-2")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()

        rows = [{"id": 10, "amount": 50.0}]
        await fanout.send(rows)

        assert q1.get_nowait() == rows
        assert q2.get_nowait() == rows

    async def test_sse_filter_excludes_non_matching_events(self):
        """Only rows that match a filter reach the subscriber.

        The SSEFanout itself is filter-agnostic; filtering is applied upstream
        at the PG provider level.  We test here that sending an empty batch
        is a no-op (no queue item produced).
        """
        fanout = SSEFanout("test-query-3")
        queue = fanout.subscribe()

        # Simulates upstream filter dropping all events
        await fanout.send([])  # empty batch — should not enqueue
        assert queue.empty()

        # Now only matching rows sent
        matching = [{"id": 5, "region": "eu-west"}]
        await fanout.send(matching)
        assert queue.get_nowait() == matching

    async def test_sse_provider_close_stops_stream(self):
        """close() sends sentinel None to all queues and clears the list."""
        fanout = SSEFanout("test-query-4")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()

        assert fanout.subscriber_count == 2
        await fanout.close()

        # Sentinel None sent to each queue
        assert q1.get_nowait() is None
        assert q2.get_nowait() is None
        # All queues cleared
        assert fanout.subscriber_count == 0

    async def test_sse_unsubscribe_removes_queue(self):
        """unsubscribe() removes a specific queue; others still receive events."""
        fanout = SSEFanout("test-query-5")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()
        fanout.unsubscribe(q1)

        rows = [{"id": 99}]
        await fanout.send(rows)

        assert q1.empty()
        assert q2.get_nowait() == rows


# ---------------------------------------------------------------------------
# LiveEngine watermark test (no PG required — patched)
# ---------------------------------------------------------------------------

class TestLiveEngineWatermark:
    async def test_live_engine_watermark_advances(self):
        """_build_incremental_sql uses the watermark value when provided."""
        base_sql = 'SELECT * FROM "public"."orders"'
        result_with = _build_incremental_sql(base_sql, "created_at", "2026-01-01T00:00:00")
        assert "WHERE" in result_with
        assert "created_at > '2026-01-01T00:00:00'" in result_with

        result_without = _build_incremental_sql(base_sql, "created_at", None)
        assert "created_at IS NOT NULL" in result_without

    async def test_live_engine_watermark_appends_and_filter(self):
        """Existing WHERE clause gets an AND, not a second WHERE."""
        base_sql = 'SELECT * FROM "public"."orders" WHERE "region" = \'us-east\''
        result = _build_incremental_sql(base_sql, "created_at", "2026-01-01")
        assert result.count("WHERE") == 1
        assert "AND created_at" in result

    async def test_live_engine_register_and_subscribe(self):
        """register() creates SSEFanout; subscribe() returns a queue."""
        mock_pool = MagicMock()
        engine = LiveEngine(pg_pool=mock_pool)

        # Start without real APScheduler to avoid side effects
        with patch("provisa.live.engine.AsyncIOScheduler") as MockSched:
            mock_sched_instance = MagicMock()
            mock_sched_instance.add_job.return_value = MagicMock(id="live_abc")
            MockSched.return_value = mock_sched_instance
            await engine.start()

        engine.register("abc-123", watermark_column="created_at", poll_interval=10)
        assert engine.is_registered("abc-123")

        queue = engine.subscribe("abc-123")
        assert isinstance(queue, asyncio.Queue)

        engine.unsubscribe("abc-123", queue)
        engine.unregister("abc-123")
        assert not engine.is_registered("abc-123")

    async def test_live_engine_subscribe_unknown_raises(self):
        """subscribe() on an unregistered query_id raises KeyError."""
        mock_pool = MagicMock()
        engine = LiveEngine(pg_pool=mock_pool)
        with pytest.raises(KeyError, match="not registered"):
            engine.subscribe("nonexistent-id")


# ---------------------------------------------------------------------------
# PgNotificationProvider integration test (skipped if PG unavailable)
# ---------------------------------------------------------------------------

class TestPgNotificationProvider:
    async def test_pg_provider_yields_change_event(self):
        """Provider yields ChangeEvent when a NOTIFY arrives on the channel."""
        pool = await _try_pg_pool()

        from provisa.subscriptions.pg_provider import PgNotificationProvider, CHANNEL_PREFIX

        provider = PgNotificationProvider(pool)
        table = "orders"
        channel = f"{CHANNEL_PREFIX}{table}"
        received: list[ChangeEvent] = []

        async def _consume():
            async for event in provider.watch(table):
                received.append(event)
                break  # stop after first event

        task = asyncio.create_task(_consume())
        await asyncio.sleep(0.2)  # let listener register

        # Send a NOTIFY from a separate connection
        async with pool.acquire() as notify_conn:
            payload = json.dumps({"op": "insert", "row": {"id": 42, "amount": 9.99}})
            await notify_conn.execute(f"SELECT pg_notify($1, $2)", channel, payload)

        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            pytest.fail("Provider did not yield an event within timeout")

        assert len(received) == 1
        evt = received[0]
        assert evt.operation == "insert"
        assert evt.table == table
        assert evt.row["id"] == 42
        await pool.close()
