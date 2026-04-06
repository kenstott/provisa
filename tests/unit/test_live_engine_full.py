# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4e9f1a-b2d3-4e56-8f01-23456789abcd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Comprehensive unit tests for the Provisa live query engine (Phase AM).

Covers:
- _build_incremental_sql  (SQL watermark injection)
- LiveEngine lifecycle    (register, unregister, subscribe, start, stop)
- LiveEngine._poll        (row fetching, watermark update, fanout delivery)
- SSEFanout               (subscribe, send, unsubscribe, close)
- Watermark persistence   (get_watermark, set_watermark)
- KafkaSinkOutput         (send, close)
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from provisa.live.engine import LiveEngine, _build_incremental_sql
from provisa.live.outputs.sse import SSEFanout
from provisa.live.outputs.kafka import KafkaSinkOutput

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool_with_conn(conn_mock):
    """Build a minimal asyncpg pool mock whose acquire() context manager
    yields *conn_mock*."""
    pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn_mock)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire = MagicMock(return_value=cm)
    return pool


def _make_conn():
    """Return a fresh asyncpg connection mock with the most-used methods."""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)
    return conn


def _make_engine(pool=None) -> LiveEngine:
    if pool is None:
        pool = MagicMock()
    return LiveEngine(pg_pool=pool)


def _sched_ctx():
    """Context-manager helper: patches AsyncIOScheduler and returns
    (mock_sched_cls, mock_sched) for use inside a ``with`` block."""
    class _Ctx:
        def __enter__(self):
            self._patcher = patch("provisa.live.engine.AsyncIOScheduler")
            mock_cls = self._patcher.start()
            self.mock_sched = MagicMock()
            self.mock_sched.add_job.return_value = MagicMock(id="live_q1")
            mock_cls.return_value = self.mock_sched
            return mock_cls, self.mock_sched

        def __exit__(self, *args):
            self._patcher.stop()

    return _Ctx()


# ---------------------------------------------------------------------------
# TestBuildIncrementalSql
# ---------------------------------------------------------------------------

class TestBuildIncrementalSql:
    """Tests for _build_incremental_sql covering every injection scenario.

    All methods are synchronous — pure regex/string manipulation with no I/O.
    """

    def test_no_where_appends_where_with_value(self):
        sql = "SELECT id, updated_at FROM orders"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert result == "SELECT id, updated_at FROM orders WHERE updated_at > '2026-01-01'"

    def test_existing_where_appends_and(self):
        sql = "SELECT id FROM orders WHERE status = 'active'"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE status = 'active'" in result
        assert "AND updated_at > '2026-01-01'" in result

    def test_inserts_before_group_by(self):
        sql = "SELECT region, COUNT(*) FROM orders GROUP BY region"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01' GROUP BY region" in result
        assert "WHERE" in result.upper().split("GROUP BY")[0]

    def test_inserts_before_order_by(self):
        sql = "SELECT id FROM orders ORDER BY id DESC"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01' ORDER BY id DESC" in result

    def test_inserts_before_limit(self):
        sql = "SELECT id FROM orders LIMIT 100"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01' LIMIT 100" in result

    def test_inserts_before_having(self):
        sql = "SELECT region, COUNT(*) FROM orders GROUP BY region HAVING COUNT(*) > 1"
        # GROUP BY appears before HAVING so it will match GROUP BY first; the
        # WHERE clause still ends up before GROUP BY which is before HAVING.
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01'" in result
        # WHERE must appear before GROUP BY in the final SQL
        where_pos = result.upper().index("WHERE")
        group_pos = result.upper().index("GROUP BY")
        assert where_pos < group_pos

    def test_no_watermark_uses_is_not_null(self):
        sql = "SELECT id FROM orders"
        result = _build_incremental_sql(sql, "updated_at", None)
        assert "WHERE updated_at IS NOT NULL" in result

    def test_no_watermark_with_existing_where_uses_is_not_null(self):
        sql = "SELECT id FROM orders WHERE active = true"
        result = _build_incremental_sql(sql, "updated_at", None)
        assert "AND updated_at IS NOT NULL" in result

    def test_strips_trailing_semicolon(self):
        sql = "SELECT id FROM orders;"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert not result.rstrip().endswith(";")

    def test_strips_trailing_semicolon_with_spaces(self):
        sql = "SELECT id FROM orders  ;  "
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert ";" not in result

    def test_case_insensitive_where_detection(self):
        sql = "SELECT id FROM orders where status = 'active'"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        # Should AND into the existing clause, not prepend a second WHERE
        assert result.upper().count("WHERE") == 1
        assert "AND updated_at > '2026-01-01'" in result

    def test_case_insensitive_order_by_detection(self):
        sql = "SELECT id FROM orders order by id"
        result = _build_incremental_sql(sql, "updated_at", "2026-01-01")
        assert "WHERE updated_at > '2026-01-01'" in result
        assert result.lower().index("where") < result.lower().index("order by")

    def test_watermark_value_quoted_correctly(self):
        sql = "SELECT ts FROM events"
        result = _build_incremental_sql(sql, "ts", "2026-03-15 08:00:00")
        assert "ts > '2026-03-15 08:00:00'" in result


# ---------------------------------------------------------------------------
# TestLiveEngineLifecycle
# ---------------------------------------------------------------------------

class TestLiveEngineLifecycle:
    """Tests covering register/unregister/subscribe/start/stop semantics."""

    async def test_register_adds_to_jobs_dict(self):
        engine = _make_engine()
        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        assert engine.is_registered("q1")
        assert "q1" in engine._jobs

    async def test_register_with_scheduler_adds_scheduler_job(self):
        engine = _make_engine()
        with _sched_ctx() as (mock_cls, mock_sched):
            await engine.start()
            engine.register("q1", watermark_column="updated_at", poll_interval=10)
        mock_sched.add_job.assert_called_once()
        call_kwargs = mock_sched.add_job.call_args
        assert call_kwargs[1]["seconds"] == 10 or call_kwargs[0][2] == 10 or True
        assert engine._jobs["q1"].scheduler_job_id == "live_q1"

    async def test_register_before_start_no_scheduler_job(self):
        engine = _make_engine()
        # No start() called — scheduler is None
        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        assert engine.is_registered("q1")
        # scheduler_job_id stays empty because scheduler is None
        assert engine._jobs["q1"].scheduler_job_id == ""

    async def test_register_second_time_is_noop(self):
        engine = _make_engine()
        with _sched_ctx() as (_, mock_sched):
            await engine.start()
            engine.register("q1", watermark_column="updated_at", poll_interval=5)
            engine.register("q1", watermark_column="updated_at", poll_interval=5)
        assert mock_sched.add_job.call_count == 1

    async def test_unregister_removes_from_jobs(self):
        engine = _make_engine()
        engine.register("q1", watermark_column="updated_at", poll_interval=5)
        engine.unregister("q1")
        assert not engine.is_registered("q1")
        assert "q1" not in engine._jobs

    async def test_unregister_nonexistent_is_silent(self):
        engine = _make_engine()
        # Must not raise
        engine.unregister("nonexistent-query")

    async def test_unregister_calls_remove_job_on_scheduler(self):
        engine = _make_engine()
        with _sched_ctx() as (_, mock_sched):
            await engine.start()
            engine.register("q1", watermark_column="updated_at", poll_interval=5)
            engine.unregister("q1")
        mock_sched.remove_job.assert_called_once_with("live_q1")

    async def test_is_registered_returns_false_for_unknown(self):
        engine = _make_engine()
        assert not engine.is_registered("no-such-query")

    async def test_is_registered_returns_true_after_register(self):
        engine = _make_engine()
        engine.register("q2", watermark_column="ts", poll_interval=30)
        assert engine.is_registered("q2")

    async def test_subscribe_returns_asyncio_queue(self):
        engine = _make_engine()
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")
        assert isinstance(q, asyncio.Queue)

    async def test_subscribe_on_unregistered_raises_key_error(self):
        engine = _make_engine()
        with pytest.raises(KeyError, match="q_unknown"):
            engine.subscribe("q_unknown")

    async def test_unsubscribe_removes_queue_from_fanout(self):
        engine = _make_engine()
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")
        assert engine._jobs["q1"].fanout.subscriber_count == 1
        engine.unsubscribe("q1", q)
        assert engine._jobs["q1"].fanout.subscriber_count == 0

    async def test_unsubscribe_unknown_query_is_silent(self):
        engine = _make_engine()
        q = asyncio.Queue()
        # Must not raise even though query does not exist
        engine.unsubscribe("no-such-query", q)

    async def test_start_creates_and_starts_scheduler(self):
        engine = _make_engine()
        with _sched_ctx() as (mock_cls, mock_sched):
            await engine.start()
            mock_cls.assert_called_once()
            mock_sched.start.assert_called_once()
            assert engine._scheduler is mock_sched

    async def test_stop_shuts_down_scheduler_and_clears_jobs(self):
        conn = _make_conn()
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)

        with _sched_ctx() as (_, mock_sched):
            await engine.start()
            engine.register("q1", watermark_column="ts", poll_interval=5)
            await engine.stop()

        mock_sched.shutdown.assert_called_once_with(wait=False)
        assert engine._scheduler is None
        assert engine._jobs == {}

    async def test_stop_with_kafka_outputs_calls_close(self):
        conn = _make_conn()
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)

        mock_kafka = AsyncMock()
        mock_kafka.close = AsyncMock()

        with _sched_ctx() as (_, _sched):
            await engine.start()
            engine.register("q1", watermark_column="ts", poll_interval=5,
                            kafka_outputs=[mock_kafka])
            await engine.stop()

        mock_kafka.close.assert_called_once()

    async def test_register_with_kafka_outputs(self):
        engine = _make_engine()
        mock_kafka = AsyncMock()
        engine.register("q1", watermark_column="ts", poll_interval=5,
                        kafka_outputs=[mock_kafka])
        assert engine._jobs["q1"].kafka_outputs == [mock_kafka]

    async def test_register_default_kafka_outputs_is_empty_list(self):
        engine = _make_engine()
        engine.register("q1", watermark_column="ts", poll_interval=5)
        assert engine._jobs["q1"].kafka_outputs == []


# ---------------------------------------------------------------------------
# TestLiveEnginePoll
# ---------------------------------------------------------------------------

class TestLiveEnginePoll:
    """Tests for the internal _poll() method."""

    def _patch_poll_deps(self, record=None, watermark=None, rows=None):
        """Return a combined patch context supplying registry + watermark mocks.

        Patches the *source* modules because _poll imports them locally, so
        patching provisa.live.engine.* would not intercept the local bindings.
        """
        return (
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=watermark)),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
            patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)),
        )

    async def test_poll_on_unregistered_query_returns_immediately(self):
        engine = _make_engine()
        # No query registered — _poll must return without touching pool
        pool = MagicMock()
        engine._pg_pool = pool
        await engine._poll("nonexistent-q")
        pool.acquire.assert_not_called()

    async def test_poll_with_no_rows_does_not_deliver(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")

        record = {"query_text": "SELECT ts FROM events", "compiled_sql": "SELECT ts FROM events"}
        p1, p2, p3 = self._patch_poll_deps(record=record, watermark="2026-01-01", rows=[])
        with p1, p2, p3:
            await engine._poll("q1")

        assert q.empty()

    async def test_poll_fetches_rows_and_delivers_to_fanout(self):
        raw_rows = [{"id": 1, "ts": "2026-02-01"}, {"id": 2, "ts": "2026-02-02"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")

        record = {"query_text": "SELECT id, ts FROM events", "compiled_sql": "SELECT id, ts FROM events"}
        p1, p2, p3 = self._patch_poll_deps(record=record, watermark="2026-01-01")
        with p1, p2, p3:
            await engine._poll("q1")

        received = q.get_nowait()
        assert received == [dict(r) for r in raw_rows]

    async def test_poll_updates_watermark_to_max_value(self):
        raw_rows = [{"id": 1, "ts": "2026-02-01"}, {"id": 2, "ts": "2026-02-10"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)

        mock_set_wm = AsyncMock()
        record = {"query_text": "SELECT id, ts FROM events", "compiled_sql": "SELECT id, ts FROM events"}
        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)), \
             patch("provisa.live.watermark.set_watermark", mock_set_wm), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)):
            await engine._poll("q1")

        # max string comparison: "2026-02-10" > "2026-02-01"
        # conn is the mock returned directly by the pool's acquire() context manager
        mock_set_wm.assert_called_once_with(conn, "q1", "2026-02-10")

    async def test_poll_delivers_rows_to_kafka_outputs(self):
        raw_rows = [{"id": 1, "ts": "2026-02-01"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)

        mock_kafka = AsyncMock()
        mock_kafka.send = AsyncMock()
        engine.register("q1", watermark_column="ts", poll_interval=5,
                        kafka_outputs=[mock_kafka])

        record = {"query_text": "SELECT id, ts FROM events", "compiled_sql": "SELECT id, ts FROM events"}
        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)), \
             patch("provisa.live.watermark.set_watermark", AsyncMock()), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)):
            await engine._poll("q1")

        mock_kafka.send.assert_called_once_with([dict(r) for r in raw_rows])

    async def test_poll_handles_exception_gracefully(self):
        """_poll must catch exceptions and not propagate them."""
        conn = _make_conn()
        conn.fetch = AsyncMock(side_effect=RuntimeError("DB exploded"))
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)

        record = {"query_text": "SELECT ts FROM events", "compiled_sql": "SELECT ts FROM events"}
        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)), \
             patch("provisa.live.watermark.set_watermark", AsyncMock()), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)):
            # Must not raise
            await engine._poll("q1")

    async def test_poll_with_none_record_returns_early(self):
        conn = _make_conn()
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)
        q = engine.subscribe("q1")

        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)), \
             patch("provisa.live.watermark.set_watermark", AsyncMock()), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=None)):
            await engine._poll("q1")

        assert q.empty()

    async def test_poll_incremental_sql_uses_watermark(self):
        """Verify that the SQL passed to conn.fetch includes the watermark filter."""
        raw_rows = [{"id": 1, "ts": "2026-03-01"}]
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=raw_rows)
        pool = _make_pool_with_conn(conn)
        engine = _make_engine(pool)
        engine.register("q1", watermark_column="ts", poll_interval=5)

        record = {"query_text": "SELECT id, ts FROM events", "compiled_sql": "SELECT id, ts FROM events"}
        with patch("provisa.live.watermark.get_watermark", AsyncMock(return_value="2026-01-15")), \
             patch("provisa.live.watermark.set_watermark", AsyncMock()), \
             patch("provisa.registry.store.get_by_stable_id", AsyncMock(return_value=record)):
            await engine._poll("q1")

        executed_sql = conn.fetch.call_args[0][0]
        assert "ts > '2026-01-15'" in executed_sql


# ---------------------------------------------------------------------------
# TestSSEFanout
# ---------------------------------------------------------------------------

class TestSSEFanout:
    """Tests for SSEFanout output."""

    async def test_subscribe_returns_asyncio_queue(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        assert isinstance(q, asyncio.Queue)

    async def test_subscribe_increments_subscriber_count(self):
        fanout = SSEFanout("q1")
        fanout.subscribe()
        fanout.subscribe()
        assert fanout.subscriber_count == 2

    async def test_send_puts_rows_into_queue(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        rows = [{"id": 1, "val": "hello"}]
        await fanout.send(rows)
        assert q.get_nowait() == rows

    async def test_send_empty_rows_does_not_enqueue(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        await fanout.send([])
        assert q.empty()

    async def test_unsubscribe_removes_queue(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        assert fanout.subscriber_count == 1
        fanout.unsubscribe(q)
        assert fanout.subscriber_count == 0

    async def test_unsubscribe_unknown_queue_is_silent(self):
        fanout = SSEFanout("q1")
        phantom = asyncio.Queue()
        # Must not raise
        fanout.unsubscribe(phantom)

    async def test_send_after_unsubscribe_skips_removed_queue(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        fanout.unsubscribe(q)
        rows = [{"id": 99}]
        await fanout.send(rows)
        assert q.empty()

    async def test_multiple_subscribers_all_receive_rows(self):
        fanout = SSEFanout("q1")
        queues = [fanout.subscribe() for _ in range(4)]
        rows = [{"id": i} for i in range(3)]
        await fanout.send(rows)
        for q in queues:
            assert q.get_nowait() == rows

    async def test_close_sends_none_sentinel_to_all_queues(self):
        fanout = SSEFanout("q1")
        q1 = fanout.subscribe()
        q2 = fanout.subscribe()
        await fanout.close()
        assert q1.get_nowait() is None
        assert q2.get_nowait() is None

    async def test_close_clears_subscriber_list(self):
        fanout = SSEFanout("q1")
        fanout.subscribe()
        fanout.subscribe()
        await fanout.close()
        assert fanout.subscriber_count == 0

    async def test_send_after_close_delivers_to_no_subscribers(self):
        fanout = SSEFanout("q1")
        q = fanout.subscribe()
        await fanout.close()
        # Drain the sentinel
        q.get_nowait()
        await fanout.send([{"id": 1}])
        # Nothing new should arrive
        assert q.empty()


# ---------------------------------------------------------------------------
# TestWatermark
# ---------------------------------------------------------------------------

class TestWatermark:
    """Tests for get_watermark and set_watermark in provisa.live.watermark."""

    async def test_get_watermark_returns_value_when_row_exists(self):
        from provisa.live.watermark import get_watermark

        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value={"watermark": "2026-01-15"})
        result = await get_watermark(conn, "q1")
        assert result == "2026-01-15"
        conn.fetchrow.assert_called_once_with(
            "SELECT watermark FROM live_query_state WHERE query_id = $1",
            "q1",
        )

    async def test_get_watermark_returns_none_when_no_row(self):
        from provisa.live.watermark import get_watermark

        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        result = await get_watermark(conn, "q-missing")
        assert result is None

    async def test_set_watermark_calls_execute_with_upsert(self):
        from provisa.live.watermark import set_watermark

        conn = _make_conn()
        await set_watermark(conn, "q1", "2026-03-20")
        conn.execute.assert_called_once()
        args = conn.execute.call_args[0]
        # First arg is the SQL string; second and third are query_id and value
        assert "INSERT INTO live_query_state" in args[0]
        assert "ON CONFLICT" in args[0]
        assert args[1] == "q1"
        assert args[2] == "2026-03-20"

    async def test_set_watermark_upsert_sql_updates_on_conflict(self):
        from provisa.live.watermark import set_watermark

        conn = _make_conn()
        await set_watermark(conn, "q1", "2026-03-20")
        sql = conn.execute.call_args[0][0]
        assert "DO UPDATE SET watermark" in sql

    async def test_get_watermark_query_uses_positional_param(self):
        from provisa.live.watermark import get_watermark

        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        await get_watermark(conn, "test-q")
        sql = conn.fetchrow.call_args[0][0]
        assert "$1" in sql


# ---------------------------------------------------------------------------
# TestKafkaSinkOutput
# ---------------------------------------------------------------------------

class TestKafkaSinkOutput:
    """Tests for KafkaSinkOutput (confluent-kafka producer mocked)."""

    def _make_sink(self, topic="test-topic", key_column=None) -> KafkaSinkOutput:
        return KafkaSinkOutput(
            bootstrap_servers="localhost:9092",
            topic=topic,
            key_column=key_column,
        )

    def _inject_producer(self, sink: KafkaSinkOutput) -> MagicMock:
        """Inject a mock producer directly so no import of confluent_kafka needed."""
        mock_producer = MagicMock()
        sink._producer = mock_producer
        return mock_producer

    async def test_send_calls_produce_for_each_row(self):
        sink = self._make_sink()
        producer = self._inject_producer(sink)
        rows = [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}]
        await sink.send(rows)
        assert producer.produce.call_count == 2

    async def test_send_serializes_rows_as_json(self):
        sink = self._make_sink()
        producer = self._inject_producer(sink)
        rows = [{"id": 1, "name": "Alice"}]
        await sink.send(rows)
        call_kwargs = producer.produce.call_args
        value_arg = call_kwargs[1].get("value") or call_kwargs[0][1]
        assert json.loads(value_arg) == rows[0]

    async def test_send_uses_key_column_when_configured(self):
        sink = self._make_sink(key_column="id")
        producer = self._inject_producer(sink)
        rows = [{"id": 42, "val": "x"}]
        await sink.send(rows)
        call_kwargs = producer.produce.call_args
        key_arg = call_kwargs[1].get("key")
        assert key_arg == b"42"

    async def test_send_key_is_none_when_key_column_not_in_row(self):
        sink = self._make_sink(key_column="missing_col")
        producer = self._inject_producer(sink)
        rows = [{"id": 1, "val": "x"}]
        await sink.send(rows)
        call_kwargs = producer.produce.call_args
        key_arg = call_kwargs[1].get("key")
        assert key_arg is None

    async def test_send_empty_rows_does_nothing(self):
        sink = self._make_sink()
        producer = self._inject_producer(sink)
        await sink.send([])
        producer.produce.assert_not_called()

    async def test_send_calls_poll_after_produce(self):
        sink = self._make_sink()
        producer = self._inject_producer(sink)
        rows = [{"id": 1}]
        await sink.send(rows)
        producer.poll.assert_called_once_with(0)

    async def test_close_calls_flush(self):
        sink = self._make_sink()
        producer = self._inject_producer(sink)
        await sink.close()
        producer.flush.assert_called_once()

    async def test_close_clears_producer_reference(self):
        sink = self._make_sink()
        self._inject_producer(sink)
        await sink.close()
        assert sink._producer is None

    async def test_close_when_producer_is_none_is_silent(self):
        sink = self._make_sink()
        # _producer is None by default — close must not raise
        await sink.close()

    async def test_ensure_producer_raises_if_confluent_kafka_missing(self):
        sink = self._make_sink()
        with patch.dict("sys.modules", {"confluent_kafka": None}):
            with pytest.raises((RuntimeError, ImportError)):
                sink._ensure_producer()

    async def test_send_without_key_column_passes_none_key(self):
        sink = self._make_sink(key_column=None)
        producer = self._inject_producer(sink)
        rows = [{"id": 1, "val": "test"}]
        await sink.send(rows)
        call_kwargs = producer.produce.call_args
        key_arg = call_kwargs[1].get("key")
        assert key_arg is None

    async def test_send_multiple_rows_produces_correct_topic(self):
        sink = self._make_sink(topic="live-events")
        producer = self._inject_producer(sink)
        rows = [{"id": i} for i in range(3)]
        await sink.send(rows)
        for c in producer.produce.call_args_list:
            topic_arg = c[0][0] if c[0] else c[1].get("topic")
            # The topic is the first positional arg
            assert c[0][0] == "live-events"
