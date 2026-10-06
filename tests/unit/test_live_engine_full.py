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
- LiveEngine lifecycle    (reconcile, subscribe, unsubscribe, start, stop)
- LiveEngine._poll        (governed reads, watermark update, fanout delivery)
- SSEFanout               (subscribe, send, unsubscribe, close)
- Watermark persistence   (get_watermark, set_watermark)
- KafkaSinkOutput         (send, close)
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.live.engine import _build_incremental_sql
from provisa.live.outputs.kafka import KafkaSinkOutput
from provisa.live.outputs.sse import SSEFanout
from tests.unit.live_engine_doubles import (
    KEY,
    OTHER_KEY,
    governed,
    make_engine,
    make_pool_with_conn,
    spec,
    sse_job_id,
    sse_type,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn():
    """Return a fresh store connection mock with the most-used methods."""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.execute = AsyncMock(return_value=None)
    return conn


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


@pytest.mark.asyncio(loop_scope="session")
class TestLiveEngineLifecycle:
    """Specs (reconcile), subscriber groups (subscribe/unsubscribe), start and stop."""

    async def test_register_adds_to_jobs_dict(self):
        engine = make_engine()
        engine.reconcile([spec()])
        assert engine.is_registered("q1")
        assert "q1" in engine._specs

    async def test_register_with_scheduler_adds_scheduler_job(self):
        sched = MagicMock()
        engine = make_engine(scheduler=sched, started=True)
        engine.reconcile([spec(poll_interval=10)])
        with governed():
            await engine.subscribe("q1", KEY)
        sched.add_job.assert_called_once()
        assert sched.add_job.call_args.kwargs["seconds"] == 10
        assert sched.add_job.call_args.kwargs["id"] == sse_job_id()
        assert engine._groups[("q1", sse_type())].job_id == sse_job_id()

    async def test_register_before_start_no_scheduler_job(self):
        sched = MagicMock()
        engine = make_engine(scheduler=sched)  # start() not called
        engine.reconcile([spec()])
        with governed():
            await engine.subscribe("q1", KEY)
        assert engine.is_registered("q1")
        sched.add_job.assert_not_called()

    async def test_register_second_time_is_noop(self):
        sched = MagicMock()
        engine = make_engine(scheduler=sched, started=True)
        engine.reconcile([spec()])
        engine.reconcile([spec()])
        with governed():
            await engine.subscribe("q1", KEY)
            await engine.subscribe("q1", KEY)  # the same key shares the one poll
        assert sched.add_job.call_count == 1

    async def test_unregister_removes_from_jobs(self):
        engine = make_engine()
        engine.reconcile([spec()])
        engine.reconcile([])
        assert not engine.is_registered("q1")
        assert "q1" not in engine._specs

    async def test_unregister_nonexistent_is_silent(self):
        engine = make_engine()
        engine.reconcile([])
        assert not engine.is_registered("nonexistent-query")
        assert engine._specs == {} and engine._groups == {}

    async def test_unregister_calls_remove_job_on_scheduler(self):
        sched = MagicMock()
        engine = make_engine(scheduler=sched, started=True)
        engine.reconcile([spec()])
        with governed():
            await engine.subscribe("q1", KEY)
        engine.reconcile([])
        sched.remove_job.assert_called_once_with(sse_job_id())

    async def test_is_registered_returns_false_for_unknown(self):
        engine = make_engine()
        assert not engine.is_registered("no-such-query")

    async def test_is_registered_returns_true_after_register(self):
        engine = make_engine()
        engine.reconcile([spec("q2", poll_interval=30)])
        assert engine.is_registered("q2")

    async def test_subscribe_returns_asyncio_queue(self):
        engine = make_engine()
        engine.reconcile([spec()])
        with governed():
            q = await engine.subscribe("q1", KEY)
        assert isinstance(q, asyncio.Queue)

    async def test_subscribe_on_unregistered_raises_key_error(self):
        engine = make_engine()
        with pytest.raises(KeyError, match="q_unknown"):
            await engine.subscribe("q_unknown", KEY)

    async def test_unsubscribe_removes_queue_from_fanout(self):
        sched = MagicMock()
        engine = make_engine(scheduler=sched, started=True)
        engine.reconcile([spec()])
        with governed():
            q = await engine.subscribe("q1", KEY)
        fanout = engine._groups[("q1", sse_type())].output
        assert fanout.subscriber_count == 1
        engine.unsubscribe("q1", KEY, q)
        assert fanout.subscriber_count == 0
        # The key's last subscriber stops its poll.
        assert ("q1", sse_type()) not in engine._groups
        sched.remove_job.assert_called_once_with(sse_job_id())

    async def test_unsubscribe_unknown_query_is_silent(self):
        engine = make_engine()
        engine.unsubscribe("no-such-query", KEY, asyncio.Queue())
        assert engine._groups == {}

    async def test_start_creates_and_starts_scheduler(self):
        """The engine schedules on the process's scheduler; it starts none of its own."""
        sched = MagicMock()
        engine = make_engine(scheduler=sched)
        await engine.start()
        assert engine._scheduler is sched
        sched.start.assert_not_called()

    async def test_stop_shuts_down_scheduler_and_clears_jobs(self):
        sched = MagicMock()
        engine = make_engine(make_pool_with_conn(_make_conn()), sched)
        await engine.start()
        engine.reconcile([spec()])
        with governed():
            await engine.subscribe("q1", KEY)
        await engine.stop()
        # Its own polls are removed; the process's scheduler runs on for everything else.
        sched.remove_job.assert_called_once_with(sse_job_id())
        sched.shutdown.assert_not_called()
        assert engine._scheduler is None
        assert engine._specs == {} and engine._groups == {}

    async def test_stop_with_kafka_outputs_calls_close(self):
        engine = make_engine(make_pool_with_conn(_make_conn()))
        mock_kafka = AsyncMock()
        with patch("provisa.live.engine.KafkaSinkOutput", return_value=mock_kafka), governed():
            await engine.start()
            engine.reconcile([spec(kafka_outputs=[_kafka_output()])])
            await engine.stop()
        mock_kafka.close.assert_called_once()

    async def test_register_with_kafka_outputs(self):
        engine = make_engine()
        mock_kafka = AsyncMock()
        with patch("provisa.live.engine.KafkaSinkOutput", return_value=mock_kafka), governed():
            engine.reconcile([spec(kafka_outputs=[_kafka_output(role="publisher")])])
        [group] = engine._groups.values()
        assert group.output is mock_kafka
        assert group.key.role_id == "publisher"  # it publishes as the role it names

    async def test_register_default_kafka_outputs_is_empty_list(self):
        engine = make_engine()
        engine.reconcile([spec()])
        assert engine._specs["q1"].kafka_outputs == []
        assert engine._groups == {}  # no output and no subscriber: nothing is polled


def _kafka_output(role: str = "publisher") -> dict:
    return {"bootstrap_servers": "k:9092", "topic": "events", "key_column": "id", "role": role}


# ---------------------------------------------------------------------------
# TestLiveEnginePoll
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestLiveEnginePoll:
    """Tests for the internal _poll() method: every poll is a governed read as its key."""

    def _patch_poll_deps(self, watermark=None):
        """Return a combined patch context supplying watermark mocks."""
        return (
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=watermark)),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
        )

    async def _subscribed(self, pool=None, key=KEY):
        engine = make_engine(pool if pool is not None else make_pool_with_conn(_make_conn()))
        engine.reconcile([spec()])
        with governed():
            q = await engine.subscribe("q1", key)
        return engine, q

    async def test_poll_on_unregistered_query_returns_immediately(self):
        engine = make_engine()
        pool = MagicMock()
        engine._tenant_db = pool
        await engine._poll("nonexistent-q", sse_type())
        pool.acquire.assert_not_called()

    async def test_poll_with_no_rows_does_not_deliver(self):
        engine, q = await self._subscribed()
        p1, p2 = self._patch_poll_deps(watermark="2026-01-01")
        with governed([]), p1, p2:
            await engine._poll("q1", sse_type())
        assert q.empty()

    async def test_poll_fetches_rows_and_delivers_to_fanout(self):
        rows = [{"id": 1, "ts": "2026-02-01"}, {"id": 2, "ts": "2026-02-02"}]
        engine, q = await self._subscribed()
        p1, p2 = self._patch_poll_deps(watermark="2026-01-01")
        with governed(rows) as reads, p1, p2:
            await engine._poll("q1", sse_type())
        assert q.get_nowait() == rows
        # The poll was the governed pipeline's read, as the subscriber's key.
        assert [key for _sql, key, _p in reads] == [KEY]

    async def test_poll_updates_watermark_to_max_value(self):
        rows = [{"id": 1, "ts": "2026-02-01"}, {"id": 2, "ts": "2026-02-10"}]
        conn = _make_conn()
        engine, _q = await self._subscribed(make_pool_with_conn(conn))
        mock_set_wm = AsyncMock()
        with (
            governed(rows),
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", mock_set_wm),
        ):
            await engine._poll("q1", sse_type())
        # max string comparison: "2026-02-10" > "2026-02-01"; kept for this key alone.
        mock_set_wm.assert_called_once_with(conn, "q1", sse_type(), "2026-02-10")

    async def test_poll_delivers_rows_to_kafka_outputs(self):
        rows = [{"id": 1, "ts": "2026-02-01"}]
        engine = make_engine(make_pool_with_conn(_make_conn()))
        mock_kafka = AsyncMock()
        with patch("provisa.live.engine.KafkaSinkOutput", return_value=mock_kafka), governed():
            engine.reconcile([spec(kafka_outputs=[_kafka_output()])])
        [(qid, output_type)] = engine._groups
        with (
            governed(rows) as reads,
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
        ):
            await engine._poll(qid, output_type)
        mock_kafka.send.assert_called_once_with(rows)
        assert [key.role_id for _sql, key, _p in reads] == ["publisher"]

    async def test_poll_handles_exception_gracefully(self):
        """_poll must catch exceptions and not propagate them."""
        engine, q = await self._subscribed()
        p1, p2 = self._patch_poll_deps()
        with governed(RuntimeError("engine exploded")), p1, p2:
            await engine._poll("q1", sse_type())  # must not raise
        assert engine.is_registered("q1")
        assert q.empty()

    async def test_poll_with_none_record_returns_early(self):
        engine, q = await self._subscribed()
        p1, p2 = self._patch_poll_deps()
        with governed([]), p1, p2:
            await engine._poll("q1", sse_type())
        assert q.empty()

    async def test_poll_incremental_sql_uses_watermark(self):
        """The governed read carries the key's watermark filter."""
        engine, _q = await self._subscribed()
        with (
            governed([{"id": 1, "ts": "2026-03-01"}]) as reads,
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value="2026-01-15")),
            patch("provisa.live.watermark.set_watermark", AsyncMock()),
        ):
            await engine._poll("q1", sse_type())
        assert "\"ts\" > '2026-01-15'" in reads[0][0]

    async def test_two_keys_never_share_rows_or_a_watermark(self):
        """Each key is its own poll, its own rows and its own watermark."""
        engine = make_engine(make_pool_with_conn(_make_conn()))
        engine.reconcile([spec()])
        with governed():
            q_us = await engine.subscribe("q1", KEY)
            q_eu = await engine.subscribe("q1", OTHER_KEY)
        by_key = {KEY: [{"id": 1, "ts": "a"}], OTHER_KEY: [{"id": 2, "ts": "b"}]}
        set_wm = AsyncMock()
        with (
            governed(lambda _sql, key: by_key[key]),
            patch("provisa.live.watermark.get_watermark", AsyncMock(return_value=None)),
            patch("provisa.live.watermark.set_watermark", set_wm),
        ):
            await engine._poll("q1", sse_type(KEY))
            await engine._poll("q1", sse_type(OTHER_KEY))
        assert q_us.get_nowait() == [{"id": 1, "ts": "a"}] and q_us.empty()
        assert q_eu.get_nowait() == [{"id": 2, "ts": "b"}] and q_eu.empty()
        assert {c.args[2] for c in set_wm.call_args_list} == {sse_type(KEY), sse_type(OTHER_KEY)}


# ---------------------------------------------------------------------------
# TestSSEFanout
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
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
        real_q = fanout.subscribe()
        phantom = asyncio.Queue()
        # Must not raise; existing subscriber must remain intact
        fanout.unsubscribe(phantom)
        assert fanout.subscriber_count == 1
        assert real_q in [sq for _, sq in fanout._queues]

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


@pytest.mark.asyncio(loop_scope="session")
class TestWatermark:
    """Tests for get_watermark and set_watermark in provisa.live.watermark."""

    def _result(self, row):
        result = MagicMock()
        result.fetchone.return_value = row
        return result

    async def test_get_watermark_returns_value_when_row_exists(self):
        from provisa.live.watermark import get_watermark

        conn = _make_conn()
        conn.execute_core = AsyncMock(
            return_value=self._result(MagicMock(_mapping={"last_watermark": "2026-01-15"}))
        )
        assert await get_watermark(conn, "q1", "sse") == "2026-01-15"
        # filters by source + output_type
        compiled = str(
            conn.execute_core.await_args.args[0].compile(compile_kwargs={"literal_binds": True})
        )
        assert "q1" in compiled and "sse" in compiled

    async def test_get_watermark_returns_none_when_no_row(self):
        from provisa.live.watermark import get_watermark

        conn = _make_conn()
        conn.execute_core = AsyncMock(return_value=self._result(None))
        assert await get_watermark(conn, "q-missing", "sse") is None

    async def test_set_watermark_upserts_value(self):
        from provisa.live.watermark import set_watermark

        conn = _make_conn()
        conn.upsert = AsyncMock()
        await set_watermark(conn, "q1", "sse", "2026-03-20")
        conn.upsert.assert_awaited_once()
        table_arg, values = conn.upsert.await_args.args[0], conn.upsert.await_args.args[1]
        assert table_arg.name == "live_query_state"
        assert values["source"] == "q1"
        assert values["output_type"] == "sse"
        assert values["last_watermark"] == "2026-03-20"

    async def test_set_watermark_updates_watermark_on_conflict(self):
        from provisa.live.watermark import set_watermark

        conn = _make_conn()
        conn.upsert = AsyncMock()
        await set_watermark(conn, "q1", "sse", "2026-03-20")
        kwargs = conn.upsert.await_args.kwargs
        assert kwargs["index_elements"] == ["source", "output_type"]
        assert "last_watermark" in kwargs["update_columns"]


# ---------------------------------------------------------------------------
# TestKafkaSinkOutput
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
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
        assert producer.produce.call_count == 0

    async def test_send_calls_poll_after_produce(self):
        sink = self._make_sink()
        producer = self._inject_producer(sink)
        rows = [{"id": 1}]
        await sink.send(rows)
        producer.poll.assert_called_once_with(0)
        assert producer.poll.call_count == 1

    async def test_close_calls_flush(self):
        sink = self._make_sink()
        producer = self._inject_producer(sink)
        await sink.close()
        producer.flush.assert_called_once()
        assert producer.flush.call_count == 1

    async def test_close_clears_producer_reference(self):
        sink = self._make_sink()
        self._inject_producer(sink)
        await sink.close()
        assert sink._producer is None

    async def test_close_when_producer_is_none_is_silent(self):
        sink = self._make_sink()
        # _producer is None by default — close must not raise
        await sink.close()
        # Producer reference remains None; no flush attempted
        assert sink._producer is None

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
            _topic_arg = c[0][0] if c[0] else c[1].get("topic")
            # The topic is the first positional arg
            assert c[0][0] == "live-events"
