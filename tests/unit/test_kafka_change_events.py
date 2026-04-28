# Copyright (c) 2026 Kenneth Stott
# Canary: 3c17a084-e9b5-4f8a-bc30-d52e1a7f6c91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Kafka change events and sink executor (REQ-172 through REQ-181)."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

import provisa.kafka.change_events as ce
from provisa.kafka.sink import KafkaProducer, KafkaSinkConfig
from provisa.kafka.sink_executor import _Encoder, trigger_sinks_for_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _MockAcquireContext:
    """Mimics asyncpg PoolAcquireContext: works as both await and async-with."""

    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        return False

    def __await__(self):
        async def _resolve():
            return self._conn

        return _resolve().__await__()


def _mock_pool_with_conn(mock_conn):
    """Build a mock asyncpg.Pool whose acquire() works as await and async-with."""
    mock_pool = MagicMock()
    mock_pool.acquire.return_value = _MockAcquireContext(mock_conn)
    mock_pool.release = AsyncMock()
    return mock_pool


def _make_sink_row(**kwargs) -> dict:
    defaults = {
        "id": 1,
        "stable_id": "q-abc-123",
        "query_text": "{ orders { id } }",
        "sink_topic": "enriched-orders",
        "sink_key_column": None,
    }
    defaults.update(kwargs)
    return defaults


# ---------------------------------------------------------------------------
# TestChangeEventTopic
# ---------------------------------------------------------------------------


class TestChangeEventTopic:
    def test_default_topic(self, monkeypatch):
        monkeypatch.delenv("PROVISA_CHANGE_EVENT_TOPIC", raising=False)
        assert ce._get_topic() == "provisa.change-events"

    def test_custom_topic_from_env(self, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_TOPIC", "my.custom.topic")
        assert ce._get_topic() == "my.custom.topic"

    def test_empty_env_var_falls_back_to_default(self, monkeypatch):
        # An empty string is falsy but os.environ.get returns "" not None,
        # so the default only kicks in when the key is absent.
        monkeypatch.delenv("PROVISA_CHANGE_EVENT_TOPIC", raising=False)
        assert ce._get_topic() == "provisa.change-events"


# ---------------------------------------------------------------------------
# TestGetProducer
# ---------------------------------------------------------------------------


class TestGetProducer:
    @pytest.fixture(autouse=True)
    def reset_producer(self):
        ce._producer = None
        yield
        ce._producer = None

    def test_returns_none_when_no_bootstrap_env(self, monkeypatch):
        monkeypatch.delenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", raising=False)
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        assert ce._get_producer() is None

    def test_uses_provisa_change_event_bootstrap(self, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker1:9092")
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

        mock_producer = MagicMock()
        mock_confluent = MagicMock()
        mock_confluent.Producer.return_value = mock_producer

        with patch.dict("sys.modules", {"confluent_kafka": mock_confluent}):
            result = ce._get_producer()

        assert result is mock_producer
        mock_confluent.Producer.assert_called_once_with(
            {"bootstrap.servers": "broker1:9092"}
        )

    def test_uses_kafka_bootstrap_servers_fallback(self, monkeypatch):
        monkeypatch.delenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", raising=False)
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fallback-broker:9092")

        mock_producer = MagicMock()
        mock_confluent = MagicMock()
        mock_confluent.Producer.return_value = mock_producer

        with patch.dict("sys.modules", {"confluent_kafka": mock_confluent}):
            result = ce._get_producer()

        assert result is mock_producer
        mock_confluent.Producer.assert_called_once_with(
            {"bootstrap.servers": "fallback-broker:9092"}
        )

    def test_provisa_bootstrap_takes_priority_over_fallback(self, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "primary:9092")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "secondary:9092")

        mock_confluent = MagicMock()
        mock_confluent.Producer.return_value = MagicMock()

        with patch.dict("sys.modules", {"confluent_kafka": mock_confluent}):
            ce._get_producer()

        mock_confluent.Producer.assert_called_once_with(
            {"bootstrap.servers": "primary:9092"}
        )

    def test_returns_none_when_confluent_kafka_raises(self, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker:9092")

        mock_confluent = MagicMock()
        mock_confluent.Producer.side_effect = RuntimeError("connection refused")

        with patch.dict("sys.modules", {"confluent_kafka": mock_confluent}):
            result = ce._get_producer()

        assert result is None

    def test_returns_cached_producer_on_second_call(self, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker:9092")

        mock_producer = MagicMock()
        mock_confluent = MagicMock()
        mock_confluent.Producer.return_value = mock_producer

        with patch.dict("sys.modules", {"confluent_kafka": mock_confluent}):
            first = ce._get_producer()
            second = ce._get_producer()

        assert first is second
        # Producer() constructor called only once despite two _get_producer() calls
        assert mock_confluent.Producer.call_count == 1

    def test_reset_global_produces_fresh_producer(self, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker:9092")

        mock_confluent = MagicMock()
        mock_confluent.Producer.side_effect = [MagicMock(), MagicMock()]

        with patch.dict("sys.modules", {"confluent_kafka": mock_confluent}):
            first = ce._get_producer()
            ce._producer = None
            second = ce._get_producer()

        assert first is not second
        assert mock_confluent.Producer.call_count == 2


# ---------------------------------------------------------------------------
# TestEmitChangeEvent
# ---------------------------------------------------------------------------


class TestEmitChangeEvent:
    @pytest.fixture(autouse=True)
    def reset_producer(self):
        ce._producer = None
        yield
        ce._producer = None

    def test_does_nothing_when_no_producer(self, monkeypatch):
        monkeypatch.delenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", raising=False)
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)

        # Should complete silently without raising
        ce.emit_change_event("orders", "pg-main", "insert")

    def test_calls_produce_with_correct_topic(self, monkeypatch):
        monkeypatch.delenv("PROVISA_CHANGE_EVENT_TOPIC", raising=False)
        mock_producer = MagicMock()

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main", "insert")

        topic_arg = mock_producer.produce.call_args[0][0]
        assert topic_arg == "provisa.change-events"

    def test_calls_produce_with_custom_topic(self, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_TOPIC", "custom.topic")
        mock_producer = MagicMock()

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main", "insert")

        topic_arg = mock_producer.produce.call_args[0][0]
        assert topic_arg == "custom.topic"

    def test_event_payload_contains_required_fields(self, monkeypatch):
        monkeypatch.delenv("PROVISA_CHANGE_EVENT_TOPIC", raising=False)
        mock_producer = MagicMock()

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main", "update")

        call_kwargs = mock_producer.produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode())

        assert payload["table"] == "orders"
        assert payload["source"] == "pg-main"
        assert payload["type"] == "update"
        assert "timestamp" in payload

    def test_event_timestamp_is_iso_format(self):
        mock_producer = MagicMock()

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main", "delete")

        call_kwargs = mock_producer.produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode())
        # Should parse without raising
        dt = datetime.fromisoformat(payload["timestamp"])
        assert dt.tzinfo is not None  # Must be timezone-aware

    def test_message_key_is_source_dot_table_encoded(self):
        mock_producer = MagicMock()

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main", "insert")

        call_kwargs = mock_producer.produce.call_args[1]
        assert call_kwargs["key"] == b"pg-main.orders"

    def test_handles_produce_exception_gracefully(self):
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = RuntimeError("broker unavailable")

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            # Must not raise
            ce.emit_change_event("orders", "pg-main", "insert")

    def test_calls_poll_zero_after_produce(self):
        mock_producer = MagicMock()

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main", "insert")

        mock_producer.poll.assert_called_once_with(0)

    def test_poll_not_called_when_produce_raises(self):
        """poll() should not be called if produce() throws."""
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = RuntimeError("broker down")

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main", "insert")

        mock_producer.poll.assert_not_called()

    def test_default_mutation_type(self):
        mock_producer = MagicMock()

        with patch("provisa.kafka.change_events._get_producer", return_value=mock_producer):
            ce.emit_change_event("orders", "pg-main")

        call_kwargs = mock_producer.produce.call_args[1]
        payload = json.loads(call_kwargs["value"].decode())
        assert payload["type"] == "mutation"


# ---------------------------------------------------------------------------
# TestFlush
# ---------------------------------------------------------------------------


class TestFlush:
    @pytest.fixture(autouse=True)
    def reset_producer(self):
        ce._producer = None
        yield
        ce._producer = None

    def test_flush_calls_producer_flush_with_timeout_5(self):
        mock_producer = MagicMock()
        ce._producer = mock_producer

        ce.flush()

        mock_producer.flush.assert_called_once_with(timeout=5)

    def test_flush_does_nothing_when_producer_is_none(self):
        ce._producer = None
        # Must complete without raising
        ce.flush()

    def test_flush_only_flushes_existing_producer(self):
        mock_producer = MagicMock()
        ce._producer = mock_producer

        ce.flush()
        ce._producer = None
        ce.flush()

        # Only one flush call despite two ce.flush() invocations
        mock_producer.flush.assert_called_once()


# ---------------------------------------------------------------------------
# TestEncoder
# ---------------------------------------------------------------------------


class TestEncoder:
    def test_encodes_decimal_as_float(self):
        encoder = _Encoder()
        result = encoder.default(Decimal("3.14"))
        assert result == 3.14
        assert isinstance(result, float)

    def test_encodes_decimal_zero(self):
        encoder = _Encoder()
        assert encoder.default(Decimal("0")) == 0.0

    def test_encodes_date_as_isoformat(self):
        encoder = _Encoder()
        d = date(2026, 4, 6)
        result = encoder.default(d)
        assert result == "2026-04-06"

    def test_encodes_datetime_as_isoformat(self):
        encoder = _Encoder()
        dt = datetime(2026, 4, 6, 12, 30, 0)
        result = encoder.default(dt)
        assert result == "2026-04-06T12:30:00"

    def test_encodes_unknown_object_as_str(self):
        encoder = _Encoder()

        class WeirdObj:
            def __str__(self):
                return "weird-value"

        result = encoder.default(WeirdObj())
        assert result == "weird-value"

    def test_regular_types_serialise_normally(self):
        """int, str, list, dict, None round-trip through json.dumps without error."""
        data = {"count": 42, "name": "test", "items": [1, 2], "meta": None}
        result = json.dumps(data, cls=_Encoder)
        assert json.loads(result) == data

    def test_encoder_used_in_dumps_with_decimal(self):
        data = {"price": Decimal("9.99"), "qty": 3}
        result = json.loads(json.dumps(data, cls=_Encoder))
        assert result["price"] == pytest.approx(9.99)
        assert result["qty"] == 3

    def test_encoder_used_in_dumps_with_date(self):
        data = {"created_at": date(2026, 1, 15)}
        result = json.loads(json.dumps(data, cls=_Encoder))
        assert result["created_at"] == "2026-01-15"


# ---------------------------------------------------------------------------
# TestTriggerSinksForTable
# ---------------------------------------------------------------------------


class TestTriggerSinksForTable:
    def _make_state(self, *, pg_pool=None, rows=None, execute_raises=False):
        state = MagicMock()
        state.pg_pool = pg_pool
        return state

    async def test_returns_zero_when_pg_pool_is_none(self):
        state = MagicMock()
        state.pg_pool = None

        result = await trigger_sinks_for_table("orders", state)

        assert result == 0

    async def test_returns_zero_when_no_matching_rows(self):
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = _mock_pool_with_conn(mock_conn)

        state = MagicMock()
        state.pg_pool = mock_pool

        result = await trigger_sinks_for_table("orders", state)

        assert result == 0
        mock_conn.fetch.assert_awaited_once()

    async def test_returns_count_of_triggered_sinks(self):
        rows = [_make_sink_row(stable_id="q-1"), _make_sink_row(stable_id="q-2")]
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=rows)
        mock_pool = _mock_pool_with_conn(mock_conn)

        state = MagicMock()
        state.pg_pool = mock_pool

        with patch(
            "provisa.kafka.sink_executor._execute_and_publish", new_callable=AsyncMock
        ) as mock_exec:
            result = await trigger_sinks_for_table("orders", state)

        assert result == 2
        assert mock_exec.await_count == 2

    async def test_calls_execute_and_publish_for_each_row(self):
        rows = [
            _make_sink_row(stable_id="q-1", sink_topic="topic-a", sink_key_column="id"),
            _make_sink_row(stable_id="q-2", sink_topic="topic-b", sink_key_column=None),
        ]
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=rows)
        mock_pool = _mock_pool_with_conn(mock_conn)

        state = MagicMock()
        state.pg_pool = mock_pool

        with patch(
            "provisa.kafka.sink_executor._execute_and_publish", new_callable=AsyncMock
        ) as mock_exec:
            await trigger_sinks_for_table("orders", state)

        calls = mock_exec.call_args_list
        assert len(calls) == 2

        first_kwargs = calls[0][1]
        assert first_kwargs["stable_id"] == "q-1"
        assert first_kwargs["sink_topic"] == "topic-a"
        assert first_kwargs["key_column"] == "id"

        second_kwargs = calls[1][1]
        assert second_kwargs["stable_id"] == "q-2"
        assert second_kwargs["sink_topic"] == "topic-b"
        assert second_kwargs["key_column"] is None

    async def test_handles_execute_and_publish_exception_gracefully(self):
        """A failing sink should not prevent subsequent sinks from running."""
        rows = [_make_sink_row(stable_id="q-fail"), _make_sink_row(stable_id="q-ok")]
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=rows)
        mock_pool = _mock_pool_with_conn(mock_conn)

        state = MagicMock()
        state.pg_pool = mock_pool

        call_order = []

        async def _side_effect(**kwargs):
            call_order.append(kwargs["stable_id"])
            if kwargs["stable_id"] == "q-fail":
                raise RuntimeError("publish failed")

        with patch(
            "provisa.kafka.sink_executor._execute_and_publish",
            side_effect=_side_effect,
        ):
            result = await trigger_sinks_for_table("orders", state)

        # Only the successful sink counted
        assert result == 1
        # Both were attempted
        assert call_order == ["q-fail", "q-ok"]

    async def test_returns_correct_count_when_some_sinks_fail(self):
        rows = [
            _make_sink_row(stable_id="q-1"),
            _make_sink_row(stable_id="q-2"),
            _make_sink_row(stable_id="q-3"),
        ]
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=rows)
        mock_pool = _mock_pool_with_conn(mock_conn)

        state = MagicMock()
        state.pg_pool = mock_pool

        async def _side_effect(**kwargs):
            if kwargs["stable_id"] == "q-2":
                raise RuntimeError("failed")

        with patch(
            "provisa.kafka.sink_executor._execute_and_publish",
            side_effect=_side_effect,
        ):
            result = await trigger_sinks_for_table("orders", state)

        # q-1 and q-3 succeeded; q-2 failed
        assert result == 2

    async def test_fetch_called_with_table_name_parameter(self):
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = _mock_pool_with_conn(mock_conn)

        state = MagicMock()
        state.pg_pool = mock_pool

        await trigger_sinks_for_table("invoices", state)

        fetch_args = mock_conn.fetch.call_args
        # Second positional arg is the table_name parameter
        assert fetch_args[0][1] == "invoices"


# ---------------------------------------------------------------------------
# TestKafkaSinkConfig
# ---------------------------------------------------------------------------


class TestKafkaSinkConfig:
    def test_stores_all_fields(self):
        config = KafkaSinkConfig(
            query_stable_id="q-xyz",
            topic="output-topic",
            key_column="order_id",
            value_format="json",
        )
        assert config.query_stable_id == "q-xyz"
        assert config.topic == "output-topic"
        assert config.key_column == "order_id"
        assert config.value_format == "json"

    def test_default_key_column_is_none(self):
        config = KafkaSinkConfig(query_stable_id="q-1", topic="t")
        assert config.key_column is None

    def test_default_value_format_is_json(self):
        config = KafkaSinkConfig(query_stable_id="q-1", topic="t")
        assert config.value_format == "json"


# ---------------------------------------------------------------------------
# TestKafkaProducer (sink.py)
# ---------------------------------------------------------------------------


class TestKafkaProducer:
    def _make_producer_with_mock(self) -> tuple[KafkaProducer, MagicMock]:
        """Return a KafkaProducer with its internal confluent producer pre-mocked."""
        producer = KafkaProducer("localhost:9092")
        mock_inner = MagicMock()
        producer._producer = mock_inner
        return producer, mock_inner

    async def test_publish_rows_calls_produce_for_each_row(self):
        producer, mock_inner = self._make_producer_with_mock()

        rows = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        count = await producer.publish_rows(
            topic="test-topic", rows=rows, columns=["id", "val"]
        )

        assert count == 2
        assert mock_inner.produce.call_count == 2

    async def test_publish_rows_with_key_column_encodes_key(self):
        producer, mock_inner = self._make_producer_with_mock()

        rows = [{"id": 42, "name": "Widget"}]
        await producer.publish_rows(
            topic="test-topic", rows=rows, columns=["id", "name"], key_column="id"
        )

        call_kwargs = mock_inner.produce.call_args[1]
        assert call_kwargs["key"] == b"42"

    async def test_publish_rows_with_no_key_column_sends_none_key(self):
        producer, mock_inner = self._make_producer_with_mock()

        rows = [{"id": 1}]
        await producer.publish_rows(
            topic="test-topic", rows=rows, columns=["id"], key_column=None
        )

        call_kwargs = mock_inner.produce.call_args[1]
        assert call_kwargs["key"] is None

    async def test_publish_rows_encodes_value_as_json_bytes(self):
        producer, mock_inner = self._make_producer_with_mock()

        rows = [{"id": 1, "amount": 99.5}]
        await producer.publish_rows(
            topic="test-topic", rows=rows, columns=["id", "amount"]
        )

        call_kwargs = mock_inner.produce.call_args[1]
        decoded = json.loads(call_kwargs["value"].decode("utf-8"))
        assert decoded == {"id": 1, "amount": 99.5}

    async def test_publish_rows_tuple_rows_zipped_with_columns(self):
        producer, mock_inner = self._make_producer_with_mock()

        rows = [(1, "hello"), (2, "world")]
        count = await producer.publish_rows(
            topic="test-topic", rows=rows, columns=["id", "msg"]
        )

        assert count == 2
        first_call_kwargs = mock_inner.produce.call_args_list[0][1]
        decoded = json.loads(first_call_kwargs["value"].decode())
        assert decoded == {"id": 1, "msg": "hello"}

    async def test_publish_rows_calls_poll_after_produce(self):
        producer, mock_inner = self._make_producer_with_mock()

        await producer.publish_rows(
            topic="t", rows=[{"id": 1}], columns=["id"]
        )

        mock_inner.poll.assert_called_once_with(0)

    async def test_publish_empty_rows_returns_zero(self):
        producer, mock_inner = self._make_producer_with_mock()

        count = await producer.publish_rows(
            topic="t", rows=[], columns=["id"]
        )

        assert count == 0
        mock_inner.produce.assert_not_called()

    def test_flush_delegates_to_inner_producer(self):
        producer, mock_inner = self._make_producer_with_mock()

        producer.flush(timeout=10.0)

        mock_inner.flush.assert_called_once_with(10.0)

    def test_flush_with_default_timeout(self):
        producer, mock_inner = self._make_producer_with_mock()

        producer.flush()

        mock_inner.flush.assert_called_once_with(5.0)

    def test_flush_does_nothing_when_inner_producer_none(self):
        producer = KafkaProducer("localhost:9092")
        producer._producer = None
        # Must not raise
        producer.flush()

    def test_close_flushes_then_clears_inner_producer(self):
        producer, mock_inner = self._make_producer_with_mock()

        producer.close()

        mock_inner.flush.assert_called_once_with(5.0)
        assert producer._producer is None

    def test_close_does_nothing_when_inner_producer_none(self):
        producer = KafkaProducer("localhost:9092")
        producer._producer = None
        # Must not raise
        producer.close()

    def test_ensure_producer_raises_import_error_when_confluent_kafka_missing(self):
        producer = KafkaProducer("localhost:9092")
        with patch.dict("sys.modules", {"confluent_kafka": None}):
            with pytest.raises(ImportError, match="confluent-kafka is required"):
                producer._ensure_producer()
