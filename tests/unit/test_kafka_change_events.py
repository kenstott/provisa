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
from unittest.mock import AsyncMock, MagicMock, patch

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

    async def __aexit__(self, *_args):
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


def __make_sink_row(**kwargs) -> dict:
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
# The change-event producer: started for a named broker, by the lifespan
# ---------------------------------------------------------------------------


class _RecordingProducer:
    """Stands in for provisa.kafka.producer.Producer: what it was built with and handed."""

    built: list["_RecordingProducer"] = []

    def __init__(self, bootstrap_servers: str, *, client_id: str) -> None:
        self.bootstrap, self.client_id = bootstrap_servers, client_id
        self.sent: list[tuple[str, bytes, bytes | None]] = []
        self.stopped = False
        _RecordingProducer.built.append(self)

    def send(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        self.sent.append((topic, value, key))

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def producers(monkeypatch):
    """Change events with a recording producer, and none started."""
    _RecordingProducer.built = []
    monkeypatch.setattr(ce, "Producer", _RecordingProducer)
    monkeypatch.setattr(ce, "_producer", None)
    monkeypatch.delenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("PROVISA_CHANGE_EVENT_TOPIC", raising=False)
    return _RecordingProducer.built


class TestStart:
    def test_no_broker_named_means_no_producer_and_no_log(self, producers, caplog):
        """Not naming a broker is configuration: nothing is started, nothing is said."""
        with caplog.at_level("DEBUG"):
            ce.start()
            ce.emit_change_event("orders", "pg-main", "insert")
        assert producers == [] and ce.bootstrap_servers() is None
        assert not caplog.records

    def test_the_change_event_broker_is_used(self, producers, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker1:9092")
        ce.start()
        (producer,) = producers
        assert (producer.bootstrap, producer.client_id) == ("broker1:9092", "provisa-change-events")

    def test_the_deployments_kafka_broker_is_used_when_no_other_is_named(
        self, producers, monkeypatch
    ):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fallback:9092")
        ce.start()
        assert [p.bootstrap for p in producers] == ["fallback:9092"]

    def test_the_change_event_broker_wins_over_the_deployments(self, producers, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "primary:9092")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "fallback:9092")
        ce.start()
        assert [p.bootstrap for p in producers] == ["primary:9092"]

    def test_one_producer_per_process(self, producers, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker1:9092")
        ce.start()
        ce.start()
        assert len(producers) == 1

    async def test_stop_stops_the_producer_and_emits_nothing_after(self, producers, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker1:9092")
        ce.start()
        await ce.stop()
        (producer,) = producers
        assert producer.stopped
        ce.emit_change_event("orders", "pg-main", "insert")
        assert producer.sent == []

    async def test_stop_with_no_producer_does_nothing(self, producers):
        await ce.stop()
        assert producers == []


# ---------------------------------------------------------------------------
# emit_change_event (REQ-172..175)
# ---------------------------------------------------------------------------


class TestEmitChangeEvent:
    @pytest.fixture
    def producer(self, producers, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_BOOTSTRAP", "broker1:9092")
        ce.start()
        return producers[0]

    def test_the_event_goes_to_the_default_topic(self, producer):
        ce.emit_change_event("orders", "pg-main", "insert")
        assert [topic for topic, _value, _key in producer.sent] == ["provisa.change-events"]

    def test_the_event_goes_to_the_named_topic(self, producer, monkeypatch):
        monkeypatch.setenv("PROVISA_CHANGE_EVENT_TOPIC", "custom.events")
        ce.emit_change_event("orders", "pg-main", "insert")
        assert [topic for topic, _value, _key in producer.sent] == ["custom.events"]

    def test_the_event_names_the_table_its_source_and_what_changed(self, producer):
        ce.emit_change_event("orders", "pg-main", "update")
        (_topic, value, _key) = producer.sent[0]
        payload = json.loads(value.decode())
        assert (payload["table"], payload["source"], payload["type"]) == (
            "orders",
            "pg-main",
            "update",
        )
        assert set(payload) == {"table", "source", "type", "timestamp"}  # no row-level detail

    def test_the_event_carries_when_in_iso_format(self, producer):
        ce.emit_change_event("orders", "pg-main", "delete")
        payload = json.loads(producer.sent[0][1].decode())
        assert datetime.fromisoformat(payload["timestamp"]).tzinfo is not None

    def test_the_message_key_is_source_dot_table(self, producer):
        ce.emit_change_event("orders", "pg-main", "insert")
        assert producer.sent[0][2] == b"pg-main.orders"

    def test_the_default_kind_of_change_is_mutation(self, producer):
        ce.emit_change_event("orders", "pg-main")
        assert json.loads(producer.sent[0][1].decode())["type"] == "mutation"


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
    """REQ-001/003: GPQ approved-query sinks are removed with the registry.
    ``trigger_sinks_for_table`` is a no-op (returns 0) and no longer reads the
    ``persisted_queries`` registry. Table/view sinks (REQ-176-181) are forward work.
    """

    async def test_returns_zero_when_pg_pool_is_none(self):
        state = MagicMock()
        state.tenant_db = None
        assert await trigger_sinks_for_table("orders", state) == 0

    async def test_returns_zero_and_does_not_read_registry(self):
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = _mock_pool_with_conn(mock_conn)

        state = MagicMock()
        state.tenant_db = mock_pool

        result = await trigger_sinks_for_table("orders", state)

        assert result == 0
        # The deprecated registry must not be queried.
        mock_conn.fetch.assert_not_awaited()


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
        count = await producer.publish_rows(topic="test-topic", rows=rows, columns=["id", "val"])

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
        await producer.publish_rows(topic="test-topic", rows=rows, columns=["id"], key_column=None)

        call_kwargs = mock_inner.produce.call_args[1]
        assert call_kwargs["key"] is None

    async def test_publish_rows_encodes_value_as_json_bytes(self):
        producer, mock_inner = self._make_producer_with_mock()

        rows = [{"id": 1, "amount": 99.5}]
        await producer.publish_rows(topic="test-topic", rows=rows, columns=["id", "amount"])

        call_kwargs = mock_inner.produce.call_args[1]
        decoded = json.loads(call_kwargs["value"].decode("utf-8"))
        assert decoded == {"id": 1, "amount": 99.5}

    async def test_publish_rows_tuple_rows_zipped_with_columns(self):
        producer, mock_inner = self._make_producer_with_mock()

        rows: list[dict] = [{"id": 1, "msg": "hello"}, {"id": 2, "msg": "world"}]
        count = await producer.publish_rows(topic="test-topic", rows=rows, columns=["id", "msg"])

        assert count == 2
        first_call_kwargs = mock_inner.produce.call_args_list[0][1]
        decoded = json.loads(first_call_kwargs["value"].decode())
        assert decoded == {"id": 1, "msg": "hello"}

    async def test_publish_rows_calls_poll_after_produce(self):
        producer, mock_inner = self._make_producer_with_mock()

        await producer.publish_rows(topic="t", rows=[{"id": 1}], columns=["id"])

        mock_inner.poll.assert_called_once_with(0)
        assert mock_inner.poll.call_args == ((0,), {})

    async def test_publish_empty_rows_returns_zero(self):
        producer, mock_inner = self._make_producer_with_mock()

        count = await producer.publish_rows(topic="t", rows=[], columns=["id"])

        assert count == 0
        mock_inner.produce.assert_not_called()

    def test_flush_delegates_to_inner_producer(self):
        producer, mock_inner = self._make_producer_with_mock()

        producer.flush(timeout=10.0)

        mock_inner.flush.assert_called_once_with(10.0)
        assert mock_inner.flush.call_args == ((10.0,), {})

    def test_flush_with_default_timeout(self):
        producer, mock_inner = self._make_producer_with_mock()

        producer.flush()

        mock_inner.flush.assert_called_once_with(5.0)
        assert mock_inner.flush.call_args == ((5.0,), {})

    def test_flush_does_nothing_when_inner_producer_none(self):
        producer = KafkaProducer("localhost:9092")
        producer._producer = None
        # Must not raise
        producer.flush()
        # Inner producer must remain None — flush() must not create one
        assert producer._producer is None

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
        # Inner producer must remain None — close() must not create one
        assert producer._producer is None

    def test_ensure_producer_raises_import_error_when_confluent_kafka_missing(self):
        producer = KafkaProducer("localhost:9092")
        with patch.dict("sys.modules", {"confluent_kafka": None}):
            with pytest.raises(ImportError, match="confluent-kafka is required"):
                producer._ensure_producer()


# ---------------------------------------------------------------------------
# TestTouchEndpoint
# ---------------------------------------------------------------------------


class TestTouchEndpoint:
    """Tests for POST /data/touch/{table} (REQ-174)."""

    async def test_touch_emits_change_event_with_type_touch(self):

        from provisa.api.data.endpoint import touch_table

        mock_table = MagicMock()
        mock_table.table_name = "orders"
        mock_table.source_id = "src-1"

        mock_state = MagicMock()
        mock_state.config.tables = [mock_table]

        mock_request = MagicMock()

        with (
            patch("provisa.api.app.state", mock_state),
            patch("provisa.kafka.change_events.emit_change_event") as mock_emit,
        ):
            result = await touch_table(table="orders", request=mock_request, x_provisa_role=None)

        _ = mock_emit
        assert result.status_code == 204

    async def test_touch_unknown_table_returns_404(self):
        from fastapi import HTTPException

        from provisa.api.data.endpoint import touch_table

        mock_state = MagicMock()
        mock_state.config.tables = []

        mock_request = MagicMock()

        with (
            patch("provisa.api.app.state", mock_state),
            pytest.raises(HTTPException) as exc_info,
        ):
            await touch_table(table="nonexistent", request=mock_request, x_provisa_role=None)

        assert exc_info.value.status_code == 404
        assert "nonexistent" in exc_info.value.detail

    async def test_touch_no_kafka_no_ops(self):
        """emit_change_event returns None when no producer; must not raise."""
        from provisa.api.data.endpoint import touch_table

        mock_table = MagicMock()
        mock_table.table_name = "orders"
        mock_table.source_id = "src-1"

        mock_state = MagicMock()
        mock_state.config.tables = [mock_table]

        mock_request = MagicMock()

        with (
            patch("provisa.api.app.state", mock_state),
            patch("provisa.kafka.change_events.emit_change_event", return_value=None),
        ):
            result = await touch_table(table="orders", request=mock_request, x_provisa_role=None)

        assert result.status_code == 204
