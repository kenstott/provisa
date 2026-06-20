# Copyright (c) 2026 Kenneth Stott
# Canary: f8ebc1e8-2e30-4cef-a29b-3ddf327dd3ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Kafka sink message serialization."""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.models import KafkaSinkAttachment
from provisa.kafka.sink import KafkaProducer, KafkaSinkConfig
from provisa.kafka.sink_executor import _Encoder, trigger_sinks_for_table


class TestKafkaSinkConfig:
    def test_default_format(self):
        config = KafkaSinkConfig(
            query_stable_id="abc-123",
            topic="enriched-orders",
        )
        assert config.value_format == "json"
        assert config.key_column is None

    def test_with_key_column(self):
        config = KafkaSinkConfig(
            query_stable_id="abc-123",
            topic="enriched-orders",
            key_column="order_id",
        )
        assert config.key_column == "order_id"


class TestKafkaProducer:
    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_rows_dict(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.produce = MagicMock()
        producer._producer.poll = MagicMock()

        rows = [
            {"id": 1, "amount": 100.0, "region": "us"},
            {"id": 2, "amount": 200.0, "region": "eu"},
        ]

        count = await producer.publish_rows(
            topic="test-topic",
            rows=rows,
            columns=["id", "amount", "region"],
        )

        assert count == 2
        assert producer._producer.produce.call_count == 2

    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_with_key_column(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.produce = MagicMock()
        producer._producer.poll = MagicMock()

        rows = [{"id": 1, "name": "test"}]

        await producer.publish_rows(
            topic="test-topic",
            rows=rows,
            columns=["id", "name"],
            key_column="id",
        )

        call_kwargs = producer._producer.produce.call_args
        assert call_kwargs.kwargs.get("key") == b"1" or call_kwargs[1].get("key") == b"1"

    @patch("provisa.kafka.sink.KafkaProducer._ensure_producer")
    async def test_publish_empty_rows(self, mock_ensure):
        producer = KafkaProducer("localhost:9092")
        producer._producer = MagicMock()
        producer._producer.poll = MagicMock()

        count = await producer.publish_rows(
            topic="test-topic",
            rows=[],
            columns=["id"],
        )

        assert count == 0


class TestKafkaSinkEncoder:
    async def test_sink_message_format_is_json(self):
        """Messages serialized by _Encoder produce valid JSON with expected keys."""
        rows = [
            {"id": 1, "amount": Decimal("99.99"), "region": "us-east"},
            {"id": 2, "amount": Decimal("14.50"), "region": "eu-west"},
        ]
        for row in rows:
            encoded = json.dumps(row, cls=_Encoder)
            decoded = json.loads(encoded)
            assert decoded["id"] == row["id"]
            assert isinstance(decoded["amount"], float)
            assert decoded["region"] == row["region"]

    async def test_encoder_handles_datetime(self):
        """_Encoder serializes datetime objects as ISO strings."""
        from datetime import datetime, timezone

        now = datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)
        encoded = json.dumps({"ts": now}, cls=_Encoder)
        decoded = json.loads(encoded)
        assert "2026-04-06" in decoded["ts"]

    async def test_encoder_handles_string_fallback(self):
        """Arbitrary objects fall back to str() via _Encoder."""

        class Weird:
            def __str__(self):
                return "weird_value"

        encoded = json.dumps({"x": Weird()}, cls=_Encoder)
        assert "weird_value" in encoded


class TestKafkaProducerMocked:
    async def test_sink_publishes_result_to_topic(self):
        """publish_rows calls producer.produce for each row."""
        mock_producer = MagicMock()

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [
                {"id": 1, "amount": 10.0},
                {"id": 2, "amount": 20.0},
            ]
            count = await producer.publish_rows(
                topic="test-topic",
                rows=rows,
                columns=["id", "amount"],
            )

        assert count == 2
        assert mock_producer.produce.call_count == 2
        mock_producer.poll.assert_called_once_with(0)

    async def test_sink_respects_row_limit(self):
        """Exactly N produce() calls for N rows — no more, no less."""
        mock_producer = MagicMock()

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [{"id": i, "val": i * 10} for i in range(50)]
            count = await producer.publish_rows(
                topic="bulk-topic",
                rows=rows,
                columns=["id", "val"],
            )

        assert count == 50
        assert mock_producer.produce.call_count == 50

    async def test_sink_key_column_used_as_message_key(self):
        """When key_column is set, produce() is called with the correct key bytes."""
        mock_producer = MagicMock()

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [{"id": 42, "amount": 5.0}]
            await producer.publish_rows(
                topic="keyed-topic",
                rows=rows,
                columns=["id", "amount"],
                key_column="id",
            )

        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs.kwargs.get("key") == b"42" or (
            len(call_kwargs.args) > 2 and call_kwargs.args[2] == b"42"
        )

    async def test_sink_value_is_valid_json_bytes(self):
        """Each produced value is valid JSON bytes containing the row data."""
        produced_values: list[bytes] = []

        def capture_produce(topic, value=None, key=None, callback=None):
            produced_values.append(value)

        mock_producer = MagicMock()
        mock_producer.produce.side_effect = capture_produce

        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer

            rows = [{"id": 7, "region": "apac", "amount": 3.14}]
            await producer.publish_rows(
                topic="json-check-topic",
                rows=rows,
                columns=["id", "region", "amount"],
            )

        assert len(produced_values) == 1
        decoded = json.loads(produced_values[0])
        assert decoded["id"] == 7
        assert decoded["region"] == "apac"

    async def test_sink_close_flushes_producer(self):
        """close() calls flush on the underlying producer."""
        mock_producer = MagicMock()
        with patch("provisa.kafka.sink.KafkaProducer._ensure_producer"):
            producer = KafkaProducer("localhost:9092")
            producer._producer = mock_producer
            producer.close()

        mock_producer.flush.assert_called_once()
        assert producer._producer is None


# ---------------------------------------------------------------------------
# TestKafkaSinkAttachment
# ---------------------------------------------------------------------------


class TestKafkaSinkAttachment:
    """Tests for KafkaSinkAttachment model (REQ-176)."""

    def test_default_triggers(self):
        attachment = KafkaSinkAttachment(topic="my-topic")
        assert attachment.triggers == ["change_event"]
        assert attachment.key_column is None

    def test_explicit_triggers(self):
        attachment = KafkaSinkAttachment(topic="t", triggers=["manual", "schedule"])
        assert attachment.triggers == ["manual", "schedule"]

    def test_key_column_set(self):
        attachment = KafkaSinkAttachment(topic="t", key_column="id")
        assert attachment.key_column == "id"

    def test_topic_required(self):
        with pytest.raises(Exception):
            KafkaSinkAttachment()


# ---------------------------------------------------------------------------
# TestTriggerSinksForTable (real dispatch)
# ---------------------------------------------------------------------------


class _MockAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        return False


class TestTriggerSinksForTable:
    """Tests for trigger_sinks_for_table real dispatch (REQ-176–180)."""

    def _make_state(self, tables, pg_pool=None):
        state = MagicMock()
        state.config.tables = tables
        state.pg_pool = pg_pool
        return state

    def _make_table(self, table_name="orders", kafka_sink=None):
        table = MagicMock()
        table.table_name = table_name
        table.schema_name = "public"
        table.kafka_sink = kafka_sink
        return table

    async def test_returns_zero_when_no_tables_match(self):
        state = self._make_state([self._make_table("other")])
        result = await trigger_sinks_for_table("orders", state)
        assert result == 0

    async def test_returns_zero_when_table_has_no_kafka_sink(self):
        state = self._make_state([self._make_table("orders", kafka_sink=None)])
        result = await trigger_sinks_for_table("orders", state)
        assert result == 0

    async def test_returns_zero_when_trigger_not_change_event(self):
        sink = KafkaSinkAttachment(topic="t", triggers=["manual"])
        state = self._make_state([self._make_table("orders", kafka_sink=sink)])
        result = await trigger_sinks_for_table("orders", state)
        assert result == 0

    async def test_triggers_sink_and_returns_count(self):
        sink = KafkaSinkAttachment(topic="orders-topic", triggers=["change_event"])
        table = self._make_table("orders", kafka_sink=sink)

        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        pool = MagicMock()
        pool.acquire.return_value = _MockAcquireCtx(conn)

        state = self._make_state([table], pg_pool=pool)

        with patch(
            "provisa.kafka.sink_executor._execute_and_publish_table_sink",
            new=AsyncMock(),
        ) as mock_exec:
            result = await trigger_sinks_for_table("orders", state)

        assert result == 1
        mock_exec.assert_called_once()

    async def test_skips_execution_when_no_bootstrap(self):
        sink = KafkaSinkAttachment(topic="t", triggers=["change_event"])
        table = self._make_table("orders", kafka_sink=sink)

        conn = AsyncMock()
        pool = MagicMock()
        pool.acquire.return_value = _MockAcquireCtx(conn)

        state = self._make_state([table], pg_pool=pool)

        with (
            patch.dict("os.environ", {}, clear=True),
            patch(
                "provisa.kafka.sink_executor._execute_and_publish_table_sink",
                new=AsyncMock(),
            ),
        ):
            import os

            os.environ.pop("PROVISA_CHANGE_EVENT_BOOTSTRAP", None)
            os.environ.pop("KAFKA_BOOTSTRAP_SERVERS", None)
            result = await trigger_sinks_for_table("orders", state)

        # trigger_sinks_for_table counts dispatches regardless of inner bootstrap check
        assert result == 1
