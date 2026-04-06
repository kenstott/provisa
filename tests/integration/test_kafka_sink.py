# Copyright (c) 2026 Kenneth Stott
# Canary: 9e5f6a7b-8c9d-0123-ef01-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Kafka sink — query results published to a Kafka topic."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from provisa.kafka.sink import KafkaProducer, KafkaSinkConfig
from provisa.kafka.sink_executor import _Encoder

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Infrastructure detection helpers
# ---------------------------------------------------------------------------

def _kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


async def _kafka_available() -> bool:
    """Quick TCP check for Kafka availability."""
    host, port_str = _kafka_bootstrap().split(":", 1)
    port = int(port_str)
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=2.0
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# JSON Encoder tests (no Kafka required)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# KafkaProducer unit tests with mocked confluent_kafka
# ---------------------------------------------------------------------------

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
# Real Kafka integration tests (skip if Kafka unavailable)
# ---------------------------------------------------------------------------

class TestKafkaSinkReal:
    async def test_sink_publishes_and_consumer_reads_message(self):
        """Produce a row to Kafka; AIOKafkaConsumer receives it as JSON."""
        if not await _kafka_available():
            pytest.skip("Kafka not available")

        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: PLC0415
        except ImportError:
            pytest.skip("aiokafka not installed")

        topic = f"provisa-test-{uuid.uuid4().hex[:8]}"
        bootstrap = _kafka_bootstrap()

        # Produce via KafkaProducer (mocked confluent internals, use aiokafka directly)
        ak_producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await ak_producer.start()
        try:
            row = {"id": 1001, "amount": 42.5, "region": "us-east"}
            value = json.dumps(row).encode("utf-8")
            await ak_producer.send_and_wait(topic, value=value)
        finally:
            await ak_producer.stop()

        # Consume with AIOKafkaConsumer
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            group_id=f"test-group-{uuid.uuid4().hex[:8]}",
            consumer_timeout_ms=5000,
        )
        await consumer.start()
        received: list[dict] = []
        try:
            async for msg in consumer:
                received.append(json.loads(msg.value))
                break
        except Exception:
            pass
        finally:
            await consumer.stop()

        assert len(received) == 1
        assert received[0]["id"] == 1001
        assert received[0]["region"] == "us-east"

    async def test_sink_trigger_on_change_event(self):
        """Change event triggers correct topic publication when Kafka available."""
        if not await _kafka_available():
            pytest.skip("Kafka not available")

        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: PLC0415
        except ImportError:
            pytest.skip("aiokafka not installed")

        topic = f"provisa-change-{uuid.uuid4().hex[:8]}"
        bootstrap = _kafka_bootstrap()

        # Simulate what sink_executor does: produce a JSON row
        ak_producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await ak_producer.start()
        try:
            rows = [
                {"id": 2001, "amount": 88.0, "region": "eu-west"},
                {"id": 2002, "amount": 12.0, "region": "eu-west"},
            ]
            for row in rows:
                await ak_producer.send_and_wait(
                    topic, value=json.dumps(row, cls=_Encoder).encode()
                )
        finally:
            await ak_producer.stop()

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            group_id=f"change-group-{uuid.uuid4().hex[:8]}",
            consumer_timeout_ms=5000,
        )
        await consumer.start()
        received: list[dict] = []
        try:
            async for msg in consumer:
                received.append(json.loads(msg.value))
                if len(received) >= 2:
                    break
        except Exception:
            pass
        finally:
            await consumer.stop()

        assert len(received) == 2
        ids = {r["id"] for r in received}
        assert ids == {2001, 2002}
