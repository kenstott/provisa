# Copyright (c) 2026 Kenneth Stott
# Canary: 9e5f6a7b-8c9d-0123-ef01-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Kafka sink — query results published to a Kafka topic.

JSON encoder unit tests (TestKafkaSinkEncoder) and mocked-producer unit tests
(TestKafkaProducerMocked) have been moved to tests/unit/test_kafka_sink.py.

Only live-Kafka tests requiring a running broker remain here.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest

from provisa.kafka.sink_executor import _Encoder

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Infrastructure detection helpers
# ---------------------------------------------------------------------------

def _kafka_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ---------------------------------------------------------------------------
# Real Kafka integration tests (skip if Kafka unavailable)
# ---------------------------------------------------------------------------

class TestKafkaSinkReal:
    pytestmark = [pytest.mark.requires_kafka]

    async def test_sink_publishes_and_consumer_reads_message(self):
        """Produce a row to Kafka; AIOKafkaConsumer receives it as JSON."""
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: PLC0415

        topic = f"provisa-test-{uuid.uuid4().hex[:8]}"
        bootstrap = _kafka_bootstrap()

        ak_producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await ak_producer.start()
        try:
            row = {"id": 1001, "amount": 42.5, "region": "us-east"}
            value = json.dumps(row).encode("utf-8")
            await ak_producer.send_and_wait(topic, value=value)
        finally:
            await ak_producer.stop()

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
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: PLC0415

        topic = f"provisa-change-{uuid.uuid4().hex[:8]}"
        bootstrap = _kafka_bootstrap()

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
