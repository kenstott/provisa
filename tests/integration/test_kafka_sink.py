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
                await ak_producer.send_and_wait(topic, value=json.dumps(row, cls=_Encoder).encode())
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


async def _read(topic: str, count: int) -> list[tuple[bytes | None, dict]]:
    """The first ``count`` messages on ``topic``, from its beginning."""
    import asyncio

    from aiokafka import AIOKafkaConsumer  # noqa: PLC0415

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=_kafka_bootstrap(),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    got: list[tuple[bytes | None, dict]] = []
    try:
        async with asyncio.timeout(60):
            async for message in consumer:
                got.append((message.key, json.loads(message.value.decode())))
                if len(got) == count:
                    break
    finally:
        await consumer.stop()
    return got


class TestTheProductsOwnProducerOnARealBroker:
    """Sinks and the live Kafka output once imported a Kafka client the product does not declare
    ("confluent-kafka is required ... pip install confluent-kafka"), so neither could publish in
    any install, and their unit tests passed against mocks. These go through the product's code
    to a real broker and read back what arrived."""

    pytestmark = [pytest.mark.requires_kafka]

    @pytest.fixture(autouse=True)
    async def _the_lifespan_stops_the_producers(self):
        yield
        from provisa.kafka import producer as kafka_producer

        await kafka_producer.stop_all()

    async def test_a_sinks_rows_land_on_its_topic_keyed_by_its_key_column(self):
        """REQ-176, REQ-181: each row is one JSON message, keyed by the sink's key column."""
        from provisa.kafka import producer as kafka_producer
        from provisa.kafka.sink import KafkaProducer

        topic = f"provisa-sink-{uuid.uuid4().hex[:8]}"
        sink = KafkaProducer(_kafka_bootstrap())
        rows = [
            {"id": 1, "region": "us-east", "amount": 10.5},
            {"id": 2, "region": "eu", "amount": 3},
        ]
        assert (
            await sink.publish_rows(
                topic, rows, columns=["id", "region", "amount"], key_column="id"
            )
            == 2
        )
        await kafka_producer.stop_all()  # as the lifespan does: what was handed over is sent

        assert await _read(topic, 2) == [(b"1", rows[0]), (b"2", rows[1])]

    async def test_a_change_triggered_table_sink_publishes_the_tables_rows(self):
        """REQ-176, REQ-177: the sink executor reads the table and publishes its rows."""
        from datetime import date
        from decimal import Decimal
        from types import SimpleNamespace

        from provisa.kafka import producer as kafka_producer
        from provisa.kafka.sink_executor import trigger_sinks_for_table

        topic = f"provisa-table-sink-{uuid.uuid4().hex[:8]}"

        class _Conn:
            async def fetch(self, _sql):
                return [{"id": 7, "total": Decimal("12.50"), "day": date(2026, 10, 8)}]

        class _Acquire:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, *_exc):
                return False

        table = SimpleNamespace(
            table_name="orders",
            schema_name="public",
            kafka_sink=SimpleNamespace(topic=topic, key_column="id", triggers=["change_event"]),
        )
        state = SimpleNamespace(
            config=SimpleNamespace(tables=[table]),
            tenant_db=SimpleNamespace(acquire=lambda: _Acquire()),
        )
        assert await trigger_sinks_for_table("orders", state) == 1
        await kafka_producer.stop_all()

        assert await _read(topic, 1) == [(b"7", {"id": 7, "total": 12.5, "day": "2026-10-08"})]

    async def test_a_live_outputs_rows_reach_its_topic(self):
        """REQ-286: the rows a live query emits are produced to the output's topic."""
        from provisa.kafka import producer as kafka_producer
        from provisa.live.outputs.kafka import KafkaSinkOutput

        topic = f"provisa-live-{uuid.uuid4().hex[:8]}"
        output = KafkaSinkOutput(_kafka_bootstrap(), topic, key_column="id")
        await output.send([{"id": 5, "status": "new"}, {"id": 6, "status": "open"}])
        await output.close()
        await kafka_producer.stop_all()

        assert await _read(topic, 2) == [
            (b"5", {"id": 5, "status": "new"}),
            (b"6", {"id": 6, "status": "open"}),
        ]
