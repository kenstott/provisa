# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7d0e62-91ab-4f35-8e27-b13fa6d5c908
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On a real broker: a listener the broker dropped from its group keeps landing (REQ-1733, REQ-1734).

A landing that holds the listener's loop past the session timeout stops its heartbeats, and the
broker drops the consumer. The commit of that batch is refused. On 2026-11-01 that ended the
listener, and a subscription on the table received nothing more; here the listener goes on, and
what is published afterwards lands."""

# Requirements: REQ-1733, REQ-1734, REQ-258

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import uuid

import pytest

from provisa.subscriptions.cdc_landing import consume_cdc_into_store
from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_kafka,
    pytest.mark.asyncio(loop_scope="session"),
]

_SESSION_MS = 6000  # the broker's least (group.min.session.timeout.ms)


async def _publish(bootstrap: str, topic: str, order_id: str) -> None:
    from aiokafka import AIOKafkaProducer  # noqa: PLC0415

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(topic, json.dumps({"order_id": order_id}).encode())
    finally:
        await producer.stop()


async def test_a_consumer_the_broker_dropped_keeps_landing(caplog):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = f"provisa-test-{uuid.uuid4().hex[:8]}"
    landed: list[str] = []
    disconnect = threading.Event()
    await _publish(bootstrap, topic, "first")

    async def _land(batch: list) -> dict[str, int]:
        if not landed:
            # Blocks the loop the consumer's heartbeat runs on, as a landing held by the store does.
            time.sleep(2 * _SESSION_MS / 1000)
        landed.extend(event.row["order_id"] for event in batch)
        return {"upsert": len(batch), "delete": 0}

    provider = KafkaNotificationProvider(
        bootstrap,
        group_id=f"provisa-test-{topic}",
        session_timeout_ms=_SESSION_MS,
        heartbeat_interval_ms=1000,
    )
    with caplog.at_level(logging.WARNING):
        listener = asyncio.create_task(
            consume_cdc_into_store(
                provider,
                _land,
                schema="s",
                table="orders",
                disconnect=disconnect,
                watch_target=topic,
            )
        )
        try:
            async with asyncio.timeout(60):
                while not landed:
                    await asyncio.sleep(0.2)
            assert "read again" in caplog.text, "the broker did not drop the consumer"
            await _publish(bootstrap, topic, "second")
            async with asyncio.timeout(60):
                while "second" not in landed:
                    assert not listener.done(), listener
                    await asyncio.sleep(0.2)
        finally:
            disconnect.set()
            await _publish(bootstrap, topic, "last")  # wakes the listener to see the disconnect
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
