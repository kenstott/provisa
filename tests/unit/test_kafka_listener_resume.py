# Copyright (c) 2026 Kenneth Stott
# Canary: 74b6e3ae-49dd-4623-9f6c-7209de76bb12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Kafka table's live listener misses no message published before it started (REQ-1733,
REQ-1734).

The table is landed at boot from the topic's earliest offset (kafka.fetch), then its
listener lands what follows. The listener's consumer group had no committed offset on its first
start and read from the newest one, so every message published between the landing and the
listener's start was never landed. Its first start reads from the earliest offset -- what the
landing covered is upserted again by primary key, which changes nothing -- and every later one
from what it committed: no message lost, none read twice."""

# Requirements: REQ-1733, REQ-1734, REQ-1730

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.kafka.fetch import KafkaConnection, fetch_rows
from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

_TOPIC = "orders"
_GROUP = "provisa-acme-shop"


class _Broker:
    """One topic of one partition, and each consumer group's committed offset."""

    def __init__(self) -> None:
        self.log: list[bytes] = []
        self.committed: dict[str, int] = {}

    def publish(self, order_id: int) -> None:
        self.log.append(json.dumps({"id": order_id}).encode())

    def consumer(self, topic, *, group_id, auto_offset_reset, **_kwargs):
        return _Consumer(self, topic, group_id, auto_offset_reset)


class _Consumer:
    def __init__(self, broker: _Broker, topic: str, group: str | None, reset: str) -> None:
        self._broker, self._topic, self._group, self._reset = broker, topic, group, reset
        self._position = 0

    async def start(self) -> None:
        committed = self._broker.committed.get(self._group) if self._group else None
        if committed is not None:
            self._position = committed
        else:
            self._position = 0 if self._reset == "earliest" else len(self._broker.log)

    async def stop(self) -> None:
        pass

    def _message(self, offset: int):
        return SimpleNamespace(
            value=self._broker.log[offset],
            timestamp=0,
            topic=self._topic,
            partition=0,
            offset=offset,
        )

    async def getmany(self, timeout_ms: int):
        messages = [self._message(o) for o in range(self._position, len(self._broker.log))]
        self._position = len(self._broker.log)
        return {(self._topic, 0): messages} if messages else {}

    def __aiter__(self):
        return self

    async def __anext__(self):
        """What the topic holds now; the stream ends where it ends, so a test reads it whole."""
        if self._position >= len(self._broker.log):
            raise StopAsyncIteration
        message = self._message(self._position)
        self._position += 1
        return message

    async def commit(self, offsets=None) -> None:
        assert self._group is not None
        for meta in offsets.values():
            self._broker.committed[self._group] = meta.offset


@pytest.fixture
def broker(monkeypatch) -> _Broker:
    import aiokafka

    broker = _Broker()
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", broker.consumer)
    return broker


async def _listen(broker: _Broker) -> list[int]:
    """The ids one start of the table's listener lands, acknowledging each, as its landing does."""
    provider = KafkaNotificationProvider(bootstrap_servers="broker:9092", group_id=_GROUP)
    landed = []
    async for event in provider.watch(_TOPIC):
        landed.append(event.row["id"])
        await provider.ack([event])
    return landed


@pytest.mark.asyncio
async def test_a_message_published_between_the_landing_and_the_listener_is_landed(broker):
    broker.publish(1)
    broker.publish(2)
    landed_at_boot = await fetch_rows(KafkaConnection("broker:9092"), _TOPIC, ["id"])
    assert landed_at_boot == [{"id": 1}, {"id": 2}]
    broker.publish(3)  # before the listener starts

    assert 3 in await _listen(broker)


@pytest.mark.asyncio
async def test_a_restarted_listener_lands_what_was_published_while_it_was_down_once(broker):
    broker.publish(1)
    await _listen(broker)
    broker.publish(2)  # while no listener runs
    broker.publish(3)

    assert await _listen(broker) == [2, 3]
