# Copyright (c) 2026 Kenneth Stott
# Canary: 74b6e3ae-49dd-4623-9f6c-7209de76bb12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Kafka table's live listener starts exactly where the table's landing stopped (REQ-1733,
REQ-1734).

The table is landed by a build that drains its topic (``kafka.fetch``), then its listener lands
what follows. The listener's group had committed nothing on its first start and read from the
newest offset, so a message published between the landing and the listener's start was never
landed. Read from the earliest, it would apply again what the landing applied, and the table's
triggers would run again for it. The landing records where it stopped; the listener's first start
reads from there, and every later one from what it committed."""

# Requirements: REQ-1733, REQ-1734, REQ-1730, REQ-2054

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from provisa.core.request_context import reset_current_org, set_current_org
from provisa.kafka import landed_offsets
from provisa.kafka.fetch import KafkaConnection, iter_row_batches
from provisa.subscriptions.cdc_landing import consume_cdc_into_store
from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

_TOPIC = "orders"
_GROUP = "provisa-acme-shop"
_BOOTSTRAP = "broker:9092"


class _Broker:
    """One topic of one partition, and each consumer group's committed offset."""

    def __init__(self) -> None:
        self.log: list[bytes] = []
        self.committed: dict[str, int] = {}

    def publish(self, order_id: int) -> None:
        self.log.append(json.dumps({"id": order_id}).encode())

    def capture(self, order_id: int) -> None:
        """A Debezium connector's change event: a row inserted."""
        payload = {"op": "c", "after": {"id": order_id}, "source": {"ts_ms": 0}}
        self.log.append(json.dumps({"payload": payload}).encode())

    def consumer(self, *topics, group_id=None, auto_offset_reset="latest", **_kwargs):
        return _Consumer(self, topics[0] if topics else None, group_id, auto_offset_reset)


class _Consumer:
    def __init__(self, broker: _Broker, topic: str | None, group: str | None, reset: str) -> None:
        self._broker, self._topic, self._group, self._reset = broker, topic, group, reset
        self._assigned: list = []
        self._position = 0

    async def start(self) -> None:
        from aiokafka import TopicPartition

        if self._topic is None:
            return  # assigned by hand
        self._assigned = [TopicPartition(self._topic, 0)]
        committed = self._broker.committed.get(self._group) if self._group else None
        if committed is not None:
            self._position = committed
        else:
            self._position = 0 if self._reset == "earliest" else len(self._broker.log)

    async def stop(self) -> None:
        pass

    async def topics(self) -> set[str]:
        return {self._topic or _TOPIC}

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {0}

    def assign(self, partitions) -> None:
        self._assigned = list(partitions)

    def assignment(self) -> set:
        return set(self._assigned)

    async def position(self, tp) -> int:
        return self._position

    async def committed(self, tp) -> int | None:
        return self._broker.committed.get(self._group) if self._group else None

    async def end_offsets(self, partitions) -> dict:
        return {tp: len(self._broker.log) for tp in partitions}

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
        return {self._assigned[0]: messages} if messages else {}

    def __aiter__(self):
        return self

    async def __anext__(self):
        """What the topic holds now; the stream ends where it ends, so a test reads it whole."""
        if self._position >= len(self._broker.log):
            raise StopAsyncIteration
        message = self._message(self._position)
        self._position += 1
        return message

    async def commit(self, offsets: dict) -> None:
        assert self._group is not None
        for meta in offsets.values():
            self._broker.committed[self._group] = getattr(meta, "offset", meta)


@pytest.fixture
def broker(monkeypatch) -> Iterator[_Broker]:
    import aiokafka

    broker = _Broker()
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", broker.consumer)
    token = set_current_org("acme")
    yield broker
    reset_current_org(token)


def _provider() -> KafkaNotificationProvider:
    return KafkaNotificationProvider(
        bootstrap_servers=_BOOTSTRAP, group_id=_GROUP, landed=landed_offsets.landed_group("shop")
    )


async def _land(broker: _Broker) -> list[int]:
    """The ids the table's build lands, draining the topic as a build does."""
    landed = landed_offsets.landed_group("shop")
    batches = iter_row_batches(KafkaConnection(_BOOTSTRAP), _TOPIC, ["id"], landed=landed)
    return [row["id"] async for batch in batches for row in batch]


async def _listen(broker: _Broker) -> list[int]:
    """The ids one start of the table's listener lands, acknowledging each, as its landing does."""
    provider = _provider()
    landed = []
    async for event in provider.watch(_TOPIC):
        landed.append(event.row["id"])
        await provider.ack([event])
    return landed


@pytest.mark.asyncio
async def test_a_message_published_between_the_landing_and_the_listener_is_landed_once(broker):
    broker.publish(1)
    broker.publish(2)
    assert await _land(broker) == [1, 2]
    broker.publish(3)  # before the listener starts

    assert await _listen(broker) == [3]


@pytest.mark.asyncio
async def test_a_restarted_listener_lands_what_was_published_while_it_was_down_once(broker):
    broker.publish(1)
    await _land(broker)
    await _listen(broker)
    broker.publish(2)  # while no listener runs
    broker.publish(3)
    await _land(broker)  # a later build records a later stop; the listener resumes its own

    assert await _listen(broker) == [2, 3]


@pytest.mark.asyncio
async def test_no_trigger_runs_again_on_the_first_start_after_a_landing(broker):
    """The changes a landed batch hands the table's triggers (``table_changed``) are only what
    the landing did not apply."""
    broker.publish(1)
    broker.publish(2)
    await _land(broker)
    broker.publish(3)
    changed: list[dict] = []

    async def land_fn(batch):
        changed.extend(event.row for event in batch)
        return {"upsert": len(batch), "delete": 0}

    await consume_cdc_into_store(
        _provider(),
        land_fn,
        schema="replicas",
        table="orders",
        disconnect=threading.Event(),
        watch_target=_TOPIC,
    )
    assert changed == [{"id": 3}]


@pytest.mark.asyncio
async def test_with_no_landing_recorded_the_listener_reads_the_topic_from_its_start(broker):
    """Nothing says where a landing stopped: nothing on the topic is passed over."""
    broker.publish(1)
    broker.publish(2)

    assert await _listen(broker) == [1, 2]


@pytest.mark.asyncio
async def test_a_debezium_listener_starts_where_the_tables_build_stopped_reading(broker):
    """A table its database's Debezium topic follows is built from the database; the build
    records where the topic stood before it read (``replica_builds.build_replica``), and the
    listener's first start reads only the changes made since."""
    from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider

    broker.capture(1)
    broker.capture(2)
    landed = landed_offsets.landed_group("shop_db")
    stopped = await landed_offsets.end_offsets(_BOOTSTRAP, "shop.public.orders")
    await landed_offsets.record(_BOOTSTRAP, landed, stopped)  # the build's rows are 1 and 2
    broker.capture(3)

    provider = DebeziumNotificationProvider(
        bootstrap_servers=_BOOTSTRAP,
        topic_prefix="shop",
        database="shop",
        consumer_group_id="provisa-debezium-acme",
        landed=landed,
    )
    seen = []
    async for event in provider.watch("orders"):
        seen.append(event.row["id"])
        await provider.ack([event])
    assert seen == [3]


def test_a_captured_tables_build_knows_the_topic_its_changes_arrive_on():
    from provisa.core.models import Source, SourceCdcConfig, SourceType
    from provisa.events.push_wiring import cdc_capture

    cdc = SourceCdcConfig(bootstrap_servers="broker:9092", topic_prefix="shop")
    src = Source(id="shop_db", type=SourceType.postgresql, host="h", database="shop", cdc=cdc)
    table = {"schema_name": "public", "table_name": "orders", "change_signal": "debezium"}
    assert cdc_capture(src, table) == ("broker:9092", "shop.public.orders")
    deltas = {**table, "change_signal": "kafka", "live": {"kafka": {"topic": "orders.deltas"}}}
    assert cdc_capture(src, deltas) == ("broker:9092", "orders.deltas")
    assert cdc_capture(src, {**table, "change_signal": "ttl"}) is None
