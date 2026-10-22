# Copyright (c) 2026 Kenneth Stott
# Canary: add0c411-eef7-48b0-9095-d7ccf5a1f970
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The provisioning demo's producer asks the broker whether it already holds orders, so a
restarted producer publishes the month of history once (demo/named/provisioning/producer.py).

aiokafka 0.14.0, as the producer's image installs it: a consumer's ``partitions_for_topic`` is
not a coroutine, and it answers None until the consumer holds metadata for the topic -- which
``start()`` does not fetch for a topic it was not given. Awaiting it was a TypeError that
stopped the producer before it published anything."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

import pytest

_PRODUCER = Path(__file__).parents[2] / "demo" / "named" / "provisioning" / "producer.py"


class _Consumer:
    """aiokafka 0.14.0's consumer as the producer uses it: metadata for a topic arrives in the
    background after ``subscribe`` asks for it, ``metadata_ticks`` sleeps later."""

    topics_on_broker: set[str] = set()
    end_offset = 0
    metadata_ticks = 2
    instances: list[_Consumer] = []

    def __init__(self, *, bootstrap_servers: str) -> None:
        self.subscribed: list[str] = []
        self._known: set[int] | None = None
        self.stopped = False
        _Consumer.instances.append(self)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True

    async def topics(self) -> set[str]:
        return set(self.topics_on_broker)

    def subscribe(self, topics) -> None:
        self.subscribed = list(topics)
        asyncio.get_running_loop().create_task(self._metadata())

    async def _metadata(self) -> None:
        for _ in range(self.metadata_ticks):
            await asyncio.sleep(0)
        if self.metadata_ticks >= 0:
            self._known = {0, 1}

    def partitions_for_topic(self, topic: str) -> set[int] | None:
        return self._known if topic in self.subscribed else None

    async def end_offsets(self, partitions):
        return {tp: self.end_offset for tp in partitions}


@pytest.fixture
def producer(monkeypatch):
    monkeypatch.setenv("FEEDS_BOOTSTRAP", "feeds:9092")
    # The demo's own broker: its topics carry no suffix (demo_bundles.store_namespaces).
    monkeypatch.setenv("TOPIC_SUFFIX", "")
    spec = importlib.util.spec_from_file_location("provisioning_producer", _PRODUCER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "AIOKafkaConsumer", _Consumer)
    monkeypatch.setattr(_Consumer, "instances", [])
    monkeypatch.setattr(_Consumer, "metadata_ticks", 2)
    return module


async def test_a_broker_with_no_orders_topic_holds_no_orders(producer, monkeypatch):
    monkeypatch.setattr(_Consumer, "topics_on_broker", {"prices", "inventory"})
    assert await producer._orders_published() is False
    (consumer,) = _Consumer.instances
    assert consumer.subscribed == [] and consumer.stopped


@pytest.mark.parametrize(("end", "published"), [(0, False), (5, True)])
async def test_an_orders_topic_is_read_once_its_partitions_are_known(
    producer, monkeypatch, end, published
):
    monkeypatch.setattr(_Consumer, "topics_on_broker", {"orders", "prices"})
    monkeypatch.setattr(_Consumer, "end_offset", end)
    assert await producer._orders_published() is published
    assert _Consumer.instances[0].stopped


async def test_partitions_that_never_arrive_fail_by_name_within_the_bound(producer, monkeypatch):
    monkeypatch.setattr(_Consumer, "topics_on_broker", {"orders"})
    monkeypatch.setattr(_Consumer, "metadata_ticks", -1)  # the topic's metadata never comes
    monkeypatch.setattr(producer, "METADATA_WAIT_S", 0.2)
    with pytest.raises(RuntimeError, match="feeds:9092 lists the orders topic"):
        await producer._orders_published()
    assert _Consumer.instances[0].stopped
