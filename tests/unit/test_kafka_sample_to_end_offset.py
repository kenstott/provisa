# Copyright (c) 2026 Kenneth Stott
# Canary: 2f1b6777-47cb-45f3-a192-212c2cdf69bb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-150: Kafka sample-mode discovery reads each partition to the end offset it had when
sampling began. A poll that comes back empty while the consumer is still being assigned its
partitions (a loaded machine) no longer ends the read with zero records. The read is bounded by the
request's own deadline, and a sample with no JSON message is refused by name instead of answering
an empty schema."""

from __future__ import annotations

import asyncio
import json

import pytest
from aiokafka import TopicPartition

from provisa.kafka import source as kafka_source

_TP = TopicPartition("orders", 0)


class _Msg:
    def __init__(self, value: bytes | None) -> None:
        self.value = value


class _Consumer:
    """aiokafka's consumer as the sampler uses it. ``polls`` are the batches successive
    ``getmany`` calls return; the partition is assigned only once the first poll is over."""

    instances: list[_Consumer] = []

    def __init__(self, *args, polls: list[list[bytes | None]], end: int, **kwargs) -> None:
        self._polls = list(polls)
        self._end = end
        self._position = 0
        self._assigned: set = set()
        self.getmany_calls = 0
        self.stopped = False
        _Consumer.instances.append(self)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {0}

    async def end_offsets(self, tps):
        return {tp: self._end for tp in tps}

    def assignment(self) -> set:
        return self._assigned

    async def position(self, tp) -> int:
        return self._position

    async def getmany(self, timeout_ms: int, max_records: int):
        self.getmany_calls += 1
        values = self._polls.pop(0) if self._polls else []
        if self.getmany_calls > 1:
            self._assigned = {_TP}  # assignment lands after the first, empty poll
        self._position += len(values)
        return {_TP: [_Msg(v) for v in values]} if values else {}


def _patch(monkeypatch, *, polls, end):
    _Consumer.instances = []
    monkeypatch.setattr(
        "aiokafka.AIOKafkaConsumer",
        lambda *a, **k: _Consumer(*a, polls=polls, end=end, **k),
    )


def _row(**fields) -> bytes:
    return json.dumps(fields).encode()


def test_an_empty_first_poll_does_not_end_the_read(monkeypatch):
    _patch(monkeypatch, polls=[[], [_row(id="1", value="a"), b"not json"]], end=2)
    records = asyncio.run(kafka_source.sample_topic_records("b:9092", "orders"))
    assert records == [{"id": "1", "value": "a"}]
    consumer = _Consumer.instances[0]
    assert consumer.getmany_calls == 2 and consumer.stopped


def test_an_empty_topic_is_not_polled(monkeypatch):
    _patch(monkeypatch, polls=[], end=0)
    assert asyncio.run(kafka_source.sample_topic_records("b:9092", "orders")) == []
    assert _Consumer.instances[0].getmany_calls == 0


def test_the_read_ends_at_the_request_deadline(monkeypatch):
    from provisa.core import request_deadline

    _patch(monkeypatch, polls=[[]] * 100, end=5)  # never assigned, never reaches its end
    calls = {"n": 0}

    def _check() -> None:
        calls["n"] += 1
        if calls["n"] > 3:
            raise TimeoutError("request deadline passed")

    monkeypatch.setattr(request_deadline, "check", _check)
    with pytest.raises(TimeoutError, match="request deadline passed"):
        asyncio.run(kafka_source.sample_topic_records("b:9092", "orders"))
    assert _Consumer.instances[0].stopped


def test_discovery_refuses_an_empty_sample_by_name(monkeypatch):
    from provisa.api.admin import discovery_schema
    from provisa.api.admin.discovery_schema import DiscoverRequest, _call_discover

    async def _no_records(*args, **kwargs):
        return []

    monkeypatch.setattr(kafka_source, "sample_topic_records", _no_records)
    hints = DiscoverRequest(topic="orders", bootstrap_servers="b:9092")
    row = {"id": "k", "type": "kafka", "host": "b", "port": 9092}
    with pytest.raises(discovery_schema.ApiError) as raised:
        asyncio.run(_call_discover(object(), "kafka", row, hints))
    assert raised.value.code == "discovery.kafka_sample_empty"
