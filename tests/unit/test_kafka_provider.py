# Copyright (c) 2026 Kenneth Stott
# Canary: fc8d0e97-404f-4cc6-830f-8a8a5da9c1f6
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for KafkaNotificationProvider.ack() (REQ-1734): commits the HIGHEST offset per
(topic, partition) among the given events — never "wherever the consumer currently is" — with a
mocked AIOKafkaConsumer.commit(), no real broker."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from provisa.subscriptions.base import ChangeEvent
from provisa.subscriptions.kafka_provider import KafkaNotificationProvider


def _event(offset: int, topic: str = "orders", partition: int = 0) -> ChangeEvent:
    return ChangeEvent(
        operation="insert", table=topic, row={"id": offset}, ack_token=(topic, partition, offset)
    )


@pytest.mark.asyncio
async def test_ack_commits_highest_offset_per_partition():
    provider = KafkaNotificationProvider(bootstrap_servers="broker:9092")
    mock_consumer = AsyncMock()
    provider._consumer = mock_consumer

    events = [_event(5), _event(7), _event(6)]  # out of order — 7 is highest
    await provider.ack(events)

    mock_consumer.commit.assert_called_once()
    offsets = mock_consumer.commit.call_args.kwargs["offsets"]
    assert len(offsets) == 1
    (tp, meta) = next(iter(offsets.items()))
    assert (tp.topic, tp.partition) == ("orders", 0)
    assert meta.offset == 8  # commit convention: highest offset + 1


@pytest.mark.asyncio
async def test_ack_commits_each_partition_independently():
    provider = KafkaNotificationProvider(bootstrap_servers="broker:9092")
    mock_consumer = AsyncMock()
    provider._consumer = mock_consumer

    events = [_event(10, partition=0), _event(3, partition=1), _event(11, partition=0)]
    await provider.ack(events)

    offsets = mock_consumer.commit.call_args.kwargs["offsets"]
    by_partition = {tp.partition: meta.offset for tp, meta in offsets.items()}
    assert by_partition == {0: 12, 1: 4}


@pytest.mark.asyncio
async def test_ack_no_op_without_a_live_consumer():
    provider = KafkaNotificationProvider(bootstrap_servers="broker:9092")
    # _consumer is None until watch() starts one — ack() before/after close() must not raise.
    await provider.ack([_event(1)])


@pytest.mark.asyncio
async def test_ack_skips_events_with_no_ack_token():
    provider = KafkaNotificationProvider(bootstrap_servers="broker:9092")
    mock_consumer = AsyncMock()
    provider._consumer = mock_consumer

    plain_event = ChangeEvent(operation="insert", table="orders", row={"id": 1})  # ack_token=None
    await provider.ack([plain_event])

    mock_consumer.commit.assert_not_called()


@pytest.mark.asyncio
async def test_watch_disables_auto_commit_and_stamps_ack_token(monkeypatch):
    """REQ-1734: enable_auto_commit=False is passed to AIOKafkaConsumer, and each yielded
    ChangeEvent carries (topic, partition, offset) as its ack_token."""
    import json as _json
    from datetime import datetime, timezone

    constructed: list[
        dict
    ] = []  # every AIOKafkaConsumer(...) call's kwargs, captured at build time

    class _FakeMsg:
        def __init__(self, value, topic, partition, offset):
            self.value = _json.dumps(value).encode()
            self.timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            self.topic = topic
            self.partition = partition
            self.offset = offset

    class _FakeConsumer:
        def __init__(self, topic: str, **kwargs):
            constructed.append(kwargs)

        async def start(self):
            pass

        async def stop(self):
            pass

        def __aiter__(self):
            return self._gen()

        async def _gen(self):
            yield _FakeMsg({"op": "insert", "row": {"id": 1}}, "orders", 0, 42)

    import provisa.subscriptions.kafka_provider as kp

    fake_module = type("aiokafka", (), {"AIOKafkaConsumer": _FakeConsumer})
    monkeypatch.setitem(__import__("sys").modules, "aiokafka", fake_module)

    provider = kp.KafkaNotificationProvider(bootstrap_servers="broker:9092")
    events = []
    async for ev in provider.watch("orders"):
        events.append(ev)

    assert len(events) == 1
    assert events[0].ack_token == ("orders", 0, 42)
    assert len(constructed) == 1
    assert constructed[0]["enable_auto_commit"] is False
