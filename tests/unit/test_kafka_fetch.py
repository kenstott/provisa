# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4d7e2a-6b1f-4a83-9d5e-2c8f6a3b7d10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka as a table source, engine-independently (REQ-1730): the row read, the loader, and the
engine-gated wiring. No real broker — ``aiokafka.AIOKafkaConsumer`` is faked."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.kafka import fetch as kf


class _FakeConsumer:
    """Replays ``batches`` (a list of raw message-value lists) one ``getmany()`` call at a time,
    then an empty batch forever — the same drain-until-empty contract a real broker gives once
    caught up to the topic's current end."""

    next_batches: list[list[bytes | None]] = []

    def __init__(self, topic, *, bootstrap_servers, **kw):
        del bootstrap_servers, kw
        self.topic = topic
        self._batches = list(_FakeConsumer.next_batches)
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def getmany(self, timeout_ms=5000):
        del timeout_ms
        if not self._batches:
            return {}
        values = self._batches.pop(0)
        return {("tp", 0): [SimpleNamespace(value=v) for v in values]}


@pytest.fixture
def fake_aiokafka(monkeypatch):
    import aiokafka

    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", _FakeConsumer)
    yield _FakeConsumer


def _conn() -> kf.KafkaConnection:
    return kf.KafkaConnection(bootstrap_servers="h:1")


@pytest.mark.asyncio
async def test_plain_json_body_is_the_row(fake_aiokafka):
    _FakeConsumer.next_batches = [[json.dumps({"id": "1", "name": "Ann"}).encode()]]
    rows = await kf.fetch_rows(_conn(), "topic", ["id", "name"])
    assert rows == [{"id": "1", "name": "Ann"}]


@pytest.mark.asyncio
async def test_op_row_envelope_keeps_only_row(fake_aiokafka):
    _FakeConsumer.next_batches = [
        [json.dumps({"op": "insert", "row": {"id": "1", "name": "Ann"}}).encode()]
    ]
    rows = await kf.fetch_rows(_conn(), "topic", ["id", "name"])
    assert rows == [{"id": "1", "name": "Ann"}]


@pytest.mark.asyncio
async def test_drains_across_multiple_batches_then_stops_on_empty(fake_aiokafka):
    _FakeConsumer.next_batches = [
        [json.dumps({"id": "1"}).encode()],
        [json.dumps({"id": "2"}).encode(), json.dumps({"id": "3"}).encode()],
    ]
    rows = await kf.fetch_rows(_conn(), "topic", ["id"])
    assert [r["id"] for r in rows] == ["1", "2", "3"]


@pytest.mark.asyncio
async def test_non_json_and_none_values_are_skipped(fake_aiokafka):
    _FakeConsumer.next_batches = [[b"not json", None, json.dumps({"id": "1"}).encode()]]
    rows = await kf.fetch_rows(_conn(), "topic", ["id"])
    assert rows == [{"id": "1"}]


@pytest.mark.asyncio
async def test_empty_topic_returns_no_rows(fake_aiokafka):
    _FakeConsumer.next_batches = []
    rows = await kf.fetch_rows(_conn(), "topic", ["id"])
    assert rows == []


def test_connection_build_joins_host_and_port():
    assert kf.KafkaConnection.build("broker1,broker2", 9092).bootstrap_servers == (
        "broker1,broker2:9092"
    )
    assert kf.KafkaConnection.build("", None).bootstrap_servers == "localhost:9092"


@pytest.mark.asyncio
async def test_loader_reads_the_registered_columns(fake_aiokafka):
    from provisa.events.source_loader import make_kafka_loader

    _FakeConsumer.next_batches = [[json.dumps({"id": "1", "name": "Ann"}).encode()]]
    source = SimpleNamespace(id="k", host="h", port=1)
    table = SimpleNamespace(
        table_name="events",
        columns=[SimpleNamespace(name=n, native_filter_type=None) for n in ("id", "name")],
    )
    rows = await make_kafka_loader()(source, table)
    assert rows == [{"id": "1", "name": "Ann"}]


def test_loader_is_wired_only_when_the_engine_does_not_read_kafka_live():
    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.federation.connector import Mechanism

    state = SimpleNamespace(config=SimpleNamespace(sources=[]))
    land = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={"kafka": SimpleNamespace(mechanism=Mechanism.FETCH, reads_in_place=False)}
        )
    )
    assert "kafka" in build_adapter_loaders(state, land)
    live = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={"kafka": SimpleNamespace(mechanism=Mechanism.ATTACH_R, reads_in_place=True)}
        )
    )
    assert "kafka" not in build_adapter_loaders(state, live)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
