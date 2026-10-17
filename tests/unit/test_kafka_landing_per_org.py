# Copyright (c) 2026 Kenneth Stott
# Canary: 1dfa8ac7-e0f1-4998-9301-a2544d96041e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Kafka live table is landed, on DuckDB, from every message it can read, in every org that
declares it (REQ-1733, REQ-1951).

What failed before: a message in the schema registry's wire format (Avro) ended the provider's
stream with an uncaught UnicodeDecodeError, and the landing loop swallowed the failure, so the
listener stopped and nothing after the message ever landed; a message without the table's key
landed as a row of NULLs; and two orgs with a table of the same source and name shared one
listener slot and one consumer group, so the second org's table was never landed and the
first's stopped when the second was wired."""

# Requirements: REQ-1733, REQ-1734, REQ-1951

from __future__ import annotations

import asyncio
import io
import json
import logging
import struct
import sys
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import fastavro
import pytest

import provisa.subscriptions.kafka_provider as kp
from provisa.subscriptions.base import ChangeEvent

_SCHEMA = fastavro.parse_schema(
    {
        "type": "record",
        "name": "order",
        "fields": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double"}],
    }
)


def _avro(record: dict) -> bytes:
    body = io.BytesIO()
    fastavro.schemaless_writer(body, _SCHEMA, record)
    return b"\x00" + struct.pack(">I", 7) + body.getvalue()


class _Msg:
    def __init__(self, value, offset):
        self.value = value
        self.timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        self.topic = "orders"
        self.partition = 0
        self.offset = offset


def _consumer_of(values: list):
    class _Consumer:
        def __init__(self, topic, **kwargs):
            pass

        async def start(self):
            pass

        async def stop(self):
            pass

        def __aiter__(self):
            async def _gen():
                for offset, value in enumerate(values):
                    yield _Msg(value, offset)

            return _gen()

    return _Consumer


class _Registry:
    """The source's registry: the one schema, id 7."""

    def __init__(self, settings):
        self.closed = False

    async def reach(self):
        pass

    async def decode(self, raw: bytes):
        return fastavro.schemaless_reader(io.BytesIO(raw[5:]), _SCHEMA, None)

    async def close(self):
        self.closed = True


async def _events(monkeypatch, values: list, **provider_kwargs) -> list[ChangeEvent]:
    monkeypatch.setitem(
        sys.modules, "aiokafka", SimpleNamespace(AIOKafkaConsumer=_consumer_of(values))
    )
    monkeypatch.setattr(kp, "SchemaRegistry", _Registry)
    provider = kp.KafkaNotificationProvider("broker:9092", **provider_kwargs)
    return [event async for event in provider.watch("orders")]


async def test_an_avro_message_is_read_through_the_source_s_registry(monkeypatch):
    from provisa.kafka.avro_registry import RegistrySettings

    events = await _events(
        monkeypatch,
        [_avro({"id": "o-1", "amount": 9.5}), json.dumps({"id": "o-2", "amount": 1.0}).encode()],
        registry=RegistrySettings(url="http://registry:8081"),
    )
    assert [e.row for e in events] == [{"id": "o-1", "amount": 9.5}, {"id": "o-2", "amount": 1.0}]
    assert [e.ack_token for e in events] == [("orders", 0, 0), ("orders", 0, 1)]


async def test_an_avro_message_with_no_registry_is_refused_by_name(monkeypatch):
    from provisa.kafka.avro_registry import SchemaRegistryRefusal

    with pytest.raises(SchemaRegistryRefusal):
        await _events(monkeypatch, [_avro({"id": "o-1", "amount": 9.5})])


async def test_a_message_that_cannot_be_read_is_reported_and_the_stream_goes_on(
    monkeypatch, caplog
):
    caplog.set_level(logging.ERROR, logger=kp.__name__)
    events = await _events(
        monkeypatch,
        [
            b"\xff\xfe not text",
            b"{not json",
            json.dumps({"id": "o-3", "amount": 2.0}).encode(),
        ],
    )
    assert [e.row for e in events] == [{"id": "o-3", "amount": 2.0}]
    reported = [r.getMessage() for r in caplog.records]
    assert any("orders[0]@0" in m for m in reported)
    assert any("orders[0]@1" in m for m in reported)


async def test_field_mapping_names_the_table_s_columns(monkeypatch):
    events = await _events(
        monkeypatch,
        [json.dumps({"orderId": "o-4", "total": 3.0}).encode()],
        field_mapping={"orderId": "id", "total": "amount"},
    )
    assert [e.row for e in events] == [{"id": "o-4", "amount": 3.0}]


async def test_a_provider_that_fails_ends_the_landing_loudly():
    from provisa.subscriptions.cdc_landing import consume_cdc_into_store

    class _Failing:
        closed = False

        async def watch(self, target):
            raise RuntimeError("the broker went away")
            yield  # an async generator

        async def ack(self, events):
            pass

        async def close(self):
            self.closed = True

    provider = _Failing()
    with pytest.raises(RuntimeError, match="the broker went away"):
        await consume_cdc_into_store(
            provider,
            AsyncMock(return_value={"upsert": 0, "delete": 0}),
            schema="s",
            table="t",
            disconnect=threading.Event(),
        )
    assert provider.closed


async def test_a_change_without_the_table_s_key_is_not_landed(caplog):
    from provisa.events.push_wiring import _run_listener

    landed: list = []

    class _Engine:
        async def apply_cdc_events(self, *, events, **kwargs):
            landed.extend(e.row for e in events)
            return {"upsert": len(events), "delete": 0}

    def _event(operation, row, offset):
        return ChangeEvent(
            operation=operation,
            table="orders",
            row=row,
            timestamp=datetime.now(timezone.utc),
            ack_token=("orders", 0, offset),
        )

    class _Provider:
        acked: list = []

        async def watch(self, target):
            yield _event("insert", {"amount": 1.0}, 0)  # no key: would land as NULLs
            yield _event("delete", {}, 1)  # a compaction tombstone
            yield _event("insert", {"id": "o-5", "amount": 2.0}, 2)

        async def ack(self, events):
            self.acked.extend(e.ack_token for e in events)

        async def close(self):
            pass

    provider = _Provider()
    with (
        patch("provisa.events.ripple.ripple", AsyncMock()),
        patch("provisa.scheduler.table_triggers.table_changed", MagicMock()),
    ):
        caplog.set_level(logging.ERROR)
        await _run_listener(
            state=SimpleNamespace(),
            engine=_Engine(),
            provider=provider,
            watch_target="orders",
            land_schema="s",
            land_table="orders",
            columns=[("id", "varchar"), ("amount", "double")],
            pk_columns=["id"],
            disconnect=threading.Event(),
            debounce_quiet=0.0,
            debounce_max_delay=5.0,
            node="kafka_src/s.orders",
            log=logging.getLogger("test"),
            key=("kafka_src", "s", "orders"),
        )
    assert landed == [{"id": "o-5", "amount": 2.0}]
    # Each is passed over, and its offset committed: a restart does not read it again.
    assert provider.acked == [("orders", 0, 0), ("orders", 0, 1), ("orders", 0, 2)]
    assert any("carries no id" in r.getMessage() for r in caplog.records)


# -- every org lands its own -----------------------------------------------------------------


@pytest.mark.unbound
async def test_two_orgs_each_land_the_topic_into_their_own_table(monkeypatch):
    from provisa.core.models import Source, SourceType
    from provisa.core.request_context import reset_current_org, set_current_org
    from provisa.events.push_wiring import shutdown_push_listeners, wire_push_listeners
    from tests.unit.test_push_wiring_orchestration import _FakeState, _kafka_table

    state = _FakeState()
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker", port=9092)
    groups: list[str] = []

    def _provider(kind, config):
        groups.append(config["group_id"])
        return MagicMock()

    async def _runs_until_stopped(**kwargs):
        await asyncio.sleep(3600)

    async def _wire(org: str) -> list:
        token = set_current_org(org)
        try:
            return await wire_push_listeners(state=state, log=logging.getLogger("test"))
        finally:
            reset_current_org(token)

    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables", AsyncMock(return_value=[_kafka_table()])
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
        patch("provisa.subscriptions.registry.get_provider", _provider),
        patch("provisa.events.push_wiring._run_listener", _runs_until_stopped),
    ):
        first = await _wire("acme")
        second = await _wire("globex")
        again = await _wire("acme")  # a re-wire of one org leaves the other's running

    assert len(first) == 1 and len(second) == 1 and again == []
    assert set(state.push_listener_disconnects) == {
        ("acme", None, "kafka_src/s.orders"),
        ("globex", None, "kafka_src/s.orders"),
    }
    assert not any(handle.done() for handle in state.push_listener_handles.values())
    assert groups == ["provisa-acme-kafka_src", "provisa-globex-kafka_src"]
    await shutdown_push_listeners(state)


@pytest.mark.unbound
async def test_a_wire_bound_to_prod_finds_the_listener_a_wire_with_no_environment_started():
    """An unbound environment and ``prod`` are one environment (REQ-1487): a re-wire under either
    finds the other's listener running and starts no second one on a second consumer group."""
    from provisa.core.models import Source, SourceType
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )
    from provisa.events.push_wiring import shutdown_push_listeners, wire_push_listeners
    from tests.unit.test_push_wiring_orchestration import _FakeState, _kafka_table

    state = _FakeState()
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker", port=9092)
    groups: list[str] = []

    def _provider(kind, config):
        groups.append(config["group_id"])
        return MagicMock()

    async def _runs_until_stopped(**kwargs):
        await asyncio.sleep(3600)

    async def _wire(env: str | None) -> list:
        token = set_current_org("acme")
        env_token = set_current_env(env)
        try:
            return await wire_push_listeners(state=state, log=logging.getLogger("test"))
        finally:
            reset_current_env(env_token)
            reset_current_org(token)

    with (
        patch(
            "provisa.api.admin.db_queries.fetch_tables", AsyncMock(return_value=[_kafka_table()])
        ),
        patch(
            "provisa.federation.registry_view.registered_sources",
            AsyncMock(return_value=[src]),
        ),
        patch("provisa.subscriptions.registry.get_provider", _provider),
        patch("provisa.events.push_wiring._run_listener", _runs_until_stopped),
    ):
        first = await _wire(None)
        again = await _wire("prod")

    assert len(first) == 1 and again == []
    assert len(state.push_listener_disconnects) == 1
    assert groups == ["provisa-acme-kafka_src"]
    await shutdown_push_listeners(state)
