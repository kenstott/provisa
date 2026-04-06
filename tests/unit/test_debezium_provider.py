# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for DebeziumNotificationProvider (REQ-261)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider
from provisa.subscriptions.registry import get_provider


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _provider(**kwargs) -> DebeziumNotificationProvider:
    defaults = dict(
        bootstrap_servers="localhost:9092",
        topic_prefix="dbserver1",
        database="mydb",
        consumer_group_id="test-group",
    )
    defaults.update(kwargs)
    return DebeziumNotificationProvider(**defaults)


def _make_msg(value_dict: dict | None, ts_ms: int | None = None) -> MagicMock:
    """Create a mock Kafka message."""
    msg = MagicMock()
    if value_dict is None:
        msg.value = None
    else:
        if ts_ms is not None:
            value_dict.setdefault("payload", value_dict).get("ts_ms")  # no-op
            if "payload" not in value_dict:
                value_dict["ts_ms"] = ts_ms
            else:
                value_dict["payload"]["ts_ms"] = ts_ms
        msg.value = json.dumps(value_dict).encode()
    return msg


def _envelope(op: str, after: dict | None = None, before: dict | None = None, ts_ms: int | None = None) -> dict:
    """Build a Debezium bare-envelope dict."""
    payload: dict = {"op": op}
    if after is not None:
        payload["after"] = after
    if before is not None:
        payload["before"] = before
    if ts_ms is not None:
        payload["ts_ms"] = ts_ms
    return payload


class FakeKafkaConsumer:
    """Async-iterable fake Kafka consumer."""

    def __init__(self, messages: list):
        self._messages = list(messages)
        self._started = False

    async def start(self):
        self._started = True

    async def stop(self):
        self._started = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


def _make_aiokafka_module(messages: list) -> MagicMock:
    consumer = FakeKafkaConsumer(messages)
    fake_module = MagicMock()
    fake_module.AIOKafkaConsumer = lambda *a, **kw: consumer
    return fake_module, consumer


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTopicName:
    def test_topic_name_built_correctly(self):
        """Topic name must be {prefix}.{database}.{table}."""
        provider = _provider(topic_prefix="myprefix", database="salesdb")
        topic = provider._build_topic("orders")
        assert topic == "myprefix.salesdb.orders"

    def test_topic_name_different_table(self):
        provider = _provider(topic_prefix="dbserver1", database="mydb")
        assert provider._build_topic("customers") == "dbserver1.mydb.customers"


class TestExtractEvent:
    def test_op_create_yields_insert(self):
        """op='c' maps to operation='insert'."""
        provider = _provider()
        envelope = _envelope("c", after={"id": 1, "name": "Alice"})
        event = provider._extract_event(envelope, "users")
        assert event is not None
        assert event.operation == "insert"
        assert event.table == "users"
        assert event.row == {"id": 1, "name": "Alice"}

    def test_op_update_yields_update(self):
        """op='u' maps to operation='update'."""
        provider = _provider()
        envelope = _envelope("u", after={"id": 1, "name": "Bob"})
        event = provider._extract_event(envelope, "users")
        assert event is not None
        assert event.operation == "update"
        assert event.row == {"id": 1, "name": "Bob"}

    def test_op_delete_yields_delete(self):
        """op='d' maps to operation='delete' using 'before' row."""
        provider = _provider()
        envelope = _envelope("d", before={"id": 1, "name": "Alice"})
        event = provider._extract_event(envelope, "users")
        assert event is not None
        assert event.operation == "delete"
        assert event.row == {"id": 1, "name": "Alice"}

    def test_op_snapshot_yields_insert(self):
        """op='r' (snapshot read) maps to operation='insert'."""
        provider = _provider()
        envelope = _envelope("r", after={"id": 5})
        event = provider._extract_event(envelope, "orders")
        assert event is not None
        assert event.operation == "insert"

    def test_watermark_from_ts_ms(self):
        """ts_ms field in envelope is used to set event timestamp."""
        provider = _provider()
        ts_ms = 1_700_000_000_000  # some epoch in ms
        envelope = _envelope("c", after={"id": 1}, ts_ms=ts_ms)
        event = provider._extract_event(envelope, "orders")
        assert event is not None
        expected_ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        assert event.timestamp == expected_ts

    def test_unknown_op_returns_none(self):
        """Unknown op codes (heartbeat etc.) return None."""
        provider = _provider()
        envelope = {"op": "x", "after": {"id": 1}}
        event = provider._extract_event(envelope, "orders")
        assert event is None

    def test_payload_key_unwrapped(self):
        """Debezium JSON converter wraps payload — must be unwrapped."""
        provider = _provider()
        envelope = {"payload": {"op": "c", "after": {"id": 99}}}
        event = provider._extract_event(envelope, "orders")
        assert event is not None
        assert event.operation == "insert"
        assert event.row == {"id": 99}


class TestParseJsonMessage:
    def test_invalid_json_returns_none(self):
        """Invalid JSON returns None, no exception raised."""
        provider = _provider()
        result = provider._parse_json_message(b"not-valid-json{{{")
        assert result is None

    def test_valid_json_returns_dict(self):
        provider = _provider()
        data = {"op": "c", "after": {"id": 1}}
        result = provider._parse_json_message(json.dumps(data).encode())
        assert result == data


class TestWatchAsync:
    @pytest.mark.asyncio
    async def test_invalid_json_skipped(self):
        """Invalid JSON messages are skipped without raising an exception."""
        valid_envelope = _envelope("c", after={"id": 1})
        messages = [
            MagicMock(value=b"invalid-json"),
            MagicMock(value=json.dumps(valid_envelope).encode()),
        ]
        fake_module, _ = _make_aiokafka_module(messages)

        provider = _provider()
        with patch.dict("sys.modules", {"aiokafka": fake_module}):
            events = []
            async for ev in provider.watch("orders"):
                events.append(ev)

        # Only the valid event should come through
        assert len(events) == 1
        assert events[0].operation == "insert"

    @pytest.mark.asyncio
    async def test_schema_change_event_skipped(self):
        """Envelopes with 'ddlType' key are silently skipped."""
        schema_change = {"ddlType": "CREATE_TABLE", "source": {}}
        valid_envelope = _envelope("u", after={"id": 2})
        messages = [
            MagicMock(value=json.dumps(schema_change).encode()),
            MagicMock(value=json.dumps(valid_envelope).encode()),
        ]
        fake_module, _ = _make_aiokafka_module(messages)

        provider = _provider()
        with patch.dict("sys.modules", {"aiokafka": fake_module}):
            events = []
            async for ev in provider.watch("orders"):
                events.append(ev)

        assert len(events) == 1
        assert events[0].operation == "update"

    @pytest.mark.asyncio
    async def test_tombstone_yields_delete(self):
        """msg.value=None (tombstone) yields a delete event."""
        tombstone = MagicMock()
        tombstone.value = None
        fake_module, _ = _make_aiokafka_module([tombstone])

        provider = _provider()
        with patch.dict("sys.modules", {"aiokafka": fake_module}):
            events = []
            async for ev in provider.watch("orders"):
                events.append(ev)

        assert len(events) == 1
        assert events[0].operation == "delete"
        assert events[0].table == "orders"

    @pytest.mark.asyncio
    async def test_op_create_watch_end_to_end(self):
        """Full watch() path: insert message produces ChangeEvent."""
        envelope = _envelope("c", after={"id": 10, "val": "x"})
        messages = [MagicMock(value=json.dumps(envelope).encode())]
        fake_module, _ = _make_aiokafka_module(messages)

        provider = _provider()
        with patch.dict("sys.modules", {"aiokafka": fake_module}):
            events = []
            async for ev in provider.watch("orders"):
                events.append(ev)

        assert len(events) == 1
        assert events[0].operation == "insert"
        assert events[0].row == {"id": 10, "val": "x"}


class TestRegistry:
    def test_get_provider_debezium_type(self):
        """get_provider('debezium', config) returns DebeziumNotificationProvider."""
        config = {
            "bootstrap_servers": "localhost:9092",
            "topic_prefix": "dbserver1",
            "database": "mydb",
        }
        provider = get_provider("debezium", config)
        assert isinstance(provider, DebeziumNotificationProvider)

    def test_get_provider_debezium_passes_config(self):
        """get_provider passes topic_prefix and database correctly."""
        config = {
            "bootstrap_servers": "kafka:9092",
            "topic_prefix": "cdc",
            "database": "salesdb",
            "consumer_group_id": "my-group",
        }
        provider = get_provider("debezium", config)
        assert provider._topic_prefix == "cdc"
        assert provider._database == "salesdb"
        assert provider._consumer_group_id == "my-group"
