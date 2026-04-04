# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for subscription providers (Phase AB)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.subscriptions.base import ChangeEvent, NotificationProvider
from provisa.subscriptions.registry import get_provider


# ---------------------------------------------------------------------------
# ChangeEvent
# ---------------------------------------------------------------------------

class TestChangeEvent:
    def test_fields(self):
        ev = ChangeEvent(operation="insert", table="orders", row={"id": 1})
        assert ev.operation == "insert"
        assert ev.table == "orders"
        assert ev.row == {"id": 1}
        assert isinstance(ev.timestamp, datetime)

    def test_default_timestamp_is_utc(self):
        ev = ChangeEvent(operation="update", table="t", row={})
        assert ev.timestamp.tzinfo is not None


# ---------------------------------------------------------------------------
# PgNotificationProvider
# ---------------------------------------------------------------------------

class FakeConnection:
    def __init__(self):
        self._listeners: dict[str, list] = {}

    async def add_listener(self, channel: str, callback):
        self._listeners.setdefault(channel, []).append(callback)

    async def remove_listener(self, channel: str, callback):
        if channel in self._listeners:
            self._listeners[channel] = [
                cb for cb in self._listeners[channel] if cb is not callback
            ]

    def fire(self, channel: str, payload: str):
        for cb in self._listeners.get(channel, []):
            cb(self, 1234, channel, payload)


class FakePool:
    def __init__(self, conn: FakeConnection):
        self._conn = conn

    async def acquire(self):
        return self._conn

    async def release(self, conn):
        pass


class TestPgProvider:
    @pytest.mark.asyncio
    async def test_emits_change_events(self):
        from provisa.subscriptions.pg_provider import PgNotificationProvider

        conn = FakeConnection()
        pool = FakePool(conn)
        provider = PgNotificationProvider(pool=pool)

        gen = provider.watch("orders")
        # Fire a notification after the listener is registered
        await asyncio.sleep(0)

        payload = json.dumps({"op": "INSERT", "row": {"id": 1}})

        async def fire_and_collect():
            # Let the generator set up
            await asyncio.sleep(0.05)
            conn.fire("provisa_orders", payload)

        task = asyncio.create_task(fire_and_collect())
        event = await gen.__anext__()
        task.cancel()

        assert event.operation == "insert"
        assert event.table == "orders"
        assert event.row == {"id": 1}

    @pytest.mark.asyncio
    async def test_close(self):
        from provisa.subscriptions.pg_provider import PgNotificationProvider

        conn = FakeConnection()
        pool = FakePool(conn)
        provider = PgNotificationProvider(pool=pool)
        await provider.close()
        assert provider._conn is None

    @pytest.mark.asyncio
    async def test_invalid_json_skipped(self):
        from provisa.subscriptions.pg_provider import PgNotificationProvider

        conn = FakeConnection()
        pool = FakePool(conn)
        provider = PgNotificationProvider(pool=pool)

        gen = provider.watch("orders")

        async def fire_messages():
            await asyncio.sleep(0.05)
            conn.fire("provisa_orders", "not-json")
            conn.fire(
                "provisa_orders",
                json.dumps({"op": "DELETE", "row": {"id": 2}}),
            )

        task = asyncio.create_task(fire_messages())
        event = await gen.__anext__()
        task.cancel()

        assert event.operation == "delete"
        assert event.row == {"id": 2}


# ---------------------------------------------------------------------------
# MongoNotificationProvider
# ---------------------------------------------------------------------------

class FakeChangeStream:
    def __init__(self, events):
        self._events = events
        self._closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._events:
            raise StopAsyncIteration
        return self._events.pop(0)

    async def close(self):
        self._closed = True


class TestMongoProvider:
    @pytest.mark.asyncio
    async def test_emits_insert_events(self):
        from provisa.subscriptions.mongo_provider import MongoNotificationProvider

        stream = FakeChangeStream([
            {
                "operationType": "insert",
                "fullDocument": {"_id": "abc", "name": "test"},
            },
        ])
        collection = MagicMock()
        collection.watch.return_value = stream
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=collection)

        provider = MongoNotificationProvider(database=db)
        events = []
        async for ev in provider.watch("users"):
            events.append(ev)

        assert len(events) == 1
        assert events[0].operation == "insert"
        assert events[0].row["name"] == "test"

    @pytest.mark.asyncio
    async def test_emits_delete_events(self):
        from provisa.subscriptions.mongo_provider import MongoNotificationProvider

        stream = FakeChangeStream([
            {
                "operationType": "delete",
                "documentKey": {"_id": "abc123"},
            },
        ])
        collection = MagicMock()
        collection.watch.return_value = stream
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=collection)

        provider = MongoNotificationProvider(database=db)
        events = []
        async for ev in provider.watch("users"):
            events.append(ev)

        assert len(events) == 1
        assert events[0].operation == "delete"
        assert events[0].row["_id"] == "abc123"

    @pytest.mark.asyncio
    async def test_close(self):
        from provisa.subscriptions.mongo_provider import MongoNotificationProvider

        stream = FakeChangeStream([])
        provider = MongoNotificationProvider(database=MagicMock())
        provider._cursor = stream
        await provider.close()
        assert stream._closed


# ---------------------------------------------------------------------------
# PollingNotificationProvider
# ---------------------------------------------------------------------------

class FakeRow(dict):
    """Dict subclass that supports asyncpg-style Record access."""
    pass


class FakePollingConn:
    def __init__(self, rows_batches):
        self._batches = rows_batches
        self._call_count = 0

    async def fetch(self, query, *args):
        if self._call_count < len(self._batches):
            batch = self._batches[self._call_count]
            self._call_count += 1
            return batch
        return []


class FakePollingPool:
    def __init__(self, conn):
        self._conn = conn

    async def acquire(self):
        return self._conn

    async def release(self, conn):
        pass


class TestPollingProvider:
    @pytest.mark.asyncio
    async def test_emits_update_events(self):
        from provisa.subscriptions.polling_provider import PollingNotificationProvider

        now = datetime.now(timezone.utc)
        rows = [FakeRow({"id": 1, "updated_at": now, "name": "a"})]
        conn = FakePollingConn([rows, []])
        pool = FakePollingPool(conn)

        provider = PollingNotificationProvider(
            pool=pool, poll_interval=0.01, soft_delete_column=None
        )
        events = []
        count = 0
        async for ev in provider.watch("orders"):
            events.append(ev)
            count += 1
            if count >= 1:
                await provider.close()
                break

        assert len(events) == 1
        assert events[0].operation == "update"
        assert events[0].row["name"] == "a"

    @pytest.mark.asyncio
    async def test_detects_soft_delete(self):
        from provisa.subscriptions.polling_provider import PollingNotificationProvider

        now = datetime.now(timezone.utc)
        rows = [FakeRow({"id": 1, "updated_at": now, "deleted_at": now})]
        conn = FakePollingConn([rows, []])
        pool = FakePollingPool(conn)

        provider = PollingNotificationProvider(
            pool=pool, poll_interval=0.01, soft_delete_column="deleted_at"
        )
        events = []
        async for ev in provider.watch("orders"):
            events.append(ev)
            await provider.close()
            break

        assert events[0].operation == "delete"

    @pytest.mark.asyncio
    async def test_warns_no_soft_delete(self, caplog):
        from provisa.subscriptions.polling_provider import PollingNotificationProvider

        import logging
        with caplog.at_level(logging.WARNING):
            PollingNotificationProvider(pool=MagicMock(), soft_delete_column=None)
        assert "no soft-delete column" in caplog.text


# ---------------------------------------------------------------------------
# KafkaNotificationProvider
# ---------------------------------------------------------------------------

class FakeKafkaMessage:
    def __init__(self, value, timestamp=None):
        self.value = value
        self.timestamp = timestamp


class FakeKafkaConsumer:
    def __init__(self, *args, **kwargs):
        self._messages = []
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


class TestKafkaProvider:
    @pytest.mark.asyncio
    async def test_emits_events_from_topic(self):
        from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

        consumer = FakeKafkaConsumer()
        consumer._messages = [
            FakeKafkaMessage(
                json.dumps({"op": "insert", "row": {"id": 1}}).encode(),
                timestamp=1700000000000,
            ),
        ]

        provider = KafkaNotificationProvider(
            bootstrap_servers="localhost:9092",
        )

        fake_module = MagicMock()
        fake_module.AIOKafkaConsumer = lambda *a, **kw: consumer

        with patch.dict("sys.modules", {"aiokafka": fake_module}):
            events = []
            async for ev in provider.watch("orders"):
                events.append(ev)

        assert len(events) == 1
        assert events[0].operation == "insert"
        assert events[0].row == {"id": 1}

    @pytest.mark.asyncio
    async def test_handles_plain_value(self):
        from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

        consumer = FakeKafkaConsumer()
        consumer._messages = [
            FakeKafkaMessage(json.dumps("hello").encode(), timestamp=None),
        ]

        provider = KafkaNotificationProvider(bootstrap_servers="localhost:9092")

        fake_module = MagicMock()
        fake_module.AIOKafkaConsumer = lambda *a, **kw: consumer

        with patch.dict("sys.modules", {"aiokafka": fake_module}):
            events = []
            async for ev in provider.watch("topic"):
                events.append(ev)

        assert len(events) == 1
        assert events[0].operation == "insert"
        assert events[0].row == {"value": "hello"}

    @pytest.mark.asyncio
    async def test_close(self):
        from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

        consumer = FakeKafkaConsumer()
        consumer._started = True
        provider = KafkaNotificationProvider(bootstrap_servers="localhost:9092")
        provider._consumer = consumer
        await provider.close()
        assert not consumer._started


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_pg_provider(self):
        provider = get_provider("postgresql", {"pool": MagicMock()})
        from provisa.subscriptions.pg_provider import PgNotificationProvider
        assert isinstance(provider, PgNotificationProvider)

    def test_mongo_provider(self):
        provider = get_provider("mongodb", {"database": MagicMock()})
        from provisa.subscriptions.mongo_provider import MongoNotificationProvider
        assert isinstance(provider, MongoNotificationProvider)

    def test_kafka_provider(self):
        provider = get_provider("kafka", {"bootstrap_servers": "localhost:9092"})
        from provisa.subscriptions.kafka_provider import KafkaNotificationProvider
        assert isinstance(provider, KafkaNotificationProvider)

    def test_polling_fallback(self):
        provider = get_provider("mysql", {"pool": MagicMock()})
        from provisa.subscriptions.polling_provider import PollingNotificationProvider
        assert isinstance(provider, PollingNotificationProvider)

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="No subscription provider"):
            get_provider("unknown_db", {})

    def test_polling_with_soft_delete(self):
        provider = get_provider(
            "mysql",
            {"pool": MagicMock(), "soft_delete_column": "deleted_at"},
        )
        assert provider._soft_delete_column == "deleted_at"
