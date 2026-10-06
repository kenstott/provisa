# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7a2c10-8f4d-4b1a-9c5e-d2f8a6b30e71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for SSE subscription endpoint (Phase AB2)."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from provisa.api.data.subscribe import (
    CHANNEL_PREFIX,
    _governed_changes,
    _sse_generator,
)
from provisa.live.governed import GovernanceKey


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeConnection:
    """Minimal stand-in for the listener registry NOTIFY callbacks are delivered through."""

    def __init__(self):
        self._listeners: dict[str, list] = {}

    async def add_listener(self, channel: str, callback):
        self._listeners.setdefault(channel, []).append(callback)

    async def remove_listener(self, channel: str, callback):
        if channel in self._listeners:
            self._listeners[channel] = [cb for cb in self._listeners[channel] if cb is not callback]

    def fire(self, channel: str, payload: str):
        for cb in self._listeners.get(channel, []):
            cb(self, 1234, channel, payload)


class FakePool:
    """Minimal stand-in for :class:`provisa.core.database.Database`.

    LISTEN registers on the database itself — its listener thread owns the LISTEN connection — so
    an SSE stream holds no pooled connection. ``acquire`` is the async context manager the real
    handle offers; the SSE generator must not need it, which ``acquired`` records.
    """

    def __init__(self, conn: FakeConnection):
        self._conn = conn
        self.acquired = 0

    async def add_listener(self, channel: str, callback):
        await self._conn.add_listener(channel, callback)

    async def remove_listener(self, channel: str, callback):
        await self._conn.remove_listener(channel, callback)

    @asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        yield self._conn


# ---------------------------------------------------------------------------
# _governed_changes: every change is read back, governed as the subscriber (REQ-336, REQ-286)
# ---------------------------------------------------------------------------

_KEY = GovernanceKey("acme", "analyst", (("region", "us"),))
_REF = '"sales"."orders"'


async def _events(*events):
    for event in events:
        yield event


def _governed(monkeypatch, answers: dict):
    """governed_rows as the pipeline answers the subscriber: ``answers`` maps a key value to the
    rows the subscriber's governed read returns for it (absent: the read returns nothing)."""
    calls: list[tuple] = []

    async def _rows(sql, key, params=None):
        calls.append((sql, key, params))
        return answers.get(params[0], []) if params else []

    monkeypatch.setattr("provisa.live.governed.governed_rows", _rows)
    return calls


async def _collect(gen) -> list:
    out = []
    async for chunk in gen:
        out.append(chunk)
    return out


def _data(chunks) -> list[dict]:
    return [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]


class TestGovernedChanges:
    @pytest.mark.asyncio
    async def test_the_raw_row_is_never_forwarded(self, monkeypatch):
        """The change event's row is read back through the pipeline as the subscriber's key;
        what the subscriber receives is that read, not the event."""
        calls = _governed(monkeypatch, {1: [{"id": 1, "region": "us", "ssn": "XXX-XX"}]})
        events = _events(("INSERT", {"id": 1, "region": "us", "ssn": "123-45", "secret": "s"}))
        chunks = await _collect(_governed_changes(events, _KEY, _REF, ["id"]))
        assert chunks[0] == ": connected\n\n"
        assert _data(chunks) == [
            {"op": "INSERT", "row": {"id": 1, "region": "us", "ssn": "XXX-XX"}}
        ]
        assert calls == [(f'SELECT * FROM {_REF} WHERE "id" = $1', _KEY, [1])]

    @pytest.mark.asyncio
    async def test_a_row_the_key_may_not_read_is_not_delivered(self, monkeypatch):
        """A row outside the subscriber's row rules is not delivered -- whatever the rule's form;
        no rule is ever treated as passing."""
        _governed(monkeypatch, {2: [{"id": 2, "region": "us"}]})  # id 1 is outside the rules
        events = _events(
            ("INSERT", {"id": 1, "region": "jp"}), ("INSERT", {"id": 2, "region": "us"})
        )
        assert _data(await _collect(_governed_changes(events, _KEY, _REF, ["id"]))) == [
            {"op": "INSERT", "row": {"id": 2, "region": "us"}}
        ]

    @pytest.mark.asyncio
    async def test_a_delete_is_delivered_only_for_a_row_this_stream_delivered(self, monkeypatch):
        _governed(monkeypatch, {1: [{"id": 1}]})
        events = _events(
            ("DELETE", {"id": 9}),  # never delivered: its deletion says nothing to this key
            ("INSERT", {"id": 1}),
            ("DELETE", {"id": 1}),
        )
        assert _data(await _collect(_governed_changes(events, _KEY, _REF, ["id"]))) == [
            {"op": "INSERT", "row": {"id": 1}},
            {"op": "DELETE", "row": {"id": 1}},
        ]

    @pytest.mark.asyncio
    async def test_a_row_that_leaves_the_keys_view_is_delivered_as_a_delete(self, monkeypatch):
        answers = {1: [{"id": 1, "region": "us"}]}
        calls: list = []

        async def _rows(sql, key, params=None):
            calls.append(params)
            # The second read finds the row moved out of the key's rules.
            return answers.get(params[0], []) if len(calls) == 1 else []

        monkeypatch.setattr("provisa.live.governed.governed_rows", _rows)
        events = _events(
            ("INSERT", {"id": 1, "region": "us"}), ("UPDATE", {"id": 1, "region": "jp"})
        )
        assert _data(await _collect(_governed_changes(events, _KEY, _REF, ["id"]))) == [
            {"op": "INSERT", "row": {"id": 1, "region": "us"}},
            {"op": "DELETE", "row": {"id": 1}},
        ]

    @pytest.mark.asyncio
    async def test_an_event_without_its_key_is_dropped(self, monkeypatch):
        calls = _governed(monkeypatch, {})
        events = _events(("INSERT", {"region": "us"}))
        assert _data(await _collect(_governed_changes(events, _KEY, _REF, ["id"]))) == []
        assert calls == []

    @pytest.mark.asyncio
    async def test_a_keepalive_tick_is_a_comment(self, monkeypatch):
        _governed(monkeypatch, {})
        chunks = await _collect(_governed_changes(_events((None, None)), _KEY, _REF, ["id"]))
        assert chunks == [": connected\n\n", ": keepalive\n\n"]


# ---------------------------------------------------------------------------
# _sse_generator
# ---------------------------------------------------------------------------


class TestSSEGenerator:
    @pytest.mark.asyncio
    async def test_emits_data_event(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()

        gen = _sse_generator(pool, "orders", disconnect)
        task = asyncio.ensure_future(gen.__anext__())
        await asyncio.sleep(0)  # the listener is registered

        # Fire a notification on the channel
        channel = f"{CHANNEL_PREFIX}orders"
        conn.fire(channel, json.dumps({"op": "INSERT", "row": {"id": 1, "name": "test"}}))

        # The change event, as the channel reported it
        assert await task == ("INSERT", {"id": 1, "name": "test"})

        disconnect.set()

    @pytest.mark.asyncio
    async def test_keepalive_on_timeout(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()

        gen = _sse_generator(pool, "orders", disconnect)

        # Patch wait_for to simulate timeout quickly
        async def fast_timeout(coro, timeout):
            coro.close()
            raise asyncio.TimeoutError()

        with patch("asyncio.wait_for", side_effect=fast_timeout):
            event = await gen.__anext__()

        assert event == (None, None)  # a keepalive tick
        disconnect.set()

    @pytest.mark.asyncio
    async def test_rls_filters_events(self, monkeypatch):
        """A notification outside the subscriber's row rules never reaches it (REQ-336)."""
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()
        _governed(monkeypatch, {2: [{"id": 2, "region": "us"}]})  # the key reads id 2 only

        stream = _governed_changes(_sse_generator(pool, "orders", disconnect), _KEY, _REF, ["id"])
        assert await stream.__anext__() == ": connected\n\n"
        pending = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0)
        channel = f"{CHANNEL_PREFIX}orders"
        conn.fire(channel, json.dumps({"op": "INSERT", "row": {"id": 1, "region": "eu"}}))
        conn.fire(channel, json.dumps({"op": "INSERT", "row": {"id": 2, "region": "us"}}))

        event = await pending
        assert json.loads(event.removeprefix("data: ").strip())["row"] == {"id": 2, "region": "us"}
        disconnect.set()

    @pytest.mark.asyncio
    async def test_masking_applied_to_streamed_row(self, monkeypatch):
        """REQ-336: a masked column reaches the subscriber as the governed read masks it, never as
        the notification carried it."""
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()
        _governed(monkeypatch, {1: [{"id": 1, "ssn": "XXX-XX", "salary": "REDACTED"}]})

        stream = _governed_changes(_sse_generator(pool, "orders", disconnect), _KEY, _REF, ["id"])
        await stream.__anext__()  # connected
        pending = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0)
        conn.fire(
            f"{CHANNEL_PREFIX}orders",
            json.dumps({"op": "INSERT", "row": {"id": 1, "ssn": "123-45", "salary": "90000"}}),
        )

        parsed = json.loads((await pending).removeprefix("data: ").strip())
        assert parsed["row"] == {"id": 1, "ssn": "XXX-XX", "salary": "REDACTED"}
        disconnect.set()

    @pytest.mark.asyncio
    async def test_listener_cleanup(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()
        disconnect.set()

        gen = _sse_generator(pool, "orders", disconnect)
        # Exhaust the generator
        chunks = []
        async for chunk in gen:
            chunks.append(chunk)

        # Listener should be removed after generator exits
        channel = f"{CHANNEL_PREFIX}orders"
        assert len(conn._listeners.get(channel, [])) == 0
        # The stream never held a pooled connection (REQ-1882: listeners must not exhaust the pool).
        assert pool.acquired == 0

    @pytest.mark.asyncio
    async def test_multiple_events_in_sequence(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()

        gen = _sse_generator(pool, "orders", disconnect)
        first = asyncio.ensure_future(gen.__anext__())
        await asyncio.sleep(0)  # the listener is registered

        channel = f"{CHANNEL_PREFIX}orders"
        for i in range(3):
            conn.fire(channel, json.dumps({"op": "INSERT", "row": {"id": i}}))

        received = [await first] + [await gen.__anext__() for _ in range(2)]
        assert received == [("INSERT", {"id": i}) for i in range(3)]

        disconnect.set()


# ---------------------------------------------------------------------------
# subscribe endpoint (integration-style with mocked state)
# ---------------------------------------------------------------------------


class TestSubscribeEndpoint:
    @pytest.mark.asyncio
    async def test_returns_503_without_pool(self, monkeypatch):
        """Endpoint returns 503 when the org's tenant_db is not bound -- after the subscriber's
        governed check, which every subscription passes first."""
        from types import SimpleNamespace

        from provisa.api.data.subscribe import subscribe
        from provisa.api.errors import ApiError
        from provisa.core.request_context import reset_current_org, set_current_org

        meta = SimpleNamespace(table_id=7, source_id="pg", domain_id="sales")
        state = SimpleNamespace(
            tenant_db=None,
            roles={"analyst": {}},
            view_context=SimpleNamespace(tables={"orders": meta}, pk_columns={7: ["id"]}),
            table_path_maps={},
            source_types={"pg": "postgresql"},
        )
        monkeypatch.setattr("provisa.api.app.state", state)
        monkeypatch.setattr("provisa.live.governed.table_ref", lambda _m: _REF)
        _governed(monkeypatch, {})
        token = set_current_org("acme")
        try:
            with pytest.raises(ApiError) as err:
                await subscribe("orders", SimpleNamespace(state=SimpleNamespace(role="analyst")))
        finally:
            reset_current_org(token)
        assert err.value.status_code == 503

    def test_channel_prefix_format(self):
        """Channel name uses the expected prefix."""
        assert CHANNEL_PREFIX == "provisa_"
        assert f"{CHANNEL_PREFIX}orders" == "provisa_orders"
