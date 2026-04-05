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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.api.data.subscribe import (
    CHANNEL_PREFIX,
    _rls_matches,
    _sse_generator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeConnection:
    """Minimal stand-in for an asyncpg connection with listener support."""

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
    """Minimal stand-in for asyncpg.Pool."""

    def __init__(self, conn: FakeConnection):
        self._conn = conn

    async def acquire(self):
        return self._conn

    async def release(self, conn):
        pass


# ---------------------------------------------------------------------------
# _rls_matches
# ---------------------------------------------------------------------------

class TestRLSMatches:
    def test_no_rules_passes(self):
        rls = MagicMock()
        rls.rules = {}
        assert _rls_matches({"id": 1}, rls, "orders") is True

    def test_matching_rule_passes(self):
        rls = MagicMock()
        rls.rules = {1: "region = 'us'"}
        assert _rls_matches({"region": "us"}, rls, "orders") is True

    def test_non_matching_rule_fails(self):
        rls = MagicMock()
        rls.rules = {1: "region = 'us'"}
        assert _rls_matches({"region": "eu"}, rls, "orders") is False

    def test_missing_column_passes(self):
        rls = MagicMock()
        rls.rules = {1: "region = 'us'"}
        assert _rls_matches({"id": 1}, rls, "orders") is True

    def test_complex_expr_passes(self):
        """Non-simple expressions are treated as permissive."""
        rls = MagicMock()
        rls.rules = {1: "region IN ('us', 'eu')"}
        assert _rls_matches({"region": "jp"}, rls, "orders") is True


# ---------------------------------------------------------------------------
# _sse_generator
# ---------------------------------------------------------------------------

class TestSSEGenerator:
    @pytest.mark.asyncio
    async def test_emits_connected_comment(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()
        disconnect.set()  # Disconnect immediately after first yield

        gen = _sse_generator(pool, "orders", None, {}, disconnect)
        first = await gen.__anext__()
        assert first == ": connected\n\n"

    @pytest.mark.asyncio
    async def test_emits_data_event(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()

        gen = _sse_generator(pool, "orders", None, {}, disconnect)
        # Get the connected comment
        await gen.__anext__()

        # Fire a notification on the channel
        channel = f"{CHANNEL_PREFIX}orders"
        payload = json.dumps({"op": "INSERT", "row": {"id": 1, "name": "test"}})
        conn.fire(channel, payload)

        # Get the data event
        event = await gen.__anext__()
        assert event == f"data: {payload}\n\n"

        disconnect.set()

    @pytest.mark.asyncio
    async def test_keepalive_on_timeout(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()

        gen = _sse_generator(pool, "orders", None, {}, disconnect)
        await gen.__anext__()  # connected

        # Patch wait_for to simulate timeout quickly
        original_wait_for = asyncio.wait_for

        async def fast_timeout(coro, timeout):
            raise asyncio.TimeoutError()

        with patch("asyncio.wait_for", side_effect=fast_timeout):
            event = await gen.__anext__()

        assert event == ": keepalive\n\n"
        disconnect.set()

    @pytest.mark.asyncio
    async def test_rls_filters_events(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()

        rls_ctx = MagicMock()
        rls_ctx.has_rules.return_value = True
        rls_ctx.rules = {1: "region = 'us'"}
        rls_contexts = {"analyst": rls_ctx}

        gen = _sse_generator(pool, "orders", "analyst", rls_contexts, disconnect)
        await gen.__anext__()  # connected

        channel = f"{CHANNEL_PREFIX}orders"

        # This event should be filtered out (region = eu, rule requires us)
        conn.fire(channel, json.dumps({"op": "INSERT", "row": {"region": "eu"}}))
        # This event should pass (region = us)
        conn.fire(channel, json.dumps({"op": "INSERT", "row": {"region": "us"}}))

        event = await gen.__anext__()
        parsed = json.loads(event.removeprefix("data: ").strip())
        assert parsed["row"]["region"] == "us"

        disconnect.set()

    @pytest.mark.asyncio
    async def test_listener_cleanup(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()
        disconnect.set()

        gen = _sse_generator(pool, "orders", None, {}, disconnect)
        # Exhaust the generator
        chunks = []
        async for chunk in gen:
            chunks.append(chunk)

        # Listener should be removed after generator exits
        channel = f"{CHANNEL_PREFIX}orders"
        assert len(conn._listeners.get(channel, [])) == 0

    @pytest.mark.asyncio
    async def test_multiple_events_in_sequence(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        disconnect = asyncio.Event()

        gen = _sse_generator(pool, "orders", None, {}, disconnect)
        await gen.__anext__()  # connected

        channel = f"{CHANNEL_PREFIX}orders"
        payloads = [
            json.dumps({"op": "INSERT", "row": {"id": i}})
            for i in range(3)
        ]
        for p in payloads:
            conn.fire(channel, p)

        received = []
        for _ in range(3):
            event = await gen.__anext__()
            received.append(event)

        for i, event in enumerate(received):
            assert event == f"data: {payloads[i]}\n\n"

        disconnect.set()


# ---------------------------------------------------------------------------
# subscribe endpoint (integration-style with mocked state)
# ---------------------------------------------------------------------------

class TestSubscribeEndpoint:
    @pytest.mark.asyncio
    async def test_returns_503_without_pool(self):
        """Endpoint returns 503 when pg_pool is None."""
        from fastapi.testclient import TestClient
        from fastapi import FastAPI

        app = FastAPI()
        from provisa.api.data.subscribe import router
        app.include_router(router)

        with patch("provisa.api.app.state") as mock_state:
            mock_state.pg_pool = None
            mock_state.rls_contexts = {}
            client = TestClient(app)
            resp = client.get("/data/subscribe/orders")
            assert resp.status_code == 503

    def test_channel_prefix_format(self):
        """Channel name uses the expected prefix."""
        assert CHANNEL_PREFIX == "provisa_"
        assert f"{CHANNEL_PREFIX}orders" == "provisa_orders"
