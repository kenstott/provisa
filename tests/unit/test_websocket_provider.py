# Copyright (c) 2026 Kenneth Stott
# Canary: 3725325e-445d-44ab-8b0d-aa5eae931f14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for WebSocketNotificationProvider and _extract_path."""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from typing import AsyncIterator
from unittest.mock import MagicMock, patch

import pytest

# Stub out websockets so provider can be imported without the real library
_ws_stub = types.ModuleType("websockets")
_ws_stub.connect = MagicMock()  # will be overridden per test
sys.modules.setdefault("websockets", _ws_stub)


# ---------------------------------------------------------------------------
# _extract_path
# ---------------------------------------------------------------------------

class TestExtractPath:
    def test_top_level_key(self):
        from provisa.subscriptions.websocket_provider import _extract_path
        assert _extract_path({"a": 1}, "a") == 1

    def test_nested_key(self):
        from provisa.subscriptions.websocket_provider import _extract_path
        assert _extract_path({"a": {"b": {"c": 42}}}, "a.b.c") == 42

    def test_list_index(self):
        from provisa.subscriptions.websocket_provider import _extract_path
        assert _extract_path({"items": [10, 20, 30]}, "items.1") == 20

    def test_missing_key_returns_none(self):
        from provisa.subscriptions.websocket_provider import _extract_path
        assert _extract_path({"a": 1}, "b") is None

    def test_missing_nested_returns_none(self):
        from provisa.subscriptions.websocket_provider import _extract_path
        assert _extract_path({"a": {}}, "a.b.c") is None

    def test_non_dict_mid_path_returns_none(self):
        from provisa.subscriptions.websocket_provider import _extract_path
        assert _extract_path({"a": 99}, "a.b") is None

    def test_list_out_of_bounds_returns_none(self):
        from provisa.subscriptions.websocket_provider import _extract_path
        assert _extract_path({"items": [1]}, "items.5") is None


# ---------------------------------------------------------------------------
# WebSocketNotificationProvider
# ---------------------------------------------------------------------------

class FakeWS:
    """Fake websocket connection: yields pre-loaded messages then stops."""

    def __init__(self, messages: list[str], sent: list[str] | None = None):
        self._messages = messages
        self._sent = sent if sent is not None else []
        self._closed = False

    async def send(self, data: str) -> None:
        self._sent.append(data)

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iter()

    async def _iter(self):
        for msg in self._messages:
            yield msg

    async def close(self) -> None:
        self._closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestWebSocketProvider:
    @pytest.mark.asyncio
    async def test_yields_insert_event_by_default(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        ws = FakeWS([json.dumps({"price": 100})])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(url="ws://localhost/feed")
            events = []
            async for ev in provider.watch("trades"):
                events.append(ev)
                await provider.close()
                break

        assert len(events) == 1
        assert events[0].operation == "insert"
        assert events[0].table == "trades"
        assert events[0].row["price"] == 100

    @pytest.mark.asyncio
    async def test_op_field_sets_operation(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        ws = FakeWS([json.dumps({"op": "delete", "id": 5})])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(url="ws://localhost/feed")
            events = []
            async for ev in provider.watch("orders"):
                events.append(ev)
                await provider.close()
                break

        assert events[0].operation == "delete"

    @pytest.mark.asyncio
    async def test_sends_subscribe_payload(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        sent: list[str] = []
        ws = FakeWS([json.dumps({"tick": 1})], sent=sent)

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(
                url="ws://localhost/feed",
                subscribe_payload={"type": "subscribe", "channel": "BTC-USD"},
            )
            async for _ in provider.watch("ticks"):
                await provider.close()
                break

        assert len(sent) == 1
        assert json.loads(sent[0]) == {"type": "subscribe", "channel": "BTC-USD"}

    @pytest.mark.asyncio
    async def test_event_path_extraction(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        msg = {"data": {"price": 55, "volume": 100}}
        ws = FakeWS([json.dumps(msg)])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(
                url="ws://localhost/feed", event_path="data"
            )
            events = []
            async for ev in provider.watch("prices"):
                events.append(ev)
                await provider.close()
                break

        assert events[0].row["price"] == 55

    @pytest.mark.asyncio
    async def test_non_json_message_skipped(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        ws = FakeWS(["not-json", json.dumps({"id": 1})])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(url="ws://localhost/feed")
            events = []
            async for ev in provider.watch("t"):
                events.append(ev)
                if len(events) >= 1:
                    await provider.close()
                    break

        assert len(events) == 1
        assert events[0].row["id"] == 1

    @pytest.mark.asyncio
    async def test_iso_timestamp_parsed(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        ts_str = "2026-01-15T12:00:00+00:00"
        ws = FakeWS([json.dumps({"timestamp": ts_str, "v": 1})])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(url="ws://localhost/feed")
            events = []
            async for ev in provider.watch("t"):
                events.append(ev)
                await provider.close()
                break

        assert events[0].timestamp == datetime.fromisoformat(ts_str)

    @pytest.mark.asyncio
    async def test_unix_timestamp_parsed(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        ts_unix = 1_700_000_000.0
        ws = FakeWS([json.dumps({"_ts": ts_unix, "v": 1})])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(url="ws://localhost/feed")
            events = []
            async for ev in provider.watch("t"):
                events.append(ev)
                await provider.close()
                break

        expected = datetime.fromtimestamp(ts_unix, tz=timezone.utc)
        assert events[0].timestamp == expected

    @pytest.mark.asyncio
    async def test_close_stops_generator(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        ws = FakeWS([json.dumps({"i": i}) for i in range(100)])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(url="ws://localhost/feed")
            count = 0
            async for _ in provider.watch("t"):
                count += 1
                await provider.close()
                break

        assert count == 1

    @pytest.mark.asyncio
    async def test_non_dict_event_wrapped(self):
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        ws = FakeWS([json.dumps(42)])

        with patch("websockets.connect", return_value=ws):
            provider = WebSocketNotificationProvider(url="ws://localhost/feed")
            events = []
            async for ev in provider.watch("t"):
                events.append(ev)
                await provider.close()
                break

        assert events[0].row == {"value": 42}


# ---------------------------------------------------------------------------
# Registry integration
# ---------------------------------------------------------------------------

class TestWebSocketRegistry:
    def test_registry_returns_websocket_provider(self):
        from provisa.subscriptions.registry import get_provider
        from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider

        provider = get_provider("websocket", {"url": "ws://localhost/feed"})
        assert isinstance(provider, WebSocketNotificationProvider)

    def test_registry_passes_subscribe_payload(self):
        from provisa.subscriptions.registry import get_provider

        payload = {"type": "subscribe"}
        provider = get_provider("websocket", {"url": "ws://x", "subscribe_payload": payload})
        assert provider._subscribe_payload == payload

    def test_registry_passes_event_path(self):
        from provisa.subscriptions.registry import get_provider

        provider = get_provider("websocket", {"url": "ws://x", "event_path": "data.events"})
        assert provider._event_path == "data.events"
