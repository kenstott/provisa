# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-338 / REQ-339 / REQ-342 / REQ-343 — streaming and polling source connectors."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from datetime import datetime, timezone
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.subscriptions import ChangeEvent

# Ensure the websockets module can be imported by the provider even when the
# real library is unavailable in the unit test context.
if "websockets" not in sys.modules:
    _ws_stub = types.ModuleType("websockets")
    _ws_stub.connect = MagicMock()
    sys.modules["websockets"] = _ws_stub

from provisa.subscriptions.websocket_provider import WebSocketNotificationProvider
from provisa.subscriptions.rss_provider import RSSNotificationProvider, parse_feed, _parse_date

scenarios("../features/REQ-338.feature")
scenarios("../features/REQ-339.feature")
scenarios("../features/REQ-342.feature")
scenarios("../features/REQ-343.feature")


class _FakeWS:
    """In-memory fake WebSocket connection used to drive the provider."""

    def __init__(self, messages: list[str]) -> None:
        self._messages = messages
        self.sent: list[str] = []
        self.closed = False

    async def send(self, data: str) -> None:
        self.sent.append(data)

    def __aiter__(self) -> AsyncIterator[str]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[str]:
        for msg in self._messages:
            yield msg

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> "_FakeWS":
        return self

    async def __aexit__(self, *args) -> None:
        pass


class _DroppingWS(_FakeWS):
    """Fake WebSocket whose iteration raises a transient disconnect error."""

    async def _iter(self) -> AsyncIterator[str]:
        # Emit any preloaded messages first, then simulate a transient drop.
        for msg in self._messages:
            yield msg
        raise ConnectionError("transient disconnect")


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# REQ-338
# ---------------------------------------------------------------------------

@given("a WebSocket source registered with an optional subscribe payload")
def register_websocket_source(shared_data: dict) -> None:
    subscribe_payload = {"action": "subscribe", "channels": ["trades"]}
    messages = [
        json.dumps({"symbol": "ABC", "price": 100, "qty": 5}),
        json.dumps({"symbol": "XYZ", "price": 200, "qty": 3}),
    ]
    fake_ws = _FakeWS(messages)

    provider = WebSocketNotificationProvider(
        url="ws://localhost/feed",
        subscribe_payload=subscribe_payload,
    )

    shared_data["provider"] = provider
    shared_data["fake_ws"] = fake_ws
    shared_data["subscribe_payload"] = subscribe_payload
    shared_data["expected_count"] = len(messages)

    assert provider is not None
    assert shared_data["subscribe_payload"]["action"] == "subscribe"


@when("Provisa connects to the WebSocket server")
@pytest.mark.asyncio
async def connect_to_websocket(shared_data: dict) -> None:
    provider: WebSocketNotificationProvider = shared_data["provider"]
    fake_ws: _FakeWS = shared_data["fake_ws"]

    events: list[ChangeEvent] = []
    with patch("websockets.connect", return_value=fake_ws):
        async for ev in provider.watch("trades"):
            events.append(ev)
            if len(events) >= shared_data["expected_count"]:
                await provider.close()
                break

    shared_data["events"] = events
    shared_data["sent"] = fake_ws.sent

    # The subscribe payload should have been sent on connect.
    assert len(fake_ws.sent) == 1
    assert json.loads(fake_ws.sent[0]) == shared_data["subscribe_payload"]


@then(
    "received JSON messages are emitted as ChangeEvents into the governed query fabric"
)
def assert_change_events_emitted(shared_data: dict) -> None:
    events: list[ChangeEvent] = shared_data["events"]

    assert len(events) == shared_data["expected_count"]
    for ev in events:
        assert isinstance(ev, ChangeEvent)
        assert ev.operation == "insert"
        assert ev.table == "trades"
        assert isinstance(ev.row, dict)
        assert isinstance(ev.timestamp, datetime)


# ---------------------------------------------------------------------------
# REQ-339 — WebSocket auto-reconnect on transient disconnect
# ---------------------------------------------------------------------------

@given("a WebSocket source that experiences a transient disconnect")
def websocket_source_with_transient_disconnect(shared_data: dict) -> None:
    # First connection emits one event then raises ConnectionError (transient
    # disconnect). The provider must auto-reconnect and the second connection
    # continues emitting events.
    first_ws = _DroppingWS([json.dumps({"symbol": "ABC", "price": 100})])
    second_ws = _FakeWS([json.dumps({"symbol": "XYZ", "price": 200})])

    provider = WebSocketNotificationProvider(
        url="ws://localhost/feed",
        reconnect_interval=0.01,
    )

    shared_data["provider"] = provider
    shared_data["first_ws"] = first_ws
    shared_data["second_ws"] = second_ws

    assert provider is not None
    # The reconnect interval must be configurable (default 5s per REQ-339).
    assert getattr(provider, "reconnect_interval", None) == 0.01


@when("the connection drops")
@pytest.mark.asyncio
async def the_connection_drops(shared_data: dict) -> None:
    provider: WebSocketNotificationProvider = shared_data["provider"]
    connections = [shared_data["first_ws"], shared_data["second_ws"]]

    def connect_side_effect(*args, **kwargs):
        if connections:
            return connections.pop(0)
        # Any subsequent reconnect (should not be reached) gets an empty conn.
        return _FakeWS([])

    events: list[ChangeEvent] = []
    with patch("websockets.connect", side_effect=connect_side_effect):
        # Bound the reconnect loop so a failing implementation cannot hang the
        # test indefinitely.
        async def _consume() -> None:
            async for ev in provider.watch("trades"):
                events.append(ev)
                if len(events) >= 2:
                    await provider.close()
                    break

        await asyncio.wait_for(_consume(), timeout=5.0)

    shared_data["events"] = events
    shared_data["remaining_connections"] = connections


@then(
    "the provider auto-reconnects and resumes emitting ChangeEvents without manual intervention"
)
def assert_provider_reconnected(shared_data: dict) -> None:
    events: list[ChangeEvent] = shared_data["events"]

    # Both connections should have been consumed (the first dropped, the
    # second reconnected and resumed).
    assert len(events) == 2
    assert shared_data["remaining_connections"] == []

    symbols = [ev.row["symbol"] for ev in events]
    assert "ABC" in symbols
    assert "XYZ" in symbols
    for ev in events:
        assert isinstance(ev, ChangeEvent)
        assert ev.operation == "insert"
        assert ev.table == "trades"


# ---------------------------------------------------------------------------
# REQ-343 — RSS 2.0 and Atom format parity
# ---------------------------------------------------------------------------

_RSS_FEED = b"""<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Item One</title>
      <link>https://example.com/1</link>
      <description>First item</description>
      <pubDate>Thu, 09 Apr 2030 10:00:00 +0000</pubDate>
      <guid>https://example.com/1</guid>
    </item>
    <item>
      <title>Item Bad Date</title>
      <link>https://example.com/2</link>
      <description>Unparseable date item</description>
      <pubDate>not-a-real-date</pubDate>
      <guid>https://example.com/2</guid>
    </item>
  </channel>
</rss>"""

_ATOM_FEED = b"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Test</title>
  <entry>
    <title>Atom Entry One</title>
    <link href="https://example.com/a1"/>
    <id>https://example.com/a1</id>
    <updated>2030-04-09T10:00:00Z</updated>
    <summary>First atom entry</summary>
  </entry>
  <entry>
    <title>Atom Entry Bad Date</title>
    <link href="https://example.com/a2"/>
    <id>https://example.com/a2</id>
    <updated>garbage</updated>
    <summary>Unparseable atom entry</summary>
  </entry>
</feed>"""


@given("an RSS 2.0 feed and an Atom feed registered as sources")
def register_rss_and_atom_feeds(shared_data: dict) -> None:
    shared_data["rss_feed"] = _RSS_FEED
    shared_data["atom_feed"] = _ATOM_FEED

    # Sanity: the raw feeds carry the format-distinguishing elements.
    assert b"<item>" in shared_data["rss_feed"]
    assert b"<entry>" in shared_data["atom_feed"]


@when("items are parsed")
def parse_rss_and_atom_feeds(shared_data: dict) -> None:
    shared_data["rss_items"] = parse_feed(shared_data["rss_feed"])
    shared_data["atom_items"] = parse_feed(shared_data["atom_feed"])

    assert len(shared_data["rss_items"]) == 2
    assert len(shared_data["atom_items"]) == 2


@then(
    "both formats extract title, link, description/summary, published, and id; "
    "unparseable dates use datetime.min"
)
def assert_rss_atom_parity(shared_data: dict) -> None:
    rss_items = shared_data["rss_items"]
    atom_items = shared_data["atom_items"]

    required_fields = {"title", "link", "description", "published", "id"}

    # RSS 2.0 <item> parity.
    rss_first = rss_items[0]
    assert required_fields <= set(rss_first.keys())
    assert rss_first["title"] == "Item One"
    assert rss_first["link"] == "https://example.com/1"
    assert rss_first["description"] == "First item"
    assert rss_first["id"] == "https://example.com/1"
    assert isinstance(rss_first["published"], datetime)
    assert rss_first["published"].year == 2030

    # Atom <entry> parity — same field set, summary mapped to description.
    atom_first = atom_items[0]
    assert required_fields <= set(atom_first.keys())
    assert atom_first["title"] == "Atom Entry One"
    assert atom_first["link"] == "https://example.com/a1"
    assert atom_first["description"] == "First atom entry"
    assert atom_first["id"] == "https://example.com/a1"
    assert isinstance(atom_first["published"], datetime)
    assert atom_first["published"].year == 2030

    # Unparseable dates fall back to datetime.min (UTC sentinel) in both
    # formats rather than silently using the current time.
    sentinel = datetime.min.replace(tzinfo=timezone.utc)
    rss_bad = rss_items[1]
    atom_bad = atom_items[1]
    assert rss_bad["published"] == sentinel
    assert atom_bad["published"] == sentinel

    # The sentinel is also produced directly by the date parser.
    assert _parse_date("not-a-real-date") == sentinel
    assert _parse_date("garbage") == sentinel

    # And valid RFC 2822 / ISO 8601 dates parse to real timezone-aware values.
    rfc = _parse_date("Thu, 09 Apr 2030 10:00:00 +0000")
    iso = _parse_date("2030-04-09T10:00:00Z")
    assert rfc != sentinel and rfc.tzinfo is not None
    assert iso != sentinel and iso.tzinfo is not None
