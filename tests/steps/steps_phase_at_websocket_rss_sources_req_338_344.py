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
from unittest.mock import MagicMock, patch

import pytest
from pytest_bdd import given, when, then, scenarios

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
def connect_to_websocket(shared_data: dict) -> None:
    provider: WebSocketNotificationProvider = shared_data["provider"]
    fake_ws: _FakeWS = shared_data["fake_ws"]

    async def _run() -> None:
        events: list[ChangeEvent] = []
        with patch("websockets.connect", return_value=fake_ws):
            async for ev in provider.watch("trades"):
                events.append(ev)
                if len(events) >= shared_data["expected_count"]:
                    await provider.close()
                    break
        shared_data["events"] = events
        shared_data["sent"] = fake_ws.sent

    asyncio.run(_run())

    # The subscribe payload should have been sent on connect.
    assert len(fake_ws.sent) == 1
    assert json.loads(fake_ws.sent[0]) == shared_data["subscribe_payload"]


@then("received JSON messages are emitted as ChangeEvents into the governed data fabric")
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
def the_connection_drops(shared_data: dict) -> None:
    provider: WebSocketNotificationProvider = shared_data["provider"]
    connections = [shared_data["first_ws"], shared_data["second_ws"]]

    def connect_side_effect(*args, **kwargs):
        if connections:
            return connections.pop(0)
        # Any subsequent reconnect (should not be reached) gets an empty conn.
        return _FakeWS([])

    async def _run() -> list[ChangeEvent]:
        events: list[ChangeEvent] = []
        with patch("websockets.connect", side_effect=connect_side_effect):

            async def _consume() -> None:
                async for ev in provider.watch("trades"):
                    events.append(ev)
                    if len(events) >= 2:
                        await provider.close()
                        break

            await asyncio.wait_for(_consume(), timeout=5.0)
        return events

    events = asyncio.run(_run())
    shared_data["events"] = events
    shared_data["remaining_connections"] = connections


@then(
    "the provider auto-reconnects after reconnect_interval and continues emitting events until close() is called"
)
def assert_provider_auto_reconnects_and_continues(shared_data: dict) -> None:
    events: list[ChangeEvent] = shared_data["events"]

    # Both connections should have been consumed (the first dropped, the
    # second reconnected and resumed).
    assert len(events) == 2, (
        f"Expected 2 events after reconnect, got {len(events)}: {[ev.row for ev in events]}"
    )
    assert shared_data["remaining_connections"] == [], (
        "Not all connections were consumed — provider did not reconnect"
    )

    symbols = [ev.row["symbol"] for ev in events]
    assert "ABC" in symbols, "Event from first (dropped) connection is missing"
    assert "XYZ" in symbols, "Event from reconnected connection is missing"

    for ev in events:
        assert isinstance(ev, ChangeEvent)
        assert ev.operation == "insert", f"Expected operation='insert', got '{ev.operation}'"
        assert ev.table == "trades", f"Expected table='trades', got '{ev.table}'"
        assert isinstance(ev.row, dict)
        assert isinstance(ev.timestamp, datetime)
        assert ev.timestamp.tzinfo is not None


@then("the provider auto-reconnects and resumes emitting ChangeEvents without manual intervention")
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
# REQ-342 — RSS polling source with watermark filtering
# ---------------------------------------------------------------------------

# Atom feed used for the REQ-342 scenario.  It has three entries:
#   - entry_old:  published BEFORE the watermark  → must be filtered out
#   - entry_new1: published AFTER  the watermark  → must be emitted
#   - entry_new2: published AFTER  the watermark  → must be emitted
_REQ342_ATOM_FEED = b"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>REQ-342 Test Feed</title>
  <entry>
    <title>Old Entry</title>
    <link href="https://example.com/old"/>
    <id>https://example.com/old</id>
    <updated>2030-04-09T08:00:00Z</updated>
    <summary>Published before the watermark</summary>
  </entry>
  <entry>
    <title>New Entry One</title>
    <link href="https://example.com/new1"/>
    <id>https://example.com/new1</id>
    <updated>2030-04-09T10:30:00Z</updated>
    <summary>Published after the watermark</summary>
  </entry>
  <entry>
    <title>New Entry Two</title>
    <link href="https://example.com/new2"/>
    <id>https://example.com/new2</id>
    <updated>2030-04-09T11:00:00Z</updated>
    <summary>Also published after the watermark</summary>
  </entry>
</feed>"""

# Watermark sits between the old entry and the two new ones.
_REQ342_WATERMARK = datetime(2030, 4, 9, 9, 0, 0, tzinfo=timezone.utc)


@given("an RSS source polling an Atom feed every 300 seconds")
def rss_source_polling_atom_feed(shared_data: dict) -> None:
    """Set up an RSSNotificationProvider configured with the default poll interval."""
    provider = RSSNotificationProvider(
        url="https://example.com/atom.xml",
        poll_interval=300,
        watermark=_REQ342_WATERMARK,
    )

    shared_data["provider"] = provider
    shared_data["atom_feed_bytes"] = _REQ342_ATOM_FEED
    shared_data["watermark"] = _REQ342_WATERMARK

    # Confirm the poll interval is the default 300 s (= 5 min).
    assert getattr(provider, "poll_interval", None) == 300
    # Confirm the watermark was accepted.
    assert getattr(provider, "watermark", None) == _REQ342_WATERMARK


@when("new items are published after the last-seen watermark")
def new_items_published_after_watermark(shared_data: dict) -> None:
    """Drive one poll cycle and collect only the events the provider emits."""
    provider: RSSNotificationProvider = shared_data["provider"]
    feed_bytes: bytes = shared_data["atom_feed_bytes"]
    watermark: datetime = shared_data["watermark"]

    # Parse the feed directly so we can reason about what *should* come out.
    # parse_feed now returns datetime in "published" (converted by _parse_date internally).
    all_items = parse_feed(feed_bytes)
    new_items = [item for item in all_items if item["published"] > watermark]
    shared_data["expected_new_items"] = new_items

    async def _fake_fetch(url: str) -> bytes:
        return feed_bytes

    async def _run() -> list[ChangeEvent]:
        with patch.object(provider, "_fetch", side_effect=_fake_fetch):
            return await provider.poll_once(table="feed_items")

    shared_data["events"] = asyncio.run(_run())


@then('only those items are emitted as ChangeEvents with operation="insert"')
def only_new_items_emitted_as_insert_events(shared_data: dict) -> None:
    """Assert that only post-watermark items were emitted, all as inserts."""
    events: list[ChangeEvent] = shared_data["events"]
    expected_new_items: list[dict] = shared_data["expected_new_items"]
    watermark: datetime = shared_data["watermark"]

    # Exactly the two new entries (not the old one) must be emitted.
    assert len(events) == len(expected_new_items), (
        f"Expected {len(expected_new_items)} events but got {len(events)}: "
        f"{[ev.row.get('title') for ev in events]}"
    )

    for ev in events:
        # Every emitted event must carry operation="insert" (REQ-342).
        assert ev.operation == "insert", f"Expected operation='insert' but got '{ev.operation}'"
        # Every emitted event must target the correct table.
        assert ev.table == "feed_items"
        # The row must be a dictionary derived from the feed item.
        assert isinstance(ev.row, dict)
        # The publication date stored in the row must be AFTER the watermark.
        pub = ev.row.get("published")
        assert pub is not None, "ChangeEvent row is missing 'published' field"
        assert pub > watermark, (
            f"Emitted item '{ev.row.get('title')}' has published={pub} "
            f"which is not after watermark={watermark}"
        )
        # Timestamps must be timezone-aware.
        assert isinstance(ev.timestamp, datetime)
        assert ev.timestamp.tzinfo is not None

    # The old entry must NOT appear among the emitted events.
    emitted_ids = {ev.row.get("id") for ev in events}
    assert "https://example.com/old" not in emitted_ids, (
        "Old entry (before watermark) was incorrectly emitted"
    )

    # Both new entries must be present.
    assert "https://example.com/new1" in emitted_ids
    assert "https://example.com/new2" in emitted_ids


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
    "both formats extract title, link, description/summary, published, and id; unparseable dates use datetime.min"
)
def assert_rss_atom_parity(shared_data: dict) -> None:
    rss_items = shared_data["rss_items"]
    atom_items = shared_data["atom_items"]

    required_fields = {"title", "link", "description", "published", "id"}

    # RSS 2.0 <item> parity.
    rss_first = rss_items[0]
    assert required_fields <= set(rss_first.keys()), (
        f"RSS item missing fields: {required_fields - set(rss_first.keys())}"
    )
    assert rss_first["title"] == "Item One", (
        f"Expected title='Item One', got '{rss_first['title']}'"
    )
    assert rss_first["link"] == "https://example.com/1", (
        f"Expected link='https://example.com/1', got '{rss_first['link']}'"
    )
    assert rss_first["description"] == "First item", (
        f"Expected description='First item', got '{rss_first['description']}'"
    )
    assert rss_first["id"] == "https://example.com/1", (
        f"Expected id='https://example.com/1', got '{rss_first['id']}'"
    )
    assert isinstance(rss_first["published"], datetime), (
        f"Expected published to be datetime, got {type(rss_first['published'])}"
    )
    assert rss_first["published"].year == 2030, (
        f"Expected published year=2030, got {rss_first['published'].year}"
    )
    assert rss_first["published"].tzinfo is not None, "RSS published date must be timezone-aware"

    # Atom <entry> parity — same field set, summary mapped to description.
    atom_first = atom_items[0]
    assert required_fields <= set(atom_first.keys()), (
        f"Atom entry missing fields: {required_fields - set(atom_first.keys())}"
    )
    assert atom_first["title"] == "Atom Entry One", (
        f"Expected title='Atom Entry One', got '{atom_first['title']}'"
    )
    assert atom_first["link"] == "https://example.com/a1", (
        f"Expected link='https://example.com/a1', got '{atom_first['link']}'"
    )
    assert atom_first["description"] == "First atom entry", (
        f"Expected description='First atom entry', got '{atom_first['description']}'"
    )
    assert atom_first["id"] == "https://example.com/a1", (
        f"Expected id='https://example.com/a1', got '{atom_first['id']}'"
    )
    assert isinstance(atom_first["published"], datetime), (
        f"Expected published to be datetime, got {type(atom_first['published'])}"
    )
    assert atom_first["published"].year == 2030, (
        f"Expected published year=2030, got {atom_first['published'].year}"
    )
    assert atom_first["published"].tzinfo is not None, "Atom published date must be timezone-aware"

    # Unparseable dates fall back to datetime.min (UTC sentinel) in both
    # formats rather than silently using the current time.
    sentinel = datetime.min.replace(tzinfo=timezone.utc)

    rss_bad = rss_items[1]
    assert required_fields <= set(rss_bad.keys()), (
        f"RSS bad-date item missing fields: {required_fields - set(rss_bad.keys())}"
    )
    assert rss_bad["published"] == sentinel, (
        f"Expected RSS bad-date item published={sentinel}, got {rss_bad['published']}"
    )

    atom_bad = atom_items[1]
    assert required_fields <= set(atom_bad.keys()), (
        f"Atom bad-date entry missing fields: {required_fields - set(atom_bad.keys())}"
    )
    assert atom_bad["published"] == sentinel, (
        f"Expected Atom bad-date entry published={sentinel}, got {atom_bad['published']}"
    )

    # The sentinel is also produced directly by the date parser for arbitrary
    # unparseable strings.
    assert _parse_date("not-a-real-date") == sentinel, (
        "_parse_date('not-a-real-date') must return datetime.min (UTC)"
    )
    assert _parse_date("garbage") == sentinel, (
        "_parse_date('garbage') must return datetime.min (UTC)"
    )

    # Valid RFC 2822 dates parse to real timezone-aware values distinct from
    # the sentinel.
    rfc = _parse_date("Thu, 09 Apr 2030 10:00:00 +0000")
    assert rfc != sentinel, "RFC 2822 date must not parse to datetime.min sentinel"
    assert rfc.tzinfo is not None, "RFC 2822 parsed date must be timezone-aware"
    assert rfc.year == 2030, f"Expected year=2030, got {rfc.year}"

    # Valid ISO 8601 dates parse to real timezone-aware values distinct from
    # the sentinel.
    iso = _parse_date("2030-04-09T10:00:00Z")
    assert iso != sentinel, "ISO 8601 date must not parse to datetime.min sentinel"
    assert iso.tzinfo is not None, "ISO 8601 parsed date must be timezone-aware"
    assert iso.year == 2030, f"Expected year=2030, got {iso.year}"

    # RFC 2822 and ISO 8601 representations of the same instant must agree.
    assert rfc == iso, (
        f"RFC 2822 and ISO 8601 representations of the same instant must be equal: "
        f"rfc={rfc}, iso={iso}"
    )
