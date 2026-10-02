# Copyright (c) 2026 Kenneth Stott
# Canary: 2055ac74-8b41-4f0f-ada8-c8336c5d13b0
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""RSS/Atom polling subscription provider.

Polls an RSS 2.0 or Atom feed URL, watermarks by item publication date,
and yields one ChangeEvent per new item.
"""

from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET

from defusedxml.ElementTree import fromstring as _safe_fromstring
from defusedxml.ElementTree import iterparse as _safe_iterparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from collections.abc import Iterator
from typing import IO, Any, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)

# Requirements: REQ-342, REQ-343, REQ-344


def _strip_ns(tag: str) -> str:
    """Strip XML namespace prefix: {ns}local → local."""
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


# REQ-343: unparseable/empty dates sort oldest via a stable sentinel rather than
# now() (which would make a date-less item look freshly published on every poll).
_UNPARSEABLE_DATE = datetime.min.replace(tzinfo=timezone.utc)


def _parse_date(value: str | None) -> datetime:
    if not value:
        return _UNPARSEABLE_DATE
    value = value.strip()
    # ISO 8601 (Atom)
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    # RFC 2822 (RSS 2.0)
    try:
        return parsedate_to_datetime(value)
    except Exception:
        return _UNPARSEABLE_DATE


def _child_text(el: ET.Element, *tags: str) -> str | None:
    """Return text of first matching child tag (bare or namespaced)."""
    for tag in tags:
        child = el.find(tag)
        if child is not None and child.text:
            return child.text
        # Try Atom namespace
        child = el.find(f"{{http://www.w3.org/2005/Atom}}{tag}")
        if child is not None and child.text:
            return child.text
    return None


def _rss_item(item: ET.Element) -> dict:  # REQ-343
    return {
        "title": _child_text(item, "title"),
        "link": _child_text(item, "link"),
        "description": _child_text(item, "description"),
        "published": _parse_date(_child_text(item, "pubDate")),
        "id": _child_text(item, "guid") or _child_text(item, "link"),
    }


def _atom_entry(entry: ET.Element) -> dict:  # REQ-343
    link_el = entry.find("{http://www.w3.org/2005/Atom}link")
    if link_el is None:
        link_el = entry.find("link")
    link = link_el.get("href") if link_el is not None else None
    return {
        "title": _child_text(entry, "title"),
        "link": link,
        "description": _child_text(entry, "summary", "content"),
        "published": _parse_date(_child_text(entry, "updated", "published")),
        "id": _child_text(entry, "id") or link,
    }


def _parse_rss(root: ET.Element) -> list[dict]:  # REQ-343
    channel = root.find("channel")
    if channel is None:
        return []
    return [_rss_item(item) for item in channel.findall("item")]


def _parse_atom(root: ET.Element) -> list[dict]:  # REQ-343
    return [_atom_entry(child) for child in root if _strip_ns(child.tag) == "entry"]


def stored_row(item: dict) -> dict:
    """A feed item as the row a replica stores: unset fields left out, and ``published`` as a
    naive UTC timestamp (the column is inferred as a timestamp without a zone, REQ-1730)."""
    row = {k: v for k, v in item.items() if v is not None}
    row["published"] = item["published"].astimezone(timezone.utc).replace(tzinfo=None)
    return row


def iter_feed_items(body: IO[bytes]) -> Iterator[dict]:  # REQ-1915
    """The items of the RSS 2.0 or Atom feed in ``body``, parsed incrementally: one item's
    element is held at a time. The same parser policy as :func:`parse_feed` (no entities, no
    DTD). A document that is neither kind of feed has no items."""
    kind: str | None = None
    depth = 0
    for event, element in _safe_iterparse(body, events=("start", "end")):
        if event == "start":
            depth += 1
            if depth == 1:
                kind = _strip_ns(element.tag).lower()
                if kind not in ("rss", "feed"):
                    return
            continue
        depth -= 1
        local = _strip_ns(element.tag)
        if kind == "rss" and depth == 2 and local == "item":
            yield _rss_item(element)
            element.clear()
        elif kind == "feed" and depth == 1 and local == "entry":
            yield _atom_entry(element)
            element.clear()


def iter_rows_spooled(url: str, spooled: Any) -> Iterator[dict]:  # REQ-1915
    """Every current item of the feed at ``url`` as a stored row, the feed's body written to a
    spool file as it arrives and parsed from the file an item at a time."""
    import httpx

    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        with spooled(lambda: client.stream("GET", url)) as body:
            for item in iter_feed_items(body):
                yield stored_row(item)


def parse_feed(xml_bytes: bytes) -> list[dict]:  # REQ-343
    """Parse RSS 2.0 or Atom feed bytes into a list of item dicts."""
    root = _safe_fromstring(xml_bytes)
    tag = _strip_ns(root.tag).lower()
    if tag == "rss":
        return _parse_rss(root)
    if tag == "feed":
        return _parse_atom(root)
    return []


class RSSNotificationProvider(NotificationProvider):  # REQ-342, REQ-343, REQ-344
    """Polls an RSS 2.0 or Atom feed and yields ChangeEvents for new items.

    Args:
        url: Full HTTP(S) URL of the feed.
        poll_interval: Seconds between polls (default 300 = 5 min).
    """

    def __init__(
        self,
        url: str,
        poll_interval: float = 300.0,
        watermark: datetime | None = None,
    ) -> None:
        self._url = url
        self._poll_interval = poll_interval
        self._watermark = watermark
        self._running = True

    @property
    def poll_interval(self) -> float:
        return self._poll_interval

    @property
    def watermark(self) -> datetime | None:
        return self._watermark

    async def _fetch(self, url: str) -> bytes:
        """Fetch raw bytes from *url*. Patched in tests."""
        import httpx

        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content

    async def poll_once(self, table: str) -> list[ChangeEvent]:
        """Perform a single fetch-parse-filter cycle; return list of new ChangeEvents. A failed
        fetch or parse raises: it is never an empty feed (REQ-1661, amended 2026-09-30 -- the land
        path read it as "zero items" and served the query nothing or the stale replica)."""
        watermark = self._watermark or datetime.min.replace(tzinfo=timezone.utc)
        raw = await self._fetch(self._url)
        items = parse_feed(raw)

        events: list[ChangeEvent] = []
        for item in items:
            pub: datetime = item["published"]  # already datetime from parse_feed
            if pub <= watermark:
                continue
            # The stored copy: unset fields left out, ``published`` naive UTC (see stored_row).
            # The watermark comparison above needs the zone-aware ``pub``, kept as it is.
            row = stored_row(item)
            events.append(
                ChangeEvent(
                    operation="insert",
                    table=table,
                    row=row,
                    timestamp=pub,
                )
            )
        return events

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        import httpx

        watermark = self._watermark or datetime.now(timezone.utc)
        log.info("RSSProvider: polling %s every %.0fs", self._url, self._poll_interval)

        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            while self._running:
                try:
                    resp = await client.get(self._url)
                    resp.raise_for_status()
                    items = parse_feed(resp.content)
                except Exception as exc:
                    log.warning("RSSProvider: fetch/parse error (%s)", exc)
                    await asyncio.sleep(self._poll_interval)
                    continue

                new_watermark = watermark
                for item in items:
                    pub: datetime = item["published"]  # already datetime from parse_feed
                    if pub <= watermark:
                        continue
                    if pub > new_watermark:
                        new_watermark = pub
                        self._watermark = new_watermark
                    row = {k: v for k, v in item.items() if v is not None}
                    row["published"] = pub
                    yield ChangeEvent(
                        operation="insert",
                        table=table,
                        row=row,
                        timestamp=pub,
                    )

                watermark = new_watermark
                self._watermark = new_watermark
                await asyncio.sleep(self._poll_interval)

    async def close(self) -> None:
        self._running = False
