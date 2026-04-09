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
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)


def _strip_ns(tag: str) -> str:
    """Strip XML namespace prefix: {ns}local → local."""
    return tag.split("}", 1)[1] if tag.startswith("{") else tag


def _parse_date(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
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
        return datetime.now(timezone.utc)


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


def _parse_rss(root: ET.Element) -> list[dict]:
    channel = root.find("channel")
    if channel is None:
        return []
    items = []
    for item in channel.findall("item"):
        items.append({
            "title": _child_text(item, "title"),
            "link": _child_text(item, "link"),
            "description": _child_text(item, "description"),
            "published": _child_text(item, "pubDate"),
            "id": _child_text(item, "guid") or _child_text(item, "link"),
        })
    return items


def _parse_atom(root: ET.Element) -> list[dict]:
    items = []
    for child in root:
        if _strip_ns(child.tag) != "entry":
            continue
        link_el = child.find("{http://www.w3.org/2005/Atom}link")
        if link_el is None:
            link_el = child.find("link")
        link = link_el.get("href") if link_el is not None else None
        items.append({
            "title": _child_text(child, "title"),
            "link": link,
            "description": _child_text(child, "summary", "content"),
            "published": _child_text(child, "updated", "published"),
            "id": _child_text(child, "id") or link,
        })
    return items


def parse_feed(xml_bytes: bytes) -> list[dict]:
    """Parse RSS 2.0 or Atom feed bytes into a list of item dicts."""
    root = ET.fromstring(xml_bytes)
    tag = _strip_ns(root.tag).lower()
    if tag == "rss":
        return _parse_rss(root)
    if tag == "feed":
        return _parse_atom(root)
    return []


class RSSNotificationProvider(NotificationProvider):
    """Polls an RSS 2.0 or Atom feed and yields ChangeEvents for new items.

    Args:
        url: Full HTTP(S) URL of the feed.
        poll_interval: Seconds between polls (default 300 = 5 min).
    """

    def __init__(self, url: str, poll_interval: float = 300.0) -> None:
        self._url = url
        self._poll_interval = poll_interval
        self._running = True

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        import httpx

        watermark = datetime.now(timezone.utc)
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
                    pub = _parse_date(item.get("published"))
                    if pub <= watermark:
                        continue
                    if pub > new_watermark:
                        new_watermark = pub
                    yield ChangeEvent(
                        operation="insert",
                        table=table,
                        row={k: v for k, v in item.items() if v is not None},
                        timestamp=pub,
                    )

                watermark = new_watermark
                await asyncio.sleep(self._poll_interval)

    async def close(self) -> None:
        self._running = False
