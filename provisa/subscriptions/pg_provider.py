# Copyright (c) 2026 Kenneth Stott
# Canary: 3724ecd5-ebb4-45a7-9f79-9bb0fedfc777
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL LISTEN/NOTIFY subscription provider."""

# Requirements: REQ-258

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

CHANNEL_PREFIX = "provisa_"


def table_channel(table: str) -> str:
    """The LISTEN/NOTIFY channel a registered PostgreSQL table's notify trigger reports on."""
    return f"{CHANNEL_PREFIX}{table}"


def schema_channel(table: str, schema: str) -> str:  # REQ-2216
    """The channel an ops table kept in each org schema reports on: one per schema, so one org's
    changes never reach another org's subscribers. NOTIFY is database-wide and the org schemas
    share a database. The schema enters as a digest, the rule schema.sql's trigger applies
    (``substr(md5(TG_TABLE_SCHEMA), 1, 12)``), which keeps the name inside PostgreSQL's limit."""
    import hashlib

    return f"{CHANNEL_PREFIX}{table}_{hashlib.md5(schema.encode(), usedforsecurity=False).hexdigest()[:12]}"


class PgNotificationProvider(NotificationProvider):  # REQ-258
    """Wraps PostgreSQL LISTEN/NOTIFY into the NotificationProvider interface.

    ``pool`` is the control-plane ``Database``; its listener thread holds the LISTEN connection
    and delivers each payload to this provider's queue on the watching loop, so a watch holds no
    pooled connection."""

    def __init__(self, pool: "Database") -> None:
        self._pool = pool

    async def watch(  # REQ-565
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        channel = f"{CHANNEL_PREFIX}{table}"
        queue: asyncio.Queue[str] = asyncio.Queue()

        def _on_notify(_db: object, pid: int, ch: str, payload: str) -> None:
            queue.put_nowait(payload)

        await self._pool.add_listener(channel, _on_notify)
        try:
            log.info("PgProvider: listening on %s", channel)

            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    parsed = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    log.warning("PgProvider: invalid JSON payload: %s", payload)
                    continue

                op = parsed.get("op", "unknown").lower()
                row = parsed.get("row", {})
                yield ChangeEvent(
                    operation=op,
                    table=table,
                    row=row,
                    timestamp=datetime.now(timezone.utc),
                )
        finally:
            await self._pool.remove_listener(channel, _on_notify)

    async def watch_many(self, tables: list[str]) -> AsyncGenerator[ChangeEvent, None]:  # REQ-565
        """Listen on multiple table channels; any change event triggers a yield."""
        channels = [f"{CHANNEL_PREFIX}{t}" for t in tables]
        queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

        def _on_notify(_db: object, pid: int, ch: str, payload: str) -> None:
            queue.put_nowait((ch, payload))

        subscribed: list[str] = []
        try:
            for ch in channels:
                await self._pool.add_listener(ch, _on_notify)
                subscribed.append(ch)
            log.info("PgProvider: listening on %s", channels)

            while True:
                try:
                    ch, payload = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    parsed = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    log.warning("PgProvider: invalid JSON payload: %s", payload)
                    continue

                table = ch[len(CHANNEL_PREFIX) :]
                op = parsed.get("op", "unknown").lower()
                row = parsed.get("row", {})
                yield ChangeEvent(
                    operation=op,
                    table=table,
                    row=row,
                    timestamp=datetime.now(timezone.utc),
                )
        finally:
            for ch in subscribed:
                await self._pool.remove_listener(ch, _on_notify)

    async def close(self) -> None:
        # Each watch removes its own listeners when it ends; nothing is held between watches.
        return None
