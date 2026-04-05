# Copyright (c) 2025 Kenneth Stott
# Canary: 3724ecd5-ebb4-45a7-9f79-9bb0fedfc777
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL LISTEN/NOTIFY subscription provider."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)

CHANNEL_PREFIX = "provisa_"


class PgNotificationProvider(NotificationProvider):
    """Wraps asyncpg LISTEN/NOTIFY into the NotificationProvider interface."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self._conn: Any | None = None

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        channel = f"{CHANNEL_PREFIX}{table}"
        queue: asyncio.Queue[str] = asyncio.Queue()

        def _on_notify(conn: object, pid: int, ch: str, payload: str) -> None:
            queue.put_nowait(payload)

        self._conn = await self._pool.acquire()
        try:
            await self._conn.add_listener(channel, _on_notify)
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
            try:
                await self._conn.remove_listener(channel, _on_notify)
            except Exception:
                log.debug("Failed to remove listener on %s", channel, exc_info=True)
            await self._pool.release(self._conn)
            self._conn = None

    async def close(self) -> None:
        self._conn = None
