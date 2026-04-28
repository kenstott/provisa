# Copyright (c) 2026 Kenneth Stott
# Canary: 325af54d-1c63-4857-9619-fda383b1c553
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Timestamp-watermark polling subscription provider."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)

class PollingNotificationProvider(NotificationProvider):
    """Polls a table for changes using a configurable watermark column (default: ``updated_at``)."""

    def __init__(
        self,
        pool: Any,
        poll_interval: float = 5.0,
        watermark_column: str = "updated_at",
    ) -> None:
        self._pool = pool
        self._poll_interval = poll_interval
        self._watermark_column = watermark_column
        self._running = True

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        watermark = datetime.now(timezone.utc)
        log.info(
            "PollingProvider: polling %s every %.1fs (watermark=%s)",
            table,
            self._poll_interval,
            self._watermark_column,
        )

        while self._running:
            conn = await self._pool.acquire()
            try:
                wc = self._watermark_column
                rows = await conn.fetch(
                    f"SELECT * FROM {table} "  # noqa: S608
                    f"WHERE {wc} > $1 ORDER BY {wc} LIMIT 100",
                    watermark,
                )

                for row in rows:
                    row_dict = dict(row)
                    ts = row_dict.get(wc, datetime.now(timezone.utc))
                    if isinstance(ts, datetime) and ts > watermark:
                        watermark = ts

                    yield ChangeEvent(
                        operation="update",
                        table=table,
                        row=row_dict,
                        timestamp=ts if isinstance(ts, datetime) else datetime.now(timezone.utc),
                    )
            finally:
                await self._pool.release(conn)

            await asyncio.sleep(self._poll_interval)

    async def close(self) -> None:
        self._running = False
