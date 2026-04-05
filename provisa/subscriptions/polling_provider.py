# Copyright (c) 2025 Kenneth Stott
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
from typing import Any, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)

_SOFT_DELETE_COLUMNS = ("deleted_at", "is_deleted")


class PollingNotificationProvider(NotificationProvider):
    """Polls a table for changes using an ``updated_at`` watermark column.

    Configurable poll interval (default 5s). Detects soft deletes when
    ``deleted_at`` or ``is_deleted`` column exists.
    """

    def __init__(
        self,
        pool: Any,
        poll_interval: float = 5.0,
        soft_delete_column: str | None = None,
    ) -> None:
        self._pool = pool
        self._poll_interval = poll_interval
        self._soft_delete_column = soft_delete_column
        self._running = True

        if soft_delete_column is None:
            log.warning(
                "PollingProvider: no soft-delete column configured; "
                "delete events will not be detected"
            )

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        watermark = datetime.now(timezone.utc)
        log.info(
            "PollingProvider: polling %s every %.1fs (soft_delete=%s)",
            table,
            self._poll_interval,
            self._soft_delete_column,
        )

        while self._running:
            conn = await self._pool.acquire()
            try:
                rows = await conn.fetch(
                    f"SELECT * FROM {table} "  # noqa: S608
                    f"WHERE updated_at > $1 ORDER BY updated_at LIMIT 100",
                    watermark,
                )

                for row in rows:
                    row_dict = dict(row)
                    ts = row_dict.get("updated_at", datetime.now(timezone.utc))
                    if isinstance(ts, datetime) and ts > watermark:
                        watermark = ts

                    op = self._detect_operation(row_dict)
                    yield ChangeEvent(
                        operation=op,
                        table=table,
                        row=row_dict,
                        timestamp=ts if isinstance(ts, datetime) else datetime.now(timezone.utc),
                    )
            finally:
                await self._pool.release(conn)

            await asyncio.sleep(self._poll_interval)

    def _detect_operation(self, row: dict[str, Any]) -> str:
        """Detect if this row represents a soft delete."""
        if self._soft_delete_column:
            val = row.get(self._soft_delete_column)
            if val is not None and val is not False and val != 0:
                return "delete"
        return "update"

    async def close(self) -> None:
        self._running = False
