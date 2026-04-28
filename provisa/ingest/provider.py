# Copyright (c) 2026 Kenneth Stott
# Canary: 84ffe4c9-0e87-468d-8faa-7ea28e695c28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Watermark-polling subscription provider for ingest backing tables (Phase AS, REQ-336)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)

_BATCH_LIMIT = 100
_DEFAULT_POLL_INTERVAL = 5.0


class IngestPollingProvider(NotificationProvider):
    """Polls ``_updated_at`` on the ingest backing table via SQLAlchemy.

    Provisa owns the write path for ingest tables and guarantees that
    ``_updated_at`` is set to NOW() on every INSERT, making it a reliable
    watermark without needing database-level LISTEN/NOTIFY support.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
    ) -> None:
        self._engine = engine
        self._poll_interval = poll_interval

    async def watch(self, table: str) -> AsyncGenerator[ChangeEvent, None]:
        watermark: datetime = datetime.now(tz=timezone.utc)

        while True:
            await asyncio.sleep(self._poll_interval)
            try:
                rows = await self._poll(table, watermark)
            except Exception:
                log.warning("Ingest poll error on table=%s", table, exc_info=True)
                continue

            for row in rows:
                ts = row.get("_updated_at")
                if ts and isinstance(ts, datetime) and ts > watermark:
                    watermark = ts
                row_data = {k: v for k, v in row.items() if not k.startswith("_")}
                yield ChangeEvent(operation="INSERT", row=row_data)

    async def _poll(self, table: str, since: datetime) -> list[dict]:
        stmt = text(
            f"SELECT * FROM {table} WHERE _updated_at > :since ORDER BY _updated_at LIMIT :lim"  # noqa: S608
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(stmt, {"since": since, "lim": _BATCH_LIMIT})
            return [dict(r._mapping) for r in result]

    async def close(self) -> None:
        # Engine lifetime is managed by ingest/engine.py; do not dispose here.
        pass
