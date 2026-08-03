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

# Requirements: REQ-260, REQ-283, REQ-336

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from datetime import datetime, timezone

from typing import Protocol, runtime_checkable

from provisa.core.database import Database
from provisa.executor.pool import SourcePool
from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)


@runtime_checkable
class PollingRowSource(Protocol):
    """Fetches rows for a watermark-polling query. Two implementations exist, one per
    kind of connection a polled table's data can actually live behind (REQ-052)."""

    async def fetch_rows(self, sql: str, params: list) -> list[dict]: ...


class DatabaseRowSource:
    """Polls through the control-plane ``Database`` — valid only when the table's data
    genuinely lives in the org's own PostgreSQL (the ``postgresql`` source type)."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def fetch_rows(self, sql: str, params: list) -> list[dict]:
        async with self._database.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]


class SourcePoolRowSource:
    """Polls a table's own registered source via ``SourcePool`` (REQ-052) — the control
    plane holds domain/table metadata, not business data, for any source that isn't
    itself the org's self-hosted PostgreSQL."""

    def __init__(self, source_pool: SourcePool, source_id: str) -> None:
        self._source_pool = source_pool
        self._source_id = source_id

    async def fetch_rows(self, sql: str, params: list) -> list[dict]:
        result = await self._source_pool.execute(self._source_id, sql, params)
        return [dict(zip(result.column_names, row)) for row in result.rows]


class PollingNotificationProvider(NotificationProvider):  # REQ-260, REQ-283, REQ-336
    """Polls a table for changes using a configurable watermark column (default: ``updated_at``)."""

    def __init__(
        self,
        row_source: PollingRowSource,
        poll_interval: float = 5.0,
        watermark_column: str = "updated_at",
    ) -> None:
        self._row_source = row_source
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
            wc = self._watermark_column
            rows = await self._row_source.fetch_rows(
                f"SELECT * FROM {table} "  # noqa: S608  # table/wc are validated identifiers, not user values; watermark is parameterized
                f"WHERE {wc} > $1 ORDER BY {wc} LIMIT 100",
                [watermark],
            )

            for row_dict in rows:
                ts = row_dict.get(wc, datetime.now(timezone.utc))
                if isinstance(ts, datetime) and ts > watermark:
                    watermark = ts

                yield ChangeEvent(
                    operation="update",
                    table=table,
                    row=row_dict,
                    timestamp=ts if isinstance(ts, datetime) else datetime.now(timezone.utc),
                )

            await asyncio.sleep(self._poll_interval)

    async def close(self) -> None:
        self._running = False


class NullNotificationProvider(NotificationProvider):
    """REQ-260: without an explicit ``watermark_column``, poll subscriptions are unavailable
    for that source. The SSE stream still opens (REQ-219) but has nothing to deliver — this
    provider's ``watch`` yields no events rather than guessing a watermark column that may
    not exist on the table."""

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        return
        yield  # pragma: no cover  # noqa: B027  # makes this an async generator

    async def close(self) -> None:
        pass
