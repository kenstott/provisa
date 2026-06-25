# Copyright (c) 2026 Kenneth Stott
# Canary: a3f8b291-7c44-4e19-b2c0-3e8f61d9aa7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GovData watermark polling provider via Calcite JDBC adapter (JPype)."""

# Requirements: REQ-260, REQ-283, REQ-285

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import AsyncGenerator, Protocol, runtime_checkable

from provisa.subscriptions.base import ChangeEvent, NotificationProvider


@runtime_checkable
class _JdbcResultSet(Protocol):
    """Structural interface for java.sql.ResultSet used in _fetch_new_rows."""

    def next(self) -> bool: ...
    def getObject(
        self, column_index: int
    ) -> object: ...  # object-ok: JPype returns opaque Java objects; narrowed via str(val) below
    def getMetaData(self) -> _JdbcResultSetMetaData: ...


@runtime_checkable
class _JdbcResultSetMetaData(Protocol):
    """Structural interface for java.sql.ResultSetMetaData."""

    def getColumnCount(self) -> int: ...
    def getColumnName(self, column: int) -> str: ...


@runtime_checkable
class _JdbcStatement(Protocol):
    """Structural interface for java.sql.Statement used in _fetch_new_rows."""

    def executeQuery(self, sql: str) -> _JdbcResultSet: ...
    def close(self) -> None: ...


@runtime_checkable
class _JdbcConnection(Protocol):
    """Structural interface for java.sql.Connection returned by askamerica.engine.get_connection."""

    def createStatement(self) -> _JdbcStatement: ...
    def close(self) -> None: ...


log = logging.getLogger(__name__)


class GovDataPollingProvider(NotificationProvider):  # REQ-260, REQ-283, REQ-285
    """Poll a GovData table using a watermark column.

    GovData has no native change feed so polling is the only option.
    askamerica.engine manages the JVM singleton internally.
    """

    def __init__(
        self,
        sources: str,
        table: str,
        watermark_column: str,
        api_key: str,
        schema: str = "",
        poll_interval: float = 30.0,
    ) -> None:
        self._sources = sources  # e.g. "sec,geo"
        self._table = table
        self._watermark_column = watermark_column
        self._api_key = api_key
        self._schema = schema.upper() if schema else sources.split(",")[0].upper().strip()
        self._poll_interval = poll_interval
        self._running = True

    def _connect(self) -> _JdbcConnection:
        from askamerica.engine import get_connection  # type: ignore[import-untyped]  # askamerica ships no stubs; Protocol narrows usage

        os.environ["ASKAMERICA_SCHEMAS"] = self._sources
        return get_connection(self._api_key)  # type: ignore[no-any-return]  # untyped third-party; Protocol narrowed above

    def _fetch_new_rows(self, conn: _JdbcConnection, watermark: datetime) -> list[dict]:
        wc = self._watermark_column
        schema = self._schema
        table = self._table
        ts = watermark.strftime("%Y-%m-%d %H:%M:%S")
        sql = (
            f'SELECT * FROM "{schema}"."{table}" '  # noqa: S608
            f"WHERE {wc} > TIMESTAMP '{ts}' "
            f"ORDER BY {wc} "
            f"FETCH FIRST 1000 ROWS ONLY"
        )
        stmt = conn.createStatement()
        rs = stmt.executeQuery(sql)
        meta = rs.getMetaData()
        col_count = int(meta.getColumnCount())
        cols = [str(meta.getColumnName(i + 1)) for i in range(col_count)]
        rows = []
        while rs.next():
            row = {}
            for i, col in enumerate(cols):
                val = rs.getObject(i + 1)
                row[col] = str(val) if val is not None else None
            rows.append(row)
        return rows

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        loop = asyncio.get_running_loop()
        watermark = datetime.now(timezone.utc)
        conn = await loop.run_in_executor(None, self._connect)
        log.info(
            "GovDataPollingProvider: polling %s.%s every %.1fs on %s",
            self._schema,
            self._table,
            self._poll_interval,
            self._watermark_column,
        )
        try:
            while self._running:
                await asyncio.sleep(self._poll_interval)
                try:
                    rows = await loop.run_in_executor(None, self._fetch_new_rows, conn, watermark)
                except Exception as exc:
                    log.warning("GovDataPollingProvider: query failed: %s", exc)
                    try:
                        conn = await loop.run_in_executor(None, self._connect)
                    except Exception:
                        pass
                    continue

                for row in rows:
                    ts = datetime.now(timezone.utc)
                    if ts > watermark:
                        watermark = ts
                    yield ChangeEvent(
                        operation="update",
                        table=self._table,
                        row=row,
                        timestamp=ts,
                    )
        finally:
            try:
                conn.close()
            except Exception:
                pass

    async def close(self) -> None:
        self._running = False
