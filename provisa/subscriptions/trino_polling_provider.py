# Copyright (c) 2026 Kenneth Stott
# Canary: 325af54d-1c63-4857-9619-fda383b1c553
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino-based watermark polling provider for cross-datasource views."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)


class TrinoPollingProvider(NotificationProvider):
    """Poll a Trino-accessible table/view using a watermark column.

    Creates its own dedicated Trino connection so polling does not contend
    with the shared query connection.  The watermark column must be present
    in the view at the SQL level but does not need to appear in the GraphQL
    schema.
    """

    def __init__(
        self,
        host: str,
        port: int,
        catalog: str,
        schema: str,
        table: str,
        watermark_column: str,
        poll_interval: float = 5.0,
        user: str = "provisa",
    ) -> None:
        self._host = host
        self._port = port
        self._catalog = catalog
        self._schema = schema
        self._table = table
        self._watermark_column = watermark_column
        self._poll_interval = poll_interval
        self._user = user
        self._running = True

    def _connect(self) -> Any:
        import trino
        return trino.dbapi.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            catalog=self._catalog,
            schema=self._schema,
            http_scheme="http",
            request_timeout=30,
        )

    def _fetch_new_rows(self, conn: Any, watermark: datetime) -> list[dict]:
        wc = self._watermark_column
        fqt = f'"{self._catalog}"."{self._schema}"."{self._table}"'
        ts = watermark.strftime("%Y-%m-%d %H:%M:%S.%f")
        sql = (
            f"SELECT * FROM {fqt} "  # noqa: S608
            f"WHERE {wc} > TIMESTAMP '{ts}' "
            f"ORDER BY {wc} "
            f"LIMIT 1000"
        )
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        loop = asyncio.get_running_loop()
        watermark = datetime.now(timezone.utc)
        conn = await loop.run_in_executor(None, self._connect)
        log.info(
            "TrinoPollingProvider: polling %s.%s.%s every %.1fs on %s",
            self._catalog, self._schema, self._table,
            self._poll_interval, self._watermark_column,
        )
        try:
            while self._running:
                await asyncio.sleep(self._poll_interval)
                try:
                    rows = await loop.run_in_executor(
                        None, self._fetch_new_rows, conn, watermark
                    )
                except Exception as exc:
                    log.warning("TrinoPollingProvider: query failed: %s", exc)
                    # Reconnect on next cycle
                    try:
                        conn = await loop.run_in_executor(None, self._connect)
                    except Exception:
                        pass
                    continue

                for row in rows:
                    ts_raw = row.get(self._watermark_column)
                    ts = ts_raw if isinstance(ts_raw, datetime) else datetime.now(timezone.utc)
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
