# Copyright (c) 2026 Kenneth Stott
# Canary: ccea9507-c866-4bf5-ba57-51d06bc0fa2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Databricks direct source driver (REQ-987).

Makes a Databricks SQL warehouse a first-class NAMED SOURCE reachable on ANY engine: Provisa reads
it directly (this driver) then lands a replica into the engine's store. It is the same
databricks-sql-connector connection the Databricks federation engine uses — the engine capability IS
the source capability. ``http_path`` (which the standard host/port/user/password args can't carry)
comes from ``Source.federation_hints`` via ``configure``. Reads deliver Arrow natively (Cloud Fetch).
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class DatabricksDriver(DirectDriver):  # REQ-987
    def __init__(self) -> None:
        self._conn: Any = None
        self._http_path: str | None = None

    def configure(self, extra: dict[str, str]) -> None:
        """``http_path`` is required (from the source's federation_hints); ``catalog`` is optional."""
        self._http_path = extra.get("http_path")

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,  # pyright: ignore[reportUnusedParameter]  (catalog carried in the SQL/hints)
        user: str,  # pyright: ignore[reportUnusedParameter]
        password: str,
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        if not self._http_path:
            raise ValueError(
                "databricks source requires 'http_path' in federation_hints "
                "(the SQL Warehouse connection detail)"
            )
        from databricks import sql as dbsql

        from provisa.federation.databricks_tls import databricks_tls_kwargs

        def _open() -> Any:
            return dbsql.connect(
                server_hostname=host,
                http_path=self._http_path,
                access_token=password,
                **databricks_tls_kwargs(),
            )

        self._conn = await asyncio.to_thread(_open)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        def _run() -> QueryResult:
            cur = self._conn.cursor()
            try:
                cur.execute(sql, params or None)
                cols = [d[0] for d in cur.description] if cur.description else []
                rows = cur.fetchall() if cur.description else []
                return QueryResult(rows=[tuple(r) for r in rows], column_names=cols)
            finally:
                cur.close()

        return await asyncio.to_thread(_run)

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None
