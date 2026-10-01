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

from typing import Any

from provisa.executor.drivers.pooled import SingleStatementConnectionDriver, run_dbapi
from provisa.executor.result import QueryResult


class DatabricksDriver(SingleStatementConnectionDriver):  # REQ-987
    """databricks-sql-connector is DB-API ``threadsafety = 1``: a connection per statement, from
    the pool (see ``pooled``)."""

    def __init__(self) -> None:
        self._http_path: str | None = None

    def configure(self, extra: dict[str, str]) -> None:
        """``http_path`` is required (from the source's federation_hints); ``catalog`` is optional."""
        self._http_path = extra.get("http_path")

    # Async only for the DirectDriver awaitable contract; connects synchronously in-thread.
    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,  # pyright: ignore[reportUnusedParameter]  (catalog carried in the SQL/hints)
        user: str,  # pyright: ignore[reportUnusedParameter]
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
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

        self._open_pool(
            _open, min_pool=min_pool, max_pool=max_pool, name=f"databricks:{host}{self._http_path}"
        )

    def _run(self, conn: Any, sql: str, params: list | None) -> QueryResult:
        return run_dbapi(conn, sql, params, lambda cur: cur.cancel)

    def _is_broken(self, exc: BaseException) -> bool:
        # RequestError and its kin (the HTTP transport, a closed session) are OperationalError;
        # a statement the warehouse rejected is ServerOperationError, which is not.
        from databricks.sql.exc import InterfaceError, OperationalError

        return isinstance(exc, (OperationalError, InterfaceError))
