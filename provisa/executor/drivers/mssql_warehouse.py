# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Microsoft Fabric Warehouse / Azure Synapse direct source driver (T-SQL over TDS/ODBC, Azure AD).

Makes Fabric/Synapse first-class NAMED SOURCES reachable on ANY engine: Provisa reads the warehouse
directly then lands a replica. The connection is PER-SOURCE — the server (``host``), database, and the
Azure AD identity all come from the source config, so two Fabric sources from DIFFERENT ACCOUNTS
coexist without any shared env. A source pins its own identity via ``federation_hints`` service-
principal fields (``tenant_id`` / ``client_id`` / ``client_secret``); absent, it falls back to the
ambient credential (``az login`` / managed identity via DefaultAzureCredential).

The ODBC driver is the Microsoft ``ODBC Driver 18 for SQL Server`` (name, or a full dylib path via
``$PROVISA_MSSQL_ODBC_DRIVER`` on nonstandard installs). pyodbc/azure-identity imported lazily.
"""

from __future__ import annotations

import asyncio
import os
import struct
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

_SQL_COPT_SS_ACCESS_TOKEN = 1256
_AAD_SCOPE = "https://database.windows.net/.default"


class MssqlWarehouseDriver(DirectDriver):
    def __init__(self) -> None:
        self._conn: Any = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """Per-source Azure AD identity: a service principal (``tenant_id``/``client_id``/
        ``client_secret``) for THIS source's account; absent → the ambient credential."""
        self._extra = dict(extra)

    def _token(self) -> str:
        e = self._extra
        if e.get("tenant_id") and e.get("client_id") and e.get("client_secret"):
            from azure.identity import ClientSecretCredential

            cred: Any = ClientSecretCredential(e["tenant_id"], e["client_id"], e["client_secret"])
        else:
            from azure.identity import DefaultAzureCredential

            cred = DefaultAzureCredential()
        return cred.get_token(_AAD_SCOPE).token

    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,
        user: str,  # pyright: ignore[reportUnusedParameter]
        password: str,  # pyright: ignore[reportUnusedParameter]
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        if not host or not database:
            raise ValueError("fabric/synapse source requires a server host and database")

        def _open() -> Any:
            import pyodbc

            raw = self._token().encode("utf-16-le")
            token_struct = struct.pack(f"<I{len(raw)}s", len(raw), raw)
            driver = os.environ.get("PROVISA_MSSQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
            driver_clause = driver if driver.startswith("/") else f"{{{driver}}}"
            conn_str = f"DRIVER={driver_clause};SERVER={host};DATABASE={database};Encrypt=yes;"
            # connect(timeout=) is the LOGIN timeout; a serverless Synapse/Fabric pool can be slow
            # to resume from auto-pause, so honor PROVISA_MSSQL_LOGIN_TIMEOUT (seconds).
            login_timeout = int(os.environ.get("PROVISA_MSSQL_LOGIN_TIMEOUT", "120"))
            return pyodbc.connect(
                conn_str,
                attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                timeout=login_timeout,
            )

        self._conn = await asyncio.to_thread(_open)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        del params  # T-SQL read arrives fully formed from the governed pipeline

        def _run() -> QueryResult:
            cur = self._conn.cursor()
            try:
                cur.execute(sql)
                cols = [c[0] for c in cur.description] if cur.description else []
                rows = [tuple(r) for r in cur.fetchall()] if cur.description else []
                return QueryResult(rows=rows, column_names=cols)
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
