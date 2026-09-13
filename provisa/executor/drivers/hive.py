# Copyright (c) 2026 Kenneth Stott
# Canary: c40fa565-7a04-4f0d-a950-75efb23c289f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hive direct source driver (HiveServer2 over Thrift, via impyla).

Makes a Hive cluster's HiveServer2 endpoint a first-class NAMED SOURCE reachable on ANY engine —
distinct from ``hive``/``hive_s3`` as Trino-scanned lake storage (provisa/transpiler/router.py's
``_FILE_SOURCES``), which reads the warehouse files directly and never goes through HiveServer2 at
all. This driver is for the case where HiveServer2 itself is the only thing reachable (no direct
filesystem/S3 access to the warehouse, or a managed Hive deployment that only exposes HS2), and
Provisa needs to read it the same way any other RDB-shaped source is read-then-landed. The driver
imports lazily so this module loads even where impyla is not installed.
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class HiveDriver(DirectDriver):
    def __init__(self) -> None:
        self._conn: Any = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """``auth_mechanism`` (default ``PLAIN``, overridable to e.g. ``GSSAPI`` for Kerberos) from
        ``Source.federation_hints``. Stock HiveServer2's default auth (``hive.server2.authentication
        =NONE``) speaks SASL PLAIN and accepts any username/password, including empty — verified
        live against apache/hive:4.0.0's HiveServer2 service; ``NOSASL`` (impyla's Impala-oriented
        default) skips the SASL handshake entirely and a stock HS2 drops the connection immediately
        (``TSocket read 0 bytes``) rather than falling back, so PLAIN must be the default here even
        with no password, not just when one is set."""
        self._extra = dict(extra)

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]  # impyla has no pool
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        from impala.dbapi import connect as hs2_connect  # pyright: ignore[reportMissingImports]

        auth_mechanism = self._extra.get("auth_mechanism") or "PLAIN"

        def _open() -> Any:
            return hs2_connect(
                host=host,
                port=port or 10000,
                database=database or None,
                user=user or None,
                password=password or None,
                auth_mechanism=auth_mechanism,
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
