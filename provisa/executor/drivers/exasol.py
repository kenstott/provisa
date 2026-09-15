# Copyright (c) 2026 Kenneth Stott
# Canary: 0a4e0039-f75d-4a11-adea-4ff8063f7d39
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Exasol direct source driver, via pyexasol (Exasol's own WebSocket client library).

Makes Exasol a first-class NAMED SOURCE reachable on ANY engine: Provisa reads it directly then
lands a replica, the same shape as the Snowflake/Databricks warehouse drivers — distinct from
Trino's own exasol JDBC connector (provisa/federation/trino_connectors.py), which reads it live and
never lands anything. The driver imports lazily so this module loads even where pyexasol is not
installed.
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class ExasolDriver(DirectDriver):
    def __init__(self) -> None:
        self._conn: Any = None
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """``tls_fingerprint`` (optional) from ``Source.federation_hints`` — the same channel
        ``Source.jdbc_url()`` (core/models.py) already reads to pin Trino's exasol JDBC connector
        to a self-signed cert. Exasol 8 always serves TLS with a certificate generated per
        container boot, so there is no CA to trust; pyexasol's own DSN syntax
        (``<host>:<port>/<FINGERPRINT>``, connection.py's ``_process_dsn``) is its documented way
        to pin one, mirroring exaplus's ``<host>/<FINGERPRINT>:<port>`` used elsewhere in this
        codebase's Exasol fixtures/tests. Without this override the base no-op left the fingerprint
        the UI form collects entirely unused — connect() always used the plain, unpinned DSN and
        failed PKIX validation against any self-signed Exasol server."""
        self._extra = dict(extra)

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,  # pyright: ignore[reportUnusedParameter]  # pyexasol has no pool
        max_pool: int = 5,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        import pyexasol  # pyright: ignore[reportMissingImports]

        fingerprint = self._extra.get("tls_fingerprint")
        dsn = f"{host}:{port or 8563}"
        if fingerprint:
            dsn = f"{dsn}/{fingerprint}"

        def _open() -> Any:
            return pyexasol.connect(
                dsn=dsn,
                user=user,
                password=password,
                schema=database or "",
            )

        self._conn = await asyncio.to_thread(_open)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:  # pyright: ignore[reportUnusedParameter]
        def _run() -> QueryResult:
            stmt = self._conn.execute(sql)
            cols = list(stmt.column_names())
            rows = stmt.fetchall()
            return QueryResult(rows=[tuple(r) for r in rows], column_names=cols)

        return await asyncio.to_thread(_run)

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None
