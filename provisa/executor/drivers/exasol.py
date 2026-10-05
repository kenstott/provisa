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

from typing import Any

from provisa.core import request_deadline
from provisa.executor.drivers.pooled import SingleStatementConnectionDriver
from provisa.executor.result import QueryResult


class ExasolDriver(SingleStatementConnectionDriver):
    """A pyexasol connection is one websocket carrying one request at a time: a connection per
    statement, from the pool (see ``pooled``)."""

    def __init__(self) -> None:
        self._extra: dict[str, str] = {}

    def configure(self, extra: dict[str, str]) -> None:
        """``tls_fingerprint`` (optional) from ``Source.federation_hints`` — the same channel
        ``Source.jdbc_url()`` (core/models.py) already reads to pin Trino's exasol JDBC connector
        to a self-signed cert. Exasol 8 always serves TLS with a certificate generated per
        container boot, so there is no CA to trust; pyexasol's own DSN syntax
        (``<host>/<FINGERPRINT>:<port>``, connection.py's ``_process_dsn``: the fingerprint
        follows the host and the port comes last, as exaplus writes it) is its documented way to
        pin one. Without this override the base no-op left the fingerprint
        the UI form collects entirely unused — connect() always used the plain, unpinned DSN and
        failed PKIX validation against any self-signed Exasol server."""
        self._extra = dict(extra)

    # Async only for the DirectDriver awaitable contract; connects synchronously in-thread.
    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        import pyexasol  # pyright: ignore[reportMissingImports]

        fingerprint = self._extra.get("tls_fingerprint")
        pinned = f"{host}/{fingerprint}" if fingerprint else host
        dsn = f"{pinned}:{port or 8563}"

        def _open() -> Any:
            return pyexasol.connect(
                dsn=dsn,
                user=user,
                password=password,
                schema=database or "",
            )

        self._open_pool(_open, min_pool=min_pool, max_pool=max_pool, name=f"exasol:{dsn}")

    def _run(self, conn: Any, sql: str, params: list | None) -> QueryResult:  # pyright: ignore[reportUnusedParameter]
        # abort_query is pyexasol's own cross-thread cancel: it opens a second websocket and
        # aborts the request running on this connection, which stays usable.
        with request_deadline.cancel_on_deadline(conn.abort_query):
            stmt = conn.execute(sql)
            # pyexasol names what a statement answered: "resultSet" (a query) or "rowCount" (DDL,
            # DML). Fetching from a rowCount statement raises "Attempt to fetch from statement
            # without result set", so a CREATE/INSERT failed after it had already run.
            if stmt.result_type != "resultSet":
                return QueryResult(rows=[], column_names=[], rowcount=stmt.rowcount())
            cols = list(stmt.column_names())
            rows = stmt.fetchall()
        return QueryResult(rows=[tuple(r) for r in rows], column_names=cols)

    def _is_broken(self, exc: BaseException) -> bool:
        import pyexasol  # pyright: ignore[reportMissingImports]

        return isinstance(exc, (pyexasol.ExaCommunicationError, pyexasol.ExaConnectionError))
