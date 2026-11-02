# Copyright (c) 2026 Kenneth Stott
# Canary: d7ee969b-5094-40ab-bc32-f4ccbb050532
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Generic SQL source's driver (REQ-2191): the SQLAlchemy driver, connected at the source's own
URL (``provisa.generic_sql.url``) rather than at a URL built from a type's drivername, and written
to in the SQL dialect the source chose. A ``jdbc:`` URL is read instead through the customer's
JDBC driver in Provisa's JVM (``provisa.generic_sql.jdbc``), behind this same driver.

Its rows are Python tuples: SQLAlchemy hands back DBAPI rows, JPype's DB-API Java values converted,
and no driver behind an arbitrary URL can be counted on for Arrow (docs/arch/fetch-paths.md)."""

# Requirements: REQ-2191

from __future__ import annotations

import asyncio
import threading
from typing import Any

from provisa.executor.drivers.base import DirectResultStream
from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver
from provisa.executor.result import QueryResult
from provisa.generic_sql.settings import GenericSqlSettings, parse


class GenericSQLDriver(SQLAlchemyDriver):  # REQ-2191
    """A SQLAlchemy driver at the URL the source's settings name, or a JDBC one for a jdbc: URL."""

    names_own_destination = True

    def __init__(self) -> None:
        super().__init__("")
        self._settings: GenericSqlSettings | None = None
        self._hints: dict[str, str] = {}
        # A jdbc: URL's one connection (jpype.dbapi2), used by one statement at a time.
        self._jdbc: Any = None
        self._jdbc_lock = threading.Lock()

    def configure(self, extra: dict[str, str]) -> None:
        self._settings = parse(extra)
        self._hints = dict(extra)

    @property
    def settings(self) -> GenericSqlSettings:
        if self._settings is None:
            raise RuntimeError("a Generic SQL driver is connected only after it is configured")
        return self._settings

    @property
    def dialect(self) -> str:
        return self.settings.sql_dialect

    @property
    def is_jdbc(self) -> bool:
        from provisa.generic_sql.jdbc import is_jdbc

        return is_jdbc(self.settings.url)

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
        # The URL names the host, port and database; the source's own user and password are set
        # on it. ``host``/``port``/``database`` are the standard fields, which this type leaves
        # empty.
        loop = asyncio.get_running_loop()
        if self.is_jdbc:
            from provisa.generic_sql import jdbc

            jar, cls = jdbc.driver_settings(self._hints)
            jdbc.refuse_where_not_allowed()  # before Provisa's JVM is started for it
            jdbc.start_jvm()
            self._shown = self.settings.url
            self._jdbc = await loop.run_in_executor(
                None, jdbc.connect, self.settings.url, jar, cls, user, password
            )
            return
        from provisa.generic_sql.url import connection_url, create_engine

        url, self._shown = connection_url(self.settings.url, user, password)
        self._drivername = url.drivername
        self._engine = await loop.run_in_executor(None, create_engine, url)

    # -- a jdbc: URL ------------------------------------------------------------------------------

    def _jdbc_run(self, sql: str, params: list | None) -> QueryResult:
        from provisa.generic_sql.jdbc import qmark

        text, values = qmark(sql, params)
        with self._jdbc_lock:
            cursor = self._jdbc.cursor()
            try:
                cursor.execute(text, values)
                if cursor.description is None:
                    rowcount = cursor.rowcount
                    return QueryResult(
                        rows=[], column_names=[], rowcount=rowcount if rowcount >= 0 else None
                    )
                columns = [d[0] for d in cursor.description]
                return QueryResult(rows=[tuple(r) for r in cursor.fetchall()], column_names=columns)
            finally:
                cursor.close()

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        if self._jdbc is not None:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self._jdbc_run, sql, params)
        return await super().execute(sql, params)

    async def open_stream(self, sql: str, params: list | None = None) -> DirectResultStream:
        if self._jdbc is not None:
            from provisa.generic_sql.jdbc import qmark

            text, values = qmark(sql, params)
            return JdbcResultStream(self._jdbc, self._jdbc_lock, text, values)
        return await super().open_stream(sql, params)

    async def close(self) -> None:
        if self._jdbc is not None:
            jdbc, self._jdbc = self._jdbc, None
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, jdbc.close)
            return
        await super().close()

    @property
    def is_connected(self) -> bool:
        return self._jdbc is not None or super().is_connected

    # -- what the database holds, as its SQLAlchemy dialect or JDBC metadata reads it -------------
    # (the Register Table pickers)

    async def _inspected(self, read):
        from sqlalchemy import inspect

        engine = self._engine
        if engine is None:
            raise RuntimeError("GenericSQLDriver is not connected")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: read(inspect(engine)))

    async def _metadata(self, read):
        """``read`` of the JDBC connection's ``DatabaseMetaData``, each result set's rows."""
        loop = asyncio.get_running_loop()

        def _run():
            with self._jdbc_lock:
                return read(self._jdbc._jcx.getMetaData())

        return await loop.run_in_executor(None, _run)

    async def schema_names(self) -> list[str]:
        """The database's schemas, as its dialect (or JDBC driver) lists them."""
        if self._jdbc is not None:
            return await self._metadata(
                lambda md: _strings(md.getSchemas(), "TABLE_SCHEM") or ["main"]
            )
        return await self._inspected(lambda inspector: list(inspector.get_schema_names()))

    async def table_names(self, schema: str) -> list[tuple[str, str | None]]:
        """``[(table, comment)]`` of ``schema``; a dialect that keeps no comments gives None."""
        if self._jdbc is not None:
            return await self._metadata(lambda md: _jdbc_tables(md, schema))

        def read(inspector) -> list[tuple[str, str | None]]:
            from sqlalchemy.exc import NotSupportedError

            listed = []
            for name in inspector.get_table_names(schema=schema):
                try:
                    comment = inspector.get_table_comment(name, schema=schema).get("text")
                except (NotImplementedError, NotSupportedError):
                    comment = None  # the dialect keeps no table comments
                listed.append((name, comment))
            return listed

        return await self._inspected(read)

    async def column_types(self, schema: str, table: str) -> list[tuple[str, str]]:
        """``[(column, type)]`` of ``schema.table``, each type as the dialect spells it."""
        if self._jdbc is not None:
            return await self._metadata(lambda md: _jdbc_columns(md, schema, table))
        return await self._inspected(
            lambda inspector: [
                (c["name"], str(c["type"])) for c in inspector.get_columns(table, schema=schema)
            ]
        )


def _jdbc_schema(schema: str) -> str | None:
    # A driver with no schemas (SQLite's) lists its tables under none; "main" is how the pickers
    # name that one schema.
    return None if schema == "main" else schema


def _strings(result_set: Any, column: str) -> list[str]:
    out = []
    try:
        while result_set.next():
            out.append(str(result_set.getString(column)))
    finally:
        result_set.close()
    return out


def _jdbc_tables(md: Any, schema: str) -> list[tuple[str, str | None]]:
    import jpype

    strings: Any = jpype.JArray(jpype.JString)
    rs = md.getTables(None, _jdbc_schema(schema), "%", strings(["TABLE"]))
    out = []
    try:
        while rs.next():
            remarks = rs.getString("REMARKS")
            out.append((str(rs.getString("TABLE_NAME")), str(remarks) if remarks else None))
    finally:
        rs.close()
    return out


def _jdbc_columns(md: Any, schema: str, table: str) -> list[tuple[str, str]]:
    rs = md.getColumns(None, _jdbc_schema(schema), table, "%")
    out = []
    try:
        while rs.next():
            out.append((str(rs.getString("COLUMN_NAME")), str(rs.getString("TYPE_NAME"))))
    finally:
        rs.close()
    return out


class JdbcResultStream(DirectResultStream):  # REQ-2191, REQ-1190
    """One statement's result read a batch at a time from the JDBC cursor; the connection is held
    for the statement until the stream is read to its end or closed."""

    def __init__(self, conn: Any, lock: threading.Lock, sql: str, values: list) -> None:
        self.column_types = None
        lock.acquire()
        self._lock: threading.Lock | None = lock
        self._cursor: Any = conn.cursor()
        try:
            self._cursor.execute(sql, values)
            description = self._cursor.description
            self.column_names = [d[0] for d in description] if description else []
            if description is None:
                self._release()
        except BaseException:
            self._release()
            raise

    def _release(self) -> None:
        cursor, self._cursor = self._cursor, None
        lock, self._lock = self._lock, None
        try:
            if cursor is not None:
                cursor.close()
        finally:
            if lock is not None:
                lock.release()

    async def fetch(self, size: int) -> list[tuple]:
        if self._cursor is None:
            return []
        try:
            rows = [tuple(r) for r in self._cursor.fetchmany(size)]
        except BaseException:
            self._release()
            raise
        if not rows:
            self._release()
        return rows

    async def close(self) -> None:
        self._release()
