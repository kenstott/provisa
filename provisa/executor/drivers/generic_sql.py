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
URL (provisa.generic_sql.url) rather than at a URL built from a type's drivername, and written
to in the SQL dialect the source chose.

Its rows are Python tuples: SQLAlchemy hands back DBAPI rows, and no driver behind an arbitrary
URL can be counted on for Arrow (docs/arch/fetch-paths.md)."""

# Requirements: REQ-2191

from __future__ import annotations

import asyncio

from provisa.executor.drivers.sqlalchemy_driver import SQLAlchemyDriver
from provisa.generic_sql.settings import GenericSqlSettings, parse


class GenericSQLDriver(SQLAlchemyDriver):  # REQ-2191
    """A SQLAlchemy driver at the URL the source's settings name."""

    names_own_destination = True

    def __init__(self) -> None:
        super().__init__("")
        self._settings: GenericSqlSettings | None = None

    def configure(self, extra: dict[str, str]) -> None:
        self._settings = parse(extra)

    @property
    def settings(self) -> GenericSqlSettings:
        if self._settings is None:
            raise RuntimeError("a Generic SQL driver is connected only after it is configured")
        return self._settings

    @property
    def dialect(self) -> str:
        return self.settings.sql_dialect

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
        from provisa.generic_sql.url import connection_url, create_engine

        # The URL names the host, port and database; the source's own user and password are set
        # on it. host/port/database are the standard fields, which this type leaves
        # empty.
        url, self._shown = connection_url(self.settings.url, user, password)
        self._drivername = url.drivername
        loop = asyncio.get_running_loop()
        self._engine = await loop.run_in_executor(None, create_engine, url)

    # -- what the database holds, as its SQLAlchemy dialect reads it (the Register Table pickers) --

    async def _inspected(self, read):
        from sqlalchemy import inspect

        engine = self._engine
        if engine is None:
            raise RuntimeError("GenericSQLDriver is not connected")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: read(inspect(engine)))

    async def schema_names(self) -> list[str]:
        """The database's schemas, as its dialect lists them."""
        return await self._inspected(lambda inspector: list(inspector.get_schema_names()))

    async def table_names(self, schema: str) -> list[tuple[str, str | None]]:
        """``[(table, comment)]`` of ``schema``; a dialect that keeps no comments gives None."""

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
        return await self._inspected(
            lambda inspector: [
                (c["name"], str(c["type"])) for c in inspector.get_columns(table, schema=schema)
            ]
        )
