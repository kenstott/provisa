# Copyright (c) 2026 Kenneth Stott
# Canary: f8db7eae-f533-4b6f-8292-4ab835238ee5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register Table lists a MySQL source's tables and columns through its own driver.

``MySQLDriver.execute`` takes ``$N`` placeholders: it escapes every literal ``%`` and binds
``$N`` as PyMySQL's ``%s``. Discovery sent ``%s`` itself; the escape made it a literal, PyMySQL's
formatting raised "not all arguments converted", and the picker came back empty. The stand-in
cursor formats the statement the way PyMySQL does, so a placeholder the driver does not bind
fails here as it does against a server."""

# Requirements: REQ-012, REQ-252, REQ-1732

from __future__ import annotations

from contextlib import contextmanager

import pytest

from provisa.api.admin.introspect import native_columns, native_tables
from provisa.executor.drivers.mysql import MySQLDriver

_TABLES = {"provisa_demo": ["widgets"]}
_COLUMNS = {("provisa_demo", "widgets"): [("id", "int"), ("name", "varchar")]}


class _Cursor:
    def __init__(self) -> None:
        self.description = None
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query: str, args=None) -> None:
        # PyMySQL: with args, the statement is %-formatted with the escaped values.
        sql = query % tuple(repr(a) for a in args) if args is not None else query
        if "information_schema.TABLES" in sql:
            schema = sql.split("TABLE_SCHEMA = ")[1].split(" ")[0].strip("'")
            self._rows = [(t, "") for t in _TABLES.get(schema, [])]
            self.description = [("TABLE_NAME",), ("TABLE_COMMENT",)]
        else:
            parts = sql.split("'")
            self._rows = list(_COLUMNS.get((parts[1], parts[3]), []))
            self.description = [("COLUMN_NAME",), ("DATA_TYPE",)]

    def fetchmany(self, _n):
        rows, self._rows = self._rows, []
        return rows


class _Conn:
    def thread_id(self) -> int:
        return 1

    def cursor(self) -> _Cursor:
        return _Cursor()


class _BlockingPool:
    @contextmanager
    def connection(self, is_broken=None):
        yield _Conn()


class _Pool:
    """The source pool: one MySQL source, executed by the real driver."""

    def __init__(self) -> None:
        self.driver = MySQLDriver()
        self.driver._pool = _BlockingPool()  # type: ignore[assignment]

    def has(self, source_id: str) -> bool:
        return source_id == "shop"

    async def execute(self, source_id: str, sql: str, params: list | None = None):
        return await self.driver.execute(sql, params)


@pytest.mark.asyncio
async def test_the_table_picker_lists_a_mysql_schemas_tables():
    tables = await native_tables("shop", "mysql", "provisa_demo", _Pool(), None, None)  # type: ignore[arg-type]
    assert [t.name for t in tables or []] == ["widgets"]


@pytest.mark.asyncio
async def test_the_column_picker_lists_a_mysql_tables_columns():
    cols = await native_columns("shop", "mysql", "provisa_demo", "widgets", _Pool())  # type: ignore[arg-type]
    assert cols == [("id", "int"), ("name", "varchar")]
