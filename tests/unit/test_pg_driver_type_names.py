# Copyright (c) 2026 Kenneth Stott
# Canary: bce605ea-b1e4-479b-a80e-db42424857e7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL driver reads its result columns' type names from the source's pg_type with the
OIDs written as integer literals (REQ-883, REQ-1946).

A Salesforce source's pgwire server received the OIDs bound as one array parameter as the text
'{25}' and could not read it as the integer list ``oid = ANY(...)`` needs: every write-route read
of a fresh driver failed "Conversion Error: Type VARCHAR with value '{25}' can't be cast to the
destination type INTEGER[]"."""

# Requirements: REQ-883, REQ-1946

from __future__ import annotations

from typing import Any

import pytest

from provisa.executor.drivers.postgresql import PostgreSQLDriver


class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self._conn = conn

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, sql: Any, params: Any = None) -> None:
        # A composed query (psycopg.sql) as the text it sends.
        text = sql.as_string(None) if hasattr(sql, "as_string") else sql
        self._conn.executed.append((text, params))

    def fetchall(self) -> list[tuple[int, str]]:
        return [(oid, name) for oid, name in self._conn.catalog.items()]


class _Conn:
    def __init__(self, catalog: dict[int, str]) -> None:
        self.catalog = catalog
        self.executed: list[tuple[str, Any]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self)


def test_the_oids_are_written_as_integer_literals_not_an_array_parameter():
    conn = _Conn({25: "text", 1043: "varchar"})
    names = PostgreSQLDriver()._type_names(conn, [1043, 25, 1043])  # type: ignore[arg-type]
    assert names == ["varchar", "text", "varchar"]
    assert conn.executed == [
        ("SELECT oid::int, typname FROM pg_type WHERE oid IN (25, 1043)", None)
    ]


def test_a_type_once_read_is_not_asked_for_again():
    drv = PostgreSQLDriver()
    conn = _Conn({25: "text"})
    drv._type_names(conn, [25])  # type: ignore[arg-type]
    drv._type_names(conn, [25])  # type: ignore[arg-type]
    assert len(conn.executed) == 1


def test_an_oid_the_catalog_lacks_is_refused_by_number():
    conn = _Conn({25: "text"})
    with pytest.raises(RuntimeError, match="9999"):
        PostgreSQLDriver()._type_names(conn, [25, 9999])  # type: ignore[arg-type]
