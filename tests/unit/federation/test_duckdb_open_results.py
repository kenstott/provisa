# Copyright (c) 2026 Kenneth Stott
# Canary: 8dff3612-daa7-456f-b859-8288d962a6dd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DuckDB connection with an undrained result refuses register/sql/create_function/load_extension
from 2.0 (ResourceInUseException); these pin that the shared-connection helpers leave none open."""

from __future__ import annotations

from typing import Any

import duckdb
import pytest

from provisa.core import engine_memory


class _StrictResult:
    def __init__(self, con: _StrictConnection, rows: list[tuple[Any, ...]]) -> None:
        self._con = con
        self._rows = rows
        con.open_result = self

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows, self._rows = self._rows, []
        self._con.open_result = None
        return rows

    def fetchone(self) -> tuple[Any, ...] | None:
        # DuckDB 2.0: reading one row does not close the result
        return self._rows[0] if self._rows else None


class _StrictConnection:
    """Refuses register/sql/create_function/load_extension while a result is open, as DuckDB 2.0."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = rows
        self.open_result: _StrictResult | None = None

    def execute(self, sql: str, params: Any = None) -> _StrictResult:
        return _StrictResult(self, list(self._rows))

    def sql(self, query: str) -> None:
        if self.open_result is not None:
            raise duckdb.Error("ResourceInUseException: result still open")


def test_strict_connection_refuses_after_fetchone() -> None:
    con = _StrictConnection([("1GB",)])
    con.execute("x").fetchone()
    with pytest.raises(duckdb.Error):
        con.sql("select 1")


def test_log_limit_leaves_no_open_result() -> None:
    con = _StrictConnection([("1GB",)])
    engine_memory.log_limit(con, "test")
    con.sql("select 1")


def test_first_row_returns_row_and_drains() -> None:
    con = _StrictConnection([("a",), ("b",)])
    assert engine_memory.first_row(con.execute("x")) == ("a",)
    con.sql("select 1")


def test_first_row_empty_is_none() -> None:
    con = _StrictConnection([])
    assert engine_memory.first_row(con.execute("x")) is None
    con.sql("select 1")


def test_filter_fetched_at_leaves_no_open_result() -> None:
    from provisa.federation.store_connection import filter_fetched_at_duckdb_native

    con = _StrictConnection([(1,)])
    filter_fetched_at_duckdb_native(con, catalog="c", schema="s", table="t", filter_hash="h")
    con.sql("select 1")
