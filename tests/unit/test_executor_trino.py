# Copyright (c) 2026 Kenneth Stott
# Canary: ce3dac0d-8092-43b5-9b45-f43380eae5d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Trino execution layer, QueryResult, parameter substitution,
trino_write CTAS helpers, and trino_flight utilities.
"""

from __future__ import annotations

import pytest

from provisa.executor.trino import QueryResult, execute_trino
from provisa.executor.trino_write import (
    TRINO_NATIVE_FORMATS,
    _iceberg_format,
    is_trino_native_format,
)
import provisa.executor.trino_flight as _flight_mod
from provisa.executor.trino_flight import _substitute_params


# ---------------------------------------------------------------------------
# QueryResult dataclass
# ---------------------------------------------------------------------------

class TestQueryResult:
    def test_construction(self):
        qr = QueryResult(rows=[(1, "a"), (2, "b")], column_names=["id", "name"])
        assert qr.rows == [(1, "a"), (2, "b")]
        assert qr.column_names == ["id", "name"]

    def test_empty_rows(self):
        qr = QueryResult(rows=[], column_names=["id"])
        assert qr.rows == []
        assert len(qr.column_names) == 1

    def test_row_count(self):
        qr = QueryResult(rows=[(i,) for i in range(10)], column_names=["n"])
        assert len(qr.rows) == 10

    def test_column_names_preserved_order(self):
        names = ["z", "a", "m", "b"]
        qr = QueryResult(rows=[], column_names=names)
        assert qr.column_names == names


# ---------------------------------------------------------------------------
# execute_trino — parameter substitution (via a real stub connection)
# ---------------------------------------------------------------------------

class _FakeCursor:
    """Minimal cursor stand-in to verify execute_trino parameter handling."""

    def __init__(self, rows=None, cols=None):
        self._rows = rows or []
        self._cols = cols or []
        self.last_sql: str | None = None
        self.last_params = None
        self.description = [(c,) for c in self._cols]

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params

    def fetchall(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, rows=None, cols=None):
        self._cursor = _FakeCursor(rows=rows, cols=cols)

    def cursor(self):
        return self._cursor


class TestExecuteTrinoParameterSubstitution:
    """execute_trino must replace $N / @N placeholders with ? before sending."""

    def test_no_params_passed_as_is(self):
        conn = _FakeConnection(rows=[(42,)], cols=["n"])
        result = execute_trino(conn, "SELECT 42 AS n")
        assert result.rows == [(42,)]
        assert result.column_names == ["n"]
        # No params → original SQL used as-is
        assert conn._cursor.last_sql == "SELECT 42 AS n"
        assert conn._cursor.last_params is None

    def test_dollar_param_replaced(self):
        conn = _FakeConnection(rows=[("x",)], cols=["v"])
        execute_trino(conn, "SELECT $1 AS v", params=["x"])
        assert "?" in conn._cursor.last_sql
        assert "$1" not in conn._cursor.last_sql

    def test_at_param_replaced(self):
        conn = _FakeConnection(rows=[("x",)], cols=["v"])
        execute_trino(conn, "SELECT @1 AS v", params=["x"])
        assert "?" in conn._cursor.last_sql
        assert "@1" not in conn._cursor.last_sql

    def test_multiple_params_all_replaced(self):
        conn = _FakeConnection(rows=[], cols=["a", "b"])
        execute_trino(conn, "SELECT $1, $2", params=["foo", "bar"])
        # Both placeholders should be replaced with ?
        assert conn._cursor.last_sql.count("?") == 2
        assert "$1" not in conn._cursor.last_sql
        assert "$2" not in conn._cursor.last_sql

    def test_replacement_order_no_prefix_collision(self):
        """$10 must not accidentally match $1 prefix — replace in reverse order."""
        conn = _FakeConnection(rows=[], cols=["a"])
        sql = "SELECT $1, $10"
        execute_trino(conn, sql, params=list(range(10)))
        # Both $1 and $10 must be replaced
        assert "$1" not in conn._cursor.last_sql
        assert "$10" not in conn._cursor.last_sql
        assert conn._cursor.last_sql.count("?") == 2

    def test_returns_query_result(self):
        conn = _FakeConnection(rows=[(1,), (2,)], cols=["id"])
        result = execute_trino(conn, "SELECT id FROM t")
        assert isinstance(result, QueryResult)
        assert result.rows == [(1,), (2,)]
        assert result.column_names == ["id"]

    def test_empty_description_gives_empty_columns(self):
        conn = _FakeConnection(rows=[], cols=[])
        conn._cursor.description = None
        result = execute_trino(conn, "SELECT 1")
        assert result.column_names == []

    def test_session_hints_execute_set_session(self):
        """Session hints must emit SET SESSION statements before the main query."""
        executed: list[str] = []

        class _TrackingCursor:
            description = [("n",)]

            def execute(self, sql, params=None):
                executed.append(sql)

            def fetchall(self):
                return [(1,)]

        class _TrackingConn:
            def cursor(self):
                return _TrackingCursor()

        execute_trino(
            _TrackingConn(),
            "SELECT 1 AS n",
            session_hints={"join_distribution_type": "BROADCAST"},
        )
        # First execute call is the SET SESSION statement
        assert any("SET SESSION" in s for s in executed)
        assert any("join_distribution_type" in s for s in executed)
        # Main query is also executed
        assert any("SELECT 1" in s for s in executed)


# ---------------------------------------------------------------------------
# trino_write — CTAS helpers
# ---------------------------------------------------------------------------

class TestTrinoNativeFormats:
    def test_parquet_is_native(self):
        assert is_trino_native_format("parquet")

    def test_orc_is_native(self):
        assert is_trino_native_format("orc")

    def test_json_not_native(self):
        assert not is_trino_native_format("json")

    def test_csv_not_native(self):
        assert not is_trino_native_format("csv")

    def test_arrow_not_native(self):
        assert not is_trino_native_format("arrow")

    def test_ndjson_not_native(self):
        assert not is_trino_native_format("ndjson")

    def test_case_insensitive(self):
        assert is_trino_native_format("PARQUET")
        assert is_trino_native_format("ORC")

    def test_constants_set(self):
        assert "parquet" in TRINO_NATIVE_FORMATS
        assert "orc" in TRINO_NATIVE_FORMATS


class TestIcebergFormat:
    def test_parquet_uppercased(self):
        assert _iceberg_format("parquet") == "PARQUET"

    def test_orc_uppercased(self):
        assert _iceberg_format("orc") == "ORC"


# ---------------------------------------------------------------------------
# trino_flight — parameter substitution
# ---------------------------------------------------------------------------

class TestSubstituteParams:
    def test_no_params_unchanged(self):
        sql = "SELECT id FROM orders"
        assert _substitute_params(sql, None) == sql

    def test_empty_params_unchanged(self):
        sql = "SELECT id FROM orders"
        assert _substitute_params(sql, []) == sql

    def test_string_param_quoted(self):
        sql = "SELECT $1"
        result = _substitute_params(sql, ["hello"])
        assert "'hello'" in result
        assert "?" not in result

    def test_string_param_escapes_single_quote(self):
        sql = "SELECT $1"
        result = _substitute_params(sql, ["it's"])
        assert "it''s" in result

    def test_none_param_becomes_null(self):
        sql = "SELECT $1"
        result = _substitute_params(sql, [None])
        assert "NULL" in result

    def test_int_param_inlined(self):
        sql = "SELECT $1"
        result = _substitute_params(sql, [42])
        assert "42" in result

    def test_multiple_params_in_order(self):
        sql = "SELECT $1, $2"
        result = _substitute_params(sql, ["a", "b"])
        assert "'a'" in result
        assert "'b'" in result

    def test_at_style_replaced(self):
        sql = "SELECT @1"
        result = _substitute_params(sql, ["x"])
        assert "'x'" in result
        assert "@1" not in result


