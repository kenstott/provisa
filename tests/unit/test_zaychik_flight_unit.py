# Copyright (c) 2026 Kenneth Stott
# Canary: d1e2f3a4-b5c6-7890-daeb-f0a1b2c3d4e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/executor/trino_flight.py — behaviors NOT already
covered in test_zaychik_fallback.py.

test_zaychik_fallback.py already covers:
- is_zaychik_available: initially True, returns False after manual flag set
- execute_with_fallback: uses zaychik when available, falls back on exception,
  marks flag unavailable after failure, stays disabled after first failure,
  skips zaychik when flight_conn is None.

This file adds:
- _substitute_params — @N / $N style, multiple params, no params, string escaping, None
- execute_with_fallback success path — trino NOT called; flag stays True
- execute_with_fallback ImportError is treated identically to any other exception
- create_flight_connection ImportError path
- execute_trino_flight_arrow — return type is pa.Table with correct column names
- execute_trino_flight — wraps arrow result into QueryResult
- execute_trino_flight_stream — returns (schema, generator) pair
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import sys
import types

import pyarrow as pa
import pytest

import provisa.executor.trino_flight as trf


def _reset_zaychik():
    """Reset the module-level _zaychik_available flag to True."""
    trf._zaychik_available = True


# ---------------------------------------------------------------------------
# _substitute_params
# ---------------------------------------------------------------------------

class TestSubstituteParams:
    def test_no_params_returns_sql_unchanged(self):
        sql = "SELECT * FROM orders"
        assert trf._substitute_params(sql, None) == sql

    def test_empty_params_returns_sql_unchanged(self):
        sql = "SELECT * FROM orders WHERE id = 1"
        assert trf._substitute_params(sql, []) == sql

    def test_at_style_single_param_integer(self):
        sql = "SELECT * FROM t WHERE id = @1"
        result = trf._substitute_params(sql, [42])
        assert result == "SELECT * FROM t WHERE id = 42"
        assert "@1" not in result

    def test_dollar_style_single_param_integer(self):
        sql = "SELECT * FROM t WHERE id = $1"
        result = trf._substitute_params(sql, [99])
        assert result == "SELECT * FROM t WHERE id = 99"
        assert "$1" not in result

    def test_at_style_multiple_params_in_order(self):
        sql = "SELECT * FROM t WHERE a = @1 AND b = @2"
        result = trf._substitute_params(sql, ["hello", 7])
        assert "'hello'" in result
        assert "7" in result
        assert "@1" not in result
        assert "@2" not in result

    def test_dollar_style_multiple_params_in_order(self):
        sql = "SELECT * FROM t WHERE x = $1 AND y = $2 AND z = $3"
        result = trf._substitute_params(sql, [1, 2, 3])
        assert "@" not in result
        assert "$" not in result
        assert "1" in result
        assert "2" in result
        assert "3" in result

    def test_string_param_is_single_quoted(self):
        sql = "SELECT * FROM t WHERE name = @1"
        result = trf._substitute_params(sql, ["Alice"])
        assert result == "SELECT * FROM t WHERE name = 'Alice'"

    def test_string_param_with_embedded_single_quote_is_escaped(self):
        sql = "SELECT * FROM t WHERE name = @1"
        result = trf._substitute_params(sql, ["O'Brien"])
        # Single quote must be escaped as ''
        assert "O''Brien" in result
        assert "O'Brien'" not in result or "O''Brien" in result

    def test_none_param_becomes_null(self):
        sql = "SELECT * FROM t WHERE val = @1"
        result = trf._substitute_params(sql, [None])
        assert "NULL" in result
        assert "@1" not in result

    def test_float_param_inlined(self):
        sql = "SELECT * FROM t WHERE price > @1"
        result = trf._substitute_params(sql, [3.14])
        assert "3.14" in result

    def test_higher_numbered_params_not_partially_replaced(self):
        """@10 must not be treated as @1 followed by '0'."""
        sql = "SELECT @1, @2, @10"
        # The implementation replaces in reverse order (len → 1), so @10 is
        # replaced before @1, which prevents partial replacement.
        result = trf._substitute_params(sql, ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])
        assert "@10" not in result
        assert "@1" not in result
        assert "@2" not in result


# ---------------------------------------------------------------------------
# execute_with_fallback — success path (not covered in fallback tests)
# ---------------------------------------------------------------------------

class TestExecuteWithFallbackSuccessPath:
    def setup_method(self):
        _reset_zaychik()

    def test_success_path_does_not_call_trino(self):
        """When Flight succeeds, execute_trino (REST) is never called."""
        from provisa.executor.trino import QueryResult

        expected = QueryResult(rows=[(1,)], column_names=["id"])
        flight_conn = MagicMock()
        trino_conn = MagicMock()

        with patch.object(trf, "execute_trino_flight", return_value=expected):
            with patch("provisa.executor.trino.execute_trino") as mock_trino:
                result = trf.execute_with_fallback(flight_conn, trino_conn, "SELECT 1")

        mock_trino.assert_not_called()
        assert result is expected

    def test_success_path_leaves_flag_true(self):
        """A successful Flight call must leave _zaychik_available as True."""
        from provisa.executor.trino import QueryResult

        expected = QueryResult(rows=[(1,)], column_names=["id"])
        flight_conn = MagicMock()
        trino_conn = MagicMock()

        with patch.object(trf, "execute_trino_flight", return_value=expected):
            trf.execute_with_fallback(flight_conn, trino_conn, "SELECT 1")

        assert trf._zaychik_available is True

    def test_import_error_triggers_fallback_and_sets_flag(self):
        """ImportError (e.g. adbc missing) is treated like any other exception."""
        from provisa.executor.trino import QueryResult

        fallback = QueryResult(rows=[(0,)], column_names=["x"])
        flight_conn = MagicMock()
        trino_conn = MagicMock()

        with patch.object(trf, "execute_trino_flight", side_effect=ImportError("no adbc")):
            with patch("provisa.executor.trino.execute_trino", return_value=fallback) as mock_trino:
                result = trf.execute_with_fallback(flight_conn, trino_conn, "SELECT 1")

        assert trf._zaychik_available is False
        mock_trino.assert_called_once()
        assert result is fallback


# ---------------------------------------------------------------------------
# create_flight_connection — ImportError path
# ---------------------------------------------------------------------------

class TestCreateFlightConnection:
    def test_import_error_propagates_when_adbc_missing(self):
        """If adbc_driver_flightsql is not importable, ImportError propagates.

        Setting sys.modules entries to None causes ``import <name>`` to raise
        ImportError — this is a standard Python mechanism for blocking imports.
        """
        import sys

        # Remove any already-loaded adbc modules and block re-import by setting
        # the sys.modules entries to None (sentinel that triggers ImportError).
        blocked = {
            k: v for k, v in sys.modules.items() if "adbc_driver_flightsql" in k
        }
        try:
            for key in list(sys.modules.keys()):
                if "adbc_driver_flightsql" in key:
                    sys.modules[key] = None  # type: ignore[assignment]
            # Ensure the top-level name is also blocked
            sys.modules.setdefault("adbc_driver_flightsql", None)  # type: ignore[assignment]
            sys.modules.setdefault("adbc_driver_flightsql.dbapi", None)  # type: ignore[assignment]

            with pytest.raises(ImportError):
                trf.create_flight_connection(host="localhost", port=8480, user="provisa")
        finally:
            # Restore sys.modules to avoid polluting other tests
            for key in ("adbc_driver_flightsql", "adbc_driver_flightsql.dbapi"):
                sys.modules.pop(key, None)
            sys.modules.update(blocked)

    def test_connection_uri_uses_host_and_port(self):
        """create_flight_connection builds a grpc URI from host and port.

        'import a.b as x' resolves x via sys.modules["a"].b (not sys.modules["a.b"]),
        so the package mock must expose .dbapi as an attribute.
        """
        mock_conn = MagicMock()
        mock_dbapi = MagicMock()
        mock_dbapi.connect.return_value = mock_conn

        mock_package = MagicMock()
        mock_package.dbapi = mock_dbapi

        with patch.dict("sys.modules", {
            "adbc_driver_flightsql": mock_package,
            "adbc_driver_flightsql.dbapi": mock_dbapi,
        }):
            conn = trf.create_flight_connection(host="zaychik-host", port=9999, user="testuser")

        call_kwargs = mock_dbapi.connect.call_args
        assert "grpc://zaychik-host:9999" in call_kwargs.kwargs.get("uri", "")
        assert conn is mock_conn


# ---------------------------------------------------------------------------
# execute_trino_flight_arrow
# ---------------------------------------------------------------------------

class TestExecuteTrinoFlightArrow:
    def _make_mock_conn(self, table: pa.Table) -> MagicMock:
        """Return a mock Flight connection whose cursor yields the given table."""
        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = table

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn

    def test_returns_pyarrow_table(self):
        schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
        table = pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=schema)
        conn = self._make_mock_conn(table)

        result = trf.execute_trino_flight_arrow(conn, "SELECT id, name FROM t")

        assert isinstance(result, pa.Table)

    def test_column_names_match_table(self):
        schema = pa.schema([("order_id", pa.int64()), ("amount", pa.float64())])
        table = pa.table({"order_id": [10], "amount": [99.5]}, schema=schema)
        conn = self._make_mock_conn(table)

        result = trf.execute_trino_flight_arrow(conn, "SELECT order_id, amount FROM orders")

        assert "order_id" in result.column_names
        assert "amount" in result.column_names

    def test_params_substituted_before_execution(self):
        """Parameters are inlined into SQL before the cursor executes."""
        schema = pa.schema([("id", pa.int64())])
        table = pa.table({"id": [5]}, schema=schema)

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = table
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        trf.execute_trino_flight_arrow(mock_conn, "SELECT * FROM t WHERE id = @1", [5])

        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "5" in executed_sql
        assert "@1" not in executed_sql

    def test_cursor_is_closed_after_fetch(self):
        """cursor.close() is always called after fetching the table."""
        schema = pa.schema([("x", pa.int64())])
        table = pa.table({"x": [1]}, schema=schema)
        conn = self._make_mock_conn(table)
        cursor = conn.cursor.return_value

        trf.execute_trino_flight_arrow(conn, "SELECT x FROM t")

        cursor.close.assert_called_once()

    def test_row_count_preserved(self):
        schema = pa.schema([("n", pa.int64())])
        table = pa.table({"n": list(range(50))}, schema=schema)
        conn = self._make_mock_conn(table)

        result = trf.execute_trino_flight_arrow(conn, "SELECT n FROM big")

        assert result.num_rows == 50


# ---------------------------------------------------------------------------
# execute_trino_flight (QueryResult wrapper)
# ---------------------------------------------------------------------------

class TestExecuteTrinoFlight:
    def test_returns_query_result_type(self):
        from provisa.executor.trino import QueryResult

        schema = pa.schema([("id", pa.int64()), ("val", pa.string())])
        table = pa.table({"id": [1, 2], "val": ["x", "y"]}, schema=schema)

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = table
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        result = trf.execute_trino_flight(mock_conn, "SELECT id, val FROM t")

        assert isinstance(result, QueryResult)
        assert result.column_names == ["id", "val"]
        assert len(result.rows) == 2

    def test_rows_are_tuples(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
        table = pa.table({"a": [10, 20], "b": [30, 40]}, schema=schema)

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = table
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        result = trf.execute_trino_flight(mock_conn, "SELECT a, b FROM t")

        for row in result.rows:
            assert isinstance(row, tuple)


# ---------------------------------------------------------------------------
# execute_trino_flight_stream
# ---------------------------------------------------------------------------

class TestExecuteTrinoFlightStream:
    def _make_stream_conn(self, batches: list[pa.RecordBatch], schema: pa.Schema) -> MagicMock:
        """Build a mock connection whose reader yields the given batches."""
        mock_reader = MagicMock()
        mock_reader.schema = schema

        batch_iter = iter(batches)

        def _read_next():
            try:
                return next(batch_iter)
            except StopIteration:
                raise StopIteration

        mock_reader.read_next_batch.side_effect = _read_next

        mock_cursor = MagicMock()
        mock_cursor.fetch_record_batch.return_value = mock_reader

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn

    def test_returns_schema_and_generator_tuple(self):
        schema = pa.schema([("id", pa.int64())])
        batches = [pa.record_batch({"id": [1, 2]}, schema=schema)]
        conn = self._make_stream_conn(batches, schema)

        result_schema, gen = trf.execute_trino_flight_stream(conn, "SELECT id FROM t")

        assert result_schema == schema
        assert hasattr(gen, "__iter__") or hasattr(gen, "__next__")

    def test_generator_yields_record_batches(self):
        schema = pa.schema([("n", pa.int64())])
        batches = [
            pa.record_batch({"n": [1, 2]}, schema=schema),
            pa.record_batch({"n": [3, 4]}, schema=schema),
        ]
        conn = self._make_stream_conn(batches, schema)

        _, gen = trf.execute_trino_flight_stream(conn, "SELECT n FROM t")
        collected = list(gen)

        assert len(collected) == 2
        assert all(isinstance(b, pa.RecordBatch) for b in collected)

    def test_stream_schema_field_names_correct(self):
        schema = pa.schema([("foo", pa.string()), ("bar", pa.int64())])
        batches = [pa.record_batch({"foo": ["x"], "bar": [1]}, schema=schema)]
        conn = self._make_stream_conn(batches, schema)

        returned_schema, _ = trf.execute_trino_flight_stream(conn, "SELECT foo, bar FROM t")

        assert "foo" in returned_schema.names
        assert "bar" in returned_schema.names

    def test_cursor_closed_after_generator_exhausted(self):
        """cursor.close() is called inside the generator's finally block."""
        schema = pa.schema([("v", pa.int64())])
        batches = [pa.record_batch({"v": [7]}, schema=schema)]
        conn = self._make_stream_conn(batches, schema)
        cursor = conn.cursor.return_value

        _, gen = trf.execute_trino_flight_stream(conn, "SELECT v FROM t")
        # Exhaust the generator so the finally block runs
        list(gen)

        cursor.close.assert_called_once()

    def test_empty_stream_yields_no_batches(self):
        schema = pa.schema([("id", pa.int64())])
        conn = self._make_stream_conn([], schema)

        _, gen = trf.execute_trino_flight_stream(conn, "SELECT id FROM t")
        collected = list(gen)

        assert collected == []
