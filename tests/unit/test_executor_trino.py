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


import base64
import json

import pyarrow as pa

from provisa.executor.result import QueryResult
from provisa.executor.trino_flight import TrinoContext, bound_statement, execute_trino_flight
from provisa.executor.trino_write import (
    ENGINE_NATIVE_FORMATS,
    _iceberg_format,
    is_engine_native_format,
)


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
# execute_trino_flight — parameter binding and session (via a stub ADBC connection, REQ-2194)
# ---------------------------------------------------------------------------


def _zaychik_table(cols: list[str], rows: list[tuple]) -> pa.Table:
    """A table as the Zaychik proxy sends it: every field names its Trino type."""
    fields = [
        pa.field(
            c,
            pa.string(),
            metadata={
                "trino.type": "varchar",
                "trino.type_signature": json.dumps(
                    {"rawType": "varchar", "arguments": [{"kind": "LONG", "value": 2147483647}]}
                ),
                "trino.encoding": "arrow",
            },
        )
        for c in cols
    ]
    columns = [[r[i] for r in rows] for i in range(len(cols))]
    return pa.Table.from_arrays(
        [pa.array(v, pa.string()) for v in columns], schema=pa.schema(fields)
    )


class _FakeStatement:
    def __init__(self, cursor):
        self._cursor = cursor

    def set_options(self, **options):
        self._cursor.options.update(options)


class _FakeCursor:
    """Minimal ADBC cursor stand-in: records the statement and the call options it was given."""

    def __init__(self, rows=None, cols=None):
        self._table = _zaychik_table(cols or [], rows or [])
        self.last_sql: str | None = None
        self.options: dict[str, str] = {}
        self.adbc_statement = _FakeStatement(self)
        self.closed = False

    def execute(self, sql, params=None):
        assert params is None, "values are bound in the statement, never as driver params"
        self.last_sql = sql

    def fetch_arrow_table(self):
        return self._table

    def adbc_cancel(self):
        self.cancelled = True

    def close(self):
        self.closed = True


class _FakeConnection:
    def __init__(self, rows=None, cols=None):
        self._cursor = _FakeCursor(rows=rows, cols=cols)

    def cursor(self):
        return self._cursor


class TestExecuteTrinoParameterSubstitution:
    """execute_trino_flight binds $N / @N values as the Trino client does (EXECUTE IMMEDIATE)."""

    def test_no_params_passed_as_is(self):
        conn = _FakeConnection(rows=[("42",)], cols=["n"])
        result = execute_trino_flight(conn, "SELECT 42 AS n")
        assert result.rows == [("42",)]
        assert result.column_names == ["n"]
        # No params → original SQL used as-is
        assert conn._cursor.last_sql == "SELECT 42 AS n"

    def test_dollar_param_replaced(self):
        conn = _FakeConnection(rows=[("x",)], cols=["v"])
        execute_trino_flight(conn, "SELECT $1 AS v", params=["x"])
        assert conn._cursor.last_sql == "EXECUTE IMMEDIATE 'SELECT ? AS v' USING 'x'"

    def test_at_param_replaced(self):
        conn = _FakeConnection(rows=[("x",)], cols=["v"])
        execute_trino_flight(conn, "SELECT @1 AS v", params=["x"])
        assert conn._cursor.last_sql == "EXECUTE IMMEDIATE 'SELECT ? AS v' USING 'x'"

    def test_multiple_params_all_replaced(self):
        conn = _FakeConnection(rows=[], cols=["a", "b"])
        execute_trino_flight(conn, "SELECT $1, $2", params=["foo", "bar"])
        assert conn._cursor.last_sql == "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 'foo','bar'"

    def test_replacement_order_no_prefix_collision(self):
        """$10 must not accidentally match $1 prefix."""
        conn = _FakeConnection(rows=[], cols=["a"])
        execute_trino_flight(conn, "SELECT $1, $10", params=list(range(10)))
        assert conn._cursor.last_sql == "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 0,9"

    def test_returns_query_result(self):
        conn = _FakeConnection(rows=[("1",), ("2",)], cols=["id"])
        result = execute_trino_flight(conn, "SELECT id FROM t")
        assert isinstance(result, QueryResult)
        assert result.rows == [("1",), ("2",)]
        assert result.column_names == ["id"]
        assert result.column_types == ["varchar"]
        assert conn._cursor.closed

    def test_empty_description_gives_empty_columns(self):
        conn = _FakeConnection(rows=[], cols=[])
        result = execute_trino_flight(conn, "SELECT 1")
        assert result.column_names == []

    def test_session_hints_ride_the_call_header(self):
        """Session properties reach Trino with the statement (a fresh session per statement
        cannot carry a SET SESSION): in the x-provisa-trino-context call header."""
        conn = _FakeConnection(rows=[("1",)], cols=["n"])
        execute_trino_flight(
            conn,
            "SELECT 1 AS n",
            context=TrinoContext(
                source="provisa/org-1",
                properties={"join_distribution_type": "BROADCAST"},
            ),
        )
        header = conn._cursor.options["adbc.flight.sql.rpc.call_header.x-provisa-trino-context"]
        assert json.loads(base64.b64decode(header)) == {
            "source": "provisa/org-1",
            "properties": {"join_distribution_type": "BROADCAST"},
        }
        assert conn._cursor.last_sql == "SELECT 1 AS n"


# ---------------------------------------------------------------------------
# trino_write — CTAS helpers
# ---------------------------------------------------------------------------


class TestTrinoNativeFormats:
    def test_parquet_is_native(self):
        assert is_engine_native_format("parquet")

    def test_orc_is_native(self):
        assert is_engine_native_format("orc")

    def test_json_not_native(self):
        assert not is_engine_native_format("json")

    def test_csv_not_native(self):
        assert not is_engine_native_format("csv")

    def test_arrow_not_native(self):
        assert not is_engine_native_format("arrow")

    def test_ndjson_not_native(self):
        assert not is_engine_native_format("ndjson")

    def test_case_insensitive(self):
        assert is_engine_native_format("PARQUET")
        assert is_engine_native_format("ORC")

    def test_constants_set(self):
        assert "parquet" in ENGINE_NATIVE_FORMATS
        assert "orc" in ENGINE_NATIVE_FORMATS


class TestIcebergFormat:
    def test_parquet_uppercased(self):
        assert _iceberg_format("parquet") == "PARQUET"

    def test_orc_uppercased(self):
        assert _iceberg_format("orc") == "ORC"


# ---------------------------------------------------------------------------
# trino_flight — parameter substitution
# ---------------------------------------------------------------------------


class TestSubstituteParams:
    """The statement a read sends: the Trino client's EXECUTE IMMEDIATE binding (REQ-2194)."""

    def test_no_params_unchanged(self):
        sql = "SELECT id FROM orders"
        assert bound_statement(sql, None) == sql

    def test_empty_params_unchanged(self):
        sql = "SELECT id FROM orders"
        assert bound_statement(sql, []) == sql

    def test_string_param_quoted(self):
        result = bound_statement("SELECT $1", ["hello"])
        assert result == "EXECUTE IMMEDIATE 'SELECT ?' USING 'hello'"

    def test_string_param_escapes_single_quote(self):
        result = bound_statement("SELECT $1", ["it's"])
        assert result == "EXECUTE IMMEDIATE 'SELECT ?' USING 'it''s'"

    def test_none_param_becomes_null(self):
        assert bound_statement("SELECT $1", [None]) == "EXECUTE IMMEDIATE 'SELECT ?' USING NULL"

    def test_int_param_inlined(self):
        assert bound_statement("SELECT $1", [42]) == "EXECUTE IMMEDIATE 'SELECT ?' USING 42"

    def test_multiple_params_in_order(self):
        result = bound_statement("SELECT $1, $2", ["a", "b"])
        assert result == "EXECUTE IMMEDIATE 'SELECT ?, ?' USING 'a','b'"

    def test_at_style_replaced(self):
        result = bound_statement("SELECT @1", ["x"])
        assert result == "EXECUTE IMMEDIATE 'SELECT ?' USING 'x'"
        assert "@1" not in result

    def test_quote_in_the_statement_is_escaped_in_the_immediate_text(self):
        result = bound_statement("SELECT 'a' || $1", ["b"])
        assert result == "EXECUTE IMMEDIATE 'SELECT ''a'' || ?' USING 'b'"
