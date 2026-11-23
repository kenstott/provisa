# Copyright (c) 2026 Kenneth Stott
# Canary: 7b2d9e04-3a61-4c58-8f17-e5a0c4d6b392
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1965: the datasource cast on DuckDB, executed by DuckDB. Each natively nested column is
registered from DuckDB's own spelling of its type, read through the to-JSON expression that
registration's attributes select, and comes back as JSON text with exact values inside."""

from __future__ import annotations

import json

import duckdb
import pytest
import sqlglot.expressions as exp

from provisa.core.ir_types import column_typing
from provisa.transpiler.native_json import (
    NON_FINITE,
    NativeJsonUnsupported,
    NestedColumn,
    native_json_marker,
    resolve_native_json,
    to_json_sql,
)
from provisa.transpiler.transpile import transpile

# (DuckDB column type, SQL literal of one value, the JSON a read returns)
_CASES = [
    ("VARCHAR[]", "['a', NULL, 'b']", ["a", None, "b"]),
    ("INTEGER[][]", "[[1, 2], [3]]", [[1, 2], [3]]),
    ("BIGINT[]", "[9007199254740993]", [9007199254740993]),
    ("BOOLEAN[]", "[true, false]", [True, False]),
    ("DECIMAL(20,2)[]", "[12345678901234567.89, 0.10]", ["12345678901234567.89", "0.10"]),
    ("DECIMAL(5,2)[][]", "[[1.50], [2.25, NULL]]", [["1.50"], ["2.25", None]]),
    ("DATE[]", "[DATE '2026-01-02']", ["2026-01-02"]),
    ("TIME[]", "[TIME '01:02:03']", ["01:02:03"]),
    (
        "TIMESTAMP[]",
        "[TIMESTAMP '2026-10-09 12:34:56.123456', NULL]",
        ["2026-10-09T12:34:56.123456", None],
    ),
    (
        "TIMESTAMP WITH TIME ZONE[]",
        "[TIMESTAMPTZ '2026-10-09 12:34:56.123456+02']",
        ["2026-10-09T10:34:56.123456"],  # the same instant, in UTC
    ),
    ("BLOB[]", "['\\x00\\xFF'::BLOB]", ["\\x00ff"]),
    ("DOUBLE[]", "[1.5, NULL]", [1.5, None]),
    (
        "UUID[]",
        "['6f1c2d3e-0000-4000-8000-000000000001'::UUID]",
        ["6f1c2d3e-0000-4000-8000-000000000001"],
    ),
    # Depth 0: nothing inside is typed; the value is DuckDB's own to-JSON.
    ("STRUCT(a INTEGER, b VARCHAR[])", "{'a': 1, 'b': ['x']}", {"a": 1, "b": ["x"]}),
    ("MAP(VARCHAR, INTEGER)", "MAP {'k': 1}", {"k": 1}),
    ("STRUCT(a INTEGER)[]", "[{'a': 1}, {'a': 2}]", [{"a": 1}, {"a": 2}]),
]


def _registered(con: duckdb.DuckDBPyConnection, table: str) -> NestedColumn:
    """The hidden attributes registration records, from the type DuckDB itself reports."""
    (reported,) = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = ? AND column_name = 'v'",
        [table],
    ).fetchone()
    typing = column_typing(reported, "duckdb")
    assert typing.data_type == "json" and typing.native_type
    return NestedColumn(typing.native_type, typing.array_depth, typing.element_type)


def _read_sql(nested: NestedColumn, table: str) -> str:
    """The JSON form a landing and a predicate read (REQ-1965): a result column of a typed array
    DuckDB holds natively is read as the array (test_native_typed_array_reads.py)."""
    return f'SELECT {to_json_sql('"v"', nested, "duckdb", "v")} AS "v" FROM "{table}"'


@pytest.mark.parametrize(("column_type", "literal", "expected"), _CASES)
def test_duckdb_hands_a_native_nested_value_over_as_json(column_type, literal, expected):
    con = duckdb.connect()
    con.execute(f"CREATE TABLE src (v {column_type})")
    con.execute(f"INSERT INTO src VALUES ({literal}), (NULL)")

    rows = con.execute(_read_sql(_registered(con, "src"), "src")).fetchall()

    assert isinstance(rows[0][0], str)  # JSON text: nothing for Provisa to convert
    assert json.loads(rows[0][0]) == expected
    assert rows[1][0] is None  # a NULL column stays NULL


@pytest.mark.parametrize("value", ["'nan'::DOUBLE", "'inf'::DOUBLE", "'-inf'::DOUBLE"])
def test_duckdb_refuses_a_float_element_that_is_not_a_finite_number(value):
    con = duckdb.connect()
    con.execute("CREATE TABLE src (v DOUBLE[])")
    con.execute(f"INSERT INTO src VALUES ([1.5, {value}])")

    with pytest.raises(duckdb.Error, match=NON_FINITE) as raised:
        con.execute(_read_sql(_registered(con, "src"), "src")).fetchall()

    # The backend's error is recognised by the rule's name and restated as the product's refusal,
    # which carries the code a client is answered with.
    from provisa.core.json_refusal import JsonRefused, non_finite_refusal

    refusal = non_finite_refusal(raised.value)
    assert isinstance(refusal, JsonRefused)
    assert (refusal.code, refusal.params) == ("json.non_finite_number", {})
    assert non_finite_refusal(RuntimeError("connection reset")) is None


def test_a_statement_with_no_native_nested_column_is_not_parsed_again():
    sql = 'SELECT "t"."id" FROM "orders" AS "t"'
    assert resolve_native_json(sql, "duckdb") is sql


def test_an_element_type_with_no_verified_form_is_refused_by_name():
    con = duckdb.connect()
    con.execute("CREATE TABLE src (v INTERVAL[])")
    nested = _registered(con, "src")

    with pytest.raises(NativeJsonUnsupported) as refused:
        _read_sql(nested, "src")

    assert refused.value.code == "query.json_operation_unsupported"
    assert refused.value.params == {
        "operation": "to_json",
        "backend": "duckdb",
        "column": "v",
        "native_type": "interval[]",
    }


def test_a_backend_with_no_to_json_form_is_refused_by_name():
    marker = native_json_marker(
        exp.column("v", "t", quoted=True), NestedColumn("array<string>", 1, "text")
    )
    with pytest.raises(NativeJsonUnsupported, match="bigquery has no verified way"):
        transpile(f'SELECT {marker.sql(dialect="postgres")} AS "v" FROM "src" AS "t"', "bigquery")
