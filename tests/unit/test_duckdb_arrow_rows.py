# Copyright (c) 2026 Kenneth Stott
# Canary: 51c0e8b2-7d4a-4f19-b36e-0a2d9c8e4f71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""DuckDB's reads come from its Arrow reader (REQ-2194); the rows a row consumer is given from
those batches are the values DuckDB's own rows hold, at every depth, except the interval and the
infinite date or timestamp, which DuckDB's rows get wrong."""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import pytest
from dateutil.relativedelta import relativedelta

from provisa.core.ir_types import infinity_of
from provisa.executor.drivers.duckdb_arrow import LOSSLESS_ARROW, arrow_rows

_VALUES = {
    "boolean": "true",
    "tinyint": "-1::TINYINT",
    "integer": "1::INTEGER",
    "ubigint": "18446744073709551615::UBIGINT",
    "hugeint": "-170141183460469231731687303715884105727::HUGEINT",
    "uhugeint": "340282366920938463463374607431768211455::UHUGEINT",
    "varint": "-123456789012345678901234567890::VARINT",
    "varint_pos": "255::VARINT",
    "varint_zero": "0::VARINT",
    "real": "1.5::FLOAT",
    "double": "2.25::DOUBLE",
    "decimal": "1.25::DECIMAL(10,2)",
    "decimal38": "-1.25::DECIMAL(38,10)",
    "varchar": "'a'",
    "blob": "'\\xAA\\x00'::BLOB",
    "date": "DATE '2024-01-02'",
    "time": "TIME '01:02:03.123456'",
    "timetz": "'01:02:03.25-07:30'::TIMETZ",
    "timetz_east": "'23:59:59+15:59'::TIMETZ",
    "timestamp": "TIMESTAMP '2024-01-02 01:02:03.123456'",
    "timestamp_s": "'2024-01-02 01:02:03'::TIMESTAMP_S",
    "timestamp_ms": "'2024-01-02 01:02:03.123'::TIMESTAMP_MS",
    "timestamp_ns": "'2024-01-02 01:02:03.123456789'::TIMESTAMP_NS",
    "timestamptz": "TIMESTAMPTZ '2024-01-02 01:02:03.123456+00'",
    "uuid": "'7d9f1c2e-1111-2222-3333-444455556666'::UUID",
    "json": "'{\"a\": 1}'::JSON",
    "enum": "'x'::ENUM('x', 'y')",
    "array": "[1, 2]::INTEGER[2]",
    "map": "MAP {'k': 1}",
    "bit": "'101010101'::BIT",
    "bit_byte": "'00000000'::BIT",
    "union": "union_value(n := 1)",
    "null": "NULL::INTEGER",
}

_SHAPES = {
    "top": "{v}",
    "list": "[{v}, NULL]",
    "struct": "{{'f': {v}}}",
    "map_value": "MAP {{'k': {v}}}",
    "list_of_struct": "[{{'f': [{v}]}}]",
}


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    yield c
    c.close()


def _through_arrow(con, sql: str) -> list[tuple]:
    cur = con.cursor()
    try:
        cur.execute(LOSSLESS_ARROW)
        cur.execute(sql)
        return arrow_rows(cur.to_arrow_reader(1024).read_all())
    finally:
        cur.close()


@pytest.mark.parametrize("shape", list(_SHAPES))
@pytest.mark.parametrize("name", list(_VALUES))
def test_rows_from_arrow_are_duckdbs_own_rows(con, name, shape):
    sql = f"SELECT {_SHAPES[shape].format(v=_VALUES[name])} AS c, 7 AS n"
    expected = con.execute(sql).fetchall()
    got = _through_arrow(con, sql)
    assert got == expected
    assert repr(got) == repr(expected)  # same Python types too, not just equal values


def test_a_batch_of_many_rows_reads_each_row(con):
    sql = "SELECT i, i::VARCHAR AS s, i * 0.5 AS d FROM range(5000) t(i)"
    assert _through_arrow(con, sql) == con.execute(sql).fetchall()


@pytest.mark.parametrize("shape", ["{v}", "[{v}]", "{{'f': {v}}}"])
def test_an_interval_keeps_its_months_at_every_depth(con, shape):
    got = _through_arrow(
        con, f"SELECT {shape.format(v=chr(39) + '1 year 2 months' + chr(39) + '::INTERVAL')} AS c"
    )
    [[value]] = got
    inner = value if shape == "{v}" else value[0] if shape == "[{v}]" else value["f"]
    assert inner == relativedelta(years=1, months=2)
    [[plain]] = _through_arrow(con, "SELECT INTERVAL '3 days' AS c")
    assert plain == timedelta(days=3)


@pytest.mark.parametrize(
    "expr, sign",
    [
        ("'infinity'::DATE", 1),
        ("'-infinity'::DATE", -1),
        ("'infinity'::TIMESTAMP", 1),
        ("'-infinity'::TIMESTAMPTZ", -1),
    ],
)
@pytest.mark.parametrize("shape", ["{v}", "[{v}]", "{{'f': {v}}}"])
def test_an_infinite_date_or_timestamp_is_infinite_at_every_depth(con, expr, sign, shape):
    [[value]] = _through_arrow(con, f"SELECT {shape.format(v=expr)} AS c")
    inner = value if shape == "{v}" else value[0] if shape == "[{v}]" else value["f"]
    assert infinity_of(inner) == sign


def test_the_last_date_a_table_holds_is_not_infinite(con):
    [[value]] = _through_arrow(con, "SELECT [DATE '9999-12-31'] AS c")
    assert value == [date(9999, 12, 31)] and infinity_of(value[0]) == 0


def test_the_lossless_export_stays_on_the_reading_cursor(con):
    cur = con.cursor()
    cur.execute(LOSSLESS_ARROW)
    assert con.execute("SELECT current_setting('arrow_lossless_conversion')").fetchone() == (False,)
    cur.close()
