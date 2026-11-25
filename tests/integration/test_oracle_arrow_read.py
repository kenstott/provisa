# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a1c27-93f5-4b0d-8d6e-2a71f5c90b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the Oracle driver reads a result as Arrow through python-oracledb's data-frame
fetch against a live Oracle Free (REQ-2194), with the types the statement declares, LOBs included,
materialized (``execute``) and streamed (``open_stream``); a type python-oracledb has no Arrow
form for is cast in the database (ROWID and UROWID to their text, JSON to its text typed as JSON,
XMLTYPE to its text), and an object type, a BFILE and a nested cursor are refused by name.

The harness provisions the oracle container (requires_oracle marker, docker-compose.test.yml,
gvenzl/oracle-free:slim)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

import oracledb  # pyright: ignore[reportMissingImports]
import pyarrow as pa
import pytest

from provisa.core.ir_types import interval_value
from provisa.executor.drivers.oracle import OracleDriver
from provisa.executor.drivers.oracle_arrow import OracleArrowUnsupported
from provisa.executor.result import ArrowResult

pytestmark = [pytest.mark.integration, pytest.mark.requires_oracle, pytest.mark.asyncio]

_SERVICE = "FREEPDB1"
_TABLE = "provisa_arrow_types"
_DROP = (
    f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {_TABLE}'; EXCEPTION WHEN OTHERS THEN "
    "IF SQLCODE != -942 THEN RAISE; END IF; END;"
)
_COLUMNS = (
    "amount NUMBER(10,2), qty NUMBER(9), score NUMBER, ratio BINARY_DOUBLE, "
    "born DATE, seen TIMESTAMP(9), at_ltz TIMESTAMP WITH LOCAL TIME ZONE, "
    "at_tz TIMESTAMP(6) WITH TIME ZONE, note CLOB, nnote NCLOB, photo BLOB, label VARCHAR2(20), "
    "span_ym INTERVAL YEAR TO MONTH, span_ds INTERVAL DAY TO SECOND"
)
_ROW = (
    "12.50, 42, 1.5, 2.25, DATE '2026-01-02', "
    "TIMESTAMP '2026-01-02 03:04:05.123456789', "
    "TIMESTAMP '2026-03-01 12:00:00 +00:00', "
    "TIMESTAMP '2026-03-01 12:00:00 -05:00', "
    "TO_CLOB('a long text'), TO_NCLOB('texte'), HEXTORAW('89504E47'), 'x', "
    "INTERVAL '1-2' YEAR TO MONTH, INTERVAL '3 04:05:06' DAY TO SECOND"
)


def _dsn() -> str:
    return oracledb.makedsn("localhost", int(os.environ["ORACLE_PORT"]), service_name=_SERVICE)


@pytest.fixture
async def driver():
    seed = oracledb.connect(user="system", password="provisa", dsn=_dsn())
    seed.autocommit = True
    cur = seed.cursor()
    cur.execute(_DROP)
    cur.execute(f"CREATE TABLE {_TABLE} (id NUMBER(9) PRIMARY KEY, {_COLUMNS})")
    cur.execute(f"INSERT INTO {_TABLE} VALUES (1, {_ROW})")
    nulls = ", ".join("NULL" for _ in _COLUMNS.split(", "))
    cur.execute(f"INSERT INTO {_TABLE} VALUES (2, {nulls})")
    drv = OracleDriver()
    await drv.connect(
        "localhost", int(os.environ["ORACLE_PORT"]), _SERVICE, "system", "provisa", max_pool=2
    )
    try:
        yield drv
    finally:
        await drv.close()
        cur.execute(_DROP)
        seed.close()


_SELECT = (
    f"SELECT amount, qty, score, ratio, born, seen, at_ltz, at_tz, note, nnote, photo, label, "
    f"span_ym, span_ds FROM {_TABLE} ORDER BY id"
)

_EXPECTED_TYPES = {
    "amount": pa.decimal128(10, 2),
    "qty": pa.decimal128(9, 0),
    "score": pa.float64(),
    "ratio": pa.float64(),
    "born": pa.timestamp("s"),
    "seen": pa.timestamp("ns"),
    "at_ltz": pa.timestamp("us", tz="UTC"),
    "at_tz": pa.timestamp("us", tz="UTC"),
    "note": pa.large_string(),
    "nnote": pa.large_string(),
    "photo": pa.large_binary(),
    "label": pa.large_string(),
    "span_ym": pa.month_day_nano_interval(),
    "span_ds": pa.month_day_nano_interval(),
}

_EXPECTED_ROWS = [
    (
        Decimal("12.50"),
        Decimal(42),
        1.5,
        2.25,
        datetime(2026, 1, 2),
        datetime(2026, 1, 2, 3, 4, 5, 123456),
        datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 1, 17, 0, tzinfo=timezone.utc),
        "a long text",
        "texte",
        b"\x89PNG",
        "x",
        interval_value(14, 0, 0),
        interval_value(0, 3, (4 * 3600 + 5 * 60 + 6) * 1_000_000),
    ),
    (None,) * 14,
]


async def test_a_result_is_read_as_arrow_with_its_declared_types(driver):
    result = await driver.execute(_SELECT)
    assert isinstance(result, ArrowResult)
    assert {f.name: f.type for f in result.table.schema} == _EXPECTED_TYPES
    assert result.rows == _EXPECTED_ROWS


async def test_a_stream_reads_the_same_rows_from_arrow_batches(driver):
    stream = await driver.open_stream(_SELECT)
    rows: list[tuple] = []
    try:
        while batch := await stream.fetch(1):
            rows.extend(batch)
    finally:
        await stream.close()
    assert stream.column_names == list(_EXPECTED_TYPES)
    assert rows == _EXPECTED_ROWS


async def test_a_column_null_by_describe_is_read(driver):
    result = await driver.execute("SELECT NULL AS nothing, 1 AS one FROM dual")
    assert result.column_names == ["nothing", "one"]
    assert result.rows == [(None, 1.0)]


_CAST_TABLE = "provisa_arrow_casts"
_POINT = "provisa_arrow_point"


def _drop(kind: str, name: str, missing: int) -> str:
    return (
        f"BEGIN EXECUTE IMMEDIATE 'DROP {kind} {name}'; EXCEPTION WHEN OTHERS THEN "
        f"IF SQLCODE != {missing} THEN RAISE; END IF; END;"
    )


@pytest.fixture
async def cast_driver(driver):
    seed = oracledb.connect(user="system", password="provisa", dsn=_dsn())
    seed.autocommit = True
    cur = seed.cursor()
    cur.execute(_drop("TABLE", _CAST_TABLE, -942))
    cur.execute(_drop("TYPE", _POINT, -4043))
    cur.execute(f"CREATE TYPE {_POINT} AS OBJECT (x NUMBER, y NUMBER)")
    cur.execute(
        f"CREATE TABLE {_CAST_TABLE} (id NUMBER(9) PRIMARY KEY, urid UROWID, doc JSON, "
        f"page XMLTYPE, at {_POINT})"
    )
    cur.execute(
        f"INSERT INTO {_CAST_TABLE} VALUES (1, NULL, JSON('{{\"a\":[1,2]}}'), "
        f"XMLTYPE('<p>x</p>'), {_POINT}(1, 2))"
    )
    cur.execute(f"UPDATE {_CAST_TABLE} SET urid = ROWID")
    cur.execute(f"INSERT INTO {_CAST_TABLE} VALUES (2, NULL, NULL, NULL, NULL)")
    cur.execute(f"SELECT ROWIDTOCHAR(ROWID) FROM {_CAST_TABLE} WHERE id = 1")
    (rowid,) = cur.fetchone()
    try:
        yield driver, rowid
    finally:
        cur.execute(_drop("TABLE", _CAST_TABLE, -942))
        cur.execute(_drop("TYPE", _POINT, -4043))
        seed.close()


_CAST_SELECT = f"SELECT ROWID AS rid, urid, doc, page FROM {_CAST_TABLE} ORDER BY id"


async def test_rowid_urowid_json_and_xmltype_are_cast_in_the_database_and_read(cast_driver):
    drv, rowid = cast_driver
    result = await drv.execute(_CAST_SELECT)
    assert isinstance(result, ArrowResult)
    assert result.column_names == ["rid", "urid", "doc", "page"]
    schema = result.table.schema
    assert pa.types.is_string(schema.field("rid").type) or pa.types.is_large_string(
        schema.field("rid").type
    )
    assert isinstance(schema.field("doc").type, pa.JsonType)  # typed as JSON
    assert result.rows[0][0] == rowid
    assert result.rows[0][1] == rowid
    assert result.rows[0][2] == '{"a":[1,2]}'
    assert result.rows[0][3].strip() == "<p>x</p>"
    assert result.rows[1][1:] == (None, None, None)


async def test_a_streamed_cast_column_reads_the_same_rows(cast_driver):
    drv, _ = cast_driver
    expected = (await drv.execute(_CAST_SELECT)).rows
    stream = await drv.open_stream(_CAST_SELECT)
    rows: list[tuple] = []
    try:
        while batch := await stream.fetch(1):
            rows.extend(batch)
    finally:
        await stream.close()
    assert rows == expected


async def test_an_object_type_column_is_refused_by_name(cast_driver):
    drv, _ = cast_driver
    with pytest.raises(OracleArrowUnsupported) as refused:
        await drv.execute(f"SELECT id, at FROM {_CAST_TABLE}")
    assert refused.value.column == "at"
    assert refused.value.type_name == "DB_TYPE_OBJECT"
    # The pool still serves the next statement.
    assert (await drv.execute(f"SELECT COUNT(*) AS n FROM {_CAST_TABLE}")).rows == [(2.0,)]


async def test_a_bfile_is_refused_by_name(driver):
    with pytest.raises(OracleArrowUnsupported) as refused:
        await driver.execute("SELECT BFILENAME('DATA_PUMP_DIR', 'none') AS f FROM dual")
    assert refused.value.column == "f"
    assert refused.value.type_name == "DB_TYPE_BFILE"
