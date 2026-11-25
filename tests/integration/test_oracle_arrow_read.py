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
materialized (``execute``) and streamed (``open_stream``); a type with no Arrow form is refused by
name.

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


async def test_a_type_without_an_arrow_form_is_refused_by_name(driver):
    with pytest.raises(OracleArrowUnsupported) as refused:
        await driver.execute(f"SELECT id, ROWID AS rid FROM {_TABLE}")
    assert refused.value.column == "rid"
    # The pool still serves the next statement.
    assert (await driver.execute(f"SELECT COUNT(*) AS n FROM {_TABLE}")).rows == [(2.0,)]
