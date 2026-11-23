# Copyright (c) 2026 Kenneth Stott
# Canary: 0d5e1d13-711c-4fa7-bc9e-e1caa4a9bae3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Response-cache entries are Arrow IPC and lossless (REQ-1896, REQ-1867 amended 2026-11-23).

Every value type the retired msgpack codec carried must come back from a hit as the value its miss
answered, of the same Python type: Decimal with its own scale, timestamps naive and aware
(including PostgreSQL's infinities), dates (including the infinities), times, intervals with and
without months, bytea, arrays, wide integers and NULL. An engine-Arrow entry comes back as the
very batches the miss produced: schema, types and the miss's own schema metadata.
"""

from __future__ import annotations

import datetime as dt
import decimal

import pyarrow as pa
import pytest
from dateutil.relativedelta import relativedelta

from provisa.cache.raw_sql import (
    KIND_ARROW,
    decode_entry,
    encode_arrow_entry,
    encode_rows_entry,
    entry_as_result,
)
from provisa.core.ir_arrow import _same
from provisa.core.ir_types import infinite_date, infinite_timestamp

UTC = dt.timezone.utc
EAST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def _round_trip(column: list, declared: str | None = None) -> list:
    rows = [(v,) for v in column]
    entry = decode_entry(encode_rows_entry(rows, ["c"], [declared] if declared else None))
    assert entry.kind == KIND_ARROW
    return [r[0] for r in entry_as_result(entry).rows]


def _assert_same(back: list, values: list) -> None:
    assert len(back) == len(values)
    for b, v in zip(back, values, strict=True):
        assert _same(b, v), f"{b!r} came back for {v!r}"
        if v is not None:
            assert type(b) is type(v), f"{type(b)} came back for {type(v)}"


@pytest.mark.parametrize(
    ("values", "declared"),
    [
        ([decimal.Decimal("19.99"), decimal.Decimal("-0.01"), None], "DECIMAL(10,2)"),
        ([decimal.Decimal("100000000000000000.123456789")], "NUMERIC"),
        ([decimal.Decimal("1.5"), decimal.Decimal("2.25")], "NUMERIC"),  # mixed scales
        ([dt.datetime(2026, 9, 28, 13, 45, 0, 123456), None], "TIMESTAMP"),
        ([dt.datetime(2026, 9, 28, 13, 45, tzinfo=UTC)], "TIMESTAMPTZ"),
        ([dt.datetime(2026, 9, 28, 13, 45, tzinfo=EAST)], "TIMESTAMPTZ"),
        (
            [
                dt.datetime(2026, 1, 1),
                infinite_timestamp(1, aware=False),
                infinite_timestamp(-1, aware=False),
            ],
            "TIMESTAMP",
        ),
        (
            [dt.datetime(2026, 1, 1, tzinfo=UTC), infinite_timestamp(1, aware=True)],
            "TIMESTAMPTZ",
        ),
        ([dt.date(2026, 9, 28), None], "DATE"),
        ([dt.date(2026, 1, 1), infinite_date(1), infinite_date(-1)], "DATE"),
        ([dt.time(23, 59, 59, 5)], "TIME"),
        ([dt.timedelta(days=3, seconds=7, microseconds=9)], "INTERVAL"),
        ([relativedelta(months=14, days=2, microseconds=5)], "INTERVAL"),
        ([b"\x00\x01\xff binary", None], "BYTEA"),
        ([[1, 2, 3], None, []], "INTEGER[]"),
        ([["a", None, "c"]], "VARCHAR[]"),
        ([2**53 + 1, -1, None], "BIGINT"),
        ([None, None], None),
        (["x", None], "VARCHAR"),
        ([True, False, None], "BOOLEAN"),
    ],
)
def test_rows_entry_round_trips_every_type(values, declared):
    _assert_same(_round_trip(values, declared), values)


def test_date_and_datetime_do_not_collide():
    rows = [(dt.date(2026, 1, 1), dt.datetime(2026, 1, 1, 0, 0, 0))]
    back = entry_as_result(decode_entry(encode_rows_entry(rows, ["d", "t"], None))).rows
    assert type(back[0][0]) is dt.date
    assert type(back[0][1]) is dt.datetime


def test_column_types_travel_in_the_entry():
    rows = [(decimal.Decimal("1"), b"x")]
    entry = decode_entry(encode_rows_entry(rows, ["price", "blob"], ["numeric", "bytea"]))
    result = entry_as_result(entry)
    assert result.column_names == ["price", "blob"]
    assert result.column_types == ["numeric", "bytea"]


def test_engine_arrow_entry_is_the_miss_batches_verbatim():
    schema = pa.schema(
        [
            pa.field("amount", pa.decimal128(18, 2)),
            pa.field("at", pa.timestamp("us", tz="UTC")),
            pa.field("gap", pa.month_day_nano_interval()),
            pa.field("blob", pa.binary()),
            pa.field("tags", pa.list_(pa.string())),
        ],
        metadata={b"engine": b"duckdb"},
    )
    batches = [
        pa.RecordBatch.from_pylist(
            [
                {
                    "amount": decimal.Decimal("12.34"),
                    "at": dt.datetime(2026, 1, 1, tzinfo=UTC),
                    "gap": pa.scalar((1, 2, 3000), pa.month_day_nano_interval()).as_py(),
                    "blob": b"\x00",
                    "tags": ["a"],
                }
            ]
            * 3,
            schema=schema,
        )
        for _ in range(2)
    ]
    entry = decode_entry(encode_arrow_entry(schema, batches, ["DECIMAL(18,2)"] * 5))
    assert entry.table.schema.equals(schema, check_metadata=True)
    assert entry.table.equals(pa.Table.from_batches(batches))
    result = entry_as_result(entry)
    assert result.column_types == ["DECIMAL(18,2)"] * 5
    assert pa.Table.from_batches(list(result.arrow_batches())).equals(entry.table)


def test_entry_is_zstd_compressed_arrow_ipc():
    rows = [(i, "same text in every row") for i in range(10_000)]
    data = encode_rows_entry(rows, ["n", "t"], ["BIGINT", "VARCHAR"])
    assert pa.ipc.open_stream(data).read_all().num_rows == 10_000
    uncompressed = pa.BufferOutputStream()
    table = pa.ipc.open_stream(data).read_all()
    with pa.ipc.new_stream(uncompressed, table.schema) as w:
        w.write_table(table)
    assert len(data) < uncompressed.getvalue().size / 2


def test_an_entry_without_a_kind_is_refused():
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, pa.schema([pa.field("n", pa.int64())])) as w:
        w.write_batch(pa.RecordBatch.from_pylist([{"n": 1}]))
    with pytest.raises(ValueError, match="no kind"):
        decode_entry(sink.getvalue().to_pybytes())
