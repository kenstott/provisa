# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2a9d47-3b18-4c05-a7f3-9d1e5b8c2a60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``interval`` is a registrable IR type with one canonical text form (ISO 8601 duration) on every
text surface; ``timetz``/``timestamptz`` resolve on every schema face; a raw ``bytea`` value renders
as Postgres's own hex text instead of crashing a JSON response (REQ-010, REQ-306, REQ-846)."""

# Requirements: REQ-010, REQ-306, REQ-846

from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace

import pytest
from dateutil.relativedelta import relativedelta
from google.protobuf.descriptor import FieldDescriptor

from provisa.compiler.type_map import (
    FILTER_TYPE_MAP,
    Interval,
    IntervalFilter,
    Time,
    column_type_to_graphql,
)
from provisa.core.ir_types import bytea_hex, iso8601_duration, to_ir, to_physical
from provisa.executor.serialize import _convert_value
from provisa.grpc.proto_gen import _physical_to_proto
from provisa.grpc.server import _proto_value


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (timedelta(days=3, seconds=4, microseconds=5), "P3DT4.000005S"),
        (timedelta(0), "PT0S"),
        (timedelta(days=2), "P2D"),
        (timedelta(hours=1, minutes=30), "PT1H30M"),
        (timedelta(microseconds=500_000), "PT0.5S"),
        (timedelta(seconds=-90), "-PT1M30S"),
        # A year-month interval keeps its months: a month has no fixed number of days.
        (relativedelta(years=1, months=2), "P1Y2M"),
        (relativedelta(months=-14), "-P1Y2M"),
        (
            relativedelta(months=14, days=3, hours=4, minutes=5, seconds=6, microseconds=500_000),
            "P1Y2M3DT4H5M6.5S",
        ),
        (relativedelta(months=1, days=-2), "P1M-2D"),
    ],
)
def test_an_interval_renders_as_its_iso_8601_duration(value, text):
    assert iso8601_duration(value) == text


def test_interval_and_timetz_resolve_in_the_ir():
    assert to_ir("interval") == "interval"
    assert to_ir("timetz") == "text"  # REQ-2171: held as its text, which keeps its offset
    assert to_physical("interval", "postgresql") == "INTERVAL"


def test_interval_is_its_own_graphql_scalar_with_an_ordered_filter():
    assert column_type_to_graphql("interval") is Interval
    assert FILTER_TYPE_MAP[Interval] is IntervalFilter
    assert set(IntervalFilter.fields) >= {"eq", "gt", "gte", "lt", "lte", "in", "is_null"}
    assert column_type_to_graphql("timetz") is Time


def test_the_interval_scalar_serializes_a_timedelta_and_keeps_engine_text():
    assert Interval.serialize(timedelta(days=1, seconds=2)) == "P1DT2S"
    assert Interval.serialize("P1DT2S") == "P1DT2S"
    with pytest.raises(TypeError, match="Interval cannot represent int"):
        Interval.serialize(5)


def test_json_results_render_interval_and_bytea_as_their_canonical_text():
    assert _convert_value(timedelta(days=3, seconds=4, microseconds=5)) == "P3DT4.000005S"
    assert _convert_value(b"\x00\x01\xff") == "\\x0001ff"
    assert _convert_value(memoryview(b"\xab")) == "\\xab"
    json.dumps([_convert_value(b"\x00"), _convert_value(timedelta(seconds=1))])


def test_an_interval_value_is_its_months_days_and_microseconds():
    from provisa.core.ir_types import interval_parts, interval_value

    assert interval_value(14, 0, 0) == relativedelta(years=1, months=2)
    assert interval_value(0, 3, 14_706_000_000) == timedelta(days=3, seconds=14706)
    assert interval_parts(relativedelta(years=1, months=2, days=3)) == (14, 3, 0)
    assert interval_parts(timedelta(microseconds=-5)) == (0, 0, -5)
    assert interval_parts(timedelta(days=-1, hours=2)) == (0, 0, -79_200_000_000)


def test_a_json_response_writes_an_interval_as_its_iso_8601_duration():
    """Not jsonable_encoder's total seconds for a timedelta, nor a year-month interval's fields."""
    from provisa.api.json_response import dumps

    body = {"ym": relativedelta(years=1, months=2), "ds": timedelta(days=3, seconds=14706)}
    assert json.loads(dumps(body)) == {"ym": "P1Y2M", "ds": "P3DT4H5M6S"}
    assert _convert_value(relativedelta(years=1, months=2)) == "P1Y2M"
    assert Interval.serialize(relativedelta(months=3)) == "P3M"


def test_an_interval_lands_as_text_a_store_reads_with_its_months():
    import duckdb

    from provisa.core.ir_arrow import arrow_value

    text = arrow_value(relativedelta(months=14, days=3, hours=4, minutes=5, seconds=6), "interval")
    assert text == "14 months 3 days 14706000000 microseconds"
    landed = duckdb.connect().execute("SELECT to_json(?::INTERVAL)", [text]).fetchone()
    assert landed == ('"1 year 2 months 3 days 04:05:06"',)


def test_an_oracle_year_to_month_interval_is_read_as_its_months():
    import oracledb  # pyright: ignore[reportMissingImports]

    from provisa.executor.drivers.zoned import _oracle_ym

    assert _oracle_ym(oracledb.IntervalYM(1, 2)) == relativedelta(years=1, months=2)
    assert _oracle_ym(oracledb.IntervalYM(-1, -2)) == relativedelta(months=-14)
    assert _oracle_ym(None) is None


@pytest.mark.parametrize(
    ("text", "value"),
    [
        (b"1 year 2 mons", relativedelta(years=1, months=2)),
        (b"3 days 04:05:06.5", timedelta(days=3, seconds=14706, microseconds=500_000)),
        (
            b"-1 years -2 mons +3 days -04:05:06",
            relativedelta(months=-14, days=3, hours=-4, minutes=-5, seconds=-6),
        ),
        (b"00:00:00", timedelta(0)),
    ],
)
def test_a_postgres_interval_is_read_with_its_months(text, value):
    from provisa.executor.drivers.postgresql import _PgIntervalLoader

    loader = _PgIntervalLoader.__new__(_PgIntervalLoader)
    assert _PgIntervalLoader._load_postgres(loader, text) == value


def test_a_postgres_binary_interval_is_read_with_its_months():
    import struct

    from provisa.executor.drivers.postgresql import _PgIntervalBinaryLoader

    raw = struct.pack("!qii", 14_706_000_000, 3, 14)
    assert _PgIntervalBinaryLoader(1186).load(raw) == relativedelta(
        months=14, days=3, hours=4, minutes=5, seconds=6
    )


def test_a_duckdb_result_reads_an_interval_with_its_months():
    import duckdb

    from provisa.executor.drivers.duckdb_driver import fetch_rows

    result = duckdb.connect().execute(
        "SELECT INTERVAL '1 year 2 months' AS ym, INTERVAL '3 days 04:05:06' AS ds, 'x' AS s "
        "UNION ALL SELECT NULL, NULL, NULL"
    )
    assert fetch_rows(result) == [
        (relativedelta(years=1, months=2), timedelta(days=3, seconds=14706), "x"),
        (None, None, None),
    ]


def test_an_arrow_interval_column_reads_as_ir_intervals():
    import pyarrow as pa

    from provisa.core.ir_arrow import column_values

    column = pa.array(
        [pa.MonthDayNano([14, 0, 0]), pa.MonthDayNano([0, 3, 14_706_000_000_000]), None],
        pa.month_day_nano_interval(),
    )
    assert column_values(column) == [
        relativedelta(years=1, months=2),
        timedelta(days=3, seconds=14706),
        None,
    ]
    assert column_values(pa.array(["x"])) == ["x"]


@pytest.mark.parametrize(
    ("value", "text", "binary"),
    [
        (relativedelta(years=1, months=2), "1 year 2 mons", (0, 0, 14)),
        (
            timedelta(days=3, seconds=14706, microseconds=500_000),
            "3 days 04:05:06.5",
            (14_706_500_000, 3, 0),
        ),
        (relativedelta(months=-14, days=1), "-1 years -2 mons 1 day", (0, 1, -14)),
        (timedelta(microseconds=-5), "-00:00:00.000005", (-5, 0, 0)),
        (timedelta(0), "00:00:00", (0, 0, 0)),
    ],
)
def test_pgwire_writes_an_interval_as_postgres_does(value, text, binary):
    import struct

    from buenavista.core import BVType
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    _, to_text, to_binary = BVTYPE_TO_PGTYPE[BVType.INTERVAL]
    assert to_text(value) == text
    assert struct.unpack("!qii", to_binary(value)) == binary


def test_bytea_hex_is_postgres_hex_output():
    assert bytea_hex(b"\x00\x01\xff") == "\\x0001ff"
    assert bytea_hex(bytearray()) == "\\x"


@pytest.mark.parametrize(
    ("column_type", "proto"),
    [
        ("timestamptz", "google.protobuf.Timestamp"),
        ("timetz", "string"),
        ("interval", "string"),
        ("bytea", "bytes"),
    ],
)
def test_proto_maps_every_postgres_temporal_and_binary_alias(column_type, proto):
    assert _physical_to_proto(column_type) == proto


def test_grpc_renders_an_interval_string_field_as_iso_8601():
    field = SimpleNamespace(type=FieldDescriptor.TYPE_STRING)
    assert _proto_value(field, timedelta(days=3, seconds=4, microseconds=5)) == "P3DT4.000005S"


@pytest.mark.parametrize("scalar_name", ["Date", "DateTime", "Interval"])
@pytest.mark.parametrize("number", [1700000000, 1.5, True])
def test_a_temporal_scalar_never_serializes_a_number(scalar_name, number):
    """Dates, times and durations are ISO 8601 strings in every GraphQL response — a number (an
    epoch offset in unknown units) is refused, never passed through or stringified."""
    from provisa.compiler import type_map

    with pytest.raises(TypeError, match=f"{scalar_name} cannot represent"):
        getattr(type_map, scalar_name).serialize(number)


def test_date_and_datetime_serialize_as_iso_8601():
    from datetime import date, datetime, timezone

    from provisa.compiler.type_map import Date, DateTime

    assert DateTime.serialize(datetime(2026, 1, 2, 3, 4, 5)) == "2026-01-02T03:04:05"
    assert (
        DateTime.serialize(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
        == "2026-01-02T03:04:05+00:00"
    )
    assert Date.serialize(date(2026, 1, 2)) == "2026-01-02"
    assert DateTime.serialize("2026-01-02T03:04:05") == "2026-01-02T03:04:05"
