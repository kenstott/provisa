# Copyright (c) 2026 Kenneth Stott
# Canary: 6e54ec52-ccbf-4e87-aa16-3d58243645bf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2050: a zoned timestamp is read as the instant it is -- by the readers that would lose its
offset (SQL Server's datetimeoffset, Oracle's TIMESTAMP WITH [LOCAL] TIME ZONE) and through every
Arrow path, where it is UTC with its zone marked."""

# Requirements: REQ-2050

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow as pa

from provisa.executor.drivers.zoned import (
    SQL_SS_TIMESTAMPOFFSET,
    _DATETIMEOFFSET,
    _oracle_ltz,
    _oracle_output_type,
    datetimeoffset,
    mssql_connection,
)

_UTC = datetime(2024, 1, 2, 1, 4, 5, 123456, tzinfo=timezone.utc)


def test_a_datetimeoffset_is_its_instant_in_utc():
    raw = _DATETIMEOFFSET.pack(2024, 1, 2, 3, 4, 5, 123456000, 2, 0)
    assert datetimeoffset(raw) == _UTC
    behind = _DATETIMEOFFSET.pack(2024, 1, 1, 20, 34, 5, 123456000, -4, -30)
    assert datetimeoffset(behind) == _UTC
    assert datetimeoffset(None) is None


def test_a_sql_server_connection_reads_datetimeoffset_through_the_converter():
    class _Conn:
        def __init__(self):
            self.converters = {}

        def add_output_converter(self, sql_type, func):
            self.converters[sql_type] = func

    conn = mssql_connection(_Conn())
    assert conn.converters == {SQL_SS_TIMESTAMPOFFSET: datetimeoffset}


def test_oracle_zoned_values_are_their_instants_in_utc():
    # A local time zone value is read in the session's zone, which is UTC.
    assert _oracle_ltz(datetime(2024, 1, 2, 1, 4, 5, 123456)) == _UTC
    assert _oracle_ltz(None) is None


def test_oracle_fetches_both_zoned_types_as_a_local_time_zone_value():
    """python-oracledb's Thin mode ignores the session's NLS formats, so a TIMESTAMP WITH TIME
    ZONE fetched as text lost its offset; fetched as a local time zone value, the database gives
    the instant in the session's zone, UTC. Every other type is fetched as it is."""
    import oracledb

    class _Cursor:
        arraysize = 100

        def var(self, db_type, *, arraysize, outconverter):
            return (db_type, arraysize, outconverter)

    class _Meta:
        def __init__(self, type_code):
            self.type_code = type_code

    for zoned in (oracledb.DB_TYPE_TIMESTAMP_TZ, oracledb.DB_TYPE_TIMESTAMP_LTZ):
        assert _oracle_output_type(_Cursor(), _Meta(zoned)) == (
            oracledb.DB_TYPE_TIMESTAMP_LTZ,
            100,
            _oracle_ltz,
        )
    for plain in (oracledb.DB_TYPE_TIMESTAMP, oracledb.DB_TYPE_DATE, oracledb.DB_TYPE_VARCHAR):
        assert _oracle_output_type(_Cursor(), _Meta(plain)) is None


def test_an_instant_lands_and_reads_as_utc_with_its_zone_marked():
    from provisa.core.catalog_types import column_arrow_type
    from provisa.core.ir_arrow import arrow_type, arrow_value

    assert arrow_type("timestamptz") == pa.timestamp("us", tz="UTC")
    assert column_arrow_type("timestamptz") == pa.timestamp("us", tz="UTC")
    assert arrow_type("timestamp") == pa.timestamp("us")
    # Text with an offset, an aware value, and a value with no zone (UTC) are the same instant.
    for given in (
        "2024-01-02T03:04:05.123456+02:00",
        _UTC.astimezone(timezone(timedelta(hours=-5))),
        datetime(2024, 1, 2, 1, 4, 5, 123456),
    ):
        assert arrow_value(given, "timestamptz") == _UTC
    column = pa.array(
        [arrow_value("2024-01-02T03:04:05.123456+02:00", "timestamptz")], arrow_type("timestamptz")
    )
    assert column.to_pylist() == [_UTC]
