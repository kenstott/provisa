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
    execute_instants,
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


def test_oracle_reads_a_local_time_zone_value_in_the_sessions_utc():
    """A local time zone value is given in the session's zone, UTC. A zoned value is cast to one in
    the database (execute_instants), so the fetch asks for nothing of it; nor of any other type."""
    import oracledb

    class _Cursor:
        arraysize = 100

        def var(self, db_type, *, arraysize, outconverter):
            return (db_type, arraysize, outconverter)

    class _Meta:
        def __init__(self, type_code):
            self.type_code = type_code

    assert _oracle_output_type(_Cursor(), _Meta(oracledb.DB_TYPE_TIMESTAMP_LTZ)) == (
        oracledb.DB_TYPE_TIMESTAMP_LTZ,
        100,
        _oracle_ltz,
    )
    for plain in (
        oracledb.DB_TYPE_TIMESTAMP_TZ,
        oracledb.DB_TYPE_TIMESTAMP,
        oracledb.DB_TYPE_DATE,
        oracledb.DB_TYPE_VARCHAR,
    ):
        assert _oracle_output_type(_Cursor(), _Meta(plain)) is None


class _Described:
    def __init__(self, name, type_code):
        self.name, self.type_code = name, type_code


class _StatementCursor:
    """Parses and runs statements, recording each; a parse describes the result's columns."""

    def __init__(self, description):
        self.parsed: list[str] = []
        self.ran: list[str] = []
        self._description = description
        self.description = None

    def parse(self, sql):
        self.parsed.append(sql)
        self.description = self._description

    def execute(self, sql, bound, **kw):
        assert kw == {"fetch_lobs": False}
        assert self.parsed, "described before it runs"
        self.ran.append(sql)


def test_a_zoned_column_is_cast_to_its_instant_in_the_database():
    """python-oracledb hands a TIMESTAMP WITH TIME ZONE on as its wall time, its offset dropped,
    whatever the fetch asks: the statement, parsed first, is run once, with the column cast to
    TIMESTAMP WITH LOCAL TIME ZONE, which the UTC session gives as the instant. Its names stay the
    statement's."""
    import oracledb

    sql = "SELECT TIMESTAMP '2024-01-02 03:04:05 +02:00', n FROM dual"
    cur = _StatementCursor(
        [
            _Described("TIMESTAMP'2024-01-0203:04:05+02:00'", oracledb.DB_TYPE_TIMESTAMP_TZ),
            _Described("N", oracledb.DB_TYPE_NUMBER),
        ]
    )
    assert execute_instants(cur, sql, [], fetch_lobs=False) == [
        "timestamp'2024-01-0203:04:05+02:00'",
        "n",
    ]
    assert cur.parsed == [sql]
    assert cur.ran == [  # one execution: the cast statement alone
        f"WITH provisa_zoned (c0, c1) AS ({sql}) SELECT "
        "CAST(c0 AS TIMESTAMP WITH LOCAL TIME ZONE) AS c0, c1 FROM provisa_zoned",
    ]


def test_a_statement_with_no_zoned_column_runs_as_given_once():
    import oracledb

    cur = _StatementCursor([_Described("N", oracledb.DB_TYPE_NUMBER)])
    assert execute_instants(cur, "SELECT n FROM t", [], fetch_lobs=False) == ["n"]
    assert cur.ran == ["SELECT n FROM t"]
    writes = _StatementCursor(None)
    assert execute_instants(writes, "UPDATE t SET n = 1", [], fetch_lobs=False) is None
    assert writes.ran == ["UPDATE t SET n = 1"]


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
