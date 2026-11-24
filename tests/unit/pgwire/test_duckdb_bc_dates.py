# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e2d71-3c8a-4f56-a0e7-61d5c2b8f934
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DATE OR TIMESTAMP BC ON DUCKDB (REQ-2236, amended 2026-11-23): DuckDB holds 4713 BC, so
PostgreSQL's ``'YYYY-MM-DD[ hh:mm:ss[+zz]] BC'`` is read as that year and written back as
PostgreSQL writes it. Each statement is the one the functions measure runs (J3 of the DuckDB edge
classification, Conformance (functions) run 45170564119), each answer PostgreSQL 17's in a session
of TimeZone UTC."""

# Requirements: REQ-2236, REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_D = "'4713-01-01 BC'::date"
_TS = "'4713-01-01 00:00:00 BC'::timestamp without time zone"
_TZ = "'4713-01-01 00:00:00+00 BC'::timestamp with time zone"
_DAY = "'2024-06-15'::date"
_AT = "'2024-06-15 12:30:45'::timestamp without time zone"
_ATZ = "'2024-06-15 12:30:45+00'::timestamp with time zone"
_IVL = "'1 day 02:03:04'::interval"
#: The day PostgreSQL 17 answered age(_TS): its one-argument age reads CURRENT_DATE.
_CAPTURED = "'2026-11-23 00:00:00'::timestamp without time zone"
_TIME = "'12:30:45'::time without time zone"
_TIMETZ = "'12:30:45+02'::time with time zone"
#: The microseconds from 2000-01-01 of 4713-01-01 00:00:00 BC, and its days.
_US = -211810204800000000
_SEND_US = "\\xfd0f7fbdaf17e000"
_SEND_DAYS = "\\xffda97cd"


def _call(name: str, *args: str) -> str:
    return f'SELECT pg_catalog."{name}"({", ".join(args)}) AS v'


def _of_two(name: str, value: str) -> str:
    return f'SELECT pg_catalog."{name}"(c1) AS v FROM (VALUES ({value}), ({value})) AS t(c1)'


#: statement -> the text PostgreSQL 17 answers.
ANSWERED: dict[str, str] = {
    _call("age", _CAPTURED, _TS): "6738 years 10 mons 22 days",
    _call("age", _TS, _AT): "-6736 years -5 mons -14 days -12:30:45",
    _call("date", _TZ): "4713-01-01 BC",
    _call("date", _TS): "4713-01-01 BC",
    _call("date_add", _TZ, _IVL): "4713-01-02 02:03:04+00 BC",
    _call("date_bin", "'15 minutes'::interval", _TS, _AT): "4714-12-31 23:45:45 BC",
    _call("date_cmp", _D, _DAY): "-1",
    _call("date_cmp_timestamp", _DAY, _TS): "1",
    _call("date_eq", _D, _DAY): "f",
    _call("date_eq_timestamp", _D, _AT): "f",
    _call("date_ge", _D, _DAY): "f",
    _call("date_ge_timestamp", _DAY, _TS): "t",
    _call("date_gt", _D, _DAY): "f",
    _call("date_gt_timestamp", _DAY, _TS): "t",
    _call("date_larger", _D, _DAY): "2024-06-15",
    _call("date_le", _D, _DAY): "t",
    _call("date_le_timestamp", _DAY, _TS): "f",
    _call("date_lt", _D, _DAY): "t",
    _call("date_lt_timestamp", _DAY, _TS): "f",
    _call("date_mi_interval", _D, _IVL): "4714-12-30 21:56:56 BC",
    _call("date_ne", _D, _DAY): "t",
    _call("date_ne_timestamp", _D, _AT): "t",
    _call("date_out", _D): "4713-01-01 BC",
    _call("date_part", "'year'::text", _TS): "-4713",
    _call("date_pl_interval", _D, _IVL): "4713-01-02 02:03:04 BC",
    _call("date_send", _D): _SEND_DAYS,
    _call("date_smaller", _D, _DAY): "4713-01-01 BC",
    _call("date_subtract", _TZ, _IVL): "4714-12-30 21:56:56+00 BC",
    _call("date_trunc", "'day'::text", _TS): "4713-01-01 00:00:00 BC",
    _call("daterange", _D, _DAY): '["4713-01-01 BC",2024-06-15)',
    _call("daterange", _D, _DAY, "'[)'::text"): '["4713-01-01 BC",2024-06-15)',
    _call("datetime_pl", _D, _TIME): "4713-01-01 12:30:45 BC",
    _call("datetimetz_pl", _D, _TIMETZ): "4713-01-01 10:30:45+00 BC",
    _call("extract", "'year'::text", _TS): "-4713",
    _call("in_range", _TS, _AT, _IVL, "true::boolean", "true::boolean"): "t",
    _call("interval_pl_date", _IVL, _D): "4713-01-02 02:03:04 BC",
    _call("interval_pl_timestamp", _IVL, _TS): "4713-01-02 02:03:04 BC",
    _call("interval_pl_timestamptz", _IVL, _TZ): "4713-01-02 02:03:04+00 BC",
    _of_two("max", _D): "4713-01-01 BC",
    _of_two("max", _TZ): "4713-01-01 00:00:00+00 BC",
    _of_two("max", _TS): "4713-01-01 00:00:00 BC",
    _of_two("min", _D): "4713-01-01 BC",
    _of_two("min", _TZ): "4713-01-01 00:00:00+00 BC",
    _of_two("min", _TS): "4713-01-01 00:00:00 BC",
    _call("overlaps", _TZ, _IVL, _ATZ, _IVL): "f",
    _call("overlaps", _TZ, _IVL, _ATZ, _ATZ): "f",
    _call("overlaps", _TZ, _ATZ, _ATZ, _IVL): "f",
    _call("overlaps", _TZ, _ATZ, _ATZ, _ATZ): "f",
    _call("overlaps", _AT, _IVL, _TS, _AT): "f",
    _call("overlaps", _TS, _AT, _AT, _IVL): "f",
    _call("overlaps", _TS, _AT, _AT, _AT): "f",
    _call("time", _TZ): "00:00:00",
    _call("timedate_pl", _TIME, _D): "4713-01-01 12:30:45 BC",
    _call("timestamp", _D, _TIME): "4713-01-01 12:30:45 BC",
    _call("timestamp", _TS, "3::integer"): "4713-01-01 00:00:00 BC",
    _call("timestamp_cmp", _TS, _AT): "-1",
    _call("timestamp_cmp_date", _TS, _DAY): "-1",
    _call("timestamp_cmp_timestamptz", _TS, _ATZ): "-1",
    _call("timestamp_eq_date", _AT, _D): "f",
    _call("timestamp_ge", _TS, _AT): "f",
    _call("timestamp_ge_date", _TS, _DAY): "f",
    _call("timestamp_ge_timestamptz", _TS, _ATZ): "f",
    _call("timestamp_gt", _TS, _AT): "f",
    _call("timestamp_gt_date", _TS, _DAY): "f",
    _call("timestamp_gt_timestamptz", _TS, _ATZ): "f",
    _call("timestamp_larger", _TS, _AT): "2024-06-15 12:30:45",
    _call("timestamp_le", _TS, _AT): "t",
    _call("timestamp_le_date", _TS, _DAY): "t",
    _call("timestamp_le_timestamptz", _TS, _ATZ): "t",
    _call("timestamp_lt", _TS, _AT): "t",
    _call("timestamp_lt_date", _TS, _DAY): "t",
    _call("timestamp_lt_timestamptz", _TS, _ATZ): "t",
    _call("timestamp_mi", _TS, _AT): "-2460439 days -12:30:45",
    _call("timestamp_mi_interval", _TS, _IVL): "4714-12-30 21:56:56 BC",
    _call("timestamp_ne_date", _AT, _D): "t",
    _call("timestamp_out", _TS): "4713-01-01 00:00:00 BC",
    _call("timestamp_pl_interval", _TS, _IVL): "4713-01-02 02:03:04 BC",
    _call("timestamp_send", _TS): _SEND_US,
    _call("timestamp_smaller", _TS, _AT): "4713-01-01 00:00:00 BC",
    _call("timestamptz", _D): "4713-01-01 00:00:00+00 BC",
    _call("timestamptz", _TS): "4713-01-01 00:00:00+00 BC",
    _call("timestamptz_cmp_timestamp", _ATZ, _TS): "1",
    _call("timestamptz_ge_timestamp", _ATZ, _TS): "t",
    _call("timestamptz_gt_timestamp", _ATZ, _TS): "t",
    _call("timestamptz_le_timestamp", _ATZ, _TS): "f",
    _call("timestamptz_lt_timestamp", _ATZ, _TS): "f",
    _call("timestamptz_mi", _TZ, _ATZ): "-2460439 days -12:30:45",
    _call("timestamptz_mi_interval", _TZ, _IVL): "4714-12-30 21:56:56+00 BC",
    _call("timestamptz_out", _TZ): "4713-01-01 00:00:00+00 BC",
    _call("timestamptz_pl_interval", _TZ, _IVL): "4713-01-02 02:03:04+00 BC",
    _call("timestamptz_send", _TZ): _SEND_US,
    _call("timetz", _TZ): "00:00:00+00",
    _call("timetzdate_pl", _TIMETZ, _D): "4713-01-01 10:30:45+00 BC",
    _call("timezone", "'1 hour'::interval", _TZ): "4713-01-01 01:00:00 BC",
    _call("timezone", "'1 hour'::interval", _TS): "4714-12-31 23:00:00+00 BC",
    _call("timezone", "'UTC'::text", _TS): "4713-01-01 00:00:00+00 BC",
    _call("timezone", _TS): "4713-01-01 00:00:00+00 BC",
    _call("tsrange", _TS, _AT): '["4713-01-01 00:00:00 BC","2024-06-15 12:30:45")',
    _call("tstzrange", _TZ, _ATZ): '["4713-01-01 00:00:00+00 BC","2024-06-15 12:30:45+00")',
    "SELECT pg_input_is_valid('4713-01-01 BC', 'date') AS v": "t",
    "SELECT pg_input_is_valid('4713-01-01 00:00:00 BC', 'timestamp') AS v": "t",
    "SELECT pg_input_is_valid('4713-01-01 00:00:00+00 BC', 'timestamptz') AS v": "t",
}


#: The calls whose limitation on DuckDB (polyfill/measured.py BOUNDED) listed a value BC, which
#: they now answer, as PostgreSQL 17 does.
ANSWERED.update(
    {
        _call("date_mii", _D, "3::integer"): "4714-12-29 BC",
        _call("date_pli", _D, "3::integer"): "4713-01-04 BC",
        _call("integer_pl_date", "3::integer", _D): "4713-01-04 BC",
        _call("date_trunc", "'day'::text", _TZ): "4713-01-01 00:00:00+00 BC",
        _call("in_range", _TZ, _ATZ, _IVL, "true::boolean", "true::boolean"): "t",
        _call("timestamp", _TZ): "4713-01-01 00:00:00 BC",
        _call("timestamptz", _TZ, "3::integer"): "4713-01-01 00:00:00+00 BC",
        _call("timestamptz_cmp", _TZ, _ATZ): "-1",
        _call("timestamptz_eq", _TZ, _ATZ): "f",
        _call("timestamptz_ge", _TZ, _ATZ): "f",
        _call("timestamptz_gt", _TZ, _ATZ): "f",
        _call("timestamptz_le", _TZ, _ATZ): "t",
        _call("timestamptz_lt", _TZ, _ATZ): "t",
        _call("timestamptz_ne", _TZ, _ATZ): "t",
        _call("timestamptz_larger", _TZ, _ATZ): "2024-06-15 12:30:45+00",
        _call("timestamptz_smaller", _TZ, _ATZ): "4713-01-01 00:00:00+00 BC",
        _call("timestamptz_cmp_date", _TZ, _DAY): "-1",
        _call("timestamptz_eq_date", _TZ, _DAY): "f",
        _call("timestamptz_ge_date", _TZ, _DAY): "f",
        _call("timestamptz_gt_date", _TZ, _DAY): "f",
        _call("timestamptz_le_date", _TZ, _DAY): "t",
        _call("timestamptz_lt_date", _TZ, _DAY): "t",
        _call("timestamptz_ne_date", _TZ, _DAY): "t",
        _call("date_cmp_timestamptz", _D, _ATZ): "-1",
        _call("date_eq_timestamptz", _D, _ATZ): "f",
        _call("date_ge_timestamptz", _D, _ATZ): "f",
        _call("date_gt_timestamptz", _D, _ATZ): "f",
        _call("date_le_timestamptz", _D, _ATZ): "t",
        _call("date_lt_timestamptz", _D, _ATZ): "t",
        _call("date_ne_timestamptz", _D, _ATZ): "t",
        _call("timestamptz_eq_timestamp", _TZ, _AT): "f",
        _call("timestamptz_ne_timestamp", _TZ, _AT): "t",
        _call("timestamp_eq_timestamptz", _TS, _ATZ): "f",
        _call("timestamp_ne_timestamptz", _TS, _ATZ): "t",
        _call("timezone", "'UTC'::text", _TZ): "4713-01-01 00:00:00 BC",
        _call("timezone", _TZ): "4713-01-01 00:00:00 BC",
    }
)


def _answer(conn: Any, sql: str) -> str:
    res = conn.conn.pgconn.exec_params(f"{sql} LIMIT 1001".encode(), [], result_format=0)
    error = (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode()
    assert res.status == pq.ExecStatus.TUPLES_OK, f"raised {error}"
    return (res.get_value(0, 0) or b"").decode()


@pytest.mark.parametrize(("sql", "answer"), list(ANSWERED.items()), ids=list(ANSWERED))
def test_a_value_bc_answers_as_postgresql_answers_it(conn: Any, sql: str, answer: str):
    assert _answer(conn, sql) == answer


def test_age_of_one_value_bc_measures_from_today_as_postgresql_defines_it(conn: Any):
    """PostgreSQL 17 defines age(timestamp) as ``age((CURRENT_DATE)::timestamp, $1)``
    (catalog_pg17_procs, oid 2059). Both sides run in one statement, so they read one clock; the
    value against a fixed day is the captured answer in ANSWERED."""
    one = f'pg_catalog."age"({_TS})'
    two = f'pg_catalog."age"((CURRENT_DATE)::timestamp without time zone, {_TS})'
    assert _answer(conn, f"SELECT {one} = {two} AS v") == "t"


#: The hash of a timestamp is int8's hash of its microseconds (timestamp_hash is hashint8).
HASHED: dict[str, str] = {
    _call("timestamp_hash", _TS): _call("hashint8", f"{_US}::bigint"),
    _call("timestamp_hash_extended", _TS, "3::bigint"): _call(
        "hashint8extended", f"{_US}::bigint", "3::bigint"
    ),
}


@pytest.mark.parametrize(("sql", "of_int8"), list(HASHED.items()), ids=list(HASHED))
def test_a_timestamp_bc_hashes_as_its_microseconds(conn: Any, sql: str, of_int8: str):
    assert _answer(conn, sql) == _answer(conn, of_int8)
