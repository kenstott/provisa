# Copyright (c) 2026 Kenneth Stott
# Canary: ddad0e9b-8727-49cb-aa2e-643015ace373
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A TIMESTAMP OR DATE AT ITS EDGE ON DUCKDB (REQ-2236, amended 2026-11-23 and 2026-11-24): the
edge values of the functions measure that main's BC and infinity work (Conformance (functions)
run 45511817771) let reach DuckDB. Where PostgreSQL's answer can be given it is (the year of a
timestamptz BC, a date past DuckDB's timestamps against a timestamp, a far date's subdiff); where
PostgreSQL raises, its error is raised (a zone it does not know, a timestamp past its range); a
timestamp past DuckDB's own range raises the 0A000 its grid row states. Each answer PostgreSQL
17's in a session of TimeZone UTC."""

# Requirements: REQ-2236, REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from provisa.transpiler.polyfill.duckdb_literals import (
    INFINITE_DIFFERENCE,
    PAST_TIMESTAMP,
    PAST_TIMESTAMPTZ,
)
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_TZ_BC = "'4713-01-01 00:00:00+00 BC'::timestamp with time zone"
_FAR_TS = "'294276-12-31 23:59:59.999999'::timestamp without time zone"
_FAR_TZ = "'294276-12-31 23:59:59.999999+00'::timestamp with time zone"
_FAR_DATE = "'5874897-12-31'::date"
_DAY = "'2024-06-15'::date"
_AT = "'2024-06-15 12:30:45'::timestamp without time zone"
_ATZ = "'2024-06-15 12:30:45+00'::timestamp with time zone"
_IVL = "'1 day 02:03:04'::interval"


def _call(name: str, *args: str) -> str:
    return f'SELECT pg_catalog."{name}"({", ".join(args)}) AS v'


#: statement -> the text PostgreSQL 17 answers.
ANSWERED: dict[str, str] = {
    _call("date_part", "'year'::text", _TZ_BC): "-4713",
    _call("extract", "'year'::text", _TZ_BC): "-4713",
    _call("date_cmp_timestamp", _FAR_DATE, _AT): "1",
    _call("date_eq_timestamp", _FAR_DATE, _AT): "f",
    _call("date_ne_timestamp", _FAR_DATE, _AT): "t",
    _call("date_lt_timestamp", _FAR_DATE, _AT): "f",
    _call("date_le_timestamp", _FAR_DATE, _AT): "f",
    _call("date_gt_timestamp", _FAR_DATE, _AT): "t",
    _call("date_ge_timestamp", _FAR_DATE, _AT): "t",
    _call("date_cmp_timestamp", _FAR_DATE, "'infinity'::timestamp without time zone"): "-1",
    _call("timestamp_cmp_date", _AT, _FAR_DATE): "-1",
    _call("timestamp_lt_date", _AT, _FAR_DATE): "t",
    _call("timestamp_gt_date", _AT, _FAR_DATE): "f",
    _call("date_cmp_timestamptz", _FAR_DATE, _ATZ): "1",
    _call("timestamptz_lt_date", _ATZ, _FAR_DATE): "t",
    _call("daterange_subdiff", _FAR_DATE, _DAY): "2145023016",
    # cot of a zero is infinity of the zero's sign (dcot checks no overflow).
    _call("cot", "'-0'::double precision"): "-Infinity",
    _call("cot", "'0'::double precision"): "Infinity",
}

#: statement -> the SQLSTATE and message it raises: PostgreSQL 17's where it raises, and where
#: DuckDB holds no such value (a timestamp past its range, an infinite interval), the refusal its
#: grid row states.
RAISED: dict[str, tuple[str, str]] = {
    _call("timestamp_cmp", _FAR_TS, _AT): ("0A000", PAST_TIMESTAMP),
    _call("timestamp_mi", _FAR_TS, _AT): ("0A000", PAST_TIMESTAMP),
    _call("timestamptz_cmp_timestamp", _FAR_TZ, _AT): ("0A000", PAST_TIMESTAMPTZ),
    _call("date_bin", "'15 minutes'::interval", _FAR_TZ, _ATZ): ("0A000", PAST_TIMESTAMPTZ),
    _call("tsrange", _AT, _FAR_TS, "'[)'::text"): ("0A000", PAST_TIMESTAMP),
    _call("tsrange_subdiff", _FAR_TS, _AT): ("0A000", PAST_TIMESTAMP),
    _call("tstzrange_subdiff", _FAR_TZ, _ATZ): ("0A000", PAST_TIMESTAMPTZ),
    _call("timezone", "'UTC'::text", _FAR_TS): ("0A000", PAST_TIMESTAMP),
    _call("isfinite", "'294277-01-01 00:00:00'::timestamp without time zone"): (
        "22008",
        'timestamp out of range: "294277-01-01 00:00:00"',
    ),
    _call("date_add", _TZ_BC, _IVL, "'abc'::text"): ("22023", 'time zone "abc" not recognized'),
    _call("date_subtract", _ATZ, _IVL, "''::text"): ("22023", 'time zone "" not recognized'),
    _call("date_trunc", "'day'::text", _TZ_BC, "'abc'::text"): (
        "22023",
        'time zone "abc" not recognized',
    ),
    _call("timezone", "'abc'::text", _AT): ("22023", 'time zone "abc" not recognized'),
    _call("timestamp_mi", "'infinity'::timestamp without time zone", _AT): (
        "0A000",
        INFINITE_DIFFERENCE.format(name="timestamp_mi"),
    ),
    _call("timestamptz_mi", _ATZ, "'-infinity'::timestamp with time zone"): (
        "0A000",
        INFINITE_DIFFERENCE.format(name="timestamptz_mi"),
    ),
}


def _result(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(f"{sql} LIMIT 1001".encode(), [], result_format=0)


def _field(res: Any, field: pq.DiagnosticField) -> str:
    return (res.error_field(field) or b"").decode()


@pytest.mark.parametrize(("sql", "answer"), list(ANSWERED.items()), ids=list(ANSWERED))
def test_an_edge_answers_as_postgresql_answers_it(conn: Any, sql: str, answer: str):
    res = _result(conn, sql)
    assert res.status == pq.ExecStatus.TUPLES_OK, (
        f"raised {_field(res, pq.DiagnosticField.MESSAGE_PRIMARY)}"
    )
    assert (res.get_value(0, 0) or b"").decode() == answer


@pytest.mark.parametrize(("sql", "raised"), list(RAISED.items()), ids=list(RAISED))
def test_an_edge_raises_its_stated_error(conn: Any, sql: str, raised: tuple[str, str]):
    res = _result(conn, sql)
    assert res.status == pq.ExecStatus.FATAL_ERROR, "answered where it raises"
    said = (
        _field(res, pq.DiagnosticField.SQLSTATE),
        _field(res, pq.DiagnosticField.MESSAGE_PRIMARY),
    )
    assert said == raised


def test_a_timestamp_within_duckdbs_range_is_read():
    """The last microsecond DuckDB holds is read, the next refused (the grid row's bound)."""
    from provisa.core.pg_error import PostgresError
    from provisa.transpiler.polyfill.duckdb_literals import refuse_past_stamps

    refuse_past_stamps("SELECT '294247-01-10 04:00:54.775806'::timestamp")
    with pytest.raises(PostgresError, match="has no exact form on duckdb"):
        refuse_past_stamps("SELECT '294247-01-10 04:00:54.775807'::timestamp")
