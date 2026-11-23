# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a1c62-5d4e-4b07-9e21-c7a0d6f4b8e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A LITERAL PAST ITS TYPE ON DUCKDB (REQ-2236, amended 2026-11-23): a literal PostgreSQL's input
function refuses is refused with PostgreSQL's SQLSTATE and message, whichever call reads it. Each
statement is the one the functions measure runs (Conformance (functions) run 45101362913), each
answer PostgreSQL 17's."""

# Requirements: REQ-2236, REQ-2167
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_OID = "'4294967296'::oid"
_OID_PAST = ("22003", 'value "4294967296" is out of range for type oid')
_IVL = "'178956971 years'::interval"
_IVL_PAST = ("22008", "interval out of range")
_INT_PAST = ("22003", 'value "2147483648" is out of range for type integer')


def _values(call: str, value: str) -> str:
    return f"SELECT {call} AS v FROM (VALUES ({value}), ({value})) AS t(c1)"


#: statement -> (SQLSTATE, message) PostgreSQL 17 raises.
REFUSED: dict[str, tuple[str, str]] = {
    **{
        f'SELECT pg_catalog."{name}"({_OID}, 12345::oid) AS v': _OID_PAST
        for name in (
            "btoidcmp", "oideq", "oidge", "oidgt", "oidlarger", "oidle", "oidlt", "oidne",
            "oidsmaller",
        )
    },  # fmt: skip
    **{f'SELECT pg_catalog."{name}"({_OID}) AS v': _OID_PAST for name in ("btequalimage", "hashoid", "int8")},
    f'SELECT pg_catalog."hashoidextended"({_OID}, 3::bigint) AS v': _OID_PAST,
    f"SELECT pg_catalog.\"nameconcatoid\"('abc'::name, {_OID}) AS v": _OID_PAST,
    _values('pg_catalog."max"(c1)', _OID): _OID_PAST,
    _values('pg_catalog."min"(c1)', _OID): _OID_PAST,
    "SELECT pg_catalog.\"oid\"('-9223372036854775808'::bigint) AS v": ("22003", "OID out of range"),
    **{
        f"SELECT pg_catalog.\"{name}\"('year'::text, {_IVL}) AS v": _IVL_PAST
        for name in ("date_part", "extract")
    },
    **{
        f'SELECT pg_catalog."{name}"({_IVL}) AS v': _IVL_PAST
        for name in ("interval_um", "justify_days", "justify_hours", "justify_interval", "time")
    },
    **{_values(f'pg_catalog."{name}"(c1)', _IVL): _IVL_PAST for name in ("max", "min", "sum")},
    _values('pg_catalog."percentile_cont"(0.25::double precision) WITHIN GROUP (ORDER BY c1)', _IVL): _IVL_PAST,
    _values(
        "pg_catalog.\"percentile_cont\"('{0.25,0.5}'::double precision[]) WITHIN GROUP (ORDER BY c1)", _IVL
    ): _IVL_PAST,
    **{
        f"SELECT pg_catalog.\"{name}\"('2147483648'::integer) AS v": _INT_PAST
        for name in ("pg_column_compression", "pg_column_toast_chunk_id", "pg_typeof")
    },
    "SELECT pg_catalog.\"date_part\"('year'::text, '5874897-12-31'::date) AS v": (
        "22008", "date out of range for timestamp"
    ),
    "SELECT pg_catalog.\"isfinite\"('5874898-01-01'::date) AS v": (
        "22008", 'date out of range: "5874898-01-01"'
    ),
    "SELECT pg_catalog.\"length\"('\\x00ff00'::bytea, 'abc'::name) AS v": (
        "22021", 'invalid byte sequence for encoding "WIN1258": 0x00'
    ),
    "SELECT pg_catalog.\"money\"('NaN'::numeric) AS v": ("0A000", "cannot convert NaN to bigint"),
    "SELECT pg_catalog.\"numeric_pl_pg_lsn\"('NaN'::numeric, '0/16B3748'::pg_lsn) AS v": (
        "0A000", "cannot add NaN to pg_lsn"
    ),
    "SELECT pg_catalog.\"pg_lsn_pli\"('0/16B3748'::pg_lsn, 'NaN'::numeric) AS v": (
        "0A000", "cannot add NaN to pg_lsn"
    ),
    "SELECT pg_catalog.\"pg_lsn_mii\"('0/16B3748'::pg_lsn, 'NaN'::numeric) AS v": (
        "0A000", "cannot subtract NaN from pg_lsn"
    ),
}  # fmt: skip


@pytest.mark.parametrize(("sql", "raised"), list(REFUSED.items()), ids=list(REFUSED))
def test_a_literal_past_its_type_is_refused_as_postgresql_refuses_it(
    conn: Any, sql: str, raised: tuple[str, str]
):
    res = conn.conn.pgconn.exec_params(f"{sql} LIMIT 1001".encode(), [], result_format=0)
    said = [res.get_value(0, 0)] if res.status == pq.ExecStatus.TUPLES_OK else None
    assert res.status == pq.ExecStatus.FATAL_ERROR, f"answered {said}"
    sqlstate, message = raised
    got = (res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode()
    assert (got, (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode()) == (
        sqlstate,
        message,
    )
