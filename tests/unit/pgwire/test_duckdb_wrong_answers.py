# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1e4c27-5d03-4a6f-8e21-c7f0a3d5b498
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The functions conformance run's statements DuckDB answered otherwise than PostgreSQL (run
41635216812, outcome wrong_answer), each sent as the probe sends it through a real pgwire server
on the DuckDB engine, against PostgreSQL 17's answer to the same statement (REQ-2167)."""

# Requirements: REQ-2167

# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

BOOL, INT8, FLOAT8, TEXT, TEXT_ARRAY, RECORD = 16, 20, 701, 25, 1009, 2249
NAME_ARRAY = 1003

#: (statement, PostgreSQL 17's result type, its rows' text)
ANSWERS: list[tuple[str, int, list[Any]]] = [
    # A record's text is record_out's, not the engine's text of a row.
    (
        "SELECT pg_catalog.\"pg_stat_get_replication_slot\"('abc'::text) AS v",
        RECORD,
        ["(abc,0,0,0,0,0,0,0,0,)"],
    ),
    (
        'SELECT pg_catalog."pg_stat_get_replication_slot"(\'a "b"\'::text) AS v',
        RECORD,
        ['("a ""b""",0,0,0,0,0,0,0,0,)'],
    ),
    (
        'SELECT pg_catalog."pg_stat_get_subscription_stats"(12345::oid) AS v',
        RECORD,
        ["(12345,0,0,)"],
    ),
    (
        'SELECT pg_catalog."pg_control_init"() AS v',
        RECORD,
        ["(8,8192,131072,8192,16777216,64,32,1996,2048,t,0)"],
    ),
    # No function is counted (track_functions is none), and data checksums are off.
    ('SELECT pg_catalog."pg_stat_get_function_calls"(12345::oid) AS v', INT8, [None]),
    ('SELECT pg_catalog."pg_stat_get_function_self_time"(12345::oid) AS v', FLOAT8, [None]),
    ('SELECT pg_catalog."pg_stat_get_function_total_time"(12345::oid) AS v', FLOAT8, [None]),
    (
        'SELECT pg_catalog."pg_stat_get_db_checksum_failures"((SELECT oid FROM '
        "pg_catalog.pg_database WHERE datname = current_database())) AS v",
        INT8,
        [None],
    ),
    # A txid is unsigned: a negative one is past every snapshot.
    (
        "SELECT pg_catalog.\"txid_visible_in_snapshot\"((-7)::bigint, '1:1:'::txid_snapshot) AS v",
        BOOL,
        ["f"],
    ),
    (
        'SELECT pg_catalog."current_schemas"(true::boolean) AS v',
        NAME_ARRAY,
        ["{pg_catalog,public}"],
    ),
    ('SELECT pg_catalog."current_schemas"(false::boolean) AS v', NAME_ARRAY, ["{public}"]),
    ('SELECT pg_catalog."current_schemas"(NULL::boolean) AS v', NAME_ARRAY, [None]),
    (
        'SELECT pg_catalog."pg_collation_for"(\'abc\'::text COLLATE "C") AS v',
        TEXT,
        ['"C"'],
    ),
    ("SELECT pg_catalog.\"pg_settings_get_flags\"('TimeZone'::text) AS v", TEXT_ARRAY, ["{}"]),
    (
        "SELECT pg_catalog.\"pg_input_error_info\"('2024-02-30'::text, 'date'::text) AS v",
        RECORD,
        ['("date/time field value out of range: ""2024-02-30""",,,22008)'],
    ),
    (
        "SELECT pg_catalog.\"pg_input_error_info\"('2024-02-xx'::text, 'date'::text) AS v",
        RECORD,
        ['("invalid input syntax for type date: ""2024-02-xx""",,,22P02)'],
    ),
]


@pytest.mark.parametrize(("sql", "oid", "rows"), ANSWERS, ids=[a[0] for a in ANSWERS])
def test_duckdb_answers_as_postgresql_17(conn: Any, sql: str, oid: int, rows: list[Any]):
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    got = [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]
    assert (res.ftype(0), got) == (oid, rows)
