# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2d17-9c3a-4f60-a1d5-7e0b9c2f6a48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A pg_catalog function called with NULL arguments, as the pgwire conformance probe calls it
(REQ-2167, PG_CATALOG FUNCTIONS ARE GAPS): served over pgwire on DuckDB with the type PostgreSQL
17.11 declares for it, a polymorphic result resolved by its arguments, and its NULL answer."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import pytest
from psycopg import pq

from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

_TSVECTOR, _FLOAT4, _JSONB, _TEXT_ARRAY, _INT4 = 3614, 700, 3802, 1009, 23

#: Each statement the probe sends, and the oid of PostgreSQL 17.11's result column for it.
TYPED: dict[str, int] = {
    # Text search: a NULL argument answers a NULL of the function's own result type.
    'SELECT pg_catalog."array_to_tsvector"(NULL::text[]) AS v': _TSVECTOR,
    'SELECT pg_catalog."json_to_tsvector"(NULL::json, NULL::jsonb) AS v': _TSVECTOR,
    'SELECT pg_catalog."json_to_tsvector"(NULL::regconfig, NULL::json, NULL::jsonb) AS v': (
        _TSVECTOR
    ),
    'SELECT pg_catalog."jsonb_to_tsvector"(NULL::jsonb, NULL::jsonb) AS v': _TSVECTOR,
    'SELECT pg_catalog."jsonb_to_tsvector"(NULL::regconfig, NULL::jsonb, NULL::jsonb) AS v': (
        _TSVECTOR
    ),
    'SELECT pg_catalog."setweight"(NULL::tsvector, NULL::"char") AS v': _TSVECTOR,
    'SELECT pg_catalog."setweight"(NULL::tsvector, NULL::"char", NULL::text[]) AS v': _TSVECTOR,
    'SELECT pg_catalog."to_tsvector"(NULL::json) AS v': _TSVECTOR,
    'SELECT pg_catalog."to_tsvector"(NULL::jsonb) AS v': _TSVECTOR,
    'SELECT pg_catalog."to_tsvector"(NULL::regconfig, NULL::json) AS v': _TSVECTOR,
    'SELECT pg_catalog."to_tsvector"(NULL::regconfig, NULL::jsonb) AS v': _TSVECTOR,
    'SELECT pg_catalog."to_tsvector"(NULL::regconfig, NULL::text) AS v': _TSVECTOR,
    'SELECT pg_catalog."to_tsvector"(NULL::text) AS v': _TSVECTOR,
    'SELECT pg_catalog."ts_delete"(NULL::tsvector, NULL::text) AS v': _TSVECTOR,
    'SELECT pg_catalog."ts_delete"(NULL::tsvector, NULL::text[]) AS v': _TSVECTOR,
    'SELECT pg_catalog."ts_filter"(NULL::tsvector, NULL::"char"[]) AS v': _TSVECTOR,
    'SELECT pg_catalog."tsvector_concat"(NULL::tsvector, NULL::tsvector) AS v': _TSVECTOR,
    'SELECT pg_catalog."ts_headline"(NULL::jsonb, NULL::tsquery) AS v': _JSONB,
    'SELECT pg_catalog."ts_headline"(NULL::jsonb, NULL::tsquery, NULL::text) AS v': _JSONB,
    'SELECT pg_catalog."ts_headline"(NULL::regconfig, NULL::jsonb, NULL::tsquery) AS v': _JSONB,
    'SELECT pg_catalog."ts_headline"(NULL::regconfig, NULL::jsonb, NULL::tsquery, NULL::text) '
    "AS v": _JSONB,
    'SELECT pg_catalog."ts_lexize"(NULL::regdictionary, NULL::text) AS v': _TEXT_ARRAY,
    'SELECT pg_catalog."tsvector_to_array"(NULL::tsvector) AS v': _TEXT_ARRAY,
    'SELECT pg_catalog."ts_rank"(NULL::real[], NULL::tsvector, NULL::tsquery) AS v': _FLOAT4,
    'SELECT pg_catalog."ts_rank"(NULL::real[], NULL::tsvector, NULL::tsquery, NULL::integer) '
    "AS v": _FLOAT4,
    'SELECT pg_catalog."ts_rank"(NULL::tsvector, NULL::tsquery) AS v': _FLOAT4,
    'SELECT pg_catalog."ts_rank"(NULL::tsvector, NULL::tsquery, NULL::integer) AS v': _FLOAT4,
    'SELECT pg_catalog."ts_rank_cd"(NULL::real[], NULL::tsvector, NULL::tsquery) AS v': _FLOAT4,
    'SELECT pg_catalog."ts_rank_cd"(NULL::real[], NULL::tsvector, NULL::tsquery, NULL::integer) '
    "AS v": _FLOAT4,
    'SELECT pg_catalog."ts_rank_cd"(NULL::tsvector, NULL::tsquery) AS v': _FLOAT4,
    'SELECT pg_catalog."ts_rank_cd"(NULL::tsvector, NULL::tsquery, NULL::integer) AS v': _FLOAT4,
    # A polymorphic result: the element type of the arguments.
    'SELECT pg_catalog."any_value"(NULL::integer) AS v': _INT4,
    'SELECT pg_catalog."any_value_transfn"(NULL::integer, NULL::integer) AS v': _INT4,
    'SELECT pg_catalog."anycompatiblenonarray_out"(NULL::integer) AS v': 2275,
    # A type's input, output and send functions.
    'SELECT pg_catalog."brin_bloom_summary_in"(NULL::cstring) AS v': 4600,
    'SELECT pg_catalog."brin_minmax_multi_summary_in"(NULL::cstring) AS v': 4601,
    'SELECT pg_catalog."gtsvectorin"(NULL::cstring) AS v': 3642,
    'SELECT pg_catalog."interval_in"(NULL::cstring, NULL::oid, NULL::integer) AS v': 1186,
    'SELECT pg_catalog."jsonb_in"(NULL::cstring) AS v': _JSONB,
    'SELECT pg_catalog."xid8in"(NULL::cstring) AS v': 5069,
    'SELECT pg_catalog."pg_ddl_command_send"(NULL::pg_ddl_command) AS v': 17,
    # An aggregate's transition and combine functions, outside an aggregate.
    'SELECT pg_catalog."int4_avg_combine"(NULL::bigint[], NULL::bigint[]) AS v': 1016,
    'SELECT pg_catalog."range_intersect_agg_transfn"(NULL::int4range, NULL::int4range) AS v': 3904,
    'SELECT pg_catalog."multirange_intersect_agg_transfn"(NULL::int4multirange, '
    "NULL::int4multirange) AS v": 4451,
    # A type modifier's input function, of a cstring[].
    **{
        f'SELECT pg_catalog."{name}typmodin"(NULL::cstring[]) AS v': _INT4
        for name in (
            "bit",
            "bpchar",
            "interval",
            "numeric",
            "time",
            "timetz",
            "timestamp",
            "timestamptz",
            "varbit",
            "varchar",
        )
    },
    # regexp_replace with start, N and flags: strict.
    'SELECT pg_catalog."regexp_replace"(NULL::text, NULL::text, NULL::text, NULL::integer, '
    "NULL::integer, NULL::text) AS v": 25,
    # Binary upgrade's functions: strict, so a NULL answers before the server's mode is asked.
    'SELECT pg_catalog."binary_upgrade_set_next_multirange_pg_type_oid"(NULL::oid) AS v': 2278,
    'SELECT pg_catalog."binary_upgrade_set_next_multirange_array_pg_type_oid"(NULL::oid) AS v': (
        2278
    ),
}


def _answer(conn: Any, sql: str) -> tuple[int, list[Any]]:
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status != pq.ExecStatus.FATAL_ERROR, (res.error_message or b"").decode()[:300]
    return res.ftype(0), [res.get_value(i, 0) for i in range(res.ntuples)]


@pytest.mark.parametrize("sql", list(TYPED))
def test_a_null_call_answers_postgresqls_type(conn: Any, sql: str):
    assert _answer(conn, sql) == (TYPED[sql], [None])


def test_unnest_of_an_integer_array_is_an_integer_column(conn: Any):
    # PostgreSQL resolves anyelement to the array's element: a NULL array has no rows.
    assert _answer(conn, 'SELECT pg_catalog."unnest"(NULL::integer[]) AS v') == (_INT4, [])
    assert _answer(conn, "SELECT pg_catalog.\"unnest\"('{4,5}'::integer[]) AS v") == (
        _INT4,
        [b"4", b"5"],
    )
