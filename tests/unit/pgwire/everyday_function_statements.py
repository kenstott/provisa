# Copyright (c) 2026 Kenneth Stott
# Canary: 4e2b9a61-7d03-4c58-a6f1-0b9d3e71c842
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The statements that prove the everyday pg_catalog functions (REQ-2171), by family: each function
with values, a NULL, and what PostgreSQL refuses. Recorded by record_pg17_everyday_functions.py,
replayed by test_pg_everyday_functions.py on DuckDB and test_everyday_functions_on_engines.py on
Trino and ClickHouse."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

_FLAT = "ARRAY[1,2,3]"
_NESTED = "ARRAY[[1,2,3],[4,5,6]]"
_EMPTY = "ARRAY[]::integer[]"
_NULL = "NULL::integer[]"

ARRAY = [
    *(f"SELECT array_ndims({a}) AS v" for a in (_FLAT, _NESTED, _EMPTY, _NULL)),
    "SELECT array_ndims(ARRAY[[[1],[2]]]) AS v",
    *(f"SELECT array_dims({a}) AS v" for a in (_FLAT, _NESTED, _EMPTY, _NULL)),
    "SELECT array_dims(ARRAY[[[1],[2]]]) AS v",
    *(
        f"SELECT {name}({a}, {d}) AS v"
        for name in ("array_lower", "array_upper")
        for a, d in (
            (_FLAT, "1"),
            (_FLAT, "2"),
            (_FLAT, "0"),
            (_NESTED, "1"),
            (_NESTED, "2"),
            (_EMPTY, "1"),
            (_NULL, "1"),
            (_FLAT, "NULL::integer"),
        )
    ),
    "SELECT array_fill(7, ARRAY[3]) AS v",
    "SELECT array_fill(7, ARRAY[0]) AS v",
    "SELECT array_fill(NULL::integer, ARRAY[2]) AS v",
    "SELECT array_fill('x'::text, ARRAY[2]) AS v",
    "SELECT array_fill(7, NULL::integer[]) AS v",
    "SELECT array_fill(7, ARRAY[2], ARRAY[1]) AS v",
    "SELECT array_fill(7, ARRAY[2], NULL::integer[]) AS v",
    "SELECT array_positions(ARRAY[1,2,1], 1) AS v",
    "SELECT array_positions(ARRAY[1,NULL,1], NULL::integer) AS v",
    "SELECT array_positions(ARRAY[1,2], 9) AS v",
    "SELECT array_positions(NULL::integer[], 1) AS v",
    "SELECT array_replace(ARRAY[1,2,1], 1, 9) AS v",
    "SELECT array_replace(ARRAY[1,NULL], NULL::integer, 9) AS v",
    "SELECT array_replace(ARRAY[1,2], 1, NULL::integer) AS v",
    "SELECT array_replace(NULL::integer[], 1, 9) AS v",
    "SELECT array_sample(ARRAY[7], 1) AS v",
    "SELECT array_sample(ARRAY[1,2], 0) AS v",
    "SELECT array_sample(ARRAY[1,2], 3) AS v",
    "SELECT array_sample(ARRAY[1,2], -1) AS v",
    "SELECT array_sample(NULL::integer[], 1) AS v",
    "SELECT array_shuffle(ARRAY[7]) AS v",
    "SELECT array_shuffle(NULL::integer[]) AS v",
    "SELECT trim_array(ARRAY[1,2,3], 1) AS v",
    "SELECT trim_array(ARRAY[1,2,3], 3) AS v",
    "SELECT trim_array(ARRAY[1,2,3], 0) AS v",
    "SELECT trim_array(ARRAY[1,2,3], 4) AS v",
    "SELECT trim_array(ARRAY[1,2,3], -1) AS v",
    "SELECT trim_array(NULL::integer[], 1) AS v",
    "SELECT trim_array(ARRAY[1,2], NULL::integer) AS v",
    "SELECT array_agg(a) AS v FROM (VALUES (ARRAY[1,2]), (ARRAY[3,4])) t(a)",
    "SELECT array_agg(ARRAY[1]) AS v",
    "SELECT generate_subscripts(ARRAY[7,8,9], 1, true) AS v",
    "SELECT generate_subscripts(ARRAY[7,8,9], 1, false) AS v",
    "SELECT generate_subscripts(ARRAY[[1,2,3],[4,5,6]], 2, true) AS v",
    "SELECT generate_subscripts(NULL::integer[], NULL::integer, NULL::boolean) AS v",
]

#: What a statement needs of the database, run first on PostgreSQL.
SETUP: list[str] = []

STATEMENTS: dict[str, list[str]] = {"Array": ARRAY}

#: The conformance probe's own statements (scripts/conformance/pgwire_function_probe.py), on the
#: default engine alone: what a client's literal of an array reads as is not a function's.
PROBES: dict[str, list[str]] = {
    "Array": [
        "SELECT pg_catalog.\"array_agg\"('{1}'::integer[]) AS v",
        'SELECT pg_catalog."array_dims"(NULL::integer[]) AS v',
        "SELECT pg_catalog.\"array_fill\"(NULL::integer, '{1}'::integer[]) AS v",
        "SELECT pg_catalog.\"array_fill\"(NULL::integer, '{1}'::integer[], '{1}'::integer[]) AS v",
        'SELECT pg_catalog."array_lower"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_ndims"(NULL::integer[]) AS v',
        'SELECT pg_catalog."array_positions"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_replace"(NULL::integer[], NULL::integer, NULL::integer) AS v',
        'SELECT pg_catalog."array_sample"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."array_shuffle"(NULL::integer[]) AS v',
        'SELECT pg_catalog."array_upper"(NULL::integer[], NULL::integer) AS v',
        'SELECT pg_catalog."generate_subscripts"(NULL::integer[], NULL::integer, NULL::boolean) AS v',
    ],
}
