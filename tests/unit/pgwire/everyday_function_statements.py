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

STRING = [
    "SELECT anytextcat(1, 'a') AS v",
    "SELECT anytextcat(true, 'a') AS v",
    "SELECT anytextcat(NULL::integer, 'a') AS v",
    "SELECT anytextcat(1, NULL::text) AS v",
    "SELECT textanycat('a', 2) AS v",
    "SELECT textanycat('a', false) AS v",
    "SELECT textanycat(NULL::text, 2) AS v",
    "SELECT textanycat('a', NULL::integer) AS v",
]

_ANY = ("1", "2", "NULL::integer")
OTHER = [
    "SELECT num_nulls(1, NULL, 2) AS v",
    "SELECT num_nonnulls(1, NULL, 2) AS v",
    "SELECT num_nulls(NULL::integer) AS v",
    "SELECT num_nonnulls(NULL::integer) AS v",
    "SELECT num_nulls(1, 'a', NULL::date) AS v",
    "SELECT num_nulls(VARIADIC ARRAY[1,NULL,3]) AS v",
    "SELECT num_nonnulls(VARIADIC ARRAY[1,NULL,3]) AS v",
    "SELECT num_nulls(VARIADIC NULL::integer[]) AS v",
    "SELECT num_nonnulls(VARIADIC ARRAY[]::integer[]) AS v",
    *(
        f"SELECT {name}({a}, {b}) AS v"
        for name in ("int8inc_any", "int8dec_any")
        for a, b in (
            ("1::bigint", "2"),
            ("1::bigint", "NULL::integer"),
            ("NULL::bigint", "2"),
            ("NULL::bigint", "NULL::integer"),
        )
    ),
    "SELECT int8inc_any(9223372036854775807::bigint, 1) AS v",
    "SELECT int8dec_any((-9223372036854775807 - 1)::bigint, 1) AS v",
    "SELECT currval(NULL::regclass) AS v",
    "SELECT nextval(NULL::regclass) AS v",
    "SELECT satisfies_hash_partition(NULL::oid, NULL::integer, NULL::integer, NULL::integer) AS v",
]

_ROWS = "(VALUES (1),(2),(2),(3),(NULL::integer)) t(x)"
_NUMBERS = "(VALUES (1.0::float8),(2.0::float8),(4.0::float8),(NULL::float8)) t(x)"
_SPANS = "(VALUES ('1 hour'::interval),('3 hours'::interval)) t(x)"
AGGREGATE = [
    *(
        f"SELECT {name}({a}) WITHIN GROUP (ORDER BY x{order}) AS v FROM {_ROWS}"
        for name in ("rank", "dense_rank", "percent_rank", "cume_dist")
        for a in ("2", "0", "9", "NULL::integer")
        for order in ("", " DESC", " NULLS FIRST", " DESC NULLS LAST")
    ),
    *(
        f"SELECT {name}(1) WITHIN GROUP (ORDER BY x) AS v FROM {_ROWS} WHERE x > 100"
        for name in ("rank", "dense_rank", "percent_rank", "cume_dist")
    ),
    f"SELECT mode() WITHIN GROUP (ORDER BY x) AS v FROM {_ROWS}",
    "SELECT mode() WITHIN GROUP (ORDER BY x) AS v FROM (VALUES (2),(2),(1),(1),(3)) t(x)",
    "SELECT mode() WITHIN GROUP (ORDER BY x DESC) AS v FROM (VALUES (2),(2),(1),(1),(3)) t(x)",
    f"SELECT ntile(2) OVER (ORDER BY x) AS v FROM {_ROWS}",
    f"SELECT percentile_disc(ARRAY[0.25,0.75]::float8[]) WITHIN GROUP (ORDER BY x) AS v "
    f"FROM {_ROWS}",
]

NUMERIC = [
    f"SELECT percentile_cont(0.5::float8) WITHIN GROUP (ORDER BY x) AS v FROM {_NUMBERS}",
    f"SELECT percentile_cont(0.25::float8) WITHIN GROUP (ORDER BY x DESC) AS v FROM {_NUMBERS}",
    f"SELECT percentile_cont(ARRAY[0.25,0.75]::float8[]) WITHIN GROUP (ORDER BY x) AS v "
    f"FROM {_NUMBERS}",
    f"SELECT percentile_cont(NULL::float8) WITHIN GROUP (ORDER BY x) AS v FROM {_NUMBERS}",
    f"SELECT percentile_cont(2::float8) WITHIN GROUP (ORDER BY x) AS v FROM {_NUMBERS}",
    f"SELECT percentile_disc(0.5::float8) WITHIN GROUP (ORDER BY x) AS v FROM {_ROWS}",
    f"SELECT percentile_disc(0::float8) WITHIN GROUP (ORDER BY x DESC) AS v FROM {_ROWS}",
    f"SELECT percentile_disc(NULL::float8) WITHIN GROUP (ORDER BY x) AS v FROM {_ROWS}",
    f"SELECT percentile_disc(-1::float8) WITHIN GROUP (ORDER BY x) AS v FROM {_ROWS}",
]

DATE = [
    f"SELECT percentile_cont(0.5::float8) WITHIN GROUP (ORDER BY x::interval) AS v FROM {_SPANS}",
    f"SELECT percentile_cont(ARRAY[0.5]::float8[]) WITHIN GROUP (ORDER BY x::interval) AS v "
    f"FROM {_SPANS}",
    f"SELECT percentile_cont(NULL::float8) WITHIN GROUP (ORDER BY x::interval) AS v FROM {_SPANS}",
]

SYSTEM = [
    "SELECT aclexplode(NULL::aclitem[]) AS v",
    "SELECT pg_options_to_table(ARRAY['fillfactor=70','autovacuum_enabled=off']) AS v",
    "SELECT * FROM pg_options_to_table(ARRAY['fillfactor=70','a=b=c','bare'])",
    "SELECT * FROM pg_options_to_table(NULL::text[])",
    "SELECT option_value FROM pg_options_to_table(ARRAY['fillfactor=70']) WHERE option_name = 'fillfactor'",
]

_RANGE_TYPES = {
    "int4": ("[1,2)", "[3,4)"),
    "int8": ("[1,2)", "[3,4)"),
    "num": ("[1.5,2.5)", "[3.5,4.5)"),
    "date": ("[2020-01-01,2020-02-01)", "[2020-03-01,2020-04-01)"),
    "ts": (
        '["2020-01-01 00:00:00","2020-02-01 00:00:00")',
        '["2020-03-01 00:00:00","2020-04-01 00:00:00")',
    ),
}
RANGE = [
    *(
        f"SELECT {kind}multirange(VARIADIC ARRAY['{a}'::{kind}range,'{b}'::{kind}range])::text AS v"
        for kind, (a, b) in _RANGE_TYPES.items()
    ),
    *(
        f"SELECT {kind}multirange(VARIADIC NULL::{kind}range[]) AS v"
        for kind in (*_RANGE_TYPES, "tstz")
    ),
    "SELECT int4multirange(VARIADIC ARRAY[]::int4range[])::text AS v",
    "SELECT multirange_cmp('{[1,2)}'::int4multirange, '{[1,3)}'::int4multirange) AS v",
    "SELECT multirange_cmp('{[1,3)}'::int4multirange, '{[1,3)}'::int4multirange) AS v",
    "SELECT multirange_cmp('{[1,3),[5,6)}'::int4multirange, '{[1,3)}'::int4multirange) AS v",
    "SELECT multirange_cmp(NULL::int4multirange, '{[1,3)}'::int4multirange) AS v",
    "SELECT range_union(int4range(1,3), int4range(2,5))::text AS v",
    "SELECT range_union(int4range(1,3), int4range(3,5))::text AS v",
    "SELECT range_union(int4range(1,3), int4range(7,9))::text AS v",
    "SELECT range_union(int4range(1,3), NULL::int4range) AS v",
    "SELECT range_union(NULL::int4range, NULL::int4range) AS v",
]

#: What a statement needs of the database, run first on PostgreSQL.
SETUP: list[str] = []

STATEMENTS: dict[str, list[str]] = {
    "Array": ARRAY,
    "Range": RANGE,
    "String": STRING,
    "Other": OTHER,
    "Aggregate and window": AGGREGATE,
    "Numeric": NUMERIC,
    "Date": DATE,
    "System": SYSTEM,
}

#: The conformance probe's own statements (scripts/conformance/pgwire_function_probe.py), on the
#: default engine alone: what a client's literal of an array reads as is not a function's.
PROBES: dict[str, list[str]] = {
    "Range": [
        'SELECT pg_catalog."datemultirange"(VARIADIC NULL::daterange[]) AS v',
        'SELECT pg_catalog."int4multirange"(VARIADIC NULL::int4range[]) AS v',
        'SELECT pg_catalog."int8multirange"(VARIADIC NULL::int8range[]) AS v',
        'SELECT pg_catalog."multirange_cmp"(NULL::int4multirange, NULL::int4multirange) AS v',
        'SELECT pg_catalog."nummultirange"(VARIADIC NULL::numrange[]) AS v',
        'SELECT pg_catalog."range_union"(NULL::int4range, NULL::int4range) AS v',
        'SELECT pg_catalog."tsmultirange"(VARIADIC NULL::tsrange[]) AS v',
        'SELECT pg_catalog."tstzmultirange"(VARIADIC NULL::tstzrange[]) AS v',
    ],
    "String": [
        "SELECT anytextcat(1.50, 'a') AS v",
        'SELECT pg_catalog."anytextcat"(NULL::integer, NULL::text) AS v',
        'SELECT pg_catalog."textanycat"(NULL::text, NULL::integer) AS v',
    ],
    "Other": [
        'SELECT pg_catalog."currval"(NULL::regclass) AS v',
        'SELECT pg_catalog."int8dec_any"(NULL::bigint, NULL::integer) AS v',
        'SELECT pg_catalog."int8inc_any"(NULL::bigint, NULL::integer) AS v',
        'SELECT pg_catalog."nextval"(NULL::regclass) AS v',
        'SELECT pg_catalog."num_nonnulls"(NULL::integer) AS v',
        'SELECT pg_catalog."num_nulls"(NULL::integer) AS v',
        'SELECT pg_catalog."satisfies_hash_partition"(NULL::oid, NULL::integer, NULL::integer, NULL::integer) AS v',
        'SELECT pg_catalog."trim_array"(NULL::integer[], NULL::integer) AS v',
    ],
    "Aggregate and window": [
        'SELECT pg_catalog."cume_dist"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
        'SELECT pg_catalog."dense_rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
        'SELECT pg_catalog."mode"() WITHIN GROUP (ORDER BY NULL::integer) AS v',
        'SELECT pg_catalog."ntile"(NULL::integer) OVER () AS v',
        'SELECT pg_catalog."percent_rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
        'SELECT pg_catalog."percentile_disc"(NULL::double precision[]) WITHIN GROUP (ORDER BY NULL::integer) AS v',
        'SELECT pg_catalog."rank"(NULL::integer) WITHIN GROUP (ORDER BY NULL::integer) AS v',
    ],
    "Numeric": [
        'SELECT pg_catalog."percentile_cont"(NULL::double precision) WITHIN GROUP (ORDER BY NULL::double precision) AS v',
        'SELECT pg_catalog."percentile_cont"(NULL::double precision[]) WITHIN GROUP (ORDER BY NULL::double precision) AS v',
        'SELECT pg_catalog."percentile_disc"(NULL::double precision) WITHIN GROUP (ORDER BY NULL::integer) AS v',
    ],
    "Date": [
        'SELECT pg_catalog."percentile_cont"(NULL::double precision) WITHIN GROUP (ORDER BY NULL::interval) AS v',
        'SELECT pg_catalog."percentile_cont"(NULL::double precision[]) WITHIN GROUP (ORDER BY NULL::interval) AS v',
    ],
    "System": [
        "SELECT pg_index_has_property(0::regclass, 'clusterable') AS v",
        "SELECT pg_index_column_has_property(0::regclass, 1, 'asc') AS v",
        'SELECT pg_catalog."aclexplode"(NULL::aclitem[]) AS v',
        'SELECT pg_catalog."pg_index_column_has_property"(NULL::regclass, NULL::integer, NULL::text) AS v',
        'SELECT pg_catalog."pg_index_has_property"(NULL::regclass, NULL::text) AS v',
        'SELECT pg_catalog."pg_options_to_table"(NULL::text[]) AS v',
    ],
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
