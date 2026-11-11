# Copyright (c) 2026 Kenneth Stott
# Canary: 7cf04dfc-f861-499c-9f9f-15cd4ce9afe3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The array functions' polyfills (REQ-2236; replayed against postgres:17.11 by REQ-2171's
everyday-function statements on DuckDB, Trino and ClickHouse).

An array's dimensions are read from its type: PostgreSQL's empty array has none, and a list's
sublists are each as long as the first, so a dimension's length is the first sublist's. PostgreSQL's
MAXDIM is 6, so Trino's and ClickHouse's forms of a dimension's length go to 6. A message PostgreSQL
writes from a value is raised as PostgreSQL raises it (provisa.core.pg_error); ClickHouse's throwIf
takes only a constant message, so there such a refusal is by name, and is a limitation."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal

FAMILY = "arrays"

#: What the forms raise where PostgreSQL's answer would need a shape a list does not hold.
_ARRAY_FILL_REFUSED = Refusal("0A000", "array_fill does not read this value")
_ARRAY_FILL_SHAPE = Limitation(
    "Only one dimension with a lower bound of 1: any other shape is refused.", _ARRAY_FILL_REFUSED
)


def _by_name(function: str, sqlstate: str) -> Limitation:
    """ClickHouse's refusal of a value PostgreSQL refuses with a message written from the value."""
    return Limitation(
        f"An out-of-range count is refused with {_refused(function).sqlstate}, not PostgreSQL's "
        f"{sqlstate} and its message.",
        _refused(function),
    )


def _refused(function: str) -> Refusal:
    return Refusal("0A000", f"{function} does not read this value")


def _no_form(function: str, engine: str) -> Refusal:
    return Refusal("0A000", f"{function}() has no form on {engine}")


ARRAY_NDIMS = Feature(
    "array_ndims",
    FAMILY,
    "The number of an array's dimensions.",
    read=Side(
        rules=(
            Rule(
                "array_ndims($a::anyarray)",
                {
                    "duckdb": r"CAST(CASE WHEN len($a) > 0 THEN "
                    r"length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER)",
                    "trino": r"CAST(CASE WHEN cardinality($a) > 0 THEN "
                    r"length(regexp_extract(typeof($a), '^(array\()+')) / 6 END AS INTEGER)",
                    "clickhouse": "CAST(CASE WHEN length($a) > 0 THEN "
                    "countSubstrings(toTypeName($a), 'Array(') END AS Nullable(Int32))",
                },
            ),
        )
    ),
)

ARRAY_DIMS = Feature(
    "array_dims",
    FAMILY,
    "The text of an array's dimensions, such as [1:2][1:3].",
    read=Side(
        rules=(
            Rule(
                "array_dims($a::anyarray)",
                {
                    "duckdb": r"array_to_string(list_transform(range(CAST(CASE WHEN len($a) > 0 "
                    r"THEN length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER)), "
                    r"_d -> '[1:' || json_array_length(to_json($a), '$' || "
                    r"repeat('[0]', CAST(_d AS INTEGER))) || ']'), '')",
                    "trino": r"array_join(transform(sequence(1, CAST(CASE WHEN cardinality($a) > 0 "
                    r"THEN length(regexp_extract(typeof($a), '^(array\()+')) / 6 END AS INTEGER)), "
                    r"d -> '[1:' || CAST(CASE d "
                    r"WHEN 1 THEN json_array_length(CAST($a AS JSON)) "
                    r"WHEN 2 THEN json_array_length(json_extract(CAST($a AS JSON), '$[0]')) "
                    r"WHEN 3 THEN json_array_length(json_extract(json_extract(CAST($a AS JSON), "
                    r"'$[0]'), '$[0]')) "
                    r"WHEN 4 THEN json_array_length(json_extract(json_extract(json_extract("
                    r"CAST($a AS JSON), '$[0]'), '$[0]'), '$[0]')) "
                    r"WHEN 5 THEN json_array_length(json_extract(json_extract(json_extract("
                    r"json_extract(CAST($a AS JSON), '$[0]'), '$[0]'), '$[0]'), '$[0]')) "
                    r"WHEN 6 THEN json_array_length(json_extract(json_extract(json_extract("
                    r"json_extract(json_extract(CAST($a AS JSON), '$[0]'), '$[0]'), '$[0]'), "
                    r"'$[0]'), '$[0]')) "
                    r"END AS VARCHAR) || ']'), '')",
                    "clickhouse": "CASE WHEN length($a) > 0 THEN arrayStringConcat(arrayMap("
                    "d -> concat('[1:', toString(CASE d "
                    "WHEN 1 THEN JSONLength(toJSONString($a)) "
                    "WHEN 2 THEN JSONLength(toJSONString($a), 1) "
                    "WHEN 3 THEN JSONLength(toJSONString($a), 1, 1) "
                    "WHEN 4 THEN JSONLength(toJSONString($a), 1, 1, 1) "
                    "WHEN 5 THEN JSONLength(toJSONString($a), 1, 1, 1, 1) "
                    "WHEN 6 THEN JSONLength(toJSONString($a), 1, 1, 1, 1, 1) END), ']'), "
                    "range(1, assumeNotNull(CAST(CASE WHEN length($a) > 0 THEN "
                    "countSubstrings(toTypeName($a), 'Array(') END AS Nullable(Int32))) + 1)), '') "
                    "END",
                },
            ),
        )
    ),
)

ARRAY_LOWER = Feature(
    "array_lower",
    FAMILY,
    "The lower bound of a dimension: 1, as a list's index starts there.",
    read=Side(
        rules=(
            Rule(
                "array_lower($a::anyarray, $d::integer)",
                {
                    "duckdb": r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN len($a) > 0 THEN "
                    r"length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER) "
                    r"THEN 1 END AS INTEGER)",
                    "trino": r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN cardinality($a) > 0 "
                    r"THEN length(regexp_extract(typeof($a), '^(array\()+')) / 6 END AS INTEGER) "
                    r"THEN 1 END AS INTEGER)",
                    "clickhouse": "CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN length($a) > 0 "
                    "THEN countSubstrings(toTypeName($a), 'Array(') END AS Nullable(Int32)) "
                    "THEN 1 END AS Nullable(Int32))",
                },
            ),
        )
    ),
)

ARRAY_UPPER = Feature(
    "array_upper",
    FAMILY,
    "The upper bound of a dimension: its length.",
    read=Side(
        rules=(
            Rule(
                "array_upper($a::anyarray, $d::integer)",
                {
                    "duckdb": r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN len($a) > 0 THEN "
                    r"length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER) "
                    r"THEN json_array_length(to_json($a), '$' || "
                    r"repeat('[0]', CAST($d - 1 AS INTEGER))) END AS INTEGER)",
                    "trino": r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN cardinality($a) > 0 "
                    r"THEN length(regexp_extract(typeof($a), '^(array\()+')) / 6 END AS INTEGER) "
                    r"THEN CASE $d "
                    r"WHEN 1 THEN json_array_length(CAST($a AS JSON)) "
                    r"WHEN 2 THEN json_array_length(json_extract(CAST($a AS JSON), '$[0]')) "
                    r"WHEN 3 THEN json_array_length(json_extract(json_extract(CAST($a AS JSON), "
                    r"'$[0]'), '$[0]')) "
                    r"WHEN 4 THEN json_array_length(json_extract(json_extract(json_extract("
                    r"CAST($a AS JSON), '$[0]'), '$[0]'), '$[0]')) "
                    r"WHEN 5 THEN json_array_length(json_extract(json_extract(json_extract("
                    r"json_extract(CAST($a AS JSON), '$[0]'), '$[0]'), '$[0]'), '$[0]')) "
                    r"WHEN 6 THEN json_array_length(json_extract(json_extract(json_extract("
                    r"json_extract(json_extract(CAST($a AS JSON), '$[0]'), '$[0]'), '$[0]'), "
                    r"'$[0]'), '$[0]')) "
                    r"END END AS INTEGER)",
                    "clickhouse": "CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN length($a) > 0 "
                    "THEN countSubstrings(toTypeName($a), 'Array(') END AS Nullable(Int32)) "
                    "THEN CASE $d "
                    "WHEN 1 THEN JSONLength(toJSONString($a)) "
                    "WHEN 2 THEN JSONLength(toJSONString($a), 1) "
                    "WHEN 3 THEN JSONLength(toJSONString($a), 1, 1) "
                    "WHEN 4 THEN JSONLength(toJSONString($a), 1, 1, 1) "
                    "WHEN 5 THEN JSONLength(toJSONString($a), 1, 1, 1, 1) "
                    "WHEN 6 THEN JSONLength(toJSONString($a), 1, 1, 1, 1, 1) "
                    "END END AS Nullable(Int32))",
                },
            ),
        )
    ),
)

ARRAY_FILL = Feature(
    "array_fill",
    FAMILY,
    "An array of a value repeated to the dimensions given.",
    read=Side(
        rules=(
            Rule(
                "array_fill($v::anyelement, $dims::integer[])",
                {
                    "duckdb": "CASE WHEN $dims IS NULL THEN error('postgresql-error 22004: ' || "
                    "'dimension array or low bound array cannot be null') "
                    "WHEN len($dims) = 1 THEN list_transform(range($dims[1]), _i -> $v) "
                    "ELSE error('postgresql-error 0A000: ' || 'array_fill does not read this value') "
                    "END",
                    "trino": "CASE WHEN $dims IS NULL THEN fail('postgresql-error 22004: ' || "
                    "'dimension array or low bound array cannot be null' || "
                    "' <end of postgresql-error>') "
                    "WHEN cardinality($dims) = 1 THEN repeat($v, $dims[1]) "
                    "ELSE fail('postgresql-error 0A000: ' || 'array_fill does not read this value' "
                    "|| ' <end of postgresql-error>') END",
                    "clickhouse": "arrayWithConstant(throwIf(length($dims) <> 1, "
                    "'postgresql-error 0A000: array_fill does not read this value "
                    "<end of postgresql-error>') + assumeNotNull($dims[1]), $v)",
                },
            ),
            Rule(
                "array_fill($v::anyelement, $dims::integer[], $lower_bounds::integer[])",
                {
                    "duckdb": "CASE WHEN $dims IS NULL OR $lower_bounds IS NULL THEN "
                    "error('postgresql-error 22004: ' || "
                    "'dimension array or low bound array cannot be null') "
                    "WHEN len($dims) = 1 AND list_filter($lower_bounds, _b -> _b <> 1) = [] "
                    "THEN list_transform(range($dims[1]), _i -> $v) "
                    "ELSE error('postgresql-error 0A000: ' || 'array_fill does not read this value') "
                    "END",
                    "trino": "CASE WHEN $dims IS NULL OR $lower_bounds IS NULL THEN "
                    "fail('postgresql-error 22004: ' || "
                    "'dimension array or low bound array cannot be null' || "
                    "' <end of postgresql-error>') "
                    "WHEN cardinality($dims) = 1 AND all_match($lower_bounds, _b -> _b = 1) "
                    "THEN repeat($v, $dims[1]) "
                    "ELSE fail('postgresql-error 0A000: ' || 'array_fill does not read this value' "
                    "|| ' <end of postgresql-error>') END",
                    "clickhouse": "arrayWithConstant(throwIf(length($dims) <> 1 OR "
                    "NOT arrayAll(_b -> _b = 1, $lower_bounds), "
                    "'postgresql-error 0A000: array_fill does not read this value "
                    "<end of postgresql-error>') + assumeNotNull($dims[1]), $v)",
                },
            ),
        ),
        limitations={
            "duckdb": (_ARRAY_FILL_SHAPE,),
            "trino": (_ARRAY_FILL_SHAPE,),
            "clickhouse": (_ARRAY_FILL_SHAPE,),
        },
    ),
)

ARRAY_POSITIONS = Feature(
    "array_positions",
    FAMILY,
    "The positions of every element equal to a value, a NULL equal to a NULL.",
    read=Side(
        rules=(
            Rule(
                "array_positions($a::anycompatiblearray, $v::anycompatible)",
                {
                    "duckdb": "CAST(list_filter(range(1, len($a) + 1), "
                    "_i -> $a[_i] IS NOT DISTINCT FROM $v) AS INTEGER[])",
                    "trino": "CASE WHEN cardinality($a) = 0 THEN CAST(ARRAY[] AS ARRAY(INTEGER)) "
                    "ELSE CAST(filter(sequence(1, cardinality($a)), "
                    "_i -> $a[_i] IS NOT DISTINCT FROM $v) AS ARRAY(INTEGER)) END",
                    "clickhouse": "arrayMap(_p -> toInt32(_p.2), arrayFilter(_p -> "
                    "((_p.1 IS NULL AND $v IS NULL) OR _p.1 = $v), arrayZip($a, arrayEnumerate($a))))",
                },
            ),
        )
    ),
)

ARRAY_REPLACE = Feature(
    "array_replace",
    FAMILY,
    "Each element equal to a value replaced by another, a NULL equal to a NULL.",
    read=Side(
        rules=(
            Rule(
                "array_replace($a::anycompatiblearray, $v::anycompatible, $w::anycompatible)",
                {
                    "duckdb": "list_transform($a, "
                    "_x -> CASE WHEN _x IS NOT DISTINCT FROM $v THEN $w ELSE _x END)",
                    "trino": "transform($a, "
                    "_x -> CASE WHEN _x IS NOT DISTINCT FROM $v THEN $w ELSE _x END)",
                    "clickhouse": "arrayMap(_x -> CASE WHEN ((_x IS NULL AND $v IS NULL) OR _x = $v) "
                    "THEN $w ELSE _x END, $a)",
                },
            ),
        )
    ),
)

ARRAY_SAMPLE = Feature(
    "array_sample",
    FAMILY,
    "A number of an array's elements, chosen at random.",
    read=Side(
        rules=(
            Rule(
                "array_sample($a::anyarray, $n::integer)",
                {
                    "duckdb": "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                    "WHEN $n < 0 OR $n > len($a) THEN error('postgresql-error 22023: ' || "
                    "'sample size must be between 0 and ' || len($a)) "
                    "ELSE list_transform(list_sort(list_transform($a, "
                    "_x -> {'k': random(), 'v': _x})), _e -> _e.v)[1:$n] END",
                    "trino": "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                    "WHEN $n < 0 OR $n > cardinality($a) THEN fail('postgresql-error 22023: ' || "
                    "'sample size must be between 0 and ' || CAST(cardinality($a) AS VARCHAR) || "
                    "' <end of postgresql-error>') ELSE slice(shuffle($a), 1, $n) END",
                    "clickhouse": "arraySlice(arrayShuffle($a), 1 + throwIf($n IS NULL OR $n < 0 OR "
                    "$n > length($a), 'postgresql-error 0A000: array_sample does not read this "
                    "value <end of postgresql-error>'), $n)",
                },
            ),
        ),
        limitations={"clickhouse": (_by_name("array_sample", "22023"),)},
    ),
)

ARRAY_SHUFFLE = Feature(
    "array_shuffle",
    FAMILY,
    "An array's elements in a random order.",
    read=Side(
        rules=(
            Rule(
                "array_shuffle($a::anyarray)",
                {
                    "duckdb": "list_transform(list_sort(list_transform($a, "
                    "_x -> {'k': random(), 'v': _x})), _e -> _e.v)",
                    "trino": "shuffle($a)",
                    "clickhouse": "arrayShuffle($a)",
                },
            ),
        )
    ),
)

TRIM_ARRAY = Feature(
    "trim_array",
    FAMILY,
    "An array without its last elements.",
    read=Side(
        rules=(
            Rule(
                "trim_array($a::anyarray, $n::integer)",
                {
                    "duckdb": "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                    "WHEN $n < 0 OR $n > len($a) THEN error('postgresql-error 2202E: ' || "
                    "'number of elements to trim must be between 0 and ' || len($a)) "
                    "ELSE $a[1:len($a) - $n] END",
                    "trino": "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                    "WHEN $n < 0 OR $n > cardinality($a) THEN fail('postgresql-error 2202E: ' || "
                    "'number of elements to trim must be between 0 and ' || "
                    "CAST(cardinality($a) AS VARCHAR) || ' <end of postgresql-error>') "
                    "ELSE slice($a, 1, cardinality($a) - $n) END",
                    "clickhouse": "arraySlice($a, 1 + throwIf($n IS NULL OR $n < 0 OR "
                    "$n > length($a), 'postgresql-error 0A000: trim_array does not read this "
                    "value <end of postgresql-error>'), length($a) - $n)",
                },
            ),
        ),
        limitations={"clickhouse": (_by_name("trim_array", "2202E"),)},
    ),
)

GENERATE_SUBSCRIPTS = Feature(
    "generate_subscripts",
    FAMILY,
    "The subscripts of a dimension, as rows, in either order.",
    read=Side(
        rules=(
            Rule(
                "generate_subscripts($a::anyarray, $d::integer, $reverse::boolean)",
                {
                    "duckdb": r"unnest(CASE WHEN $reverse THEN range(CAST(CASE WHEN $d BETWEEN 1 "
                    r"AND CAST(CASE WHEN len($a) > 0 THEN length(regexp_extract(typeof($a), "
                    r"'(\[\])+$')) // 2 END AS INTEGER) THEN json_array_length(to_json($a), '$' "
                    r"|| repeat('[0]', CAST($d - 1 AS INTEGER))) END AS INTEGER), 0, -1) "
                    r"ELSE range(1, CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN len($a) > 0 "
                    r"THEN length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER) "
                    r"THEN json_array_length(to_json($a), '$' || "
                    r"repeat('[0]', CAST($d - 1 AS INTEGER))) END AS INTEGER) + 1) END)",
                    "trino": _no_form("generate_subscripts", "trino"),
                    "clickhouse": _no_form("generate_subscripts", "clickhouse"),
                },
            ),
        ),
        limitations={
            "trino": (
                Limitation(
                    "A set of rows from an array is refused.",
                    _no_form("generate_subscripts", "trino"),
                ),
            ),
            "clickhouse": (
                Limitation(
                    "A set of rows from an array is refused.",
                    _no_form("generate_subscripts", "clickhouse"),
                ),
            ),
        },
    ),
)

FEATURES: tuple[Feature, ...] = (
    ARRAY_NDIMS,
    ARRAY_DIMS,
    ARRAY_LOWER,
    ARRAY_UPPER,
    ARRAY_FILL,
    ARRAY_POSITIONS,
    ARRAY_REPLACE,
    ARRAY_SAMPLE,
    ARRAY_SHUFFLE,
    TRIM_ARRAY,
    GENERATE_SUBSCRIPTS,
)
