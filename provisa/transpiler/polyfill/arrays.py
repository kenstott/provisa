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

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite

FAMILY = "arrays"

#: The dialects whose forms are Trino's SQL.
_TRINO = ("trino", "presto", "athena")
#: The dialects of a source a statement goes to directly that no array row writes a form for.
_NO_FORMS = tuple(d for d in DIALECTS if d not in {*NATIVE, "duckdb", "clickhouse", *_TRINO})

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


def _no_form_on(function: str, dialects: tuple[str, ...]) -> dict[str, Refusal]:
    """Each of ``dialects`` refusing ``function``, by its own name."""
    return {dialect: _no_form(function, dialect) for dialect in dialects}


def _on_sources(function: str) -> dict[str, Refusal]:
    """A source a statement goes to directly has no form of an array function."""
    return _no_form_on(function, _NO_FORMS)


def _on_trino(form: str) -> dict[str, str]:
    """Trino's form, on each dialect whose forms are Trino's SQL."""
    return dict.fromkeys(_TRINO, form)


ARRAY_NDIMS = Feature(
    "array_ndims",
    FAMILY,
    "The number of an array's dimensions.",
    read=Side(
        rules=(
            Rule(
                "array_ndims($a::anyarray)",
                {
                    **_on_sources("array_ndims"),
                    "duckdb": r"CAST(CASE WHEN len($a) > 0 THEN "
                    r"length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER)",
                    **_on_trino(
                        r"CAST(CASE WHEN cardinality($a) > 0 THEN "
                        r"length(regexp_extract(typeof($a), '^(array\()+')) / 6 END AS INTEGER)"
                    ),
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
                    **_on_sources("array_dims"),
                    "duckdb": r"array_to_string(list_transform(range(CAST(CASE WHEN len($a) > 0 "
                    r"THEN length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER)), "
                    r"_d -> '[1:' || json_array_length(to_json($a), '$' || "
                    r"repeat('[0]', CAST(_d AS INTEGER))) || ']'), '')",
                    **_on_trino(
                        r"array_join(transform(sequence(1, CAST(CASE WHEN cardinality($a) > 0 "
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
                        r"END AS VARCHAR) || ']'), '')"
                    ),
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
                    **_on_sources("array_lower"),
                    "duckdb": r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN len($a) > 0 THEN "
                    r"length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER) "
                    r"THEN 1 END AS INTEGER)",
                    **_on_trino(
                        r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN cardinality($a) > 0 "
                        r"THEN length(regexp_extract(typeof($a), '^(array\()+')) / 6 END AS INTEGER) "
                        r"THEN 1 END AS INTEGER)"
                    ),
                    "clickhouse": "CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN length($a) > 0 "
                    "THEN countSubstrings(toTypeName($a), 'Array(') END AS Nullable(Int32)) "
                    "THEN 1 END AS Nullable(Int32))",
                },
            ),
        )
    ),
)

#: A dimension's length, or NULL for a dimension the array does not have: its upper bound, as a
#: list's index starts at 1. The engines' own lengths take no dimension and answer the first one's
#: for any (DuckDB's refuses any other), so this is each one's array_length too.
_DIMENSION_LENGTH: dict[str, str] = {
    "duckdb": r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN len($a) > 0 THEN "
    r"length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER) "
    r"THEN json_array_length(to_json($a), '$' || "
    r"repeat('[0]', CAST($d - 1 AS INTEGER))) END AS INTEGER)",
    **_on_trino(
        r"CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN cardinality($a) > 0 "
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
        r"END END AS INTEGER)"
    ),
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
}

ARRAY_UPPER = Feature(
    "array_upper",
    FAMILY,
    "The upper bound of a dimension: its length.",
    read=Side(
        rules=(
            Rule(
                "array_upper($a::anyarray, $d::integer)",
                {
                    **_on_sources("array_upper"),
                    **_DIMENSION_LENGTH,
                },
            ),
        )
    ),
)

#: The sources a statement goes to directly that hold no array type: array_length is refused there.
_ARRAY_LESS = ("mysql", "sqlite", "oracle", "tsql", "singlestore", "exasol", "redshift")

#: The first dimension's length, NULL for an empty or NULL array, on each dialect whose own array
#: length takes no dimension: the source's length, where it is above 0 (BigQuery's answers 0 of a
#: NULL array, Hive's and Databricks' -1).
_FIRST_LENGTH: dict[str, str] = {
    "duckdb": "CAST(CASE WHEN len($a) > 0 THEN len($a) END AS INTEGER)",
    **_on_trino("CAST(CASE WHEN cardinality($a) > 0 THEN cardinality($a) END AS INTEGER)"),
    "clickhouse": "CAST(CASE WHEN length($a) > 0 THEN length($a) END AS Nullable(Int32))",
    "snowflake": "CAST(CASE WHEN ARRAY_SIZE($a) > 0 THEN ARRAY_SIZE($a) END AS INTEGER)",
    "bigquery": "CASE WHEN ARRAY_LENGTH($a) > 0 THEN ARRAY_LENGTH($a) END",
    "databricks": "CAST(CASE WHEN SIZE($a) > 0 THEN SIZE($a) END AS INT)",
    "hive": "CAST(CASE WHEN SIZE($a) > 0 THEN SIZE($a) END AS INT)",
    "druid": "CASE WHEN ARRAY_LENGTH($a) > 0 THEN ARRAY_LENGTH($a) END",
}

#: The dialects whose own array length answers the first dimension's only (sqlglot drops a
#: dimension argument: "Cannot transpile dimension argument"): any other is refused.
_FIRST_ONLY = ("snowflake", "bigquery", "databricks", "hive", "druid")


def _first_only(dialect: str) -> Refusal:
    return Refusal("0A000", f"array_length() reads only dimension 1 on {dialect}")


ARRAY_LENGTH = Feature(
    "array_length",
    FAMILY,
    "The length of a dimension; NULL for a dimension the array does not have.",
    read=Side(
        rules=(
            # The first dimension, written as the number 1: its length on every dialect with arrays.
            Rule(
                "array_length($a::anyarray, 1)",
                {**_no_form_on("array_length", _ARRAY_LESS), **_FIRST_LENGTH},
            ),
            # generate_subscripts(a, d) is read as its reverse-false form (pg_functions).
            Rule(
                "array_length($a::anyarray, $d::integer)",
                {
                    **_no_form_on("array_length", _ARRAY_LESS),
                    **_DIMENSION_LENGTH,
                    **{dialect: _first_only(dialect) for dialect in _FIRST_ONLY},
                },
            ),
        ),
        limitations={
            dialect: (
                Limitation(
                    "Only the first dimension, written as the number 1: any other dimension, or one"
                    " not written as a number, is refused, where PostgreSQL answers its length or"
                    " NULL.",
                ),
            )
            for dialect in _FIRST_ONLY
        },
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
                    **_on_sources("array_fill"),
                    "duckdb": "CASE WHEN $dims IS NULL THEN error('postgresql-error 22004: ' || "
                    "'dimension array or low bound array cannot be null') "
                    "WHEN len($dims) = 1 THEN list_transform(range($dims[1]), _i -> $v) "
                    "ELSE error('postgresql-error 0A000: ' || 'array_fill does not read this value') "
                    "END",
                    **_on_trino(
                        "CASE WHEN $dims IS NULL THEN fail('postgresql-error 22004: ' || "
                        "'dimension array or low bound array cannot be null' || "
                        "' <end of postgresql-error>') "
                        "WHEN cardinality($dims) = 1 THEN repeat($v, $dims[1]) "
                        "ELSE fail('postgresql-error 0A000: ' || 'array_fill does not read this value' "
                        "|| ' <end of postgresql-error>') END"
                    ),
                    "clickhouse": "arrayWithConstant(throwIf(length($dims) <> 1, "
                    "'postgresql-error 0A000: array_fill does not read this value "
                    "<end of postgresql-error>') + assumeNotNull($dims[1]), $v)",
                },
            ),
            Rule(
                "array_fill($v::anyelement, $dims::integer[], $lower_bounds::integer[])",
                {
                    **_on_sources("array_fill"),
                    "duckdb": "CASE WHEN $dims IS NULL OR $lower_bounds IS NULL THEN "
                    "error('postgresql-error 22004: ' || "
                    "'dimension array or low bound array cannot be null') "
                    "WHEN len($dims) = 1 AND list_filter($lower_bounds, _b -> _b <> 1) = [] "
                    "THEN list_transform(range($dims[1]), _i -> $v) "
                    "ELSE error('postgresql-error 0A000: ' || 'array_fill does not read this value') "
                    "END",
                    **_on_trino(
                        "CASE WHEN $dims IS NULL OR $lower_bounds IS NULL THEN "
                        "fail('postgresql-error 22004: ' || "
                        "'dimension array or low bound array cannot be null' || "
                        "' <end of postgresql-error>') "
                        "WHEN cardinality($dims) = 1 AND all_match($lower_bounds, _b -> _b = 1) "
                        "THEN repeat($v, $dims[1]) "
                        "ELSE fail('postgresql-error 0A000: ' || 'array_fill does not read this value' "
                        "|| ' <end of postgresql-error>') END"
                    ),
                    "clickhouse": "arrayWithConstant(throwIf(length($dims) <> 1 OR "
                    "NOT arrayAll(_b -> _b = 1, $lower_bounds), "
                    "'postgresql-error 0A000: array_fill does not read this value "
                    "<end of postgresql-error>') + assumeNotNull($dims[1]), $v)",
                },
            ),
        ),
        limitations=dict.fromkeys(("duckdb", *_TRINO, "clickhouse"), (_ARRAY_FILL_SHAPE,)),
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
                    **_on_sources("array_positions"),
                    "duckdb": "CAST(list_filter(range(1, len($a) + 1), "
                    "_i -> $a[_i] IS NOT DISTINCT FROM $v) AS INTEGER[])",
                    **_on_trino(
                        "CASE WHEN cardinality($a) = 0 THEN CAST(ARRAY[] AS ARRAY(INTEGER)) "
                        "ELSE CAST(filter(sequence(1, cardinality($a)), "
                        "_i -> $a[_i] IS NOT DISTINCT FROM $v) AS ARRAY(INTEGER)) END"
                    ),
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
                    **_on_sources("array_replace"),
                    "duckdb": "list_transform($a, "
                    "_x -> CASE WHEN _x IS NOT DISTINCT FROM $v THEN $w ELSE _x END)",
                    **_on_trino(
                        "transform($a, "
                        "_x -> CASE WHEN _x IS NOT DISTINCT FROM $v THEN $w ELSE _x END)"
                    ),
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
                    **_on_sources("array_sample"),
                    "duckdb": "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                    "WHEN $n < 0 OR $n > len($a) THEN error('postgresql-error 22023: ' || "
                    "'sample size must be between 0 and ' || len($a)) "
                    "ELSE list_transform(list_sort(list_transform($a, "
                    "_x -> {'k': random(), 'v': _x})), _e -> _e.v)[1:$n] END",
                    **_on_trino(
                        "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                        "WHEN $n < 0 OR $n > cardinality($a) THEN fail('postgresql-error 22023: ' || "
                        "'sample size must be between 0 and ' || CAST(cardinality($a) AS VARCHAR) || "
                        "' <end of postgresql-error>') ELSE slice(shuffle($a), 1, $n) END"
                    ),
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
                    **_on_sources("array_shuffle"),
                    "duckdb": "list_transform(list_sort(list_transform($a, "
                    "_x -> {'k': random(), 'v': _x})), _e -> _e.v)",
                    **_on_trino("shuffle($a)"),
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
                    **_on_sources("trim_array"),
                    "duckdb": "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                    "WHEN $n < 0 OR $n > len($a) THEN error('postgresql-error 2202E: ' || "
                    "'number of elements to trim must be between 0 and ' || len($a)) "
                    "ELSE $a[1:len($a) - $n] END",
                    **_on_trino(
                        "CASE WHEN $a IS NULL OR $n IS NULL THEN NULL "
                        "WHEN $n < 0 OR $n > cardinality($a) THEN fail('postgresql-error 2202E: ' || "
                        "'number of elements to trim must be between 0 and ' || "
                        "CAST(cardinality($a) AS VARCHAR) || ' <end of postgresql-error>') "
                        "ELSE slice($a, 1, cardinality($a) - $n) END"
                    ),
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
                    **_on_sources("generate_subscripts"),
                    "duckdb": r"unnest(CASE WHEN $reverse THEN range(CAST(CASE WHEN $d BETWEEN 1 "
                    r"AND CAST(CASE WHEN len($a) > 0 THEN length(regexp_extract(typeof($a), "
                    r"'(\[\])+$')) // 2 END AS INTEGER) THEN json_array_length(to_json($a), '$' "
                    r"|| repeat('[0]', CAST($d - 1 AS INTEGER))) END AS INTEGER), 0, -1) "
                    r"ELSE range(1, CAST(CASE WHEN $d BETWEEN 1 AND CAST(CASE WHEN len($a) > 0 "
                    r"THEN length(regexp_extract(typeof($a), '(\[\])+$')) // 2 END AS INTEGER) "
                    r"THEN json_array_length(to_json($a), '$' || "
                    r"repeat('[0]', CAST($d - 1 AS INTEGER))) END AS INTEGER) + 1) END)",
                    **_no_form_on("generate_subscripts", (*_TRINO, "clickhouse")),
                },
            ),
            # Of no order: Trino has no form of either (run 43307392116 sent it as written); every
            # other dialect keeps what it had.
            Rule(
                "generate_subscripts($a::anyarray, $d::integer)",
                {
                    **dict.fromkeys(trino.OTHERS, trino.KEPT),
                    **trino.refused_on_trino("generate_subscripts"),
                },
            ),
        ),
    ),
)

#: PostgreSQL 17 (array_userfuncs.c) reads a NULL array given to array_append or array_prepend as
#: an empty one, and array_cat answers the other argument, NULL only where both are NULL. ClickHouse
#: has no NULL of an array (an Array is never Nullable): each is written as that answer there.


def _no_array_append(function: str) -> Limitation:
    """ClickHouse has no function of PostgreSQL's name: only a NULL array has a form."""
    return Limitation(
        f"Only a NULL array is written: ClickHouse has no {function}, so a call on an array that "
        "is not NULL fails with ClickHouse's own error."
    )


ARRAY_APPEND = Feature(
    "array_append",
    FAMILY,
    "An array with an element added at its end; a NULL array is read as an empty one.",
    read=Side(
        rules=(
            Rule(
                "array_append(NULL::anycompatiblearray, $v::anycompatible)",
                {"clickhouse": "[$v]", **_on_trino("ARRAY[$v]")},
            ),
            # Trino has no array_append: its concatenation, a NULL array read as an empty one.
            Rule(
                "array_append($a::anycompatiblearray, $v::anycompatible)",
                _on_trino("CASE WHEN $a IS NULL THEN ARRAY[$v] ELSE $a || ARRAY[$v] END"),
            ),
        ),
        limitations={"clickhouse": (_no_array_append("array_append"),)},
    ),
)

ARRAY_PREPEND = Feature(
    "array_prepend",
    FAMILY,
    "An array with an element added at its start; a NULL array is read as an empty one.",
    read=Side(
        rules=(
            Rule(
                "array_prepend($v::anycompatible, NULL::anycompatiblearray)",
                {"clickhouse": "[$v]", **_on_trino("ARRAY[$v]")},
            ),
            # Trino has no array_prepend: its concatenation, a NULL array read as an empty one.
            Rule(
                "array_prepend($v::anycompatible, $a::anycompatiblearray)",
                _on_trino("CASE WHEN $a IS NULL THEN ARRAY[$v] ELSE ARRAY[$v] || $a END"),
            ),
        ),
        limitations={"clickhouse": (_no_array_append("array_prepend"),)},
    ),
)

#: The dialects with no form of array_cat.
_NO_ARRAY_CAT = tuple(d for d in DIALECTS if d not in {*NATIVE, "duckdb", "clickhouse", *_TRINO})

ARRAY_CAT = Feature(
    "array_cat",
    FAMILY,
    "Two arrays concatenated; a NULL array is the other one, and two NULL arrays are NULL.",
    read=Side(
        rules=(
            # Both NULL: ClickHouse's NULL, as for a STRICT call of a NULL array
            # (provisa.transpiler.function_macros), before a rule is read.
            Rule(
                "array_cat(NULL::anycompatiblearray, $b::anycompatiblearray)",
                {
                    **_no_form_on("array_cat", _NO_ARRAY_CAT),
                    "duckdb": "$b",
                    **_on_trino("$b"),
                    "clickhouse": "$b",
                },
            ),
            Rule(
                "array_cat($a::anycompatiblearray, NULL::anycompatiblearray)",
                {
                    **_no_form_on("array_cat", _NO_ARRAY_CAT),
                    "duckdb": "$a",
                    **_on_trino("$a"),
                    "clickhouse": "$a",
                },
            ),
            Rule(
                "array_cat($a::anycompatiblearray, $b::anycompatiblearray)",
                {
                    **_no_form_on("array_cat", (*_NO_ARRAY_CAT, "clickhouse")),
                    # DuckDB's list_concat reads a NULL list as an empty one.
                    "duckdb": "CASE WHEN $a IS NOT NULL OR $b IS NOT NULL "
                    "THEN list_concat($a, $b) END",
                    # Trino's concat of arrays is NULL where either is: a column can be NULL at run
                    # time, so a NULL array is the other one before concat is reached.
                    **_on_trino(
                        "CASE WHEN $a IS NULL THEN $b WHEN $b IS NULL THEN $a ELSE concat($a, $b) END"
                    ),
                },
            ),
        ),
    ),
)


def _equality(name: str, summary: str, operator: str, trino_test: str) -> Feature:
    """array_eq or array_ne: an array's btree comparison, a NULL element equal to a NULL element
    (arrayfuncs.c compares two NULL elements as equal), strict in its arrays. DuckDB's and
    ClickHouse's own comparison of two lists takes a NULL element so, and every other dialect is
    sent the form it had; Trino's ``=`` of two arrays is NULL where an element is, and its
    ``IS NOT DISTINCT FROM`` compares them element by element as PostgreSQL does."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    f"{name}($a::anyarray, $b::anyarray)",
                    {
                        **{d: f"$a {operator} $b" for d in DIALECTS if d not in NATIVE},
                        **_on_trino(
                            "CASE WHEN $a IS NULL OR $b IS NULL THEN CAST(NULL AS BOOLEAN) "
                            f"ELSE $a {trino_test} $b END"
                        ),
                    },
                ),
            )
        ),
    )


ARRAY_EQ = _equality(
    "array_eq", "Two arrays equal, a NULL element equal to a NULL element.", "=",
    "IS NOT DISTINCT FROM",
)  # fmt: skip
ARRAY_NE = _equality(
    "array_ne", "Two arrays not equal, a NULL element equal to a NULL element.", "<>",
    "IS DISTINCT FROM",
)  # fmt: skip

ARRAY_POSITION = Feature(
    "array_position",
    FAMILY,
    "The position of the first element equal to a value, a NULL equal to a NULL; NULL for none.",
    read=Side(
        rules=(
            # Trino's array_position answers 0 for a value it does not find, and NULL for a NULL
            # value: PostgreSQL answers NULL, and finds a NULL. Its sequence takes no empty range.
            Rule(
                "array_position($a::anycompatiblearray, $v::anycompatible)",
                _on_trino(
                    "CAST(CASE WHEN cardinality($a) > 0 THEN element_at(filter(sequence(1, "
                    "cardinality($a)), _i -> $a[_i] IS NOT DISTINCT FROM $v), 1) END AS INTEGER)"
                ),
            ),
            # From a start (sqlglot reads it as ArrayPosition's zero_based): DuckDB's from its
            # slice; any other dialect refuses it by name.
            Rule(
                "array_position($a::anycompatiblearray, $v::anycompatible, $s::integer)",
                {
                    **_no_form_on(
                        "array_position", tuple(d for d in DIALECTS if d not in {*NATIVE, "duckdb"})
                    ),
                    "duckdb": "list_position($a[greatest($s, 1):], $v) + greatest($s, 1) - 1",
                },
            ),
        )
    ),
)


#: The element types whose order on Trino is PostgreSQL's: a number past a float's NaN, a truth
#: value and a day. Text orders by the database's collation, where Trino orders by code points.
_ORDERED_ELEMENTS = frozenset({"smallint[]", "integer[]", "bigint[]", "boolean[]", "date[]"})

#: array_cmp (arrayfuncs.c) of two one-dimensional arrays: the first element that differs, a NULL
#: larger than any value and equal to a NULL; past the shorter one's end, the longer is larger. A
#: nested list (an array of more dimensions) is refused, as its order is of its flattened elements.
_ARRAY_CMP = (
    "CASE WHEN $a IS NULL OR $b IS NULL THEN CAST(NULL AS INTEGER) "
    "WHEN strpos(typeof($a), 'array(array(') = 1 OR strpos(typeof($b), 'array(array(') = 1 "
    "THEN fail('postgresql-error 0A000: ' || '{name} does not read this value' "
    "|| ' <end of postgresql-error>') "
    "ELSE CAST(coalesce(element_at(filter(transform("
    "sequence(1, least(cardinality($a), cardinality($b)) + 1), "
    "_i -> CASE WHEN _i > least(cardinality($a), cardinality($b)) "
    "THEN sign(cardinality($a) - cardinality($b)) "
    "WHEN $a[_i] IS NULL AND $b[_i] IS NULL THEN 0 WHEN $a[_i] IS NULL THEN 1 "
    "WHEN $b[_i] IS NULL THEN -1 WHEN $a[_i] < $b[_i] THEN -1 WHEN $a[_i] > $b[_i] THEN 1 "
    "ELSE 0 END), _c -> _c <> 0), 1), 0) AS INTEGER) END"
)

#: Each ordering function, its pattern, and Trino's answer from the comparison ``{cmp}``.
_ORDERINGS = {
    "array_lt": "{cmp} < 0",
    "array_le": "{cmp} <= 0",
    "array_gt": "{cmp} > 0",
    "array_ge": "{cmp} >= 0",
    "array_larger": "CASE WHEN $a IS NULL OR $b IS NULL THEN NULL WHEN {cmp} > 0 THEN $a ELSE $b END",
    "array_smaller": "CASE WHEN $a IS NULL OR $b IS NULL THEN NULL WHEN {cmp} < 0 THEN $a ELSE $b END",
    "btarraycmp": "{cmp}",
}


def _ordering_pattern(name: str) -> str:
    return f"{name}($a::anyarray, $b::anyarray)"


def ordered_arrays(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """Two arrays ordered as btree's array_cmp orders them, on Trino, where both are said to hold
    elements Trino orders as PostgreSQL does; any other is refused by name."""
    call = trino.called(node)
    if call is None:
        return None
    (name,) = call[0] & set(_ORDERINGS)
    a, b = call[1]
    if trino.said(a) not in _ORDERED_ELEMENTS or trino.said(b) != trino.said(a):
        raise Refused(trino.no_form(name, dialect))
    cmp = "(" + _ARRAY_CMP.format(name=name) + ")"
    form = _ORDERINGS[name].format(cmp=cmp)
    return trino.written(form, _ordering_pattern(name), {"a": a, "b": b})


_ORDERED = Rewrite(ordered_arrays)
_ORDER_SUMMARIES = {
    "array_lt": ("<", "One array before another, a NULL element after any value."),
    "array_le": ("<=", "One array before or equal to another, a NULL element after any value."),
    "array_gt": (">", "One array after another, a NULL element after any value."),
    "array_ge": (">=", "One array after or equal to another, a NULL element after any value."),
}
_ORDER_LIMITATION = Limitation(
    "Only arrays of one dimension said to be of smallint, integer, bigint, boolean or date: any "
    "other (text, which orders by the database's collation, or a column of no said type) is "
    "refused by name, as Trino's own comparison refuses a NULL element."
)


def _ordering(name: str, summary: str, others: str) -> Feature:
    """An ordering of two arrays: Trino's from array_cmp, every other dialect the form it had."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    _ordering_pattern(name),
                    {
                        **{d: others for d in DIALECTS if d not in NATIVE},
                        **trino.on_trino(_ORDERED),
                    },
                ),
            ),
            limitations=dict.fromkeys(_TRINO, (_ORDER_LIMITATION,)),
        ),
    )


ARRAY_LT, ARRAY_LE, ARRAY_GT, ARRAY_GE = (
    _ordering(name, summary, f"$a {operator} $b")
    for name, (operator, summary) in _ORDER_SUMMARIES.items()
)
ARRAY_LARGER = _ordering(
    "array_larger",
    "The larger of two arrays, a NULL element after any value.",
    "CASE WHEN $a > $b THEN $a WHEN $a <= $b THEN $b END",
)
ARRAY_SMALLER = _ordering(
    "array_smaller",
    "The smaller of two arrays, a NULL element after any value.",
    "CASE WHEN $a < $b THEN $a WHEN $a >= $b THEN $b END",
)
#: btarraycmp: every other dialect keeps the old table's (provisa.transpiler.pg_forms_index), which
#: reads an array's elements as written first.
BTARRAYCMP = Feature(
    "btarraycmp",
    FAMILY,
    "btree's comparison of two arrays: -1, 0 or 1, a NULL element after any value.",
    read=Side(
        rules=(Rule(_ordering_pattern("btarraycmp"), trino.trino_only(_ORDERED)),),
        limitations=dict.fromkeys(_TRINO, (_ORDER_LIMITATION,)),
    ),
)


FEATURES: tuple[Feature, ...] = (
    ARRAY_NDIMS,
    ARRAY_DIMS,
    ARRAY_LOWER,
    ARRAY_UPPER,
    ARRAY_LENGTH,
    ARRAY_FILL,
    ARRAY_EQ,
    ARRAY_NE,
    ARRAY_POSITION,
    ARRAY_POSITIONS,
    ARRAY_REPLACE,
    ARRAY_SAMPLE,
    ARRAY_SHUFFLE,
    TRIM_ARRAY,
    GENERATE_SUBSCRIPTS,
    ARRAY_APPEND,
    ARRAY_PREPEND,
    ARRAY_CAT,
    ARRAY_LT,
    ARRAY_LE,
    ARRAY_GT,
    ARRAY_GE,
    ARRAY_LARGER,
    ARRAY_SMALLER,
    BTARRAYCMP,
)
