# Copyright (c) 2026 Kenneth Stott
# Canary: 8ec19f2d-fe24-4e4a-87dc-79bf65c48353
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The function table's rows of numbers (REQ-2167, REQ-2171): ``ROWS``, which
provisa.transpiler.rows collects into provisa.transpiler.function_macros.MACROS."""

# Requirements: REQ-2167, REQ-2171, REQ-2236

from __future__ import annotations

from provisa.transpiler import pg_forms, pg_forms_types
from provisa.transpiler.rows.base import (
    _CASTS_CHECKED,
    _CLICKHOUSE,
    _DUCKDB,
    _EVERY,
    _NO_SPLIT_PART,
    _ORDER,
    _TRINO,
    MAPPED,
    Macro,
    Pg,
    _macro,
    _operated,
    _operators,
    _pg,
    strict,
)

#: round/trunc(numeric, n) on DuckDB: NULL where the scale is NULL, which DuckDB 2.0 refuses to bind.
_NULL_SCALE = "CASE WHEN n IS NULL THEN NULL ELSE {}(x, COALESCE(n, 0)) END"

#: uuid_extract_timestamp, as PostgreSQL 17's uuid.c reads it: a version 7 UUID's milliseconds
#: (DuckDB's own), a version 1 UUID's 100-nanosecond count since 1582-10-15, and NULL for any other
#: version or a variant that is not RFC 9562's. DuckDB's own refuses all but version 7.
_UUID_HEX = "replace(CAST(u AS VARCHAR), '-', '')"
_UUID_TIMESTAMP = (
    f"CASE WHEN (CAST('0x' || substr({_UUID_HEX}, 17, 1) AS INTEGER) & 12) <> 8 THEN NULL "
    "WHEN uuid_extract_version(u) = 7 THEN uuid_extract_timestamp(u) "
    "WHEN uuid_extract_version(u) = 1 THEN TIMESTAMPTZ '1970-01-01 00:00:00+00' + to_microseconds("
    f"((CAST('0x' || substr({_UUID_HEX}, 14, 3) AS BIGINT) << 48) "
    f"| (CAST('0x' || substr({_UUID_HEX}, 9, 4) AS BIGINT) << 32) "
    f"| CAST('0x' || substr({_UUID_HEX}, 1, 8) AS BIGINT)) // 10 - 12219292800000000) END"
)

#: The bits of an integer's two's complement, as to_bin and to_oct read them.
_TWOS_COMPLEMENT = (
    "(CAST(value AS HUGEINT) & CASE WHEN typeof(value) IN ('BIGINT', 'HUGEINT') "
    "THEN CAST(18446744073709551615 AS HUGEINT) ELSE CAST(4294967295 AS HUGEINT) END)"
)


#: Integers of two widths: each pair's result is the wider of the two.
_INTEGER_PAIRS = (
    ("int24", "SMALLINT", "INTEGER"),
    ("int42", "INTEGER", "SMALLINT"),
    ("int28", "SMALLINT", "BIGINT"),
    ("int82", "BIGINT", "SMALLINT"),
    ("int48", "INTEGER", "BIGINT"),
    ("int84", "BIGINT", "INTEGER"),
)
_FLOAT_PAIRS = (("float48", "FLOAT", "DOUBLE"), ("float84", "DOUBLE", "FLOAT"))
_ARITHMETIC = {"pl": "+", "mi": "-", "mul": "*", "div": "/"}


def _wider(left: str, right: str) -> str:
    return "BIGINT" if "BIGINT" in (left, right) else "INTEGER"


def _integer_pairs() -> tuple[Macro, ...]:
    def compared(operator: str, left: str, right: str) -> str:
        return f"CAST(a AS {left}) {operator} CAST(b AS {right})"

    wider = {"INTEGER": "int4", "BIGINT": "int8"}
    return (
        *(
            # An integer of two widths is its wider type's operator: PostgreSQL's own promotion.
            _operated(f"{prefix}{suffix}", f"{wider[_wider(left, right)]}{suffix}")
            for prefix, left, right in _INTEGER_PAIRS
            for suffix in _ARITHMETIC
        ),
        *(
            _operators(
                f"{prefix}{suffix}",
                "a, b",
                compared(operator, left, right),
                {_EVERY: Pg(compared(operator, left, right))},
            )
            for prefix, left, right in _INTEGER_PAIRS
            for suffix, operator in _ORDER.items()
        ),
    )


def _float_pairs() -> tuple[Macro, ...]:
    """A real and a double precision are double precision's operator, as PostgreSQL promotes."""
    return tuple(
        _operated(f"{prefix}{suffix}", f"float8{suffix}")
        for prefix, _, _ in _FLOAT_PAIRS
        for suffix in (*_ARITHMETIC, *_ORDER)
    )


def _rounded(integer: str) -> str:
    """A float's cast to an integer: rint's, and in that type's range."""
    return pg_forms_types.integer_of(pg_forms_types.rint("value"), integer)


def _numeric_conversions() -> tuple[Macro, ...]:
    """A function named for a number type: its own row reads what the engine holds; a numeric, a
    jsonb, a bit and a "char" have rows of their own, as pg_proc's are, chosen by OVERLOADS."""
    digits = pg_forms.JSONB_DIGITS

    def as_integer(sql_type: str) -> str:
        return pg_forms.jsonb_number(
            pg_forms.numeric_as_integer(digits, sql_type), pg_forms.PG_INTEGERS[sql_type]
        )

    def as_float(sql_type: str) -> str:
        return pg_forms.jsonb_number(
            pg_forms.numeric_as_float(digits, sql_type), pg_forms.PG_FLOATS[sql_type]
        )

    integers = (("int2", "SMALLINT"), ("int4", "INTEGER"), ("int8", "BIGINT"))
    floats = (("float4", "FLOAT"), ("float8", "DOUBLE"))
    own_floats = {"FLOAT": pg_forms.AS_REAL, "DOUBLE": "CAST(value AS DOUBLE)"}
    pg_floats = {"float4": pg_forms.PG_AS_REAL, "float8": "CAST(value AS DOUBLE PRECISION)"}
    return (
        *(
            _macro(
                name,
                "value",
                strict(
                    "value",
                    sql_type,
                    pg_forms.of_any_number(
                        "value", as_integer(sql_type), pg_forms.in_range("value", sql_type)
                    ),
                ),
                {_CASTS_CHECKED: Pg(pg_forms_types.integer_of("value", name))},
            )
            for name, sql_type in integers
        ),
        *(
            _pg(f"{of}to{name[3:] and 'i' + name[3:]}", "value", _rounded(name))
            for of in ("d", "f")
            for name, _ in integers
        ),
        *(
            _macro(
                name,
                "value",
                strict(
                    "value",
                    sql_type,
                    pg_forms.of_any_number("value", as_float(sql_type), own_floats[sql_type]),
                ),
                {_CASTS_CHECKED: Pg(pg_floats[name])},
            )
            for name, sql_type in floats
        ),
        *(
            _macro(
                f"numeric_{name}",
                "value",
                strict("value", sql_type, pg_forms.numeric_as_integer(digits, sql_type)),
                {_CASTS_CHECKED: Pg(pg_forms_types.integer_of("ROUND(value)", name))},
            )
            for name, sql_type in integers
        ),
        *(
            _macro(
                f"numeric_{name}",
                "value",
                strict("value", sql_type, pg_forms.numeric_as_float(digits, sql_type)),
                {_CASTS_CHECKED: Pg(pg_floats[name])},
            )
            for name, sql_type in floats
        ),
        _macro(
            "jsonb_numeric",
            "value",
            strict("value", "VARCHAR", pg_forms.jsonb_number(digits, "numeric")),
        ),
        *(
            _macro(f"jsonb_{name}", "value", strict("value", sql_type, as_integer(sql_type)))
            for name, sql_type in integers
        ),
        *(
            _macro(f"jsonb_{name}", "value", strict("value", sql_type, as_float(sql_type)))
            for name, sql_type in floats
        ),
        _macro(
            "bittoint4", "value", strict("value", "INTEGER", pg_forms.bit_as_integer("INTEGER", 32))
        ),
        _macro(
            "bittoint8", "value", strict("value", "BIGINT", pg_forms.bit_as_integer("BIGINT", 64))
        ),
        _macro("chartoi4", "value", strict("value", "INTEGER", pg_forms.CHAR_AS_INTEGER)),
    )


#: The regression aggregates read only the rows where both arguments are not NULL.
_PAIRED_X = "CASE WHEN y IS NOT NULL THEN x END"
_PAIRED_Y = "CASE WHEN x IS NOT NULL THEN y END"
_SLOPE = f"COVAR_POP(y, x) / VAR_POP({_PAIRED_X})"


def _regression(name: str, form: str, native: frozenset[str] = frozenset()) -> Macro:
    native_call = Pg(f"{name.upper()}(y, x)")
    forms: dict[frozenset[str], str] = {_DUCKDB: native_call, _EVERY: Pg(form)}
    return Macro(name, ("y", "x"), forms, own={native: MAPPED} if native else {})


_REGRESSIONS: tuple[Macro, ...] = (
    _regression("regr_count", "COUNT(CASE WHEN y IS NOT NULL AND x IS NOT NULL THEN 1 END)"),
    _regression("regr_avgx", f"AVG({_PAIRED_X})"),
    _regression("regr_avgy", f"AVG({_PAIRED_Y})"),
    _regression("regr_sxx", f"VAR_POP({_PAIRED_X}) * COUNT({_PAIRED_X})"),
    _regression("regr_syy", f"VAR_POP({_PAIRED_Y}) * COUNT({_PAIRED_Y})"),
    _regression("regr_sxy", f"COVAR_POP(y, x) * COUNT({_PAIRED_X})"),
    _regression(
        "regr_r2",
        f"CASE WHEN VAR_POP({_PAIRED_X}) = 0 THEN NULL WHEN VAR_POP({_PAIRED_Y}) = 0 THEN 1.0 "
        "ELSE POWER(CORR(y, x), 2) END",
    ),
    _regression("regr_slope", f"CASE WHEN VAR_POP({_PAIRED_X}) <> 0 THEN {_SLOPE} END", _TRINO),
    _regression(
        "regr_intercept",
        f"CASE WHEN VAR_POP({_PAIRED_X}) <> 0 "
        f"THEN AVG({_PAIRED_Y}) - {_SLOPE} * AVG({_PAIRED_X}) END",
        _TRINO,
    ),
)

_NUMBERS: tuple[Macro, ...] = (
    *_integer_pairs(),
    *_float_pairs(),
    *_numeric_conversions(),
    _pg("dexp", "x", strict("x", "DOUBLE", pg_forms.DEXP), _CLICKHOUSE),  # engine_refusals: exp
    _pg("dlog1", "x", strict("x", "DOUBLE", pg_forms.DLOG1), _CLICKHOUSE),  # and ln
    _pg("dlog10", "x", strict("x", "DOUBLE", pg_forms.DLOG10)),
    _pg("dsqrt", "x", strict("x", "DOUBLE", pg_forms.DSQRT)),
    _pg("dpow", "x, y", strict("x, y", "DOUBLE", pg_forms.DPOW)),
    _pg("cosd", "x", strict("x", "DOUBLE", pg_forms.COSD)),
    _pg("cotd", "x", strict("x", "DOUBLE", pg_forms.COTD)),
    _pg("asind", "x", strict("x", "DOUBLE", pg_forms.ASIND)),
    _pg("acosd", "x", strict("x", "DOUBLE", pg_forms.ACOSD)),
    _pg("atand", "x", pg_forms.ATAND),
    _pg("atan2d", "x, y", pg_forms.ATAN2D),
    Macro(
        "erf",
        ("x",),
        {_EVERY: Pg(strict("x", "DOUBLE", pg_forms.ERF))},
        own={_CLICKHOUSE: "erf(x)"},
    ),
    Macro(
        "erfc",
        ("x",),
        {_EVERY: Pg(strict("x", "DOUBLE", pg_forms.ERFC))},
        own={_CLICKHOUSE: "erfc(x)"},
    ),
    *(
        Macro(
            name,
            ("x",),
            {
                _DUCKDB: Pg(f"{name.upper()}(x)"),
                **({_TRINO: Pg(pg_forms.TRINO_ASINH)} if name == "asinh" else {}),
                _EVERY: Pg(form),
            },
            own={_CLICKHOUSE: MAPPED},
        )
        for name, form in pg_forms.HYPERBOLICS.items()
    ),
    Macro("cot", ("x",), {_DUCKDB: Pg("COT(x)"), _EVERY: Pg(pg_forms.COT)}),
    _pg("log", "x", "LOG10(x)"),
    _pg("log10", "x", "LOG10(x)"),
    _pg("random_normal", "", pg_forms.RANDOM_NORMAL.format(mean="0.0", stddev="1.0")),
    _pg(
        "random_normal",
        "mean",
        strict("mean", "DOUBLE", pg_forms.RANDOM_NORMAL.format(mean="mean", stddev="1.0")),
    ),
    _pg(
        "random_normal",
        "mean, stddev",
        strict(
            "mean, stddev",
            "DOUBLE",
            pg_forms.RANDOM_NORMAL.format(mean="mean", stddev="stddev"),
        ),
    ),
    _pg("trim_scale", "value", strict("value", "TEXT", pg_forms.TRIM_SCALE)),
    _pg("min_scale", "value", strict("value", "INTEGER", pg_forms.MIN_SCALE), _NO_SPLIT_PART),
    *_REGRESSIONS,
    _pg("int8dec", "x", pg_forms_types.integer_of("(CAST(x AS DECIMAL(38, 0)) - 1)", "int8")),
    *(
        Macro(
            f"numeric_{name}",
            ("x",),
            {
                _DUCKDB: Pg(f"{name.upper()}(x)"),
                _EVERY: Pg(f"CAST({name.upper()}(x) AS DECIMAL(38, 0))"),
            },
        )
        for name in ("ceil", "floor", "round", "trunc")
    ),
    _pg("bool_int4", "value", "CASE WHEN value THEN 1 WHEN NOT value THEN 0 END"),
    _pg("float4_avg", "x", "AVG(CAST(x AS DOUBLE PRECISION))"),
    _macro("int2int4_sum", "v", strict("v", "BIGINT", pg_forms.INT2INT4_SUM)),
    _macro("uuid_extract_timestamp", "u", strict("u", "TIMESTAMPTZ", _UUID_TIMESTAMP)),
    _macro("int2vectorsend", "v", strict("v", "BLOB", pg_forms.INT2VECTOR_SEND)),
    _macro(
        "integer_pl_date",
        "n, d",
        strict(
            "n, d",
            "DATE",
            f"CASE WHEN isfinite(d) THEN {pg_forms.DATE_PLUS_DAYS} ELSE d END",
        ),
    ),
)

#: Where an engine's own function, given these, answers in other digits or another type than
#: PostgreSQL's exact numeric (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): DuckDB's forms read
#: the value's digits (provisa.federation.duckdb_pg_numeric), and no other dialect has a form.
_NUMERIC_TEXT = "CAST(a0 AS VARCHAR)"
_SECOND_NUMERIC = "CAST(a1 AS VARCHAR)"
_AVG = "provisa_numeric_quotient(CAST(sum(a0) AS VARCHAR), CAST(count(a0) AS VARCHAR))"
_WHOLE_SQUARES = "sum(CAST(a0 AS HUGEINT) * CAST(a0 AS HUGEINT))"


def _exact_unary(function: str) -> str:
    return f"provisa_numeric_unary('{function}', {_NUMERIC_TEXT})"


def _exact_binary(function: str) -> str:
    return f"provisa_numeric_binary('{function}', {_NUMERIC_TEXT}, {_SECOND_NUMERIC})"


def _spread(function: str, squares: str) -> str:
    return (
        f"provisa_numeric_spread('{function}', count(a0), CAST(sum(a0) AS VARCHAR), "
        f"CAST({squares} AS VARCHAR))"
    )


_SPREADS = ("stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp")
_EXACT_ROWS: tuple[Macro, ...] = (
    Macro("exact_avg", ("a0",), {_DUCKDB: _AVG}),
    *(Macro(f"exact_{n}", ("a0",), {_DUCKDB: _spread(n, _WHOLE_SQUARES)}) for n in _SPREADS),
    *(
        Macro(f"exact_{n}_numeric", ("a0",), {_DUCKDB: _spread(n, "sum(a0 * a0)")})
        for n in _SPREADS
    ),
    *(
        Macro(f"exact_{n}", ("a0",), {_DUCKDB: _exact_unary(n)})
        for n in ("exp", "ln", "log10", "sqrt")
    ),
    Macro("exact_log", ("a0",), {_DUCKDB: _exact_unary("log10")}),
    Macro("exact_log", ("a0", "a1"), {_DUCKDB: _exact_binary("log")}),
    Macro("exact_power", ("a0", "a1"), {_DUCKDB: _exact_binary("power")}),
    Macro("exact_pow", ("a0", "a1"), {_DUCKDB: _exact_binary("power")}),
    Macro("exact_div", ("a0", "a1"), {_DUCKDB: _exact_binary("div")}),
    Macro("exact_float8_numeric", ("a0",), {_DUCKDB: "provisa_numeric_of_float(a0, 15)"}),
    Macro("exact_float4_numeric", ("a0",), {_DUCKDB: "provisa_numeric_of_float(a0, 6)"}),
    Macro("exact_pg_size_pretty", ("a0",), {_DUCKDB: _exact_unary("pg_size_pretty")}),
    # interval_avg cascades a month's remainder, and DuckDB has no sum of an interval.
    Macro("exact_avg_interval", ("a0",)),
    # What regexp_replace's Nth match is takes the engine's own search.
    Macro(
        "regexp_replace_at",
        ("value", "pattern", "replacement", "start"),
        {
            _DUCKDB: "CASE WHEN contains(pattern, '^') OR start < 1 THEN "
            + pg_forms_types.refused(
                "0A000", f"'regexp_replace {pg_forms_types.REFUSED}'", "VARCHAR"
            )
            + " ELSE left(value, start - 1) || "
            "regexp_replace(substring(value, start), pattern, replacement) END"
        },
    ),
)
#: The overloads of the exact rows, by what the statement says of their first argument.
_EXACT_OVERLOADS: dict[tuple[str, int, str], str] = {
    **{("avg", 1, of): "exact_avg" for of in ("smallint", "int", "bigint", "decimal")},
    ("avg", 1, "interval"): "exact_avg_interval",
    **{(n, 1, of): f"exact_{n}" for n in _SPREADS for of in ("smallint", "int", "bigint")},
    **{(n, 1, "decimal"): f"exact_{n}_numeric" for n in _SPREADS},
    **{(n, 1, "decimal"): f"exact_{n}" for n in ("exp", "ln", "log10", "sqrt", "log")},
    **{(n, 2, "decimal"): f"exact_{n}" for n in ("log", "power", "pow", "div")},
    ("numeric", 1, "double precision"): "exact_float8_numeric",
    ("numeric", 1, "real"): "exact_float4_numeric",
    ("pg_size_pretty", 1, "decimal"): "exact_pg_size_pretty",
}

ROWS: tuple[Macro, ...] = (
    # DuckDB 2.0 refuses a NULL scale when it binds (1.5 answered NULL, as PostgreSQL does).
    Macro(
        "round_numeric",
        ("x", "n"),
        {_DUCKDB: Pg(_NULL_SCALE.format("ROUND")), _EVERY: Pg("ROUND(x, n)")},
    ),
    Macro(
        "trunc_numeric",
        ("x", "n"),
        {_DUCKDB: Pg(_NULL_SCALE.format("TRUNC")), _EVERY: Pg("TRUNC(x, n)")},
    ),
    _macro("to_number", "value, format", "provisa_to_number(value, format)"),
    _macro("to_hex", "value", f"format('{{:x}}', {_TWOS_COMPLEMENT})"),
    _macro("pg_to_hex", "value", f"format('{{:x}}', {_TWOS_COMPLEMENT})"),  # as readable writes it
    _pg("dcbrt", "x", "CBRT(x)"),
    _macro(
        "dround",
        "x",
        "round_even(CAST(x AS DOUBLE), 0)",
        {_EVERY: Pg(pg_forms_types.rint("x"))},
    ),
    _pg("dtrunc", "x", "TRUNC(CAST(x AS DOUBLE PRECISION))"),
    _macro(
        "sind",
        "x",
        "CASE WHEN isnan(CAST(x AS DOUBLE)) THEN CAST(x AS DOUBLE) ELSE provisa_sind(x) END",
        {_CASTS_CHECKED: Pg(strict("x", "DOUBLE", pg_forms.SIND))},
    ),
    _macro(
        "tand",
        "x",
        "CASE WHEN isnan(CAST(x AS DOUBLE)) THEN CAST(x AS DOUBLE) ELSE provisa_tand(x) END",
        {_CASTS_CHECKED: Pg(strict("x", "DOUBLE", pg_forms.TAND))},
    ),
    _pg(
        "scale",
        "x",
        "CASE WHEN x IS NULL THEN CAST(NULL AS INTEGER) "
        "WHEN STRPOS(CAST(x AS TEXT), '.') > 0 "
        "THEN CAST(LENGTH(SPLIT_PART(CAST(x AS TEXT), '.', 2)) AS INTEGER) ELSE 0 END",
        _NO_SPLIT_PART,
    ),
    _macro("to_bin", "value", f"format('{{:b}}', {_TWOS_COMPLEMENT})"),
    _macro("to_oct", "value", f"format('{{:o}}', {_TWOS_COMPLEMENT})"),
    *_NUMBERS,
    *_EXACT_ROWS,
)
