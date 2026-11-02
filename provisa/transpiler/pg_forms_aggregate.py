# Copyright (c) 2026 Kenneth Stott
# Canary: b7e41d09-3c58-4f26-a90b-5d17c2e8f630
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's aggregate state functions a client can call (REQ-2171): each a step of an aggregate
over an array, so a form of array and arithmetic operators in PostgreSQL's syntax, which sqlglot
renders for every dialect, and the final functions that read such a state. Each follows
src/backend/utils/adt/float.c or numeric.c line by line, with its checks: a state of the wrong
size, and a float's overflow. The state functions of type internal have no call from SQL at all."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from provisa.transpiler.pg_forms_types import Form, refused

_FLOATS = "DOUBLE PRECISION[]"
_INTEGERS = "BIGINT[]"
_INTERNAL_ERROR = "XX000"
_NUMERIC_OUT_OF_RANGE = "22003"
_NAN = "CAST('NaN' AS DOUBLE PRECISION)"
_INFINITY = "CAST('Infinity' AS DOUBLE PRECISION)"
_OVERFLOW = refused(_NUMERIC_OUT_OF_RANGE, "'value out of range: overflow'", "DOUBLE PRECISION")


def _inf(value: str) -> str:
    return f"(abs({value}) = {_INFINITY})"


def _array_size(state: str) -> str:
    return f"COALESCE(array_length({state}, 1), 0)"


def _wrong_size(state: str, size: int) -> str:
    nulls = " OR ".join(f"{state}[{at}] IS NULL" for at in range(1, size + 1))
    return f"{_array_size(state)} <> {size} OR {nulls}"


def _strict(params: str, sql_type: str, *whens: str) -> str:
    nulls = " OR ".join(f"{p} IS NULL" for p in params.split(", "))
    return f"CASE WHEN {nulls} THEN CAST(NULL AS {sql_type}) {' '.join(whens)} END"


def _floats(*values: str) -> str:
    return f"CAST(ARRAY[{', '.join(values)}] AS {_FLOATS})"


def _bad_state(function: str, size: int, *states: str) -> str:
    """check_float8_array."""
    message = refused(
        _INTERNAL_ERROR, f"'{function}: expected {size}-element float8 array'", "DOUBLE PRECISION"
    )
    bad = " OR ".join(_wrong_size(state, size) for state in states)
    return f"WHEN {bad} THEN {_floats(message)}"


def _bad_average(*states: str) -> str:
    message = refused(_INTERNAL_ERROR, "'expected 2-element int8 array'", "BIGINT")
    bad = " OR ".join(_wrong_size(state, 2) for state in states)
    return f"WHEN {bad} THEN CAST(ARRAY[{message}] AS {_INTEGERS})"


def _average_step(name: str, sign: str) -> Form:
    """int2_avg_accum and its kin: the state is a count and a sum, of bigints."""
    step = f"CAST(ARRAY[a[1] {sign} 1, a[2] {sign} CAST(b AS BIGINT)] AS {_INTEGERS})"
    return Form(name, "a, b", _strict("a, b", _INTEGERS, _bad_average("a"), f"ELSE {step}"))


def _plus(a: str, b: str) -> str:
    """float8_pl, which raises what only its operands' being finite can overflow."""
    return (
        f"CASE WHEN {_inf(f'({a}) + ({b})')} AND NOT {_inf(a)} AND NOT {_inf(b)} "
        f"THEN {_OVERFLOW} ELSE ({a}) + ({b}) END"
    )


def _accum(name: str) -> Form:
    """float8_accum: the Youngs-Cramer step of N, Sx and Sxx."""
    n = "(a[1] + 1)"
    sx = "(a[2] + b)"
    square = f"(b * {n} - {sx})"
    sxx = f"(a[3] + {square} * {square} / ({n} * a[1]))"
    after_first = (
        f"CASE WHEN {_inf(sx)} OR {_inf(sxx)} THEN "
        f"CASE WHEN NOT {_inf('a[2]')} AND NOT {_inf('b')} THEN {_OVERFLOW} ELSE {_NAN} END "
        f"ELSE {sxx} END"
    )
    first = f"CASE WHEN isnan(b) OR {_inf('b')} THEN {_NAN} ELSE a[3] END"
    state = _floats(n, sx, f"CASE WHEN a[1] > 0 THEN {after_first} ELSE {first} END")
    return Form(name, "a, b", _strict("a, b", _FLOATS, _bad_state(name, 3, "a"), f"ELSE {state}"))


def _sxx_of_two(s1: str, s2: str, x1: str, x2: str, n: str) -> str:
    return f"({s1} + {s2} + a[1] * b[1] * ({x1}) * ({x2}) / {n})"


def _checked(value: str, one: str, other: str) -> str:
    return (
        f"CASE WHEN {_inf(value)} AND NOT {_inf(one)} AND NOT {_inf(other)} "
        f"THEN {_OVERFLOW} ELSE {value} END"
    )


def _combine() -> Form:
    """float8_combine: a side with no rows is the other side."""
    n = "(a[1] + b[1])"
    tmp = "a[2] / a[1] - b[2] / b[1]"
    sxx = _checked(_sxx_of_two("a[3]", "b[3]", tmp, tmp, n), "a[3]", "b[3]")
    return Form(
        "float8_combine",
        "a, b",
        _strict(
            "a, b",
            _FLOATS,
            _bad_state("float8_combine", 3, "a", "b"),
            "WHEN a[1] = 0 THEN b",
            "WHEN b[1] = 0 THEN a",
            f"ELSE {_floats(n, _plus('a[2]', 'b[2]'), sxx)}",
        ),
    )


def _regression_accum() -> Form:
    """float8_regr_accum: the same step of N, Sx, Sxx, Sy, Syy and Sxy, of y (b) and x (c)."""
    name = "float8_regr_accum"
    n = "(a[1] + 1)"
    sx = "(a[2] + c)"
    sy = "(a[4] + b)"
    tmp_x = f"(c * {n} - {sx})"
    tmp_y = f"(b * {n} - {sy})"
    scale = f"(1.0 / ({n} * a[1]))"
    sxx = f"(a[3] + {tmp_x} * {tmp_x} * {scale})"
    syy = f"(a[5] + {tmp_y} * {tmp_y} * {scale})"
    sxy = f"(a[6] + {tmp_x} * {tmp_y} * {scale})"
    x_finite = f"NOT {_inf('a[2]')} AND NOT {_inf('c')}"
    y_finite = f"NOT {_inf('a[4]')} AND NOT {_inf('b')}"
    overflowed = (
        f"(({_inf(sx)} OR {_inf(sxx)}) AND {x_finite}) "
        f"OR (({_inf(sy)} OR {_inf(syy)}) AND {y_finite}) "
        f"OR ({_inf(sxy)} AND {x_finite} AND {y_finite})"
    )

    def after_first(value: str) -> str:
        """A later row: an infinity past the checks reads as NaN."""
        return f"CASE WHEN {_inf(value)} THEN {_NAN} ELSE {value} END"

    def of_rows(value: str, kept: str, *inputs: str) -> str:
        bad = " OR ".join(f"isnan({i}) OR {_inf(i)}" for i in inputs)
        return (
            f"CASE WHEN a[1] > 0 THEN {after_first(value)} WHEN {bad} THEN {_NAN} ELSE {kept} END"
        )

    state = _floats(
        n,
        sx,
        of_rows(sxx, "a[3]", "c"),
        sy,
        of_rows(syy, "a[5]", "b"),
        of_rows(sxy, "a[6]", "b", "c"),
    )
    return Form(
        name,
        "a, b, c",
        _strict(
            "a, b, c",
            _FLOATS,
            _bad_state(name, 6, "a"),
            f"WHEN a[1] > 0 AND ({overflowed}) THEN {_floats(_OVERFLOW)}",
            f"ELSE {state}",
        ),
    )


def _regression_combine() -> Form:
    """float8_regr_combine."""
    n = "(a[1] + b[1])"
    tmp_x = "a[2] / a[1] - b[2] / b[1]"
    tmp_y = "a[4] / a[1] - b[4] / b[1]"
    return Form(
        "float8_regr_combine",
        "a, b",
        _strict(
            "a, b",
            _FLOATS,
            _bad_state("float8_regr_combine", 6, "a", "b"),
            "WHEN a[1] = 0 THEN b",
            "WHEN b[1] = 0 THEN a",
            "ELSE "
            + _floats(
                n,
                _plus("a[2]", "b[2]"),
                _checked(_sxx_of_two("a[3]", "b[3]", tmp_x, tmp_x, n), "a[3]", "b[3]"),
                _plus("a[4]", "b[4]"),
                _checked(_sxx_of_two("a[5]", "b[5]", tmp_y, tmp_y, n), "a[5]", "b[5]"),
                _checked(_sxx_of_two("a[6]", "b[6]", tmp_x, tmp_y, n), "a[6]", "b[6]"),
            ),
        ),
    )


def _no_aggregate(name: str, message: str) -> Form:
    """A step PostgreSQL runs only inside an aggregate, and refuses by that name outside one."""
    raised = refused(_INTERNAL_ERROR, f"'{message}'", "BIGINT")
    return Form(name, "a, b", _strict("a, b", "BIGINT", f"ELSE {raised}"))


STATE_FUNCTIONS: tuple[Form, ...] = (
    Form("booland_statefunc", "a, b", _strict("a, b", "BOOLEAN", "ELSE a AND b")),
    Form("boolor_statefunc", "a, b", _strict("a, b", "BOOLEAN", "ELSE a OR b")),
    Form("any_value_transfn", "a, b", "CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE a END"),
    _average_step("int2_avg_accum", "+"),
    _average_step("int4_avg_accum", "+"),
    _average_step("int2_avg_accum_inv", "-"),
    _average_step("int4_avg_accum_inv", "-"),
    _no_aggregate("int4_avg_combine", "aggregate function called in non-aggregate context"),
    _no_aggregate(
        "range_intersect_agg_transfn",
        "range_intersect_agg_transfn called in non-aggregate context",
    ),
    _no_aggregate(
        "multirange_intersect_agg_transfn",
        "multirange_intersect_agg_transfn called in non-aggregate context",
    ),
    _accum("float8_accum"),
    _accum("float4_accum"),
    _combine(),
    _regression_accum(),
    _regression_combine(),
)


def _root(value: str) -> str:
    """C's sqrt, which is NaN below zero where an engine's may raise."""
    return f"CASE WHEN {value} < 0 THEN {_NAN} ELSE sqrt({value}) END"


def _final(name: str, size: int, answer: str, *nulls: str) -> Form:
    """A final function of a float8 state: check_float8_array, then NULL where the state holds
    too few rows, ``nulls`` in order, else ``answer``."""
    message = refused(
        _INTERNAL_ERROR, f"'{name}: expected {size}-element float8 array'", "DOUBLE PRECISION"
    )
    whens = (
        f"WHEN {_wrong_size('a', size)} THEN {message}",
        *(f"WHEN {null}" for null in nulls),
        f"ELSE CAST({answer} AS DOUBLE PRECISION)",
    )
    return Form(name, "a", _strict("a", "DOUBLE PRECISION", *whens))


_NONE = "THEN CAST(NULL AS DOUBLE PRECISION)"
# N, Sx, Sxx of float8_accum; N, Sx, Sxx, Sy, Syy, Sxy of float8_regr_accum.
_N, _SX, _SXX, _SY, _SYY, _SXY = (f"a[{at}]" for at in range(1, 7))
_NO_ROWS = f"{_N} < 1 {_NONE}"
_VERTICAL = f"{_SXX} = 0 {_NONE}"

#: float8_avg and its kin (float.c): the final functions of avg, var, stddev, covar, corr, regr_*.
FINAL_FUNCTIONS: tuple[Form, ...] = (
    _final("float8_avg", 3, f"{_SX} / {_N}", f"{_N} = 0 {_NONE}"),
    _final("float8_var_pop", 3, f"{_SXX} / {_N}", f"{_N} = 0 {_NONE}"),
    _final("float8_var_samp", 3, f"{_SXX} / ({_N} - 1)", f"{_N} <= 1 {_NONE}"),
    _final("float8_stddev_pop", 3, _root(f"{_SXX} / {_N}"), f"{_N} = 0 {_NONE}"),
    _final("float8_stddev_samp", 3, _root(f"{_SXX} / ({_N} - 1)"), f"{_N} <= 1 {_NONE}"),
    _final("float8_covar_pop", 6, f"{_SXY} / {_N}", _NO_ROWS),
    _final("float8_covar_samp", 6, f"{_SXY} / ({_N} - 1)", f"{_N} < 2 {_NONE}"),
    _final("float8_regr_avgx", 6, f"{_SX} / {_N}", _NO_ROWS),
    _final("float8_regr_avgy", 6, f"{_SY} / {_N}", _NO_ROWS),
    _final("float8_regr_sxx", 6, _SXX, _NO_ROWS),
    _final("float8_regr_syy", 6, _SYY, _NO_ROWS),
    _final("float8_regr_sxy", 6, _SXY, _NO_ROWS),
    _final("float8_regr_slope", 6, f"{_SXY} / {_SXX}", _NO_ROWS, _VERTICAL),
    _final(
        "float8_regr_intercept",
        6,
        f"({_SY} - {_SX} * {_SXY} / {_SXX}) / {_N}",
        _NO_ROWS,
        _VERTICAL,
    ),
    # Per the standard, a horizontal line's r2 is 1.
    _final(
        "float8_regr_r2",
        6,
        f"({_SXY} * {_SXY}) / ({_SXX} * {_SYY})",
        _NO_ROWS,
        _VERTICAL,
        f"{_SYY} = 0 THEN CAST(1.0 AS DOUBLE PRECISION)",
    ),
    # PostgreSQL 17 does not clamp a roundoff past 1.
    _final(
        "float8_corr",
        6,
        f"{_SXY} / {_root(f'{_SXX} * {_SYY}')}",
        _NO_ROWS,
        f"{_SXX} = 0 OR {_SYY} = 0 {_NONE}",
    ),
)
#: The state and final functions whose arguments may be an array's text form, which no engine's
#: cast reads.
ARRAY_INPUT = frozenset(form.name for form in (*STATE_FUNCTIONS, *FINAL_FUNCTIONS))
