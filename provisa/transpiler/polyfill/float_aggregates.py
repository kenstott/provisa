# Copyright (c) 2026 Kenneth Stott
# Canary: 4e9b1c73-8a26-4f5d-b0e7-3c1d6a92f584
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Aggregates of doubles on ClickHouse at their edges (REQ-2236; the function support measure's
edge values, REQ-2171): avg, the variances and standard deviations of a real or a double, and the
regressions and correlation of two doubles, as PostgreSQL's float.c computes them.

PostgreSQL keeps a group's count and its sums (float8_accum, float8_regr_accum: N, Sx, Sxx, Sy,
Syy, Sxy, the squares by Youngs and Cramer's method). Any NaN or infinity among a variable's
values makes its sum of squares (and the cross sum) NaN; a sum that overflows from finite values
raises 22003. Each answer is read from those: NULL of no values (of fewer than two for a sample's),
and a regression of a vertical line (Sxx = 0) NULL; a correlation of either line NULL; r2 of a
horizontal one 1.

ClickHouse's varPop, covarPop and corr answer 0 or NaN where PostgreSQL answers NaN or NULL. Here
the sums of squares are ClickHouse's varPopStable and covarPopStable (Welford's method) times the
count, which are the same sums within a double's rounding; NaN where a value is no finite number;
the overflow raised where the values are finite and a sum is not."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Rule, Side, written_in
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement
from provisa.transpiler.polyfill.numbers import FAMILY, ch_checked, ch_fail, ch_only
from provisa.transpiler.polyfill.numbers import fail as numbers_fail

_NAN = "toFloat64('nan')"
_OVERFLOW = ch_fail("22003", "value out of range: overflow")


def _not_finite(value: str, of: str) -> str:
    """Whether any of a group's values (of the rows ``of`` holds) is no finite number."""
    return f"countIf({of} AND NOT isFinite({value})) > 0"


# --- one variable: avg, the variances and standard deviations ------------------------------------

_V = "toFloat64($v)"
_N = "count($v)"
_V_NOT_FINITE = _not_finite(_V, "$v IS NOT NULL")
_SUM = f"sum({_V})"
_SQUARES = f"varPopStable({_V}) * {_N}"
_V_OVERFLOWS = _OVERFLOW.format(
    condition=f"NOT {_V_NOT_FINITE} AND (isInfinite({_SUM}) OR isInfinite({_SQUARES}))"
)

#: Each one-variable aggregate: the least count it answers of, its value, and whether it is the
#: square root of that value.
_SPREADS = {
    "var_pop": (1, "varPopStable", False),
    "var_samp": (2, "varSampStable", False),
    "variance": (2, "varSampStable", False),
    "stddev_pop": (1, "varPopStable", True),
    "stddev_samp": (2, "varSampStable", True),
    "stddev": (2, "varSampStable", True),
}


def spread_of_doubles(function: str) -> str:
    """var_pop, var_samp, stddev_pop, stddev_samp (variance and stddev, the sample's) of a real or
    a double, of ``$v``."""
    least, spread, rooted = _SPREADS[function]
    value = f"if({_V_NOT_FINITE}, {_NAN}, {spread}({_V}))"
    if rooted:
        value = f"sqrt({value})"
    return ch_checked([_V_OVERFLOWS], f"if({_N} < {least}, NULL, {value})")


#: avg of a real or a double: its sum over its count, the sum's overflow from finite values raised.
AVG_OF_DOUBLES = ch_checked(
    [_OVERFLOW.format(condition=f"NOT {_V_NOT_FINITE} AND isInfinite({_SUM})")],
    f"if({_N} < 1, NULL, {_SUM} / {_N})",
)


#: On Trino: whether a group's real or double values are each finite (Trino's own sums and
#: variances read them), and PostgreSQL's 22003 where they are and a sum, or the sum of squares,
#: overflows (float8_accum).
_T_FINITE = "count_if(NOT is_finite($v)) = 0"
_T_OVERFLOW = numbers_fail("22003", "value out of range: overflow")


def trino_of_doubles(function: str) -> str:
    """``function`` (sum, avg, a variance or a standard deviation) of a real or a double on Trino,
    of ``$v``: Trino's own, the overflow of its sums from finite values raised."""
    squares = "" if function == "sum" else " OR is_infinite(var_pop($v) * count($v))"
    return (
        f"CASE WHEN {_T_FINITE} AND (is_infinite(sum($v)){squares}) THEN {_T_OVERFLOW} "
        f"ELSE {function}($v) END"
    )


def is_spread(function: str) -> bool:
    return function in _SPREADS


# --- two variables: the regressions and correlation (regr_*(Y, X), corr(Y, X)) ---------------------

_PAIR = "$y IS NOT NULL AND $x IS NOT NULL"
_X = "if($y IS NULL, NULL, toFloat64($x))"
_Y = "if($x IS NULL, NULL, toFloat64($y))"
_PAIRS = f"countIf({_PAIR})"
_X_NOT_FINITE = _not_finite("toFloat64($x)", _PAIR)
_Y_NOT_FINITE = _not_finite("toFloat64($y)", _PAIR)
_SX, _SY = f"sum({_X})", f"sum({_Y})"
_X_SQUARES = f"varPopStable({_X}) * {_PAIRS}"
_Y_SQUARES = f"varPopStable({_Y}) * {_PAIRS}"
_CROSS = f"covarPopStable({_X}, {_Y}) * {_PAIRS}"
_SXX = f"if({_X_NOT_FINITE}, {_NAN}, {_X_SQUARES})"
_SYY = f"if({_Y_NOT_FINITE}, {_NAN}, {_Y_SQUARES})"
_SXY = f"if({_X_NOT_FINITE} OR {_Y_NOT_FINITE}, {_NAN}, {_CROSS})"
_PAIR_OVERFLOWS = _OVERFLOW.format(
    condition=(
        f"(NOT {_X_NOT_FINITE} AND (isInfinite({_SX}) OR isInfinite({_X_SQUARES}))) OR "
        f"(NOT {_Y_NOT_FINITE} AND (isInfinite({_SY}) OR isInfinite({_Y_SQUARES}))) OR "
        f"(NOT {_X_NOT_FINITE} AND NOT {_Y_NOT_FINITE} AND isInfinite({_CROSS}))"
    )
)

#: Each two-variable aggregate (float.c's final functions): what it answers of the sums, NULL of
#: no pairs.
_REGRESSIONS = {
    "regr_sxx": ("The sum of squares of the independent variable.", _SXX),
    "regr_syy": ("The sum of squares of the dependent variable.", _SYY),
    "regr_slope": (
        "The slope of the least-squares line.",
        f"if({_SXX} = 0, NULL, {_SXY} / {_SXX})",
    ),
    "regr_intercept": (
        "The y-intercept of the least-squares line.",
        f"if({_SXX} = 0, NULL, ({_SY} - {_SX} * {_SXY} / {_SXX}) / {_PAIRS})",
    ),
    "regr_r2": (
        "The square of the correlation coefficient.",
        f"if({_SXX} = 0, NULL, if({_SYY} = 0, 1, {_SXY} * {_SXY} / ({_SXX} * {_SYY})))",
    ),
    "corr": (
        "The correlation coefficient.",
        f"if({_SXX} = 0 OR {_SYY} = 0, NULL, {_SXY} / sqrt({_SXX} * {_SYY}))",
    ),
}


def _trino_line(name: str) -> str:
    """regr_slope or regr_intercept on Trino, whose own answers NULL where a variable holds NaN or
    an infinity: a non-finite x makes Sxx NaN, and the answer NaN; a non-finite y makes Sxy NaN,
    and the answer NaN unless Sxx is 0 (a vertical line, NULL). Else Trino's own."""
    pair = "$y IS NOT NULL AND $x IS NOT NULL"
    return (
        f"CASE WHEN count_if({pair} AND NOT is_finite($x)) > 0 THEN nan() "
        f"WHEN count_if({pair} AND NOT is_finite($y)) > 0 "
        "THEN IF(var_pop(IF($y IS NULL, NULL, $x)) = 0, NULL, nan()) "
        f"ELSE {name}($y, $x) END"
    )


#: The regressions whose Trino answer differs from PostgreSQL's at NaN.
_TRINO_LINES = ("regr_slope", "regr_intercept")

REGRESSIONS: tuple[Feature, ...] = tuple(
    Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    f"{name}($y, $x)",
                    {
                        **ch_only(
                            ch_checked([_PAIR_OVERFLOWS], f"if({_PAIRS} < 1, NULL, {answer})")
                        ),
                        **(trino.on_trino(_trino_line(name)) if name in _TRINO_LINES else {}),
                    },
                ),
            )
        ),
    )
    for name, (summary, answer) in _REGRESSIONS.items()
)

#: sum of a bigint is a numeric (int8_sum), exact past a bigint's range, where ClickHouse's sum
#: of an Int64 wraps: its sum in Decimal128 (38 digits, the sum of up to 2^64 bigints). sum of a
#: real or a double raises its overflow from finite values (float4pl, float8pl).
_SUM_OF_BIGINTS = "sum(toDecimal128($v, 0))"
_SUM_OF_DOUBLES = ch_checked(
    [_OVERFLOW.format(condition=f"NOT {_V_NOT_FINITE} AND isInfinite({_SUM})")], "sum($v)"
)
#: A real's sum is a real, added as reals (float4pl): ClickHouse's sumWithOverflow keeps a Float32.
_REAL_SUM = "sumWithOverflow($v)"
_SUM_OF_REALS = ch_checked(
    [_OVERFLOW.format(condition=f"NOT {_V_NOT_FINITE} AND isInfinite({_REAL_SUM})")], _REAL_SUM
)


def _sum(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """sum of a bigint, or of a real or a double, as the statement says its argument's type; any
    other keeps what it had. Of a real or a double over a window or under a FILTER (the form's
    several aggregates would read the outer call's alone), it keeps what it had too."""
    from provisa.transpiler.function_macros import said_type

    call = trino.called(node)
    if call is None or "sum" not in call[0] or len(call[1]) != 1:
        return None
    argument = call[1][0]
    if isinstance(argument, exp.Distinct):
        return None
    said = said_type(argument) or trino.said(argument)
    if said == "bigint":
        form = _SUM_OF_BIGINTS
    elif said in ("real", "double precision") and not _over_or_filtered(node):
        form = _SUM_OF_REALS if said == "real" else _SUM_OF_DOUBLES
    else:
        return None
    return graft(replacement(form, dialect, "sum($v)"), {"v": argument})


def _trino_sum(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """sum of a real or a double on Trino, as the statement says its argument's type: the overflow
    of the sum from finite values raised (float4pl, float8pl). Any other keeps what it had, as
    does one over a window or under a FILTER."""
    from provisa.transpiler.function_macros import said_type

    call = trino.called(node)
    if call is None or "sum" not in call[0] or len(call[1]) != 1:
        return None
    argument = call[1][0]
    if isinstance(argument, exp.Distinct) or _over_or_filtered(node):
        return None
    if (said_type(argument) or trino.said(argument)) not in ("real", "double precision"):
        return None
    return graft(
        replacement(trino_of_doubles("sum"), written_in(dialect), "sum($v)"), {"v": argument}
    )


def _over_or_filtered(node: exp.Expr) -> bool:
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    return isinstance(outer, (exp.Filter, exp.Window))


SUM = Feature(
    "sum",
    FAMILY,
    "The sum of a group's bigints, exact; of its reals or doubles, the overflow raised.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    "clickhouse": Rewrite(_sum, mentions=r"\bsum\s*\("),
                    **trino.on_trino(Rewrite(_trino_sum, mentions=r"\bsum\s*\(")),
                },
            ),
        )
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 150
FEATURES: tuple[Feature, ...] = (*REGRESSIONS, SUM)
