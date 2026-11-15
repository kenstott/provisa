# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f4a91-c63e-4b58-9e1a-52f0b8d3c6a4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""avg, variance and stddev of integers on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A
STATED BOUND COMES BEFORE A REFUSAL).

PostgreSQL answers avg, var_samp, var_pop, stddev_samp and stddev_pop (and variance and stddev, the
sample's) of a smallint, an integer or a bigint as a numeric: the group's sums are exact integers
(int8_avg, numeric_poly_*), and the answer is numeric_div's quotient of them, rounded half away
from zero at select_div_scale's digits (PostgreSQL 17's numeric.c): 16 significant digits at least,
by the base-10000 weights and leading digits of the two integers. A standard deviation is sqrt_var
of that rounded variance, at the same digits, rounded half up from the exact root.

ClickHouse's own avg and varSamp answer a Float64. Its form here reads the same integers PostgreSQL
does, as Int256 (sum, the sum of the squares, the count), and writes numeric_div's digits from them
in Int256 arithmetic: the scale from each integer's text, the quotient rounded half away from zero,
and the root as the integer square root (a Float64 estimate, two Newton steps, then the one-step
corrections). The answer is the numeric's text, described as numeric; no value is rounded but as
PostgreSQL rounds it.

The bound: Int256 holds every intermediate of a group of up to 2^40 values. A group of more raises
0A000 before any digit is written, never a wrapped value. Of DISTINCT values the sums are of
groupUniqArray's, each value once. Under a FILTER or over a window the call is refused by name, as
it was: the form's aggregates are several, and ClickHouse would read the window or the filter of
the outer call only. A call of another argument type (numeric, a float, a column no statement
types) keeps what it had."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "exact aggregates of integers"

_CLICKHOUSE = "clickhouse"
#: The integer types whose sums PostgreSQL keeps exact.
_INTEGERS = frozenset({"smallint", "integer", "int", "bigint"})
#: The most values a group may hold: Int256 holds every intermediate of up to this many.
MOST_VALUES = 1 << 40
#: Each function, by what it answers: the mean, or the sample's or the population's spread, and
#: whether that spread is its square root.
_AVG = "avg"
_SPREADS = {
    "variance": (True, False),
    "var_samp": (True, False),
    "var_pop": (False, False),
    "stddev": (True, True),
    "stddev_samp": (True, True),
    "stddev_pop": (False, True),
}
#: sqlglot's names of these calls, as PostgreSQL's (variance_samp is var_samp; stdev is stddev).
_SQLGLOT_NAMES = {"variance_samp": "var_samp", "variance_pop": "var_pop", "stdev": "stddev"}
_PATTERN = "{}($v)"


# --- numeric_div's digits, in ClickHouse's Int256 ----------------------------------------------


def _power_of_ten(digits: str) -> str:
    return f"toInt256(concat('1', repeat('0', toUInt64({digits}))))"


def _weight(value: str) -> str:
    """The base-10000 weight of an integer's leading digit (0 for zero)."""
    return f"intDiv(length(toString(abs({value}))) - 1, 4)"


def _first(value: str) -> str:
    """An integer's leading base-10000 digit (0 for zero)."""
    text = f"toString(abs({value}))"
    return f"toInt256(substring({text}, 1, length({text}) - 4 * {_weight(value)}))"


def _scale(dividend: str, divisor: str) -> str:
    """select_div_scale of two integers: 16 significant digits, none below 0."""
    weight = f"{_weight(dividend)} - {_weight(divisor)}"
    lower = f"if({_first(dividend)} <= {_first(divisor)}, 1, 0)"
    return f"greatest(16 - 4 * ({weight} - {lower}), 0)"


def _quotient(dividend: str, divisor: str, scale: str) -> str:
    """|dividend / divisor| at ``scale`` digits, rounded half away from zero (divisor > 0)."""
    return (
        f"intDiv(2 * abs({dividend}) * {_power_of_ten(scale)} + {divisor}, 2 * {divisor})"
    )  # fmt: skip


def _text(digits: str, scale: str) -> str:
    """A non-negative integer of ``scale`` digits past the point, as numeric_out writes it."""
    raw = f"toString({digits})"
    padded = f"leftPad({raw}, greatest(length({raw}), toUInt64({scale}) + 1), '0')"
    point = f"length({padded}) - {scale}"
    return (
        f"if({scale} = 0, {padded}, "
        f"concat(substring({padded}, 1, {point}), '.', substring({padded}, {point} + 1)))"
    )


def _bound(variables: str, body: str, values: str) -> str:
    """``body`` with ``variables`` bound to ``values``: ClickHouse's lambda of one-element arrays."""
    return f"arrayElement(arrayMap({variables} -> {body}, {values}), 1)"


def _divided(dividend: str, divisor: str) -> str:
    """numeric_div's text of two Int256 integers, the divisor positive."""
    quotient = _quotient(dividend, divisor, "_r")
    sign = f"if({dividend} < 0 AND _q != 0, '-', '')"
    written = _bound("(_q, _r)", f"concat({sign}, {_text('_q', '_r')})", f"[{quotient}], [_r]")
    return _bound("_r", written, f"[{_scale(dividend, divisor)}]")


def _integer_root(square: str) -> str:
    """The integer square root of a non-negative Int256: a Float64 estimate, two Newton steps
    (which leave it at the root or one past), and the one-step corrections either way."""
    estimate = f"greatest(toInt256(sqrt(toFloat64({square}))), toInt256(1))"

    def step(value: str) -> str:
        return f"intDiv({value} + intDiv({square}, {value}), 2)"

    stepped = _bound("_x1", step("_x1"), f"[{step(estimate)}]")
    corrected = (
        f"_x2 - if(_x2 * _x2 > {square}, 1, 0) + if((_x2 + 1) * (_x2 + 1) <= {square}, 1, 0)"
    )
    return _bound("_x2", corrected, f"[{stepped}]")


def _rooted(dividend: str, divisor: str) -> str:
    """sqrt_var of numeric_div's quotient of two positive Int256 integers, at its scale: the root
    of the rounded quotient, to one digit more, rounded half up."""
    square = f"{_quotient(dividend, divisor, '_r')} * {_power_of_ten('_r')} * 100"
    written = _bound("_t", _text("intDiv(_t + 5, 10)", "_r"), f"[{_integer_root(square)}]")
    return _bound("_r", written, f"[{_scale(dividend, divisor)}]")


_NO_ANSWER = "CAST(NULL AS Nullable(String))"


def _too_many(function: str) -> str:
    return ch_fail(
        "0A000",
        f"{function} of more than {MOST_VALUES} values has no exact form on clickhouse",
    ).format(condition=f"count($v) > {MOST_VALUES}")


#: The group's sums, as Int256: of every value, or of each distinct value once (groupUniqArray
#: skips a NULL, as DISTINCT does).
_SUMS = {
    False: ("sum(toInt256($v))", "sum(toInt256($v) * toInt256($v))", "toInt256(count($v))"),
    True: (
        "arraySum(groupUniqArray(toInt256($v)))",
        "arraySum(arrayMap(_y -> _y * _y, groupUniqArray(toInt256($v))))",
        "toInt256(length(groupUniqArray(toInt256($v))))",
    ),
}


def avg_form(distinct: bool = False) -> str:
    """avg of an integer: the sum over the count, as numeric_div writes it; NULL of no value."""
    total, _, count = _SUMS[distinct]
    answer = _bound("(_s, _n)", _divided("_s", "_n"), f"[{total}], [{count}]")
    return f"if(count($v) = 0, {_NO_ANSWER}, if({_too_many(_AVG)} = 0, {answer}, {_NO_ANSWER}))"


def spread_form(function: str, distinct: bool = False) -> str:
    """var_samp, var_pop, stddev_samp or stddev_pop of an integer, as numeric_stddev_internal
    writes it: (N * sum(x^2) - sum(x)^2) over N * (N - 1) or N * N; 0 where that numerator is not
    above 0; NULL of no value, and of one for the sample's."""
    sample, root = _SPREADS[function]
    total, squares, count = _SUMS[distinct]
    divisor = "_n * (_n - 1)" if sample else "_n * _n"
    spread = (_rooted if root else _divided)("_m", "_d")
    numerator = "[_n * _s2 - _s * _s]"
    answered = _bound("(_m, _d)", f"if(_m <= 0, '0', {spread})", f"{numerator}, [{divisor}]")
    answer = _bound("(_s, _s2, _n)", answered, f"[{total}], [{squares}], [{count}]")
    fewest = 1 if sample else 0
    checked = f"if({_too_many(function)} = 0, {answer}, {_NO_ANSWER})"
    return f"if({count} <= {fewest}, {_NO_ANSWER}, {checked})"


# --- the rows ------------------------------------------------------------------------------------


def _function(node: exp.Expr) -> str | None:
    """PostgreSQL's name of the aggregate ``node`` calls, of these rows; None for another call."""
    call = trino.called(node)
    if call is None:
        return None
    for name in sorted(call[0]):
        named = _SQLGLOT_NAMES.get(name, name)
        if named == _AVG or named in _SPREADS:
            return named
    return None


def _integer_argument(node: exp.Expr) -> tuple[exp.Expr, bool] | None:
    """The one argument of a plain call, where the statement says it is an integer, and whether
    the call is of DISTINCT values."""
    from provisa.transpiler.function_macros import said_type

    call = trino.called(node)
    if call is None or len(call[1]) != 1:
        return None
    argument, distinct = call[1][0], False
    if isinstance(argument, exp.Distinct):
        if len(argument.expressions) != 1:
            return None
        argument, distinct = argument.expressions[0], True
    said = said_type(argument) or trino.said(argument)
    return (argument, distinct) if said in _INTEGERS else None


def _over_or_filtered(node: exp.Expr) -> bool:
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    return isinstance(outer, (exp.Filter, exp.Window))


def exact_of_integers(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """avg, variance or stddev of an integer, at PostgreSQL's numeric digits.

    None for a call of another type, which keeps what it had. Over a window or under a FILTER,
    refused by name: ClickHouse would read the window or the filter of the form's outer call
    only."""
    function = _function(node)
    argued = None if function is None else _integer_argument(node)
    if function is None or argued is None:
        return None
    if _over_or_filtered(node):
        raise Refused(trino.no_form(function, dialect))
    argument, distinct = argued
    form = avg_form(distinct) if function == _AVG else spread_form(function, distinct)
    pattern = _PATTERN.format(function)
    return graft(replacement(form, dialect, pattern), {"v": argument})


_EXACT = Rewrite(exact_of_integers)
#: Every dialect but ClickHouse keeps what it had (provisa.transpiler.function_macros' forms).
_FORMS = {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE}, _CLICKHOUSE: _EXACT}

_BOUND = (
    f"A group of more than {MOST_VALUES} (2^40) values raises 0A000 ({{}} of more than "
    f"{MOST_VALUES} values has no exact form on clickhouse): ClickHouse's Int256, which holds "
    "the sums, holds every step of up to that many. Over a window or under a FILTER, refused by "
    "name."
)


def _feature(function: str, summary: str, answer: str) -> Feature:
    return Feature(
        function,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(_PATTERN.format(function), _FORMS),),
            limitations={
                _CLICKHOUSE: (Limitation(f"Of an integer: {answer} " + _BOUND.format(function)),)
            },
        ),
    )


_DIGITS = (
    "PostgreSQL's numeric digits, exact, from the integers' sums in Int256, rounded only where "
    "PostgreSQL's numeric_div rounds."
)
_ROOT = (
    "PostgreSQL's numeric digits, exact: the integer square root of the rounded variance, in "
    "Int256, rounded only where PostgreSQL's sqrt_var rounds."
)

FEATURES: tuple[Feature, ...] = (
    _feature("avg", "The mean of a group's values.", _DIGITS),
    _feature("variance", "The sample variance of a group's values.", _DIGITS),
    _feature("var_samp", "The sample variance of a group's values.", _DIGITS),
    _feature("var_pop", "The population variance of a group's values.", _DIGITS),
    _feature("stddev", "The sample standard deviation of a group's values.", _ROOT),
    _feature("stddev_samp", "The sample standard deviation of a group's values.", _ROOT),
    _feature("stddev_pop", "The population standard deviation of a group's values.", _ROOT),
)
