# Copyright (c) 2026 Kenneth Stott
# Canary: 8c2f4e61-3b7a-4d95-a1e8-5f0c9d27b346
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Floating point and integer math at its edges on ClickHouse (REQ-2236; the function support
measure's edge values, REQ-2171). Each row answered a wrong value there; each is written in
ClickHouse's own functions after PostgreSQL's float.c, int.c and numeric.c.

Order: PostgreSQL orders NaN above every other value and equal to itself (float.c's float8_cmp_internal),
where ClickHouse's comparisons are IEEE's (NaN unordered, each comparison of it false). The
comparison functions of reals and doubles, and the larger and smaller of two, compare a pair of
NaN and the value, NaN first.

Domains: PostgreSQL raises where a function's argument is outside its domain or its answer overflows
a double, where ClickHouse answers NaN or infinity: acos, asin and atanh past 1 in magnitude, acosh
below 1, sin, cos, tan and cot of an infinity (22003 input is out of range); sqrt of a negative
(2201F), log and log10 of a negative or zero (2201E); power of a negative to a non-integer and of
zero to a negative (2201F), or overflowing (22003); degrees overflowing (22003). sign of NaN is 0.

Integers: abs of the least value of its type has no value of the type (22003), as lcm past the type
(22003); factorial of a negative is undefined (22003); chr of a negative or zero is 22023 or 54000
(PostgreSQL holds no NUL in a text), past 0x10FFFF or of a surrogate 54000."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.pgwire import function_types
from provisa.transpiler import pg_forms
from provisa.transpiler.polyfill import notation, trino
from provisa.transpiler.polyfill.matching import graft, match
from provisa.transpiler.polyfill.notation import Form, Rewrite, replacement
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side, written_in
from provisa.transpiler.polyfill.numbers import (
    FAMILY,
    ch_checked,
    ch_fail,
    ch_once,
    ch_only,
    fail,
)


def _ch(form: str) -> dict[str, str | Rewrite]:
    """ClickHouse's ``form``, its holes each read once; every other dialect keeps what it had."""
    return ch_only(ch_once(form))


def _ch_trino(form: str, trino_form: str) -> dict[str, Form]:
    """ClickHouse's ``form`` (its holes each read once) and Trino's ``trino_form``; every other
    dialect keeps what it had."""
    return {**_ch(form), **trino.on_trino(trino_form)}


def _doubles(pattern: str, form: str) -> Rewrite:
    """``form`` of a call none of whose arguments the statement says is a numeric; a numeric's
    call keeps what it had: PostgreSQL answers it in numeric's exact digits, which a double does
    not hold."""

    def doubles(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node)
        if call is None or any(function_types.argument_type(a) == "numeric" for a in call[1]):
            return None
        bindings = match(notation.pattern(pattern), _typed(node))
        if bindings is None:
            return None
        return graft(replacement(form, written_in(dialect), pattern), bindings)

    doubles.__doc__ = f"{pattern} of no numeric: a double's answer, its edges as PostgreSQL's."
    doubles.__qualname__ = f"of_no_numeric_{_written(pattern)}"
    return Rewrite(doubles)


def _of_no_numeric(pattern: str, form: str, trino_form: str | None = None) -> dict[str, Form]:
    """ClickHouse's ``form`` (its holes each read once), and Trino's ``trino_form`` where one is
    given, of a call none of whose arguments the statement says is a numeric (_doubles)."""
    forms: dict[str, Form] = {**ch_only(_doubles(pattern, ch_once(form)))}
    if trino_form is not None:
        forms.update(trino.on_trino(_doubles(pattern, trino_form)))
    return forms


def _t_checked(raises: list[tuple[str, str, str]], answer: str) -> str:
    """Trino's ``answer`` where none of ``raises`` (condition, SQLSTATE, message) holds, else the
    first's error: Trino reads a CASE's branches in order, and only the branch it takes."""
    branches = " ".join(
        f"WHEN {when} THEN {fail(state, message)}" for when, state, message in raises
    )
    return f"CASE {branches} ELSE {answer} END"


def _typed(node: exp.Expr) -> exp.Expr:
    """A call written by a name sqlglot leaves a plain call (quoted, or of pg_catalog: "log10") as
    the node sqlglot reads that name as unquoted (log of base 10), as the grid reads it."""
    if not isinstance(node, exp.Anonymous):
        return node
    arguments = [argument.copy() for argument in node.expressions]
    return exp.func(node.name.lower(), *arguments, dialect=notation.PATTERN_DIALECT)


def _written(pattern: str) -> str:
    return pattern.split("(", 1)[0]


def _ordered(hole: str) -> str:
    """A float's place in PostgreSQL's order: NaN after every other value, all NaN equal."""
    return f"tuple(isNaN({hole}), if(isNaN({hole}), 0, toFloat64({hole})))"


def _compared(operator: str) -> str:
    """Two floats compared in PostgreSQL's order; NULL where either is NULL (each is strict)."""
    return f"if($a IS NULL OR $b IS NULL, NULL, {_ordered('$a')} {operator} {_ordered('$b')})"


def _t_nan_compared(operator: str) -> str:
    """Two floats compared in PostgreSQL's order on Trino, either NULL aside: NaN after every
    other value and equal to itself, where Trino's comparisons are IEEE's."""
    return (
        "CASE WHEN is_nan($a) OR is_nan($b) "
        f"THEN IF(is_nan($a), 1, 0) {operator} IF(is_nan($b), 1, 0) ELSE $a {operator} $b END"
    )


def _t_compared(operator: str) -> str:
    """Two floats compared in PostgreSQL's order on Trino; NULL where either is NULL."""
    return f"IF($a IS NULL OR $b IS NULL, NULL, {_t_nan_compared(operator)})"


_COMPARISONS = {
    "eq": ("=", "equal to"),
    "ne": ("!=", "not equal to"),
    "lt": ("<", "less than"),
    "le": ("<=", "less than or equal to"),
    "gt": (">", "greater than"),
    "ge": (">=", "greater than or equal to"),
}
#: Each pair of float types a comparison function takes, by its prefix.
_PAIRS = {
    "float4": ("a real", "a real"),
    "float8": ("a double", "a double"),
    "float48": ("a real", "a double"),
    "float84": ("a double", "a real"),
}

COMPARISONS: tuple[Feature, ...] = tuple(
    Feature(
        f"{prefix}{suffix}",
        FAMILY,
        f"Whether {left} is {said} {right}, NaN after every other value.",
        read=Side(
            rules=(
                Rule(
                    f"{prefix}{suffix}($a, $b)",
                    _ch_trino(_compared(operator), _t_compared(operator)),
                ),
            )
        ),
    )
    for prefix, (left, right) in _PAIRS.items()
    for suffix, (operator, said) in _COMPARISONS.items()
)

#: float4larger and its kin: the first where it is greater (or less) in PostgreSQL's order, else the
#: second (float.c's float8larger).
_CHOSEN = {"larger": (">", "greater"), "smaller": ("<", "less")}

CHOSEN: tuple[Feature, ...] = tuple(
    Feature(
        f"{prefix}{suffix}",
        FAMILY,
        f"The {said} of two floats, NaN after every other value.",
        read=Side(
            rules=(
                Rule(
                    f"{prefix}{suffix}($a, $b)",
                    _ch_trino(
                        f"if({_compared(operator)}, $a, if($a IS NULL, NULL, $b))",
                        f"CASE WHEN $a IS NULL OR $b IS NULL THEN NULL "
                        f"WHEN {_t_nan_compared(operator)} THEN $a ELSE $b END",
                    ),
                ),
            )
        ),
    )
    for prefix in ("float4", "float8")
    for suffix, (operator, said) in _CHOSEN.items()
)

_OUT_OF_RANGE = ch_fail("22003", "input is out of range")
_OVERFLOW = ch_fail("22003", "value out of range: overflow")
_FINITE = "NOT isInfinite(toFloat64({}))"


def _domain(
    name: str,
    outside: str,
    summary: str,
    answer: str | None = None,
    trino_outside: str = "",
    trino_answer: str = "",
) -> Feature:
    """``name`` of a double, PostgreSQL's 22003 where ``outside`` (of ``$x``) holds; on Trino
    where ``trino_outside`` holds, else ``trino_answer``."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    f"{name}($x)",
                    _ch_trino(
                        ch_checked(
                            [_OUT_OF_RANGE.format(condition=outside)], answer or f"{name}($x)"
                        ),
                        _t_checked(
                            [(trino_outside, "22003", "input is out of range")], trino_answer
                        ),
                    ),
                ),
            )
        ),
    )


_INFINITE = "isInfinite(toFloat64($x))"
_T_INFINITE = "is_infinite($x)"
#: Past 1e8 x * x - 1 is x * x to a double's precision, and x * x overflows past 1e154: acosh(x)
#: is ln(2x) there, written as ln(x) + ln(2) (libm's acosh of a large value).
_T_ACOSH = "IF($x > 1e8, ln($x) + ln(2.0), ln($x + sqrt($x * $x - 1.0)))"

DOMAINS: tuple[Feature, ...] = (
    _domain(
        "acos",
        "abs($x) > 1",
        "The arc cosine of a value from -1 to 1.",
        trino_outside="abs($x) > 1",
        trino_answer="acos($x)",
    ),
    _domain(
        "asin",
        "abs($x) > 1",
        "The arc sine of a value from -1 to 1.",
        trino_outside="abs($x) > 1",
        trino_answer="asin($x)",
    ),
    _domain(
        "atanh",
        "abs($x) > 1",
        "The inverse hyperbolic tangent of a value from -1 to 1.",
        trino_outside="abs($x) > 1",
        trino_answer="0.5 * ln((1.0 + $x) / (1.0 - $x))",
    ),
    _domain(
        "acosh",
        "$x < 1",
        "The inverse hyperbolic cosine of a value of 1 or more.",
        trino_outside="$x < 1",
        trino_answer=_T_ACOSH,
    ),
    _domain(
        "sin",
        _INFINITE,
        "The sine of a finite value.",
        trino_outside=_T_INFINITE,
        trino_answer="sin($x)",
    ),
    _domain(
        "cos",
        _INFINITE,
        "The cosine of a finite value.",
        trino_outside=_T_INFINITE,
        trino_answer="cos($x)",
    ),
    _domain(
        "tan",
        _INFINITE,
        "The tangent of a finite value.",
        trino_outside=_T_INFINITE,
        trino_answer="tan($x)",
    ),
    _domain(
        "cot",
        _INFINITE,
        "The cotangent of a finite value.",
        "1.0 / tan($x)",
        trino_outside=_T_INFINITE,
        trino_answer="1.0 / tan($x)",
    ),
)

SQRT = Feature(
    "sqrt",
    FAMILY,
    "The square root of a value of zero or more.",
    read=Side(
        rules=(
            Rule(
                "sqrt($x)",
                _of_no_numeric(
                    "sqrt($x)",
                    ch_checked(
                        [
                            ch_fail("2201F", "cannot take square root of a negative number").format(
                                condition="$x < 0"
                            )
                        ],
                        "sqrt($x)",
                    ),
                    _t_checked(
                        [("$x < 0", "2201F", "cannot take square root of a negative number")],
                        "sqrt($x)",
                    ),
                ),
            ),
        )
    ),
)

_LOG_RAISES = [
    ch_fail("2201E", "cannot take logarithm of a negative number").format(condition="$x < 0"),
    ch_fail("2201E", "cannot take logarithm of zero").format(condition="$x = 0"),
]

_LOG10 = ch_checked(_LOG_RAISES, "log10($x)")
_T_LOG_RAISES = [
    ("$x < 0", "2201E", "cannot take logarithm of a negative number"),
    ("$x = 0", "2201E", "cannot take logarithm of zero"),
]
_T_LOG10 = _t_checked(_T_LOG_RAISES, "log10($x)")


LOGS: tuple[Feature, ...] = (
    Feature(
        "log",
        FAMILY,
        "The base 10 logarithm of a value above zero.",
        read=Side(rules=(Rule("log($x)", _of_no_numeric("log($x)", _LOG10, _T_LOG10)),)),
    ),
    Feature(
        "log10",
        FAMILY,
        "The base 10 logarithm of a value above zero.",
        read=Side(rules=(Rule("log10($x)", _of_no_numeric("log10($x)", _LOG10, _T_LOG10)),)),
    ),
)

_UNDERFLOW = ch_fail("22003", "value out of range: underflow")

#: dexp: exp of a finite value overflowing or underflowing to zero raises 22003; of NaN NaN, of an
#: infinity infinity or zero.
_EXP = ch_checked(
    [
        _OVERFLOW.format(condition=f"isInfinite(exp($x)) AND {_FINITE.format('$x')}"),
        _UNDERFLOW.format(condition=f"exp($x) = 0 AND {_FINITE.format('$x')}"),
    ],
    "exp($x)",
)
#: dlog1: ln of a negative or zero raises 2201E.
_LN = ch_checked(
    [
        ch_fail("2201E", "cannot take logarithm of a negative number").format(condition="$x < 0"),
        ch_fail("2201E", "cannot take logarithm of zero").format(condition="$x = 0"),
    ],
    "log($x)",
)

_T_FINITE = "is_finite($x)"
_T_EXP = _t_checked(
    [
        (f"{_T_FINITE} AND is_infinite(exp($x))", "22003", "value out of range: overflow"),
        (f"{_T_FINITE} AND exp($x) = 0", "22003", "value out of range: underflow"),
    ],
    "exp($x)",
)
_T_LN = _t_checked(_T_LOG_RAISES, "ln($x)")

#: ClickHouse server 24.3's exp and log are its own vectorized approximations, not libm's: exp(0.5)
#: answers 1.6487212707014907 where PostgreSQL's (glibc's) is 1.6487212707001282. chDB's build
#: answers libm's digits.
_APPROXIMATED = (
    "On a ClickHouse server of 24.3, its {} is its own approximation, not libm's, and can differ "
    "from PostgreSQL's answer from the 12th significant digit; chDB answers PostgreSQL's digits."
)

#: tanh as sinh / cosh, each of which ClickHouse answers as libm does; past 22 both overflow toward
#: infinity where tanh is 1 to a double's precision.
_TANH = "if(abs($x) > 22, if($x > 0, 1.0, -1.0), sinh($x) / cosh($x))"

EXPONENTIALS: tuple[Feature, ...] = (
    Feature(
        "exp",
        FAMILY,
        "e raised to a power; 22003 where a finite power overflows or underflows.",
        read=Side(
            rules=(Rule("exp($x)", _of_no_numeric("exp($x)", _EXP, _T_EXP)),),
            limitations={"clickhouse": (Limitation(_APPROXIMATED.format("exp")),)},
        ),
    ),
    Feature(
        "ln",
        FAMILY,
        "The natural logarithm of a value above zero.",
        read=Side(
            rules=(Rule("ln($x)", _of_no_numeric("ln($x)", _LN, _T_LN)),),
            limitations={"clickhouse": (Limitation(_APPROXIMATED.format("log")),)},
        ),
    ),
    Feature(
        "tanh",
        FAMILY,
        "The hyperbolic tangent.",
        read=Side(
            rules=(Rule("tanh($x)", _of_no_numeric("tanh($x)", _TANH)),),
            limitations={
                "clickhouse": (
                    Limitation(
                        "The ratio of ClickHouse's sinh and cosh, within 2 units in the last place "
                        "of libm's tanh (ClickHouse's own tanh answers 0 for a value below 1e-90); "
                        "1 in magnitude past 22, where tanh rounds to 1."
                    ),
                )
            },
        ),
    ),
)

#: power of two doubles (float.c's dpow): a NaN operand answers before any check, NaN to the
#: power of 0 and 1 to the power of NaN answering 1, as ClickHouse's pow does.
_POWER = "pow($x, $y)"
_POWER_RAISES = [
    ch_fail("2201F", "zero raised to a negative power is undefined").format(
        condition="$x = 0 AND $y < 0"
    ),
    ch_fail(
        "2201F", "a negative number raised to a non-integer power yields a complex result"
    ).format(condition="$x < 0 AND NOT isNaN(toFloat64($y)) AND floor($y) != $y"),
    _OVERFLOW.format(
        condition=f"isInfinite(toFloat64({_POWER})) AND {_FINITE.format('$x')} "
        f"AND {_FINITE.format('$y')}"
    ),
]

#: dpow on Trino, whose power is Java's: a NaN operand, 1 to any power and -1 or 1 to an infinite
#: power answer as C's pow does before Java's is read; then dpow's errors.
_T_BOTH_FINITE = "is_finite($x) AND is_finite($y)"
_T_POWER = (
    "CASE WHEN is_nan($x) THEN IF($y = 0, 1.0, nan()) "
    "WHEN is_nan($y) THEN IF($x = 1, 1.0, nan()) "
    f"WHEN $x = 0 AND $y < 0 THEN {fail('2201F', 'zero raised to a negative power is undefined')} "
    "WHEN $x < 0 AND floor($y) != $y THEN "
    f"{fail('2201F', 'a negative number raised to a non-integer power yields a complex result')} "
    "WHEN $x = 1 OR (abs($x) = 1 AND is_infinite($y)) THEN 1.0 "
    f"WHEN {_T_BOTH_FINITE} AND is_infinite(power($x, $y)) "
    f"THEN {fail('22003', 'value out of range: overflow')} "
    f"WHEN {_T_BOTH_FINITE} AND $x != 0 AND power($x, $y) = 0 "
    f"THEN {fail('22003', 'value out of range: underflow')} "
    "ELSE power($x, $y) END"
)

POWERS: tuple[Feature, ...] = tuple(
    Feature(
        name,
        FAMILY,
        "A value raised to a power.",
        read=Side(
            rules=(
                Rule(
                    f"{name}($x, $y)",
                    _of_no_numeric(f"{name}($x, $y)", ch_checked(_POWER_RAISES, _POWER), _T_POWER),
                ),
            )
        ),
    )
    for name in ("power", "pow")
)

DEGREES = Feature(
    "degrees",
    FAMILY,
    "Radians as degrees.",
    read=Side(
        rules=(
            Rule(
                "degrees($x)",
                _ch_trino(
                    ch_checked(
                        [
                            _OVERFLOW.format(
                                condition=f"isInfinite(degrees($x)) AND {_FINITE.format('$x')}"
                            )
                        ],
                        "degrees($x)",
                    ),
                    _t_checked(
                        [
                            (
                                f"{_T_FINITE} AND is_infinite(degrees($x))",
                                "22003",
                                "value out of range: overflow",
                            )
                        ],
                        "degrees($x)",
                    ),
                ),
            ),
        )
    ),
)

SIGN = Feature(
    "sign",
    FAMILY,
    "The sign of a value: -1, 0 or 1; 0 of NaN.",
    read=Side(
        rules=(
            Rule(
                "sign($x)",
                {
                    **_ch("if(isNaN(toFloat64($x)), 0, sign($x))"),
                    **trino.on_trino(_doubles("sign($x)", "IF(is_nan($x), 0, sign($x))")),
                },
            ),
        )
    ),
)

#: 2^e mod 360 for e from 0 to 14; past 2 its period is 12 (2^e is 0 mod 8, and 2 has order 12 mod 45).
_TWO_TO_THE_MOD_360 = [pow(2, e, 360) for e in range(15)]


def _fmod_360(x: str) -> str:
    """C's fmod(x, 360.0) of a finite double ``x``, exact, its sign kept (a minus zero too), where
    ClickHouse's % of a float is inexact past 2^53. ``x`` is m * 2^e, m the 53 bits of its
    mantissa: past 360, e is 43 or less below zero, where the remainder is m's remainder of 360 *
    2^-e scaled back by a power of two; at zero or above it is (m mod 360) * (2^e mod 360) mod 360."""
    bits = f"reinterpretAsUInt64({x})"
    e = f"(toInt32(bitAnd(bitShiftRight({bits}, 52), 2047)) - 1075)"
    m = f"bitOr(bitAnd({bits}, 4503599627370495), 4503599627370496)"
    fraction = (
        f"toFloat64(({m}) % bitShiftLeft(toUInt64(360), -({e}))) / "
        f"reinterpretAsFloat64(bitShiftLeft(toUInt64(1023 - ({e})), 52))"
    )
    power = f"arrayElement({_TWO_TO_THE_MOD_360}, if({e} < 3, {e}, 3 + ({e} - 3) % 12) + 1)"
    whole = f"toFloat64((({m}) % 360) * {power} % 360)"
    return f"if(abs({x}) < 360.0, {x}, if({x} < 0, -1.0, 1.0) * if({e} < 0, {fraction}, {whole}))"


def _degrees_of(name: str) -> Feature:
    """``name`` (sind, cosd, tand or cotd) of a double: float.c's, its angle folded by an exact
    fmod; NaN of NaN, 22003 of an infinity."""
    answer = pg_forms.degree_answers("_r", minus_zero=True)[name]
    folded = f"arrayElement(arrayMap(_r -> {answer}, [{_fmod_360('_v')}]), 1)"
    form = ch_checked(
        [_OUT_OF_RANGE.format(condition="isInfinite(_v)")],
        f"if(isNaN(_v), _v, {folded})",
    )
    return Feature(
        name,
        FAMILY,
        f"The {name[:-1]} of an angle in degrees, exact at 0, 30, 45, 60 and 90 as PostgreSQL's.",
        read=Side(
            rules=(
                Rule(
                    f"{name}($x)",
                    _ch(f"arrayElement(arrayMap(_v -> {form}, [toFloat64($x)]), 1)"),
                ),
            )
        ),
    )


DEGREE_TRIGONOMETRY: tuple[Feature, ...] = tuple(
    _degrees_of(name) for name in ("sind", "cosd", "tand", "cotd")
)

#: Each integer type ClickHouse holds a PostgreSQL integer in, and PostgreSQL's name of it.
_INTEGER_TYPES = {"Int16": "smallint", "Int32": "integer", "Int64": "bigint"}


def _of_type(hole: str, held: str) -> str:
    return f"toTypeName(assumeNotNull({hole})) = '{held}'"


#: abs: the least value of an integer type is its own negation, which has no value of the type.
_ABS = "if($a < 0, -$a, $a)"
_LEAST = {"smallint": -32768, "integer": -2147483648, "bigint": -9223372036854775808}
#: A column of a type no statement says: its ClickHouse type, read at run time, says which least.
_ABS_OF_A_COLUMN = ch_checked(
    [
        ch_fail("22003", f"{name} out of range").format(
            condition=f"{_of_type('$a', held)} AND $a < 0 AND -$a < 0"
        )
        for held, name in _INTEGER_TYPES.items()
    ],
    _ABS,
)


def _abs(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """abs of an integer the statement says (22003 of its type's least value), or of a column of
    a type no statement says; any other value keeps ClickHouse's own abs: a float's and a
    numeric's are exact, and a form's own call (an offset of a zone) is of no integer's least."""
    call = trino.called(node)
    if call is None or len(call[1]) != 1:
        return None
    argument = call[1][0]
    said = function_types.argument_type(argument)
    if said in _LEAST:
        form = ch_once(
            ch_checked(
                [ch_fail("22003", f"{said} out of range").format(condition=f"$a = {_LEAST[said]}")],
                _ABS,
            )
        )
    elif said is None and isinstance(argument.unnest(), exp.Column):
        form = _ABS_OF_A_COLUMN
    else:
        return None
    return graft(replacement(form, dialect, "abs($a)"), {"a": argument})


def _t_abs(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """abs of an integer the statement says on Trino: PostgreSQL's 22003 of its type's least
    value, where Trino raises its own NUMERIC_VALUE_OUT_OF_RANGE. Any other keeps Trino's abs."""
    call = trino.called(node)
    if call is None or len(call[1]) != 1:
        return None
    said = function_types.argument_type(call[1][0])
    if said not in _LEAST:
        return None
    form = _t_checked([(f"$a = {_LEAST[said]}", "22003", f"{said} out of range")], "abs($a)")
    return trino.written(form, "abs($a)", {"a": call[1][0]})


ABS = Feature(
    "abs",
    FAMILY,
    "The absolute value; of an integer type's least value, out of range.",
    read=Side(
        rules=(Rule("abs($a)", {**ch_only(Rewrite(_abs)), **trino.on_trino(Rewrite(_t_abs))}),)
    ),
)

_INTEGER_TYPES_SAID = frozenset(_LEAST)


def _t_mod(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """mod of two integers the statement says on Trino: PostgreSQL's 22012 of a zero divisor,
    where Trino raises its own DIVISION_BY_ZERO. Any other keeps Trino's mod."""
    if isinstance(node, exp.Mod):  # sqlglot's node of mod(a, b) and of a % b, which are one
        args = [node.this, node.expression]
    else:
        call = trino.called(node)
        if call is None or "mod" not in call[0] or len(call[1]) != 2:  # noqa: PLR2004
            return None
        args = call[1]
    if not all(function_types.argument_type(a) in _INTEGER_TYPES_SAID for a in args):
        return None
    form = _t_checked([("$b = 0", "22012", "division by zero")], "$a % $b")
    return trino.written(form, "mod($a, $b)", {"a": args[0], "b": args[1]})


#: int2, int4 and int8 of a real or a double (float.c's dtoi4 and kin): rint of the value, and
#: PostgreSQL's 22003 of NaN or of one past the type, where Trino's cast of NaN raised its own.
_T_INTEGERS = {
    "int2": ("SMALLINT", "smallint", 16),
    "int4": ("INTEGER", "integer", 32),
    "int8": ("BIGINT", "bigint", 64),
}
_T_RINT = "CASE WHEN $x - floor($x) = 0.5 THEN 2 * round($x / 2) ELSE round($x) END"


def _t_integer_of_float(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """int2, int4 or int8 of a value the statement says is a real or a double, on Trino."""
    call = trino.called(node)
    if call is None or len(call[1]) != 1:
        return None
    names = call[0] & set(_T_INTEGERS)
    if len(names) != 1 or function_types.argument_type(call[1][0]) not in _FLOAT_NAMES:
        return None
    sql_type, pg_name, bits = _T_INTEGERS[next(iter(names))]
    # past 2^63 a double's bigint bound is no exact double: compare with the bound as a double
    lowest, past = -(1 << (bits - 1)), 1 << (bits - 1)
    rint = _T_RINT.replace("$x", "CAST($x AS DOUBLE)")
    form = (
        f"CASE WHEN is_nan(CAST($x AS DOUBLE)) OR {rint} < {lowest}e0 OR {rint} >= {past}e0 "
        f"THEN {fail('22003', f'{pg_name} out of range')} ELSE CAST({rint} AS {sql_type}) END"
    )
    return trino.written(form, "f($x)", {"x": call[1][0]})


INTEGERS_OF_FLOATS = Feature(
    "integers of floats on trino",
    FAMILY,
    "int2, int4 or int8 of a real or a double: rounded half to even, out of range of NaN.",
    read=Side(
        rules=(
            Rule(
                None,
                trino.on_trino(Rewrite(_t_integer_of_float, mentions=r"\bint[248]\b", first=True)),
            ),
        )
    ),
)


MOD = Feature(
    "mod of integers on trino",
    FAMILY,
    "The remainder of two integers; of a zero divisor, division by zero.",
    read=Side(rules=(Rule(None, trino.on_trino(Rewrite(_t_mod, mentions=r"\bmod\b|%"))),)),
)

#: lcm of two integers (int.c's int4lcm and int8lcm): 0 where either is 0, else |a / gcd * b|,
#: PostgreSQL's 22003 past the type (a smallint's lcm is an integer's). Computed in Int128, which
#: holds the product of two bigints, then written as an Int64, which holds every answer.
#: ClickHouse evaluates every branch, and its gcd of a zero (or of a NULL, read as its default 0)
#: divides by it: either is read as 1 there.
_NONZERO_A = "toInt128(if(ifNull($a, 0) = 0, 1, assumeNotNull($a)))"
_NONZERO_B = "toInt128(if(ifNull($b, 0) = 0, 1, assumeNotNull($b)))"
_LCM = f"abs(intDiv({_NONZERO_A}, gcd({_NONZERO_A}, {_NONZERO_B})) * {_NONZERO_B})"
_LCM_LARGEST = {"Int16": 2147483647, "Int32": 2147483647, "Int64": 9223372036854775807}
_LCM_NAMES = {"Int16": "integer", "Int32": "integer", "Int64": "bigint"}

LCM = Feature(
    "lcm",
    FAMILY,
    "The least common multiple of two integers; out of range past the type.",
    read=Side(
        rules=(
            Rule(
                "lcm($a, $b)",
                _of_no_numeric(
                    "lcm($a, $b)",
                    ch_checked(
                        [
                            ch_fail("22003", f"{_LCM_NAMES[held]} out of range").format(
                                condition=f"{_of_type('$a', held)} AND $a != 0 AND $b != 0 "
                                f"AND {_LCM} > {largest}"
                            )
                            for held, largest in _LCM_LARGEST.items()
                        ],
                        f"if($a IS NULL OR $b IS NULL, NULL, "
                        f"toInt64(if($a = 0 OR $b = 0, 0, {_LCM})))",
                    ),
                ),
            ),
        )
    ),
)

#: chr of a code point (oracle_compat.c's chr, a UTF-8 database): its UTF-8 bytes. A negative is
#: 22023, zero (NUL), a surrogate and past 0x10FFFF 54000.
_CP = "toInt64($x)"
_CONTINUED = "bitOr(0x80, bitAnd(bitShiftRight({cp}, {shift}), 0x3F))"


def _continuation(shift: int) -> str:
    return _CONTINUED.format(cp=_CP, shift=shift)


_UTF8 = (
    f"multiIf({_CP} < 0x80, char({_CP}), "
    f"{_CP} < 0x800, char(bitOr(0xC0, bitShiftRight({_CP}, 6)), {_continuation(0)}), "
    f"{_CP} < 0x10000, char(bitOr(0xE0, bitShiftRight({_CP}, 12)), {_continuation(6)}, "
    f"{_continuation(0)}), "
    f"char(bitOr(0xF0, bitShiftRight({_CP}, 18)), {_continuation(12)}, {_continuation(6)}, "
    f"{_continuation(0)}))"
)
_OUTSIDE = f"{_CP} < 1 OR {_CP} > 0x10FFFF OR ({_CP} >= 0xD800 AND {_CP} <= 0xDFFF)"

_CHR = ch_checked(
    [
        ch_fail("22023", "character number must be positive").format(condition=f"{_CP} < 0"),
        ch_fail("54000", "null character not permitted").format(condition=f"{_CP} = 0"),
        ch_fail("54000", "requested character too large for encoding").format(
            condition=f"{_CP} > 0x10FFFF"
        ),
        ch_fail("54000", "requested character not valid for encoding").format(
            condition=f"{_CP} >= 0xD800 AND {_CP} <= 0xDFFF"
        ),
    ],
    f"if({_OUTSIDE}, NULL, {_UTF8})",
)

_CHR_ONCE = ch_once(_CHR)
#: chr on Trino: its own chr of a code point, PostgreSQL's errors of the ones it has no character of.
_T_CHR = _t_checked(
    [
        ("$x < 0", "22023", "character number must be positive"),
        ("$x = 0", "54000", "null character not permitted"),
        ("$x > 1114111", "54000", "requested character too large for encoding"),
        ("$x >= 55296 AND $x <= 57343", "54000", "requested character not valid for encoding"),
    ],
    "chr($x)",
)


def _chr(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """chr of a code point. sqlglot reads "char" (the one-byte type's cast) as its chr too: a call
    written by that name keeps what it had."""
    if isinstance(node, exp.Anonymous) and node.name.lower() != "chr":
        return None
    call = trino.called(node)
    if call is None or len(call[1]) != 1:
        return None
    form = _T_CHR if dialect in trino.TRINO else _CHR_ONCE
    return graft(replacement(form, written_in(dialect), "chr($x)"), {"x": call[1][0]})


CHR = Feature(
    "chr",
    FAMILY,
    "The character of a code point, as its UTF-8 bytes.",
    read=Side(
        rules=(Rule("chr($x)", {**ch_only(Rewrite(_chr)), **trino.on_trino(Rewrite(_chr))}),)
    ),
)

#: in_range of numbers (a window frame's bound, int8.c's and float.c's in_range): whether the
#: value is at or below (less) or at or above the base plus or minus (subtract) the offset. A
#: negative offset, or a NaN one, is 22013. Integers: the sum is exact in Int128, which answers as
#: PostgreSQL's overflow rule does (a sum past the type is past every value). Floats: NaN after
#: every value, an infinite offset from an infinite base of its sign every value.
_IN_RANGE_NULL = "$v IS NULL OR $b IS NULL OR $o IS NULL OR $s IS NULL OR $l IS NULL"
_NEGATIVE_OFFSET = ch_fail("22013", "invalid preceding or following size in window function")
_INTEGER_SUM = "if($s, toInt128($b) - toInt128($o), toInt128($b) + toInt128($o))"
_IN_RANGE_INTEGERS = ch_checked(
    [_NEGATIVE_OFFSET.format(condition="$o < 0")],
    f"if({_IN_RANGE_NULL}, NULL, "
    f"if($l, toInt128($v) <= {_INTEGER_SUM}, toInt128($v) >= {_INTEGER_SUM}))",
)
_FLOAT_SUM = "if($s, toFloat64($b) - toFloat64($o), toFloat64($b) + toFloat64($o))"
_IN_RANGE_FLOATS = ch_checked(
    [_NEGATIVE_OFFSET.format(condition="isNaN(toFloat64($o)) OR $o < 0")],
    f"multiIf({_IN_RANGE_NULL}, NULL, "
    "isNaN(toFloat64($v)), if(isNaN(toFloat64($b)), true, NOT $l), "
    "isNaN(toFloat64($b)), $l, "
    "isInfinite(toFloat64($o)) AND isInfinite(toFloat64($b)) AND if($s, $b > 0, $b < 0), true, "
    f"if($l, toFloat64($v) <= {_FLOAT_SUM}, toFloat64($v) >= {_FLOAT_SUM}))",
)
#: in_range of floats on Trino (float.c's in_range_float8_float8), as 1, 0 or NULL: the caller
#: compares it with 1, a Trino boolean not being comparable with an integer.
_T_V, _T_B, _T_O = (f"CAST(${h} AS DOUBLE)" for h in "vbo")
_T_FLOAT_SUM = f"IF($s, {_T_B} - {_T_O}, {_T_B} + {_T_O})"
_T_IN_RANGE_FLOATS = (
    f"CASE WHEN {_IN_RANGE_NULL} THEN NULL "
    f"WHEN is_nan({_T_O}) OR {_T_O} < 0 THEN "
    f"{fail('22013', 'invalid preceding or following size in window function')} "
    f"WHEN is_nan({_T_V}) THEN IF(is_nan({_T_B}) OR NOT $l, 1, 0) "
    f"WHEN is_nan({_T_B}) THEN IF($l, 1, 0) "
    f"WHEN is_infinite({_T_O}) AND is_infinite({_T_B}) AND IF($s, {_T_B} > 0, {_T_B} < 0) THEN 1 "
    f"WHEN IF($l, {_T_V} <= {_T_FLOAT_SUM}, {_T_V} >= {_T_FLOAT_SUM}) THEN 1 ELSE 0 END"
)
_IN_RANGE = "in_range($v, $b, $o, $s, $l)"
_INTEGER_NAMES = frozenset({"smallint", "integer", "bigint"})
_FLOAT_NAMES = frozenset({"real", "double precision"})


def in_range_of_numbers(
    said: list[exp.Expr], args: list[exp.Expr], dialect: str
) -> exp.Expr | None:
    """in_range of integers or of floats on ClickHouse, and of floats on Trino, of ``args``, as
    the statement says the value's and the offset's types (of ``said``, the arguments as written);
    None for any other (a date's, a numeric's, a column no statement types), which keeps what it
    had. pg_ranges lowers every in_range before the grid runs, and asks here first."""
    if len(args) != 5:  # noqa: PLR2004 -- PostgreSQL's five arguments
        return None
    value, _, offset = (function_types.argument_type(a) for a in said[:3])
    if dialect in trino.TRINO and value in _FLOAT_NAMES and offset in _FLOAT_NAMES:
        bindings = dict(zip("vbosl", args, strict=True))
        return graft(replacement(_T_IN_RANGE_FLOATS, written_in(dialect), _IN_RANGE), bindings)
    if dialect != "clickhouse":
        return None
    if value in _INTEGER_NAMES and offset in _INTEGER_NAMES:
        form = _IN_RANGE_INTEGERS
    elif value in _FLOAT_NAMES and offset in _FLOAT_NAMES:
        form = _IN_RANGE_FLOATS
    else:
        return None
    bindings = dict(zip("vbosl", args, strict=True))
    return graft(replacement(ch_once(form), dialect, _IN_RANGE), bindings)


def _in_range(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """in_range of integers or of floats: in_range_of_numbers of the call's arguments."""
    call = trino.called(node)
    return None if call is None else in_range_of_numbers(call[1], call[1], dialect)


IN_RANGE = Feature(
    "in_range",
    FAMILY,
    "Whether a value is within an offset of a base: a window frame's bound.",
    read=Side(rules=(Rule(_IN_RANGE, ch_only(Rewrite(_in_range))),)),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 140
FEATURES: tuple[Feature, ...] = (
    *COMPARISONS,
    *CHOSEN,
    *DOMAINS,
    SQRT,
    *LOGS,
    *EXPONENTIALS,
    *POWERS,
    DEGREES,
    *DEGREE_TRIGONOMETRY,
    SIGN,
    ABS,
    MOD,
    INTEGERS_OF_FLOATS,
    LCM,
    CHR,
    IN_RANGE,
)
