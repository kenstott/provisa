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

from provisa.transpiler.polyfill.grid import Feature, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Rewrite, replacement
from provisa.transpiler.polyfill.numbers import FAMILY, ch_checked, ch_fail, ch_only


def _ordered(hole: str) -> str:
    """A float's place in PostgreSQL's order: NaN after every other value, all NaN equal."""
    return f"tuple(isNaN({hole}), if(isNaN({hole}), 0, toFloat64({hole})))"


def _compared(operator: str) -> str:
    """Two floats compared in PostgreSQL's order; NULL where either is NULL (each is strict)."""
    return f"if($a IS NULL OR $b IS NULL, NULL, {_ordered('$a')} {operator} {_ordered('$b')})"


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
        read=Side(rules=(Rule(f"{prefix}{suffix}($a, $b)", ch_only(_compared(operator))),)),
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
                    ch_only(f"if({_compared(operator)}, $a, if($a IS NULL, NULL, $b))"),
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


def _domain(name: str, outside: str, summary: str, answer: str | None = None) -> Feature:
    """``name`` of a double, PostgreSQL's 22003 where ``outside`` (of ``$x``) holds."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(
                Rule(
                    f"{name}($x)",
                    ch_only(
                        ch_checked(
                            [_OUT_OF_RANGE.format(condition=outside)], answer or f"{name}($x)"
                        )
                    ),
                ),
            )
        ),
    )


_INFINITE = "isInfinite(toFloat64($x))"

DOMAINS: tuple[Feature, ...] = (
    _domain("acos", "abs($x) > 1", "The arc cosine of a value from -1 to 1."),
    _domain("asin", "abs($x) > 1", "The arc sine of a value from -1 to 1."),
    _domain("atanh", "abs($x) > 1", "The inverse hyperbolic tangent of a value from -1 to 1."),
    _domain("acosh", "$x < 1", "The inverse hyperbolic cosine of a value of 1 or more."),
    _domain("sin", _INFINITE, "The sine of a finite value."),
    _domain("cos", _INFINITE, "The cosine of a finite value."),
    _domain("tan", _INFINITE, "The tangent of a finite value."),
    _domain("cot", _INFINITE, "The cotangent of a finite value.", "1.0 / tan($x)"),
)

SQRT = Feature(
    "sqrt",
    FAMILY,
    "The square root of a value of zero or more.",
    read=Side(
        rules=(
            Rule(
                "sqrt($x)",
                ch_only(
                    ch_checked(
                        [
                            ch_fail("2201F", "cannot take square root of a negative number").format(
                                condition="$x < 0"
                            )
                        ],
                        "sqrt($x)",
                    )
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


def _log10(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """log10 of one value: sqlglot reads log10 as log of base 10 where it is written plainly, and
    as a plain call where it is quoted or of pg_catalog, so no pattern names both."""
    if isinstance(node, exp.Anonymous) and node.name.lower() == "log10":
        args = list(node.expressions)
    elif isinstance(node, exp.Log) and node.expression is not None and node.this == _TEN:
        args = [node.expression]
    else:
        return None
    if len(args) != 1:
        return None
    return graft(replacement(_LOG10, dialect, "log10($x)"), {"x": args[0]})


_TEN = exp.Literal.number(10)

LOGS: tuple[Feature, ...] = (
    Feature(
        "log",
        FAMILY,
        "The base 10 logarithm of a value above zero.",
        read=Side(rules=(Rule("log($x)", ch_only(_LOG10)),)),
    ),
    Feature(
        "log10",
        FAMILY,
        "The base 10 logarithm of a value above zero.",
        read=Side(rules=(Rule(None, {"clickhouse": Rewrite(_log10, mentions=r"\blog10\b")}),)),
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

POWERS: tuple[Feature, ...] = tuple(
    Feature(
        name,
        FAMILY,
        "A value raised to a power.",
        read=Side(rules=(Rule(f"{name}($x, $y)", ch_only(ch_checked(_POWER_RAISES, _POWER))),)),
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
                ch_only(
                    ch_checked(
                        [
                            _OVERFLOW.format(
                                condition=f"isInfinite(degrees($x)) AND {_FINITE.format('$x')}"
                            )
                        ],
                        "degrees($x)",
                    )
                ),
            ),
        )
    ),
)

SIGN = Feature(
    "sign",
    FAMILY,
    "The sign of a value: -1, 0 or 1; 0 of NaN.",
    read=Side(rules=(Rule("sign($x)", ch_only("if(isNaN(toFloat64($x)), 0, sign($x))")),)),
)

#: Each integer type ClickHouse holds a PostgreSQL integer in, and PostgreSQL's name of it.
_INTEGER_TYPES = {"Int16": "smallint", "Int32": "integer", "Int64": "bigint"}


def _of_type(hole: str, held: str) -> str:
    return f"toTypeName(assumeNotNull({hole})) = '{held}'"


#: abs: the least value of an integer type is its own negation, which has no value of the type.
ABS = Feature(
    "abs",
    FAMILY,
    "The absolute value; of an integer type's least value, out of range.",
    read=Side(
        rules=(
            Rule(
                "abs($a)",
                ch_only(
                    ch_checked(
                        [
                            ch_fail("22003", f"{name} out of range").format(
                                condition=f"{_of_type('$a', held)} AND $a < 0 AND -$a < 0"
                            )
                            for held, name in _INTEGER_TYPES.items()
                        ],
                        "if($a < 0, -$a, $a)",
                    )
                ),
            ),
        )
    ),
)

FEATURES: tuple[Feature, ...] = (
    *COMPARISONS,
    *CHOSEN,
    *DOMAINS,
    SQRT,
    *LOGS,
    *POWERS,
    DEGREES,
    SIGN,
    ABS,
)
