# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7d31-a2c4-4f1e-8d69-3c1f9a7e6b24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's exact numeric, for the forms of provisa.transpiler.function_macros (REQ-2171, AN
ANSWER THAT DIFFERS IS REFUSED): DuckDB's functions of a decimal are its double's, and PostgreSQL's
answer has the digits numeric.c chooses. Each function takes and answers a numeric's text, with the
scale of numeric.c's own function: ``select_div_scale`` and ``estimate_ln_dweight`` are read from
it. A numeric is a Decimal here, its exponent its dscale."""

# Requirements: REQ-2171

from __future__ import annotations

import decimal
import math
from decimal import Decimal
from fractions import Fraction

from provisa.core.pg_error import END, MARK
from provisa.transpiler.pg_forms_types import REFUSED

_MIN_SIG_DIGITS = 16
_MAX_SCALE = 1000
_DEC_DIGITS = 4
_NBASE = 10000
_LOG10_E = 0.434294481903252
#: What Decimal is told beyond the digits an answer keeps, so the last is rounded from true digits.
_GUARD = 40


def _error(sqlstate: str, message: str) -> ValueError:
    return ValueError(f"{MARK} {sqlstate}: {message}{END}")


def _dscale(x: Decimal) -> int:
    return max(0, -int(x.as_tuple().exponent))


def _weight(x: Decimal) -> int:
    """The base-10000 weight of a value's first digit: numeric's groups align at the point."""
    return x.copy_abs().adjusted() // _DEC_DIGITS


def _groups(x: Decimal) -> tuple[int, int, bool]:
    """A nonzero value's first two base-10000 digits and whether it has a digit past the first."""
    size, weight = Fraction(abs(x)), _weight(x)
    first = int(size / Fraction(_NBASE) ** weight)
    rest = size - first * Fraction(_NBASE) ** weight
    return first, int(rest / Fraction(_NBASE) ** (weight - 1)), rest != 0


def _bounded(scale: int, *least: int) -> int:
    return min(max(scale, *least, 0), _MAX_SCALE)


def _rounded(value: Fraction, scale: int) -> Decimal:
    """``value`` at ``scale`` places, a half away from zero: round_var."""
    shifted = abs(value) * 10**scale
    digits = (2 * shifted.numerator + shifted.denominator) // (2 * shifted.denominator)
    sign = 1 if value < 0 and digits else 0
    return Decimal((sign, tuple(map(int, str(digits))), -scale))


def _div_scale(a: Decimal, b: Decimal) -> int:
    """select_div_scale: 16 significant digits of the quotient, as its weight estimates them."""
    weight_a, first_a = (0, 0) if a == 0 else (_weight(a), _groups(a)[0])
    weight_b, first_b = (0, 0) if b == 0 else (_weight(b), _groups(b)[0])
    weight = weight_a - weight_b - (1 if first_a <= first_b else 0)
    return _bounded(_MIN_SIG_DIGITS - weight * _DEC_DIGITS, _dscale(a), _dscale(b))


def _ln_dweight(x: Decimal) -> int:
    """estimate_ln_dweight: the decimal weight of ln(x)."""
    if Decimal("0.9") <= x <= Decimal("1.1"):
        offset = x - 1
        if offset == 0:
            return 0
        return _weight(offset) * _DEC_DIGITS + int(math.log10(_groups(offset)[0]))
    first, second, more = _groups(x)
    dweight = _weight(x) * _DEC_DIGITS
    digits = float(first)
    if more:
        digits = float(first * _NBASE + second)
        dweight -= _DEC_DIGITS
    return int(math.log10(abs(math.log(digits) + dweight * math.log(10))))


def _high(digits: int) -> decimal.Context:
    return decimal.Context(
        prec=digits + _GUARD, rounding=decimal.ROUND_HALF_EVEN, Emax=10**9, Emin=-(10**9)
    )


def _at(value: Decimal, scale: int) -> str:
    return format(_rounded(Fraction(value), scale), "f")


def _sqrt(x: Decimal) -> str:
    if x < 0:
        raise _error("2201F", "cannot take square root of a negative number")
    weight = 0 if x == 0 else _weight(x)
    root_weight = (weight + 1) * _DEC_DIGITS // 2 - 1
    scale = _bounded(_MIN_SIG_DIGITS - root_weight, _dscale(x))
    return _at(_high(scale + abs(root_weight)).sqrt(x), scale)


def _ln(x: Decimal) -> str:
    if x == 0:
        raise _error("2201E", "cannot take logarithm of zero")
    if x < 0:
        raise _error("2201E", "cannot take logarithm of a negative number")
    scale = _bounded(_MIN_SIG_DIGITS - _ln_dweight(x), _dscale(x))
    return _at(_high(scale + abs(x.adjusted())).ln(x), scale)


def _log(base: Decimal, x: Decimal) -> str:
    for value in (base, x):
        if value == 0:
            raise _error("2201E", "cannot take logarithm of zero")
        if value < 0:
            raise _error("2201E", "cannot take logarithm of a negative number")
    if base == 1:
        raise _error("22012", "division by zero")
    weight = _ln_dweight(x) - _ln_dweight(base)
    scale = _bounded(_MIN_SIG_DIGITS - weight, _dscale(base), _dscale(x))
    context = _high(scale + abs(x.adjusted()) + abs(base.adjusted()) + _GUARD)
    return _at(context.divide(context.ln(x), context.ln(base)), scale)


def _exp(x: Decimal) -> str:
    size = float(x) * _LOG10_E
    if size >= _MAX_SCALE * 100:
        raise _error("22003", "value overflows numeric format")
    scale = _bounded(_MIN_SIG_DIGITS - int(max(size, -_MAX_SCALE)), _dscale(x))
    return _at(_high(scale + abs(int(size)) + 1).exp(x), scale)


def _integer_power(base: Decimal, n: int, scale_of_n: int) -> str:
    """power_var_int: exact, at the scale its estimate of the result's weight says."""
    estimate = 0.0 if base == 0 else n * math.log10(abs(float(base)))
    scale = _bounded(_MIN_SIG_DIGITS - int(estimate), _dscale(base), scale_of_n)
    return format(_rounded(Fraction(base) ** n, scale), "f")


def _power(base: Decimal, power: Decimal) -> str:
    if base == 0 and power < 0:
        raise _error("2201F", "zero raised to a negative power is undefined")
    is_integer = power == power.to_integral_value()
    if base < 0 and not is_integer:
        raise _error(
            "2201F", "a negative number raised to a non-integer power yields a complex result"
        )
    if base == 0 and power == 0:
        return format(Decimal(1).quantize(Decimal(1).scaleb(-_dscale(power))), "f")
    if is_integer and abs(power) < 2**31:
        return _integer_power(base, int(power), _dscale(power))
    if base == 0:
        return format(Decimal(0).quantize(Decimal(1).scaleb(-_MIN_SIG_DIGITS)), "f")
    size = float(power) * math.log(float(base)) * _LOG10_E
    scale = _bounded(_MIN_SIG_DIGITS - int(size), _dscale(base), _dscale(power))
    context = _high(scale + abs(int(size)) + 1)
    return _at(context.power(base, power), scale)


def _at_quotient(a: Decimal, b: Decimal) -> str:
    return format(_rounded(Fraction(a) / Fraction(b), _div_scale(a, b)), "f")


def _size_pretty(size: Decimal) -> str:
    """pg_size_pretty(numeric): a shift short of each unit, so the last is rounded a half away."""
    if abs(size) < 10 * 1024:
        return f"{format(size, 'f')} bytes"
    value = size
    for unit, limit in zip(("kB", "MB", "GB", "TB"), (20 * 1024 - 1,) * 4):
        value = _truncated(Fraction(value) / (1 << (9 if unit == "kB" else 10)))
        if abs(value) < limit:
            return f"{format(_half_rounded(value), 'f')} {unit}"
    value = _truncated(Fraction(value) / (1 << 10))
    return f"{format(_half_rounded(value), 'f')} PB"


def _truncated(value: Fraction) -> Decimal:
    return Decimal(int(value))


def _half_rounded(value: Decimal) -> Decimal:
    return _truncated(Fraction(value + (1 if value >= 0 else -1)) / 2)


_UNARY = {
    "exp": _exp,
    "ln": _ln,
    "log10": lambda x: _log(Decimal(10), x),
    "pg_size_pretty": _size_pretty,
    "sqrt": _sqrt,
}


def _div(a: Decimal, b: Decimal) -> str:
    if b == 0:
        raise _error("22012", "division by zero")
    return format(Decimal(int(Fraction(a) / Fraction(b))), "f")


_BINARY = {"div": _div, "log": _log, "power": _power}


def numeric_unary(name: str, value: str) -> str:
    return _UNARY[name](Decimal(value))


def numeric_binary(name: str, a: str, b: str) -> str:
    return _BINARY[name](Decimal(a), Decimal(b))


def numeric_quotient(a: str, b: str) -> str:
    """numeric_div: what avg answers of a sum and a count."""
    return _at_quotient(Decimal(a), Decimal(b))


def numeric_spread(name: str, count: int, total: str, squares: str) -> str | None:
    """numeric_stddev_internal: the variance's numerator and denominator exact, rounded at the
    quotient's scale, and its root at the same."""
    sample = name not in ("var_pop", "stddev_pop")
    if count == 0 or (sample and count == 1):
        return None
    n, sum_x, sum_x2 = Decimal(count), Decimal(total), Decimal(squares)
    context = decimal.Context(
        prec=len(sum_x.as_tuple().digits) * 2 + len(sum_x2.as_tuple().digits) + 40
    )
    numerator = context.subtract(context.multiply(n, sum_x2), context.multiply(sum_x, sum_x))
    numerator = numerator.quantize(Decimal(1).scaleb(-_dscale(sum_x2)), context=context)
    if numerator <= 0:
        return "0"
    denominator = n * (n - 1) if sample else n * n
    scale = _div_scale(numerator, denominator)
    variance = _rounded(Fraction(numerator) / Fraction(denominator), scale)
    if name in ("variance", "var_samp", "var_pop"):
        return format(variance, "f")
    return _at(_high(scale + abs(variance.adjusted())).sqrt(variance), scale)


def numeric_of_float(value: float, digits: int) -> str:
    """float8_numeric and float4_numeric: the float's shortest ``digits`` significant digits."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return format(Decimal(f"{value:.{digits}g}"), "f")


def to_number(value: str, form: str) -> str:
    """to_number of a format of digits, a point and a sign (formatting.c's plain cases); any other
    is refused by name."""
    template = form.upper()
    signed = template.startswith("S")
    body = template[1:] if signed else template
    whole, _, fraction = body.partition(".")
    plain = set(whole) <= set("90,") and set(fraction) <= set("90")
    text = value.strip().replace(",", "")
    negative = text.startswith("-")
    digits = text.lstrip("+-")
    head, _, tail = digits.partition(".")
    if not plain or not (head + tail).isdigit() or len(head) > whole.count("9") + whole.count("0"):
        raise ValueError(f"{MARK} 0A000: to_number {REFUSED}{END}")
    scale = len(fraction)
    number = Decimal(f"{head or 0}.{tail or 0}")
    number = number.quantize(Decimal(1).scaleb(-scale), rounding=decimal.ROUND_DOWN)
    return format(-number if negative else number, "f")


def en_us_key(value: str, called: str) -> str:
    """A text that compares as ``value`` does under en_US.utf8, for letters and digits: their
    letters, then their case, lower first. Anything else it weighs by rules of its own, and a value
    holding it is refused, naming ``called``, the function ordering it."""
    if not value.isascii() or not value.isalnum():
        raise ValueError(f"{MARK} 0A000: {called} {REFUSED}{END}")
    return f"{value.lower()}\x01{''.join('b' if c.isupper() else 'a' for c in value)}"
