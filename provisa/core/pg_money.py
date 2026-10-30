# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6c9e14-7b3d-4a18-9c52-e08d41a7b6f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's money values (REQ-2167, REQ-2171), as src/backend/utils/adt/cash.c reads and writes
them under the one lc_monetary pgwire reports, en_US.UTF-8: a dollar sign, comma thousands, two
decimals, a leading minus. Values are whole cents, money's own int8."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from provisa.core.pg_error import PostgresError

#: The lc_monetary every money value is read and written under, reported to every client.
LC_MONETARY = "en_US.UTF-8"

_INT8_MIN = -(2**63)
_INT8_MAX = 2**63 - 1
#: Each comes as one digit a cents value can show in.
_CENTS_DIGITS = 2

_SPACES = " \t\n\r\f\v"


def _out_of_range(text: str) -> PostgresError:
    return PostgresError("22003", f'value "{text}" is out of range for type money')


def _invalid(text: str) -> PostgresError:
    return PostgresError("22P02", f'invalid input syntax for type money: "{text}"')


def _skip(text: str, at: int) -> int:
    while at < len(text) and text[at] in _SPACES:
        at += 1
    return at


def _past_symbol(text: str, at: int) -> int:
    """Past the spaces, one dollar sign and the spaces after it."""
    at = _skip(text, at)
    if text[at : at + 1] == "$":
        at += 1
    return _skip(text, at)


def _digits(text: str, at: int) -> tuple[int, int, int]:
    """(value, decimals read, position): the digits ``text`` has from ``at``, a dot among them,
    the commas skipped, up to two decimals."""
    value = 0
    decimals = 0
    dot = False
    while at < len(text):
        char = text[at]
        if char in "0123456789" and (not dot or decimals < _CENTS_DIGITS):
            value = value * 10 + int(char)
            decimals += dot
        elif char == "." and not dot:
            dot = True
        elif char != ",":
            break
        at += 1
    return value, decimals, at


def cash_in(text: str) -> int:
    """money's input: whole cents, rounded half away from zero on the third decimal; PostgreSQL's
    own error for what it refuses."""
    at = _past_symbol(text, 0)
    negative = text[at : at + 1] in ("-", "(")
    if text[at : at + 1] in ("-", "(", "+"):
        at += 1
    at = _past_symbol(text, at)
    value, decimals, at = _digits(text, at)
    if text[at : at + 1].isdigit() and text[at] >= "5":
        value += 1
    value *= 10 ** (_CENTS_DIGITS - decimals)
    while text[at : at + 1].isdigit():
        at += 1
    while at < len(text):
        char = text[at]
        if char == "-":
            negative = True
        elif not (char in _SPACES or char in ")+$"):
            raise _invalid(text)
        at += 1
    value = -value if negative else value
    if not _INT8_MIN <= value <= _INT8_MAX:
        raise _out_of_range(text)
    return value


def cash_out(cents: int) -> str:
    """money's output: ``$1,234.56``, ``-$1,234.56``."""
    dollars, part = divmod(abs(cents), 100)
    return f"{'-' if cents < 0 else ''}${dollars:,}.{part:02d}"


_SMALL = (
    "zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen "
    "fifteen sixteen seventeen eighteen nineteen twenty"
).split()
_TENS = "x x twenty thirty forty fifty sixty seventy eighty ninety".split()
#: The one-word amounts: _SMALL covers 0..20.
_TEEN_END = 20


def _word(value: int) -> str:
    """cash.c's num_word: a number below a thousand. An even multiple of ten over a hundred takes
    no "and"; below twenty it does."""
    tens = value % 100
    if value <= _TEEN_END:
        return _SMALL[value]
    if not tens:
        return f"{_SMALL[value // 100]} hundred"
    hundreds = f"{_SMALL[value // 100]} hundred " if value > 99 else ""  # noqa: PLR2004
    if value % 10 == 0 and tens > 10:  # noqa: PLR2004
        return f"{hundreds}{_TENS[tens // 10]}"
    if tens < _TEEN_END:
        return f"{hundreds}and {_SMALL[tens]}" if hundreds else _SMALL[tens]
    return f"{hundreds}{_TENS[tens // 10]} {_SMALL[tens % 10]}"


_SCALES = ("quadrillion", "trillion", "billion", "million", "thousand")


def cash_words(cents: int) -> str:
    """cash_words: the amount in words, its first letter a capital. A scale with nothing after it
    leaves two spaces, as cash.c's concatenation does (``One thousand  dollars``)."""
    value = abs(cents)
    dollars = value // 100
    out = "minus " if cents < 0 else ""
    remaining = dollars
    groups = []
    for _ in (*_SCALES, "units"):
        groups.append(remaining % 1000)
        remaining //= 1000
    groups.reverse()
    for group, scale in zip(groups, _SCALES):
        if group:
            out += f"{_word(group)} {scale} "
    if groups[-1]:
        out += _word(groups[-1])
    if dollars == 0:
        out += "zero"
    part = value % 100
    out += f" {'dollar' if dollars == 1 else 'dollars'} and {_word(part)}"
    out += " cent" if part == 1 else " cents"
    return out[0].upper() + out[1:]
