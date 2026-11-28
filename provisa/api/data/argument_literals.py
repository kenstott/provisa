# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2b9d47-0c81-4a3f-b6d2-9a17c4e8f053
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A quoted literal a SQL statement passes to a command, read as PostgreSQL reads it (REQ-1159,
REQ-2236): a quoted literal has the type unknown, and a function's parameter reads it with its own
type's input function, so ``greet('1')`` binds the double 1.0 to a double parameter and
``greet('abc')`` is refused with float8in's error (22P02). Only a quoted literal is read this way:
a value that arrives typed (a Cypher string, a SQL TRUE) is checked against the declared type as
it is (action_exec._of_declared_types)."""

# Requirements: REQ-1159, REQ-2236

from __future__ import annotations

import datetime
import math
import re
from collections.abc import Callable
from decimal import Decimal

from provisa.core.pg_error import PostgresError
from provisa.transpiler.pg_type_literals import Refused, integer_read, numeric_text


class UnknownLiteral(str):
    """A quoted SQL literal: PostgreSQL's type unknown, whose value is the type of the parameter
    it is passed to."""


#: Each integer IR type's range and the name its input error says (int2in, int4in, int8in).
_INTEGERS = {
    "smallint": (-(2**15), 2**15 - 1),
    "integer": (-(2**31), 2**31 - 1),
    "bigint": (-(2**63), 2**63 - 1),
}
#: float4in's and float8in's text: a decimal with an optional exponent, between spaces.
_FLOAT_TEXT = re.compile(r"\s*[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?\s*", re.ASCII)
_FLOAT_SPECIALS = {
    "nan": math.nan,
    "+nan": math.nan,
    "-nan": math.nan,
    "infinity": math.inf,
    "+infinity": math.inf,
    "inf": math.inf,
    "+inf": math.inf,
    "-infinity": -math.inf,
    "-inf": -math.inf,
}
#: Each float IR type's PostgreSQL name and its largest finite magnitude and smallest nonzero one.
_FLOATS = {
    "float": ("real", 3.4028234663852886e38, 1.401298464324817e-45),
    "double": ("double precision", 1.7976931348623157e308, 5e-324),
}
#: boolin's words, as parse_bool_with_len reads them: any prefix of true, false, yes or no, "on",
#: "of" or "off" (an "o" alone is either), "1" and "0".
_TRUE = {"t", "tr", "tru", "true", "y", "ye", "yes", "on", "1"}
_FALSE = {"f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0"}
_ISO_DATE = re.compile(r"\s*(\d{4})-(\d{2})-(\d{2})\s*", re.ASCII)
_ISO_TIMESTAMP = re.compile(
    r"\s*(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?\s*",
    re.ASCII,
)


def _syntax(type_name: str, text: str) -> PostgresError:
    return PostgresError("22P02", f'invalid input syntax for type {type_name}: "{text}"')


def _integer(ir: str) -> Callable[[str], int]:
    low, high = _INTEGERS[ir]

    def read(text: str) -> int:
        number = integer_read(text)
        if number is None:
            raise _syntax(ir, text)
        if not low <= number <= high:
            raise PostgresError("22003", f'value "{text}" is out of range for type {ir}')
        return number

    return read


def _float(ir: str) -> Callable[[str], float]:
    name, largest, smallest = _FLOATS[ir]

    def read(text: str) -> float:
        special = _FLOAT_SPECIALS.get(text.strip().lower())
        if special is not None:
            return special
        if _FLOAT_TEXT.fullmatch(text) is None:
            raise _syntax(name, text)
        number = float(text)
        mantissa = text.strip().lower().split("e")[0].strip("+-.0")
        if (
            abs(number) > largest
            or (number != 0 and abs(number) < smallest)
            or (number == 0 and mantissa)
        ):
            raise PostgresError("22003", f'"{text}" is out of range for type {name}')
        return number

    return read


def _numeric(text: str) -> Decimal:
    try:
        return Decimal(numeric_text(text))
    except Refused as refused:
        raise PostgresError(refused.sqlstate, refused.message) from refused


def _boolean(text: str) -> bool:
    word = text.strip().lower()
    if word in _TRUE:
        return True
    if word in _FALSE:
        return False
    raise _syntax("boolean", text)


def _date(text: str) -> datetime.date:
    matched = _ISO_DATE.fullmatch(text)
    if matched is None:
        raise _syntax("date", text)
    try:
        return datetime.date(*(int(part) for part in matched.groups()))
    except ValueError:
        raise PostgresError("22008", f'date/time field value out of range: "{text}"') from None


def _timestamp(text: str) -> datetime.datetime:
    matched = _ISO_TIMESTAMP.fullmatch(text)
    if matched is None:
        raise _syntax("timestamp", text)
    year, month, day, hour, minute, second, fraction = matched.groups()
    try:
        return datetime.datetime(
            int(year),
            int(month),
            int(day),
            int(hour or 0),
            int(minute or 0),
            int(second or 0),
            int((fraction or "0").ljust(6, "0")),
        )
    except ValueError:
        raise PostgresError("22008", f'date/time field value out of range: "{text}"') from None


#: The IR types whose quoted literal is read here, each by its input function. The literal of a
#: type not here is passed on as written, the text the command's source reads it from.
_READERS: dict[str, Callable[[str], object]] = {
    **{ir: _integer(ir) for ir in _INTEGERS},
    **{ir: _float(ir) for ir in _FLOATS},
    "numeric": _numeric,
    "boolean": _boolean,
    "date": _date,
    "timestamp": _timestamp,
}


def read_literal(ir: str, literal: UnknownLiteral) -> object:
    """``literal`` read as a value of the IR type ``ir`` by that type's input function; raises
    :class:`PostgresError` with PostgreSQL's error for a text the input function refuses."""
    reader = _READERS.get(ir)
    return str(literal) if reader is None else reader(str(literal))
