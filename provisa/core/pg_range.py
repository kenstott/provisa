# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1c2a84-3e57-4d09-b6a1-c97d40e8f215
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's range and multirange values, in Python (REQ-2171, RANGES ARE EMULATED ON EVERY
ENGINE; REQ-2167).

The algorithms are src/backend/utils/adt/rangetypes.c's and multirangetypes.c's. A value is held
in PostgreSQL's text form everywhere else; here it is read, so a literal in a statement is
canonical and refused as PostgreSQL refuses it before any engine sees it, and a statement of
literals is answered where it is read."""

# Requirements: REQ-2171, REQ-2167

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from provisa.core.pg_error import PostgresError

_INT4_MAX = 2**31 - 1
_INT8_MAX = 2**63 - 1

#: Words a date or timestamp bound may be written as, which sort beyond every value.
_INFINITE = {"infinity": 1, "+infinity": 1, "-infinity": -1}


@dataclass(frozen=True)
class RangeType:
    """One of PostgreSQL's built-in range types: its element and its multirange."""

    name: str
    oid: int
    element: str  # integer, bigint, numeric, timestamp, timestamptz, date
    multirange: str
    multirange_oid: int

    @property
    def discrete(self) -> bool:
        return self.element in ("integer", "bigint", "date")


RANGE_TYPES: dict[str, RangeType] = {
    r.name: r
    for r in (
        RangeType("int4range", 3904, "integer", "int4multirange", 4451),
        RangeType("int8range", 3926, "bigint", "int8multirange", 4536),
        RangeType("numrange", 3906, "numeric", "nummultirange", 4532),
        RangeType("tsrange", 3908, "timestamp", "tsmultirange", 4533),
        RangeType("tstzrange", 3910, "timestamptz", "tstzmultirange", 4534),
        RangeType("daterange", 3912, "date", "datemultirange", 4535),
    )
}
MULTIRANGE_TYPES: dict[str, RangeType] = {r.multirange: r for r in RANGE_TYPES.values()}


class Infinite:
    """A date or timestamp bound written ``infinity`` or ``-infinity``: a value, not an absent
    bound (``upper_inf`` of ``[2020-01-01,infinity)`` is false)."""

    __slots__ = ("sign",)

    def __init__(self, sign: int) -> None:
        self.sign = sign

    def __eq__(self, other: object) -> bool:  # object-ok: the data model's own signature
        return isinstance(other, Infinite) and other.sign == self.sign

    def __hash__(self) -> int:
        return hash(self.sign)

    def __str__(self) -> str:
        return "infinity" if self.sign > 0 else "-infinity"


def _compare_values(a: Any, b: Any) -> int:
    if isinstance(a, Infinite) or isinstance(b, Infinite):
        a_sign = a.sign if isinstance(a, Infinite) else 0
        b_sign = b.sign if isinstance(b, Infinite) else 0
        return (a_sign > b_sign) - (a_sign < b_sign)
    return (a > b) - (a < b)


def _syntax(type_name: str, written: str) -> PostgresError:
    return PostgresError("22007", f'invalid input syntax for type {type_name}: "{written}"')


def _integer(written: str, type_name: str, limit: int) -> int:
    try:
        value = int(written.strip())
    except ValueError:
        raise PostgresError(
            "22P02", f'invalid input syntax for type {type_name}: "{written}"'
        ) from None
    if not -limit - 1 <= value <= limit:
        raise PostgresError("22003", f'value "{written}" is out of range for type {type_name}')
    return value


def _numeric(written: str) -> Decimal:
    try:
        value = Decimal(written.strip())
    except InvalidOperation:
        raise PostgresError(
            "22P02", f'invalid input syntax for type numeric: "{written}"'
        ) from None
    return value


def _date(written: str) -> Any:
    word = _INFINITE.get(written.strip().lower())
    if word is not None:
        return Infinite(word)
    try:
        return date.fromisoformat(written.strip())
    except ValueError:
        raise _syntax("date", written) from None


def _timestamp(written: str, zoned: bool) -> Any:
    word = _INFINITE.get(written.strip().lower())
    if word is not None:
        return Infinite(word)
    try:
        value = datetime.fromisoformat(written.strip())
    except ValueError:
        name = "timestamp with time zone" if zoned else "timestamp"
        raise _syntax(name, written) from None
    if not zoned:
        return value.replace(tzinfo=None)
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc)


def read_element(rtype: RangeType, written: str) -> Any:
    """A bound's value, read as the range's element type."""
    element = rtype.element
    if element == "integer":
        return _integer(written, "integer", _INT4_MAX)
    if element == "bigint":
        return _integer(written, "bigint", _INT8_MAX)
    if element == "numeric":
        return _numeric(written)
    if element == "date":
        return _date(written)
    return _timestamp(written, element == "timestamptz")


def _clock(value: datetime) -> str:
    written = value.strftime("%Y-%m-%d %H:%M:%S")
    return f"{written}.{value.microsecond:06d}".rstrip("0") if value.microsecond else written


def element_text(value: Any) -> str:
    """A bound's value in PostgreSQL's text form."""
    if isinstance(value, Infinite):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return _clock(value)
        return _clock(value.astimezone(timezone.utc)) + "+00"
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _step(rtype: RangeType, value: Any) -> Any:
    """The discrete value after ``value``, refused where the element type ends."""
    if isinstance(value, Infinite):
        return value
    if rtype.element == "date":
        return value + timedelta(days=1)
    limit = _INT4_MAX if rtype.element == "integer" else _INT8_MAX
    if value >= limit:
        raise PostgresError(
            "22003", "integer out of range" if rtype.element == "integer" else "bigint out of range"
        )
    return value + 1


@dataclass(frozen=True)
class Range:
    """A range value; a ``None`` bound is unbounded, ``empty`` holds no value."""

    rtype: RangeType
    empty: bool
    lower: Any = None
    upper: Any = None
    lower_inc: bool = False
    upper_inc: bool = False


def _empty(rtype: RangeType) -> Range:
    return Range(rtype, True)


def make_range(rtype: RangeType, lower: Any, upper: Any, lower_inc: bool, upper_inc: bool) -> Range:
    """range_serialize: refused when reversed, canonical when discrete, empty when no value is
    held. An unbounded side is exclusive."""
    lower_inc = lower_inc and lower is not None
    upper_inc = upper_inc and upper is not None
    if lower is not None and upper is not None:
        order = _compare_values(lower, upper)
        if order > 0:
            raise PostgresError(
                "22000", "range lower bound must be less than or equal to range upper bound"
            )
        if order == 0 and not (lower_inc and upper_inc):
            return _empty(rtype)
    if rtype.discrete:
        if lower is not None and not lower_inc:
            lower, lower_inc = _step(rtype, lower), True
        if upper is not None and upper_inc:
            upper, upper_inc = _step(rtype, upper), False
        if lower is not None and upper is not None and _compare_values(lower, upper) >= 0:
            return _empty(rtype)
    return Range(rtype, False, lower, upper, lower_inc, upper_inc)


def _malformed(kind: str, written: str, detail: str) -> PostgresError:
    error = PostgresError("22P02", f'malformed {kind} literal: "{written}"')
    error.detail = detail  # type: ignore[attr-defined]
    return error


_SPACE = " \t\n\r\f\v"


def _read_bound(text: str, at: int, enders: str) -> tuple[str | None, int]:
    """range_parse_bound: the bound's text from ``at`` up to one of ``enders``, quotes and
    backslashes read; None for an absent bound."""
    if text[at : at + 1] in tuple(enders):
        return None, at
    out: list[str] = []
    quoted = False
    while at < len(text):
        char = text[at]
        if char in enders and not quoted:
            break
        if char == "\\":
            at += 1
            if at >= len(text):
                raise _malformed("range", text, "Unexpected end of input.")
            out.append(text[at])
        elif char == '"':
            if quoted and text[at + 1 : at + 2] == '"':
                out.append('"')
                at += 1
            else:
                quoted = not quoted
        else:
            out.append(char)
        at += 1
    if at >= len(text):
        raise _malformed("range", text, "Unexpected end of input.")
    return "".join(out), at


def parse_range(rtype: RangeType, text: str) -> Range:
    """range_parse and range_in."""
    stripped = text.strip(_SPACE)
    if stripped.lower() == "empty":
        return _empty(rtype)
    if stripped[:1] not in ("[", "("):
        raise _malformed("range", text, "Missing left parenthesis or bracket.")
    lower_inc = stripped[0] == "["
    lower, at = _read_bound(stripped, 1, ",)]")
    if stripped[at] != ",":
        raise _malformed("range", text, "Missing comma after lower bound.")
    upper, at = _read_bound(stripped, at + 1, ",)]")
    if stripped[at] == ",":
        raise _malformed("range", text, "Too many commas.")
    if at != len(stripped) - 1:
        raise _malformed("range", text, "Junk after right parenthesis or bracket.")
    return make_range(
        rtype,
        None if lower is None else read_element(rtype, lower),
        None if upper is None else read_element(rtype, upper),
        lower_inc,
        stripped[at] == "]",
    )


_QUOTED = re.compile(r'[",\\()\[\]\s]')


def _bound_text(value: Any) -> str:
    written = element_text(value)
    if written == "" or _QUOTED.search(written):
        return '"' + written.replace("\\", "\\\\").replace('"', '""') + '"'
    return written


def range_text(value: Range) -> str:
    """range_out."""
    if value.empty:
        return "empty"
    lower = "" if value.lower is None else _bound_text(value.lower)
    upper = "" if value.upper is None else _bound_text(value.upper)
    return f"{'[' if value.lower_inc else '('}{lower},{upper}{']' if value.upper_inc else ')'}"


# --- comparison, as range_cmp_bounds --------------------------------------------------------------


def _cmp_bounds(a: Any, a_inc: bool, a_lower: bool, b: Any, b_inc: bool, b_lower: bool) -> int:
    if a is None:
        if b is None and a_lower == b_lower:
            return 0
        return -1 if a_lower else 1
    if b is None:
        return 1 if b_lower else -1
    order = _compare_values(a, b)
    if order:
        return order
    if not a_inc and not b_inc:
        return 0 if a_lower == b_lower else (1 if a_lower else -1)
    if not a_inc:
        return 1 if a_lower else -1
    if not b_inc:
        return -1 if b_lower else 1
    return 0


def _ll(a: Range, b: Range) -> int:
    return _cmp_bounds(a.lower, a.lower_inc, True, b.lower, b.lower_inc, True)


def _uu(a: Range, b: Range) -> int:
    return _cmp_bounds(a.upper, a.upper_inc, False, b.upper, b.upper_inc, False)


def _lu(a: Range, b: Range) -> int:
    """a's lower bound against b's upper."""
    return _cmp_bounds(a.lower, a.lower_inc, True, b.upper, b.upper_inc, False)


def range_cmp(a: Range, b: Range) -> int:
    if a.empty or b.empty:
        return (b.empty and not a.empty) - (a.empty and not b.empty)
    return _ll(a, b) or _uu(a, b)


def overlaps(a: Range, b: Range) -> bool:
    return not a.empty and not b.empty and _lu(a, b) <= 0 and _lu(b, a) <= 0


def contains(a: Range, b: Range) -> bool:
    if b.empty:
        return True
    return not a.empty and _ll(a, b) <= 0 and _uu(a, b) >= 0


def contains_element(a: Range, value: Any) -> bool:
    if a.empty:
        return False
    above = a.lower is None or _cmp_bounds(a.lower, a.lower_inc, True, value, True, True) <= 0
    below = a.upper is None or _cmp_bounds(a.upper, a.upper_inc, False, value, True, False) >= 0
    return above and below


def before(a: Range, b: Range) -> bool:
    return not a.empty and not b.empty and _lu(b, a) > 0


def overleft(a: Range, b: Range) -> bool:
    return not a.empty and not b.empty and _uu(a, b) <= 0


def overright(a: Range, b: Range) -> bool:
    return not a.empty and not b.empty and _ll(a, b) >= 0


def _touch(upper: Any, upper_inc: bool, lower: Any, lower_inc: bool) -> bool:
    return (
        upper is not None
        and lower is not None
        and _compare_values(upper, lower) == 0
        and upper_inc != lower_inc
    )


def adjacent(a: Range, b: Range) -> bool:
    if a.empty or b.empty:
        return False
    return _touch(a.upper, a.upper_inc, b.lower, b.lower_inc) or _touch(
        b.upper, b.upper_inc, a.lower, a.lower_inc
    )


def _bounds(a: Range, lower_from: Range, upper_from: Range) -> Range:
    return make_range(
        a.rtype, lower_from.lower, upper_from.upper, lower_from.lower_inc, upper_from.upper_inc
    )


def merge(a: Range, b: Range) -> Range:
    """range_merge: the smallest range holding both, whatever lies between them."""
    if a.empty:
        return b
    if b.empty:
        return a
    return _bounds(a, a if _ll(a, b) <= 0 else b, a if _uu(a, b) >= 0 else b)


def union(a: Range, b: Range) -> Range:
    if not a.empty and not b.empty and not overlaps(a, b) and not adjacent(a, b):
        raise PostgresError("22000", "result of range union would not be contiguous")
    return merge(a, b)


def intersect(a: Range, b: Range) -> Range:
    if not overlaps(a, b):
        return _empty(a.rtype)
    return _bounds(a, a if _ll(a, b) >= 0 else b, a if _uu(a, b) <= 0 else b)


def minus(a: Range, b: Range) -> Range:
    """range_minus_internal."""
    if a.empty or b.empty:
        return a
    cmp_l1l2, cmp_u1u2 = _ll(a, b), _uu(a, b)
    cmp_l1u2, cmp_l2u1 = _lu(a, b), _lu(b, a)
    if cmp_l1l2 < 0 and cmp_u1u2 > 0:
        raise PostgresError("22000", "result of range difference would not be contiguous")
    if cmp_l1u2 > 0 or cmp_l2u1 > 0:
        return a
    if cmp_l1l2 >= 0 and cmp_u1u2 <= 0:
        return _empty(a.rtype)
    if cmp_l1l2 <= 0 and cmp_l2u1 <= 0 and cmp_u1u2 <= 0:
        return make_range(a.rtype, a.lower, b.lower, a.lower_inc, not b.lower_inc)
    return make_range(a.rtype, b.upper, a.upper, not b.upper_inc, a.upper_inc)


# --- multiranges ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Multirange:
    """A multirange value: its ranges in order, each apart from the next, none empty."""

    rtype: RangeType
    ranges: tuple[Range, ...] = ()


def make_multirange(rtype: RangeType, ranges: Any) -> Multirange:
    """multirange_canonicalize: empties gone, in order, each touching pair merged."""
    ordered = sorted((r for r in ranges if not r.empty), key=functools.cmp_to_key(range_cmp))
    out: list[Range] = []
    for item in ordered:
        if out and (overlaps(out[-1], item) or adjacent(out[-1], item)):
            out[-1] = merge(out[-1], item)
        else:
            out.append(item)
    return Multirange(rtype, tuple(out))


def parse_multirange(rtype: RangeType, text: str) -> Multirange:
    """multirange_in."""
    stripped = text.strip(_SPACE)
    if stripped[:1] != "{":
        raise _malformed("multirange", text, "Missing left brace.")
    if stripped[-1:] != "}":
        raise _malformed("multirange", text, "Junk after closing right brace.")
    inner = stripped[1:-1].strip(_SPACE)
    if not inner:
        return Multirange(rtype)
    ranges: list[Range] = []
    at = 0
    while True:
        start = at
        quoted = False
        while at < len(inner) and (quoted or inner[at] not in ")]"):
            if inner[at] == "\\":
                at += 1
            elif inner[at] == '"':
                quoted = not quoted
            at += 1
        if at >= len(inner):
            word_end = inner.find(",", start)
            word = inner[start : word_end if word_end >= 0 else len(inner)].strip(_SPACE)
            if word.lower() != "empty":
                raise _malformed("multirange", text, "Expected range start.")
            ranges.append(_empty(rtype))
            at = word_end if word_end >= 0 else len(inner)
        else:
            comma = inner.find(",", start)
            if inner[start:].lstrip(_SPACE)[:5].lower() == "empty" and 0 <= comma < at:
                ranges.append(_empty(rtype))
                at = comma
            else:
                at += 1
                ranges.append(parse_range(rtype, inner[start:at]))
        rest = inner[at:].lstrip(_SPACE)
        if not rest:
            break
        if rest[0] != ",":
            raise _malformed("multirange", text, "Expected comma or end of multirange.")
        at = len(inner) - len(rest) + 1
    return make_multirange(rtype, ranges)


def multirange_text(value: Multirange) -> str:
    """multirange_out."""
    return "{" + ",".join(range_text(r) for r in value.ranges) + "}"


def whole(value: Multirange) -> Range:
    """range_merge of a multirange: its first lower bound to its last upper."""
    if not value.ranges:
        return _empty(value.rtype)
    return _bounds(value.ranges[0], value.ranges[0], value.ranges[-1])


def mr_union(a: Multirange, b: Multirange) -> Multirange:
    return make_multirange(a.rtype, a.ranges + b.ranges)


def mr_intersect(a: Multirange, b: Multirange) -> Multirange:
    pieces = [intersect(x, y) for x in a.ranges for y in b.ranges]
    return make_multirange(a.rtype, pieces)


def _subtract(item: Range, taken: Range) -> list[Range]:
    """``item`` without ``taken``: a difference may be two pieces here, unlike a range's."""
    if not overlaps(item, taken):
        return [item]
    pieces: list[Range] = []
    if _ll(item, taken) < 0:
        pieces.append(
            make_range(item.rtype, item.lower, taken.lower, item.lower_inc, not taken.lower_inc)
        )
    if _uu(item, taken) > 0:
        pieces.append(
            make_range(item.rtype, taken.upper, item.upper, not taken.upper_inc, item.upper_inc)
        )
    return pieces


def mr_minus(a: Multirange, b: Multirange) -> Multirange:
    held = list(a.ranges)
    for taken in b.ranges:
        held = [piece for item in held for piece in _subtract(item, taken)]
    return make_multirange(a.rtype, held)


def mr_cmp(a: Multirange, b: Multirange) -> int:
    """multirange_cmp: range by range, then the shorter first."""
    for x, y in zip(a.ranges, b.ranges):
        order = range_cmp(x, y)
        if order:
            return order
    return (len(a.ranges) > len(b.ranges)) - (len(a.ranges) < len(b.ranges))


__all__ = [
    "Infinite",
    "MULTIRANGE_TYPES",
    "Multirange",
    "RANGE_TYPES",
    "Range",
    "RangeType",
    "adjacent",
    "before",
    "contains",
    "contains_element",
    "element_text",
    "intersect",
    "make_multirange",
    "make_range",
    "merge",
    "minus",
    "mr_cmp",
    "mr_intersect",
    "mr_minus",
    "mr_union",
    "multirange_text",
    "overlaps",
    "overleft",
    "overright",
    "parse_multirange",
    "parse_range",
    "range_cmp",
    "range_text",
    "read_element",
    "union",
    "whole",
]
