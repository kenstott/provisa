# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6f2a91-3b58-4c7e-8e14-9a2b5c7d1f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's former Python writers (``oracle.py``) with PostgreSQL 17's answer where theirs
was not it: the reference the wired path is held to. ``pg17()`` replaces, for its duration, each
writer or coercer listed in ``DEFECTS`` with one that answers as PostgreSQL does; everything else
is the former writers' own code. ``rust/pgwire/scripts/pg_types_check.py`` holds these answers to
a live server, and ``test_differential.py`` shows each defect on a witness value (the former
writers and PostgreSQL differ there; the wired path answers as PostgreSQL does).
"""

from __future__ import annotations

import datetime
import decimal
import ipaddress
import math
import struct
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from decimal import Decimal
from typing import Any

from buenavista.core import BVType

import provisa.pgwire.cast_types as ct
import provisa.pgwire.server as srv
from tests.unit.pgwire_encoder import oracle as bp
from tests.unit.pgwire_encoder import python_ref
from provisa.core.ir_types import infinite_date, infinite_timestamp

DAY_US = 86_400_000_000

#: Each product defect the reference corrects: its key, and what PostgreSQL 17 does instead.
DEFECTS = {
    "numeric_text": "numeric text is numeric_out's plain digits (Decimal.__str__ writes 1E-7, -0.00)",
    "numeric_binary": "numeric binary keeps every digit, NaN and +/-Infinity (abs() under the default "
    "context rounds past 28 digits; Infinity raises)",
    "empty_array": "an array with no elements has no dimensions ({{},{}} is {}; binary ndim 0)",
    "array_element_list": "a sub-array where an element belongs is malformed (the product writes "
    "str(list) as the element)",
    "vector_lower_bound": "oidvector and int2vector count from 0 (array_send writes lower bound 0)",
    "inet_text": "an IPv6 address holding an IPv4 one is written in dotted quad (::1.2.3.4, "
    "::ffff:1.2.3.4), in inet, cidr and text alike (the product wrote ipaddress's text)",
    "duckdb_inet": "DuckDB's INET reaches Arrow with its address a decimal128; _duckdb_inet raises",
    "range_bounds": "a range bound may be infinity, BC or past year 9999 (fromisoformat raises)",
    "tsquery_binary": "tsquery has a binary form, tsquerysend of the parsed query (the product "
    "refuses binary)",
    "money_range": "money past int8 cents is refused in text too",
    "interval_days": "a duration's days are its microseconds over a day, truncated exactly (the "
    "product divides in floating point)",
    "coerced_infinity": "an infinite date or timestamp keeps its infinity through the date, "
    "timestamp and no-zone coercers",
    "uuid_bytes": "a uuid column of 16 plain bytes (fixed_size_binary(16) without arrow.uuid) is "
    "the UUID they hold (the product writes str(bytes), and refuses binary)",
}


# --- numeric ---


def numeric_text(v: Any) -> str:
    if not isinstance(v, Decimal):
        return str(v)
    if v.is_nan():
        return "NaN"
    if v.is_infinite():
        return "Infinity" if v > 0 else "-Infinity"
    return format(v.copy_abs() if v.is_zero() else v, "f")


_ORIGINAL_NUMERIC_BINARY = bp._numeric_to_pg_binary


def numeric_binary(v: Any) -> bytes:
    d = v if isinstance(v, Decimal) else Decimal(str(v))
    if d.is_nan():
        return struct.pack("!HhHH", 0, 0, 0xC000, 0)
    if d.is_infinite():
        # dscale 32: numeric_send reads it through the short header's bits (0xD000 & 0x1F80)
        return struct.pack("!HhHH", 0, 0, 0xF000 if d < 0 else 0xD000, 32)
    exact = decimal.Context(prec=decimal.MAX_PREC, Emax=decimal.MAX_EMAX, Emin=decimal.MIN_EMIN)
    with decimal.localcontext(exact):
        return _ORIGINAL_NUMERIC_BINARY(d)


# --- arrays ---


def _empty_nested(values: Any) -> bool:
    return isinstance(values, list) and all(
        isinstance(x, list) and _empty_nested(x) for x in values
    )


def array_entry(
    element: Any,
    array_oid: int,
    element_text: Callable[[Any], str] | None = None,
    element_oid: int | None = None,
    *,
    vector: bool = False,
) -> tuple[int, Callable[[Any], str], Callable[[Any], bytes]]:
    """buenavista's _array_entry, its element writers read now (so the reference's own scalar
    writers are the elements'), with PostgreSQL's empty arrays, malformed elements and vectors."""
    elem_oid, elem_text, elem_binary = bp.BVTYPE_TO_PGTYPE[element]
    elem_oid = elem_oid if element_oid is None else element_oid
    text_of = element_text or elem_text

    def items(values: Any) -> str:
        written = (
            "NULL"
            if v is None
            else items(v)
            if isinstance(v, list)
            else bp._array_text_element(text_of(v))
            for v in values
        )
        return "{" + ",".join(written) + "}"

    def text(values: Any) -> str:
        return "{}" if _empty_nested(values) else items(values)

    def binary(values: Any) -> bytes:
        dimensions, flat = bp._array_shape(values)
        if not vector and dimensions and math.prod(dimensions) == 0:
            dimensions = []
        has_null = any(v is None for v in flat)
        out = [struct.pack("!iii", len(dimensions), 1 if has_null else 0, elem_oid)]
        out.extend(struct.pack("!ii", length, 0 if vector else 1) for length in dimensions)
        for v in flat:
            if v is None:
                out.append(struct.pack("!i", -1))
            elif isinstance(v, list):
                raise ValueError("malformed array literal: a sub-array as an element")
            else:
                encoded = elem_binary(v)
                out.append(struct.pack("!i", len(encoded)) + encoded)
        return b"".join(out)

    return (array_oid, text, binary)


def _vector_entry(element: Any, vector_oid: int) -> tuple[int, Any, Any]:
    array_binary = array_entry(element, vector_oid, vector=True)[2]

    def binary(values: Any) -> bytes:
        return array_binary([int(v) for v in values.split()] if isinstance(values, str) else values)

    text = bp.BVTYPE_TO_PGTYPE[BVType.OIDVECTOR][1]
    return (vector_oid, text, binary)


def _arrays() -> dict[Any, Any]:
    t = BVType
    return {
        t.INTEGERARRAY: array_entry(t.INTEGER, 1007),
        t.BIGINTARRAY: array_entry(t.BIGINT, 1016),
        t.SMALLINTARRAY: array_entry(t.SMALLINT, 1005),
        t.FLOATARRAY: array_entry(t.FLOAT, 1022),
        t.REALARRAY: array_entry(t.REAL, 1021),
        t.DECIMALARRAY: array_entry(t.DECIMAL, 1231),
        t.BOOLARRAY: array_entry(t.BOOL, 1000, lambda v: "t" if v else "f"),
        t.STRINGARRAY: array_entry(t.TEXT, 1009),
        t.OIDARRAY: array_entry(t.OID, 1028),
        t.MONEYARRAY: array_entry(t.MONEY, 791),
        t.INTERVALARRAY: array_entry(t.INTERVAL, 1187),
        t.CHARARRAY: array_entry(t.TEXT, 1002, element_oid=18),
        t.NAMEARRAY: array_entry(t.TEXT, 1003, element_oid=19),
        t.REGTYPEARRAY: array_entry(t.REGTYPE, 2211),
        ct.XML_ARRAY: array_entry(t.XML, 143),
        t.ACLITEMARRAY: (1034, array_entry(t.TEXT, 1034)[1], bp._no_binary_aclitem),
        t.OIDVECTOR: _vector_entry(t.OID, 30),
        t.INT2VECTOR: _vector_entry(t.SMALLINT, 22),
    }


# --- addresses ---

_INTERFACES = (ipaddress.IPv4Interface, ipaddress.IPv6Interface)


def pg_address(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    """inet_net_ntop: lowercase hextets without leading zeros, the longest run of two or more zero
    words (the first of equal runs) as ::, and an IPv6 address whose first 80 bits are zero and
    next 16 zero or ffff ending in dotted quad. Written from the address's bytes, not by
    ipaddress, whose text of an IPv4-mapped address changes between patch releases."""
    if ip.version == 4:
        return ".".join(str(b) for b in ip.packed)
    words = [int.from_bytes(ip.packed[k : k + 2], "big") for k in range(0, 16, 2)]
    best_base, best_len, cur_base, cur_len = -1, 0, -1, 0
    for k, w in enumerate(words + [1]):
        if w == 0 and k < 8:
            cur_base, cur_len = (k, 1) if cur_base == -1 else (cur_base, cur_len + 1)
        else:
            if cur_base != -1 and (best_base == -1 or cur_len > best_len):
                best_base, best_len = cur_base, cur_len
            cur_base, cur_len = -1, 0
    if best_len < 2:
        best_base, best_len = -1, 0
    if best_base == 0 and (best_len == 6 or (best_len == 5 and words[5] == 0xFFFF)):
        head = "::" if best_len == 6 else "::ffff:"
        return head + ".".join(str(b) for b in ip.packed[12:])
    if best_base == -1:
        return ":".join(f"{w:x}" for w in words)
    left = ":".join(f"{w:x}" for w in words[:best_base])
    right = ":".join(f"{w:x}" for w in words[best_base + best_len :])
    return f"{left}::{right}"


def inet_text(v: Any) -> str:
    if isinstance(v, _INTERFACES):
        address = pg_address(v.ip)
        full = v.network.prefixlen == v.max_prefixlen
        return address if full else f"{address}/{v.network.prefixlen}"
    if isinstance(v, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return pg_address(v)  # a bare address (psycopg's inet of a host) is inet_out's too
    return str(v)


def text_text(v: Any) -> str:
    """text: an interface (the value _duckdb_inet makes) as PostgreSQL's text(inet), its address
    then its prefix; anything else as str()."""
    return cidr_text(v) if isinstance(v, _INTERFACES) else str(v)


def cidr_text(v: Any) -> str:
    if isinstance(v, _INTERFACES):
        return f"{pg_address(v.ip)}/{v.network.prefixlen}"
    return str(v)


_ORIGINAL_DUCKDB_INET = srv._duckdb_inet


def duckdb_inet(value: Any) -> Any:
    if isinstance(value, dict) and isinstance(value.get("address"), Decimal):
        value = {**value, "address": int(value["address"])}
    return _ORIGINAL_DUCKDB_INET(value)


# --- ranges ---


def _days_from_civil(y: int, m: int, d: int) -> int:
    y -= m <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146_097 + doe - 719_468


def _pg_stamp(text: str) -> tuple[int, int, bool, int]:
    """(days since 2000-01-01, microseconds since 2000-01-01 UTC, has a time, infinity) of a date
    or timestamp as PostgreSQL writes it: ISO, a BC suffix, a year past 9999, or infinity."""
    if text in ("infinity", "-infinity"):
        return 0, 0, False, 1 if text == "infinity" else -1
    bc = text.endswith(" BC")
    body = text[:-3] if bc else text
    date, _, clock = body.partition(" ")
    year_text, month, day = date.rsplit("-", 2)
    year = 1 - int(year_text) if bc else int(year_text)
    days = _days_from_civil(year, int(month), int(day)) - 10957
    if not clock:
        return days, days * DAY_US, False, 0
    offset = 0
    for sign in "+-":
        if sign in clock:
            clock, _, zone = clock.partition(sign)
            parts = [int(p) for p in zone.split(":")]
            offset = (parts[0] * 3600 + (parts[1] if len(parts) > 1 else 0) * 60) + (
                parts[2] if len(parts) > 2 else 0
            )
            offset = -offset if sign == "-" else offset
            break
    hms, _, frac = clock.partition(".")
    h, m, s = (int(x) for x in hms.split(":"))
    micros = int((frac + "000000")[:6]) if frac else 0
    total = days * DAY_US + ((h * 60 + m) * 60 + s) * 1_000_000 + micros - offset * 1_000_000
    return days, total, True, 0


def _date_bound(b: str) -> bytes:
    try:
        value = datetime.date.fromisoformat(b)
    except ValueError:
        days, _, has_time, inf = _pg_stamp(b)
        if has_time:
            raise
        return struct.pack("!i", 0x7FFFFFFF if inf > 0 else -0x80000000 if inf else days)
    return bp._date_binary(value)


def _stamp_bound(aware: bool) -> Callable[[str], bytes]:
    micros_of = bp._micros_since_2000_utc if aware else bp._micros_since_2000

    def bound(b: str) -> bytes:
        try:
            value = datetime.datetime.fromisoformat(b)
        except ValueError:
            _, micros, _, inf = _pg_stamp(b)
            if inf:
                return struct.pack("!q", 0x7FFFFFFFFFFFFFFF if inf > 0 else -0x8000000000000000)
            if aware and not b.endswith(" BC") and "+" not in b[10:] and "-" not in b[10:]:
                raise
            return struct.pack("!q", micros)
        return bp._timestamp_binary(micros_of)(value)

    return bound


def _ranges() -> dict[Any, Any]:
    t = BVType
    ranges = {
        t.INT4RANGE: (3904, str, bp._range_binary(lambda b: struct.pack("!i", int(b)))),
        t.INT8RANGE: (3926, str, bp._range_binary(lambda b: struct.pack("!q", int(b)))),
        t.NUMRANGE: (3906, str, bp._range_binary(numeric_binary)),
        t.TSRANGE: (3908, str, bp._range_binary(_stamp_bound(False))),
        t.TSTZRANGE: (3910, str, bp._range_binary(_stamp_bound(True))),
        t.DATERANGE: (3912, str, bp._range_binary(_date_bound)),
    }
    for rng, multi, oid in (
        (t.INT4RANGE, t.INT4MULTIRANGE, 4451),
        (t.INT8RANGE, t.INT8MULTIRANGE, 4536),
        (t.NUMRANGE, t.NUMMULTIRANGE, 4532),
        (t.TSRANGE, t.TSMULTIRANGE, 4533),
        (t.TSTZRANGE, t.TSTZMULTIRANGE, 4534),
        (t.DATERANGE, t.DATEMULTIRANGE, 4535),
    ):
        ranges[multi] = (oid, str, bp._multirange_binary(ranges[rng][2]))
    return ranges


# --- money, intervals, coercers ---

_ORIGINAL_MONEY_TEXT = bp._money_text


def money_text(v: Any) -> str:
    if not isinstance(v, str) and not -(2**63) <= bp._money_cents(v) < 2**63:
        raise ValueError("money out of range")
    return _ORIGINAL_MONEY_TEXT(v)


_ORIGINAL_INTERVAL_PARTS = bp._interval_parts


def interval_parts(v: Any) -> tuple[int, int, int]:
    if getattr(v, "pg_parts", None) is None and isinstance(v, datetime.timedelta):
        micros = v // datetime.timedelta(microseconds=1)
        days = abs(micros) // DAY_US * (1 if micros >= 0 else -1)
        return 0, days, micros - days * DAY_US
    return _ORIGINAL_INTERVAL_PARTS(v)


def _infinity(v: Any) -> int:
    return getattr(v, "pg_infinity", 0)


def to_date(v: Any) -> Any:
    return infinite_date(_infinity(v)) if _infinity(v) else srv._to_date(v)


def to_timestamp(v: Any) -> Any:
    inf = _infinity(v)
    return infinite_timestamp(inf, aware=False) if inf else srv._to_timestamp(v)


def of_no_zone(v: Any) -> Any:
    inf = _infinity(v)
    return infinite_timestamp(inf, aware=False) if inf else ct._of_no_zone(v)


#: The product's coercer -> the reference's.
COERCERS: dict[Any, Callable[[Any], Any]] = {
    srv._to_date: to_date,
    srv._to_timestamp: to_timestamp,
    ct._of_no_zone: of_no_zone,
    srv._duckdb_inet: duckdb_inet,
}


def _setattr(stack: ExitStack, obj: Any, name: str, value: Any) -> None:
    original = getattr(obj, name)
    setattr(obj, name, value)
    stack.callback(setattr, obj, name, original)


@contextmanager
def pg17() -> Iterator[None]:
    """Within: the product's encode path with each of DEFECTS corrected to PostgreSQL's answer."""
    table = bp.BVTYPE_TO_PGTYPE
    saved = dict(table)
    with ExitStack() as stack:
        stack.callback(lambda: (table.clear(), table.update(saved)))
        _setattr(stack, bp, "_interval_parts", interval_parts)
        _setattr(stack, bp, "_money_text", money_text)
        _setattr(stack, bp, "_numeric_to_pg_binary", numeric_binary)
        t = BVType
        table[t.DECIMAL] = (1700, numeric_text, numeric_binary)
        table[t.MONEY] = (790, money_text, table[t.MONEY][2])
        table[t.INET] = (869, inet_text, table[t.INET][2])
        table[t.CIDR] = (650, cidr_text, table[t.CIDR][2])
        table[t.TEXT] = (25, text_text, lambda v: text_text(v).encode("utf-8"))
        table.update(_ranges())
        table.update(_arrays())

        def coerced(qr: Any, row: Any) -> list:
            out = list(row)
            for i, coercer in qr._coerce:
                if out[i] is not None:
                    out[i] = COERCERS.get(coercer, coercer)(out[i])
            return out

        _setattr(stack, python_ref, "COERCE", coerced)
        yield
