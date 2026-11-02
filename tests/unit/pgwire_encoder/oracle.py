# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's Python DataRow writers as they stood before REQ-2194 wired the Rust encoder in:
buenavista's ``BVTYPE_TO_PGTYPE`` converters and ``send_data_rows`` loop, and the reg* writers
``provisa.pgwire.server`` registered. The product no longer has them; they are kept here, frozen,
only as the reference the differential test (``test_differential.py``) holds the wired path to,
with ``pg_reference.pg17()`` correcting each to PostgreSQL 17's answer where it was not.
"""

# ruff: noqa
# fmt: off
import datetime
import decimal
import ipaddress
import json
import math
import re
import struct
import uuid as _uuid_mod
from buenavista.core import BVType, RawDataRowBytes

_INT32_STRUCT = struct.Struct("!i")

_PG_EPOCH = datetime.datetime(2000, 1, 1)
_PG_EPOCH_UTC = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
_PG_DATE_EPOCH = datetime.date(2000, 1, 1)

def _interval_parts(v):
    """(months, days, microseconds) of an interval value: a timedelta, or a relativedelta when it
    has months (a year-month interval). A value marked with its ``pg_parts`` is one neither keeps
    the fields of, 26:01:00 apart from 1 day 02:01:00."""
    exact = getattr(v, "pg_parts", None)
    if exact is not None:
        return exact
    if isinstance(v, datetime.timedelta):
        micros = v // datetime.timedelta(microseconds=1)
        # Integer division truncated toward zero: days and time share the value's sign.
        days, rest = divmod(abs(micros), 86_400_000_000)
        sign = -1 if micros < 0 else 1
        return 0, sign * days, sign * rest
    seconds = (v.hours * 60 + v.minutes) * 60 + v.seconds
    return v.years * 12 + v.months, v.days, seconds * 1_000_000 + v.microseconds


def _interval_text(v) -> str:
    """An interval as Postgres writes it under IntervalStyle postgres (1 year 2 mons 3 days
    04:05:06.5), the text a client's interval parser reads."""
    months, days, micros = _interval_parts(v)
    years, months = int(months / 12), months - int(months / 12) * 12
    parts = []
    # EncodeInterval: a field after a negative one says its own sign.
    after_negative = False
    for n, unit, plural in ((years, "year", "s"), (months, "mon", "s"), (days, "day", "s")):
        if n:
            sign = "+" if after_negative and n > 0 else ""
            parts.append(f"{sign}{n} {unit}{'' if n == 1 else plural}")
            after_negative = n < 0
    if micros or not parts:
        hours, rem = divmod(abs(micros), 3_600_000_000)
        minutes, rem = divmod(rem, 60_000_000)
        secs, frac = divmod(rem, 1_000_000)
        sign = "-" if micros < 0 else "+" if after_negative else ""
        clock = f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}"
        parts.append(clock + (f".{frac:06d}".rstrip("0") if frac else ""))
    return " ".join(parts)


def _time_to_microseconds(t):
    hours_to_microseconds = t.hour * 60 * 60 * 1e6
    minutes_to_microseconds = t.minute * 60 * 1e6
    seconds_to_microseconds = t.second * 1e6
    microseconds = t.microsecond
    total_microseconds = (
        hours_to_microseconds + minutes_to_microseconds + seconds_to_microseconds + microseconds
    )
    return int(total_microseconds)


def _infinity(v) -> int:
    """1 or -1 for a value standing for PostgreSQL's ``infinity`` or ``-infinity`` date or
    timestamp (its ``pg_infinity``, the nearest value Python holds marked with its sign), else 0."""
    return getattr(v, "pg_infinity", 0)


def _infinity_text(v) -> str:
    return "infinity" if _infinity(v) > 0 else "-infinity"


def _date_text(v) -> str:
    return _infinity_text(v) if _infinity(v) else v.isoformat()


def _date_binary(r) -> bytes:
    # date binary: int32 days since 2000-01-01; infinity is the int32 range's ends.
    if _infinity(r):
        return struct.pack("!i", 0x7FFFFFFF if _infinity(r) > 0 else -0x80000000)
    return int.to_bytes((r.toordinal() - 730120), 4, "big", signed=True)


def _pg_offset(offset) -> str:
    """A UTC offset as PostgreSQL's ISO output writes it: +HH, with :MM and :SS only when not
    zero (+00, +05:30, -03)."""
    seconds = int(offset.total_seconds())
    sign = "-" if seconds < 0 else "+"
    hours, rest = divmod(abs(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{sign}{hours:02d}"
    if minutes or secs:
        text += f":{minutes:02d}"
    if secs:
        text += f":{secs:02d}"
    return text


def _pg_clock(v) -> str:
    """A time of day as PostgreSQL writes it: HH:MM:SS, then the fraction of a second with its
    trailing zeros dropped, and the UTC offset when the value has one. A value read as its text
    is that text (REQ-2171)."""
    if isinstance(v, str):
        return v
    text = f"{v.hour:02d}:{v.minute:02d}:{v.second:02d}"
    if v.microsecond:
        text += f".{v.microsecond:06d}".rstrip("0")
    offset = v.utcoffset()
    return text if offset is None else text + _pg_offset(offset)


def _timestamp_text(v) -> str:
    """timestamp_out / timestamptz_out in the ISO date style."""
    if _infinity(v):
        return _infinity_text(v)
    return f"{v.year:04d}-{v.month:02d}-{v.day:02d} {_pg_clock(v)}"


def _timestamp_binary(micros):
    """timestamp/timestamptz binary: int64 microseconds since 2000-01-01 (``micros`` reads them);
    infinity is the int64 range's ends."""

    def encode(r) -> bytes:
        if _infinity(r):
            return struct.pack(
                "!q", 0x7FFFFFFFFFFFFFFF if _infinity(r) > 0 else -0x8000000000000000
            )
        return int.to_bytes(micros(r), 8, "big", signed=True)

    return encode


def _micros_since_2000_utc(dt):
    """timestamptz binary: microseconds since 2000-01-01 UTC. A value with no time zone is not a
    timestamptz — refused rather than assumed to be UTC."""
    if dt.tzinfo is None:
        raise ValueError(f"timestamptz value without a time zone: {dt!r}")
    return _micros_since_2000(dt)


_FULL_OFFSET = re.compile(r"[+-]\d\d:\d\d$")


def _timetz_to_pg_binary(t):
    """timetz binary: int64 microseconds since midnight + int32 zone offset in seconds WEST of UTC."""
    if isinstance(t, str):  # read as its PostgreSQL text (REQ-2171)
        t = datetime.time.fromisoformat(t if _FULL_OFFSET.search(t) else t + ":00")
    offset = t.utcoffset()
    if offset is None:
        raise ValueError(f"timetz value without a time zone: {t!r}")
    return struct.pack("!qi", _time_to_microseconds(t), -int(offset.total_seconds()))


def _uuid_to_pg_binary(u):
    return (u if isinstance(u, _uuid_mod.UUID) else _uuid_mod.UUID(str(u))).bytes


def _inet_text(v):
    """inet_out: an address with its full prefix is written without it."""
    if isinstance(v, (ipaddress.IPv4Interface, ipaddress.IPv6Interface)):
        return str(v.ip) if v.network.prefixlen == v.max_prefixlen else str(v)
    return str(v)


def _address_binary(is_cidr):
    """inet_send / cidr_send: family (2 IPv4, 3 IPv6), prefix bits, is-cidr flag, address length,
    then the address."""

    def encode(v):
        if is_cidr:
            net = ipaddress.ip_network(str(v))
            address, bits = net.network_address.packed, net.prefixlen
        else:
            iface = ipaddress.ip_interface(str(v))
            address, bits = iface.ip.packed, iface.network.prefixlen
        family = 2 if len(address) == 4 else 3
        return bytes([family, bits, 1 if is_cidr else 0, len(address)]) + address

    return encode


def _macaddr_binary(v):
    raw = bytes.fromhex(re.sub(r"[:\-.]", "", str(v)))
    if len(raw) != 6:
        raise ValueError(f"not a macaddr: {v!r}")
    return raw


def _macaddr8_binary(v):
    """macaddr8_send: 8 bytes; a 6-byte address is widened as PostgreSQL does (FF:FE inserted)."""
    raw = bytes.fromhex(re.sub(r"[:\-.]", "", str(v)))
    if len(raw) == 6:
        raw = raw[:3] + b"\xff\xfe" + raw[3:]
    if len(raw) != 8:
        raise ValueError(f"not a macaddr8: {v!r}")
    return raw


_FLOAT8 = re.compile(r"[-+]?(?:Infinity|NaN|inf|(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)")


def _floats(v, count=None):
    """The float8s of a shape's text form, in order."""
    values = [float(x) for x in _FLOAT8.findall(str(v))]
    if count is not None and len(values) != count:
        raise ValueError(f"expected {count} numbers in {v!r}")
    return values


def _shape_binary(count):
    """line_send, lseg_send, box_send, circle_send: their float8s in text order."""
    return lambda r: struct.pack(f"!{count}d", *_floats(r, count))


def _path_binary(r):
    """path_send: closed flag (a path written in parentheses is closed), point count, points."""
    values = _floats(r)
    closed = 1 if str(r).strip().startswith("(") else 0
    return struct.pack(f"!bi{len(values)}d", closed, len(values) // 2, *values)


def _polygon_binary(r):
    values = _floats(r)
    return struct.pack(f"!i{len(values)}d", len(values) // 2, *values)


def _bit_binary(r):
    """bit_send / varbit_send: bit count, then the bits packed high-first, the last byte padded."""
    bits = str(r)
    if bits.strip("01"):
        raise ValueError(f"not a bit string: {r!r}")
    padded = bits + "0" * (-len(bits) % 8)
    return struct.pack("!i", len(bits)) + int(padded or "0", 2).to_bytes(len(padded) // 8, "big")


def _range_bounds(text):
    """``(lower, upper, flags)`` of a range's text form; each bound None when unbounded."""
    text = text.strip()
    if text.lower() == "empty":
        return None, None, 0x01
    flags = (0x02 if text[0] == "[" else 0) | (0x04 if text[-1] == "]" else 0)
    bounds, current, quoted, i = [], "", False, 0
    inner = text[1:-1]
    while i < len(inner):
        ch = inner[i]
        if ch == "\\" and i + 1 < len(inner):
            current += inner[i + 1]
            i += 2
            continue
        if ch == '"':
            quoted = not quoted
        elif ch == "," and not quoted:
            bounds.append(current)
            current = ""
        else:
            current += ch
        i += 1
    bounds.append(current)
    if len(bounds) != 2:
        raise ValueError(f"not a range: {text!r}")
    lower, upper = (b.strip() or None for b in bounds)
    flags |= (0x08 if lower is None else 0) | (0x10 if upper is None else 0)
    return lower, upper, flags & ~(0x02 if lower is None else 0) & ~(0x04 if upper is None else 0)


def _range_binary(bound):
    """range_send: flags, then each bounded bound length-prefixed in its element's binary form."""

    def encode(r):
        lower, upper, flags = _range_bounds(str(r))
        out = [bytes([flags])]
        for value in (lower, upper):
            if value is not None:
                encoded = bound(value)
                out.append(struct.pack("!i", len(encoded)) + encoded)
        return b"".join(out)

    return encode


def _multirange_ranges(text):
    """Each range's text of a multirange's text form."""
    inner, ranges, start, quoted = text.strip()[1:-1], [], None, False
    for at, ch in enumerate(inner):
        if ch == '"':
            quoted = not quoted
        elif quoted:
            continue
        elif ch in "[(" and start is None:
            start = at
        elif ch in "])":
            ranges.append(inner[start : at + 1])
            start = None
    return ranges


def _multirange_binary(range_binary):
    """multirange_send: the range count, then each range length-prefixed in its binary form."""

    def encode(r):
        encoded = [range_binary(item) for item in _multirange_ranges(str(r))]
        return struct.pack("!i", len(encoded)) + b"".join(
            struct.pack("!i", len(e)) + e for e in encoded
        )

    return encode


_TSVECTOR_WEIGHTS = {"A": 3, "B": 2, "C": 1, "D": 0}


def _tsvector_binary(r):
    """tsvectorsend: lexeme count; each lexeme NUL-terminated, its position count, then each
    position with its weight in the top two bits."""
    text, i, lexemes = str(r), 0, []
    while i < len(text):
        if text[i] == " ":
            i += 1
            continue
        if text[i] != "'":
            raise ValueError(f"not a tsvector: {r!r}")
        i += 1
        lexeme = ""
        while True:
            if text[i] == "\\":
                lexeme += text[i + 1]
                i += 2
            elif text[i] == "'" and text[i + 1 : i + 2] == "'":
                lexeme += "'"
                i += 2
            elif text[i] == "'":
                i += 1
                break
            else:
                lexeme += text[i]
                i += 1
        positions = []
        if text[i : i + 1] == ":":
            match = re.match(r":((?:\d+[A-D]?,?)+)", text[i:])
            if match is None:
                raise ValueError(f"not a tsvector: {r!r}")
            for item in match.group(1).rstrip(",").split(","):
                weight = _TSVECTOR_WEIGHTS[item[-1]] if item[-1] in _TSVECTOR_WEIGHTS else 0
                positions.append((weight << 14) | int(item.rstrip("ABCD")))
            i += match.end()
        lexemes.append(
            lexeme.encode("utf-8")
            + b"\x00"
            + struct.pack(f"!H{len(positions)}H", len(positions), *positions)
        )
    return struct.pack("!i", len(lexemes)) + b"".join(lexemes)


def _text_only(name):
    """A type held as its text whose binary form that text does not give: its value is built by
    PostgreSQL itself. Listed as a difference in docs/conformance/pgwire.md."""

    def binary(_value):
        raise ValueError(f"{name} is sent in text format only")

    return binary


def _text_bytes(r):
    return str(r).encode("utf-8")


def _unsigned_binary(width):
    """xid, cid, xid8: an unsigned integer, whose text form is its digits."""
    return lambda r: int(r).to_bytes(width, "big", signed=False)


def _tid_binary(r):
    """tidsend: the block number and offset of ``(block,offset)``."""
    block, offset = str(r).strip("()").split(",")
    return struct.pack("!IH", int(block), int(offset))


def _snapshot_binary(r):
    """txid_snapshot_send / pg_snapshot_send: the in-progress count, xmin, xmax and each xid of
    ``xmin:xmax:xip,xip``."""
    xmin, xmax, xip = str(r).split(":")
    running = [int(x) for x in xip.split(",") if x]
    return struct.pack(f"!iqq{len(running)}q", len(running), int(xmin), int(xmax), *running)


def _jsonpath_binary(r):
    """jsonpath_send: a version byte, then its text."""
    return b"\x01" + _text_bytes(r)


def _wkb_not_sent(r):
    """PostGIS's binary form is EWKB; Provisa holds geometry and geography as WKT and sends them
    as text only (docs/conformance/pgwire.md)."""
    raise ValueError("geometry and geography are sent in text format only")


def _shortest_text(sig: str, negative: bool, fixed_below: int) -> str:
    """float8out / float4out for a finite value, from its shortest round-trip form ``sig`` (a
    ``d.ddde[+-]x`` string): plain decimal for an exponent in [-4, ``fixed_below``), else
    ``d.ddde+XX``. PostgreSQL's limits are printf's: 15 for a float8, 6 for a float4."""
    mantissa, exponent = sig.split("e")
    digits = mantissa.replace(".", "").rstrip("0") or "0"
    exp = int(exponent)
    sign = "-" if negative else ""
    if digits == "0":
        return sign + "0"
    if exp < -4 or exp >= fixed_below:
        body = digits[0] + ("." + digits[1:] if len(digits) > 1 else "")
        return f"{sign}{body}e{'-' if exp < 0 else '+'}{abs(exp):02d}"
    if exp < 0:
        return sign + "0." + "0" * (-exp - 1) + digits
    if len(digits) <= exp + 1:
        return sign + digits + "0" * (exp + 1 - len(digits))
    return sign + digits[: exp + 1] + "." + digits[exp + 1 :]


def _non_finite_text(x: float):
    if x != x:
        return "NaN"
    if x in (float("inf"), float("-inf")):
        return "Infinity" if x > 0 else "-Infinity"
    return None


_FLOAT8_FIXED_BELOW = 15
_FLOAT4_FIXED_BELOW = 6


def _float8_text(x):
    """float8out: the shortest exact form (2, -1, NaN, Infinity), as PostgreSQL writes it."""
    x = float(x)
    special = _non_finite_text(x)
    if special is not None:
        return special
    sig = format(decimal.Decimal(repr(abs(x))), "e")
    return _shortest_text(sig, math.copysign(1.0, x) < 0, _FLOAT8_FIXED_BELOW)


def _float4_text(x):
    """float4out: the shortest form that reads back as the same 4-byte float."""
    x = float(x)
    special = _non_finite_text(x)
    if special is not None:
        return special
    wanted = struct.pack("!f", abs(x))
    negative = math.copysign(1.0, x) < 0
    for digits in range(1, 10):
        sig = f"{abs(x):.{digits - 1}e}"
        try:
            reads_back = struct.pack("!f", float(sig)) == wanted
        except OverflowError:
            # the shortest form of the largest float4 sits just past it in a double
            reads_back = digits == 8 and wanted == struct.pack("!f", 3.4028234663852886e38)
        if reads_back:
            return _shortest_text(sig, negative, _FLOAT4_FIXED_BELOW)
    return _shortest_text(f"{abs(x):.8e}", negative, _FLOAT4_FIXED_BELOW)


def _point_parts(v):
    if isinstance(v, (tuple, list)):
        return float(v[0]), float(v[1])
    x, y = str(v).strip().strip("()").split(",")
    return float(x), float(y)


def _point_text(v):
    if isinstance(v, (tuple, list)):
        x, y = _point_parts(v)
        return f"({_float8_text(x)},{_float8_text(y)})"
    return str(v)


def _money_value(v):
    """A money value as a decimal: a number, or money's own text (``-$1,234.57``)."""
    if isinstance(v, str):
        text = v.strip().replace(",", "").replace("$", "")
        return decimal.Decimal(text)
    return v if isinstance(v, decimal.Decimal) else decimal.Decimal(str(v))


def _money_cents(v):
    return int(
        _money_value(v).quantize(decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP) * 100
    )


def _money_of_cents(v):
    """cash_recv: money's binary form, its int8 cents, as a decimal."""
    return decimal.Decimal(struct.unpack("!q", v)[0]).scaleb(-2)


def _money_text(v):
    """cash_out under lc_monetary en_US.UTF-8: a dollar sign, comma thousands, two decimals."""
    if isinstance(v, str):
        return v
    cents = _money_cents(v)
    sign = "-" if cents < 0 else ""
    dollars, rem = divmod(abs(cents), 100)
    return f"{sign}${dollars:,}.{rem:02d}"


def _micros_since_2000(dt):
    # A naive value (TIMESTAMP WITHOUT TIME ZONE — what DuckDB and psycopg2 return for `timestamp`)
    # counts from a naive epoch; subtracting the aware one raised TypeError mid-stream.
    epoch = datetime.datetime(2000, 1, 1)
    if dt.tzinfo is not None:
        epoch = epoch.replace(tzinfo=datetime.timezone.utc)
    delta = dt - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


# PostgreSQL binary numeric (OID 1700) encoder.
# Format: int16 ndigits, int16 weight, uint16 sign, int16 dscale, then ndigits x uint16 base-10000 digits.
def _numeric_to_pg_binary(v) -> bytes:
    d = v if isinstance(v, decimal.Decimal) else decimal.Decimal(str(v))
    if d.is_nan():
        return struct.pack("!HhHH", 0, 0, 0xC000, 0)

    sign_flag = 0x4000 if d < 0 else 0x0000
    # copy_abs, not abs(): abs() rounds to the context's 28 digits.
    d_abs = d.copy_abs()

    _, _, exp = d_abs.as_tuple()
    dscale = max(0, -exp) if isinstance(exp, int) and exp < 0 else 0

    s = format(d_abs, "f")
    if "." in s:
        int_str, frac_str = s.split(".")
    else:
        int_str, frac_str = s, ""

    int_str = int_str.lstrip("0") or ""

    if int_str:
        pad = (-len(int_str)) % 4
        int_str_padded = "0" * pad + int_str
        int_groups = [int(int_str_padded[i : i + 4]) for i in range(0, len(int_str_padded), 4)]
        weight = len(int_groups) - 1
    else:
        int_groups = []
        weight = -1

    if frac_str:
        pad = (-len(frac_str)) % 4
        frac_str_padded = frac_str + "0" * pad
        frac_groups = [int(frac_str_padded[i : i + 4]) for i in range(0, len(frac_str_padded), 4)]
        while frac_groups and frac_groups[0] == 0 and not int_groups:
            frac_groups.pop(0)
            weight -= 1
    else:
        frac_groups = []

    digits = int_groups + frac_groups
    while digits and digits[-1] == 0:
        digits.pop()

    ndigits = len(digits)
    if ndigits == 0:
        return struct.pack("!HhHH", 0, 0, sign_flag, dscale)

    return struct.pack(f"!HhHH{ndigits}H", ndigits, weight, sign_flag, dscale, *digits)


PG_UNKNOWN = (705, str)
BVTYPE_TO_PGTYPE = {
    BVType.SMALLINT: (21, str, lambda r: r.to_bytes(2, "big", signed=True)),
    BVType.REAL: (700, _float4_text, lambda r: struct.pack("!f", r)),
    BVType.JSONB: (
        3802,
        lambda v: json.dumps(v) if not isinstance(v, str) else v,
        # jsonb binary = a version byte (1) + the JSON text.
        lambda r: b"\x01" + (json.dumps(r) if not isinstance(r, str) else r).encode("utf-8"),
    ),
    BVType.TIMESTAMPTZ: (1184, _timestamp_text, _timestamp_binary(_micros_since_2000_utc)),
    BVType.TIMETZ: (1266, _pg_clock, _timetz_to_pg_binary),
    BVType.UUID: (2950, str, _uuid_to_pg_binary),
    BVType.NULL: (-1, lambda *_: None),
    BVType.ARRAY: (
        2277,
        lambda v: "{" + ",".join(v) + "}",
        lambda r: ("{" + ",".join(r) + "}").encode("utf-8"),
    ),
    BVType.BIGINT: (20, str, lambda r: r.to_bytes(8, "big", signed=True)),
    BVType.BOOL: (
        16,
        # boolout: PostgreSQL's text form of a boolean is t / f.
        lambda v: "t" if v else "f",
        lambda r: b"\x01" if r else b"\x00",
    ),
    BVType.BYTES: (17, lambda v: "\\x" + v.hex(), lambda r: r),
    BVType.DATE: (1082, _date_text, _date_binary),
    BVType.DECIMAL: (1700, str, _numeric_to_pg_binary),
    BVType.FLOAT: (701, _float8_text, lambda r: struct.pack("!d", r)),
    BVType.INTEGER: (23, str, lambda r: r.to_bytes(4, "big", signed=True)),
    BVType.OID: (26, str, lambda r: r.to_bytes(4, "big", signed=False)),
    BVType.INTERVAL: (
        1186,
        _interval_text,
        # interval binary = int64 time-of-day microseconds, int32 days, int32 months (16 bytes);
        # the former "!qih" wrote 14 bytes, which no binary client can decode.
        lambda r: struct.pack("!qii", *reversed(_interval_parts(r))),
    ),
    BVType.JSON: (
        114,
        lambda v: json.dumps(v) if not isinstance(v, str) else v,
        lambda r: (json.dumps(r) if not isinstance(r, str) else r).encode("utf-8"),
    ),
    # str(r) in binary too: a declared text-family column may carry a non-str value (a UUID, a
    # driver-specific scalar) — its text form is the wire value in both formats.
    BVType.TEXT: (25, str, lambda r: str(r).encode("utf-8")),
    BVType.TIME: (
        1083,
        _pg_clock,
        lambda r: int.to_bytes(_time_to_microseconds(r), 8, "big"),
    ),
    # signed: a value before 2000-01-01 is a negative offset (unsigned raised OverflowError).
    BVType.TIMESTAMP: (1114, _timestamp_text, _timestamp_binary(_micros_since_2000)),
    # Provisa's IR subtypes (REQ-2171): each sent as its PostgreSQL type, in its text and binary
    # forms.
    BVType.INET: (869, _inet_text, _address_binary(False)),
    BVType.CIDR: (650, str, _address_binary(True)),
    BVType.MACADDR: (829, str, _macaddr_binary),
    BVType.XML: (142, str, lambda r: str(r).encode("utf-8")),
    BVType.POINT: (600, _point_text, lambda r: struct.pack("!dd", *_point_parts(r))),
    BVType.MONEY: (790, _money_text, lambda r: struct.pack("!q", _money_cents(r))),
    BVType.MACADDR8: (774, str, _macaddr8_binary),
    BVType.LINE: (628, str, _shape_binary(3)),
    BVType.LSEG: (601, str, _shape_binary(4)),
    BVType.BOX: (603, str, _shape_binary(4)),
    BVType.PATH: (602, str, _path_binary),
    BVType.POLYGON: (604, str, _polygon_binary),
    BVType.CIRCLE: (718, str, _shape_binary(3)),
    BVType.GEOMETRY: (8310, str, _wkb_not_sent),
    BVType.GEOGRAPHY: (8311, str, _wkb_not_sent),
    BVType.INT4RANGE: (3904, str, _range_binary(lambda b: struct.pack("!i", int(b)))),
    BVType.INT8RANGE: (3926, str, _range_binary(lambda b: struct.pack("!q", int(b)))),
    BVType.NUMRANGE: (3906, str, _range_binary(_numeric_to_pg_binary)),
    BVType.TSRANGE: (
        3908,
        str,
        _range_binary(
            lambda b: _timestamp_binary(_micros_since_2000)(datetime.datetime.fromisoformat(b))
        ),
    ),
    BVType.TSTZRANGE: (
        3910,
        str,
        _range_binary(
            lambda b: _timestamp_binary(_micros_since_2000_utc)(datetime.datetime.fromisoformat(b))
        ),
    ),
    BVType.DATERANGE: (
        3912,
        str,
        _range_binary(lambda b: _date_binary(datetime.date.fromisoformat(b))),
    ),
    BVType.BIT: (1560, str, _bit_binary),
    BVType.VARBIT: (1562, str, _bit_binary),
    BVType.TSVECTOR: (3614, str, _tsvector_binary),
    # PostgreSQL's other built-in types (REQ-2171), each value its canonical text.
    BVType.CHAR: (18, str, lambda r: str(r).encode("latin-1")),
    BVType.NAME: (19, str, _text_bytes),
    BVType.TID: (27, str, _tid_binary),
    BVType.XID: (28, str, _unsigned_binary(4)),
    BVType.CID: (29, str, _unsigned_binary(4)),
    BVType.PG_NODE_TREE: (194, str, _text_bytes),
    BVType.ACLITEM: (1033, str, _text_only("aclitem")),
    BVType.REFCURSOR: (1790, str, _text_bytes),
    BVType.REGPROCEDURE: (2202, str, _text_only("regprocedure")),
    BVType.REGOPER: (2203, str, _text_only("regoper")),
    BVType.REGOPERATOR: (2204, str, _text_only("regoperator")),
    BVType.TXID_SNAPSHOT: (2970, str, _snapshot_binary),
    BVType.PG_NDISTINCT: (3361, str, _text_only("pg_ndistinct")),
    BVType.PG_DEPENDENCIES: (3402, str, _text_only("pg_dependencies")),
    BVType.TSQUERY: (3615, str, _text_only("tsquery")),
    BVType.GTSVECTOR: (3642, str, _text_only("gtsvector")),
    BVType.REGCONFIG: (3734, str, _text_only("regconfig")),
    BVType.REGDICTIONARY: (3769, str, _text_only("regdictionary")),
    BVType.JSONPATH: (4072, str, _jsonpath_binary),
    BVType.REGROLE: (4096, str, _text_only("regrole")),
    BVType.REGCOLLATION: (4191, str, _text_only("regcollation")),
    BVType.PG_BRIN_BLOOM_SUMMARY: (4600, str, _text_only("pg_brin_bloom_summary")),
    BVType.PG_BRIN_MINMAX_MULTI_SUMMARY: (4601, str, _text_only("pg_brin_minmax_multi_summary")),
    BVType.PG_MCV_LIST: (5017, str, _text_only("pg_mcv_list")),
    BVType.PG_SNAPSHOT: (5038, str, _snapshot_binary),
    BVType.XID8: (5069, str, _unsigned_binary(8)),
}

for _range_type, _multirange_type, _multirange_oid in (
    (BVType.INT4RANGE, BVType.INT4MULTIRANGE, 4451),
    (BVType.INT8RANGE, BVType.INT8MULTIRANGE, 4536),
    (BVType.NUMRANGE, BVType.NUMMULTIRANGE, 4532),
    (BVType.TSRANGE, BVType.TSMULTIRANGE, 4533),
    (BVType.TSTZRANGE, BVType.TSTZMULTIRANGE, 4534),
    (BVType.DATERANGE, BVType.DATEMULTIRANGE, 4535),
):
    BVTYPE_TO_PGTYPE[_multirange_type] = (
        _multirange_oid,
        str,
        _multirange_binary(BVTYPE_TO_PGTYPE[_range_type][2]),
    )


_ARRAY_TEXT_SPECIAL = re.compile(r'[{}",\\\s]')


def _array_text_element(text: str) -> str:
    """An array element's text as PostgreSQL's array_out writes it: double-quoted (with \\ and "
    escaped) when it is empty, spells NULL, or holds a brace, quote, comma, backslash or space."""
    if text == "" or text.upper() == "NULL" or _ARRAY_TEXT_SPECIAL.search(text):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _array_shape(values: list) -> tuple[list[int], list]:
    """A rectangular array's length in each dimension and its elements in order. PostgreSQL holds
    no other shape, so a list of unequal lists has no array to send."""
    dimensions, level = [], values
    while isinstance(level, list):
        dimensions.append(len(level))
        level = level[0] if level else None
    flat = values
    for _ in dimensions[1:]:
        flat = [v for row in flat for v in row]
    if len(flat) != math.prod(dimensions):
        raise ValueError(f"multidimensional array has unequal sublists: {values}")
    return dimensions, flat


def _array_entry(element: BVType, array_oid: int, element_text=None, element_oid=None):
    """The (OID, text, binary) entry of an array of ``element``, of any dimensions: text as
    ``{{a,b},{NULL,c}}``, binary as PostgreSQL's array_send (dimensions, null flag, element OID,
    each dimension's length and lower bound, then each element length-prefixed, -1 for NULL).
    ``element_oid`` names an element type sent in ``element``'s form (a "char" as text)."""
    elem_oid, elem_text, elem_binary = BVTYPE_TO_PGTYPE[element]
    elem_oid = elem_oid if element_oid is None else element_oid
    text_of = element_text or elem_text

    def text(values):
        written = (
            "NULL"
            if v is None
            else text(v)
            if isinstance(v, list)
            else _array_text_element(text_of(v))
            for v in values
        )
        return "{" + ",".join(written) + "}"

    def binary(values):
        dimensions, flat = _array_shape(values)
        has_null = any(v is None for v in flat)
        out = [struct.pack("!iii", len(dimensions), 1 if has_null else 0, elem_oid)]
        out.extend(struct.pack("!ii", length, 1) for length in dimensions)
        for v in flat:
            if v is None:
                out.append(struct.pack("!i", -1))
            else:
                encoded = elem_binary(v)
                out.append(struct.pack("!i", len(encoded)) + encoded)
        return b"".join(out)

    return (array_oid, text, binary)


BVTYPE_TO_PGTYPE.update(
    {
        BVType.INTEGERARRAY: _array_entry(BVType.INTEGER, 1007),
        BVType.BIGINTARRAY: _array_entry(BVType.BIGINT, 1016),
        BVType.SMALLINTARRAY: _array_entry(BVType.SMALLINT, 1005),
        BVType.FLOATARRAY: _array_entry(BVType.FLOAT, 1022),
        BVType.REALARRAY: _array_entry(BVType.REAL, 1021),
        BVType.DECIMALARRAY: _array_entry(BVType.DECIMAL, 1231),
        # array_out writes a boolean element as t / f.
        BVType.BOOLARRAY: _array_entry(BVType.BOOL, 1000, lambda v: "t" if v else "f"),
        BVType.STRINGARRAY: _array_entry(BVType.TEXT, 1009),
        BVType.OIDARRAY: _array_entry(BVType.OID, 1028),
        BVType.MONEYARRAY: _array_entry(BVType.MONEY, 791),
        BVType.INTERVALARRAY: _array_entry(BVType.INTERVAL, 1187),
    }
)


def _vector_entry(element: BVType, vector_oid: int):
    """oidvector / int2vector: written as their elements separated by spaces (oidvectorout,
    int2vectorout), sent as a one-dimensional array of the element type (array_send). A column
    read as its PostgreSQL text (REQ-2171) is that text."""
    array_binary = _array_entry(element, vector_oid)[2]

    def binary(values):
        return array_binary([int(v) for v in values.split()] if isinstance(values, str) else values)

    return (
        vector_oid,
        lambda values: values if isinstance(values, str) else " ".join(str(v) for v in values),
        binary,
    )


BVTYPE_TO_PGTYPE.update(
    {
        BVType.OIDVECTOR: _vector_entry(BVType.OID, 30),
        BVType.INT2VECTOR: _vector_entry(BVType.SMALLINT, 22),
    }
)


def _no_binary_aclitem(_values):
    # aclitem has no binary send function in PostgreSQL either: a client asks for it as text.
    raise TypeError("aclitem has no binary form; request the column as text")


def _lsn_position(value) -> int:
    """A pg_lsn as its position: written X/Y (X the high 32 bits, in hex), or the integer itself
    (asyncpg reads a source's pg_lsn so)."""
    if isinstance(value, int):
        return value
    high, low = str(value).split("/")
    return (int(high, 16) << 32) | int(low, 16)


def _lsn_text(value) -> str:
    position = _lsn_position(value)
    return f"{position >> 32:X}/{position & 0xFFFFFFFF:X}"


def _lsn_binary(value) -> bytes:
    """pg_lsn_send: the position as one unsigned 8-byte integer."""
    return _lsn_position(value).to_bytes(8, "big")


BVTYPE_TO_PGTYPE.update(
    {
        BVType.ACLITEMARRAY: (
            1034,
            _array_entry(BVType.TEXT, 1034)[1],
            _no_binary_aclitem,
        ),
        BVType.CHARARRAY: _array_entry(BVType.TEXT, 1002, element_oid=18),
        BVType.NAMEARRAY: _array_entry(BVType.TEXT, 1003, element_oid=19),
        BVType.PG_LSN: (3220, _lsn_text, _lsn_binary),
    }
)



# --- provisa.pgwire.server: the reg* writers it registered, and xml[] ---

from collections.abc import Callable
from typing import Any

XML_ARRAY: Any = "xml[]"


def _reg_text(names: dict[int, str]) -> Callable[[Any], str]:  # REQ-2167
    """A reg* type's text form, as regprocout / regtypeout write it: the object's name, "-" for
    0, and the number itself for an oid that names no object. A value already in its text form
    (a source's own reg* column, read as text) is that text."""

    def text(value: Any) -> str:
        if isinstance(value, str):
            return value
        if value == 0:
            return "-"
        return names[value] if value in names else str(value)

    return text


def _reg_binary(value: Any) -> bytes:
    if isinstance(value, str):
        raise TypeError(f"a reg* value read as its text form {value!r} has no binary form here")
    return value.to_bytes(4, "big", signed=False)


def _named_text(value: dict | str) -> str:  # REQ-2167
    return value if isinstance(value, str) else value["name"]  # a source's own column: its text


def _named_binary(value: dict | str) -> bytes:  # REQ-2167
    if isinstance(value, str):
        raise TypeError(f"a reg* value read as its text form {value!r} has no binary form here")
    return value["oid"].to_bytes(4, "big", signed=False)


def _regtype_binary(oids: dict[str, int]) -> Callable[[Any], bytes]:  # REQ-2167
    """A regtype's binary form: an engine answers one by its name (pg_typeof), sent as its oid."""

    def binary(value: Any) -> bytes:
        return _reg_binary(oids.get(value, value) if isinstance(value, str) else value)

    return binary


def _register_reg_types() -> None:
    from provisa.pgwire.catalog_pg17 import PROC_NAMES, TYPES

    type_names = {oid: entry[-1] for oid, entry in TYPES.items()}
    type_oids = {name: oid for oid, name in type_names.items()}
    BVTYPE_TO_PGTYPE[BVType.REGPROC] = (24, _reg_text(PROC_NAMES), _reg_binary)
    # A regclass or regnamespace value is its oid and its name (catalog_regs): sent as the name
    # in text, the oid in binary.
    BVTYPE_TO_PGTYPE[BVType.REGCLASS] = (2205, _named_text, _named_binary)
    BVTYPE_TO_PGTYPE[BVType.REGNAMESPACE] = (4089, _named_text, _named_binary)
    BVTYPE_TO_PGTYPE[BVType.REGTYPE] = (2206, _reg_text(type_names), _regtype_binary(type_oids))
    BVTYPE_TO_PGTYPE[BVType.REGTYPEARRAY] = _array_entry(BVType.REGTYPE, 2211)
    oid, text, binary = _array_entry(BVType.XML, 143)
    # array_send of an empty array: no dimensions, no null flag, the element's OID.
    empty = struct.pack("!iii", 0, 0, 142)
    BVTYPE_TO_PGTYPE[XML_ARRAY] = (oid, text, lambda values: binary(values) if values else empty)


_register_reg_types()


# --- buenavista's send_data_rows loop over the converters above ---

_DATA_ROW_CHUNK_SIZE = 500
DATA_ROW = b"D"


def send_data_rows(wfile: Any, query_result: Any, rows: Any) -> int:
    """The DataRow bytes ``rows`` (Python row values, already coerced) were written as: each
    column's converter for its wire type and result format, in 500-row writes."""
    cnt = 0
    converters = []
    ncols = query_result.column_count()
    for i in range(ncols):
        bvtype = query_result.column(i)[1]
        pgtype = BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)
        use_binary = (
            query_result.result_format
            and query_result.result_format[i] == 1
            and len(pgtype) > 2
            and pgtype[2] is not None
        )
        if use_binary:
            converters.append((pgtype[2], False))
        else:
            converters.append((pgtype[1], True))
    pack_i32 = _INT32_STRUCT.pack
    chunk = bytearray()
    chunk_n = 0
    for row in rows:
        if isinstance(row, RawDataRowBytes):
            chunk += row
        else:
            row_buf = bytearray()
            for j in range(ncols):
                r = row[j]
                if r is None:
                    row_buf += b"\xff\xff\xff\xff"
                else:
                    converter, do_encode = converters[j]
                    v = converter(r)
                    if do_encode:
                        v = v.encode("utf-8")
                    row_buf += pack_i32(len(v))
                    row_buf += v
            chunk += struct.pack("!cih", DATA_ROW, len(row_buf) + 6, ncols)
            chunk += row_buf
        cnt += 1
        chunk_n += 1
        if chunk_n >= _DATA_ROW_CHUNK_SIZE:
            wfile.write(bytes(chunk))
            chunk.clear()
            chunk_n = 0
    if chunk_n:
        wfile.write(bytes(chunk))
    return cnt
