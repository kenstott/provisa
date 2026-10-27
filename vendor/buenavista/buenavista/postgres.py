# Copyright (c) 2026 Kenneth Stott
# Canary: 83b42410-aaf4-4c83-83af-0fae2f3a5f8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import datetime
import decimal
import hashlib
import ipaddress
import uuid as _uuid_mod
import io
import json
import logging
import os
import random
import re
import socket
import socketserver
import struct
from typing import Dict, List, Optional

from .core import BVType, Connection, Extension, Session, QueryResult, RawDataRowBytes
from .rewrite import Rewriter

logger = logging.getLogger(__name__)

_INT32_STRUCT = struct.Struct("!i")

NULL_BYTE = b"\x00"


class ServerResponse:
    """Byte codes for server responses in the PG wire protocol."""

    AUTHENTICATION_REQUEST = b"R"
    BACKEND_KEY_DATA = b"K"
    BIND_COMPLETE = b"2"
    CLOSE_COMPLETE = b"3"
    COMMAND_COMPLETE = b"C"
    COPY_DATA = b"d"
    COPY_DONE = b"c"
    COPY_FAIL = b"f"
    COPY_IN_RESPONSE = b"G"
    COPY_OUT_RESPONSE = b"H"
    DATA_ROW = b"D"
    EMPTY_QUERY_RESPONSE = b"I"
    ERROR_RESPONSE = b"E"
    NO_DATA = b"n"
    NOTICE_RESPONSE = b"N"
    PARAMETER_DESCRIPTION = b"t"
    PARAMETER_STATUS = b"S"
    PARSE_COMPLETE = b"1"
    PORTAL_SUSPENDED = b"s"
    READY_FOR_QUERY = b"Z"
    ROW_DESCRIPTION = b"T"


def _decode_numeric(v: bytes) -> decimal.Decimal:
    """Decode PG binary numeric: ndigits/weight/sign/dscale + base-10000 digit array."""
    if len(v) < 8:
        return decimal.Decimal(0)
    ndigits, weight, sign, dscale = struct.unpack("!hhhh", v[:8])
    if sign == 0xC000:
        return decimal.Decimal("NaN")
    digits = [struct.unpack("!H", v[8 + i * 2 : 10 + i * 2])[0] for i in range(ndigits)]
    value = decimal.Decimal(0)
    for i, d in enumerate(digits):
        value += decimal.Decimal(d) * (decimal.Decimal(10000) ** (weight - i))
    if sign == 0x4000:
        value = -value
    return value.quantize(decimal.Decimal(10) ** -dscale) if dscale else value


_PG_EPOCH = datetime.datetime(2000, 1, 1)
_PG_EPOCH_UTC = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
_PG_DATE_EPOCH = datetime.date(2000, 1, 1)


def _text_bool(s: str) -> bool:
    v = s.strip().lower()
    if v in ("t", "true", "1", "y", "yes", "on"):
        return True
    if v in ("f", "false", "0", "n", "no", "off"):
        return False
    raise ValueError(f"invalid boolean parameter {s!r}")


# Text-format Bind values, decoded by the parameter's declared type OID (REQ-589): the value stays
# BOUND (never spliced into the SQL), so it must arrive as the typed value the client declared.
# An OID not listed (0 = unspecified, text types) keeps the string — the engine casts it by context,
# exactly as it cast the untyped string literal this value used to be spliced in as.
TEXT_PARAM_DECODERS = {
    16: _text_bool,
    20: int,
    21: int,
    23: int,
    700: float,
    701: float,
    1700: decimal.Decimal,
    1082: datetime.date.fromisoformat,
    1114: datetime.datetime.fromisoformat,
    1184: datetime.datetime.fromisoformat,
    2950: _uuid_mod.UUID,
}


TYPE_OIDS = {
    # see Postgres pg_type_d.h — values are (name, decoder_callable, example)
    16: ("BOOLOID", lambda v: v[0] != 0, False),
    17: ("BYTEAOID", bytes, b""),
    20: ("INT8OID", lambda v: struct.unpack("!q", v)[0], 42),
    21: ("INT2OID", lambda v: struct.unpack("!h", v)[0], 42),
    23: ("INT4OID", lambda v: struct.unpack("!i", v)[0], 42),
    25: ("TEXTOID", lambda v: v.decode("utf-8"), ""),
    700: ("FLOAT4OID", lambda v: struct.unpack("!f", v)[0], 42.0),
    701: ("FLOAT8OID", lambda v: struct.unpack("!d", v)[0], 42.0),
    1043: ("VARCHAROID", lambda v: v.decode("utf-8"), ""),
    1082: (
        "DATEOID",
        lambda v: _PG_DATE_EPOCH + datetime.timedelta(days=struct.unpack("!i", v)[0]),
        datetime.date.today(),
    ),
    1114: (
        "TIMESTAMPOID",
        lambda v: _PG_EPOCH + datetime.timedelta(microseconds=struct.unpack("!q", v)[0]),
        datetime.datetime.now(),
    ),
    1184: (
        "TIMESTAMPTZOID",
        lambda v: _PG_EPOCH_UTC + datetime.timedelta(microseconds=struct.unpack("!q", v)[0]),
        datetime.datetime.now(datetime.timezone.utc),
    ),
    1700: ("NUMERICOID", _decode_numeric, decimal.Decimal(0)),
    2950: ("UUIDOID", lambda v: str(_uuid_mod.UUID(bytes=bytes(v))), ""),
    26: ("OIDOID", lambda v: struct.unpack("!I", v)[0], 0),
    0: ("UNSPECIFIED", lambda v: v.decode("utf-8"), ""),
    1028: (
        "_OIDOID",
        lambda v: [
            struct.unpack_from("!I", v, 20 + i * 8 + 4)[0]
            for i in range(struct.unpack_from("!i", v, 12)[0])
        ],
        [],
    ),
}


class TransactionStatus:
    IDLE = b"I"
    IN_TRANSACTION = b"T"
    IN_FAILED_TRANSACTION = b"E"


class ClientCommand:
    """Byte codes for client commands in the PG wire protocol."""

    BIND = b"B"
    CLOSE = b"C"
    DESCRIBE = b"D"
    EXECUTE = b"E"
    FLUSH = b"H"
    QUERY = b"Q"
    PARSE = b"P"
    PASSWORD_MESSAGE = b"p"
    SYNC = b"S"
    TERMINATE = b"X"


def _interval_parts(v):
    """(months, days, microseconds) of an interval value: a timedelta, or a relativedelta when it
    has months (a year-month interval)."""
    if isinstance(v, datetime.timedelta):
        micros = v // datetime.timedelta(microseconds=1)
        days = int(micros / 86_400_000_000)
        return 0, days, micros - days * 86_400_000_000
    seconds = (v.hours * 60 + v.minutes) * 60 + v.seconds
    return v.years * 12 + v.months, v.days, seconds * 1_000_000 + v.microseconds


def _interval_text(v) -> str:
    """An interval as Postgres writes it under IntervalStyle postgres (1 year 2 mons 3 days
    04:05:06.5), the text a client's interval parser reads."""
    months, days, micros = _interval_parts(v)
    years, months = int(months / 12), months - int(months / 12) * 12
    parts = [
        f"{n} {unit}{'' if n == 1 else plural}"
        for n, unit, plural in ((years, "year", "s"), (months, "mon", "s"), (days, "day", "s"))
        if n
    ]
    if micros or not parts:
        hours, rem = divmod(abs(micros), 3_600_000_000)
        minutes, rem = divmod(rem, 60_000_000)
        secs, frac = divmod(rem, 1_000_000)
        clock = f"{'-' if micros < 0 else ''}{hours:02d}:{minutes:02d}:{secs:02d}"
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


def _timestamp_text(v) -> str:
    return _infinity_text(v) if _infinity(v) else v.isoformat().replace("T", " ")


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


def _timetz_to_pg_binary(t):
    """timetz binary: int64 microseconds since midnight + int32 zone offset in seconds WEST of UTC."""
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


def _wkb_not_sent(r):
    """PostGIS's binary form is EWKB; Provisa holds geometry and geography as WKT and sends them
    as text only (docs/conformance/pgwire.md)."""
    raise ValueError("geometry and geography are sent in text format only")


def _float8_text(x):
    """float8out: the shortest exact form, with no trailing .0."""
    text = repr(float(x))
    return text[:-2] if text.endswith(".0") else text


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


def _money_text(v):
    """cash_out under lc_monetary C: a dollar sign, comma thousands, two decimals."""
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
    d_abs = abs(d)

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
    BVType.REAL: (700, str, lambda r: struct.pack("!f", r)),
    BVType.JSONB: (
        3802,
        lambda v: json.dumps(v) if not isinstance(v, str) else v,
        # jsonb binary = a version byte (1) + the JSON text.
        lambda r: b"\x01" + (json.dumps(r) if not isinstance(r, str) else r).encode("utf-8"),
    ),
    BVType.TIMESTAMPTZ: (1184, _timestamp_text, _timestamp_binary(_micros_since_2000_utc)),
    BVType.TIMETZ: (1266, lambda v: v.isoformat(), _timetz_to_pg_binary),
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
    BVType.FLOAT: (701, str, lambda r: struct.pack("!d", r)),
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
        lambda v: v.isoformat(),
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
}


_ARRAY_TEXT_SPECIAL = re.compile(r'[{}",\\\s]')


def _array_text_element(text: str) -> str:
    """An array element's text as PostgreSQL's array_out writes it: double-quoted (with \\ and "
    escaped) when it is empty, spells NULL, or holds a brace, quote, comma, backslash or space."""
    if text == "" or text.upper() == "NULL" or _ARRAY_TEXT_SPECIAL.search(text):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _array_entry(element: BVType, array_oid: int, element_text=None, element_oid=None):
    """The (OID, text, binary) entry of a one-dimensional array of ``element``: text as
    ``{a,b,NULL}``, binary as PostgreSQL's array_send (dimensions, null flag, element OID, then
    each element length-prefixed, -1 for NULL). ``element_oid`` names an element type sent in
    ``element``'s form (a "char" as text)."""
    elem_oid, elem_text, elem_binary = BVTYPE_TO_PGTYPE[element]
    elem_oid = elem_oid if element_oid is None else element_oid
    text_of = element_text or elem_text

    def text(values):
        return (
            "{"
            + ",".join("NULL" if v is None else _array_text_element(text_of(v)) for v in values)
            + "}"
        )

    def binary(values):
        has_null = any(v is None for v in values)
        out = [struct.pack("!iiiii", 1, 1 if has_null else 0, elem_oid, len(values), 1)]
        for v in values:
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
    }
)


def _vector_entry(element: BVType, vector_oid: int):
    """oidvector / int2vector: written as their elements separated by spaces (oidvectorout,
    int2vectorout), sent as a one-dimensional array of the element type (array_send)."""
    return (
        vector_oid,
        lambda values: " ".join(str(v) for v in values),
        _array_entry(element, vector_oid)[2],
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
        BVType.PG_LSN: (3220, _lsn_text, _lsn_binary),
    }
)


class BVBuffer(object):
    """A helper for reading and writing bytes in the format the PG wire protocol expects."""

    def __init__(self, stream=None):
        if not stream:
            stream = io.BytesIO()
        self.stream = stream

    def read_bytes(self, n) -> bytes:
        return self.stream.read(n)

    def read_byte(self) -> bytes:
        return self.read_bytes(1)

    def read_int16(self) -> int:
        data = self.read_bytes(2)
        return struct.unpack("!h", data)[0]

    def read_uint32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!I", data)[0]

    def read_int32(self) -> int:
        data = self.read_bytes(4)
        return struct.unpack("!i", data)[0]

    def write_bytes(self, value: bytes):
        self.stream.write(value)

    def write_byte(self, value):
        self.stream.write(struct.pack("!c", value))

    def write_int16(self, value: int):
        self.stream.write(struct.pack("!h", value))

    def write_int32(self, value: int):
        self.stream.write(struct.pack("!i", value))

    def write_string(self, value):
        self.stream.write(value.encode() if isinstance(value, str) else value)
        self.stream.write(b"\x00")

    def get_value(self) -> bytes:
        return self.stream.getvalue()


class BVContext:
    """Manages the state of a single connection to the server."""

    def __init__(self, session: Session, rewriter: Optional[Rewriter], params: Dict[str, str]):
        self.session = session
        self.rewriter = rewriter
        self.params = params
        self.process_id = random.randint(0, 2**32 - 1)
        self.secret_key = random.randint(0, 2**32 - 1)
        self.stmts = {}
        self.portals = {}
        self.result_cache = {}
        # statement name -> its Describe(Statement) result, held for the Bind that follows (see
        # describe_statement / add_portal).
        self.described = {}
        # statement name -> the shape its Describe(Statement) told the client. Every later Execute
        # of the statement is encoded to it (the client decodes by what it was told once).
        self.statement_shapes = {}
        # statement name -> what the session prepared at Describe(Statement) for the NEXT Bind's
        # Execute to continue from (single use); portal name -> the one its Bind took.
        self.statement_prepared = {}
        self.portal_prepared = {}
        self.has_error = False
        self.authenticated = False
        self.salt: bytes | None = None

    def get_hashed_password(self, auth: dict) -> str:
        assert self.salt is not None
        user = self.params["user"]
        password = auth[user]
        first = hashlib.md5(password.encode("utf-8") + user.encode("utf-8")).hexdigest()
        return "md5" + hashlib.md5(first.encode("utf-8") + self.salt).hexdigest()

    def mark_error(self):
        self.has_error = True

    def transaction_status(self):
        if self.session.in_transaction():
            if self.has_error:
                return TransactionStatus.IN_FAILED_TRANSACTION
            else:
                return TransactionStatus.IN_TRANSACTION
        return TransactionStatus.IDLE

    def execute_sql(
        self, sql: str, params=None, result_fmt=None, *, prepared=None, shape=None
    ) -> QueryResult:
        logger.info("Input SQL: %s", sql)
        if self.rewriter:
            sql = self.rewriter.rewrite(sql)
            logger.info("Rewritten SQL: %s", sql)
        if prepared is None and shape is None:
            qr = self.session.execute_sql(sql, params, result_fmt)
        else:
            qr = self.session.execute_sql(sql, params, result_fmt, prepared=prepared, shape=shape)
        if qr.has_results():
            if result_fmt and len(result_fmt) != qr.column_count():
                qr.result_format = [result_fmt[0]] * qr.column_count()
            else:
                qr.result_format = result_fmt
        return qr

    def describe_portal(self, name: str) -> QueryResult:
        stmt, params, result_fmt = self.portals[name]
        sql, _ = self.stmts[stmt]
        query_result = self.execute_sql(
            sql=sql,
            params=params,
            result_fmt=result_fmt,
            prepared=self.portal_prepared.pop(name, None),
            shape=self.statement_shapes.get(stmt),
        )
        self.result_cache[name] = query_result
        return query_result

    def describe_statement(self, name: str) -> QueryResult:
        sql, param_oids = self.stmts[name]
        params = []
        for typeoid in param_oids:
            type = TYPE_OIDS.get(typeoid)
            if type:
                params.append(type[2])
            else:
                raise Exception(f"Unsupported parameter type: {typeoid}")
        if self.rewriter:
            sql = self.rewriter.rewrite(sql)
        # The session describes the statement WITHOUT running it where it can: the result carries
        # the shape the client is told (``statement_shape``) and what the session prepared for the
        # Execute to continue from (``prepared``). Every later Execute of the statement is encoded
        # to that shape; the prepared work is taken by the next Bind only.
        #
        # A session result with neither is one it could only produce by running the statement (a
        # catalog answer). That run is handed to the Execute of the Bind that follows (add_portal)
        # instead of running it a second time. Only a statement without parameters qualifies: with
        # parameters this run used placeholder example values.
        qr = self.session.describe_sql(sql, params)
        self._drop_described(name)
        shape = getattr(qr, "statement_shape", None)
        if shape is not None:
            self.statement_shapes[name] = shape
            self.statement_prepared[name] = getattr(qr, "prepared", None)
        elif not param_oids and qr.has_results():
            self.described[name] = qr
        return qr

    def _drop_described(self, stmt: str) -> None:
        self.statement_shapes.pop(stmt, None)
        self.statement_prepared.pop(stmt, None)
        held = self.described.pop(stmt, None)
        if held is not None:
            held.close()

    def release_results(self) -> None:
        """Close every result this connection still holds (suspended portals, Describe-time
        results awaiting a Bind) — each may hold a live engine cursor / source connection."""
        held = list(self.result_cache.values()) + list(self.described.values())
        self.result_cache.clear()
        self.described.clear()
        self.statement_shapes.clear()
        self.statement_prepared.clear()
        self.portal_prepared.clear()
        for qr in held:
            qr.close()

    def holds_described(self, query_result: QueryResult) -> bool:
        return any(held is query_result for held in self.described.values())

    def execute_portal(self, name: str) -> QueryResult:
        if name in self.result_cache:
            query_result = self.result_cache[name]
            del self.result_cache[name]
            return query_result
        else:
            stmt, params, result_fmt = self.portals[name]
            sql, _ = self.stmts[stmt]
            qr = self.execute_sql(
                sql=sql,
                params=params,
                result_fmt=result_fmt,
                prepared=self.portal_prepared.pop(name, None),
                shape=self.statement_shapes.get(stmt),
            )
            return qr

    def add_statement(self, name: str, sql: str, param_oids: List[int]):
        # A Parse of DIFFERENT text replaces the statement, so its held Describe result is stale.
        # A re-Parse of the identical statement is the same statement — asyncpg re-sends Parse for
        # the unnamed statement between its Describe and its Bind — so the held result stays.
        prior = self.stmts.get(name)
        # asyncpg re-Parses with UNSPECIFIED (0) parameter types after its Describe; the types that
        # Describe resolved (and told the client, which then encodes its Bind values in them) must
        # survive, or a binary int8 value is decoded as if untyped.
        if prior is not None and prior[0] == sql and all(o == 0 for o in param_oids):
            param_oids = prior[1]
        if prior != (sql, param_oids):
            self._drop_described(name)
        self.stmts[name] = (sql, param_oids)

    def close_statement(self, name: str):
        self._drop_described(name)
        del self.stmts[name]

    def add_portal(self, name: str, stmt: str, params: List, result_formats: List[int]):
        # A Bind REPLACES the portal it names (the unnamed portal on every simple client query). A
        # result the PREVIOUS query left cached under this name — suspended because it exactly
        # filled an Execute(limit), e.g. asyncpg's fetchval (limit=1) on a 1-row result — belongs
        # to that old portal: serving it to the new one returned the old, drained result (0 rows)
        # and never ran the new query. Close it with the portal it belonged to.
        stale = self.result_cache.pop(name, None)
        if stale is not None:
            stale.close()
        self.portals[name] = (stmt, params, result_formats)
        # What the statement's Describe prepared goes to THIS portal's Execute, once.
        self.portal_prepared.pop(name, None)
        prepared = self.statement_prepared.pop(stmt, None)
        if prepared is not None:
            self.portal_prepared[name] = prepared
        # Hand the statement's Describe-time result to this portal's Execute (see
        # describe_statement): it is the same statement with no parameters, so the result IS what
        # the Execute would compute. Used once — a later Bind of the statement runs it fresh.
        held = self.described.pop(stmt, None)
        if held is None:
            return
        if params or not held.reusable_for_execute(result_formats):
            held.close()
            return
        if result_formats and len(result_formats) != held.column_count():
            held.result_format = [result_formats[0]] * held.column_count()
        else:
            held.result_format = result_formats
        self.result_cache[name] = held

    def close_portal(self, name: str):
        del self.portals[name]
        self.portal_prepared.pop(name, None)
        # A Describe-then-Close flow (no Execute — e.g. a client fetching metadata only, or
        # aborting after Describe) leaves an already-executed result in result_cache with nobody
        # ever draining it. Close it here so a live cursor/source-connection it holds is released
        # deterministically instead of waiting on GC (which may never run promptly, or at all, for
        # a generator suspended before its first yield — see StreamingQueryResult.close()).
        query_result = self.result_cache.pop(name, None)
        if query_result is not None:
            query_result.close()

    def flush(self):
        pass

    def sync(self):
        if self.has_error:
            self.has_error = False


class BuenaVistaHandler(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.r = BVBuffer(self.rfile)
        ctx = None
        try:
            ctx = self.handle_startup(self.server.conn)  # type: ignore[attr-defined]
            if ctx:
                self.server.ctxts[ctx.process_id] = ctx  # type: ignore[attr-defined]
            while ctx:
                type_code = self.r.read_byte()
                if not type_code or type_code == ClientCommand.TERMINATE:
                    break

                msglen = self.r.read_uint32()
                if msglen > 4:
                    payload = self.r.read_bytes(msglen - 4)
                else:
                    payload = b""

                if not ctx.authenticated:
                    if type_code == ClientCommand.PASSWORD_MESSAGE:
                        self.handle_md5_password(ctx, payload)
                    else:
                        raise Exception("Not authenticated")
                elif type_code == ClientCommand.QUERY:
                    self.handle_query(ctx, payload)
                elif type_code == ClientCommand.PARSE:
                    self.handle_parse(ctx, payload)
                elif type_code == ClientCommand.BIND:
                    self.handle_bind(ctx, payload)
                elif type_code == ClientCommand.DESCRIBE:
                    self.handle_describe(ctx, payload)
                elif type_code == ClientCommand.EXECUTE:
                    self.handle_execute(ctx, payload)
                elif type_code == ClientCommand.CLOSE:
                    self.handle_close(ctx, payload)
                elif type_code == ClientCommand.SYNC:
                    ctx.sync()
                    self.send_ready_for_query(ctx)
                elif type_code == ClientCommand.FLUSH:
                    ctx.flush()
                else:
                    raise Exception("Unknown type_code: %s" % type_code)
        except Exception as e:
            logger.exception(e)
            self.send_error(e)

        if ctx:
            ctx.release_results()
            self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
            del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
            ctx = None

    def handle_startup(self, conn: Connection) -> Optional["BVContext"]:
        msglen = self.r.read_uint32() - 4
        code = self.r.read_uint32()
        if code == 80877103:  ## SSL request
            self.send_notice()
            return self.handle_startup(conn)
        elif code == 196608:  # Protocol 3.0
            msg = [x.decode("utf-8") for x in self.r.read_bytes(msglen - 4).split(NULL_BYTE)]
            params = dict(zip(msg[::2], msg[1::2]))
            logger.info("Client connection params: %s", params)
            ctx = BVContext(conn.create_session(), self.server.rewriter, params)  # type: ignore[attr-defined]
            self.send_auth_request(ctx)
            return ctx
        elif code == 80877102:  ## Cancel request
            process_id, secret_key = self.r.read_uint32(), self.r.read_uint32()
            ctx = self.server.ctxts.get(process_id)  # type: ignore[attr-defined]
            if ctx and ctx.secret_key == secret_key:
                self.server.conn.close_session(ctx.session)  # type: ignore[attr-defined]
                del self.server.ctxts[ctx.process_id]  # type: ignore[attr-defined]
                ctx = None
            return None
        else:
            raise Exception(f"Unsupported startup message: {code}")

    def send_auth_request(self, ctx: BVContext):
        if self.server.auth is None:  # type: ignore[attr-defined]
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
        else:
            self.send_authentication_md5(ctx)

    def send_authentication_ok(self):
        self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 8, 0))

    def send_authentication_md5(self, ctx: BVContext):
        ctx.salt = os.urandom(4)
        self.wfile.write(struct.pack("!cii", ServerResponse.AUTHENTICATION_REQUEST, 12, 5))
        self.wfile.write(ctx.salt)

    def handle_md5_password(self, ctx: BVContext, payload: bytes):
        client_side = payload.decode("utf-8").rstrip("\x00")
        server_side = ctx.get_hashed_password(self.server.auth)  # type: ignore[attr-defined]
        if client_side == server_side:
            self.send_authentication_ok()
            self.handle_post_auth(ctx)
        else:
            self.send_error("Invalid password")

    def handle_post_auth(self, ctx: BVContext):
        self.send_parameter_status(self.server.conn.parameters())  # type: ignore[attr-defined]
        self.send_backend_key_data(ctx)
        self.send_ready_for_query(ctx)
        ctx.authenticated = True
        return

    def handle_query(self, ctx: BVContext, payload: bytes):
        logger.debug("Handle query")
        decoded = payload.decode("utf-8").rstrip("\x00")
        try:
            if req := Extension.check_json(decoded):
                method = req.get("method")
                extension = self.server.extensions.get(method)  # type: ignore[attr-defined]
                if not extension:
                    raise Exception("Unknown method: " + str(method))
                else:
                    query_result = extension.apply(req.get("params"), ctx.session)
            else:
                query_result = ctx.execute_sql(decoded)
        except Exception as e:
            self.send_error(e)
            self.send_ready_for_query(ctx)
            return

        if not query_result:
            raise Exception("No query result for query: " + decoded)
        elif query_result.has_results():
            self.send_row_description(query_result)
            row_count = self.send_data_rows(query_result)
            self.send_command_complete("SELECT %d\x00" % row_count)
        else:
            status = query_result.status()
            self.send_command_complete(f"{status}\x00")
        self.send_ready_for_query(ctx)

    def handle_parse(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling parse")
        ba = bytearray(payload)
        stmt_idx = ba.index(NULL_BYTE)
        stmt = ba[:stmt_idx].decode("utf-8")
        query_idx = ba.index(NULL_BYTE, stmt_idx + 1)
        sql = ba[stmt_idx + 1 : query_idx].decode("utf-8")
        logger.debug("Parsed statement: %s", sql)
        buf = BVBuffer(io.BytesIO(ba[query_idx + 1 :]))
        num_params = buf.read_int16()
        param_oids = []
        for _ in range(num_params):
            param_oids.append(buf.read_int32())
        ctx.add_statement(stmt, sql, param_oids)
        self.send_parse_complete()

    def handle_bind(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling bind")
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        stmt_idx = ba.index(NULL_BYTE, portal_idx + 1)
        stmt = ba[portal_idx + 1 : stmt_idx].decode("utf-8")
        buf = BVBuffer(io.BytesIO(ba[stmt_idx + 1 :]))
        num_formats = buf.read_int16()
        formats = []
        for i in range(num_formats):
            formats.append(buf.read_int16())
        num_params = buf.read_int16()
        if num_formats < num_params:
            if formats:
                formats = [formats[0]] * num_params
            else:
                formats = [0] * num_params
        params = []
        for i in range(num_params):
            nb = buf.read_int32()
            if nb == -1:
                params.append(None)
                continue
            v = buf.read_bytes(nb)
            stmt_oids = ctx.stmts[stmt][1]
            typeoid = stmt_oids[i] if i < len(stmt_oids) else 0
            logger.debug("Format: %d, Type: %d", formats[i], typeoid)
            if formats[i] == 0:
                decoded = v.decode("utf-8")
                if typeoid in TEXT_PARAM_DECODERS:
                    params.append(TEXT_PARAM_DECODERS[typeoid](decoded))
                elif typeoid == 0:
                    # Unspecified type — store raw string; do not date-parse
                    if decoded.startswith("{") and decoded.endswith("}"):
                        params.append(decoded[1:-1].split(","))
                    else:
                        params.append(decoded)
                else:
                    if decoded.startswith("{") and decoded.endswith("}"):
                        params.append(decoded[1:-1].split(","))
                    else:
                        params.append(decoded)
            else:
                type = TYPE_OIDS.get(typeoid)
                if type:
                    params.append(type[1](v))
                else:
                    raise Exception(f"Unsupported binary parameter type: {typeoid}")

        logger.debug("Bind params: %s", params)
        num_result_formats = buf.read_int16()
        result_formats = []
        for i in range(num_result_formats):
            result_formats.append(buf.read_int16())
        ctx.add_portal(portal, stmt, params, result_formats)
        self.send_bind_complete()

    def handle_describe(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling describe")
        ba = bytearray(payload)
        describe_type = ba[0]
        query_result = None
        if describe_type == ord("P"):
            portal = ba[1 : len(ba) - 1].decode("utf-8")
            try:
                query_result = ctx.describe_portal(portal)
            except Exception as e:
                self.send_error(e, ctx)
                return
        elif describe_type == ord("S"):
            stmt = ba[1 : len(ba) - 1].decode("utf-8")
            try:
                query_result = ctx.describe_statement(stmt)
            except Exception as e:
                self.send_error(e, ctx)
                return
            param_oids = ctx.stmts[stmt][1]
            self.send_parameter_description(param_oids)
        else:
            raise Exception(f"Unknown describe type: {describe_type}")
        if query_result.has_results():
            self.send_row_description(query_result)
        else:
            self.send_no_data()

    def handle_execute(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling execute")
        if ctx.has_error:
            logger.info("Skipping execute due to previous error")
            return
        ba = bytearray(payload)
        portal_idx = ba.index(NULL_BYTE)
        portal = ba[:portal_idx].decode("utf-8")
        limit = struct.unpack("!i", ba[portal_idx + 1 : portal_idx + 5])[0]
        query_result = None
        try:
            query_result = ctx.execute_portal(portal)
        except Exception as e:
            self.send_error(e, ctx)
            return
        if query_result.has_results():
            row_count = self.send_data_rows(query_result, limit)
            if limit == 0 or row_count < limit:
                self.send_command_complete("SELECT %d\x00" % row_count)
            else:
                ctx.result_cache[portal] = query_result
                self.send_portal_suspend()
        else:
            status = query_result.status()
            self.send_command_complete(f"{status}\x00")

    def handle_close(self, ctx: BVContext, payload: bytes):
        logger.debug("Handling close")
        close_type = payload[0]
        if close_type == ord("S"):
            ctx.close_statement(payload[1:-1].decode("utf-8"))
        elif close_type == ord("P"):
            ctx.close_portal(payload[1:-1].decode("utf-8"))
        else:
            raise Exception(f"Unknown close type: {close_type}")
        self.send_close_complete()

    def send_parameter_description(self, param_oids: List[int]):
        buf = BVBuffer()
        for oid in param_oids:
            buf.write_int32(oid)
        out = buf.get_value()
        sig = struct.pack(
            "!cih",
            ServerResponse.PARAMETER_DESCRIPTION,
            len(out) + 6,
            len(param_oids),
        )
        self.wfile.write(sig + out)

    def send_row_description(self, query_result: QueryResult):
        buf = BVBuffer()
        for i in range(query_result.column_count()):
            name, bvtype = query_result.column(i)
            oid = query_result.column_type_oid(i)
            if oid is None:
                oid = BVTYPE_TO_PGTYPE.get(bvtype, PG_UNKNOWN)[0]
            buf.write_string(name)
            fmt = query_result.result_format[i] if query_result.result_format else 0
            buf.write_bytes(struct.pack("!ihihih", 0, 0, oid, 0, -1, fmt))
        out = buf.get_value()
        sig = struct.pack(
            "!cih",
            ServerResponse.ROW_DESCRIPTION,
            len(out) + 6,
            query_result.column_count(),
        )
        self.wfile.write(sig + out)

    # Rows accumulated per wfile.write() call: bounded (never the whole result, so a large scan
    # still streams incrementally as query_result.rows() lazily produces rows — see
    # ProvisaQueryResult's own streaming docstring), but large enough that per-write Python/BufferedWriter
    # call overhead is paid once per _DATA_ROW_CHUNK_SIZE rows instead of once per row. Profiled
    # live (80M-row order_items scan): the previous per-row write() cadence measured 3.29us/row;
    # this chunking measured 3.06us/row on a 16-column synthetic batch, ~1.4x over the original
    # per-row-BytesIO implementation this replaced.
    _DATA_ROW_CHUNK_SIZE = 500

    def send_data_rows(self, query_result: QueryResult, limit: int = 0) -> int:
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
        for row in query_result.rows():
            if isinstance(row, RawDataRowBytes):
                # Already a complete, wire-framed DataRow message (see RawDataRowBytes) — forward
                # verbatim, skipping the per-column converter loop entirely.
                chunk += row
                cnt += 1
                chunk_n += 1
                if chunk_n >= self._DATA_ROW_CHUNK_SIZE:
                    self.wfile.write(bytes(chunk))
                    chunk.clear()
                    chunk_n = 0
                if limit > 0 and cnt >= limit:
                    break
                continue
            # Precompiled struct.Struct + one bytearray per row (not a fresh BVBuffer/BytesIO
            # object per row, and not one struct.pack call per int32 field) — profiled to cut
            # server-side row-encoding cost roughly in half on its own.
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
            chunk += struct.pack("!cih", ServerResponse.DATA_ROW, len(row_buf) + 6, ncols)
            chunk += row_buf
            cnt += 1
            chunk_n += 1
            if chunk_n >= self._DATA_ROW_CHUNK_SIZE:
                self.wfile.write(bytes(chunk))
                chunk.clear()
                chunk_n = 0
            if limit > 0 and cnt >= limit:
                break
        if chunk_n:
            self.wfile.write(bytes(chunk))
        return cnt

    def send_error(self, exception, ctx: Optional[BVContext] = None):
        estr = str(exception)
        logger.error(estr)
        buf = BVBuffer()
        buf.write_byte(b"M")
        buf.write_string(estr)
        buf.write_byte(NULL_BYTE)
        out = buf.get_value()
        err_sig = struct.pack("!ci", ServerResponse.ERROR_RESPONSE, len(out) + 4)
        self.wfile.write(err_sig + out)
        if ctx:
            ctx.mark_error()

    def send_notice(self):
        self.wfile.write(ServerResponse.NOTICE_RESPONSE)

    def send_backend_key_data(self, ctx):
        self.wfile.write(
            struct.pack(
                "!ciII",
                ServerResponse.BACKEND_KEY_DATA,
                12,
                ctx.process_id,
                ctx.secret_key,
            )
        )

    def send_ready_for_query(self, ctx: Optional[BVContext]):
        logger.debug("Sending ready for query")
        status = ctx.transaction_status() if ctx else TransactionStatus.IDLE
        self.wfile.write(struct.pack("!cic", ServerResponse.READY_FOR_QUERY, 5, status))

    def send_parameter_status(self, params: Dict[str, str]):
        for name, value in params.items():
            buf = BVBuffer()
            buf.write_string(name)
            buf.write_string(value)
            out = buf.get_value()
            psig = struct.pack("!ci", ServerResponse.PARAMETER_STATUS, len(out) + 4)
            self.wfile.write(psig + out)

    def send_parse_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.PARSE_COMPLETE, 4))

    def send_bind_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.BIND_COMPLETE, 4))

    def send_close_complete(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.CLOSE_COMPLETE, 4))

    def send_no_data(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.NO_DATA, 4))

    def send_portal_suspend(self):
        self.wfile.write(struct.pack("!ci", ServerResponse.PORTAL_SUSPENDED, 4))

    def send_command_complete(self, tag: str):
        buf = BVBuffer()
        buf.write_bytes(struct.pack("!ci", ServerResponse.COMMAND_COMPLETE, len(tag) + 4))
        buf.write_bytes(tag.encode())
        out = buf.get_value()
        self.wfile.write(out)


class BuenaVistaServer(socketserver.ThreadingTCPServer):
    """A Python socketserver for the Buena Vista Postgres proxy."""

    allow_reuse_address = True

    def __init__(
        self,
        server_address,
        conn: Connection,
        *,
        rewriter: Optional[Rewriter] = None,
        extensions: Optional[List[Extension]] = None,
        auth: Optional[Dict[str, str]] = None,
    ):
        super().__init__(server_address, BuenaVistaHandler)
        self.conn = conn
        self.rewriter = rewriter
        self.extensions = {e.type(): e for e in extensions or []}
        self.ctxts = {}
        self.auth = auth

    def verify_request(self, request, client_address) -> bool:
        del request
        return client_address[0] == "127.0.0.1" or "BUENAVISTA_HOST" in os.environ
