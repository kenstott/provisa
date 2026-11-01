# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1de4a0-93b6-4f28-b5a1-48e0f26d7c13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL's input, output and send functions answer for a value a statement writes as a
literal (REQ-2171), where an engine holds no such value: a numeric's digits and scale, an interval's
three fields, a tsvector, an array's or range's text and binary form. A literal's answer is the same
on every engine, so it is read here, once, as postgres:17.11 reads it, and the statement carries the
answer (provisa.transpiler.pg_forms_type_io). A value in a column is the form's.

A function here takes the text and returns the answer, raises :class:`Refused` with PostgreSQL's own
error, or returns None for a text it cannot read as PostgreSQL does, which is refused by name."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import ipaddress
import math
import re
import struct
from decimal import Decimal, InvalidOperation, localcontext

_NUMERIC_DIGITS = 4
_NEGATIVE = 0x4000
_NAN = 0xC000
_POSITIVE_INFINITY = 0xD000
_NEGATIVE_INFINITY = 0xF000
_INFINITY_SCALE = 0x20


class Refused(Exception):
    """PostgreSQL's error for a literal."""

    def __init__(self, sqlstate: str, message: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.message = message


_NUMERIC_TEXT = re.compile(r"\s*([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?\s*")
_SPECIALS = {
    "nan": "NaN",
    "+nan": "NaN",
    "-nan": "NaN",
    "infinity": "Infinity",
    "+infinity": "Infinity",
    "inf": "Infinity",
    "+inf": "Infinity",
    "-infinity": "-Infinity",
    "-inf": "-Infinity",
}


def numeric_text(text: str) -> str:
    """numeric_out(numeric_in(text)): digits as written, an exponent worked through."""
    special = _SPECIALS.get(text.strip().lower())
    if special is not None:
        return special
    matched = _NUMERIC_TEXT.fullmatch(text)
    if matched is None or not (matched[2] or matched[3]):
        raise Refused("22P02", f'invalid input syntax for type numeric: "{text}"')
    sign, whole, fraction, power = matched[1], matched[2], matched[3] or "", int(matched[4] or 0)
    scale = max(len(fraction) - power, 0)
    with localcontext(prec=len(whole) + len(fraction) + abs(power) + 2):
        value = Decimal(f"{whole or 0}.{fraction or 0}").scaleb(power)
        written = f"{value:.{scale}f}"
    return f"-{written}" if sign == "-" and value != 0 else written


def numeric_bytes(text: str) -> bytes:
    """numeric_send of a numeric's text: its base 10000 digits, the first at ``weight``."""
    if text == "NaN":
        return struct.pack(">HhHH", 0, 0, _NAN, 0)
    if text in ("Infinity", "-Infinity"):
        sign = _POSITIVE_INFINITY if text == "Infinity" else _NEGATIVE_INFINITY
        return struct.pack(">HhHH", 0, 0, sign, _INFINITY_SCALE)
    negative = text.startswith("-")
    whole, _, fraction = text.lstrip("-").partition(".")
    scale = len(fraction)
    whole = whole.lstrip("0")
    groups_left = (len(whole) + _NUMERIC_DIGITS - 1) // _NUMERIC_DIGITS
    padded = whole.rjust(groups_left * _NUMERIC_DIGITS, "0") + fraction
    padded += "0" * (-len(padded) % _NUMERIC_DIGITS)
    digits = [int(padded[i : i + _NUMERIC_DIGITS]) for i in range(0, len(padded), _NUMERIC_DIGITS)]
    weight = groups_left - 1
    while digits and digits[0] == 0:
        digits.pop(0)
        weight -= 1
    while digits and digits[-1] == 0:
        digits.pop()
    if not digits:
        return struct.pack(">HhHH", 0, 0, 0, scale)
    sign = _NEGATIVE if negative else 0
    head = struct.pack(">HhHH", len(digits), weight, sign, scale)
    return head + b"".join(struct.pack(">H", digit) for digit in digits)


_UNITS = {
    "year": ("months", 12), "years": ("months", 12), "yr": ("months", 12), "yrs": ("months", 12),
    "y": ("months", 12), "mon": ("months", 1), "mons": ("months", 1), "month": ("months", 1),
    "months": ("months", 1), "day": ("days", 1), "days": ("days", 1), "d": ("days", 1),
    "week": ("days", 7), "weeks": ("days", 7), "w": ("days", 7),
    "hour": ("us", 3600000000), "hours": ("us", 3600000000), "hr": ("us", 3600000000),
    "hrs": ("us", 3600000000), "h": ("us", 3600000000), "minute": ("us", 60000000),
    "minutes": ("us", 60000000), "min": ("us", 60000000), "mins": ("us", 60000000),
    "second": ("us", 1000000), "seconds": ("us", 1000000), "sec": ("us", 1000000),
    "secs": ("us", 1000000), "s": ("us", 1000000),
}  # fmt: skip
_CLOCK = re.compile(r"([+-]?)(\d+):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?")
_ISO = re.compile(
    r"P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", re.IGNORECASE
)
_INT64_MAX = 9223372036854775807
_INT32_MAX = 2147483647
_DAY_US = 86400000000


def interval_fields(text: str) -> tuple[int, int, int] | None:
    """An interval's text as its months, days and microseconds."""
    lowered = text.strip().lower()
    if lowered == "infinity":
        return _INT32_MAX, _INT32_MAX, _INT64_MAX
    if lowered == "-infinity":
        return -_INT32_MAX - 1, -_INT32_MAX - 1, -_INT64_MAX - 1
    iso = _ISO.fullmatch(lowered.upper())
    if iso is not None and lowered.startswith("p"):
        y, mo, d, h, mi, s = (int(g or 0) for g in iso.groups())
        return y * 12 + mo, d, (h * 3600 + mi * 60 + s) * 1000000
    fields = {"months": 0, "days": 0, "us": 0}
    words = lowered.split()
    at = 0
    while at < len(words):
        clock = _CLOCK.fullmatch(words[at])
        if clock is not None:
            sign = -1 if clock[1] == "-" else 1
            seconds = int(clock[2]) * 3600 + int(clock[3]) * 60 + int(clock[4] or 0)
            micro = int((clock[5] or "").ljust(6, "0") or 0)
            fields["us"] += sign * (seconds * 1000000 + micro)
            at += 1
            continue
        if at + 1 >= len(words) or words[at + 1] not in _UNITS:
            return None
        try:
            amount = Decimal(words[at])
        except InvalidOperation:
            return None
        field, size = _UNITS[words[at + 1]]
        scaled = amount * size
        if field == "days" and scaled != scaled.to_integral_value():
            fields["us"] += int((scaled - int(scaled)) * _DAY_US)
            scaled = Decimal(int(scaled))
        if scaled != scaled.to_integral_value():
            return None
        fields[field] += int(scaled)
        at += 2
    return (fields["months"], fields["days"], fields["us"]) if words else None


def _plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def interval_text(months: int, days: int, us: int) -> str:
    """interval_out under IntervalStyle postgres."""
    if (months, days, us) == (_INT32_MAX, _INT32_MAX, _INT64_MAX):
        return "infinity"
    if (months, days, us) == (-_INT32_MAX - 1, -_INT32_MAX - 1, -_INT64_MAX - 1):
        return "-infinity"
    parts: list[str] = []
    negative = False
    years = abs(months) // 12 * (1 if months >= 0 else -1)
    rest = abs(months) % 12 * (1 if months >= 0 else -1)
    for count, word in ((years, "year"), (rest, "mon"), (days, "day")):
        if count:
            sign = "+" if negative and count > 0 else ""
            parts.append(f"{sign}{_plural(count, word)}")
            negative = count < 0
    if us or not parts:
        sign = "-" if us < 0 else ("+" if negative else "")
        seconds, micro = divmod(abs(us), 1000000)
        clock = f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"
        if micro:
            clock += f".{micro:06d}".rstrip("0")
        parts.append(f"{sign}{clock}")
    return " ".join(parts)


def interval_bytes(months: int, days: int, us: int) -> bytes:
    return struct.pack(">qii", us, days, months)


_LEXEME = re.compile(r"\s*(?:'((?:[^']|'')*)'|([^\s':]+))(?::([\d,A-Da-d]+))?")
_WEIGHTS = {"A": 3, "B": 2, "C": 1, "D": 0}
_LETTERS = {3: "A", 2: "B", 1: "C", 0: ""}


def tsvector_lexemes(text: str) -> list[tuple[str, list[tuple[int, int]]]] | None:
    """A tsvector's text as its lexemes in order, each with its (position, weight)s."""
    found: dict[str, list[tuple[int, int]]] = {}
    at = 0
    while at < len(text.rstrip()):
        matched = _LEXEME.match(text, at)
        if matched is None:
            return None
        lexeme = (matched[1] or "").replace("''", "'") if matched[2] is None else matched[2]
        places = found.setdefault(lexeme, [])
        for place in (matched[3] or "").split(",") if matched[3] else ():
            number = re.fullmatch(r"(\d+)([A-Da-d]?)", place)
            if number is None:
                return None
            places.append((int(number[1]), _WEIGHTS.get(number[2].upper(), 0)))
        at = matched.end()
    return [(k, sorted(set(found[k]))) for k in sorted(found, key=str.encode)]


def tsvector_text(lexemes: list[tuple[str, list[tuple[int, int]]]]) -> str:
    written = []
    for lexeme, places in lexemes:
        quoted = "'" + lexeme.replace("'", "''") + "'"
        at = ",".join(f"{p}{_LETTERS[w]}" for p, w in places)
        written.append(f"{quoted}:{at}" if at else quoted)
    return " ".join(written)


def tsvector_bytes(lexemes: list[tuple[str, list[tuple[int, int]]]]) -> bytes:
    out = struct.pack(">i", len(lexemes))
    for lexeme, places in lexemes:
        out += lexeme.encode() + b"\0" + struct.pack(">H", len(places))
        out += b"".join(struct.pack(">H", (w << 14) | p) for p, w in places)
    return out


def _typmod_values(text: str) -> list[str]:
    inner = text.strip()
    if not inner.startswith("{") or not inner.endswith("}") or inner == "{}":
        raise Refused("2202E", "typmod array must be one-dimensional")
    return [v.strip() for v in inner[1:-1].split(",")]


def _numeric_typmod(values: list[int]) -> int:
    if len(values) not in (1, 2):
        raise Refused("22023", "invalid NUMERIC type modifier")
    precision = values[0]
    if not 1 <= precision <= 1000:
        raise Refused("22023", f"NUMERIC precision {precision} must be between 1 and 1000")
    scale = values[1] if len(values) == 2 else 0
    if not -1000 <= scale <= 1000:
        raise Refused("22023", f"NUMERIC scale {scale} must be between -1000 and 1000")
    return ((precision << 16) | (scale & 0x7FF)) + 4


def _length_typmod(name: str, values: list[int], longest: int, header: int) -> int:
    if len(values) != 1:
        raise Refused("22023", "invalid type modifier")
    if values[0] < 1:
        raise Refused("22023", f"length for type {name} must be at least 1")
    if values[0] > longest:
        raise Refused("22023", f"length for type {name} cannot exceed {longest}")
    return values[0] + header


def _clock_typmod(name: str, zoned: str, values: list[int]) -> int:
    if len(values) != 1:
        raise Refused("22023", "invalid type modifier")
    if values[0] < 0:
        raise Refused("22023", f"{name}({values[0]}){zoned} precision must not be negative")
    return min(values[0], 6)


_LONGEST_TEXT = 10485760
_LONGEST_BITS = 83886080
_FULL_RANGE = 32767
_FULL_PRECISION = 0xFFFF
_INTERVAL_FIELDS = {
    4: "year", 2: "month", 8: "day", 1024: "hour", 2048: "minute", 4096: "second", 6: "year to month",
    1032: "day to hour", 3080: "day to minute", 7176: "day to second", 3072: "hour to minute",
    7168: "hour to second", 6144: "minute to second",
}  # fmt: skip


def _interval_typmod(values: list[int]) -> int:
    if len(values) not in (1, 2):
        raise Refused("22023", "invalid INTERVAL type modifier")
    fields = values[0]
    precision = values[1] if len(values) == 2 else _FULL_PRECISION
    if fields != _FULL_RANGE and fields not in _INTERVAL_FIELDS:
        raise Refused("22023", "invalid INTERVAL type modifier")
    if precision != _FULL_PRECISION:
        precision = min(precision, 6)
    if fields == _FULL_RANGE and precision == _FULL_PRECISION:
        return -1
    return ((fields & 0x7FFF) << 16) | (precision & 0xFFFF)


def typmod_in(stem: str, text: str) -> int:
    """A type's typmodin of its cstring[] text."""
    values = [int(v) for v in _typmod_values(text)]
    if stem == "numeric":
        return _numeric_typmod(values)
    if stem == "varchar":
        return _length_typmod("varchar", values, _LONGEST_TEXT, 4)
    if stem == "bpchar":
        return _length_typmod("char", values, _LONGEST_TEXT, 4)
    if stem in ("bit", "varbit"):
        return _length_typmod(stem, values, _LONGEST_BITS, 0)
    if stem == "interval":
        return _interval_typmod(values)
    name = "TIME" if stem.startswith("timet") or stem == "time" else "TIMESTAMP"
    if stem.startswith("timestamp"):
        name = "TIMESTAMP"
    return _clock_typmod(name, " WITH TIME ZONE" if stem.endswith("tz") else "", values)


def numeric_typmod_out(typmod: int) -> str:
    if typmod < 4:
        return ""
    bits = typmod - 4
    return f"({bits >> 16 & 0xFFFF},{((bits & 0x7FF) ^ 1024) - 1024})"


def interval_typmod_out(typmod: int) -> str:
    if typmod < 0:
        return ""
    fields = typmod >> 16 & 0x7FFF
    precision = typmod & 0xFFFF
    named = _INTERVAL_FIELDS.get(fields)
    if fields == _FULL_RANGE:
        named = ""
    if named is None:
        raise Refused("XX000", f"invalid INTERVAL typmod: {typmod:#x}")
    if precision == _FULL_PRECISION:
        return f" {named}" if named else ""
    return f" {named}({precision})" if named else f"({precision})"


def bit_bytes(text: str) -> bytes:
    size = len(text)
    padded = text.ljust((size + 7) // 8 * 8, "0")
    number = int(padded, 2).to_bytes(len(padded) // 8, "big") if padded else b""
    return struct.pack(">i", size) + number


def bit_read(text: str, typmod: int, fixed: bool) -> str:
    bad = next((c for c in text if c not in "01"), None)
    if bad is not None:
        raise Refused("22P02", f'"{bad}" is not a valid binary digit')
    if fixed and typmod >= 0 and len(text) != typmod:
        raise Refused("22026", f"bit string length {len(text)} does not match type bit({typmod})")
    if not fixed and typmod >= 0 and len(text) > typmod:
        raise Refused("22001", f"bit string too long for type bit varying({typmod})")
    return text


#: A float8 aggregate's state: its transition values, of arrays of 3 and of 6.
def _state(values: list[float], size: int, name: str) -> list[float]:
    if len(values) != size:
        raise Refused("XX000", f"{name}: expected {size}-element float8 array")
    return values


def float8_answer(name: str, values: list[float]) -> float | None:
    """A float8 aggregate's final function of its state."""
    short = name.removeprefix("float8_")
    if short in ("avg", "var_pop", "var_samp", "stddev_pop", "stddev_samp"):
        n, sx, sxx = _state(values, 3, name)
        if short == "avg":
            return None if n == 0 else sx / n
        if n == 0 or (short.endswith("samp") and n <= 1):
            return None
        variance = sxx / (n if short.endswith("pop") else n - 1)
        return math.sqrt(variance) if short.startswith("stddev") else variance
    n, sx, sxx, sy, syy, sxy = _state(values, 6, name)
    if n < 1 and short not in ():
        return None
    return {
        "regr_sxx": lambda: sxx,
        "regr_syy": lambda: syy,
        "regr_sxy": lambda: sxy,
        "regr_avgx": lambda: sx / n,
        "regr_avgy": lambda: sy / n,
        "covar_pop": lambda: sxy / n,
        "covar_samp": lambda: None if n < 2 else sxy / (n - 1),
        "corr": lambda: None if sxx == 0 or syy == 0 else sxy / math.sqrt(sxx * syy),
        "regr_r2": lambda: None if sxx == 0 else (1.0 if syy == 0 else sxy * sxy / (sxx * syy)),
        "regr_slope": lambda: None if sxx == 0 else sxy / sxx,
        "regr_intercept": lambda: None if sxx == 0 else (sy - sx * sxy / sxx) / n,
    }[short]()


def inet_bytes(text: str, is_cidr: bool) -> bytes | None:
    """inet_send and cidr_send: family, bits, whether a cidr, the address's length and its bytes."""
    try:
        network = ipaddress.ip_interface(text.strip())
    except ValueError:
        return None
    family = 2 if network.version == 4 else 3
    packed = network.ip.packed
    if "/" not in text:
        bits = len(packed) * 8
    else:
        bits = network.network.prefixlen
    return bytes([family, bits, 1 if is_cidr else 0, len(packed)]) + packed


def jsonb_bytes(text: str) -> bytes:
    """jsonb_send: its version, then the text jsonb_out writes."""
    return b"\x01" + text.encode()


_ELEMENT_OIDS = {"int": 23, "text": 25, "decimal": 1700, "boolean": 16}
_OF_OID = {23: "int", 25: "text", 1700: "decimal", 16: "boolean"}
_TRUE_WORDS = frozenset({"t", "true", "y", "yes", "on", "1"})
_FALSE_WORDS = frozenset({"f", "false", "n", "no", "off", "0"})
_ARRAY_BOUNDS = re.compile(r"\s*\[(-?\d+):(-?\d+)\]\s*=\s*")
_INTEGER_ELEMENT = re.compile(r"\s*[+-]?\d+\s*")


def _malformed(text: str) -> Refused:
    return Refused("22P02", f'malformed array literal: "{text}"')


def _elements(text: str, at: int, whole: str) -> tuple[list[object], int]:
    """A brace's items from ``at``, just past its ``{``: strings, None for NULL, lists for braces."""
    items: list[object] = []
    while True:
        while at < len(text) and text[at].isspace():
            at += 1
        if at >= len(text):
            raise _malformed(whole)
        if text[at] == "}":
            return items, at + 1
        if text[at] == "{":
            inner, at = _elements(text, at + 1, whole)
            items.append(inner)
        elif text[at] == '"':
            at += 1
            word = ""
            while at < len(text) and text[at] != '"':
                if text[at] == "\\":
                    at += 1
                word += text[at] if at < len(text) else ""
                at += 1
            if at >= len(text):
                raise _malformed(whole)
            at += 1
            items.append(word)
        else:
            start = at
            while at < len(text) and text[at] not in ",}":
                at += 1
            word = text[start:at].strip()
            items.append(None if word.upper() == "NULL" else word)
        while at < len(text) and text[at].isspace():
            at += 1
        if at < len(text) and text[at] == ",":
            at += 1
            continue
        if at < len(text) and text[at] == "}":
            continue
        raise _malformed(whole)


def _element(word: str | None, kind: str) -> str | None:
    """An item's text as its type writes it; PostgreSQL's error for one it refuses."""
    if word is None:
        return None
    if kind == "int":
        if _INTEGER_ELEMENT.fullmatch(word) is None:
            raise Refused("22P02", f'invalid input syntax for type integer: "{word}"')
        return str(int(word))
    if kind == "decimal":
        return numeric_text(word)
    if kind == "boolean":
        lowered = word.lower()
        if lowered in _TRUE_WORDS:
            return "t"
        if lowered in _FALSE_WORDS:
            return "f"
        raise Refused("22P02", f'invalid input syntax for type boolean: "{word}"')
    return word


def _shape(items: list[object]) -> list[int]:
    return [len(items), *(_shape(items[0]) if items and isinstance(items[0], list) else [])]


def _flat(items: list[object]) -> list[str | None]:
    out: list[str | None] = []
    for item in items:
        out.extend(_flat(item) if isinstance(item, list) else [item])  # type: ignore[arg-type]
    return out


def array_read(text: str, kind: str) -> tuple[list[object], list[int]]:
    """An array's text as its items, each its type's text, and each dimension's lower bound."""
    lower: list[int] = []
    body = text
    bounds = _ARRAY_BOUNDS.match(text)
    if bounds is not None:
        lower = [int(bounds[1])]
        body = text[bounds.end() :]
    stripped = body.strip()
    if not stripped.startswith("{"):
        raise _malformed(text)
    items, end = _elements(stripped, 1, text)
    if stripped[end:].strip():
        raise _malformed(text)

    def typed(values: list[object]) -> list[object]:
        return [typed(v) if isinstance(v, list) else _element(v, kind) for v in values]  # type: ignore[arg-type]

    return typed(items), lower


def _quoted_element(word: str | None) -> str:
    if word is None:
        return "NULL"
    if word == "" or word.upper() == "NULL" or re.search(r'[{},"\\\s]', word):
        return '"' + word.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return word


def array_text(items: list[object], lower: list[int]) -> str:
    def written(values: list[object]) -> str:
        return (
            "{"
            + ",".join(written(v) if isinstance(v, list) else _quoted_element(v) for v in values)  # type: ignore[arg-type]
            + "}"
        )

    head = "".join(f"[{low}:{low + size - 1}]" for low, size in zip(lower, _shape(items)))
    return f"{head}={written(items)}" if head else written(items)


def _element_bytes(word: str, kind: str) -> bytes:
    if kind == "int":
        return struct.pack(">i", int(word))
    if kind == "decimal":
        return numeric_bytes(word)
    if kind == "boolean":
        return b"\x01" if word == "t" else b"\x00"
    return word.encode()


def array_bytes(items: list[object], lower: list[int], kind: str) -> bytes:
    flat = _flat(items)
    shape = _shape(items) if items else []
    oid = _ELEMENT_OIDS[kind]
    head = struct.pack(">iii", len(shape), 1 if None in flat else 0, oid)
    for at, size in enumerate(shape):
        head += struct.pack(">ii", size, lower[at] if at < len(lower) else 1)
    body = b""
    for word in flat:
        if word is None:
            body += struct.pack(">i", -1)
        else:
            value = _element_bytes(word, kind)
            body += struct.pack(">i", len(value)) + value
    return head + body


_RANGE_TEXT = re.compile(r"\s*([\[(])\s*([^,]*?)\s*,\s*([^)\]]*?)\s*([)\]])\s*")
_EMPTY, _LOWER_INCLUDED, _UPPER_INCLUDED, _NO_LOWER, _NO_UPPER = 1, 2, 4, 8, 16
_DISCRETE = frozenset({"int4", "int8"})
_RANGE_KINDS = {3904: "int4", 3926: "int8", 3906: "num", 3912: "date"}


def range_read(text: str, kind: str) -> tuple[str | None, str | None, bool, bool] | None:
    """A range's text as its lower, upper and whether each is included; None for empty."""
    if text.strip().lower() == "empty":
        return None
    matched = _RANGE_TEXT.fullmatch(text)
    if matched is None:
        raise Refused("22P02", f'malformed range literal: "{text}"')
    low = matched[2] or None
    high = matched[3] or None
    low_in = matched[1] == "[" and low is not None
    high_in = matched[4] == "]" and high is not None
    if kind in _DISCRETE:
        if low is not None and not low_in:
            low, low_in = str(int(low) + 1), True
        if high is not None and high_in:
            high, high_in = str(int(high) + 1), False
        if low is not None and high is not None:
            if int(low) > int(high):
                raise Refused(
                    "22000", "range lower bound must be less than or equal to range upper bound"
                )
            if low == high:
                return None
    elif kind == "num" and low is not None and high is not None:
        if Decimal(low) > Decimal(high):
            raise Refused(
                "22000", "range lower bound must be less than or equal to range upper bound"
            )
    return low, high, low_in, high_in


def range_text(value: tuple[str | None, str | None, bool, bool] | None) -> str:
    if value is None:
        return "empty"
    low, high, low_in, high_in = value
    return f"{'[' if low_in else '('}{low or ''},{high or ''}{']' if high_in else ')'}"


def _bound_bytes(word: str, kind: str) -> bytes:
    if kind == "int4":
        value = struct.pack(">i", int(word))
    elif kind == "int8":
        value = struct.pack(">q", int(word))
    elif kind == "num":
        value = numeric_bytes(word)
    else:
        from datetime import date

        value = struct.pack(">i", (date.fromisoformat(word) - date(2000, 1, 1)).days)
    return struct.pack(">i", len(value)) + value


def range_bytes(value: tuple[str | None, str | None, bool, bool] | None, kind: str) -> bytes:
    if value is None:
        return bytes([_EMPTY])
    low, high, low_in, high_in = value
    flags = (
        (_LOWER_INCLUDED if low_in else 0)
        | (_UPPER_INCLUDED if high_in else 0)
        | (_NO_LOWER if low is None else 0)
        | (_NO_UPPER if high is None else 0)
    )
    out = bytes([flags])
    if low is not None:
        out += _bound_bytes(low, kind)
    if high is not None:
        out += _bound_bytes(high, kind)
    return out


_MULTIRANGE_ITEM = re.compile(r"[\[(][^\])]*[\])]")


def multirange_read(text: str, kind: str) -> list[tuple[str | None, str | None, bool, bool]]:
    stripped = text.strip()
    if not stripped.startswith("{") or not stripped.endswith("}"):
        raise Refused("22P02", f'malformed multirange literal: "{text}"')
    ranges = [range_read(m, kind) for m in _MULTIRANGE_ITEM.findall(stripped)]
    kept = sorted((r for r in ranges if r is not None), key=lambda r: int(r[0] or 0))
    merged: list[tuple[str | None, str | None, bool, bool]] = []
    for value in kept:
        if merged and value[0] is not None and merged[-1][1] is not None:
            if int(value[0]) <= int(merged[-1][1]):
                last = merged[-1]
                high = str(max(int(last[1] or 0), int(value[1] or 0)))
                merged[-1] = (last[0], high, last[2], last[3])
                continue
        merged.append(value)
    return merged


def multirange_text(values: list[tuple[str | None, str | None, bool, bool]]) -> str:
    return "{" + ",".join(range_text(v) for v in values) + "}"


def multirange_bytes(values: list[tuple[str | None, str | None, bool, bool]], kind: str) -> bytes:
    out = struct.pack(">i", len(values))
    for value in values:
        written = range_bytes(value, kind)
        out += struct.pack(">i", len(written)) + written
    return out


def record_text(values: list[str | None]) -> str:
    """record_out of a row's fields, each its type's text."""

    def written(word: str | None) -> str:
        if word is None:
            return ""
        if word == "" or re.search(r'[(),"\\\s]', word):
            return '"' + word.replace('"', '""').replace("\\", "\\\\") + '"'
        return word

    return "(" + ",".join(written(v) for v in values) + ")"
