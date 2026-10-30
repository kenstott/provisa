# Copyright (c) 2026 Kenneth Stott
# Canary: d4e7a913-6f02-4c58-b1d3-8e25f70a6c49
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's scalar functions on DuckDB (REQ-2167, PGWIRE'S CATALOG FUNCTIONS): macros where a
DuckDB expression carries the function, on any argument a statement can name, a column too; UDFs
where the value needs PostgreSQL's own algorithm (the exact sind, the bytes of a bytea, a regular
expression's groups). A UDF is strict unless it says otherwise. A numeric of no fixed scale is
returned as its text, which pgwire sends as the numeric PostgreSQL declares (cast_types)."""

# Requirements: REQ-2167

from __future__ import annotations

import functools
import json
import math
import re
import unicodedata
from decimal import Decimal
from typing import Any

from provisa.core.pg_error import END, MARK

_BACKSLASH = "\\"


def _error(sqlstate: str, message: str) -> Exception:
    """PostgreSQL's error from a UDF, as provisa.core.pg_error reads it back."""
    return ValueError(f"{MARK} {sqlstate}: {message}{END}")


# --- trigonometry in degrees, as src/backend/utils/adt/float.c computes it ------------------------

_RADIANS_PER_DEGREE = math.pi / 180.0
_SIN_30 = math.sin(30.0 * _RADIANS_PER_DEGREE)
_ONE_MINUS_COS_60 = 1.0 - math.cos(60.0 * _RADIANS_PER_DEGREE)


def _sind_0_to_30(x: float) -> float:
    return (math.sin(x * _RADIANS_PER_DEGREE) / _SIN_30) / 2.0


def _cosd_0_to_60(x: float) -> float:
    return 1.0 - ((1.0 - math.cos(x * _RADIANS_PER_DEGREE)) / _ONE_MINUS_COS_60) / 2.0


def _sind_q1(x: float) -> float:
    return _sind_0_to_30(x) if x <= 30.0 else _cosd_0_to_60(90.0 - x)


def _cosd_q1(x: float) -> float:
    return _cosd_0_to_60(x) if x <= 60.0 else _sind_0_to_30(90.0 - x)


_TAN_45 = _sind_q1(45.0) / _cosd_q1(45.0)


def sind(degrees: float) -> float:
    if math.isinf(degrees):
        raise _error("22003", "input is out of range")
    if math.isnan(degrees):
        return degrees
    sign = 1
    x = math.fmod(degrees, 360.0)
    if x < 0.0:
        x, sign = -x, -sign
    if x > 180.0:
        x, sign = x - 180.0, -sign
    if x > 90.0:
        x = 180.0 - x
    return sign * _sind_q1(x)


def tand(degrees: float) -> float:
    if math.isinf(degrees):
        raise _error("22003", "input is out of range")
    if math.isnan(degrees):
        return degrees
    sign = 1
    x = math.fmod(degrees, 360.0)
    if x < 0.0:
        x, sign = -x, -sign
    if x > 180.0:
        x, sign = 360.0 - x, -sign
    if x > 90.0:
        x, sign = 180.0 - x, -sign
    cosine = _cosd_q1(x)
    if cosine == 0.0:
        return math.inf * sign
    return ((_sind_q1(x) / cosine) / _TAN_45) * sign


# --- bytea ----------------------------------------------------------------------------------------


def _bytes_like_regex(pattern: bytes) -> re.Pattern[bytes]:
    """A LIKE pattern over bytes: % any bytes, _ one byte, a backslash escapes the next."""
    out = bytearray()
    position = 0
    while position < len(pattern):
        byte = pattern[position : position + 1]
        if byte == b"\\" and position + 1 < len(pattern):
            position += 1
            out += re.escape(pattern[position : position + 1])
        elif byte == b"%":
            out += b".*"
        elif byte == b"_":
            out += b"."
        else:
            out += re.escape(byte)
        position += 1
    return re.compile(bytes(out), re.DOTALL)


def bytea_like(value: bytes, pattern: bytes) -> bool:
    return _bytes_like_regex(pattern).fullmatch(value) is not None


def _checked_index(index: int, size: int) -> None:
    if index < 0 or index >= size:
        raise _error("2202E", f"index {index} out of valid range, 0..{size - 1}")


def get_byte(value: bytes, index: int) -> int:
    _checked_index(index, len(value))
    return value[index]


def set_byte(value: bytes, index: int, new: int) -> bytes:
    _checked_index(index, len(value))
    return value[:index] + bytes([new & 0xFF]) + value[index + 1 :]


def get_bit(value: bytes, index: int) -> int:
    _checked_index(index, len(value) * 8)
    return (value[index // 8] >> (index % 8)) & 1


def set_bit(value: bytes, index: int, new: int) -> bytes:
    _checked_index(index, len(value) * 8)
    if new not in (0, 1):
        raise _error("22023", "new bit must be 0 or 1")
    byte = value[index // 8] & ~(1 << (index % 8)) | (new << (index % 8))
    return value[: index // 8] + bytes([byte]) + value[index // 8 + 1 :]


def bit_count(value: bytes) -> int:
    return sum(byte.bit_count() for byte in value)


def bytea_substr(value: bytes, start: int, length: int | None) -> bytes:
    """substring(bytea FROM start [FOR length]): 1-based; a start before 1 uses up the length."""
    if length is not None and length < 0:
        raise _error("22011", "negative substring length not allowed")
    end = None if length is None else start + length
    begin = max(start, 1) - 1
    return value[begin:] if end is None else value[begin : max(end - 1, begin)]


def bytea_overlay(value: bytes, placing: bytes, start: int, length: int | None) -> bytes:
    skip = len(placing) if length is None else length
    if start < 1:
        raise _error("22011", "negative substring length not allowed")
    return value[: start - 1] + placing + value[start - 1 + skip :]


#: PostgreSQL's server encoding names, by the Python codec that reads them.
_ENCODINGS = {
    "UTF8": "utf-8",
    "UNICODE": "utf-8",
    "SQL_ASCII": "ascii",
    "LATIN1": "latin-1",
    "LATIN2": "iso8859-2",
    "LATIN5": "iso8859-9",
    "LATIN9": "iso8859-15",
    "WIN1250": "cp1250",
    "WIN1251": "cp1251",
    "WIN1252": "cp1252",
    "WIN1256": "cp1256",
    "EUC_JP": "euc_jp",
    "EUC_KR": "euc_kr",
    "EUC_CN": "gb2312",
    "GB18030": "gb18030",
    "GBK": "gbk",
    "BIG5": "big5",
    "SJIS": "shift_jis",
    "KOI8R": "koi8_r",
    "KOI8U": "koi8_u",
}


def _codec(name: str, role: str) -> str:
    codec = _ENCODINGS.get(re.sub(r"[^A-Za-z0-9_]", "", name).upper())
    if codec is None:
        raise _error("22023", f'invalid {role} encoding name "{name}"')
    return codec


def _decoded(value: bytes, encoding: str, codec: str) -> str:
    try:
        return value.decode(codec)
    except UnicodeDecodeError as exc:
        byte = value[exc.start : exc.start + 1].hex()
        raise _error("22021", f'invalid byte sequence for encoding "{encoding}": 0x{byte}') from exc


def convert(value: bytes, source: str, destination: str) -> bytes:
    """convert(bytea, source, destination)."""
    text = _decoded(value, source.upper(), _codec(source, "source"))
    try:
        return text.encode(_codec(destination, "destination"))
    except UnicodeEncodeError as exc:
        raise _error(
            "22P05",
            f"character with byte sequence 0x{exc.object[exc.start].encode().hex()} in encoding "
            f'"UTF8" has no equivalent in encoding "{destination.upper()}"',
        ) from exc


def encoded_length(value: bytes, encoding: str) -> int:
    return len(_decoded(value, encoding.upper(), _codec(encoding, "source")))


# --- regular expressions --------------------------------------------------------------------------


def _flags(flags: str, function: str) -> int:
    known = {"i": re.IGNORECASE, "s": re.DOTALL, "n": re.MULTILINE, "m": re.MULTILINE, "c": 0}
    out = 0
    for flag in flags:
        if flag == "g":
            raise _error("22023", f'{function}() does not support the "global" option')
        if flag not in known:
            raise _error("22023", f'invalid regular expression option: "{flag}"')
        out |= known[flag]
    return out


def _compiled(pattern: str, flags: int) -> re.Pattern[str]:
    try:
        return re.compile(pattern, flags)
    except re.error as exc:
        raise _error("2201B", f"invalid regular expression: {exc}") from exc


def regexp_match(value: str, pattern: str, flags: str) -> list[str | None] | None:
    found = _compiled(pattern, _flags(flags, "regexp_match")).search(value)
    if found is None:
        return None
    return list(found.groups()) or [found.group(0)]


def regexp_substr(  # noqa: PLR0913 -- PostgreSQL's six arguments
    value: str, pattern: str, start: int, n: int, flags: str, subexpr: int
) -> str | None:
    for name, bound, least in (("start", start, 1), ("n", n, 1), ("subexpr", subexpr, 0)):
        if bound < least:
            raise _error("22023", f'invalid value for parameter "{name}": {bound}')
    regex = _compiled(pattern, _flags(flags, "regexp_substr"))
    found = None
    matches = regex.finditer(value, start - 1)
    for _ in range(n):
        found = next(matches, None)
        if found is None:
            return None
    assert found is not None
    return found.group(subexpr) if subexpr <= regex.groups else None


def json_object(keys: list[str | None], values: list[str | None] | None) -> str:
    """json_object(text[]) -- alternating keys and values -- or json_object(keys, values)."""
    if values is None:
        if len(keys) % 2:
            raise _error("2202E", "array must have even number of elements")
        keys, values = keys[0::2], keys[1::2]
    elif len(keys) != len(values):
        raise _error("2202E", "mismatched array dimensions")
    if any(key is None for key in keys):
        raise _error("22004", "null value not allowed for object key")
    fields = (f"{json.dumps(k)} : {json.dumps(v)}" for k, v in zip(keys, values))
    return "{" + ", ".join(fields) + "}"


def regexp_split(value: str, pattern: str, flags: str) -> list[str]:
    regex = _compiled(pattern, _flags(flags, "regexp_split_to_array"))
    out: list[str] = []
    begin = 0
    position = 0
    for found in regex.finditer(value):
        if found.end() == found.start() and (found.start() == 0 or found.start() == len(value)):
            continue  # an empty match at either end splits nothing
        out.append(value[begin : found.start()])
        begin = found.end()
        position = begin
    del position
    out.append(value[begin:])
    return out


def _similar_escape(pattern: str | None, escape: str | None) -> str | None:
    """similar_escape_internal: a SIMILAR TO pattern as a POSIX regular expression."""
    if pattern is None:
        return None
    if escape is not None and len(escape) > 1:
        raise _error("2200C", "invalid escape string")
    esc = _BACKSLASH if escape is None else escape
    out = ["^(?:"]
    after_escape = in_bracket = False
    for char in pattern:
        if after_escape:
            out.append(char if char in '"' else _BACKSLASH + char)
            after_escape = False
        elif esc and char == esc:
            after_escape = True
        elif in_bracket:
            out.append(_BACKSLASH + char if char == _BACKSLASH else char)
            in_bracket = char != "]"
        elif char == "[":
            out.append(char)
            in_bracket = True
        elif char == "%":
            out.append(".*")
        elif char == "_":
            out.append(".")
        elif char == "(":
            out.append("(?:")
        elif char in (_BACKSLASH, ".", "^", "$"):
            out.append(_BACKSLASH + char)
        else:
            out.append(char)
    out.append(")$")
    return "".join(out)


def similar_substring(value: str, pattern: str, escape: str) -> str | None:
    """substring(value SIMILAR pattern ESCAPE escape): the text between the pattern's two escape
    double-quotes, or the whole match where it has none."""
    separator = f'{escape}"'
    parts = pattern.split(separator)
    if len(parts) == 3:  # noqa: PLR2004 -- before, between and after the two separators
        regex = f"({_similar_body(parts[0], escape)})({_similar_body(parts[1], escape)})({_similar_body(parts[2], escape)})"
        group = 2
    else:
        regex, group = f"({_similar_body(pattern, escape)})", 1
    found = re.fullmatch(regex, value, re.DOTALL)
    return None if found is None else found.group(group)


def _similar_body(pattern: str, escape: str) -> str:
    return str(_similar_escape(pattern, escape))[len("^(?:") : -len(")$")]


def like_escape(pattern: str, escape: str) -> str:
    """like_escape(pattern, escape): the pattern with ``escape`` as the backslash it stands for;
    an empty escape means none, so each backslash is doubled."""
    if escape == "":
        return pattern.replace(_BACKSLASH, _BACKSLASH * 2)
    if len(escape) > 1:
        raise _error("22025", "invalid escape string")
    if escape == _BACKSLASH:
        return pattern
    out: list[str] = []
    escaped = False
    for char in pattern:
        if escaped:
            out.append(char if char == escape else _BACKSLASH + char)
            escaped = False
        elif char == escape:
            escaped = True
        elif char == _BACKSLASH:
            out.append(_BACKSLASH * 2)
        else:
            out.append(char)
    return "".join(out)


def bytea_like_escape(pattern: bytes, escape: bytes) -> bytes:
    return like_escape(pattern.decode("latin-1"), escape.decode("latin-1")).encode("latin-1")


def string_to_array(value: str | None, delimiter: str | None, null: str | None) -> list | None:
    """string_to_array's semantics, which string_to_table unnests: no delimiter is each character,
    an empty one the whole string; a field equal to ``null`` is NULL."""
    if value is None:
        return None
    if value == "":
        return []
    if delimiter is None:
        fields = list(value)
    elif delimiter == "":
        fields = [value]
    else:
        fields = value.split(delimiter)
    return [None if null is not None and field == null else field for field in fields]


# --- text and unicode -----------------------------------------------------------------------------


def _unicode_form(form: str) -> str:
    upper = form.upper()
    if upper not in ("NFC", "NFD", "NFKC", "NFKD"):
        raise _error("22023", f"invalid normalization form: {form}")
    return upper


def normalize(value: str, form: str) -> str:
    return unicodedata.normalize(_unicode_form(form), value)  # type: ignore[arg-type]


def is_normalized(value: str, form: str) -> bool:
    return unicodedata.is_normalized(_unicode_form(form), value)  # type: ignore[arg-type]


def unicode_assigned(value: str) -> bool:
    return all(unicodedata.category(char) != "Cn" for char in value)


_UNISTR_ESCAPE = re.compile(
    r"\\(?:\\|\+([0-9A-Fa-f]{6})|u([0-9A-Fa-f]{4})|U([0-9A-Fa-f]{8})|([0-9A-Fa-f]{4}))"
)


def unistr(value: str) -> str:
    """unistr(text): each \\XXXX, \\+XXXXXX, \\uXXXX, \\UXXXXXXXX and \\\\ read."""
    out: list[str] = []
    position = 0
    while position < len(value):
        char = value[position]
        if char != _BACKSLASH:
            out.append(char)
            position += 1
            continue
        found = _UNISTR_ESCAPE.match(value, position)
        if found is None:
            raise _error("22P02", "invalid Unicode escape")
        digits = next((g for g in found.groups() if g is not None), None)
        out.append(_BACKSLASH if digits is None else chr(int(digits, 16)))
        position = found.end()
    return "".join(out)


def to_ascii(value: str, encoding: str) -> str:
    """to_ascii(text, encoding): a LATIN1, LATIN2 or WIN1250 string's letters without accents,
    any other non-ASCII character a space."""
    if encoding.upper() not in ("LATIN1", "LATIN2", "WIN1250"):
        raise _error("0A000", f"encoding conversion from {encoding.upper()} to ASCII not supported")
    plain = unicodedata.normalize("NFKD", value)
    kept = (c for c in plain if not unicodedata.combining(c))
    return "".join(c if ord(c) < 128 else " " for c in kept)  # noqa: PLR2004 -- ASCII's range


def parse_ident(value: str, strict: bool) -> list[str]:
    """parse_ident(str, strict): its dot-separated parts, as PostgreSQL's parser reads them."""
    invalid = _error("22P02", f'string is not a valid identifier: "{value}"')
    parts: list[str] = []
    position = 0
    while True:
        while position < len(value) and value[position].isspace():
            position += 1
        if position < len(value) and value[position] == '"':
            end = position + 1
            chars: list[str] = []
            while True:
                if end >= len(value):
                    raise _error(
                        "22P02",
                        f'string is not a valid identifier: "{value}"',
                    )
                if value[end] == '"':
                    if value[end + 1 : end + 2] != '"':
                        break
                    end += 1
                chars.append(value[end])
                end += 1
            if not chars:
                raise invalid
            parts.append("".join(chars))
            position = end + 1
        else:
            end = position
            while end < len(value) and (
                value[end].isalnum() or value[end] in "_$" or ord(value[end]) >= 128  # noqa: PLR2004
            ):
                end += 1
            if end == position:
                raise invalid
            parts.append(value[position:end].lower())
            position = end
        while position < len(value) and value[position].isspace():
            position += 1
        if position >= len(value):
            return parts
        if value[position] != ".":
            if strict:
                raise invalid
            return parts
        position += 1


_SIZE_UNITS = {
    "bytes": 0, "b": 0, "kb": 10, "mb": 20, "gb": 30, "tb": 40, "pb": 50,
}  # fmt: skip
_SIZE = re.compile(r"\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)\s*(.*?)\s*")


def size_bytes(value: str) -> int:
    """pg_size_bytes(text), by dbsize.c: a number and an optional unit."""
    found = _SIZE.fullmatch(value)
    if found is None:
        raise _error("22023", f'invalid size: "{value}"')
    unit = found[2]
    bits = 0 if unit == "" else _SIZE_UNITS.get(unit.lower())
    if bits is None:
        raise _error("22023", f'invalid size: "{value}"')
    return int(Decimal(found[1]) * (1 << bits))


# --- numeric, as text -----------------------------------------------------------------------------


def _decimal_pair(a: str, b: str) -> tuple[int, int, int]:
    """Two numerics as integers of their larger scale, and that scale."""
    x, y = Decimal(a), Decimal(b)
    scale = max(-x.as_tuple().exponent, -y.as_tuple().exponent, 0)  # type: ignore[operator]
    return int(x.scaleb(scale)), int(y.scaleb(scale)), scale


def _scaled(value: int, scale: int) -> str:
    return str(Decimal(value).scaleb(-scale).quantize(Decimal(1).scaleb(-scale)))


def numeric_gcd(a: str, b: str) -> str:
    x, y, scale = _decimal_pair(a, b)
    return _scaled(math.gcd(x, y), scale)


def numeric_lcm(a: str, b: str) -> str:
    x, y, scale = _decimal_pair(a, b)
    return _scaled(math.lcm(x, y), scale)


def numeric_factorial(n: int) -> str:
    if n < 0:
        raise _error("22003", "factorial of a negative number is undefined")
    return str(math.factorial(n))


def numeric_series(start: str, stop: str, step: str | None) -> list[str]:
    """generate_series over numerics, of the larger scale of its bounds and step."""
    first, last, by = Decimal(start), Decimal(stop), Decimal(1 if step is None else step)
    if by == 0:
        raise _error("22023", "step size cannot equal zero")
    exponent = min(first.as_tuple().exponent, last.as_tuple().exponent, by.as_tuple().exponent, 0)  # type: ignore[type-var]
    out: list[str] = []
    value = first
    while (value <= last) if by > 0 else (value >= last):
        out.append(str(value.quantize(Decimal(1).scaleb(exponent))))  # type: ignore[arg-type]
        value += by
    return out


def to_base(value: int, bits: int, base: int) -> str:
    """to_bin and to_oct: the value's two's complement in ``bits`` bits, in base 2 or 8."""
    return format(value & ((1 << bits) - 1), "b" if base == 2 else "o")  # noqa: PLR2004


def abbrev(value: str, kind: str) -> str:
    """abbrev(inet) drops a /32 (or /128); abbrev(cidr) also drops the zero bytes the mask covers."""
    address, _, bits = value.partition("/")
    if kind != "cidr":
        full = 128 if ":" in address else 32
        return address if bits in ("", str(full)) else value
    if ":" in address or not bits:
        return value
    octets = address.split(".")
    keep = max(math.ceil(int(bits) / 8), 1)
    return ".".join(octets[:keep]) + "/" + bits


# --- the registrations ----------------------------------------------------------------------------

_UDFS: tuple[tuple[str, Any, list[str], str], ...] = (
    ("provisa_sind", sind, ["DOUBLE"], "DOUBLE"),
    ("provisa_tand", tand, ["DOUBLE"], "DOUBLE"),
    ("provisa_bytea_like", bytea_like, ["BLOB", "BLOB"], "BOOLEAN"),
    ("provisa_get_byte", get_byte, ["BLOB", "BIGINT"], "INTEGER"),
    ("provisa_set_byte", set_byte, ["BLOB", "BIGINT", "BIGINT"], "BLOB"),
    ("provisa_get_bit", get_bit, ["BLOB", "BIGINT"], "INTEGER"),
    ("provisa_set_bit", set_bit, ["BLOB", "BIGINT", "BIGINT"], "BLOB"),
    ("provisa_bit_count", bit_count, ["BLOB"], "BIGINT"),
    ("provisa_convert", convert, ["BLOB", "VARCHAR", "VARCHAR"], "BLOB"),
    ("provisa_encoded_length", encoded_length, ["BLOB", "VARCHAR"], "INTEGER"),
    ("provisa_similar_substring", similar_substring, ["VARCHAR"] * 3, "VARCHAR"),
    ("provisa_regexp_split", regexp_split, ["VARCHAR"] * 3, "VARCHAR[]"),
    ("provisa_like_escape", like_escape, ["VARCHAR", "VARCHAR"], "VARCHAR"),
    ("provisa_bytea_like_escape", bytea_like_escape, ["BLOB", "BLOB"], "BLOB"),
    ("provisa_normalize", normalize, ["VARCHAR", "VARCHAR"], "VARCHAR"),
    ("provisa_is_normalized", is_normalized, ["VARCHAR", "VARCHAR"], "BOOLEAN"),
    ("provisa_unicode_assigned", unicode_assigned, ["VARCHAR"], "BOOLEAN"),
    ("provisa_unistr", unistr, ["VARCHAR"], "VARCHAR"),
    ("provisa_to_ascii", to_ascii, ["VARCHAR", "VARCHAR"], "VARCHAR"),
    ("provisa_parse_ident", parse_ident, ["VARCHAR", "BOOLEAN"], "VARCHAR[]"),
    ("provisa_size_bytes", size_bytes, ["VARCHAR"], "BIGINT"),
    ("provisa_numeric_gcd", numeric_gcd, ["VARCHAR", "VARCHAR"], "VARCHAR"),
    ("provisa_numeric_lcm", numeric_lcm, ["VARCHAR", "VARCHAR"], "VARCHAR"),
    ("provisa_factorial", numeric_factorial, ["BIGINT"], "VARCHAR"),
    ("provisa_to_base", to_base, ["BIGINT", "BIGINT", "BIGINT"], "VARCHAR"),
    ("provisa_abbrev", abbrev, ["VARCHAR", "VARCHAR"], "VARCHAR"),
)

#: Strict UDFs that answer NULL themselves.
_NULL_ANSWERING_UDFS: tuple[tuple[str, Any, list[str], str], ...] = (
    ("provisa_regexp_match", regexp_match, ["VARCHAR"] * 3, "VARCHAR[]"),
    (
        "provisa_regexp_substr",
        regexp_substr,
        ["VARCHAR", "VARCHAR", "BIGINT", "BIGINT", "VARCHAR", "BIGINT"],
        "VARCHAR",
    ),
)

#: UDFs that see a NULL argument: PostgreSQL's function is not strict in it.
_NON_STRICT_UDFS: tuple[tuple[str, Any, list[str], str], ...] = (
    ("provisa_similar_escape", _similar_escape, ["VARCHAR", "VARCHAR"], "VARCHAR"),
    ("provisa_string_to_array", string_to_array, ["VARCHAR"] * 3, "VARCHAR[]"),
)
#: A strict UDF over three arguments, one optional (NULL standing for its absence).
_OPTIONAL_LAST_UDFS: tuple[tuple[str, Any, list[str], str], ...] = (
    ("provisa_bytea_substr", bytea_substr, ["BLOB", "BIGINT", "BIGINT"], "BLOB"),
    ("provisa_bytea_overlay", bytea_overlay, ["BLOB", "BLOB", "BIGINT", "BIGINT"], "BLOB"),
    ("provisa_numeric_series", numeric_series, ["VARCHAR"] * 3, "VARCHAR[]"),
    ("provisa_json_object", json_object, ["VARCHAR[]", "VARCHAR[]"], "VARCHAR"),
)


def _text_matches() -> tuple[str, ...]:
    like = "(a LIKE b ESCAPE '\\')"
    ilike = "(a ILIKE b ESCAPE '\\')"
    regex = "regexp_matches(a, b)"
    iregex = "regexp_matches(a, b, 'i')"
    family: dict[str, str] = {
        "like": like,
        "nlike": f"NOT {like}",
        "iclike": ilike,
        "icnlike": f"NOT {ilike}",
        "regexeq": regex,
        "regexne": f"NOT {regex}",
        "icregexeq": iregex,
        "icregexne": f"NOT {iregex}",
    }
    return tuple(
        f"CREATE MACRO {prefix}{suffix}(a, b) AS {body}"
        for prefix in ("text", "name", "bpchar")
        for suffix, body in family.items()
    )


def _comparisons(prefix: str, *suffixes: str) -> tuple[str, ...]:
    operators = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
    return tuple(
        f"CREATE MACRO {prefix}{suffix}(a, b) AS a {operators[suffix]} b" for suffix in suffixes
    )


#: oid_send: an oid's four bytes, for the send functions of the object identifier types.
_OID_BYTES = "unhex(printf('%08x', CAST(v AS BIGINT)))"
_INT8_BYTES = "unhex(printf('%016x', CAST(v AS BIGINT)))"

#: quote_literal: an E'' string, its backslashes doubled, when it holds one.
_QUOTED_LITERAL = (
    "CAST(CASE "
    f"WHEN contains(CAST(value AS VARCHAR), '{_BACKSLASH}') THEN 'E''' || "
    f"replace(replace(CAST(value AS VARCHAR), '{_BACKSLASH}', '{_BACKSLASH * 2}'), '''', '''''') "
    "|| '''' ELSE '''' || replace(CAST(value AS VARCHAR), '''', '''''') || '''' END AS VARCHAR)"
)

_MACROS: tuple[str, ...] = (
    *_text_matches(),
    """CREATE MACRO "like"(a, b) AS (a LIKE b ESCAPE '\\')""",
    """CREATE MACRO notlike(a, b) AS NOT (a LIKE b ESCAPE '\\')""",
    "CREATE MACRO textcat(a, b) AS CAST(a || b AS VARCHAR)",
    "CREATE MACRO byteacat(a, b) AS CAST(a || b AS BLOB)",
    "CREATE MACRO bytealike(a, b) AS provisa_bytea_like(a, b)",
    "CREATE MACRO byteanlike(a, b) AS NOT provisa_bytea_like(a, b)",
    "CREATE MACRO bitcat(a, b) AS CAST(CAST(a AS VARCHAR) || CAST(b AS VARCHAR) AS BIT)",
    "CREATE MACRO bitshiftleft(value, n) AS CAST(substr(CAST(value AS VARCHAR), n + 1) || "
    "repeat('0', least(n, length(CAST(value AS VARCHAR)))) AS BIT)",
    "CREATE MACRO bitshiftright(value, n) AS CAST(repeat('0', least(n, "
    "length(CAST(value AS VARCHAR)))) || left(CAST(value AS VARCHAR), "
    "greatest(length(CAST(value AS VARCHAR)) - n, 0)) AS BIT)",
    "CREATE MACRO get_byte(value, n) AS provisa_get_byte(value, n)",
    "CREATE MACRO set_byte(value, n, new) AS provisa_set_byte(value, n, new)",
    f"CREATE MACRO quote_literal(value) AS {_QUOTED_LITERAL}",
    "CREATE MACRO quote_nullable(value) AS CASE WHEN value IS NULL THEN 'NULL' "
    f"ELSE {_QUOTED_LITERAL} END",
    "CREATE MACRO dcbrt(x) AS cbrt(x)",
    "CREATE MACRO dround(x) AS round_even(CAST(x AS DOUBLE), 0)",
    "CREATE MACRO dtrunc(x) AS trunc(CAST(x AS DOUBLE))",
    "CREATE MACRO sind(x) AS provisa_sind(x)",
    "CREATE MACRO tand(x) AS provisa_tand(x)",
    "CREATE MACRO every(x) AS bool_and(x)",
    "CREATE MACRO scale(x) AS CASE WHEN x IS NULL THEN CAST(NULL AS INTEGER) "
    "WHEN contains(CAST(x AS VARCHAR), '.') "
    "THEN CAST(length(split_part(CAST(x AS VARCHAR), '.', 2)) AS INTEGER) ELSE 0 END",
    "CREATE MACRO to_bin(value) AS provisa_to_base(value, "
    "CASE WHEN typeof(value) IN ('BIGINT', 'HUGEINT') THEN 64 ELSE 32 END, 2)",
    "CREATE MACRO to_oct(value) AS provisa_to_base(value, "
    "CASE WHEN typeof(value) IN ('BIGINT', 'HUGEINT') THEN 64 ELSE 32 END, 8)",
    "CREATE MACRO pg_size_bytes(size) AS provisa_size_bytes(size)",
    "CREATE MACRO pg_convert(value, source, destination) AS "
    "provisa_convert(value, source, destination)",
    "CREATE MACRO pg_json_object(value) AS provisa_json_object(value, NULL), "
    "(ks, vs) AS CASE WHEN ks IS NOT NULL AND vs IS NOT NULL THEN provisa_json_object(ks, vs) END",
    "CREATE MACRO convert_from(value, source) AS decode(provisa_convert(value, source, 'UTF8'))",
    "CREATE MACRO convert_to(value, destination) AS "
    "provisa_convert(encode(value), 'UTF8', destination)",
    "CREATE MACRO regexp_match(value, pattern) AS provisa_regexp_match(value, pattern, ''), "
    "(value, pattern, flags) AS provisa_regexp_match(value, pattern, flags)",
    "CREATE MACRO regexp_substr(value, pattern) AS "
    "provisa_regexp_substr(value, pattern, 1, 1, '', 0), "
    "(value, pattern, start) AS provisa_regexp_substr(value, pattern, start, 1, '', 0), "
    "(value, pattern, start, n) AS provisa_regexp_substr(value, pattern, start, n, '', 0), "
    "(value, pattern, start, n, flags) AS "
    "provisa_regexp_substr(value, pattern, start, n, flags, 0), "
    "(value, pattern, start, n, flags, subexpr) AS "
    "provisa_regexp_substr(value, pattern, start, n, flags, subexpr)",
    "CREATE MACRO string_to_table(value, delimiter) AS "
    "unnest(provisa_string_to_array(value, delimiter, NULL)), "
    "(value, delimiter, null_string) AS "
    "unnest(provisa_string_to_array(value, delimiter, null_string))",
    "CREATE MACRO similar_escape(pattern, escape) AS provisa_similar_escape(pattern, escape)",
    "CREATE MACRO similar_to_escape(pattern) AS CASE WHEN pattern IS NOT NULL THEN "
    "provisa_similar_escape(pattern, NULL) END, (pattern, escape) AS "
    "CASE WHEN pattern IS NOT NULL AND escape IS NOT NULL THEN "
    "provisa_similar_escape(pattern, escape) END",
    "CREATE MACRO is_normalized(value, form) AS provisa_is_normalized(value, form)",
    "CREATE MACRO unicode_assigned(value) AS provisa_unicode_assigned(value)",
    "CREATE MACRO unicode_version() AS '15.1'",
    "CREATE MACRO icu_unicode_version() AS CAST(NULL AS VARCHAR)",
    "CREATE MACRO unistr(value) AS provisa_unistr(value)",
    # The database's encoding is UTF8, which to_ascii refuses; a named single-byte one it strips.
    "CREATE MACRO to_ascii(value) AS provisa_to_ascii(value, 'UTF8'), "
    "(value, encoding) AS provisa_to_ascii(value, CAST(encoding AS VARCHAR))",
    "CREATE MACRO parse_ident(value) AS provisa_parse_ident(value, true), "
    "(value, strict) AS provisa_parse_ident(value, strict)",
    "CREATE MACRO abbrev(value) AS "
    "provisa_abbrev(CAST(value AS VARCHAR), CASE typeof(value) WHEN 'INET' THEN 'inet' "
    "ELSE 'cidr' END)",
    "CREATE MACRO nameconcatoid(name, oid) AS "
    "CAST(left(name, 63 - length('_' || oid)) || '_' || oid AS VARCHAR)",
    "CREATE MACRO jsonb_typeof(value) AS CASE json_type(value) WHEN 'OBJECT' THEN 'object' "
    "WHEN 'ARRAY' THEN 'array' WHEN 'VARCHAR' THEN 'string' WHEN 'BOOLEAN' THEN 'boolean' "
    "WHEN 'NULL' THEN 'null' WHEN NULL THEN NULL ELSE 'number' END",
    *_comparisons("oidvector", "eq", "ne", "lt", "le", "gt", "ge"),
    *_comparisons("xid8", "eq", "ne", "lt", "le", "gt", "ge"),
    "CREATE MACRO xid8cmp(a, b) AS CASE WHEN a < b THEN -1 WHEN a > b THEN 1 WHEN a = b THEN 0 END",
    "CREATE MACRO xid8_larger(a, b) AS CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN "
    "greatest(a, b) END",
    "CREATE MACRO xid8_smaller(a, b) AS CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN "
    "least(a, b) END",
    f"CREATE MACRO xid8send(v) AS {_INT8_BYTES}",
    *(
        f"CREATE MACRO {name}send(v) AS {_OID_BYTES}"
        for name in (
            "regclass",
            "regcollation",
            "regconfig",
            "regdictionary",
            "regnamespace",
            "regoper",
            "regoperator",
            "regprocedure",
            "regproc",
            "regrole",
            "regtype",
        )
    ),
)


def _strict_in_all(function: Any) -> Any:
    @functools.wraps(function)
    def call(*args: Any) -> Any:
        return None if any(a is None for a in args) else function(*args)

    return call


def _null_for_a_required_argument(function: Any) -> Any:
    """``function``, answering NULL where any argument but the last, its optional one, is NULL."""

    @functools.wraps(function)  # its signature is what DuckDB counts the parameters by
    def call(*args: Any) -> Any:
        return None if any(a is None for a in args[:-1]) else function(*args)

    return call


def register(con: Any) -> None:
    """Create the UDFs and macros on DuckDB connection ``con``."""
    for name, function, parameters, returns in _UDFS:
        con.create_function(name, function, parameters, returns, side_effects=False)
    special = (
        *_NON_STRICT_UDFS,
        *(
            (name, _null_for_a_required_argument(function), parameters, returns)
            for name, function, parameters, returns in _OPTIONAL_LAST_UDFS
        ),
        *(
            (name, _strict_in_all(function), parameters, returns)
            for name, function, parameters, returns in _NULL_ANSWERING_UDFS
        ),
    )
    for name, function, parameters, returns in special:
        con.create_function(
            name, function, parameters, returns, null_handling="special", side_effects=False
        )
    for macro in _MACROS:
        con.execute(macro)
