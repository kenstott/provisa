# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7c1a96-0b35-4d28-8f61-c2d97e3a5b10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SQL of pg_catalog's Type operators, I/O and casts family, for the rows of
provisa.transpiler.function_macros: a pattern over a type list is rows generated once. A :class:`Form`
is one overload; ``strict`` says PostgreSQL returns NULL for a NULL argument, so function_macros
wraps the answer as it wraps any strict row, and ``answer`` is SQL over its parameters in DuckDB's
dialect. A type the engine does not have is held in PostgreSQL's text form, so a form first reads
that text (``_in`` of its type) as PostgreSQL's input function reads it, refusals included."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from dataclasses import dataclass

from provisa.core.pg_error import raising


@dataclass(frozen=True)
class Form:
    """One overload: ``returns`` is the DuckDB type of a NULL of its result."""

    name: str
    params: str
    returns: str
    answer: str
    strict: bool = True
    held: bool = False  # its arguments are read as their text, whatever the statement casts them to


_ORDER = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_DIVISION_BY_ZERO = raising("22012", "'division by zero'")


def _fails(sqlstate: str, message: str) -> str:
    """A refusal whose message is a constant."""
    return raising(sqlstate, "'" + message.replace("'", "''") + "'")


def _order(stem: str, read: str = "{}", held: bool = False, ne: str = "ne") -> list[Form]:
    """``stem``'s six comparison functions, each operand read by ``read``."""
    a, b = read.replace("{}", "a"), read.replace("{}", "b")
    return [
        Form(f"{stem}{ne if name == 'ne' else name}", "a, b", "BOOLEAN", f"{a} {op} {b}", held=held)
        for name, op in _ORDER.items()
    ]


def _compare(a: str, b: str) -> str:
    """A cmp function's answer."""
    return f"CASE WHEN {a} < {b} THEN -1 WHEN {a} > {b} THEN 1 ELSE 0 END"


# --- the integers -------------------------------------------------------------------------------

#: name, SQL type, PostgreSQL's name of it, the wider type its arithmetic is checked in, its bits.
_INTEGERS = (
    ("int2", "SMALLINT", "smallint", "BIGINT", 16),
    ("int4", "INTEGER", "integer", "BIGINT", 32),
    ("int8", "BIGINT", "bigint", "HUGEINT", 64),
)


def _in_range(value: str, sql_type: str, pg_name: str) -> str:
    """``value`` as the integer type, or PostgreSQL's refusal of one it does not fit."""
    return (
        f"CASE WHEN try_cast({value} AS {sql_type}) IS NULL "
        f"THEN {_fails('22003', f'{pg_name} out of range')} "
        f"ELSE try_cast({value} AS {sql_type}) END"
    )


def _shifted(sql_type: str, bits: int) -> tuple[str, str]:
    """int2shl and int2shr, int4's, int8's: C's shift of the promoted operand. Its count is taken
    modulo the promoted width, an int2's being 32, and its result cut to the type's bits."""
    width = 64 if bits == 64 else 32  # noqa: PLR2004 -- an int8, and what C promotes the rest to
    count = f"(CAST(b AS HUGEINT) & {width - 1})"
    unsigned = f"(CAST(a AS HUGEINT) & {(1 << bits) - 1})"
    kept = f"({unsigned} & ((CAST(1 AS HUGEINT) << greatest({bits} - {count}, 0)) - 1))"
    left = f"CASE WHEN {count} >= {bits} THEN CAST(0 AS HUGEINT) ELSE {kept} << {count} END"
    signed = (
        f"CAST(CASE WHEN ({left}) >= {1 << (bits - 1)} THEN ({left}) - {1 << bits} "
        f"ELSE ({left}) END AS {sql_type})"
    )
    return signed, f"CAST(CAST(a AS HUGEINT) >> {count} AS {sql_type})"


def _integer(stem: str, sql_type: str, pg_name: str, wide: str, bits: int) -> list[Form]:
    a, b = f"CAST(CAST(a AS {sql_type}) AS {wide})", f"CAST(CAST(b AS {sql_type}) AS {wide})"

    def checked(value: str) -> str:
        return _in_range(value, sql_type, pg_name)

    shl, shr = _shifted(sql_type, bits)
    own_a, own_b = f"CAST(a AS {sql_type})", f"CAST(b AS {sql_type})"
    return [
        *_order(stem, f"CAST({{}} AS {sql_type})"),
        Form(f"{stem}pl", "a, b", sql_type, checked(f"{a} + {b}")),
        Form(f"{stem}mi", "a, b", sql_type, checked(f"{a} - {b}")),
        Form(f"{stem}mul", "a, b", sql_type, checked(f"{a} * {b}")),
        Form(
            f"{stem}div",
            "a, b",
            sql_type,
            f"CASE WHEN {b} = 0 THEN {_DIVISION_BY_ZERO} ELSE {checked(f'{a} // {b}')} END",
        ),
        Form(
            f"{stem}mod",
            "a, b",
            sql_type,
            f"CASE WHEN {b} = 0 THEN {_DIVISION_BY_ZERO} ELSE CAST({a} % {b} AS {sql_type}) END",
        ),
        Form(f"{stem}um", "a", sql_type, checked(f"-{a}")),
        Form(f"{stem}up", "a", sql_type, own_a),
        Form(f"{stem}abs", "a", sql_type, checked(f"abs({a})")),
        Form(f"{stem}and", "a, b", sql_type, f"{own_a} & {own_b}"),
        Form(f"{stem}or", "a, b", sql_type, f"{own_a} | {own_b}"),
        Form(f"{stem}xor", "a, b", sql_type, f"xor({own_a}, {own_b})"),
        Form(f"{stem}not", "a", sql_type, f"~{own_a}"),
        Form(f"{stem}shl", "a, b", sql_type, shl),
        Form(f"{stem}shr", "a, b", sql_type, shr),
        Form(f"{stem}larger", "a, b", sql_type, f"greatest({own_a}, {own_b})"),
        Form(f"{stem}smaller", "a, b", sql_type, f"least({own_a}, {own_b})"),
    ]


#: The transition functions of integer sums: not strict, a NULL state or value is passed over.
def _sum(name: str, state_type: str, value: str, added: str) -> Form:
    answer = (
        f"CASE WHEN a IS NULL THEN {value} WHEN b IS NULL THEN CAST(a AS {state_type}) "
        f"ELSE {added} END"
    )
    return Form(name, "a, b", state_type, answer, strict=False)


_BIGINT_WRAP = (
    "CAST(((CAST(a AS HUGEINT) + b + 9223372036854775808) % 18446744073709551616 + "
    "18446744073709551616) % 18446744073709551616 - 9223372036854775808 AS BIGINT)"
)
_INTEGER_FORMS = (
    *(form for row in _INTEGERS for form in _integer(*row)),
    _sum("int2_sum", "BIGINT", "CAST(b AS BIGINT)", _BIGINT_WRAP),
    _sum("int4_sum", "BIGINT", "CAST(b AS BIGINT)", _BIGINT_WRAP),
    Form("int4inc", "a", "INTEGER", _in_range("CAST(a AS BIGINT) + 1", "INTEGER", "integer")),
    Form("int8inc", "a", "BIGINT", _in_range("CAST(a AS HUGEINT) + 1", "BIGINT", "bigint")),
)


# --- the floats ---------------------------------------------------------------------------------

_OVERFLOW = _fails("22003", "value out of range: overflow")
_UNDERFLOW = _fails("22003", "value out of range: underflow")
_FLOATS = (("float4", "FLOAT"), ("float8", "DOUBLE"))


def _finite_inputs(sql_type: str) -> str:
    return f"NOT isinf(CAST(a AS {sql_type})) AND NOT isinf(CAST(b AS {sql_type}))"


def _float_checked(value: str, sql_type: str, zero_ok: str) -> str:
    """float.c's check: an infinity from finite operands overflowed; a zero, where ``zero_ok`` does
    not hold, underflowed."""
    return (
        f"list_transform([{value}], _v -> CASE WHEN isinf(_v) AND {_finite_inputs(sql_type)} "
        f"THEN {_OVERFLOW} WHEN _v = 0 AND NOT ({zero_ok}) THEN {_UNDERFLOW} ELSE _v END)[1]"
    )


def _float(stem: str, sql_type: str) -> list[Form]:
    a, b = f"CAST(a AS {sql_type})", f"CAST(b AS {sql_type})"
    return [
        *_order(stem, f"CAST({{}} AS {sql_type})"),
        Form(f"{stem}pl", "a, b", sql_type, _float_checked(f"{a} + {b}", sql_type, "true")),
        Form(f"{stem}mi", "a, b", sql_type, _float_checked(f"{a} - {b}", sql_type, "true")),
        Form(
            f"{stem}mul",
            "a, b",
            sql_type,
            _float_checked(f"{a} * {b}", sql_type, f"{a} = 0 OR {b} = 0"),
        ),
        Form(
            f"{stem}div",
            "a, b",
            sql_type,
            f"CASE WHEN {b} = 0 AND NOT isnan({a}) THEN {_DIVISION_BY_ZERO} ELSE "
            f"{_float_checked(f'{a} / {b}', sql_type, f'{a} = 0 OR isinf({b})')} END",
        ),
        Form(f"{stem}abs", "a", sql_type, f"abs({a})"),
        Form(f"{stem}um", "a", sql_type, f"-{a}"),
        Form(f"{stem}up", "a", sql_type, a),
        Form(f"{stem}larger", "a, b", sql_type, f"greatest({a}, {b})"),
        Form(f"{stem}smaller", "a, b", sql_type, f"least({a}, {b})"),
    ]


_FLOAT_FORMS = tuple(form for stem, sql_type in _FLOATS for form in _float(stem, sql_type))

# --- the types a comparison is all there is to ----------------------------------------------------


def _refused_input(type_name: str, value: str = "_t") -> str:
    """What an input function says of text that is no value of its type."""
    return raising("22P02", f"'invalid input syntax for type {type_name}: \"' || {value} || '\"'")


def _bound(value: str, answer: str, name: str = "_t") -> str:
    """``answer`` with ``name`` bound to ``value``: a form names its value more than once."""
    return f"list_transform([{value}], {name} -> {answer})[1]"


def _text(value: str) -> str:
    return f"CAST({value} AS VARCHAR)"


def _colons(digits: str) -> str:
    """Hex digits as a MAC address writes them."""
    return f"rtrim(regexp_replace({digits}, '(..)', '\\1:', 'g'), ':')"


def _read_mac(value: str) -> str:
    """A macaddr's text as macaddr_out writes it, read as macaddr_in reads its six forms."""
    hexes = "[0-9A-Fa-f]"
    pair, four, six = f"{hexes}{{2}}", f"{hexes}{{4}}", f"{hexes}{{6}}"
    forms = "|".join(
        (
            f"(?:{pair}:){{5}}{pair}",
            f"(?:{pair}-){{5}}{pair}",
            f"{six}[:-]{six}",
            f"{four}[.]{four}[.]{four}",
            f"{four}-{four}-{four}",
            f"{six}{six}",
        )
    )
    digits = "lower(regexp_replace(trim(_t), '[^0-9A-Fa-f]', '', 'g'))"
    return _bound(
        _text(value),
        f"CASE WHEN NOT regexp_full_match(trim(_t), '{forms}') THEN {_refused_input('macaddr')} "
        f"ELSE {_colons(digits)} END",
    )


def _read_mac8(value: str) -> str:
    """A macaddr8's text as macaddr8_out writes it: six bytes gain the ff:fe of EUI-64."""
    pair = "[0-9A-Fa-f]{2}"
    form = f"(?:{pair}[:.-]?){{5,7}}{pair}"
    digits = "lower(regexp_replace(trim(_t), '[:.-]', '', 'g'))"
    eight = (
        f"CASE WHEN length({digits}) = 12 THEN left({digits}, 6) || 'fffe' || right({digits}, 6) "
    )
    return _bound(
        _text(value),
        f"CASE WHEN NOT regexp_full_match(trim(_t), '{form}') OR length({digits}) NOT IN (12, 16) "
        f"THEN {_refused_input('macaddr8')} ELSE "
        f"{_colons(f'{eight}ELSE {digits} END')} END",
    )


def _read_lsn(value: str) -> str:
    """A pg_lsn's text as its position."""
    hexes = "[0-9A-Fa-f]{1,8}"
    number = (
        "(CAST('0x' || split_part(_t, '/', 1) AS UBIGINT)::HUGEINT * 4294967296 + "
        "CAST('0x' || split_part(_t, '/', 2) AS UBIGINT))"
    )
    return _bound(
        _text(value),
        f"CASE WHEN NOT regexp_full_match(_t, '{hexes}/{hexes}') THEN {_refused_input('pg_lsn')} "
        f"ELSE {number} END",
    )


def _lsn_text(position: str) -> str:
    return _bound(
        position,
        "upper(printf('%x/%x', CAST(_p // 4294967296 AS UBIGINT), "
        "CAST(_p % 4294967296 AS UBIGINT)))",
        "_p",
    )


def _read_tid(value: str) -> str:
    """A tid's text as [block, offset]."""
    return _bound(
        _text(value),
        f"CASE WHEN NOT regexp_full_match(trim(_t), '[(] *[0-9]+ *, *[0-9]+ *[)]') "
        f"THEN {_refused_input('tid')} ELSE list_transform(regexp_extract_all(_t, '[0-9]+'), "
        "_n -> CAST(_n AS BIGINT)) END",
    )


def _read_unsigned(value: str, type_name: str, limit: int) -> str:
    """An xid's, a cid's or an oid's text, as uint32in_subr or uint64in_subr reads it."""
    return _bound(
        _text(value),
        f"CASE WHEN try_cast(trim(_t) AS HUGEINT) IS NULL THEN {_refused_input(type_name)} "
        f"WHEN CAST(trim(_t) AS HUGEINT) >= {limit} OR "
        f"CAST(trim(_t) AS HUGEINT) < -{limit // 2} THEN "
        + raising("22003", f"'value \"' || _t || '\" is out of range for type {type_name}'")
        + f" ELSE (CAST(trim(_t) AS HUGEINT) + {limit}) % {limit} END",
    )


def _tid_text(blocks: str) -> str:
    return _bound(blocks, "'(' || _b[1] || ',' || _b[2] || ')'", "_b")


_UINT32 = 4294967296
#: The default collation PostgreSQL's catalog names: the engine's plain comparison is by byte.
_COLLATED = 'CAST({} AS VARCHAR) COLLATE "en_US"'
_BPCHAR = "rtrim(CAST({} AS VARCHAR), ' ') COLLATE \"en_US\""
_BPCHAR_BYTES = "rtrim(CAST({} AS VARCHAR), ' ')"
_NAME = "left(CAST({} AS VARCHAR), 63)"
_TEXT_BYTES = "CAST({} AS VARCHAR)"


_COMPARED = {"lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_EQUAL = {"eq": "=", "ne": "<>"}


def _collated(stem: str, equal: str, ordered: str, separator: str = "") -> list[Form]:
    """Equal by byte, ordered by the database's collation."""
    a, b = equal.replace("{}", "a"), equal.replace("{}", "b")
    c, d = ordered.replace("{}", "a"), ordered.replace("{}", "b")
    return [
        *(Form(f"{stem}{name}", "a, b", "BOOLEAN", f"{a} {op} {b}") for name, op in _EQUAL.items()),
        *(
            Form(f"{stem}{separator}{name}", "a, b", "BOOLEAN", f"{c} {op} {d}")
            for name, op in _COMPARED.items()
        ),
    ]


def _cross(left: str, right: str, a: str, b: str) -> list[Form]:
    """``left``_eq_``right`` and its kin, each operand brought to their common type."""
    return [
        *(
            Form(f"{left}_{name}_{right}", "a, b", "BOOLEAN", f"{a} {op} {b}")
            for name, op in _ORDER.items()
        ),
        Form(f"{left}_cmp_{right}", "a, b", "INTEGER", _compare(a, b)),
    ]


def _name_text(stem: str, other: str, a: str, b: str) -> list[Form]:
    """nameeqtext and texteqname and their kin: by byte, as a name's collation is C."""
    return [
        Form(f"{stem}{name}{other}", "a, b", "BOOLEAN", f"{a} {op} {b}")
        for name, op in _ORDER.items()
    ]


#: type stem, how an operand reads, what it is held as: a cmp, and an order of its values.
_DATE = "CAST({} AS DATE)"
_TIME = "CAST({} AS TIME)"
_TIMESTAMP = "CAST({} AS TIMESTAMP)"
_TIMESTAMPTZ = "CAST({} AS TIMESTAMPTZ)"
_INTERVAL = "CAST({} AS INTERVAL)"
_UUID = "CAST(CAST({} AS UUID) AS VARCHAR)"
_BIT = "CAST({} AS VARCHAR)"
#: timetz_cmp_internal: the true time, then the zone, west of Greenwich being the greater.
_TIMETZ = (
    "[date_part('hour', CAST({} AS TIMETZ)) * 3600000000 + "
    "date_part('minute', CAST({} AS TIMETZ)) * 60000000 + "
    "date_part('microsecond', CAST({} AS TIMETZ)) - "
    "date_part('timezone', CAST({} AS TIMETZ)) * 1000000, "
    "-date_part('timezone', CAST({} AS TIMETZ))]"
)
_ORDERED: tuple[tuple[str, str, str, bool], ...] = (
    ("date_", _DATE, "DATE", False),
    ("time_", _TIME, "TIME", False),
    ("timetz_", _TIMETZ, "TIMETZ", False),
    ("timestamp_", _TIMESTAMP, "TIMESTAMP", False),
    ("timestamptz_", _TIMESTAMPTZ, "TIMESTAMPTZ", False),
    ("interval_", _INTERVAL, "INTERVAL", False),
    ("macaddr_", _read_mac("{}"), "VARCHAR", True),
    ("macaddr8_", _read_mac8("{}"), "VARCHAR", True),
    ("uuid_", _UUID, "UUID", False),
    ("pg_lsn_", _read_lsn("{}"), "VARCHAR", True),
)


def _ordered(stem: str, read: str, returns: str, held: bool) -> list[Form]:
    a, b = read.replace("{}", "a"), read.replace("{}", "b")
    return [
        *_order(stem, read, held),
        Form(f"{stem}cmp", "a, b", "INTEGER", _compare(a, b), held=held),
    ]


def _cmp(name: str, read: str, held: bool = False) -> Form:
    a, b = read.replace("{}", "a"), read.replace("{}", "b")
    return Form(name, "a, b", "INTEGER", _compare(a, b), held=held)


_READ_OID = _read_unsigned("{}", "oid", _UINT32)
_SCALARS: tuple[Form, ...] = (
    *_order("bool", "CAST({} AS BOOLEAN)"),
    *_order("bytea", "CAST({} AS BLOB)"),
    _cmp("byteacmp", "CAST({} AS BLOB)"),
    *_collated("text", _TEXT_BYTES, _COLLATED, "_"),
    *_order("text_pattern_", _TEXT_BYTES)[2:],
    *_collated("bpchar", _BPCHAR_BYTES, _BPCHAR),
    _cmp("bpcharcmp", _BPCHAR),
    *_order("bpchar_pattern_", _BPCHAR_BYTES)[2:],
    *_order("name", _NAME),
    *_name_text("name", "text", _NAME.replace("{}", "a"), _TEXT_BYTES.replace("{}", "b")),
    *_name_text("text", "name", _TEXT_BYTES.replace("{}", "a"), _NAME.replace("{}", "b")),
    *(form for row in _ORDERED for form in _ordered(*row)),
    *_order("tid", _read_tid("{}"), True),
    *_order("oid", _READ_OID, True),
    *_order("bit", _BIT),
    _cmp("bitcmp", _BIT),
    *_order("varbit", _BIT),
    _cmp("varbitcmp", _BIT),
    *_cross("date", "timestamp", _TIMESTAMP.replace("{}", "a"), _TIMESTAMP.replace("{}", "b")),
    *_cross(
        "date", "timestamptz", _TIMESTAMPTZ.replace("{}", "a"), _TIMESTAMPTZ.replace("{}", "b")
    ),
    *_cross("timestamp", "date", _TIMESTAMP.replace("{}", "a"), _TIMESTAMP.replace("{}", "b")),
    *_cross(
        "timestamp", "timestamptz", _TIMESTAMPTZ.replace("{}", "a"), _TIMESTAMPTZ.replace("{}", "b")
    ),
    *_cross(
        "timestamptz", "date", _TIMESTAMPTZ.replace("{}", "a"), _TIMESTAMPTZ.replace("{}", "b")
    ),
    *_cross(
        "timestamptz", "timestamp", _TIMESTAMPTZ.replace("{}", "a"), _TIMESTAMPTZ.replace("{}", "b")
    ),
)


def _pick(name: str, read: str, returns: str, shown: str, held: bool = False) -> list[Form]:
    """``name``larger and ``name``smaller, written with the ``{}`` of the word: the first operand
    answers only where it is strictly the one asked for, as timestamp_larger and its kin answer."""
    a, b = read.replace("{}", "a"), read.replace("{}", "b")
    first, second = shown.replace("{}", "a"), shown.replace("{}", "b")
    return [
        Form(
            name.format(word),
            "a, b",
            returns,
            f"CASE WHEN {a} {op} {b} THEN {first} ELSE {second} END",
            held=held,
        )
        for word, op in (("larger", ">"), ("smaller", "<"))
    ]


_PICKS: tuple[Form, ...] = (
    *_pick("bpchar_{}", _BPCHAR, "VARCHAR", _TEXT_BYTES),
    *_pick("text_{}", _COLLATED, "VARCHAR", _TEXT_BYTES),
    *_pick("date_{}", _DATE, "DATE", _DATE),
    *_pick("time_{}", _TIME, "TIME", _TIME),
    *_pick("timetz_{}", _TIMETZ, "TIMETZ", "CAST({} AS TIMETZ)"),
    *_pick("timestamp_{}", _TIMESTAMP, "TIMESTAMP", _TIMESTAMP),
    *_pick("timestamptz_{}", _TIMESTAMPTZ, "TIMESTAMPTZ", _TIMESTAMPTZ),
    *_pick("interval_{}", _INTERVAL, "INTERVAL", _INTERVAL),
    *_pick("pg_lsn_{}", _read_lsn("{}"), "VARCHAR", _lsn_text(_read_lsn("{}")), True),
    *_pick("oid{}", _READ_OID, "BIGINT", _READ_OID, True),
    *_pick("tid{}", _read_tid("{}"), "VARCHAR", _tid_text(_read_tid("{}")), True),
)


# --- dates and times ----------------------------------------------------------------------------

_DAY_US = 86400000000
_FIELDS = (
    "datepart('year', {i}) * 12 + datepart('month', {i})",
    "datepart('day', {i})",
    "datepart('hour', {i}) * 3600000000 + datepart('minute', {i}) * 60000000 + "
    "datepart('microsecond', {i})",
)


def _months(value: str) -> str:
    return _FIELDS[0].format(i=value)


def _days(value: str) -> str:
    return _FIELDS[1].format(i=value)


def _micros(value: str) -> str:
    return _FIELDS[2].format(i=value)


def _interval_of(months: str, days: str, micros: str) -> str:
    return (
        f"to_months(CAST({months} AS INTEGER)) + to_days(CAST({days} AS INTEGER)) + "
        f"to_microseconds(CAST({micros} AS BIGINT))"
    )


def _timestamp_difference(a: str, b: str, kind: str) -> str:
    """timestamp_mi: days and time, never months, the days justified out of the time."""
    refused = _fails("22008", "cannot subtract infinite timestamps")
    difference = f"epoch_us({a}) - epoch_us({b})"
    return (
        f"CASE WHEN NOT isfinite({a}) OR NOT isfinite({b}) THEN {refused} ELSE "
        + _bound(
            difference,
            f"to_days(CAST(_d // {_DAY_US} AS INTEGER)) + to_microseconds(_d % {_DAY_US})",
            "_d",
        )
        + " END"
    )


def _moved(value: str, sql_type: str, interval: str, sign: str) -> str:
    """A timestamp moved by an interval; an infinite one stays where it is."""
    return (
        f"CASE WHEN isfinite(CAST({value} AS {sql_type})) THEN CAST({value} AS {sql_type}) "
        f"{sign} CAST({interval} AS INTERVAL) ELSE CAST({value} AS {sql_type}) END"
    )


def _time_moved(value: str, interval: str, sign: str, sql_type: str) -> str:
    return (
        f"CAST({value} AS {sql_type}) {sign} to_microseconds({_micros(_cast_interval(interval))})"
    )


def _cast_interval(value: str) -> str:
    return f"CAST({value} AS INTERVAL)"


_DATE_RANGE = _fails("22008", "date out of range")
_INFINITE_DATES = _fails("22008", "cannot subtract infinite dates")
_A_DATE, _B_DATE = "CAST(a AS DATE)", "CAST(b AS DATE)"
_DATE_PLUS = (
    "CASE WHEN NOT isfinite({d}) THEN {d} ELSE "
    "CAST(try_cast({d} + CAST({n} AS INTEGER) AS DATE) AS DATE) END"
)
_NEGATED = _interval_of(
    f"-({_months('i')})",
    f"-({_days('i')})",
    f"-({_micros('i')})",
)


def _scaled(factor: str) -> str:
    """interval_mul's fields of ``i`` times ``_f``: each field cut to whole, the remainder of the
    months given to the days at 30 a month and of the days to the time at 24 hours a day."""
    months, days, micros = f"({_months('i')})", f"({_days('i')})", f"({_micros('i')})"
    round_us = "round({} * 1000000) / 1000000"
    month_rest = round_us.format(f"(({months} * _f - trunc({months} * _f)) * 30)")
    second_rest = round_us.format(f"({days} * _f - trunc({days} * _f) + _r - trunc(_r)) * 86400")
    answer = _interval_of(
        f"trunc({months} * _f)",
        f"trunc({days} * _f) + trunc(_r) + trunc(_s / 86400)",
        f"round({micros} * _f + (_s - trunc(_s / 86400) * 86400) * 1000000)",
    )
    return _bound(
        factor,
        _bound(month_rest, _bound(second_rest, answer, "_s"), "_r"),
        "_f",
    )


_INTERVAL_I = "CAST(a AS INTERVAL)"
_FLOAT_B = "CAST(b AS DOUBLE)"
_DATES: tuple[Form, ...] = (
    Form(
        "date_mi",
        "a, b",
        "INTEGER",
        f"CASE WHEN NOT isfinite({_A_DATE}) OR NOT isfinite({_B_DATE}) THEN {_INFINITE_DATES} "
        f"ELSE CAST({_A_DATE} - {_B_DATE} AS INTEGER) END",
    ),
    Form("date_pli", "a, b", "DATE", _DATE_PLUS.format(d=_A_DATE, n="b")),
    Form("date_mii", "a, b", "DATE", _DATE_PLUS.format(d=_A_DATE, n="-CAST(b AS INTEGER)")),
    Form("date_pl_interval", "a, b", "TIMESTAMP", _moved("a", "TIMESTAMP", "b", "+")),
    Form("date_mi_interval", "a, b", "TIMESTAMP", _moved("a", "TIMESTAMP", "b", "-")),
    Form("interval_pl_date", "a, b", "TIMESTAMP", _moved("b", "TIMESTAMP", "a", "+")),
    Form("timestamp_pl_interval", "a, b", "TIMESTAMP", _moved("a", "TIMESTAMP", "b", "+")),
    Form("timestamp_mi_interval", "a, b", "TIMESTAMP", _moved("a", "TIMESTAMP", "b", "-")),
    Form("interval_pl_timestamp", "a, b", "TIMESTAMP", _moved("b", "TIMESTAMP", "a", "+")),
    Form("timestamptz_pl_interval", "a, b", "TIMESTAMPTZ", _moved("a", "TIMESTAMPTZ", "b", "+")),
    Form("timestamptz_mi_interval", "a, b", "TIMESTAMPTZ", _moved("a", "TIMESTAMPTZ", "b", "-")),
    Form("interval_pl_timestamptz", "a, b", "TIMESTAMPTZ", _moved("b", "TIMESTAMPTZ", "a", "+")),
    Form("date_subtract", "a, b", "TIMESTAMPTZ", _moved("a", "TIMESTAMPTZ", "b", "-")),
    Form(
        "timestamp_mi",
        "a, b",
        "INTERVAL",
        _timestamp_difference("CAST(a AS TIMESTAMP)", "CAST(b AS TIMESTAMP)", "timestamp"),
    ),
    Form(
        "timestamptz_mi",
        "a, b",
        "INTERVAL",
        _timestamp_difference("CAST(a AS TIMESTAMPTZ)", "CAST(b AS TIMESTAMPTZ)", "timestamp"),
    ),
    Form("time_pl_interval", "a, b", "TIME", _time_moved("a", "b", "+", "TIME")),
    Form("time_mi_interval", "a, b", "TIME", _time_moved("a", "b", "-", "TIME")),
    Form("interval_pl_time", "a, b", "TIME", _time_moved("b", "a", "+", "TIME")),
    Form("timetz_pl_interval", "a, b", "TIMETZ", _time_moved("a", "b", "+", "TIMETZ")),
    Form("timetz_mi_interval", "a, b", "TIMETZ", _time_moved("a", "b", "-", "TIMETZ")),
    Form("interval_pl_timetz", "a, b", "TIMETZ", _time_moved("b", "a", "+", "TIMETZ")),
    Form(
        "time_mi_time",
        "a, b",
        "INTERVAL",
        "to_microseconds(CAST(epoch_us(TIMESTAMP '2000-01-01' + CAST(a AS TIME)) - "
        "epoch_us(TIMESTAMP '2000-01-01' + CAST(b AS TIME)) AS BIGINT))",
    ),
    Form(
        "interval_pl",
        "a, b",
        "INTERVAL",
        _interval_of(
            f"({_months(_INTERVAL_I)}) + ({_months('CAST(b AS INTERVAL)')})",
            f"({_days(_INTERVAL_I)}) + ({_days('CAST(b AS INTERVAL)')})",
            f"({_micros(_INTERVAL_I)}) + ({_micros('CAST(b AS INTERVAL)')})",
        ),
    ),
    Form(
        "interval_mi",
        "a, b",
        "INTERVAL",
        _interval_of(
            f"({_months(_INTERVAL_I)}) - ({_months('CAST(b AS INTERVAL)')})",
            f"({_days(_INTERVAL_I)}) - ({_days('CAST(b AS INTERVAL)')})",
            f"({_micros(_INTERVAL_I)}) - ({_micros('CAST(b AS INTERVAL)')})",
        ),
    ),
    Form("interval_um", "a", "INTERVAL", _bound(_INTERVAL_I, _NEGATED, "i")),
    Form("interval_mul", "a, b", "INTERVAL", _bound(_INTERVAL_I, _scaled(_FLOAT_B), "i")),
    Form(
        "interval_div",
        "a, b",
        "INTERVAL",
        f"CASE WHEN {_FLOAT_B} = 0 THEN {_DIVISION_BY_ZERO} ELSE "
        + _bound(_INTERVAL_I, _scaled(f"1.0 / {_FLOAT_B}"), "i")
        + " END",
    ),
)

FORMS: tuple[Form, ...] = (*_INTEGER_FORMS, *_FLOAT_FORMS, *_SCALARS, *_PICKS, *_DATES)

#: The rows whose arguments are the text of a type the engine does not have.
HELD = frozenset(form.name for form in FORMS if form.held)
