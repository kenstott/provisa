# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7c1a96-0b35-4d28-8f61-c2d97e3a5b10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The generated rule of pg_catalog's operator-named functions, for the rows of
provisa.transpiler.function_macros (REQ-2171, A REWRITE IS A SQLGLOT EXPANSION, ALWAYS POSTGRESQL
TO THE TARGET). A function named for an operator is that operator, from a table of types and the
patterns of their names: int4pl(a, b) is a + b. Each :class:`Form` is one overload in PostgreSQL's
syntax, which sqlglot renders for every dialect; nothing here is listed per dialect. A form states
its casts, so a dialect's division or rounding cannot change the answer, and checks its range, so
PostgreSQL's refusal is raised."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from dataclasses import dataclass

from provisa.core.pg_error import END, MARK


@dataclass(frozen=True)
class Form:
    """One overload: its name, its parameters and its form."""

    name: str
    params: str
    sql: str

    @property
    def raises(self) -> bool:
        """Whether the form raises PostgreSQL's error: only a dialect whose cast checks can."""
        return MARK in self.sql


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def refused(sqlstate: str, message: str, sql_type: str = "INTEGER") -> str:
    """PostgreSQL's error ``sqlstate`` with the SQL text ``message``, as a value of ``sql_type``:
    the cast of a text that is no number, which any dialect that checks a cast raises, and which
    provisa.core.pg_error.from_engine reads back."""
    text = f"{_quoted(f'{MARK} {sqlstate}: ')} || {message} || {_quoted(END)}"
    return f"CAST(CAST({text} AS INTEGER) AS {sql_type})"


def fails(sqlstate: str, message: str, sql_type: str = "INTEGER") -> str:
    """A refusal whose message is a constant."""
    return refused(sqlstate, _quoted(message), sql_type)


_fails = fails


#: Ends the refusal of a value a form cannot answer as PostgreSQL does.
REFUSED = "does not read this value"


def not_served(function: str, sql_type: str) -> str:
    """A value this form cannot answer as PostgreSQL does: refused by name, not answered."""
    return _fails("0A000", f"{function} {REFUSED}", sql_type)


_not_served = not_served


def rint(value: str) -> str:
    """float8's rint, half to even: ROUND is half away from zero on most dialects."""
    x = f"CAST({value} AS DOUBLE PRECISION)"
    half = f"{x} - FLOOR({x}) = 0.5"
    return f"CASE WHEN {half} THEN 2 * ROUND({x} / 2) ELSE ROUND({x}) END"


def _bound(value: str, answer: str, name: str) -> str:
    """``answer`` with ``name`` holding ``value``: a form that needs a value twice."""
    return f"(SELECT {answer} FROM (SELECT {value} AS {name}) AS b{name})"


def _reads(read: str, *params: str) -> tuple[str, ...]:
    return tuple(read.replace("{}", p) for p in params)


def _before(value: str, separator: str) -> str:
    """``value`` up to ``separator``; all of it where there is none."""
    at = f"STRPOS({value}, '{separator}')"
    return f"CASE WHEN {at} > 0 THEN LEFT({value}, {at} - 1) WHEN {at} = 0 THEN {value} END"


def _after(value: str, separator: str) -> str:
    """``value`` past ``separator``; nothing where there is none."""
    at = f"STRPOS({value}, '{separator}')"
    return f"CASE WHEN {at} > 0 THEN SUBSTR({value}, {at} + 1) WHEN {at} = 0 THEN '' END"


_ORDER = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_DIVISION_BY_ZERO = "division by zero"


def _compare(a: str, b: str) -> str:
    """A cmp function: NULL for a NULL argument, as it is strict."""
    return f"CASE WHEN {a} < {b} THEN -1 WHEN {a} > {b} THEN 1 WHEN {a} = {b} THEN 0 END"


def _strict(sql_type: str) -> str:
    """The first branch of a CASE of a form over a and b that refuses a value: a strict function
    answers a NULL argument before it looks at the other."""
    return f"WHEN a IS NULL OR b IS NULL THEN CAST(NULL AS {sql_type}) "


def _larger(a: str, b: str, first: str, second: str) -> str:
    return f"CASE WHEN {a} > {b} THEN {first} WHEN {a} <= {b} THEN {second} END"


def _smaller(a: str, b: str, first: str, second: str) -> str:
    return f"CASE WHEN {a} < {b} THEN {first} WHEN {a} >= {b} THEN {second} END"


@dataclass(frozen=True)
class _Ordered:
    """A type's comparison functions: ``stem`` and a pattern's name, each operand read by ``read``.
    ``shown`` is what larger and smaller answer, where that is not what was compared."""

    stem: str
    read: str
    cmp: bool = False
    picks: bool = False
    shown: str | None = None
    ne: str = "ne"

    def forms(self) -> list[Form]:
        a, b = _reads(self.read, "a", "b")
        first, second = _reads(self.shown or self.read, "a", "b")
        forms = [
            Form(f"{self.stem}{self.ne if name == 'ne' else name}", "a, b", f"{a} {op} {b}")
            for name, op in _ORDER.items()
        ]
        if self.cmp:
            forms.append(Form(f"{self.stem}cmp", "a, b", _compare(a, b)))
        if self.picks:
            forms.append(Form(f"{self.stem}larger", "a, b", _larger(a, b, first, second)))
            forms.append(Form(f"{self.stem}smaller", "a, b", _smaller(a, b, first, second)))
        return forms


_TEXT = "CAST({} AS TEXT)"
_COLLATED = "CAST({} AS TEXT) COLLATE 'en_US'"
_BPCHAR = "RTRIM(CAST({} AS TEXT), ' ')"
_BPCHAR_COLLATED = "RTRIM(CAST({} AS TEXT), ' ') COLLATE 'en_US'"
_NAME = "LEFT(CAST({} AS TEXT), 63)"
_DATE = "CAST({} AS DATE)"
_TIME = "CAST({} AS TIME)"
_TIMETZ = "CAST({} AS TIMETZ)"
_TIMESTAMP = "CAST({} AS TIMESTAMP)"
_TIMESTAMPTZ = "CAST({} AS TIMESTAMPTZ)"
_INTERVAL = "CAST({} AS INTERVAL)"
_UUID = "CAST({} AS UUID)"
_BYTEA = "CAST({} AS BYTEA)"
_NUMERIC = "CAST({} AS DECIMAL(38, 18))"


def _hex_of(value: str) -> str:
    return f"LOWER(REGEXP_REPLACE(CAST({value} AS TEXT), '[^0-9A-Fa-f]', '', 'g'))"


def _lsn_key(value: str) -> str:
    """A pg_lsn as digits that order as its position does."""
    text = f"CAST({value} AS TEXT)"
    return f"LPAD(UPPER({_before(text, '/')}), 8, '0') || LPAD(UPPER({_after(text, '/')}), 8, '0')"


def _lsn_number(value: str) -> str:
    """A pg_lsn's position: its digits' values, of any dialect's arithmetic."""
    digits = _lsn_key(value)
    terms = " + ".join(
        f"(STRPOS('0123456789ABCDEF', SUBSTR(_k, {i + 1}, 1)) - 1) * "
        f"CAST({16 ** (15 - i)} AS DECIMAL(38, 0))"
        for i in range(16)
    )
    return _bound(digits, f"CAST({terms} AS DECIMAL(38, 0))", "_k")


def _lsn_text(number: str) -> str:
    """A position, rounded, as pg_lsn_out writes it, or the refusal of one past pg_lsn's range."""
    low = "(_n % 4294967296)"
    high = f"CAST((_n - {low}) / 4294967296 AS BIGINT)"
    return _bound(
        f"ROUND({number})",
        f"CASE WHEN _n < 0 OR _n >= 18446744073709551616 "
        f"THEN {_fails('22023', 'pg_lsn out of range', 'TEXT')} "
        f"ELSE UPPER(HEX({high})) || '/' || UPPER(HEX(CAST({low} AS BIGINT))) END",
        "_n",
    )


def _tid_part(value: str, part: int) -> str:
    pair = f"TRIM(BOTH '()' FROM CAST({value} AS TEXT))"
    return f"CAST({_before(pair, ',') if part == 1 else _after(pair, ',')} AS BIGINT)"


def _tid_key(value: str) -> str:
    """A tid's block and offset as one number that orders as the pair does."""
    return f"({_tid_part(value, 1)} * 65536 + {_tid_part(value, 2)})"


_ORDERED: tuple[_Ordered, ...] = (
    _Ordered("int2", "CAST({} AS SMALLINT)", picks=True),
    _Ordered("int4", "CAST({} AS INTEGER)", picks=True),
    _Ordered("int8", "CAST({} AS BIGINT)", picks=True),
    _Ordered("float4", "CAST({} AS REAL)", picks=True),
    _Ordered("float8", "CAST({} AS DOUBLE PRECISION)", picks=True),
    _Ordered("numeric_", _NUMERIC, cmp=True, picks=True, shown=_TEXT),
    _Ordered("bool", "CAST({} AS BOOLEAN)"),
    _Ordered("bytea", _BYTEA, cmp=True),
    _Ordered("char", _TEXT),
    _Ordered("oid", "CAST({} AS BIGINT)", picks=True),
    _Ordered("bit", _TEXT, cmp=True),
    _Ordered("varbit", _TEXT, cmp=True),
    _Ordered("date_", _DATE, cmp=True, picks=True),
    _Ordered("time_", _TIME, cmp=True, picks=True),
    _Ordered("timetz_", _TIMETZ, cmp=True, picks=True),
    _Ordered("timestamp_", _TIMESTAMP, cmp=True, picks=True),
    _Ordered("timestamptz_", _TIMESTAMPTZ, cmp=True, picks=True),
    _Ordered("interval_", _INTERVAL, cmp=True, picks=True),
    _Ordered("uuid_", _UUID, cmp=True),
    _Ordered("macaddr_", _hex_of("{}"), cmp=True),
    _Ordered("macaddr8_", _hex_of("{}"), cmp=True),
    _Ordered("pg_lsn_", _lsn_key("{}"), cmp=True, picks=True, shown=_lsn_text(_lsn_number("{}"))),
    _Ordered("tid", _tid_key("{}"), picks=True, shown=_TEXT),
    _Ordered("name", _NAME),
)


def _text_orders() -> list[Form]:
    """The string types: equal by bytes, ordered by the database's collation, or by bytes where a
    name says pattern; a name's own collation is C."""
    a, b = _reads(_TEXT, "a", "b")
    x, y = _reads(_COLLATED, "a", "b")
    trimmed_a, trimmed_b = _reads(_BPCHAR, "a", "b")
    padded_a, padded_b = _reads(_BPCHAR_COLLATED, "a", "b")
    name_a, name_b = _reads(_NAME, "a", "b")
    ordered = {k: v for k, v in _ORDER.items() if k not in ("eq", "ne")}
    return [
        Form("texteq", "a, b", f"{a} = {b}"),
        Form("textne", "a, b", f"{a} <> {b}"),
        Form("bpchareq", "a, b", f"{trimmed_a} = {trimmed_b}"),
        Form("bpcharne", "a, b", f"{trimmed_a} <> {trimmed_b}"),
        Form("bpcharcmp", "a, b", _compare(padded_a, padded_b)),
        Form("text_larger", "a, b", _larger(x, y, a, b)),
        Form("text_smaller", "a, b", _smaller(x, y, a, b)),
        Form("bpchar_larger", "a, b", _larger(padded_a, padded_b, a, b)),
        Form("bpchar_smaller", "a, b", _smaller(padded_a, padded_b, a, b)),
        *(
            form
            for name, op in ordered.items()
            for form in (
                Form(f"text_{name}", "a, b", f"{x} {op} {y}"),
                Form(f"text_pattern_{name}", "a, b", f"{a} {op} {b}"),
                Form(f"bpchar{name}", "a, b", f"{padded_a} {op} {padded_b}"),
                Form(f"bpchar_pattern_{name}", "a, b", f"{trimmed_a} {op} {trimmed_b}"),
            )
        ),
        *(
            form
            for name, op in _ORDER.items()
            for form in (
                Form(f"name{name}text", "a, b", f"{name_a} {op} {b}"),
                Form(f"text{name}name", "a, b", f"{a} {op} {name_b}"),
            )
        ),
    ]


def _crossed(left: str, right: str, read: str) -> list[Form]:
    """``left``_eq_``right`` and its kin, each operand brought to their common type."""
    a, b = _reads(read, "a", "b")
    return [
        *(Form(f"{left}_{name}_{right}", "a, b", f"{a} {op} {b}") for name, op in _ORDER.items()),
        Form(f"{left}_cmp_{right}", "a, b", _compare(a, b)),
    ]


_DATED = (
    ("date", "timestamp", _TIMESTAMP),
    ("timestamp", "date", _TIMESTAMP),
    ("date", "timestamptz", _TIMESTAMPTZ),
    ("timestamptz", "date", _TIMESTAMPTZ),
    ("timestamp", "timestamptz", _TIMESTAMPTZ),
    ("timestamptz", "timestamp", _TIMESTAMPTZ),
)


#: name, its type, PostgreSQL's name of it, the type its arithmetic cannot overflow, its bits.
_INTEGERS = (
    ("int2", "SMALLINT", "smallint", "BIGINT", 16),
    ("int4", "INTEGER", "integer", "BIGINT", 32),
    ("int8", "BIGINT", "bigint", "DECIMAL(38, 0)", 64),
)


def _in_range(value: str, sql_type: str, pg_name: str, bits: int) -> str:
    """``value`` as the integer type, or PostgreSQL's refusal of one it does not fit: a dialect's
    cast may wrap or saturate where it does not refuse."""
    lowest, highest = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
    return (
        f"CASE WHEN {value} < {lowest} OR {value} > {highest} "
        f"THEN {_fails('22003', f'{pg_name} out of range', sql_type)} "
        f"ELSE CAST({value} AS {sql_type}) END"
    )


def integer_of(value: str, stem: str) -> str:
    """``value`` as the integer type ``stem`` names, or PostgreSQL's refusal of one it does not fit."""
    _, sql_type, pg_name, _, bits = next(row for row in _INTEGERS if row[0] == stem)
    return _in_range(value, sql_type, pg_name, bits)


def _shifted_left(own: str, sql_type: str, bits: int) -> str:
    """int2shl and its kin: C's shift of the promoted operand, its count taken modulo the promoted
    width, the result cut to the type's bits. Written in numbers: a dialect's shift refuses what
    leaves the type, or a negative operand."""
    width = max(bits, 32)
    count = f"(CAST(b AS INTEGER) & {width - 1})"
    size = 1 << bits
    product = f"CAST({own} AS DECIMAL(38, 0)) * CAST(POWER(2, {count}) AS DECIMAL(38, 0))"
    return _bound(
        f"(({product}) % {size} + {size}) % {size}",
        f"CAST(CASE WHEN _m >= {1 << (bits - 1)} THEN _m - {size} ELSE _m END AS {sql_type})",
        "_m",
    )


def _integer(stem: str, sql_type: str, pg_name: str, wide: str, bits: int) -> list[Form]:
    own_a, own_b = f"CAST(a AS {sql_type})", f"CAST(b AS {sql_type})"
    a, b = f"CAST(a AS {wide})", f"CAST(b AS {wide})"
    zero = _fails("22012", _DIVISION_BY_ZERO, sql_type)
    count = f"(CAST(b AS INTEGER) & {max(bits, 32) - 1})"
    wider = max(bits, 32)
    promoted = f"CAST(a AS {'INTEGER' if bits < wider else sql_type})"

    def checked(value: str) -> str:
        return _in_range(value, sql_type, pg_name, bits)

    return [
        Form(f"{stem}pl", "a, b", checked(f"({a} + {b})")),
        Form(f"{stem}mi", "a, b", checked(f"({a} - {b})")),
        Form(f"{stem}mul", "a, b", checked(f"({a} * {b})")),
        Form(
            f"{stem}div",
            "a, b",
            f"CASE {_strict(sql_type)}WHEN {own_b} = 0 THEN {zero} "
            f"WHEN {own_b} = -1 THEN {checked(f'(-{a})')} "
            f"ELSE CAST({own_a} / {own_b} AS {sql_type}) END",
        ),
        Form(
            f"{stem}mod",
            "a, b",
            f"CASE {_strict(sql_type)}WHEN {own_b} = 0 THEN {zero} "
            f"WHEN {own_b} = -1 THEN {own_a} - {own_a} "
            f"ELSE CAST({own_a} % {own_b} AS {sql_type}) END",
        ),
        Form(f"{stem}um", "a", checked(f"(-{a})")),
        Form(f"{stem}up", "a", own_a),
        Form(f"{stem}abs", "a", checked(f"ABS({a})")),
        Form(f"{stem}and", "a, b", f"CAST({own_a} & {own_b} AS {sql_type})"),
        Form(f"{stem}or", "a, b", f"CAST({own_a} | {own_b} AS {sql_type})"),
        Form(
            f"{stem}xor", "a, b", f"CAST(({own_a} | {own_b}) - ({own_a} & {own_b}) AS {sql_type})"
        ),
        Form(f"{stem}not", "a", f"CAST(~{own_a} AS {sql_type})"),
        Form(f"{stem}shl", "a, b", _shifted_left(own_a, sql_type, bits)),
        # A negative operand through its complement: not every dialect's >> keeps the sign.
        Form(
            f"{stem}shr",
            "a, b",
            f"CAST(CASE WHEN {own_a} < 0 THEN ~(~{promoted} >> {count}) "
            f"ELSE {promoted} >> {count} END AS {sql_type})",
        ),
    ]


def _sum(name: str, state_type: str, value: str, added: str) -> Form:
    """A transition function of an integer sum: not strict, a NULL state or value is passed over."""
    return Form(
        name,
        "a, b",
        f"CASE WHEN a IS NULL THEN {value} WHEN b IS NULL THEN CAST(a AS {state_type}) "
        f"ELSE {added} END",
    )


#: int2_sum and int4_sum add in a bigint that wraps past its range.
_BIGINT_WRAP = _bound(
    "((CAST(a AS DECIMAL(38, 0)) + CAST(b AS DECIMAL(38, 0)) + 9223372036854775808) "
    "% 18446744073709551616 + 18446744073709551616) % 18446744073709551616",
    "CAST(_w - 9223372036854775808 AS BIGINT)",
    "_w",
)


def _float_checked(value: str, sql_type: str, zero_ok: str) -> str:
    """float.c's check: an infinity from finite operands overflowed; a zero, where ``zero_ok`` does
    not hold, underflowed."""
    a, b = f"CAST(a AS {sql_type})", f"CAST(b AS {sql_type})"
    infinite = f"CAST('Infinity' AS {sql_type})"
    return _bound(
        value,
        f"CASE WHEN ABS(_r) = {infinite} AND ABS({a}) <> {infinite} AND ABS({b}) <> {infinite} "
        f"THEN {_fails('22003', 'value out of range: overflow', sql_type)} "
        f"WHEN _r = 0 AND NOT ({zero_ok}) "
        f"THEN {_fails('22003', 'value out of range: underflow', sql_type)} ELSE _r END",
        "_r",
    )


def _float(stem: str, sql_type: str) -> list[Form]:
    a, b = f"CAST(a AS {sql_type})", f"CAST(b AS {sql_type})"
    infinite = f"CAST('Infinity' AS {sql_type})"
    nan = f"CAST('NaN' AS {sql_type})"
    return [
        Form(f"{stem}pl", "a, b", _float_checked(f"{a} + {b}", sql_type, "true")),
        Form(f"{stem}mi", "a, b", _float_checked(f"{a} - {b}", sql_type, "true")),
        Form(f"{stem}mul", "a, b", _float_checked(f"{a} * {b}", sql_type, f"{a} = 0 OR {b} = 0")),
        Form(
            f"{stem}div",
            "a, b",
            f"CASE {_strict(sql_type)}WHEN {b} = 0 AND {a} <> {nan} "
            f"THEN {_fails('22012', _DIVISION_BY_ZERO, sql_type)} WHEN {b} = 0 THEN {nan} ELSE "
            + _float_checked(f"{a} / {b}", sql_type, f"{a} = 0 OR ABS({b}) = {infinite}")
            + " END",
        ),
        Form(f"{stem}abs", "a", f"ABS({a})"),
        Form(f"{stem}um", "a", f"-{a}"),
        Form(f"{stem}up", "a", a),
    ]


def _numeric_forms() -> list[Form]:
    """numeric's exact operators, within what the engine's decimal holds; each keeps the scale of
    the operands it is given."""
    return [
        Form("numeric_add", "a, b", "a + b"),
        Form("numeric_sub", "a, b", "a - b"),
        Form("numeric_mul", "a, b", "a * b"),
        Form("numeric_uminus", "a", "-a"),
        Form("numeric_uplus", "a", "a"),
        Form("numeric_abs", "a", "ABS(a)"),
        Form("numeric_inc", "a", "a + 1"),
        Form("numeric_mod", "a, b", _numeric_mod()),
    ]


def _numeric_mod() -> str:
    """numeric_mod at one scale, which is what keeps the engine's remainder exact, cut back to the
    larger scale of its operands."""
    a, b = "CAST(a AS TEXT)", "CAST(b AS TEXT)"
    scale = f"GREATEST(LENGTH({_after(a, '.')}), LENGTH({_after(b, '.')}))"
    return (
        f"CASE {_strict('TEXT')}WHEN b = 0 THEN {_fails('22012', _DIVISION_BY_ZERO, 'TEXT')} ELSE "
        + _bound(
            "CAST(CAST(a AS DECIMAL(38, 18)) % CAST(b AS DECIMAL(38, 18)) AS TEXT)",
            f"LEFT(_r, STRPOS(_r, '.') + {scale} - CASE WHEN {scale} = 0 THEN 1 ELSE 0 END)",
            "_r",
        )
        + " END"
    )


_INTERVAL_A, _INTERVAL_B = "CAST(a AS INTERVAL)", "CAST(b AS INTERVAL)"


def _moved(stem: str, read: str, returns: str) -> list[Form]:
    """``stem``_pl_interval, _mi_interval and interval_pl_``stem``."""
    pl = f"CAST({read.replace('{}', 'a')} + {_INTERVAL_B} AS {returns})"
    mi = f"CAST({read.replace('{}', 'a')} - {_INTERVAL_B} AS {returns})"
    flipped = f"CAST({read.replace('{}', 'b')} + {_INTERVAL_A} AS {returns})"
    return [
        Form(f"{stem}_pl_interval", "a, b", pl),
        Form(f"{stem}_mi_interval", "a, b", mi),
        Form(f"interval_pl_{stem}", "a, b", flipped),
    ]


#: The days a date holds, from 4713 BC, as date2j counts them from 2000-01-01.
_DATE_DAYS = (-2451545, 2145031948)


def _date_moved(sign: str) -> str:
    """date_pli and date_mii: the date's day, moved, or PostgreSQL's refusal of one past its range."""
    days = (
        f"(CAST(CAST(a AS DATE) - CAST('2000-01-01' AS DATE) AS BIGINT) {sign} CAST(b AS BIGINT))"
    )
    return (
        f"CASE WHEN {days} < {_DATE_DAYS[0]} OR {days} >= {_DATE_DAYS[1]} "
        f"THEN {_fails('22008', 'date out of range', 'DATE')} "
        f"ELSE CAST(a AS DATE) {sign} CAST(b AS INTEGER) END"
    )


_DATED_ARITHMETIC: tuple[Form, ...] = (
    Form("date_mi", "a, b", "CAST(CAST(a AS DATE) - CAST(b AS DATE) AS INTEGER)"),
    Form("date_pli", "a, b", _date_moved("+")),
    Form("date_mii", "a, b", _date_moved("-")),
    *_moved("date", "CAST({} AS TIMESTAMP)", "TIMESTAMP"),
    *_moved("timestamp", _TIMESTAMP, "TIMESTAMP"),
    *_moved("timestamptz", _TIMESTAMPTZ, "TIMESTAMPTZ"),
    *_moved("time", _TIME, "TIME"),
    *_moved("timetz", _TIMETZ, "TIMETZ"),
    Form("timestamp_mi", "a, b", "CAST(a AS TIMESTAMP) - CAST(b AS TIMESTAMP)"),
    Form("timestamptz_mi", "a, b", "CAST(a AS TIMESTAMPTZ) - CAST(b AS TIMESTAMPTZ)"),
    Form(
        "time_mi_time",
        "a, b",
        "(CAST('2000-01-01' AS DATE) + CAST(a AS TIME)) - "
        "(CAST('2000-01-01' AS DATE) + CAST(b AS TIME))",
    ),
    Form("interval_pl", "a, b", f"{_INTERVAL_A} + {_INTERVAL_B}"),
    Form("interval_mi", "a, b", f"{_INTERVAL_A} - {_INTERVAL_B}"),
    Form("interval_um", "a", f"-{_INTERVAL_A}"),
    Form("date_subtract", "a, b", "CAST(a AS TIMESTAMPTZ) - CAST(b AS INTERVAL)"),
    Form(
        "date_subtract",
        "a, b, z",
        "((CAST(a AS TIMESTAMPTZ) AT TIME ZONE z) - CAST(b AS INTERVAL)) AT TIME ZONE z",
    ),
)

_BIT_A, _BIT_B = "CAST(a AS BIT)", "CAST(b AS BIT)"


def _bit_sizes(verb: str) -> str:
    """bit_and's refusal of strings of two sizes, up to its answer."""
    a, b = _reads(_TEXT, "a", "b")
    sizes = f"CAST(LENGTH({a}) AS TEXT) || ' and ' || CAST(LENGTH({b}) AS TEXT)"
    return (
        f"CASE WHEN LENGTH({a}) <> LENGTH({b}) THEN "
        + refused("22026", f"'cannot {verb} bit strings of sizes ' || {sizes}", "TEXT")
        + " ELSE "
    )


def _bit_operator(name: str, verb: str, op: str) -> Form:
    return Form(name, "a, b", _bit_sizes(verb) + f"CAST({_BIT_A} {op} {_BIT_B} AS TEXT) END")


_BITS: tuple[Form, ...] = (
    _bit_operator("bitand", "AND", "&"),
    _bit_operator("bitor", "OR", "|"),
    Form(
        "bitxor",
        "a, b",
        _bit_sizes("XOR") + f"CAST(({_BIT_A} | {_BIT_B}) & ~({_BIT_A} & {_BIT_B}) AS TEXT) END",
    ),
    Form("bitnot", "a", f"CAST(~{_BIT_A} AS TEXT)"),
)


_LSNS: tuple[Form, ...] = (
    Form("pg_lsn_mi", "a, b", f"{_lsn_number('a')} - {_lsn_number('b')}"),
    Form("pg_lsn_pli", "a, b", _lsn_text(f"{_lsn_number('a')} + CAST(b AS DECIMAL(38, 6))")),
    Form("pg_lsn_mii", "a, b", _lsn_text(f"{_lsn_number('a')} - CAST(b AS DECIMAL(38, 6))")),
    Form("numeric_pl_pg_lsn", "a, b", _lsn_text(f"CAST(a AS DECIMAL(38, 6)) + {_lsn_number('b')}")),
)


_OCTETS = r"^([0-9]+)[.]([0-9]+)[.]([0-9]+)[.]([0-9]+)$"


def _octet(address: str, index: int) -> str:
    return f"TRY_CAST(REGEXP_REPLACE({address}, '{_OCTETS}', '\\{index}') AS BIGINT)"


def _address(value: str) -> str:
    """An IPv4 address as a number."""
    host = _before(f"CAST({value} AS TEXT)", "/")
    return " + ".join(f"{_octet(host, i)} * {256 ** (4 - i)}" for i in range(1, 5))


def _prefix(value: str) -> str:
    """A network's mask length: an address names one host where it says none."""
    written = f"TRY_CAST({_after(f'CAST({value} AS TEXT)', '/')} AS INTEGER)"
    return f"CASE WHEN {value} IS NOT NULL THEN COALESCE({written}, 32) END"


def _v4(function: str, sql_type: str, answer: str, *params: str) -> str:
    """``answer`` over each parameter's address (_ip) and mask length (_len). An IPv6 address is
    refused by name: its 128 bits are past what a dialect's integers hold."""
    held = answer
    for param in params:
        held = _bound(_prefix(param), held, f"_len{param}")
        held = _bound(_address(param), held, f"_ip{param}")
    sixes = " OR ".join(f"STRPOS(CAST({p} AS TEXT), ':') > 0" for p in params)
    return f"CASE WHEN {sixes} THEN {_not_served(function, sql_type)} ELSE {held} END"


def _host_bits(prefix: str) -> str:
    return f"CAST(POWER(2, 32 - {prefix}) AS BIGINT)"


def _network(param: str, prefix: str | None = None) -> str:
    """An address with its host bits cleared."""
    size = _host_bits(prefix or f"_len{param}")
    return f"(_ip{param} - _ip{param} % {size})"


def _dotted(number: str) -> str:
    """An address as an inet writes it."""
    octets = (
        f"CAST(CAST({number} AS BIGINT) / CAST({256 ** (4 - i)} AS BIGINT) % 256 AS TEXT)"
        for i in range(1, 5)
    )
    return " || '.' || ".join(octets)


def _inet_text(number: str, prefix: str) -> str:
    return (
        f"CAST({_dotted(number)} || CASE WHEN {prefix} = 32 THEN '' "
        f"ELSE '/' || CAST({prefix} AS TEXT) END AS TEXT)"
    )


def _network_key(param: str) -> str:
    """network_cmp's order as a number: the network, its mask length, the address."""
    return (
        f"(CAST({_network(param)} AS DECIMAL(38, 0)) * 1099511627776 + "
        f"CAST(_len{param} AS DECIMAL(38, 0)) * 4294967296 + _ip{param})"
    )


def _within(function: str, smaller: str, larger: str, strict_: bool) -> Form:
    """network_sub and its kin: the larger network holds the smaller."""
    compare = ">" if strict_ else ">="
    return Form(
        function,
        "a, b",
        _v4(
            function,
            "BOOLEAN",
            f"_len{smaller} {compare} _len{larger} AND "
            f"{_network(smaller, f'_len{larger}')} = {_network(larger)}",
            "a",
            "b",
        ),
    )


def _inet_bitwise(name: str, op: str) -> Form:
    return Form(
        name,
        "a, b",
        _v4(
            name,
            "TEXT",
            _inet_text(f"_ipa {op} _ipb", "GREATEST(_lena, _lenb)"),
            "a",
            "b",
        ),
    )


def _inet_moved(name: str, params: str, inet: str, number: str) -> Form:
    moved = _bound(
        number,
        "CASE WHEN _m < 0 OR _m > 4294967295 THEN "
        + _fails("22003", "result is out of range", "TEXT")
        + f" ELSE {_inet_text('CAST(_m AS BIGINT)', f'_len{inet}')} END",
        "_m",
    )
    return Form(name, params, _v4(name, "TEXT", moved, inet))


def _set_masklen(name: str, cleared: bool) -> Form:
    # NULL for an IPv6 value, which _v4 refuses before this branch: DuckDB 2.0 evaluates the
    # branch's bound subquery even when it is not taken, and its mask check must not raise then.
    bits = (
        "CASE WHEN STRPOS(CAST(a AS TEXT), ':') > 0 THEN CAST(NULL AS INTEGER) "
        "WHEN CAST(b AS INTEGER) = -1 THEN 32 ELSE CAST(b AS INTEGER) END"
    )
    wrote = _inet_text(_network("a", "_n") if cleared else "_ipa", "_n")
    if cleared:
        wrote = f"CAST({_dotted(_network('a', '_n'))} || '/' || CAST(_n AS TEXT) AS TEXT)"
    return Form(
        name,
        "a, b",
        _v4(
            name,
            "TEXT",
            _bound(
                bits,
                "CASE WHEN _n < 0 OR _n > 32 THEN "
                + refused("22023", "'invalid mask length: ' || CAST(b AS TEXT)", "TEXT")
                + f" ELSE {wrote} END",
                "_n",
            ),
            "a",
        ),
    )


_WIDE_B = "CAST(b AS DECIMAL(38, 0))"
_NETWORK_KEYS: tuple[Form, ...] = (
    *(
        Form(
            f"network_{name}",
            "a, b",
            _v4(
                f"network_{name}",
                "BOOLEAN",
                f"{_network_key('a')} {op} {_network_key('b')}",
                "a",
                "b",
            ),
        )
        for name, op in _ORDER.items()
    ),
    Form(
        "network_cmp",
        "a, b",
        _v4("network_cmp", "INTEGER", _compare(_network_key("a"), _network_key("b")), "a", "b"),
    ),
    Form(
        "network_larger",
        "a, b",
        _v4(
            "network_larger",
            "TEXT",
            _larger(_network_key("a"), _network_key("b"), "CAST(a AS TEXT)", "CAST(b AS TEXT)"),
            "a",
            "b",
        ),
    ),
    Form(
        "network_smaller",
        "a, b",
        _v4(
            "network_smaller",
            "TEXT",
            _smaller(_network_key("a"), _network_key("b"), "CAST(a AS TEXT)", "CAST(b AS TEXT)"),
            "a",
            "b",
        ),
    ),
    _within("network_sub", "a", "b", True),
    _within("network_subeq", "a", "b", False),
    _within("network_sup", "b", "a", True),
    _within("network_supeq", "b", "a", False),
    Form(
        "network_overlap",
        "a, b",
        _v4(
            "network_overlap",
            "BOOLEAN",
            f"{_network('a', 'LEAST(_lena, _lenb)')} = {_network('b', 'LEAST(_lena, _lenb)')}",
            "a",
            "b",
        ),
    ),
    Form("masklen", "a", _v4("masklen", "INTEGER", "CAST(_lena AS INTEGER)", "a")),
    Form(
        "hostmask",
        "a",
        _v4("hostmask", "TEXT", _inet_text(f"{_host_bits('_lena')} - 1", "32"), "a"),
    ),
    _set_masklen("set_masklen", False),
    _set_masklen("cidr_set_masklen", True),
    Form(
        "cidr",
        "a",
        _v4(
            "cidr",
            "TEXT",
            f"CAST({_dotted(_network('a'))} || '/' || CAST(_lena AS TEXT) AS TEXT)",
            "a",
        ),
    ),
    Form(
        "inet_same_family",
        "a, b",
        "(STRPOS(CAST(a AS TEXT), ':') > 0) = (STRPOS(CAST(b AS TEXT), ':') > 0)",
    ),
    _inet_bitwise("inetand", "&"),
    _inet_bitwise("inetor", "|"),
    Form("inetnot", "a", _v4("inetnot", "TEXT", _inet_text("4294967295 - _ipa", "_lena"), "a")),
    _inet_moved("inetpl", "a, b", "a", f"_ipa + {_WIDE_B}"),
    _inet_moved("inetmi_int8", "a, b", "a", f"_ipa - {_WIDE_B}"),
    _inet_moved("int8pl_inet", "b, a", "a", f"_ipa + {_WIDE_B}"),
    Form("inetmi", "a, b", _v4("inetmi", "BIGINT", "_ipa - _ipb", "a", "b")),
)


def _digit_value(digits: str, position: int) -> str:
    return f"(STRPOS('0123456789abcdef', SUBSTR({digits}, {position}, 1)) - 1)"


def _word(digits: str, start: int, length: int) -> str:
    """``length`` hex digits of ``digits`` as a number."""
    return (
        "("
        + " + ".join(
            f"{_digit_value(digits, start + i)} * {16 ** (length - 1 - i)}" for i in range(length)
        )
        + ")"
    )


def _mac_words(combine: str, word: int, words: int, *params: str) -> str:
    """Each ``word`` hex digits of a MAC address combined, as the address writes them: a word fits
    any integer however long the address."""

    def part(start: int) -> str:
        operands = [_word(_hex_of(p), start, word) for p in params]
        return f"LPAD(LOWER(HEX({combine.format(*operands)})), {word}, '0')"

    return " || ".join(part(i * word + 1) for i in range(words))


def _mac_bitwise(stem: str, word: int, words: int) -> list[Form]:
    pairs = word * words // 2
    mask = (1 << (word * 4)) - 1

    def written(combine: str, *params: str) -> str:
        digits = _mac_words(combine, word, words, *params)
        return f"CAST({_bound(digits, _colons('_h', pairs), '_h')} AS TEXT)"

    return [
        Form(f"{stem}_and", "a, b", written("{0} & {1}", "a", "b")),
        Form(f"{stem}_or", "a, b", written("{0} | {1}", "a", "b")),
        Form(f"{stem}_not", "a", written(f"{mask} - {{0}}", "a")),
    ]


def _colons(digits: str, pairs: int) -> str:
    """Hex digits as a MAC address writes them."""
    return " || ':' || ".join(f"SUBSTR({digits}, {i * 2 + 1}, 2)" for i in range(pairs))


def _interval_fields(value: str) -> tuple[str, str, str]:
    """An interval's months, days and microseconds."""
    months = f"(EXTRACT(YEAR FROM {value}) * 12 + EXTRACT(MONTH FROM {value}))"
    micros = (
        f"(EXTRACT(HOUR FROM {value}) * 3600000000 + EXTRACT(MINUTE FROM {value}) * 60000000 + "
        f"EXTRACT(MICROSECOND FROM {value}))"
    )
    return (
        f"CAST({months} AS BIGINT)",
        f"CAST(EXTRACT(DAY FROM {value}) AS BIGINT)",
        f"CAST({micros} AS BIGINT)",
    )


def _interval_of(months: str, days: str, micros: str) -> str:
    """An interval of its fields, each multiplied in the integers an interval's product reads."""
    seconds = f"CAST(TRUNC(CAST({micros} AS DOUBLE PRECISION) / 1000000) AS BIGINT)"
    rest = f"CAST({micros} - {seconds} * 1000000 AS INTEGER)"
    return (
        f"CAST({months} AS INTEGER) * INTERVAL '1 month' + "
        f"CAST({days} AS INTEGER) * INTERVAL '1 day' + "
        f"CAST({seconds} / 3600 AS INTEGER) * INTERVAL '1 hour' + "
        f"CAST({seconds} % 3600 AS INTEGER) * INTERVAL '1 second' + "
        f"{rest} * INTERVAL '1 microsecond'"
    )


def _interval_scaled(function: str, op: str, factor: str = "CAST(b AS DOUBLE PRECISION)") -> str:
    """interval_mul and interval_div, as timestamp.c cascades them: what ``op`` leaves of a month
    runs on to days at thirty a month and what it leaves of a day to seconds, each rounded to six
    places; rounding may leave a whole day of seconds, which is carried."""
    m, d, u = _interval_fields("_i")
    month = f"TRUNC({m} {op} _x)"
    spilled = f"(ROUND(({m} {op} _x - {month}) * 30000000) / 1000000)"
    rest = f"({d} {op} _x - TRUNC({d} {op} _x) + {spilled} - TRUNC({spilled}))"
    seconds = f"(ROUND({rest} * 86400000000) / 1000000)"
    carried = f"TRUNC({seconds} / 86400)"
    days = f"(TRUNC({d} {op} _x) + TRUNC({spilled}) + {carried})"
    micros = f"ROUND({u} {op} _x + ({seconds} - {carried} * 86400) * 1000000)"
    return _bound(
        "CAST(a AS INTERVAL)",
        _bound(
            factor,
            f"CASE WHEN ABS(_x) = CAST('Infinity' AS DOUBLE PRECISION) "
            f"OR _x = CAST('NaN' AS DOUBLE PRECISION) "
            f"THEN {_not_served(function, 'INTERVAL')} "
            f"WHEN ABS({month}) > 2147483647 OR ABS({days}) > 2147483647 "
            f"OR ABS({micros}) > 9.2e18 THEN {_fails('22008', 'interval out of range', 'INTERVAL')} "
            f"ELSE {_interval_of(month, days, f'CAST({micros} AS BIGINT)')} END",
            "_x",
        ),
        "_i",
    )


_INTERVAL_SCALED: tuple[Form, ...] = (
    Form("interval_mul", "a, b", _interval_scaled("interval_mul", "*")),
    Form(
        "interval_div",
        "a, b",
        f"CASE {_strict('INTERVAL')}WHEN CAST(b AS DOUBLE PRECISION) = 0 "
        f"THEN {_fails('22012', _DIVISION_BY_ZERO, 'INTERVAL')} ELSE "
        # The divisor bound NULLIF 0: this branch's subquery is evaluated by DuckDB 2.0 even when
        # the zero divisor (or a NULL argument) has already been answered above.
        + _interval_scaled("interval_div", "/", "NULLIF(CAST(b AS DOUBLE PRECISION), 0)")
        + " END",
    ),
)

#: macaddr8_set7bit sets the seventh bit of the first byte; macaddr and macaddr8 convert.
_MACS: tuple[Form, ...] = (
    *_mac_bitwise("macaddr", 6, 2),
    *_mac_bitwise("macaddr8", 8, 2),
    Form(
        "macaddr8_set7bit",
        "a",
        _bound(
            _hex_of("a"),
            _bound(
                f"LPAD(LOWER(HEX({_word('_h', 1, 2)} | 2)), 2, '0') || SUBSTR(_h, 3)",
                _colons("_s", 8),
                "_s",
            ),
            "_h",
        ),
    ),
    Form(
        "macaddr8",
        "a",
        _bound(_hex_of("a"), _colons("LEFT(_h, 6) || 'fffe' || RIGHT(_h, 6)", 8), "_h"),
    ),
    Form(
        "macaddr",
        "a",
        _bound(
            _hex_of("a"),
            "CASE WHEN SUBSTR(_h, 7, 4) <> 'fffe' THEN "
            + _fails(
                "22003",
                "macaddr8 data out of range to convert to macaddr",
                "TEXT",
            )
            + f" ELSE {_colons('LEFT(_h, 6) || RIGHT(_h, 6)', 6)} END",
            "_h",
        ),
    ),
)


OPERATORS: tuple[Form, ...] = (
    *(form for row in _ORDERED for form in row.forms()),
    *_text_orders(),
    *(form for row in _DATED for form in _crossed(*row)),
    *(form for row in _INTEGERS for form in _integer(*row)),
    *_float("float4", "REAL"),
    *_float("float8", "DOUBLE PRECISION"),
    *_numeric_forms(),
    *_DATED_ARITHMETIC,
    *_INTERVAL_SCALED,
    *_MACS,
    *_NETWORK_KEYS,
    *_BITS,
    *_LSNS,
    Form("cideq", "a, b", "CAST(a AS BIGINT) = CAST(b AS BIGINT)"),
    Form("xideq", "a, b", "CAST(a AS BIGINT) = CAST(b AS BIGINT)"),
    Form("xidneq", "a, b", "CAST(a AS BIGINT) <> CAST(b AS BIGINT)"),
    # A xid against an int4: the int4's bits, unsigned.
    Form("xideqint4", "a, b", "CAST(a AS BIGINT) = (CAST(b AS BIGINT) & 4294967295)"),
    Form("xidneqint4", "a, b", "CAST(a AS BIGINT) <> (CAST(b AS BIGINT) & 4294967295)"),
    Form("textlen", "a", "CAST(CHAR_LENGTH(CAST(a AS TEXT)) AS INTEGER)"),
    Form(
        "int4inc",
        "a",
        _in_range("(CAST(a AS BIGINT) + 1)", "INTEGER", "integer", 32),
    ),
    Form(
        "int8inc",
        "a",
        _in_range("(CAST(a AS DECIMAL(38, 0)) + 1)", "BIGINT", "bigint", 64),
    ),
    _sum("int2_sum", "BIGINT", "CAST(b AS BIGINT)", _BIGINT_WRAP),
    _sum("int4_sum", "BIGINT", "CAST(b AS BIGINT)", _BIGINT_WRAP),
)


#: PostgreSQL's array operators, in the dialect sqlglot writes them for: any other refuses them.
FORMS_BY_NAME = {form.name: form for form in OPERATORS}

ARRAY_OPERATORS: tuple[Form, ...] = (
    # A NULL element is equal to nothing, so an array holding one is contained in no other.
    Form("arraycontains", "a, b", "a @> b AND ARRAY_POSITION(b, NULL) IS NULL"),
    Form("arraycontained", "a, b", "a <@ b AND ARRAY_POSITION(a, NULL) IS NULL"),
    Form("arrayoverlap", "a, b", "a && b"),
)

#: Engine tweaks, each added after a replay failed. DuckDB's / of two bigints is a double, which
#: loses the low bits of a bigint past 2^53.
DUCKDB_TWEAKS = {
    form.name: form.sql.replace(
        "CAST(a AS BIGINT) / CAST(b AS BIGINT)", "CAST(a AS BIGINT) // CAST(b AS BIGINT)"
    )
    for form in OPERATORS
    if form.name == "int8div"
}
#: The forms that read an argument's text: a statement's cast to their type says nothing more, and
#: a cast to character(n) says how far it pads.
HELD = frozenset(form.name for form in OPERATORS if form.name.startswith(("tid", "bpchar"))) | {
    "width_bucket_numeric"
}
#: The forms whose overflow only the engine's own arithmetic raises: they check no range themselves.
#: date_subtract's AT TIME ZONE is also what sqlglot drops for MySQL.
OVERFLOW_UNCHECKED = frozenset(form.name for form in (*_numeric_forms(), *_DATED_ARITHMETIC))
