# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7c1a96-0b35-4d28-8f61-c2d97e3a5b10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two generated rules of pg_catalog's functions, for the rows of
provisa.transpiler.function_macros (REQ-2171, A REWRITE IS A SQLGLOT EXPANSION, ALWAYS POSTGRESQL
TO THE TARGET). Each :class:`Form` is one overload in PostgreSQL's syntax, which sqlglot renders for
every dialect; nothing here is listed per dialect.

OPERATORS: a function named for an operator is that operator, from a table of types and the
patterns of their names. int4pl(a, b) is a + b. A form states its casts, so a dialect's division or
rounding cannot change the answer, and checks its range, so PostgreSQL's refusal is raised.

IO: a type's ``out`` is its value as PostgreSQL's text, its ``in`` that text as a value, its
``send`` its wire bytes. A type's text is read only in the shapes that read the same on PostgreSQL
and on the engine; a value in any other shape is refused by name (AN ANSWER THAT DIFFERS IS
REFUSED), never answered differently."""

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


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def refused(sqlstate: str, message: str, sql_type: str = "INTEGER") -> str:
    """PostgreSQL's error ``sqlstate`` with the SQL text ``message``, as a value of ``sql_type``:
    the cast of a text that is no number, which any dialect that checks a cast raises, and which
    provisa.core.pg_error.from_engine reads back."""
    text = f"{_quoted(f'{MARK} {sqlstate}: ')} || {message} || {_quoted(END)}"
    return f"CAST(CAST({text} AS INTEGER) AS {sql_type})"


def _fails(sqlstate: str, message: str, sql_type: str = "INTEGER") -> str:
    """A refusal whose message is a constant."""
    return refused(sqlstate, _quoted(message), sql_type)


def _not_served(function: str, sql_type: str) -> str:
    """A value this form cannot answer as PostgreSQL does: refused by name, not answered."""
    return _fails("0A000", f"{function} does not read this value", sql_type)


def _bound(value: str, answer: str, name: str) -> str:
    """``answer`` with ``name`` holding ``value``: a form that needs a value twice."""
    return f"(SELECT {answer} FROM (SELECT {value} AS {name}) AS b{name})"


def _reads(read: str, *params: str) -> tuple[str, ...]:
    return tuple(read.replace("{}", p) for p in params)


def _before(value: str, separator: str) -> str:
    """``value`` up to ``separator``; all of it where there is none."""
    at = f"STRPOS({value}, '{separator}')"
    return f"CASE WHEN {at} > 0 THEN LEFT({value}, {at} - 1) ELSE {value} END"


def _after(value: str, separator: str) -> str:
    """``value`` past ``separator``; nothing where there is none."""
    at = f"STRPOS({value}, '{separator}')"
    return f"CASE WHEN {at} > 0 THEN SUBSTR({value}, {at} + 1) ELSE '' END"


# --- OPERATORS ----------------------------------------------------------------------------------

_ORDER = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_DIVISION_BY_ZERO = "division by zero"


def _compare(a: str, b: str) -> str:
    """A cmp function: NULL for a NULL argument, as it is strict."""
    return f"CASE WHEN {a} < {b} THEN -1 WHEN {a} > {b} THEN 1 WHEN {a} = {b} THEN 0 END"


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
    _Ordered("numeric_", _NUMERIC, cmp=True, picks=True, shown="{}"),
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
    _Ordered("pg_lsn_", _lsn_key("{}"), cmp=True, picks=True, shown=_TEXT),
    _Ordered("tid", _tid_key("{}"), picks=True, shown=_TEXT),
    _Ordered("name", _NAME),
    _Ordered("array_", "{}", picks=True),
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
        Form("bpchar_larger", "a, b", _larger(padded_a, padded_b, trimmed_a, trimmed_b)),
        Form("bpchar_smaller", "a, b", _smaller(padded_a, padded_b, trimmed_a, trimmed_b)),
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
            f"CASE WHEN {own_b} = 0 THEN {zero} WHEN {own_b} = -1 THEN {checked(f'(-{a})')} "
            f"ELSE CAST({own_a} / {own_b} AS {sql_type}) END",
        ),
        Form(
            f"{stem}mod",
            "a, b",
            f"CASE WHEN {own_b} = 0 THEN {zero} WHEN {own_b} = -1 THEN {own_a} - {own_a} "
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
            f"CASE WHEN {b} = 0 AND {a} <> {nan} "
            f"THEN {_fails('22012', _DIVISION_BY_ZERO, sql_type)} ELSE "
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
        Form(
            "numeric_mod",
            "a, b",
            f"CASE WHEN b = 0 THEN {_fails('22012', _DIVISION_BY_ZERO)} ELSE a % b END",
        ),
    ]


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
        "CAST(CAST(a AS TIME) AS INTERVAL) - CAST(CAST(b AS TIME) AS INTERVAL)",
    ),
    Form("interval_pl", "a, b", f"{_INTERVAL_A} + {_INTERVAL_B}"),
    Form("interval_mi", "a, b", f"{_INTERVAL_A} - {_INTERVAL_B}"),
    Form("interval_um", "a", f"-{_INTERVAL_A}"),
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
    """A position as pg_lsn_out writes it, or the refusal of one past pg_lsn's range."""
    high = "CAST(TRUNC(_n / 4294967296) AS BIGINT)"
    low = "CAST(_n - TRUNC(_n / 4294967296) * 4294967296 AS BIGINT)"
    return _bound(
        number,
        f"CASE WHEN _n < 0 OR _n >= 18446744073709551616 "
        f"THEN {_fails('22003', 'pg_lsn out of range', 'TEXT')} "
        f"ELSE UPPER(HEX({high})) || '/' || UPPER(HEX({low})) END",
        "_n",
    )


_LSNS: tuple[Form, ...] = (
    Form("pg_lsn_mi", "a, b", f"{_lsn_number('a')} - {_lsn_number('b')}"),
    Form("pg_lsn_pli", "a, b", _lsn_text(f"{_lsn_number('a')} + CAST(b AS DECIMAL(38, 0))")),
    Form("pg_lsn_mii", "a, b", _lsn_text(f"{_lsn_number('a')} - CAST(b AS DECIMAL(38, 0))")),
    Form("numeric_pl_pg_lsn", "a, b", _lsn_text(f"CAST(a AS DECIMAL(38, 0)) + {_lsn_number('b')}")),
)


_OPERATORS: tuple[Form, ...] = (
    *(form for row in _ORDERED for form in row.forms()),
    *_text_orders(),
    *(form for row in _DATED for form in _crossed(*row)),
    *(form for row in _INTEGERS for form in _integer(*row)),
    *_float("float4", "REAL"),
    *_float("float8", "DOUBLE PRECISION"),
    *_numeric_forms(),
    *_DATED_ARITHMETIC,
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
ARRAY_OPERATORS: tuple[Form, ...] = (
    Form("arraycontains", "a, b", "a @> b"),
    Form("arraycontained", "a, b", "a <@ b"),
    Form("arrayoverlap", "a, b", "a && b"),
)


# --- IO -----------------------------------------------------------------------------------------

_SPACES = "[ \\t\\n\\r\\f\\v]"


def _trimmed(value: str) -> str:
    return f"REGEXP_REPLACE(CAST({value} AS TEXT), '^{_SPACES}+|{_SPACES}+$', '', 'g')"


def _no_value(type_name: str, value: str, sql_type: str, sqlstate: str = "22P02") -> str:
    """What an input function says of text that is no value of its type."""
    return refused(
        sqlstate, f"'invalid input syntax for type {type_name}: \"' || {value} || '\"'", sql_type
    )


def _big_endian(value: str, bits: int) -> str:
    """A BIGINT's hex digits in ``bits`` of two's complement."""

    def word(of: str) -> str:
        return f"LPAD(LOWER(HEX(({of}) & 4294967295)), 8, '0')"

    if bits == 64:
        return f"{word(f'CAST({value} AS BIGINT) >> 32')} || {word(f'CAST({value} AS BIGINT)')}"
    return f"RIGHT({word(f'CAST({value} AS BIGINT)')}, {bits // 4})"


def _read_integer(sql_type: str, pg_name: str, bits: int) -> str:
    """int4in and its kin in the decimal shape every dialect reads. A number past the type is
    PostgreSQL's refusal; hex, octal, binary and underscores are not read here."""
    lowest, highest = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
    wide = "CAST(_s AS DECIMAL(38, 0))"
    return _bound(
        _trimmed("v"),
        f"CASE WHEN NOT _s ~ '^[+-]?[0-9]+$' THEN {_no_value(pg_name, 'v', sql_type)} "
        f"WHEN LENGTH(_s) > 30 OR {wide} < {lowest} OR {wide} > {highest} THEN "
        + refused("22003", f"'value \"' || v || '\" is out of range for type {pg_name}'", sql_type)
        + f" ELSE CAST(_s AS {sql_type}) END",
        "_s",
    )


def _read_unsigned(pg_name: str, digits: str) -> str:
    """oidin and its kin: unsigned, within the type's bits."""
    return _bound(
        _trimmed("v"),
        f"CASE WHEN NOT _s ~ '^[0-9]+$' THEN {_no_value(pg_name, 'v', 'BIGINT')} "
        f"WHEN LENGTH(_s) > 30 OR CAST(_s AS DECIMAL(38, 0)) > {digits} THEN "
        + refused("22003", f"'value \"' || v || '\" is out of range for type {pg_name}'", "BIGINT")
        + " ELSE CAST(_s AS BIGINT) END",
        "_s",
    )


_FLOAT_SHAPE = r"^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$"
_FLOAT_WORDS = "^[+-]?(inf|infinity|nan)$"


def _read_float(sql_type: str, pg_name: str) -> str:
    """float8in and float4in: a value past the type's range, either way, is refused by name."""
    past = refused("22003", f"'\"' || v || '\" is out of range for type {pg_name}'", sql_type)
    return _bound(
        f"LOWER({_trimmed('v')})",
        f"CASE WHEN _w ~ '{_FLOAT_WORDS}' THEN CAST(_w AS {sql_type}) "
        f"WHEN _w ~ '{_FLOAT_SHAPE}' THEN "
        + _bound(
            f"TRY_CAST(_w AS {sql_type})",
            f"CASE WHEN _f IS NULL OR ABS(_f) = CAST('Infinity' AS {sql_type}) THEN {past} "
            f"WHEN _f = 0 AND REGEXP_REPLACE(_w, 'e.*', '') ~ '[1-9]' THEN {past} ELSE _f END",
            "_f",
        )
        + f" ELSE {_no_value(pg_name, 'v', sql_type)} END",
        "_w",
    )


_BOOLEAN_WORDS = ("t|tr|tru|true|y|ye|yes|on|1", "f|fa|fal|fals|false|n|no|of|off|0")


def _read_bool() -> str:
    return _bound(
        f"LOWER({_trimmed('v')})",
        f"CASE WHEN _w ~ '^({_BOOLEAN_WORDS[0]})$' THEN true "
        f"WHEN _w ~ '^({_BOOLEAN_WORDS[1]})$' THEN false "
        f"ELSE {_no_value('boolean', 'v', 'BOOLEAN')} END",
        "_w",
    )


def _float_text(value: str, last_fixed: int) -> str:
    """float8out and float4out: the shortest digits the engine writes, set out as PostgreSQL sets
    them, positional from 1e-4 to the type's width, scientific otherwise."""
    mantissa = _before("_s", "e")
    power = f"CAST(COALESCE(NULLIF({_after('_s', 'e')}, ''), '0') AS INTEGER)"
    whole = _before(mantissa, ".")
    part = _after(mantissa, ".")
    significant = f"RTRIM(LTRIM({whole} || {part}, '0'), '0')"
    point = (
        f"LENGTH({whole}) + {power} - "
        f"(LENGTH({whole} || {part}) - LENGTH(LTRIM({whole} || {part}, '0')))"
    )
    scientific = (
        "LEFT(_d, 1) || CASE WHEN LENGTH(_d) > 1 THEN '.' || SUBSTR(_d, 2) ELSE '' END || 'e' || "
        "CASE WHEN _p - 1 < 0 THEN '-' ELSE '+' END || LPAD(CAST(ABS(_p - 1) AS TEXT), 2, '0')"
    )
    fixed = (
        "CASE WHEN _p <= 0 THEN '0.' || REPEAT('0', -_p) || _d "
        "WHEN LENGTH(_d) <= _p THEN _d || REPEAT('0', _p - LENGTH(_d)) "
        "ELSE LEFT(_d, _p) || '.' || SUBSTR(_d, _p + 1) END"
    )
    shaped = f"CASE WHEN _p - 1 < -4 OR _p - 1 >= {last_fixed} THEN {scientific} ELSE {fixed} END"
    digits = _bound(significant, _bound(point, shaped, "_p"), "_d")
    written = _bound(f"CAST(ABS({value}) AS TEXT)", digits, "_s")
    return (
        f"CASE WHEN {value} IS NULL THEN CAST(NULL AS TEXT) "
        f"WHEN CAST({value} AS TEXT) ~* 'nan' THEN 'NaN' "
        f"WHEN CAST({value} AS TEXT) ~* '^inf' THEN 'Infinity' "
        f"WHEN CAST({value} AS TEXT) ~* '^-inf' THEN '-Infinity' "
        f"WHEN {value} = 0 THEN CASE WHEN CAST({value} AS TEXT) ~ '^-' THEN '-0' ELSE '0' END "
        f"ELSE CASE WHEN {value} < 0 THEN '-' ELSE '' END || {written} END"
    )


_FLOAT8_TEXT = 15
_FLOAT4_TEXT = 6

_ISO_DATE = "[0-9]{4}-[0-9]{2}-[0-9]{2}"
_CLOCK = "[0-9]{2}:[0-9]{2}(:[0-9]{2}([.][0-9]{1,6})?)?"
_OFFSET = "([+-][0-9]{2}(:?[0-9]{2})?)"
_INFINITE = "|[+-]?infinity"


def _read_dated(type_name: str, pg_name: str, shape: str) -> str:
    """date_in and its kin in the shape every dialect reads alike. A value of that shape no day or
    time has is PostgreSQL's out of range; any other shape is no value here."""
    past = refused("22008", "'date/time field value out of range: \"' || v || '\"'", type_name)
    return _bound(
        _trimmed("v"),
        f"CASE WHEN NOT _s ~ '^({shape})$' THEN {_no_value(pg_name, 'v', type_name, '22007')} "
        f"WHEN TRY_CAST(_s AS {type_name}) IS NULL THEN {past} "
        f"ELSE CAST(_s AS {type_name}) END",
        "_s",
    )


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


def _interval_text(value: str) -> str:
    """interval_out: years and months apart, a field after a negative one saying its own sign."""

    def field(amount: str, unit: str, after_negative: str) -> str:
        sign = f"CASE WHEN {after_negative} AND {amount} > 0 THEN '+' ELSE '' END"
        plural = f"CASE WHEN ABS({amount}) = 1 THEN '' ELSE 's' END"
        return (
            f"CASE WHEN {amount} <> 0 THEN ' ' || {sign} || CAST({amount} AS TEXT) || "
            f"' {unit}' || {plural} ELSE '' END"
        )

    years = "CAST(CASE WHEN _m < 0 THEN -(-_m / 12) ELSE _m / 12 END AS BIGINT)"
    months = "(_m - 12 * CAST(CASE WHEN _m < 0 THEN -(-_m / 12) ELSE _m / 12 END AS BIGINT))"
    after_month = f"CASE WHEN {months} <> 0 THEN {months} < 0 ELSE {years} < 0 END"
    after_day = f"CASE WHEN _d <> 0 THEN _d < 0 ELSE {after_month} END"
    hours = "CAST(ABS(_u) / 3600000000 AS BIGINT)"
    minutes = "CAST(ABS(_u) % 3600000000 / 60000000 AS BIGINT)"
    seconds = "CAST(ABS(_u) % 60000000 / 1000000 AS BIGINT)"
    fraction = "(ABS(_u) % 1000000)"

    def two(amount: str) -> str:
        return f"LPAD(CAST({amount} AS TEXT), 2, '0')"

    clock = (
        f"CASE WHEN _u < 0 THEN '-' WHEN {after_day} THEN '+' ELSE '' END || "
        f"{two(hours)} || ':' || {two(minutes)} || ':' || {two(seconds)} || "
        f"CASE WHEN {fraction} > 0 THEN '.' || "
        f"RTRIM(LPAD(CAST({fraction} AS TEXT), 6, '0'), '0') ELSE '' END"
    )
    text = (
        f"LTRIM({field(years, 'year', 'false')} || {field(months, 'mon', f'{years} < 0')} || "
        f"{field('_d', 'day', after_month)} || CASE WHEN _u <> 0 OR (_m = 0 AND _d = 0) "
        f"THEN ' ' || {clock} ELSE '' END, ' ')"
    )
    m, d, u = _interval_fields("_i")
    return _bound(
        f"CAST({value} AS INTERVAL)",
        _bound(m, _bound(d, _bound(u, text, "_u"), "_d"), "_m"),
        "_i",
    )


_INTERVAL_WORDS = "(year|mon|month|week|day|hour|min|minute|sec|second)s?"
_INTERVAL_SHAPE = (
    f"(([+-]?[0-9]+ {_INTERVAL_WORDS})( [+-]?[0-9]+ {_INTERVAL_WORDS})*"
    f"( [+-]?{_CLOCK})?|[+-]?{_CLOCK})"
)


def _read_interval() -> str:
    return _bound(
        _trimmed("v"),
        f"CASE WHEN _s ~ '^{_INTERVAL_SHAPE}$' THEN CAST(_s AS INTERVAL) "
        f"ELSE {_no_value('interval', 'v', 'INTERVAL', '22007')} END",
        "_s",
    )


def _read_numeric() -> str:
    """numeric_in: its text, exact and of any length, as numeric_out writes it. An exponent is not
    read here."""
    unsigned = "LTRIM(_w, '+-')"
    whole = f"COALESCE(NULLIF(LTRIM({_before(unsigned, '.')}, '0'), ''), '0')"
    part = _after("_w", ".")
    magnitude = f"{whole} || CASE WHEN {part} = '' THEN '' ELSE '.' || {part} END"
    return _bound(
        f"LOWER({_trimmed('v')})",
        "CASE WHEN _w IN ('nan', '+nan', '-nan') THEN 'NaN' "
        "WHEN _w IN ('inf', '+inf', 'infinity', '+infinity') THEN 'Infinity' "
        "WHEN _w IN ('-inf', '-infinity') THEN '-Infinity' "
        "WHEN _w ~ '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)$' THEN "
        f"CASE WHEN _w ~ '^-' AND _w ~ '[1-9]' THEN '-' ELSE '' END || {magnitude} "
        f"ELSE {_no_value('numeric', 'v', 'TEXT')} END",
        "_w",
    )


def _read_bytea() -> str:
    """byteain: its hex form, or text with no escape in it."""
    return _bound(
        "CAST(v AS TEXT)",
        "CASE WHEN _t ~ '^\\\\\\\\x([0-9a-fA-F]{2})*$' THEN UNHEX(SUBSTR(_t, 3)) "
        "WHEN _t ~ '\\\\\\\\' THEN "
        + _not_served("byteain", "BYTEA")
        + " ELSE ENCODE(_t, 'utf-8') END",
        "_t",
    )


def _colons(digits: str, pairs: int) -> str:
    """Hex digits as a MAC address writes them."""
    return " || ':' || ".join(f"SUBSTR({digits}, {i * 2 + 1}, 2)" for i in range(pairs))


_PAIR = "[0-9A-Fa-f]{2}"


def _read_mac() -> str:
    """macaddr_in's six forms."""
    four, six = "[0-9A-Fa-f]{4}", "[0-9A-Fa-f]{6}"
    forms = "|".join(
        (
            f"({_PAIR}:){{5}}{_PAIR}",
            f"({_PAIR}-){{5}}{_PAIR}",
            f"{six}[:-]{six}",
            f"{four}[.]{four}[.]{four}",
            f"{four}-{four}-{four}",
            f"{six}{six}",
        )
    )
    return _bound(
        _trimmed("v"),
        f"CASE WHEN NOT _s ~ '^({forms})$' THEN {_no_value('macaddr', 'v', 'TEXT')} "
        f"ELSE {_bound(_hex_of('_s'), _colons('_h', 6), '_h')} END",
        "_s",
    )


def _read_mac8() -> str:
    """macaddr8_in: six bytes gain the ff:fe of EUI-64."""
    form = f"({_PAIR}[:.-]?){{5,7}}{_PAIR}"
    eight = "CASE WHEN LENGTH(_h) = 12 THEN LEFT(_h, 6) || 'fffe' || RIGHT(_h, 6) ELSE _h END"
    return _bound(
        _trimmed("v"),
        f"CASE WHEN NOT _s ~ '^{form}$' OR LENGTH({_hex_of('_s')}) NOT IN (12, 16) "
        f"THEN {_no_value('macaddr8', 'v', 'TEXT')} "
        f"ELSE {_bound(_hex_of('_s'), _bound(eight, _colons('_e', 8), '_e'), '_h')} END",
        "_s",
    )


def _lsn_read() -> str:
    def part(digits: str) -> str:
        return f"COALESCE(NULLIF(LTRIM(UPPER({digits}), '0'), ''), '0')"

    return _bound(
        _trimmed("v"),
        f"CASE WHEN NOT _s ~ '^[0-9A-Fa-f]{{1,8}}/[0-9A-Fa-f]{{1,8}}$' "
        f"THEN {_no_value('pg_lsn', 'v', 'TEXT')} ELSE {part(_before('_s', '/'))} || '/' || {part(_after('_s', '/'))} END",
        "_s",
    )


def _tid_read() -> str:
    return _bound(
        _trimmed("v"),
        f"CASE WHEN NOT _s ~ '^[(][0-9]+,[0-9]+[)]$' OR {_tid_part('_s', 1)} > 4294967295 "
        f"OR {_tid_part('_s', 2)} > 65535 THEN {_no_value('tid', 'v', 'TEXT')} ELSE _s END",
        "_s",
    )


def _read_bits() -> str:
    return (
        "CASE WHEN CAST(v AS TEXT) ~ '[^01]' THEN "
        + refused(
            "22P02",
            "'\"' || SUBSTR(REGEXP_REPLACE(CAST(v AS TEXT), '[01]', '', 'g'), 1, 1) || "
            "'\" is not a valid binary digit'",
            "TEXT",
        )
        + " ELSE CAST(v AS TEXT) END"
    )


def _utf8(value: str) -> str:
    return f"ENCODE(CAST({value} AS TEXT), 'utf-8')"


def _bytes(*hexes: str) -> str:
    return f"UNHEX({' || '.join(hexes)})"


#: The engine's own text of the type is PostgreSQL's, and a cast reads what the shape names.
_DATED_IO = (
    ("date", "DATE", "date", f"{_ISO_DATE}{_INFINITE}"),
    ("time", "TIME", "time", _CLOCK),
    ("timetz", "TIMETZ", "time with time zone", f"{_CLOCK}{_OFFSET}"),
    (
        "timestamp",
        "TIMESTAMP",
        "timestamp",
        f"{_ISO_DATE}([ T]{_CLOCK})?{_INFINITE}",
    ),
    (
        "timestamptz",
        "TIMESTAMPTZ",
        "timestamp with time zone",
        f"{_ISO_DATE}([ T]{_CLOCK}{_OFFSET}?)?{_INFINITE}",
    ),
)


def _dated_bytes() -> tuple[Form, ...]:
    days = "(CAST(v AS DATE) - CAST('2000-01-01' AS DATE))"
    micros = "(EXTRACT(HOUR FROM v) * 3600000000 + EXTRACT(MINUTE FROM v) * 60000000 + EXTRACT(MICROSECOND FROM v))"
    since = "CAST(EXTRACT(EPOCH FROM CAST(v AS TIMESTAMP) - CAST('2000-01-01' AS TIMESTAMP)) * 1000000 AS BIGINT)"
    return (
        Form("date_send", "v", _bytes(_big_endian(f"CAST({days} AS BIGINT)", 32))),
        Form("time_send", "v", _bytes(_big_endian(f"CAST({micros} AS BIGINT)", 64))),
        Form("timestamp_send", "v", _bytes(_big_endian(since, 64))),
    )


def _io(stem: str, read: str, text: str, send: str | None, separator: str = "") -> list[Form]:
    forms = [
        Form(f"{stem}{separator}in", "v", read),
        Form(f"{stem}{separator}out", "v", text),
    ]
    if send is not None:
        forms.append(Form(f"{stem}{separator}send", "v", send))
    return forms


def _text_io(stem: str, read: str = _TEXT, separator: str = "") -> list[Form]:
    written = read.replace("{}", "v")
    return _io(stem, written, written, _utf8(written), separator)


def _shaped(function: str, pattern: str, sql_type: str = "TEXT") -> str:
    return (
        f"CASE WHEN CAST(v AS TEXT) ~ '^{pattern}$' THEN CAST(v AS TEXT) "
        f"ELSE {_not_served(function, sql_type)} END"
    )


#: A number as float8out writes it.
_NUMBER = r"-?(0|[1-9][0-9]{0,14})([.][0-9]*[1-9])?(e[+-][0-9]{2,3})?"
_POINT = rf"[(]{_NUMBER},{_NUMBER}[)]"
#: A shape's text as its out function writes it: read as it stands, any other refused by name.
_SHAPES = (
    ("point", _POINT),
    ("line", rf"[{{]{_NUMBER},{_NUMBER},{_NUMBER}[}}]"),
    ("lseg", rf"\[{_POINT},{_POINT}\]"),
    ("path", rf"([(]{_POINT}(,{_POINT})*[)]|\[{_POINT}(,{_POINT})*\])"),
    ("poly", rf"[(]{_POINT}(,{_POINT})*[)]"),
    ("circle", rf"<{_POINT},{_NUMBER}>"),
)
#: A pseudo-type's input and output, and whether its input takes a type and a typmod.
_PSEUDO = (
    ("any", False), ("anyarray", False), ("anycompatible", False), ("anycompatiblearray", False),
    ("anycompatiblenonarray", False), ("anyelement", False), ("anyenum", False),
    ("anynonarray", False), ("anycompatiblemultirange", True), ("anycompatiblerange", True),
    ("anymultirange", True), ("anyrange", True),
)  # fmt: skip


def _pseudo() -> list[Form]:
    """A pseudo-type's I/O: PostgreSQL has it refuse any value."""
    forms = []
    for name, sized in _PSEUDO:
        accepted = _fails("0A000", f"cannot accept a value of type {name}", "TEXT")
        shown = _fails("0A000", f"cannot display a value of type {name}", "TEXT")
        forms.append(
            Form(
                f"{name}_in",
                "v, o, m" if sized else "v",
                f"CASE WHEN v IS NOT NULL THEN {accepted} END",
            )
        )
        forms.append(Form(f"{name}_out", "v", f"CASE WHEN v IS NOT NULL THEN {shown} END"))
    return forms


_SIZED = "'(' || CAST(v - 4 AS TEXT) || ')'"
_PRECISION = "'(' || CAST(v AS TEXT) || ')'"


def _typmod_out(stem: str, answer: str, least: int = 0) -> Form:
    return Form(f"{stem}typmodout", "v", f"CASE WHEN v < {least} THEN '' ELSE {answer} END")


def _time_typmod(stem: str, zone: str) -> Form:
    precision = f"CASE WHEN v < 0 THEN '' ELSE {_PRECISION} END"
    return Form(f"{stem}typmodout", "v", f"{precision} || ' {zone} time zone'")


_TYPMODS: tuple[Form, ...] = (
    _typmod_out("varchar", f"CASE WHEN v > 4 THEN {_SIZED} ELSE '' END"),
    _typmod_out("bpchar", f"CASE WHEN v > 4 THEN {_SIZED} ELSE '' END"),
    _typmod_out("bit", _PRECISION),
    _typmod_out("varbit", _PRECISION),
    _typmod_out(
        "numeric",
        "'(' || CAST(((v - 4) >> 16) & 65535 AS TEXT) || ',' || "
        "CAST((((v - 4) & 2047) # 1024) - 1024 AS TEXT) || ')'",
        4,
    ),
    _time_typmod("time", "without"),
    _time_typmod("timetz", "with"),
    _time_typmod("timestamp", "without"),
    _time_typmod("timestamptz", "with"),
)


def _integer_io(stem: str, sql_type: str, pg_name: str, bits: int) -> list[Form]:
    return _io(
        stem,
        _read_integer(sql_type, pg_name, bits),
        f"CAST(CAST(v AS {sql_type}) AS TEXT)",
        _bytes(_big_endian(f"CAST(v AS {sql_type})", bits)),
    )


def _unsigned_io(stem: str, pg_name: str, limit: str) -> list[Form]:
    return _io(
        stem,
        _read_unsigned(pg_name, limit),
        "CAST(CAST(v AS BIGINT) AS TEXT)",
        _bytes(_big_endian("CAST(v AS BIGINT)", 32)),
    )


_BOOLEAN_BYTES = "UNHEX(CASE WHEN CAST(v AS BOOLEAN) THEN '01' ELSE '00' END)"
_BYTEA_TEXT = "'\\x' || LOWER(HEX(CAST(v AS BYTEA)))"
_MAC_BYTES = f"UNHEX({_hex_of('v')})"
_LSN_BYTES = f"UNHEX({_lsn_key('v')})"
_TID_BYTES = _bytes(_big_endian(_tid_part("v", 1), 32), _big_endian(_tid_part("v", 2), 16))
_JSON_IN = (
    "CASE WHEN TRY_CAST(v AS JSON) IS NULL AND v IS NOT NULL THEN "
    + _fails("22P02", "invalid input syntax for type json", "TEXT")
    + " ELSE CAST(v AS TEXT) END"
)

_IO: tuple[Form, ...] = (
    *(
        form
        for stem, sql_type, pg_name, _, bits in _INTEGERS
        for form in _integer_io(stem, sql_type, pg_name, bits)
    ),
    *_io(
        "float4",
        _read_float("REAL", "real"),
        _float_text("CAST(v AS REAL)", _FLOAT4_TEXT),
        None,
    ),
    *_io(
        "float8",
        _read_float("DOUBLE PRECISION", "double precision"),
        _float_text("CAST(v AS DOUBLE PRECISION)", _FLOAT8_TEXT),
        None,
    ),
    *_io(
        "bool",
        _read_bool(),
        "CASE CAST(v AS BOOLEAN) WHEN true THEN 't' WHEN false THEN 'f' END",
        _BOOLEAN_BYTES,
    ),
    *_unsigned_io("oid", "oid", "4294967295"),
    *_unsigned_io("xid", "xid", "4294967295"),
    *_unsigned_io("cid", "cid", "4294967295"),
    Form("xid8in", "v", _read_unsigned("xid8", "18446744073709551615")),
    Form("xid8out", "v", "CAST(v AS TEXT)"),
    *_text_io("text"),
    *_text_io("cstring", separator="_"),
    Form("unknownout", "v", _TEXT.replace("{}", "v")),
    Form("unknownsend", "v", _utf8("v")),
    *_text_io("name", _NAME),
    *_text_io("char", "LEFT(CAST({} AS TEXT), 1)"),
    Form("varcharout", "v", _TEXT.replace("{}", "v")),
    Form("varcharsend", "v", _utf8("v")),
    Form("bpcharout", "v", _TEXT.replace("{}", "v")),
    Form("bpcharsend", "v", _utf8("v")),
    Form("varcharin", "v, o, m", "CASE WHEN m < 4 THEN CAST(v AS TEXT) ELSE LEFT(v, m - 4) END"),
    Form(
        "bpcharin",
        "v, o, m",
        "CASE WHEN m < 4 THEN CAST(v AS TEXT) ELSE RPAD(LEFT(v, m - 4), m - 4, ' ') END",
    ),
    *_io("bytea", _read_bytea(), _BYTEA_TEXT, "CAST(v AS BYTEA)"),
    *(
        form
        for stem, sql_type, pg_name, shape in _DATED_IO
        for form in (
            Form(
                f"{stem}_in",
                "v" if stem == "date" else "v, o, m",
                _read_dated(sql_type, pg_name, shape),
            ),
            Form(f"{stem}_out", "v", f"CAST(CAST(v AS {sql_type}) AS TEXT)"),
        )
    ),
    *_dated_bytes(),
    Form("interval_in", "v, o, m", _read_interval()),
    Form("interval_out", "v", _interval_text("v")),
    *_io(
        "uuid",
        "CASE WHEN TRY_CAST(v AS UUID) IS NULL AND v IS NOT NULL THEN "
        + _no_value("uuid", "v", "TEXT")
        + " ELSE CAST(CAST(v AS UUID) AS TEXT) END",
        "CAST(CAST(v AS UUID) AS TEXT)",
        f"UNHEX({_hex_of('v')})",
        "_",
    ),
    *_io("macaddr", _read_mac(), _colons(_hex_of("v"), 6), _MAC_BYTES, "_"),
    *_io("macaddr8", _read_mac8(), _colons(_hex_of("v"), 8), _MAC_BYTES, "_"),
    *_io("pg_lsn", _lsn_read(), _lsn_read(), _LSN_BYTES, "_"),
    *_io("tid", _tid_read(), "CAST(v AS TEXT)", _TID_BYTES),
    Form("bit_in", "v, o, m", _read_bits()),
    Form("bit_out", "v", "CAST(v AS TEXT)"),
    Form("varbit_in", "v, o, m", _read_bits()),
    Form("varbit_out", "v", "CAST(v AS TEXT)"),
    Form("numeric_in", "v, o, m", _read_numeric()),
    Form("numeric_out", "v", "CAST(v AS TEXT)"),
    *_io("json", _JSON_IN, "CAST(v AS TEXT)", _utf8("v"), "_"),
    Form("xml_out", "v", "CAST(v AS TEXT)"),
    *(
        form
        for name, pattern in _SHAPES
        for form in (
            Form(f"{name}_in", "v", _shaped(f"{name}_in", pattern)),
            Form(f"{name}_out", "v", _shaped(f"{name}_out", pattern)),
        )
    ),
    *_pseudo(),
    *_TYPMODS,
)

OPERATORS: tuple[Form, ...] = _OPERATORS
IO: tuple[Form, ...] = _IO
