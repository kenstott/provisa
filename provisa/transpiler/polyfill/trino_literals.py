# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e4c91-6d3a-4f58-9e0c-a81f5d3b7c26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A literal Trino's cast reads otherwise than PostgreSQL's input function (REQ-2236; the function
support measure's edge values, run 44670380355, REQ-2171). Each is read where the statement is
transpiled, as postgres:17.11 reads it, and each answered a wrong value on Trino before.

oid and integers: oidin and int4in's ranges and texts, as on ClickHouse (provisa.transpiler.polyfill
.clickhouse's readers); Trino held 4294967296 as an oid and answered of it.

numeric: a literal cast to a numeric of no precision is cast to a decimal holding its digits
(provisa.transpiler.pg_literals.exact_inputs). Past 38 digits Trino's DECIMAL holds none, and its
cast of DECIMAL (38, 0) dropped the fraction: refused by name. NaN and infinity Trino's decimal
holds neither; a text the input does not read is PostgreSQL's 22P02.

timestamp and date: Trino's TIMESTAMP holds microseconds since the epoch in 64 bits, to
294247-01-10 04:00:54.775807, and wrapped a later one around to a year BC. A text past PostgreSQL's
own range (294276 AD for a timestamp, 5874897 AD for a date) is PostgreSQL's 22008; one past
Trino's, below PostgreSQL's, is refused by name.

time and timestamp precision: Trino's TIME and TIMESTAMP of no precision are of milliseconds, and
rounded 23:59:59.999999 to the next day; a text of more than three digits of a second's fraction is
cast to its type of microseconds, PostgreSQL's.

"char": Trino's CHAR(1) holds an empty text as a space, which text("char") answered; a "char" is
held as the VARCHAR of its one byte, as charin's form holds it."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import calendar
import re

import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.polyfill import clickhouse, trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite

FAMILY = "literals"

#: The last instant Trino's TIMESTAMP(6) holds: 2^63 - 1 microseconds after the epoch.
TRINO_STAMP_END = "294247-01-10 04:00:54.775807"
_TRINO_STAMP_END = (294247, 1, 10, 4, 0, 54, 775807)
#: The last instant and day PostgreSQL's timestamp and date hold.
_PG_STAMP_END = (294276, 12, 31, 23, 59, 59, 999999)
_PG_LAST_YEAR = 5874897

#: A timestamp's or a date's text in ISO form (DecodeDateTime's common case): a year of any
#: digits, a month, a day, and optionally a time of day with a fraction of a second.
_STAMP_TEXT = re.compile(
    r"\s*(\d{4,})-(\d{1,2})-(\d{1,2})"
    r"(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d+))?)?)?\s*",
    re.ASCII,
)
#: A time of day's text with a fraction of a second.
_CLOCK_TEXT = re.compile(r"\s*\d{1,2}:\d{1,2}:\d{1,2}\.(\d+)\s*", re.ASCII)
#: Trino's TIME and TIMESTAMP of no precision hold milliseconds.
_TRINO_DIGITS = 3


def _plain_text_cast(node: exp.Expr, *types: exp.DType) -> tuple[exp.Cast, str] | None:
    """``node``, a cast of a text the statement writes to one of ``types`` that raises on failure,
    of no precision, with the text; None for any other node."""
    cast = clickhouse._plain_cast(node, *types)  # noqa: SLF001 -- the one reader of a plain cast
    if cast is None or cast.to.expressions:
        return None
    text = clickhouse._text_of(cast.this)  # noqa: SLF001 -- the one reader of a literal's text
    return None if text is None else (cast, text)


def _of_microseconds(cast: exp.Cast, fraction: str | None) -> exp.Expr | None:
    """``cast`` of its type of microseconds where ``fraction`` has more digits than Trino's type
    of no precision holds; None where it has none more."""
    if fraction is None or len(fraction) <= _TRINO_DIGITS:
        return None
    to = cast.to.copy()
    to.set("expressions", [exp.DataTypeParam(this=exp.Literal.number(6))])
    return exp.Cast(this=cast.this.copy(), to=to)


def _instant(read: re.Match[str]) -> tuple[int, ...]:
    year, month, day, hour, minute, second = (int(read[i] or 0) for i in range(1, 7))
    micros = int((read[7] or "0")[:6].ljust(6, "0"))
    return (year, month, day, hour, minute, second, micros)


#: A date's, a timestamp's or a timestamptz's text of a year before Christ (DecodeDateTime's ISO
#: form with BC): a year, a month, a day, a time of day with a fraction, an offset, BC.
_BC_TEXT = re.compile(
    r"\s*(\d{1,4})-(\d{1,2})-(\d{1,2})"
    r"(?:[ T](\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?)?"
    r"\s*(?:([+-])(\d{1,2})(?::?(\d{2}))?)?\s+BC\s*",
    re.ASCII | re.IGNORECASE,
)
#: PostgreSQL's first day, 4714-11-24 BC: astronomical year -4713.
_PG_FIRST_DAY = (-4713, 11, 24)
_EPOCH = "DATE '1970-01-01'"


def days_from_civil(year: int, month: int, day: int) -> int:
    """The days from 1970-01-01 to a day of the proleptic Gregorian calendar, its year astronomical
    (1 BC is 0): Trino's DATE and PostgreSQL's date count the same calendar."""
    year -= month <= 2  # noqa: PLR2004 -- March starts the computing year
    era = year // 400  # Python floors a negative quotient, as the C form arranges
    of_era = year - era * 400
    of_year = (153 * (month + (-3 if month > 2 else 9)) + 2) // 5 + day - 1  # noqa: PLR2004
    of_era_days = of_era * 365 + of_era // 4 - of_era // 100 + of_year
    return era * 146097 + of_era_days - 719468


def _bc(text: str, kind: str) -> tuple[int, int] | None:
    """A text of a year BC as the days from the epoch and the milliseconds of its day (an offset
    taken off: the instant in UTC); None for another text. PostgreSQL's 22008 before its first day;
    a fraction of less than a millisecond is refused by name (Trino adds no microseconds)."""
    read = _BC_TEXT.fullmatch(text)
    if read is None:
        return None
    year, month, day = 1 - int(read[1]), int(read[2]), int(read[3])
    if not 1 <= month <= 12 or not 1 <= day <= calendar.monthrange(year % 400 + 2000, month)[1]:  # noqa: PLR2004
        return None  # a day that does not exist: the engine's cast refuses it
    if (year, month, day) < _PG_FIRST_DAY:
        raise Refused(Refusal("22008", f'{kind} out of range: "{text}"'))
    fraction = (read[7] or "").ljust(6, "0")
    micros = int(fraction[:6])
    if micros % 1000:
        raise Refused(
            Refusal("0A000", f"{kind}_in() of a year BC to the microsecond has no form on trino")
        )
    seconds = int(read[4] or 0) * 3600 + int(read[5] or 0) * 60 + int(read[6] or 0)
    if read[8]:
        offset = int(read[9]) * 3600 + int(read[10] or 0) * 60
        seconds -= offset if read[8] == "+" else -offset
    days, seconds = divmod(seconds, 86400)
    return days_from_civil(year, month, day) + days, seconds * 1000 + micros // 1000


def _bc_day(days: int) -> exp.Expr:
    return sqlglot.parse_one(f"date_add('day', {days}, {_EPOCH})", read="trino")


def _bc_stamp(days: int, millis: int) -> exp.Expr:
    return sqlglot.parse_one(
        f"date_add('millisecond', {millis}, CAST(date_add('day', {days}, {_EPOCH}) AS TIMESTAMP(3)))",
        read="trino",
    )


def bc_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text of a year BC cast to a date, a timestamp or a timestamptz: Trino's cast reads no BC,
    so the value is the days from the epoch (and the milliseconds of its day), which Trino's DATE
    and TIMESTAMP hold, as Trino's date_add writes them; a timestamptz's instant in UTC."""
    plain = _plain_text_cast(
        node, exp.DataType.Type.DATE, exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ
    )
    if plain is None or "bc" not in plain[1].lower():
        return None
    cast, text = plain
    if cast.to.is_type(exp.DataType.Type.DATE):
        read = _bc(text, "date")
        return None if read is None else _said(_bc_day(read[0]), cast)
    zoned = cast.to.is_type(exp.DataType.Type.TIMESTAMPTZ)
    read = _bc(text, "timestamp with time zone" if zoned else "timestamp")
    if read is None:
        return None
    stamp = _bc_stamp(*read)
    if zoned:
        stamp = exp.Anonymous(this="with_timezone", expressions=[stamp, exp.Literal.string("UTC")])
    return _said(stamp, cast)


def _said(value: exp.Expr, cast: exp.Cast) -> exp.Cast:
    """``value`` cast to the type the statement said (a cast of its own type on Trino): a row that
    reads a call after its arguments are written (to_char's) still reads the value's type."""
    return exp.Cast(this=value, to=cast.to.copy())


def stamp_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text cast to a timestamp: PostgreSQL's 22008 past its range, refused by name past Trino's,
    and cast to TIMESTAMP(6) where it has a fraction of more than milliseconds."""
    plain = _plain_text_cast(node, exp.DataType.Type.TIMESTAMP)
    if plain is None:
        return None
    cast, text = plain
    read = _STAMP_TEXT.fullmatch(text)
    if read is None:
        return None
    instant = _instant(read)
    if instant > _PG_STAMP_END:
        raise Refused(Refusal("22008", f'timestamp out of range: "{text}"'))
    if instant > _TRINO_STAMP_END:
        raise Refused(
            Refusal(
                "0A000",
                f"timestamp_in() of a value past {TRINO_STAMP_END} has no exact form on {dialect}",
            )
        )
    return _of_microseconds(cast, read[7])


def date_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text cast to a date: PostgreSQL's 22008 past its last day, which Trino's DATE holds."""
    plain = _plain_text_cast(node, exp.DataType.Type.DATE)
    if plain is None:
        return None
    _, text = plain
    read = _STAMP_TEXT.fullmatch(text)
    if read is None or read[4] is not None:
        return None
    year, month, day = int(read[1]), int(read[2]), int(read[3])
    if year <= _PG_LAST_YEAR or not 1 <= month <= 12:
        return None
    if not 1 <= day <= calendar.monthrange(year % 400 + 2000, month)[1]:
        return None  # a day that does not exist: Trino's cast refuses it
    raise Refused(Refusal("22008", f'date out of range: "{text}"'))


def held_literal(node: exp.Expr, dialect: str) -> exp.Expr:
    """``node`` as this module's rows read it, for a rewrite that keeps it as a hole of what it
    writes (the walk does not descend into what a rule writes): a text of a year BC as its days,
    a date or a timestamp past PostgreSQL's range its 22008, past Trino's refused by name, a
    numeric of more digits than Trino's decimal holds refused by name. A node no row reads is the
    value itself."""
    for read in (bc_read, date_read, stamp_read, numeric_read):
        written = read(node, dialect)
        if written is not None:
            return written
    return node


def clock_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text cast to a time of day of a fraction of more than milliseconds, cast to TIME(6)."""
    plain = _plain_text_cast(node, exp.DataType.Type.TIME)
    if plain is None:
        return None
    cast, text = plain
    read = _CLOCK_TEXT.fullmatch(text)
    return None if read is None else _of_microseconds(cast, read[1])


def _is_one_byte_char(to: exp.DataType) -> bool:
    """Whether ``to`` is PostgreSQL's one-byte "char" (a quoted name; character is CHAR)."""
    kind = to.args.get("kind")
    return (
        to.this == exp.DataType.Type.USERDEFINED
        and isinstance(kind, exp.Identifier)
        and kind.quoted
        and kind.name == "char"
    )


def one_byte_char_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text cast to "char", read as charin reads it: its first byte, or the empty text of an
    empty one, held as a VARCHAR (Trino's CHAR(1) holds an empty text as a space). A first
    character of more than one byte is refused by name, as charin's form refuses it."""
    if not isinstance(node, exp.Cast) or isinstance(node, exp.TryCast):
        return None
    if not _is_one_byte_char(node.to):
        return None
    text = clickhouse._text_of(node.this)  # noqa: SLF001 -- the one reader of a literal's text
    if text is None:
        return None
    first = text[:1]
    if len(first.encode()) > 1:
        raise Refused(Refusal("0A000", "charin does not read this value"))
    return exp.Cast(this=exp.Literal.string(first), to=exp.DataType.build("VARCHAR"))


#: Each function of one character value whose answer reads its text without trailing spaces
#: (bpchar's rtrim to text, bpcharlen), and its Trino form of that text ``$t``.
_UNPADDED = {
    "length": "length($t)",
    "char_length": "length($t)",
    "character_length": "length($t)",
    "text": "$t",
    "name": "substr($t, 1, 63)",
}


def unpadded(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A function of a character (bpchar) value the statement says, of its text without trailing
    spaces: Trino's CHAR keeps them in its VARCHAR and its length, PostgreSQL's bpchar drops them
    (''::character is of length 0, its text '')."""
    call = trino.called(node)
    if call is None or len(call[1]) != 1:
        return None
    # sqlglot's Length answers to each name of a length, which have one form
    forms = {_UNPADDED[name] for name in call[0] & set(_UNPADDED)}
    said = trino.said(call[1][0])
    if len(forms) != 1 or said is None or not said.startswith("character"):
        return None
    if said.startswith("character varying"):
        return None
    form = forms.pop().replace("$t", "rtrim(CAST($a AS VARCHAR), ' ')")
    return trino.written(form, "f($a)", {"a": call[1][0]})


#: The digits of a bigint.
_BIGINT_DIGITS = 19


def _decimal_type(node: exp.Expr) -> tuple[int, int] | None:
    """The precision and scale of the decimal a statement casts ``node`` to; None for another."""
    value = node.unnest()
    if not isinstance(value, exp.Cast) or not value.to.is_type(exp.DataType.Type.DECIMAL):
        return None
    said = [int(param.this.this) for param in value.to.expressions]
    if not said:
        return None
    return said[0], said[1] if len(said) > 1 else 0


def int8_sum_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """int8_sum(numeric, bigint), a step of a bigint's sum, not strict: the bigint as a decimal of
    its 19 digits. Where the numeric's type and a bigint's need more than 38 digits Trino would
    hold the sum at a smaller scale, so the call is refused by name."""
    call = trino.called(node)
    if call is None or "int8_sum" not in call[0] or len(call[1]) != 2:  # noqa: PLR2004
        return None
    try:
        state, value = (held_literal(arg, dialect) for arg in call[1])
    except Refused as refused:  # a rule read before any pattern: its refusal is the client's
        raise Refused(refused.refusal, ruling=True) from refused
    said = _decimal_type(state)
    if said is not None:
        precision, scale = said
        if max(precision - scale, _BIGINT_DIGITS) + 1 + scale > _MOST_DIGITS:
            raise Refused(
                Refusal(
                    "0A000",
                    f"int8_sum() of a numeric of scale past {_MOST_DIGITS - _BIGINT_DIGITS - 1} "
                    f"has no exact form on {dialect}",
                ),
                ruling=True,  # a rule read before any pattern: its refusal is the client's
            )
    big = "CAST($b AS DECIMAL(19, 0))"
    form = (
        f"CASE WHEN $a IS NULL AND $b IS NULL THEN NULL WHEN $a IS NULL THEN {big} "
        f"WHEN $b IS NULL THEN $a ELSE $a + {big} END"
    )
    return trino.written(form, "int8_sum($a, $b)", {"a": state, "b": value})


#: The scale numeric_mod's form computes its remainder at (provisa.transpiler.pg_forms_types).
_MOD_SCALE = 18


def numeric_mod_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """numeric_mod of a numeric the statement says is of a scale past 18, refused by name: the
    form's remainder is computed at scale 18 (DECIMAL(38, 18)), which drops the digits past it.
    Any other call keeps the form."""
    call = trino.called(node)
    if call is None or "numeric_mod" not in call[0] or len(call[1]) != 2:  # noqa: PLR2004
        return None
    scales = [said[1] for said in map(_decimal_type, call[1]) if said is not None]
    if not scales or max(scales) <= _MOD_SCALE:
        return None
    raise Refused(
        Refusal(
            "0A000",
            f"numeric_mod() of a numeric of scale past {_MOD_SCALE} has no exact form on {dialect}",
        ),
        ruling=True,  # a rule read before any pattern: its refusal is the client's
    )


#: The decimal digits Trino's DECIMAL holds.
_MOST_DIGITS = 38
NUMERIC_PAST = f"numeric_in() of a value of more than {_MOST_DIGITS} digits has no exact form on"
NUMERIC_NOT_FINITE = "numeric_in() of NaN or infinity has no form on"


def numeric_read(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text cast to a numeric of no precision that exact_inputs left so (it is cast to a decimal
    of its digits where Trino holds them): refused by name past 38 digits and of NaN or infinity,
    PostgreSQL's 22P02 of a text the input does not read."""
    plain = _plain_text_cast(node, exp.DataType.Type.DECIMAL)
    if plain is None:
        return None
    _, text = plain
    value = clickhouse._numeric_of_text(text)  # noqa: SLF001 -- numeric_in's one reader
    if value is None:
        raise Refused(Refusal("22P02", f'invalid input syntax for type numeric: "{text}"'))
    if not value.is_finite():
        raise Refused(Refusal("0A000", f"{NUMERIC_NOT_FINITE} {dialect}"))
    _, digits, exponent = value.as_tuple()
    if max(len(digits), -int(exponent)) > _MOST_DIGITS:
        raise Refused(Refusal("0A000", f"{NUMERIC_PAST} {dialect}"))
    return None


def _row(
    name: str, summary: str, read, mentions: str, limitation: str, first: bool = False
) -> Feature:  # noqa: ANN001 -- a rewrite function
    """A row of Trino's rewrite ``read``; ``first``: read before any row's pattern (a call
    another row's pattern matches on every dialect, length(bytea)'s)."""
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, trino.on_trino(Rewrite(read, mentions=mentions, first=first))),),
            limitations=dict.fromkeys(trino.TRINO, (Limitation(limitation),)),
        ),
    )


FEATURES: tuple[Feature, ...] = (
    _row(
        "an oid literal on trino",
        "A text cast to an oid.",
        clickhouse.oid_read,
        r"(?:::|\bas)\s*\"?oid\b",
        "A text the statement writes is read as PostgreSQL reads it: past -2147483648 to "
        "4294967295 it raises 22003, a text the input does not read 22P02, and a negative value is "
        "its 32 bits unsigned. A column's cast keeps Trino's own.",
    ),
    _row(
        "an integer literal on trino",
        "A literal cast to smallint, integer or bigint.",
        clickhouse.integer_read,
        r"[\d'.]\s*\)?\s*(?:::|\bas)\s*\"?(?:smallint|integer|bigint|int[248]?)\b"
        r"|\b(?:smallint|integer|bigint|int[248])\s+'",
        "A number or a text the statement writes is read as PostgreSQL reads it: a number "
        "rounded half away from zero, a text by the integer input; past the type it raises "
        "22003, a text the input does not read 22P02.",
    ),
    _row(
        "a numeric literal on trino",
        "A text cast to a numeric of no precision.",
        numeric_read,
        r"'\s*\)?\s*(?:::|\bas)\s*\"?(?:numeric|decimal)\b|\b(?:numeric|decimal)\s+'",
        f"A value of up to {_MOST_DIGITS} digits keeps them all; one of more raises 0A000 "
        f"({NUMERIC_PAST} trino). NaN and infinity raise 0A000 ({NUMERIC_NOT_FINITE} trino): "
        "Trino's decimal holds neither. A text the input does not read raises 22P02.",
    ),
    _row(
        "a timestamp literal on trino",
        "A text cast to a timestamp.",
        stamp_read,
        r"(?:::|\bas)\s*timestamp\b|\btimestamp\s+'",
        f"Held to {TRINO_STAMP_END}, the last microsecond Trino's TIMESTAMP holds: a text "
        "past it raises 0A000 (timestamp_in() of a value past it has no exact form on trino), "
        "one past PostgreSQL's range 22008. A fraction of more than milliseconds is read to the "
        "microsecond.",
    ),
    _row(
        "a date literal on trino",
        "A text cast to a date.",
        date_read,
        r"::\s*date\b|\bas\s+date\b|\bdate\s+'",
        f"A text past {_PG_LAST_YEAR}-12-31, PostgreSQL's last day, raises 22008 as "
        "PostgreSQL's input does, where Trino's DATE holds it.",
    ),
    _row(
        'a "char" literal on trino',
        'A text cast to the one-byte "char".',
        one_byte_char_read,
        r"(?:::|\bas)\s*\"char\"",
        "Held as a VARCHAR of its first byte, or empty, as charin reads it; a first character "
        "of more than one byte raises 0A000 (charin does not read this value).",
    ),
    _row(
        "a character's text on trino",
        "The text, length or name of a character (bpchar) value.",
        unpadded,
        r"(?:::|\bas)\s*(?:character|char|bpchar)\b",
        "Of a value the statement says is a character: its text without trailing spaces, as "
        "bpchar's text and length read it, where Trino's CHAR keeps them.",
        first=True,
    ),
    _row(
        "int8_sum on trino",
        "A step of a bigint's sum: a numeric plus a bigint.",
        int8_sum_read,
        r"\bint8_sum\b",
        "The bigint is a decimal of its 19 digits. Where the numeric's type and a bigint's "
        f"need more than {38} digits (a scale past 18) it raises 0A000 (int8_sum() of a "
        "numeric of scale past 18 has no exact form on trino): Trino would hold the sum at a "
        "smaller scale.",
        first=True,
    ),
    _row(
        "numeric_mod on trino",
        "The remainder of two numerics.",
        numeric_mod_read,
        r"\bnumeric_mod\b",
        f"Computed at scale {_MOD_SCALE}: of a numeric the statement says is of a scale past it "
        f"it raises 0A000 (numeric_mod() of a numeric of scale past {_MOD_SCALE} has no exact "
        "form on trino).",
        first=True,
    ),
    _row(
        "a year BC on trino",
        "A text of a year before Christ cast to a date, a timestamp or a timestamptz.",
        bc_read,
        r"\bBC\b",
        "Trino's cast reads no BC: the value is written as its days from 1970-01-01 (and the "
        "milliseconds of its day), which Trino's DATE and TIMESTAMP hold, a timestamptz's instant "
        "in UTC. Before 4714-11-24 BC it raises 22008 as PostgreSQL's input does; a fraction of "
        "less than a millisecond raises 0A000.",
    ),
    _row(
        "a time literal on trino",
        "A text cast to a time of day.",
        clock_read,
        r"(?:::|\bas)\s*time\b|\btime\s+'",
        "A fraction of more than milliseconds is read to the microsecond, where Trino's TIME of "
        "no precision rounds it to the millisecond.",
    ),
)
