# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b1d2e-8a47-4c95-b0e1-7d2c9a5e4f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A time of day on ClickHouse (REQ-2236, amended 2026-11-15; job J1391).

The ClickHouse server Provisa is run on (24.3 in CI) has no Time64 and no toTime64: only the
embedded chdb (26.x) has them. A time of day is held there as PostgreSQL holds it, an Int64 count
of microseconds of the day (0 .. 86 400 000 000, 24:00:00 the last), as an interval is held as
PostgreSQL's own three fields (polyfill.intervals) and a timetz as the pair of numbers the
maintainer ruled. Every comparison of two held times is ClickHouse's own comparison of integers,
so it is exact.

Where a time reaches the client -- a column of the statement's select list, or a cast of one to
text -- it is written as time_out's text (hours, minutes, seconds, and a fraction without its
trailing zeros), which pgwire reads back as the time it is (provisa.pgwire.cast_types
.clock_of_text), the mechanism an interval's answer uses (interval_out's text,
cast_types.interval_of_text). This module holds what the forms share: the held value of a literal,
its NULL, the text, and the mark a form puts on a node whose value is a held time."""

# Requirements: REQ-2236

from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import Literal

import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.polyfill import instants, trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Rewrite
from provisa.transpiler.polyfill.numbers import ch_fail

_CLICKHOUSE = "clickhouse"
_TIME = exp.DataType.Type.TIME
_STAMPS = (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ)
_TEXTS = (exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR)

#: The meta key of a node a form wrote whose value is a held time of day (an Int64 of its
#: microseconds).
HELD = "provisa_time"

SECOND_US = 1_000_000
DAY_US = 86_400 * SECOND_US

#: A NULL of a held time.
NULL_HELD = "CAST(NULL AS Nullable(Int64))"
#: A NULL of a time's text, where the select list shows it.
NULL_TEXT = "CAST(NULL AS Nullable(String))"


def _refused(function: str, refusal: Refusal) -> Exception:
    """The client's refusal ``refusal`` of a call of ``function`` that ClickHouse's semantics
    (provisa.transpiler.transpile.clickhouse_semantics), after the grid's rows, cannot tell it."""
    from provisa.transpiler.function_macros import RowRefused

    return RowRefused(function, _CLICKHOUSE, refusal)


def text_of(micros: int) -> str:
    """time_out of a time's microseconds of the day, in Python: 24:00:00 the last."""
    seconds, fraction = divmod(micros, SECOND_US)
    clock = f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"
    return f"{clock}.{fraction:06d}".rstrip("0") if fraction else clock


def held_literal(micros: int) -> exp.Expr:
    """A held time of a literal's microseconds."""
    node = exp.Anonymous(this="toInt64", expressions=[exp.Literal.number(micros)])
    node.meta[HELD] = True
    return node


def held_null() -> exp.Expr:
    node = sqlglot.parse_one(NULL_HELD, read=_CLICKHOUSE)
    node.meta[HELD] = True
    return node


#: The refusal of 24:00:00 where a column of the select list shows it: PostgreSQL's time holds it,
#: but pgwire reads no time of day past 23:59:59.999999 back (cast_types.clock_of_text).
LAST_SHOWN = Refusal(
    "0A000",
    "time 24:00:00 in a result column has no form on clickhouse: pgwire reads no time of day "
    "past 23:59:59.999999",
)


def text_sql(held: str, column: bool = False) -> str:
    """ClickHouse's SQL of time_out's text of a held time ``held`` (an Int64, maybe NULL): two
    digits each of hours, minutes and seconds, and the fraction without its trailing zeros.
    ``column``: the text is a result column's, which raises LAST_SHOWN of 24:00:00."""
    x = "provisa_clock"

    def two(count: str) -> str:
        return f"leftPad(toString({count}), 2, '0')"

    fraction = f"replaceRegexpOne(leftPad(toString({x} % {SECOND_US}), 6, '0'), '0+$', '')"
    written = (
        f"concat({two(f'intDiv({x}, 3600000000)')}, ':', {two(f'intDiv({x}, 60000000) % 60')}, "
        f"':', {two(f'intDiv({x}, {SECOND_US}) % 60')}, "
        f"if({x} % {SECOND_US} = 0, '', concat('.', {fraction})))"
    )
    if column:
        last = ch_fail(LAST_SHOWN.sqlstate, LAST_SHOWN.message).format(
            condition=f"({x} = {DAY_US})"
        )
        written = f"if({last} = 0, {written}, '')"
    return f"arrayElement(arrayMap({x} -> {written}, [toInt64({held})]), 1)"


def graft(template: str, **nodes: exp.Expr) -> exp.Expr:
    """ClickHouse's SQL ``template`` parsed, each column named by a key of ``nodes`` that node."""
    tree = sqlglot.parse_one(template, read=_CLICKHOUSE)

    def grafted(inner: exp.Expr) -> exp.Expr:
        if isinstance(inner, exp.Column) and inner.name in nodes:
            return nodes[inner.name].copy()
        return inner

    return tree.transform(grafted)


def shown(node: exp.Expr, column: bool = False) -> exp.Expr:
    """``node``, a held time, as time_out's text: a literal's text where the held value is a
    literal, a NULL of text for a NULL. ``column``: a result column's, which raises LAST_SHOWN of
    24:00:00."""
    value = node.unnest()
    if isinstance(value, exp.Anonymous) and value.name == "toInt64" and value.meta.get(HELD):
        micros = value.expressions[0]
        if isinstance(micros, exp.Literal) and not micros.is_string:
            if column and int(micros.this) == DAY_US:
                raise _refused("time_out", LAST_SHOWN)
            return exp.Literal.string(text_of(int(micros.this)))
    if isinstance(value, exp.Cast) and isinstance(value.this, exp.Null):
        return sqlglot.parse_one(NULL_TEXT, read=_CLICKHOUSE)
    return graft(text_sql("provisa_held_clock", column), provisa_held_clock=node)


def time_valued(node: exp.Expr) -> bool:
    """Whether ``node``, as the statement writes it or a form wrote it, is a time of day: a cast
    to time, a held time a form wrote, or a branch of them."""
    value = node.unnest()
    if value.meta.get(HELD):
        return True
    if isinstance(value, exp.Cast):
        return value.to.is_type(_TIME)
    if isinstance(value, exp.Case):
        branches = [i.args.get("true") for i in value.args.get("ifs") or []]
        branches.append(value.args.get("default"))
        return any(b is not None and time_valued(b) for b in branches)
    if isinstance(value, exp.If):
        return time_valued(value.args["true"])
    return False


def _date_valued(node: exp.Expr) -> bool:
    """Whether ``node`` is a date the statement says: a cast to date, or a date a form wrote as
    Date32 (polyfill.clickhouse's literal of a date)."""
    value = node.unnest()
    if isinstance(value, exp.Cast):
        return value.to.is_type(exp.DataType.Type.DATE, exp.DataType.Type.DATE32)
    if isinstance(value, exp.Anonymous):
        return value.name.lower() == "todate32"
    return isinstance(value, exp.Date)


#: A time's text as time_in reads it of a value no form reads at translation: a date before it
#: (which time_in reads past), hours, minutes, seconds, and up to six digits of a fraction.
_CLOCK_READ = (
    r"^\\s*(?:[0-9]{4}-[0-9]{2}-[0-9]{2}[ T])?([0-9]{1,2}):([0-9]{2})"
    r"(?::([0-9]{2})(?:[.]([0-9]{1,6}))?)?\\s*$"
)


def _read_text(value: str) -> str:
    """A held time of a value whose type the statement does not say (a table's column), read from
    its text at run time; PostgreSQL's 22007 of a text not of a clock, 22008 of a field past its
    range. A timestamp's text is read past its date, as time_in reads it."""
    unread = ch_fail("22007", "invalid input syntax for type time").format(
        condition="(isNotNull(provisa_t) AND empty(provisa_g))"
    )
    fields = "".join(
        f"toInt64OrZero(provisa_g[{i}])" + suffix
        for i, suffix in ((1, " * 60 + "), (2, ") * 60 + "), (3, ") * 1000000 + "))
    )
    fraction = "toInt64OrZero(rightPad(provisa_g[4], 6, '0'))"
    micros = f"((({fields}{fraction})"
    outside = ch_fail("22008", "date/time field value out of range").format(
        condition=(
            f"(toInt64OrZero(provisa_g[2]) > 59 OR toInt64OrZero(provisa_g[3]) > 60 "
            f"OR {micros} > {DAY_US})"
        )
    )
    body = (
        f"if(isNull(provisa_t) OR empty(provisa_g), {NULL_HELD}, "
        f"toNullable({micros} + {outside})) + {unread}"
    )
    return (
        f"arrayElement(arrayMap((provisa_t, provisa_g) -> {body}, [toString({value})], "
        f"[extractGroups(ifNull(toString({value}), ''), '{_CLOCK_READ}')]), 1)"
    )


def _of_stamp(stamp: str) -> str:
    """A held time of a timestamp held as DateTime64(6, 'UTC'): its wall time's microseconds of
    the day, exact (a day before 1970 has a negative count from the epoch)."""
    return (
        f"((toUnixTimestamp64Micro({stamp}) % toInt64({DAY_US}) + toInt64({DAY_US})) "
        f"% toInt64({DAY_US}))"
    )


def cast(
    node: exp.Cast, said: Callable[[exp.Expr], exp.Expr], micros_of: Callable[[str], int | None]
) -> exp.Expr:
    """A cast to time, held: a literal's microseconds (``micros_of`` reads its text as time_in
    does), a NULL, a time already held, a timestamp's time of day, or a text read at run time.
    ``said`` is what ClickHouse is told of any other node it holds."""
    value = node.this.unnest()
    while isinstance(value, exp.Cast) and value.to.is_type(_TIME):
        value = value.this.unnest()
    if value.meta.get(HELD):
        return value
    if isinstance(value, exp.Null):
        return held_null()
    if isinstance(value, exp.Literal) and value.is_string:
        micros = micros_of(value.this)
        if micros is None:
            raise _refused(
                "time_in",
                Refusal("0A000", f'time_in() of "{value.this}" has no form on {_CLICKHOUSE}'),
            )
        return held_literal(micros)
    stamp = isinstance(value, exp.CurrentTimestamp) or (
        isinstance(value, exp.Cast) and value.to.is_type(*_STAMPS)
    )
    template = _of_stamp("provisa_v") if stamp else _read_text("provisa_v")
    held = graft(template, provisa_v=value.transform(said))
    held.meta[HELD] = True
    return held


def held(node: exp.Expr, said: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A time-valued ``node`` (time_valued) as ClickHouse holds it."""
    value = node.unnest()
    if isinstance(value, exp.Cast) and value.to.is_type(_TIME):
        return said(value)
    return node.transform(said)


#: The last microsecond from the epoch DateTime64(6, 'UTC') holds.
_LAST_US = (instants.LAST - datetime.datetime(1970, 1, 1)) // datetime.timedelta(microseconds=1)


def semantics(node: exp.Expr, said: Callable[[exp.Expr], exp.Expr]) -> exp.Expr | None:
    """What ClickHouse is told of ``node`` where it holds a time of day; None where it does not.

    - a cast to time: held (``cast``);
    - a time cast to text: time_out's text;
    - a date plus a time (date + time, time + date): the timestamp, DateTime64(6, 'UTC'), exact,
      0A000 past DateTime64's last day;
    - a time less a time no row read (its operands not literals): refused by name, for ClickHouse
      would answer the difference of two integers, which is no interval."""
    if isinstance(node, exp.Cast) and node.to.is_type(*_TEXTS) and time_valued(node.this):
        return shown(held(node.this, said))
    if isinstance(node, exp.Add):
        pair = (node.this, node.expression)
        if _date_valued(pair[0]) and time_valued(pair[1]):
            day, clock, name = pair[0], pair[1], "datetime_pl"
        elif time_valued(pair[0]) and _date_valued(pair[1]):
            clock, day, name = pair[0], pair[1], "timedate_pl"
        else:
            return None
        stamp = f"(toInt64(toDate32(provisa_d)) * {DAY_US} + toInt64(provisa_c))"
        outside = ch_fail("0A000", instants.out_of_bound(name, instants.STAMP_BOUND)).format(
            condition=f"({stamp} > {_LAST_US})"
        )
        return graft(
            f"fromUnixTimestamp64Micro({stamp} + {outside}, 'UTC')",
            provisa_d=_day(day, said),
            provisa_c=held(clock, said),
        )
    if isinstance(node, exp.Sub) and time_valued(node.this) and time_valued(node.expression):
        raise _refused(
            "time_mi_time",
            Refusal("0A000", f"time_mi_time() of a time no row reads has no form on {_CLICKHOUSE}"),
        )
    return None


def _day(node: exp.Expr, said: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A date as Date32 reads it: a cast's value read by toDate32, not ClickHouse's Date, which
    holds 1970 to 2149 alone."""
    value = node.unnest()
    if isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.DATE):
        return exp.Anonymous(this="toDate32", expressions=[value.this.transform(said)])
    return node.transform(said)


def select_shown(before: exp.Expr, after: exp.Expr) -> exp.Expr:
    """``after``, the statement ClickHouse is told, with each column of its select list that is a
    time in ``before`` (the statement as the forms left it) written as time_out's text."""
    if not isinstance(before, exp.Select) or not isinstance(after, exp.Select):
        return after
    if len(before.selects) != len(after.selects):
        return after
    for was, now in zip(before.selects, after.selects, strict=True):
        if not time_valued(was.unalias()):
            continue
        value = now.unalias()
        if isinstance(value, exp.Literal) and value.is_string:
            continue  # already its text
        value.replace(shown(value, column=True))
    return after


FAMILY = "time of day on ClickHouse"


def _typed(argument: exp.Expr, *types: exp.DType) -> bool:
    """Whether the statement says ``argument`` is of one of ``types`` (a cast to it), or it is a
    held time or a date a form wrote."""
    if exp.DataType.Type.TIME in types and time_valued(argument):
        return True
    if exp.DataType.Type.DATE in types and _date_valued(argument):
        return True
    value = argument.unnest()
    return isinstance(value, exp.Cast) and value.to.is_type(*types)


def _date_and_time(
    names: frozenset[str], swapped: bool
) -> Callable[[exp.Expr, str], exp.Expr | None]:
    """A call of one of ``names`` of a date and a time (``swapped``: a time and a date) as the sum
    ClickHouse is told of them (``semantics``): the timestamp, DateTime64(6, 'UTC'), exact."""
    kinds = (exp.DataType.Type.TIME, exp.DataType.Type.DATE)
    first, second = kinds if swapped else kinds[::-1]

    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = trino.called(node) if dialect == _CLICKHOUSE else None
        if call is None or not call[0] & names or len(call[1]) != 2:
            return None
        a, b = call[1]
        if not (_typed(a, first) and _typed(b, second)):
            return None
        return exp.Paren(this=exp.Add(this=a.copy(), expression=b.copy()))

    rewrite.__doc__ = (
        f"{'/'.join(sorted(names))} of a {'time and a date' if swapped else 'date and a time'}: "
        "their sum, a timestamp held as DateTime64(6, 'UTC')."
    )
    rewrite.__qualname__ = rewrite.__name__ = f"{sorted(names)[0]}_of_a_clock"
    return rewrite


def time_of_stamp(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """time of a timestamp: a cast of it to time, its wall time's microseconds of the day."""
    call = trino.called(node) if dialect == _CLICKHOUSE else None
    if call is None or "time" not in call[0] or len(call[1]) != 1:
        return None
    (stamp,) = call[1]
    if not _typed(stamp, exp.DataType.Type.TIMESTAMP):
        return None
    return exp.Cast(this=stamp.copy(), to=exp.DataType.build("time", dialect="postgres"))


def _integer(argument: exp.Expr) -> int | None | Literal[False]:
    """An integer the statement writes (a literal, signed, maybe cast): its value; None for a NULL;
    False for any other value."""
    value = argument.unnest()
    while isinstance(value, exp.Cast):
        value = value.this.unnest()
    if isinstance(value, exp.Null):
        return None
    negative = isinstance(value, exp.Neg)
    if negative:
        value = value.this.unnest()
    if not isinstance(value, exp.Literal) or value.is_string or not value.this.isdigit():
        return False
    return -int(value.this) if negative else int(value.this)


def time_scaled(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """time of a time and a precision (time_scale): its microseconds rounded half up to the
    precision's digits (AdjustTimeForTypmod); a precision outside 0 to 6 leaves it."""
    call = trino.called(node) if dialect == _CLICKHOUSE else None
    if call is None or "time" not in call[0] or len(call[1]) != 2:
        return None
    clock, precision = call[1]
    digits = _integer(precision)
    if not _typed(clock, exp.DataType.Type.TIME) or digits is False:
        return None
    if digits is None:
        return held_null()
    if not 0 <= digits <= 6:
        return clock.copy()
    scale = 10 ** (6 - digits)
    rounded = graft(
        f"toInt64(intDiv(toInt64(provisa_c) + {scale // 2}, {scale}) * {scale})",
        provisa_c=clock.copy(),
    )
    rounded.meta[HELD] = True
    return rounded


#: Each unit extract reads of a time (time_part_common), as numeric: the held time's SQL of it,
#: its scale. date_part's double precision of each: its SQL, as timestamp.c computes the double.
_PARTS: dict[str, tuple[str, str]] = {
    "microseconds": (
        "toDecimal64(provisa_c % 60000000, 0)",
        "toFloat64(intDiv(provisa_c % 60000000, 1000000)) * 1000000.0 "
        "+ toFloat64(provisa_c % 1000000)",
    ),
    "milliseconds": (
        "toDecimal64(provisa_c % 60000000, 3) / 1000",
        "toFloat64(intDiv(provisa_c % 60000000, 1000000)) * 1000.0 "
        "+ toFloat64(provisa_c % 1000000) / 1000.0",
    ),
    "second": (
        "toDecimal64(provisa_c % 60000000, 6) / 1000000",
        "toFloat64(intDiv(provisa_c % 60000000, 1000000)) + toFloat64(provisa_c % 1000000) / 1000000.0",
    ),
    "minute": (
        "toDecimal64(intDiv(provisa_c, 60000000) % 60, 0)",
        "toFloat64(intDiv(provisa_c, 60000000) % 60)",
    ),
    "hour": (
        "toDecimal64(intDiv(provisa_c, 3600000000), 0)",
        "toFloat64(intDiv(provisa_c, 3600000000))",
    ),
    "epoch": (
        "toDecimal64(provisa_c, 6) / 1000000",
        "toFloat64(provisa_c) / 1000000.0",
    ),
}
#: Each unit's other spellings PostgreSQL reads (datetktbl's and deltatktbl's).
_PART_NAMES = {
    "microseconds": ("microsecond", "microseconds", "us", "usec", "usecs", "useconds"),
    "milliseconds": ("millisecond", "milliseconds", "ms", "msec", "msecs", "mseconds"),
    "second": ("second", "seconds", "s", "sec", "secs"),
    "minute": ("minute", "minutes", "m", "min", "mins"),
    "hour": ("hour", "hours", "h", "hr", "hrs"),
    "epoch": ("epoch",),
}
_PART_OF = {spelling: unit for unit, names in _PART_NAMES.items() for spelling in names}


def _text(argument: exp.Expr) -> str | None | Literal[False]:
    """A text the statement writes (maybe cast): its value; None for a NULL; False for any other."""
    value = argument.unnest()
    while isinstance(value, exp.Cast):
        value = value.this.unnest()
    if isinstance(value, exp.Null):
        return None
    if isinstance(value, exp.Literal) and value.is_string:
        return value.this
    return False


def _part_of(names: frozenset[str], numeric: bool) -> Callable[[exp.Expr, str], exp.Expr | None]:
    """extract (``numeric``) or date_part of a unit and a time, called by its name."""

    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        if dialect != _CLICKHOUSE or not isinstance(node, exp.Anonymous):
            return None
        if node.name.lower() not in names or len(node.expressions) != 2:
            return None
        unit, clock = node.expressions
        text = _text(unit)
        if text is False or not _typed(clock, exp.DataType.Type.TIME):
            return None
        if text is None:
            typed = "Nullable(Decimal(18, 6))" if numeric else "Nullable(Float64)"
            return graft(f"CAST(NULL AS {typed})")
        part = _PART_OF.get(text.strip().lower())
        if part is None:
            return None  # a unit time has none of (day, year): the function's refusal
        held = graft(_PARTS[part][0 if numeric else 1], provisa_c=clock.copy())
        return graft(
            "if(isNull(provisa_t), NULL, provisa_p)", provisa_t=clock.copy(), provisa_p=held
        )

    name = sorted(names)[0]
    rewrite.__doc__ = f"{name} of a unit and a time of day, of its held microseconds."
    rewrite.__qualname__ = rewrite.__name__ = f"{name}_of_a_clock"
    return rewrite


_HELD_STATED = (
    "ClickHouse's server has no Time64: a time is held as an Int64 of its microseconds of the "
    "day, and shown in the select list as time_out's text (polyfill.clocks)."
)
_SUM_STATED = (
    _HELD_STATED + " The answer is held as DateTime64(6, 'UTC'), exact from 1900-01-01 to "
    "2299-12-31: a date there plus 24:00:00 past its last day raises 0A000. The same sum answers "
    "the operators date + time and time + date."
)


def _row(
    name: str,
    summary: str,
    form: Callable[[exp.Expr, str], exp.Expr | None],
    stated: str,
    mentions: str,
) -> Feature:
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, {_CLICKHOUSE: Rewrite(form, mentions=mentions)}),),
            limitations={_CLICKHOUSE: (Limitation(stated),)},
        ),
    )


_PART_STATED = (
    " The unit is read as a literal or a NULL (microseconds, milliseconds, second, minute, hour, "
    "epoch, and their other spellings); any other unit, and one the statement computes, keeps the "
    "function's refusal by name. Called by its name only: extract(unit FROM time), which the "
    "translator reads as the same node as date_part's, keeps its refusal."
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 190
FEATURES: tuple[Feature, ...] = (
    _row(
        "extract(text, time)",
        "A unit of a time of day, as numeric.",
        _part_of(frozenset({"extract"}), numeric=True),
        _HELD_STATED + " Each unit is computed of the microseconds as time_part_common computes "
        "it, a numeric of PostgreSQL's scale (second 6, milliseconds 3, epoch 6)." + _PART_STATED,
        r"\bextract\b",
    ),
    _row(
        "date_part(text, time)",
        "A unit of a time of day, as double precision.",
        _part_of(frozenset({"date_part"}), numeric=False),
        _HELD_STATED + " Each unit is computed of the microseconds in double precision as "
        "timestamp.c computes it, so it is the same double." + _PART_STATED,
        r"\bdate_part\b",
    ),
    _row(
        "datetime_pl(date, time)",
        "A date plus a time of day, a timestamp (+).",
        _date_and_time(frozenset({"datetime_pl"}), swapped=False),
        _SUM_STATED,
        r"\bdatetime_pl\b",
    ),
    _row(
        "timestamp(date, time)",
        "The timestamp of a date and a time of day.",
        _date_and_time(frozenset({"timestamp"}), swapped=False),
        _SUM_STATED,
        r"\btimestamp\b",
    ),
    _row(
        "timedate_pl(time, date)",
        "A time of day plus a date, a timestamp (+).",
        _date_and_time(frozenset({"timedate_pl"}), swapped=True),
        _SUM_STATED,
        r"\btimedate_pl\b",
    ),
    _row(
        "time(timestamp)",
        "The time of day of a timestamp.",
        time_of_stamp,
        _HELD_STATED + " The timestamp is held as DateTime64(6, 'UTC'), exact from 1900-01-01 "
        "to 2299-12-31; its wall time's time of day is read.",
        r"\btime\b",
    ),
    _row(
        "time(time, integer)",
        "A time of day rounded to a precision (time_scale).",
        time_scaled,
        _HELD_STATED + " The microseconds are rounded half up to the precision's digits, as "
        "AdjustTimeForTypmod rounds them (23:59:59.9999995 to 24:00:00 at six); a precision "
        "outside 0 to 6 leaves the time. The precision is read as a literal or a NULL; any other is "
        "refused by name.",
        r"\btime\b",
    ),
)

__all__ = [
    "FAMILY",
    "FEATURES",
    "DAY_US",
    "cast",
    "graft",
    "held",
    "select_shown",
    "semantics",
    "time_valued",
    "HELD",
    "NULL_HELD",
    "NULL_TEXT",
    "SECOND_US",
    "held_literal",
    "held_null",
    "shown",
    "text_of",
    "text_sql",
]
