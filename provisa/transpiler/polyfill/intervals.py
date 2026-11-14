# Copyright (c) 2026 Kenneth Stott
# Canary: 166110f8-d4a4-40aa-a316-de78d39388ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An interval on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES BEFORE A
REFUSAL; design note J1305, representation A, chosen by the maintainer).

ClickHouse has no interval of PostgreSQL's. Here one is held as PostgreSQL holds it: three
integers, months (Int32), days (Int32) and microseconds (Int64), as a ClickHouse
Tuple(Nullable(Int32), Nullable(Int32), Nullable(Int64)); a NULL interval is the tuple of three
NULLs (a ClickHouse tuple is never NULL itself). Every answer below is computed from those three
integers as PostgreSQL's own C code computes it (timestamp.c), so it is exact:

- a literal (a string cast to interval) is read here, as the statement is transpiled, by
  PostgreSQL's DecodeInterval rules (IntervalStyle postgres): amounts and their unit words, a clock
  (h:mm[:ss[.ffffff]]), signs, fractions, '@' and 'ago'. Any other form (ISO 8601, SQL standard,
  a bare number, infinity) is refused by name: it is read nowhere here;
- comparisons read PostgreSQL's order: months * 30 days + days * 24 hours + microseconds, an
  Int128 that holds every interval exactly;
- arithmetic raises PostgreSQL's 22008 (interval out of range) where a field overflows;
- an answer that is an interval and reaches the select list is written as interval_out's text
  (IntervalStyle postgres), which pgwire reads back as the interval it is
  (provisa.pgwire.cast_types.interval_of_text); one that another row here reads stays the tuple;
- a timestamp or a date plus an interval is computed in integers (months, with the day clamped to
  the month's last day, then days, then microseconds), and a result, or a step of it, outside
  DateTime64's bound raises 0A000, as the other bounded rows do.

Every other dialect keeps what it had: no row here has a form outside ClickHouse."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import datetime
import re
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.interval_input import (
    DAY_US as _DAY_US,
    HOUR_US as _HOUR_US,
    I32_MAX as _I32_MAX,
    I32_MIN as _I32_MIN,
    I64_MAX as _I64_MAX,
    I64_MIN as _I64_MIN,
    INPUT_UNITS as _INPUT_UNITS,
    MINUTE_US as _MINUTE_US,
    SECOND_US as _SECOND_US,
    interval_read,
)
from provisa.transpiler.polyfill import instants, trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite
from provisa.transpiler.polyfill.numbers import ch_fail

FAMILY = "interval on ClickHouse"

_CLICKHOUSE = "clickhouse"
#: The meta key of a node this module wrote whose value is a held interval (the tuple).
HELD = "provisa_interval"

#: An interval's order: a month is 30 days, a day 24 hours (interval_cmp_value).
_MONTH_US = 30 * _DAY_US


def interval_text(months: int, days: int, micros: int) -> str:
    """interval_out under IntervalStyle postgres (EncodeInterval), in Python: what the written
    form computes on ClickHouse, for the oracle and the tests."""
    parts: list[str] = []
    before = False
    years, rest = int(months / 12), months - int(months / 12) * 12
    for count, word in ((years, "year"), (rest, "mon"), (days, "day")):
        if count:
            plus = "+" if before and count > 0 else ""
            parts.append(f"{plus}{count} {word}{'' if count == 1 else 's'}")
            before = count < 0
    if micros or not parts:
        sign = "-" if micros < 0 else ("+" if before else "")
        whole, fraction = divmod(abs(micros), _SECOND_US)
        clock = f"{whole // 3600:02d}:{whole // 60 % 60:02d}:{whole % 60:02d}"
        if fraction:
            clock += f".{fraction:06d}".rstrip("0")
        parts.append(sign + clock)
    return " ".join(parts)


# --- ClickHouse's SQL of a held interval ----------------------------------------------------------

_NULL_HELD = (
    "tuple(CAST(NULL AS Nullable(Int32)), CAST(NULL AS Nullable(Int32)), "
    "CAST(NULL AS Nullable(Int64)))"
)
_RANGE = "interval out of range"


def _literal(months: int, days: int, micros: int) -> str:
    return (
        f"tuple(toNullable(toInt32({months})), toNullable(toInt32({days})), "
        f"toNullable(toInt64({micros})))"
    )


def _field(held: str, index: int) -> str:
    return f"tupleElement({held}, {index})"


def _key(held: str) -> str:
    """An interval's place in PostgreSQL's order (interval_cmp_value), exact in Int128."""
    m, d, u = (_field(held, i) for i in (1, 2, 3))
    return f"(toInt128({m}) * {_MONTH_US} + toInt128({d}) * {_DAY_US} + toInt128({u}))"


def _outside(months: str, days: str, micros: str) -> str:
    """Whether fields computed past their types hold no interval: a field past its type, or the
    fields of an infinity, which PostgreSQL 17 raises for a finite operation's result."""
    return (
        f"({months} < {_I32_MIN} OR {months} > {_I32_MAX} OR {days} < {_I32_MIN} "
        f"OR {days} > {_I32_MAX} OR {micros} < {_I64_MIN} OR {micros} > {_I64_MAX} "
        f"OR ({months} = {_I32_MAX} AND {days} = {_I32_MAX} AND {micros} = {_I64_MAX}) "
        f"OR ({months} = {_I32_MIN} AND {days} = {_I32_MIN} AND {micros} = {_I64_MIN}))"
    )


def _raise(sqlstate: str, message: str, condition: str) -> str:
    return ch_fail(sqlstate, message).format(condition=condition)


def _checked_tuple(months: str, days: str, micros: str) -> str:
    """The interval of computed fields, raising 22008 where they hold none."""
    raised = _raise("22008", _RANGE, _outside(months, days, micros))
    return _fields_raising(months, days, micros, raised)


def _fields_raising(months: str, days: str, micros: str, raised: str) -> str:
    """The interval of three fields, ``raised`` (a throwIf) read with its microseconds: a cast to
    Int32 that holds a raise is written as accurateCast (transpile.clickhouse_semantics)."""
    return (
        f"tuple(CAST({months} AS Nullable(Int32)), CAST({days} AS Nullable(Int32)), "
        f"CAST({micros} + {raised} AS Nullable(Int64)))"
    )


def _decimal(number: str, scale: int) -> str:
    """An Int128's text as a numeric of ``scale`` digits past the point."""
    if scale == 0:
        return f"toString({number})"
    unit = 10**scale
    return (
        f"concat(if({number} < 0, '-', ''), toString(intDiv(abs({number}), {unit})), '.', "
        f"leftPad(toString(abs({number}) % {unit}), {scale}, '0'))"
    )


class _Sql:
    """The ClickHouse SQL a row writes, with what the statement holds (a column, a value another
    row wrote) as placeholders, grafted after it is parsed."""

    def __init__(self) -> None:
        self.nodes: list[exp.Expr] = []
        self.names = 0

    def embed(self, node: exp.Expr) -> str:
        self.nodes.append(node)
        return f"provisa_node_{len(self.nodes) - 1}"

    def name(self) -> str:
        self.names += 1
        return f"provisa_iv{self.names}"

    def let(self, values: list[str], body: Callable[..., str]) -> str:
        """``body`` of names each bound to one of ``values``, so a value written once is read
        many times (ClickHouse has no let: a lambda over a one-element array)."""
        names = [self.name() for _ in values]
        params = names[0] if len(names) == 1 else f"({', '.join(names)})"
        arrays = ", ".join(f"[{value}]" for value in values)
        return f"arrayElement(arrayMap({params} -> {body(*names)}, {arrays}), 1)"

    def text(self, held: str) -> str:
        """interval_out of a held interval (EncodeInterval, IntervalStyle postgres)."""

        def written(x: str) -> str:
            m, d, u = (_field(x, i) for i in (1, 2, 3))
            years, months = f"intDiv({m}, 12)", f"({m} % 12)"

            def part(count: str, word: str, before: str) -> str:
                plus = f"if(({before}) AND {count} > 0, '+', '')"
                plural = f"if({count} = 1, '', 's')"
                return (
                    f"if({count} = 0, '', concat({plus}, toString({count}), ' {word}', {plural}))"
                )

            before_day = f"if({months} != 0, {months} < 0, {years} < 0)"
            before_clock = f"if({d} != 0, {d} < 0, {before_day})"
            a = f"abs(toInt128({u}))"
            hours = f"intDiv({a}, {_HOUR_US})"
            clock = (
                f"concat(if({hours} < 10, '0', ''), toString({hours}), ':', "
                f"leftPad(toString(intDiv({a}, {_MINUTE_US}) % 60), 2, '0'), ':', "
                f"leftPad(toString(intDiv({a}, {_SECOND_US}) % 60), 2, '0'), "
                f"if({a} % {_SECOND_US} = 0, '', concat('.', replaceRegexpOne("
                f"leftPad(toString({a} % {_SECOND_US}), 6, '0'), '0+$', ''))))"
            )
            shown = f"{u} != 0 OR ({m} = 0 AND {d} = 0)"
            sign = f"if({u} < 0, '-', if({before_clock}, '+', ''))"
            parts = [
                part(years, "year", "false"),
                part(months, "mon", f"{years} < 0"),
                part(d, "day", before_day),
                f"if({shown}, concat({sign}, {clock}), '')",
            ]
            p = self.name()
            joined = f"arrayStringConcat(arrayFilter({p} -> {p} != '', [{', '.join(parts)}]), ' ')"
            return f"if(isNull({m}), CAST(NULL AS Nullable(String)), {joined})"

        return self.let([held], written)

    def parsed(self, text: str) -> exp.Expr:
        tree = sqlglot.parse_one(text, read=_CLICKHOUSE)
        nodes = self.nodes

        def grafted(node: exp.Expr) -> exp.Expr:
            if isinstance(node, exp.Column) and node.name.startswith("provisa_node_"):
                return nodes[int(node.name.removeprefix("provisa_node_"))].copy()
            return node

        return tree.transform(grafted)


# --- what a row reads of its arguments ----------------------------------------------------------


def _said(node: exp.Expr) -> str | None:
    said = trino.said(node)
    return None if said is None else said.split("(")[0]


def _string(node: exp.Expr) -> str | None:
    return node.this if isinstance(node, exp.Literal) and node.is_string else None


#: A held interval read of an argument: its SQL, None where the row does not read it.
def _interval(argument: exp.Expr, sql: _Sql, untyped: bool = False) -> str | None:
    """The held interval ``argument`` is: a literal read here, a NULL, a column of a VALUES list
    of them, or a call or an operator another row here answers. ``untyped``: an untyped string is
    the interval's input (the call's only overload takes an interval there)."""
    value = argument.unnest()
    if value.meta.get(HELD):
        return sql.embed(value)
    if isinstance(value, exp.Null):
        return _NULL_HELD
    if untyped and _string(value) is not None:
        return _read_literal(_string(value) or "")
    if isinstance(value, exp.Cast):
        if _said(value) != "interval":
            return None
        inner = value.this.unnest()
        if isinstance(inner, exp.Null):
            return _NULL_HELD
        text = _string(inner)
        return None if text is None else _read_literal(text)
    if isinstance(value, exp.Column):
        return _values_column(value, sql)
    return _written(value, sql)


def _read_literal(text: str) -> str | None:
    read = interval_read(text)
    return None if read is None else _literal(*read)


def _cell(cell: exp.Expr) -> str | None:
    """A cell of a VALUES list read as an interval: a NULL, or a literal cast to one."""
    value = cell.unnest()
    if value.meta.get(HELD):
        return ""  # a cell already written (the walk reads a statement twice)
    if isinstance(value, exp.Null):
        return _NULL_HELD
    if isinstance(value, exp.Cast) and _said(value) == "interval":
        inner = value.this.unnest()
        if isinstance(inner, exp.Null):
            return _NULL_HELD
        text = _string(inner)
        return None if text is None else _read_literal(text)
    return None


def _values_column(column: exp.Column, sql: _Sql) -> str | None:
    """A column of the VALUES list the statement reads, each of its cells an interval literal or a
    NULL: the cells are written as held intervals, where every reference to the column is a call
    of a row here."""
    select = column.find_ancestor(exp.Select)
    if select is None or select.args.get("joins"):
        return None
    source = select.args.get("from_")
    values = source.this if source is not None else None
    if not isinstance(values, exp.Values) or values.args.get("alias") is None:
        return None
    names = [c.name for c in values.args["alias"].columns]
    if column.name not in names or column.table not in ("", values.alias):
        return None
    index = names.index(column.name)
    cells = [row.expressions[index] for row in values.expressions]
    written = [_cell(cell) for cell in cells]
    if None in written or not any(
        isinstance(c.unnest(), exp.Cast) or c.unnest().meta.get(HELD) for c in cells
    ):
        return None
    for other in select.find_all(exp.Column):
        if other.name == column.name and not _read_by_a_row(other):
            return None  # the column is read as it is elsewhere: the select list's own value
    for cell, text in zip(cells, written):
        if text:
            held = sqlglot.parse_one(text, read=_CLICKHOUSE)
            held.meta[HELD] = True
            cell.replace(held)
    return sql.embed(column)


def _held_column(column: exp.Column) -> bool:
    """Whether ``column`` is one of a VALUES list whose cells a row here wrote as intervals."""
    select = column.find_ancestor(exp.Select)
    source = select.args.get("from_") if select is not None else None
    values = source.this if source is not None else None
    if not isinstance(values, exp.Values) or values.args.get("alias") is None:
        return False
    names = [c.name for c in values.args["alias"].columns]
    if column.name not in names:
        return False
    index = names.index(column.name)
    return any(row.expressions[index].meta.get(HELD) for row in values.expressions)


def _read_by_a_row(column: exp.Column) -> bool:
    """Whether ``column`` is an argument of a call or an operator a row here reads."""
    node = column.parent
    while node is not None and not isinstance(node, exp.Select):
        if isinstance(node, exp.Values):
            return True
        call = trino.called(node)
        if call is not None and call[0] & _NAMES:
            return True
        if type(node) in _OPERATORS:
            return True
        node = node.parent
    return False


#: A timestamp's or a date's literal, read here: its micro seconds from the epoch.
_STAMP = re.compile(
    r"\s*(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?\s*", re.ASCII
)
_DATE = re.compile(r"\s*(\d{4})-(\d{2})-(\d{2})\s*", re.ASCII)
_EPOCH = datetime.datetime(1970, 1, 1)
_FIRST_US = (instants.FIRST - _EPOCH) // datetime.timedelta(microseconds=1)
_LAST_US = (instants.LAST - _EPOCH) // datetime.timedelta(microseconds=1)


def _instant_literal(text: str, day: bool) -> int | None:
    matched = (_DATE if day else _STAMP).fullmatch(text)
    if matched is None:
        return None
    parts = [int(g) if g else 0 for g in matched.groups()[:6]]
    fraction = 0 if day or not matched.group(7) else int(matched.group(7).ljust(6, "0"))
    try:
        stamp = datetime.datetime(*parts, microsecond=fraction)  # type: ignore[misc]
    except ValueError:
        return None
    return (stamp - _EPOCH) // datetime.timedelta(microseconds=1)


def _instant(argument: exp.Expr, kind: str, untyped: bool) -> str | None:
    """A timestamp or a date ``argument``, read here: its wall time's microseconds from the epoch
    (a literal in ISO form), or ``NULL``. None for any other value, which no row here reads."""
    value = argument.unnest()
    if isinstance(value, exp.Cast):
        if _said(value) not in _KIND_TYPES[kind]:
            return None
        value = value.this.unnest()
    elif not untyped and not isinstance(value, exp.Null):
        return None
    if isinstance(value, exp.Null):
        return "NULL"
    text = _string(value)
    if text is None:
        return None
    micros = _instant_literal(text, kind == "date")
    return None if micros is None else str(micros)


_KIND_TYPES = {
    "timestamp": frozenset({"timestamp", "timestamp without time zone"}),
    "date": frozenset({"date"}),
}


# --- the forms of each row ------------------------------------------------------------------------


def _strict(fields: list[str], answer: str, null: str) -> str:
    """``answer``, or ``null`` where any of ``fields`` is NULL (a STRICT function's answer)."""
    test = " OR ".join(f"isNull({f})" for f in fields)
    return f"if({test}, {null}, {answer})"


_BOOL_NULL = "CAST(NULL AS Nullable(Bool))"
_COMPARISONS = {"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}


def _compared(op: str) -> Callable[[_Sql, list[str]], str]:
    def form(sql: _Sql, args: list[str]) -> str:
        return f"CAST({_key(args[0])} {op} {_key(args[1])} AS Nullable(Bool))"

    return form


def _cmp(sql: _Sql, args: list[str]) -> str:
    def body(x: str, y: str) -> str:
        kx, ky = _key(x), _key(y)
        answer = f"multiIf({kx} < {ky}, -1, {kx} > {ky}, 1, 0)"
        return f"CAST({_strict([kx, ky], answer, 'NULL')} AS Nullable(Int32))"

    return sql.let(args, body)


def _chosen(op: str) -> Callable[[_Sql, list[str]], str]:
    """interval_larger (op >) and interval_smaller (<): the first where it compares so, else the
    second (a tie answers the second, as PostgreSQL's does)."""

    def form(sql: _Sql, args: list[str]) -> str:
        def body(x: str, y: str) -> str:
            answer = f"if({_key(x)} {op} {_key(y)}, {x}, {y})"
            return _strict([_field(x, 1), _field(y, 1)], answer, _NULL_HELD)

        return sql.let(args, body)

    return form


def _arithmetic(op: str) -> Callable[[_Sql, list[str]], str]:
    """interval_pl (+) and interval_mi (-): field by field, 22008 where one overflows."""

    def form(sql: _Sql, args: list[str]) -> str:
        def body(x: str, y: str) -> str:
            m = f"toInt64({_field(x, 1)}) {op} toInt64({_field(y, 1)})"
            d = f"toInt64({_field(x, 2)}) {op} toInt64({_field(y, 2)})"
            u = f"toInt128({_field(x, 3)}) {op} toInt128({_field(y, 3)})"
            return sql.let([m, d, u], _checked_tuple)

        return sql.let(args, body)

    return form


def _negated(sql: _Sql, args: list[str]) -> str:
    def body(x: str) -> str:
        m, d = f"-toInt64({_field(x, 1)})", f"-toInt64({_field(x, 2)})"
        u = f"-toInt128({_field(x, 3)})"
        return sql.let([m, d, u], _checked_tuple)

    return sql.let(args, body)


def _justify_days(sql: _Sql, args: list[str]) -> str:
    def body(x: str) -> str:
        whole = f"intDiv({_field(x, 2)}, 30)"
        days = f"toInt64({_field(x, 2)}) - {whole} * 30"
        months = f"toInt64({_field(x, 1)}) + {whole}"

        def adjusted(m: str, d: str, u: str) -> str:
            up, down = f"({m} > 0 AND {d} < 0)", f"({m} < 0 AND {d} > 0)"
            m2 = f"multiIf({up}, {m} - 1, {down}, {m} + 1, {m})"
            d2 = f"multiIf({up}, {d} + 30, {down}, {d} - 30, {d})"
            raised = _raise("22008", _RANGE, f"({m} < {_I32_MIN} OR {m} > {_I32_MAX})")
            return _fields_raising(m2, d2, u, raised)

        return sql.let([months, days, _field(x, 3)], adjusted)

    return sql.let(args, body)


def _justify_hours(sql: _Sql, args: list[str]) -> str:
    def body(x: str) -> str:
        whole = f"intDiv({_field(x, 3)}, {_DAY_US})"
        micros = f"toInt64({_field(x, 3)}) - {whole} * {_DAY_US}"
        days = f"toInt64({_field(x, 2)}) + {whole}"

        def adjusted(d: str, u: str, m: str) -> str:
            up, down = f"({d} > 0 AND {u} < 0)", f"({d} < 0 AND {u} > 0)"
            d2 = f"multiIf({up}, {d} - 1, {down}, {d} + 1, {d})"
            u2 = f"multiIf({up}, {u} + {_DAY_US}, {down}, {u} - {_DAY_US}, {u})"
            raised = _raise("22008", _RANGE, f"({d} < {_I32_MIN} OR {d} > {_I32_MAX})")
            return _fields_raising(m, d2, u2, raised)

        return sql.let([days, micros, _field(x, 1)], adjusted)

    return sql.let(args, body)


def _justify_interval(sql: _Sql, args: list[str]) -> str:
    """interval_justify_interval (PostgreSQL 17): days to months first where days and time share a
    sign, then time to days, days to months, and the signs made one."""

    def body(x: str) -> str:
        m0, d0, u0 = (f"toInt64({_field(x, i)})" for i in (1, 2, 3))
        pre = f"(({d0} > 0 AND {u0} > 0) OR ({d0} < 0 AND {u0} < 0))"
        m1 = f"if({pre}, {m0} + intDiv({d0}, 30), {m0})"
        d1 = f"if({pre}, {d0} - intDiv({d0}, 30) * 30, {d0})"

        def step(m: str, d: str, u: str) -> str:
            first = f"({m} < {_I32_MIN} OR {m} > {_I32_MAX})"
            wholeday = f"intDiv({u}, {_DAY_US})"
            u2 = f"{u} - {wholeday} * {_DAY_US}"
            d2 = f"{d} + {wholeday}"

            def months(d3: str, u3: str) -> str:
                over_days = f"({d3} < {_I32_MIN} OR {d3} > {_I32_MAX})"
                m4 = f"{m} + intDiv({d3}, 30)"
                d4 = f"{d3} - intDiv({d3}, 30) * 30"

                def signs(mm: str, dd: str, uu: str) -> str:
                    over = f"({first} OR {over_days} OR {mm} < {_I32_MIN} OR {mm} > {_I32_MAX})"
                    up = f"({mm} > 0 AND ({dd} < 0 OR ({dd} = 0 AND {uu} < 0)))"
                    down = f"({mm} < 0 AND ({dd} > 0 OR ({dd} = 0 AND {uu} > 0)))"
                    m5 = f"multiIf({up}, {mm} - 1, {down}, {mm} + 1, {mm})"
                    d5 = f"multiIf({up}, {dd} + 30, {down}, {dd} - 30, {dd})"

                    def last(m6: str, d6: str) -> str:
                        up2, down2 = f"({d6} > 0 AND {uu} < 0)", f"({d6} < 0 AND {uu} > 0)"
                        d7 = f"multiIf({up2}, {d6} - 1, {down2}, {d6} + 1, {d6})"
                        u7 = f"multiIf({up2}, {uu} + {_DAY_US}, {down2}, {uu} - {_DAY_US}, {uu})"
                        raised = _raise("22008", _RANGE, over)
                        return _fields_raising(m6, d7, u7, raised)

                    return sql.let([m5, d5], last)

                return sql.let([m4, d4, u3], signs)

            return sql.let([d2, u2], months)

        return sql.let([m1, d1, u0], step)

    return sql.let(args, body)


def _isfinite(sql: _Sql, args: list[str]) -> str:
    """isfinite: no interval held here is infinite (an infinity's literal is refused by name)."""
    return f"if(isNull({_field(args[0], 1)}), {_BOOL_NULL}, CAST(true AS Nullable(Bool)))"


# --- extract and date_part of an interval ------------------------------------------------------

#: The units extract and date_part read of an interval (deltatktbl and datetktbl), by the first
#: ten characters, as interval_part_common reads them.
_PART_UNITS = {
    **{k: v for k, v in _INPUT_UNITS.items()},
    **dict.fromkeys(("qtr", "quarter"), "quarter"),
    "epoch": "epoch",
}
#: Words PostgreSQL knows as a unit that an interval has no part of: 0A000.
_NOT_SUPPORTED = frozenset(
    {"dow", "doy", "isodow", "isoyear", "julian", "timezone", "timezone_h", "timezone_m"}
)


def _units_of(text: str) -> str:
    """downcase_truncate_identifier's text of a unit, as PostgreSQL's errors name it."""
    return text.lower()[:63]


def _parts_form(unit: str, x: str, numeric: bool) -> str:
    """A part of the held interval ``x`` (interval2itm, then interval_part_common): text of a
    numeric (extract) or a Float64 (date_part)."""
    m, d, u = (_field(x, i) for i in (1, 2, 3))
    hours = f"intDiv({u}, {_HOUR_US})"
    minutes = f"intDiv({u} - {hours} * {_HOUR_US}, {_MINUTE_US})"
    seconds_us = f"({u} % {_MINUTE_US})"  # tm_sec * 1000000 + tm_usec
    years = f"intDiv({m}, 12)"
    integers = {
        "microsecond": seconds_us,
        "minute": minutes,
        "hour": hours,
        "day": d,
        "week": f"intDiv({d}, 7)",
        "month": f"({m} % 12)",
        # -month wraps in PostgreSQL's int32 where month is its least: -(-8 / 3 + 1) = 1
        "quarter": (
            f"multiIf({m} >= 0, intDiv({m} % 12, 3) + 1, {m} = {_I32_MIN}, 1, "
            f"-(intDiv((-toInt64({m})) % 12, 3) + 1))"
        ),
        "year": years,
        "decade": f"intDiv({years}, 10)",
        "century": f"intDiv({years}, 100)",
        "millennium": f"intDiv({years}, 1000)",
    }
    if unit in integers:
        value = f"toInt128({integers[unit]})"
        return _decimal(value, 0) if numeric else f"toFloat64({value})"
    if unit in ("millisecond", "second"):
        scale = 3 if unit == "millisecond" else 6
        if numeric:
            return _decimal(f"toInt128({seconds_us})", scale)
        whole = f"intDiv({seconds_us}, {_SECOND_US})"
        fraction = f"({seconds_us} % {_SECOND_US})"
        if unit == "millisecond":
            return f"(toFloat64({whole}) * 1000.0 + toFloat64({fraction}) / 1000.0)"
        return f"(toFloat64({whole}) + toFloat64({fraction}) / 1000000.0)"
    assert unit == "epoch"
    if numeric:
        seconds = (
            f"((toInt128(1461) * intDiv({m}, 12) + toInt128(120) * ({m} % 12) "
            f"+ toInt128(4) * {d}) * 21600)"
        )
        return _decimal(f"({seconds} * {_SECOND_US} + toInt128({u}))", 6)
    return (
        f"(toFloat64({u}) / 1000000.0 + (365.25 * 86400) * toFloat64(intDiv({m}, 12)) "
        f"+ toFloat64(30 * 86400) * toFloat64({m} % 12) + 86400.0 * toFloat64({d}))"
    )


def _part(numeric: bool) -> Callable[[_Sql, list[str], str | None], str]:
    returns = "Nullable(String)" if numeric else "Nullable(Float64)"
    null = f"CAST(NULL AS {returns})"

    def form(sql: _Sql, args: list[str], unit_text: str | None) -> str:
        if unit_text is None:
            return null
        lowered = _units_of(unit_text)
        unit = _PART_UNITS.get(lowered[:10])

        def body(x: str) -> str:
            present = f"isNotNull({_field(x, 1)})"
            if unit is None or lowered in _NOT_SUPPORTED:
                known = lowered in _NOT_SUPPORTED
                state, verb = ("0A000", "supported") if known else ("22023", "recognized")
                message = f'unit "{lowered}" not {verb} for type interval'
                return f"CAST(if({present}, {_raise(state, message, present)}, NULL) AS {returns})"
            answer = _parts_form(unit, x, numeric)
            return f"if({present}, CAST({answer} AS {returns}), {null})"

        return sql.let(args, body)

    return form


# --- aggregates ---------------------------------------------------------------------------------


def _extreme(function: str, op: str) -> Callable[[_Sql, list[str]], str]:
    """max (op 'Max') and min ('Min') of a column of intervals. PostgreSQL answers a tie's last
    row in its scan, which ClickHouse does not keep: a tie of intervals that are equal but written
    apart ('1 mon', '30 days') raises 0A000."""

    def form(sql: _Sql, args: list[str]) -> str:
        h = args[0]
        m, d, u = (_field(h, i) for i in (1, 2, 3))
        present = f"isNotNull({m})"
        key = _key(h)
        first = (
            f"arg{op}If({h}, tuple({key}, toInt64({m}), toInt64({d}), toInt128({u})), {present})"
        )
        other = (
            f"arg{op}If({h}, tuple({key}, -toInt64({m}), -toInt64({d}), -toInt128({u})), {present})"
        )
        count = f"countIf({present})"
        message = (
            f"{function}() of intervals that are equal but written apart has no exact form on "
            f"{_CLICKHOUSE}"
        )

        def body(a: str, b: str, n: str) -> str:
            tie = f"({n} > 0 AND {a} != {b})"
            raised = _raise("0A000", message, tie)
            held = _fields_raising(_field(a, 1), _field(a, 2), _field(a, 3), raised)
            return f"if({n} = 0, {_NULL_HELD}, {held})"

        return sql.let([first, other, count], body)

    return form


def _sum(sql: _Sql, args: list[str]) -> str:
    """sum: each field's total. PostgreSQL raises 22008 where a running total overflows, which
    depends on the rows' order: a total outside its field raises 22008 (every order overflows);
    a total inside, where the rows of one sign alone could reach the field's end, raises 0A000."""
    h = args[0]
    present = f"isNotNull({_field(h, 1)})"
    totals, ends = [], []
    for index, (low, high) in enumerate(((_I32_MIN, _I32_MAX),) * 2 + ((_I64_MIN, _I64_MAX),)):
        value = f"toInt128({_field(h, index + 1)})"
        totals.append(f"sumIf({value}, {present})")
        rising = f"sumIf({value}, {present} AND {value} > 0)"
        falling = f"sumIf({value}, {present} AND {value} < 0)"
        ends.append(f"({rising} >= {high} OR {falling} <= {low})")
    count = f"countIf({present})"
    message = (
        f"sum() of intervals whose running total may reach a field's end has no exact form on "
        f"{_CLICKHOUSE}"
    )

    def body(m: str, d: str, u: str, n: str, reach: str) -> str:
        over = _outside(m, d, u)
        overflow = _raise("22008", _RANGE, f"({n} > 0 AND {over})")
        bounded = _raise("0A000", message, f"({n} > 0 AND NOT {over} AND {reach})")
        held = _fields_raising(m, d, u, f"{overflow} + {bounded}")
        return f"if({n} = 0, {_NULL_HELD}, {held})"

    return sql.let([*totals, count, " OR ".join(ends)], body)


# --- a timestamp or a date and an interval -------------------------------------------------------


def _days_from_civil(y: str, m: str, d: str) -> str:
    """Days from 1970-01-01 of a proleptic Gregorian date (Hinnant's days_from_civil), in Int64."""
    yy = f"(toInt64({y}) - if({m} <= 2, 1, 0))"
    era = f"intDiv({yy} - if({yy} < 0, 399, 0), 400)"
    yoe = f"({yy} - {era} * 400)"
    doy = f"(intDiv(153 * ({m} + if({m} > 2, -3, 9)) + 2, 5) + {d} - 1)"
    doe = f"({yoe} * 365 + intDiv({yoe}, 4) - intDiv({yoe}, 100) + {doy})"
    return f"({era} * 146097 + {doe} - 719468)"


_NULL_STAMP = "CAST(NULL AS Nullable(DateTime64(6, 'UTC')))"
_MONTH_DAYS = "[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]"


def _shifted(function: str, negate: bool) -> Callable[[_Sql, str, str], str]:
    """timestamp_pl_interval (and _mi_, the interval negated first, 22008 where a field cannot
    be): the months added, the day clamped to its month's last day, then the days, then the
    microseconds; each step inside DateTime64's bound, or 0A000. The timestamp is a literal, so its
    calendar is read here; the answer is DateTime64(6, 'UTC'), its wall time."""
    message = instants.out_of_bound(function, instants.STAMP_BOUND)

    def form(sql: _Sql, stamp: str, interval: str) -> str:
        if stamp == "NULL":
            return _NULL_STAMP
        t = int(stamp)
        whole = _EPOCH + datetime.timedelta(microseconds=t)
        tod = t - (t // _DAY_US) * _DAY_US
        start = whole.year * 12 + whole.month - 1

        def body(x: str) -> str:
            m0, d0 = (f"toInt64({_field(x, i)})" for i in (1, 2))
            u0 = f"toInt128({_field(x, 3)})"
            negation = "0"
            if negate:
                m0, d0, u0 = f"(-{m0})", f"(-{d0})", f"(-{u0})"
                negation = _raise(
                    "22008",
                    _RANGE,
                    f"({m0} > {_I32_MAX} OR {d0} > {_I32_MAX} OR {u0} > {_I64_MAX} "
                    f"OR ({m0} = {_I32_MAX} AND {d0} = {_I32_MAX} AND {u0} = {_I64_MAX}))",
                )

            def calendar(total: str) -> str:
                ny = f"intDiv({total} - if({total} < 0, 11, 0), 12)"

                def day(y: str, m: str) -> str:
                    leap = f"(({y} % 4 = 0 AND {y} % 100 != 0) OR {y} % 400 = 0)"
                    last = f"if({m} = 2 AND {leap}, 29, arrayElement({_MONTH_DAYS}, toUInt8({m})))"
                    nd = f"least({whole.day}, {last})"
                    first = f"toInt128({_days_from_civil(y, m, nd)}) * {_DAY_US} + {tod}"
                    s1 = f"if({m0} = 0, toInt128({t}), {first})"

                    def steps(one: str, two: str, three: str) -> str:
                        outside = " OR ".join(
                            f"({s} < {_FIRST_US} OR {s} > {_LAST_US})" for s in (one, two, three)
                        )
                        raised = _raise("0A000", message, f"({outside})")
                        held = f"toInt64(if({outside}, {_FIRST_US}, {three}))"
                        return f"fromUnixTimestamp64Micro({held} + {raised} + {negation}, 'UTC')"

                    def after(one: str) -> str:
                        two = f"({one} + toInt128({d0}) * {_DAY_US})"
                        return sql.let(
                            [two], lambda second: steps(one, second, f"({second} + {u0})")
                        )

                    return sql.let([s1], after)

                return sql.let([ny, f"({total} - {ny} * 12 + 1)"], day)

            answer = sql.let([f"(toInt64({start}) + {m0})"], calendar)
            return f"if(isNull({_field(x, 1)}), {_NULL_STAMP}, {answer})"

        return sql.let([interval], body)

    return form


# --- the rows -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class _Row:
    """A function of intervals: its PostgreSQL name, its arguments' kinds (i: an interval,
    t: a timestamp, d: a date, u: a unit's text), what it answers (interval, or another type's
    ClickHouse value) and its form."""

    names: frozenset[str]
    kinds: str
    form: Callable[..., str]
    interval: bool
    untyped: bool = False
    aggregate: bool = False


def _rows() -> dict[str, _Row]:
    rows: dict[str, _Row] = {}
    for name, op in _COMPARISONS.items():
        rows[f"interval_{name}"] = _Row(
            frozenset({f"interval_{name}"}), "ii", _compared(op), False, untyped=True
        )
    rows["interval_cmp"] = _Row(frozenset({"interval_cmp"}), "ii", _cmp, False, untyped=True)
    rows["interval_larger"] = _Row(
        frozenset({"interval_larger"}), "ii", _chosen(">"), True, untyped=True
    )
    rows["interval_smaller"] = _Row(
        frozenset({"interval_smaller"}), "ii", _chosen("<"), True, untyped=True
    )
    rows["interval_pl"] = _Row(
        frozenset({"interval_pl"}), "ii", _arithmetic("+"), True, untyped=True
    )
    rows["interval_mi"] = _Row(
        frozenset({"interval_mi"}), "ii", _arithmetic("-"), True, untyped=True
    )
    rows["interval_um"] = _Row(frozenset({"interval_um"}), "i", _negated, True, untyped=True)
    rows["justify_days"] = _Row(frozenset({"justify_days"}), "i", _justify_days, True, untyped=True)
    rows["justify_hours"] = _Row(
        frozenset({"justify_hours"}), "i", _justify_hours, True, untyped=True
    )
    rows["justify_interval"] = _Row(
        frozenset({"justify_interval"}), "i", _justify_interval, True, untyped=True
    )
    rows["isfinite"] = _Row(frozenset({"isfinite"}), "i", _isfinite, False)
    rows["extract"] = _Row(frozenset({"extract"}), "ui", _part(True), False)
    rows["date_part"] = _Row(frozenset({"date_part"}), "ui", _part(False), False)
    rows["max"] = _Row(frozenset({"max"}), "i", _extreme("max", "Max"), True, aggregate=True)
    rows["min"] = _Row(frozenset({"min"}), "i", _extreme("min", "Min"), True, aggregate=True)
    rows["sum"] = _Row(frozenset({"sum"}), "i", _sum, True, aggregate=True)
    for function, kinds, negate in (
        ("timestamp_pl_interval", "ti", False),
        ("timestamp_mi_interval", "ti", True),
        ("interval_pl_timestamp", "it", False),
        ("date_pl_interval", "di", False),
        ("date_mi_interval", "di", True),
        ("interval_pl_date", "id", False),
    ):
        rows[function] = _Row(
            frozenset({function}), kinds, _shifted(function, negate), False, untyped=True
        )
    return rows


_ROWS = _rows()
_NAMES = frozenset(_ROWS)

#: PostgreSQL's operators of intervals, by sqlglot's node: the row each is, by its operands'
#: kinds (unary minus has one).
_OPERATORS: dict[type[exp.Expr], dict[str, str]] = {
    exp.EQ: {"ii": "interval_eq"},
    exp.NEQ: {"ii": "interval_ne"},
    exp.LT: {"ii": "interval_lt"},
    exp.LTE: {"ii": "interval_le"},
    exp.GT: {"ii": "interval_gt"},
    exp.GTE: {"ii": "interval_ge"},
    exp.Add: {
        "ii": "interval_pl",
        "ti": "timestamp_pl_interval",
        "it": "interval_pl_timestamp",
        "di": "date_pl_interval",
        "id": "interval_pl_date",
    },
    exp.Sub: {"ii": "interval_mi", "ti": "timestamp_mi_interval", "di": "date_mi_interval"},
    exp.Neg: {"i": "interval_um"},
}


def _kind(argument: exp.Expr) -> str | None:
    """An operand's kind, as the statement says it: i, t or d; None for any other."""
    value = argument.unnest()
    if value.meta.get(HELD):
        return "i"
    said = _said(value)
    if said == "interval" or (isinstance(value, exp.Column) and _held_column(value)):
        return "i"
    if said in _KIND_TYPES["timestamp"]:
        return "t"
    if said == "date":
        return "d"
    call = trino.called(value)
    if call is not None:
        named = call[0] & _NAMES
        if named and _ROWS[next(iter(named))].interval:
            return "i"
    if type(value) in _OPERATORS:
        operands = _operands(value)
        if operands is not None and _OPERATORS[type(value)].get(operands[0]) in _INTERVAL_ROWS:
            return "i"
    return None


_INTERVAL_ROWS = frozenset(name for name, row in _ROWS.items() if row.interval)


def _operands(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    args = [node.this] if isinstance(node, exp.Neg) else [node.this, node.expression]
    kinds = [_kind(a) for a in args]
    if None in kinds:
        return None
    return "".join(k for k in kinds if k is not None), args


def _mentions_interval(node: exp.Expr) -> bool:
    """Whether ``node`` may hold an interval: a type of interval, a value a row here wrote, a call
    of a row here, or a column of a VALUES list of intervals. Read before any operand's type,
    which is costly to read of a deep expression (function_types reads every call it holds)."""
    for inner in node.walk():
        if inner.meta.get(HELD):
            return True
        if isinstance(inner, exp.DataType) and inner.this == exp.DataType.Type.INTERVAL:
            return True
        if isinstance(inner, exp.Anonymous) and inner.name.lower() in _NAMES - _SHARED:
            return True
        if isinstance(inner, exp.Column) and _interval_column(inner):
            return True
    return False


#: The names of rows here that other types' calls have too: a call of one says no interval.
_SHARED = frozenset({"max", "min", "sum", "extract", "date_part", "isfinite"})


def _interval_column(column: exp.Column) -> bool:
    """Whether ``column`` is one of a VALUES list whose cells are intervals (cast, or written)."""
    select = column.find_ancestor(exp.Select)
    source = select.args.get("from_") if select is not None else None
    values = source.this if source is not None else None
    if not isinstance(values, exp.Values) or values.args.get("alias") is None:
        return False
    names = [c.name for c in values.args["alias"].columns]
    if column.name not in names:
        return False
    index = names.index(column.name)
    for row in values.expressions:
        cell = row.expressions[index].unnest()
        if cell.meta.get(HELD) or (
            isinstance(cell, exp.Cast) and cell.to.this == exp.DataType.Type.INTERVAL
        ):
            return True
    return False


def _call(node: exp.Expr) -> tuple[str, _Row, list[exp.Expr]] | None:
    """The row ``node`` is a call of, by its name or its operator, and its arguments."""
    if not _mentions_interval(node):
        return None
    if type(node) in _OPERATORS:
        operands = _operands(node)
        if operands is None:
            return None
        name = _OPERATORS[type(node)].get(operands[0])
        return None if name is None else (name, _ROWS[name], operands[1])
    call = trino.called(node)
    if call is None:
        return None
    named = call[0] & _NAMES
    if not named:
        return None
    name = next(iter(named))
    row = _ROWS[name]
    args = call[1]
    if len(args) != len(row.kinds):
        return None
    return name, row, args


def _written(node: exp.Expr, sql: _Sql) -> str | None:
    """The ClickHouse SQL of ``node``, a call of a row here, its arguments read; None where the row
    does not read them. An interval answer is the held tuple."""
    found = _call(node)
    if found is None:
        return None
    name, row, args = found
    if row.aggregate and not _plain_aggregate(node):
        return None
    if (name == "isfinite" or row.aggregate) and _said_kinds(args) != "i":
        return None  # max, min, sum and isfinite of another type are other rows'
    read: list[str] = []
    unit: str | None = None
    for kind, argument in zip(row.kinds, args):
        if kind == "i":
            value = _interval(argument, sql, untyped=row.untyped)
        elif kind == "u":
            text = _unit(argument)
            if text is False:
                return None
            unit = text if isinstance(text, str) else None
            continue
        else:
            value = _instant(argument, "timestamp" if kind == "t" else "date", row.untyped)
        if value is None:
            return None
        read.append(value)
    if row.kinds == "ui":
        return row.form(sql, read, unit)
    if row.kinds in ("ti", "di"):
        return row.form(sql, read[0], read[1])
    if row.kinds in ("it", "id"):
        return row.form(sql, read[1], read[0])
    return row.form(sql, read)


def _said_kinds(args: list[exp.Expr]) -> str:
    return "".join(_kind(a) or "?" for a in args)


def _plain_aggregate(node: exp.Expr) -> bool:
    """An aggregate of its rows: no window, no DISTINCT, no FILTER, which this row does not read."""
    if isinstance(node.parent, (exp.Window, exp.Filter)):
        return False
    return not any(isinstance(a, exp.Distinct) for a in node.iter_expressions())


def _unit(argument: exp.Expr) -> str | None | bool:
    """extract's and date_part's unit: a literal's text, None for a NULL, False for one a row
    computes (refused by name)."""
    value = argument.unnest()
    if isinstance(value, exp.Cast):
        value = value.this.unnest()
    if isinstance(value, exp.Null):
        return None
    text = _string(value)
    if text is None and isinstance(value, exp.Var):
        text = value.name  # extract(year from x): its unit a keyword
    return False if text is None else text


def _answered_as_text(node: exp.Expr) -> bool | None:
    """Where an interval answer goes: True to the client (the select list), False to another row
    here, None anywhere else (the call is refused by name)."""
    parent = node.parent
    while isinstance(parent, exp.Paren):
        parent = parent.parent
    if isinstance(parent, exp.Alias):
        parent = parent.parent
        return True if isinstance(parent, exp.Select) else None
    if isinstance(parent, exp.Select):
        return True if node.arg_key == "expressions" or isinstance(node.parent, exp.Paren) else None
    if parent is None:
        # a copy of a pattern rule's argument, read by that rule (function_macros._polyfilled):
        # the walk's first reading (inline's) saw where it stands, and refused it anywhere else
        return False
    if type(parent) in _OPERATORS:
        return False
    call = trino.called(parent)
    if call is not None and call[0] & _NAMES:
        return False
    return None


def _rewrite(name: str) -> Callable[[exp.Expr, str], exp.Expr | None]:
    """The rewrite of the row ``name``: a call of it (or of its operator) held as
    Tuple(months, days, microseconds), in PostgreSQL's arithmetic."""

    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        if dialect != _CLICKHOUSE or (type(node) not in _OPERATORS and trino.called(node) is None):
            return None
        found = _call(node)
        if found is None or found[0] != name:
            return None
        sql = _Sql()
        text = _written(node, sql)
        if text is None:
            return None
        shown = _answered_as_text(node) if found[1].interval else False
        if shown is None and type(node) in _OPERATORS:
            # an operator names no function, so no older refusal reads it (engine_refusals): its
            # interval would reach ClickHouse as a type it has none of
            raise Refused(Refusal("0A000", f"{name}() has no form on {_CLICKHOUSE}"), ruling=True)
        if shown is None:
            return None
        tree = sql.parsed(sql.text(text) if shown else text)
        if found[1].interval and not shown:
            tree.meta[HELD] = True
        return tree

    rewrite.__doc__ = f"{name} of intervals held as Tuple(months, days, microseconds)."
    rewrite.__qualname__ = rewrite.__name__ = f"{name}_of_intervals"
    return rewrite


#: What a statement a row may rewrite holds where it names no function: an interval's operator.
_MENTIONS = r"\binterval\b"

_HELD_STATED = (
    "Held as Tuple(months Int32, days Int32, microseconds Int64), PostgreSQL's own fields: exact. "
    "A literal is read as PostgreSQL's interval input reads it (IntervalStyle postgres): amounts "
    "and their units, a clock, signs, fractions, '@' and 'ago'. ISO 8601 and SQL standard input, "
    "a bare number, infinity, and a value whose type no statement says (a table's column) are "
    "refused by name."
)
_TEXT_STATED = (
    " An interval answer in the select list is interval_out's text (IntervalStyle postgres); one "
    "anywhere else but another interval call is refused by name."
)
_RANGE_STATED = " A field that overflows raises 22008 (interval out of range), as PostgreSQL does."


@dataclass(frozen=True)
class _Doc:
    feature: str
    summary: str
    stated: str = ""


_DOCS: dict[str, _Doc] = {
    "interval_eq": _Doc("interval_eq(interval, interval)", "Whether two intervals are equal (=)."),
    "interval_ne": _Doc("interval_ne(interval, interval)", "Whether two intervals differ (<>)."),
    "interval_lt": _Doc("interval_lt(interval, interval)", "Whether an interval is less (<)."),
    "interval_le": _Doc("interval_le(interval, interval)", "Whether an interval is not more (<=)."),
    "interval_gt": _Doc("interval_gt(interval, interval)", "Whether an interval is more (>)."),
    "interval_ge": _Doc("interval_ge(interval, interval)", "Whether an interval is not less (>=)."),
    "interval_cmp": _Doc(
        "interval_cmp(interval, interval)", "btree's comparison of two intervals."
    ),
    "interval_larger": _Doc(
        "interval_larger(interval, interval)", "The larger of two intervals.", _TEXT_STATED
    ),
    "interval_smaller": _Doc(
        "interval_smaller(interval, interval)", "The smaller of two intervals.", _TEXT_STATED
    ),
    "interval_pl": _Doc(
        "interval_pl(interval, interval)",
        "The sum of two intervals (+).",
        _TEXT_STATED + _RANGE_STATED,
    ),
    "interval_mi": _Doc(
        "interval_mi(interval, interval)",
        "The difference of two intervals (-).",
        _TEXT_STATED + _RANGE_STATED,
    ),
    "interval_um": _Doc(
        "interval_um(interval)", "An interval negated (unary -).", _TEXT_STATED + _RANGE_STATED
    ),
    "justify_days": _Doc(
        "justify_days(interval)",
        "An interval with each 30 days as a month.",
        _TEXT_STATED + _RANGE_STATED,
    ),
    "justify_hours": _Doc(
        "justify_hours(interval)",
        "An interval with each 24 hours as a day.",
        _TEXT_STATED + _RANGE_STATED,
    ),
    "justify_interval": _Doc(
        "justify_interval(interval)",
        "An interval justified by days and hours, its signs one.",
        _TEXT_STATED + _RANGE_STATED,
    ),
    "isfinite": _Doc(
        "isfinite(interval)",
        "Whether an interval is finite.",
        " No interval held here is infinite: an infinity's literal is refused by name.",
    ),
    "extract": _Doc(
        "extract(text, interval)",
        "A part of an interval, as a numeric.",
        " The unit is read as the statement writes it, a literal or a NULL; one a row computes is "
        "refused by name. A unit an interval has no part of raises 0A000, and one PostgreSQL does "
        "not know 22023, as PostgreSQL's do.",
    ),
    "date_part": _Doc(
        "date_part(text, interval)",
        "A part of an interval, as a double precision.",
        " The unit is read as the statement writes it, a literal or a NULL; one a row computes is "
        "refused by name. A unit an interval has no part of raises 0A000, and one PostgreSQL does "
        "not know 22023, as PostgreSQL's do.",
    ),
    "max": _Doc(
        "max(interval)",
        "The largest of a group's intervals.",
        _TEXT_STATED + " PostgreSQL answers the last of intervals that are equal but written "
        "apart ('1 mon' and '30 days'), in its scan's order, which ClickHouse does not keep: such "
        "a tie for the largest raises 0A000. A window, DISTINCT or FILTER is refused by name.",
    ),
    "min": _Doc(
        "min(interval)",
        "The smallest of a group's intervals.",
        _TEXT_STATED + " PostgreSQL answers the last of intervals that are equal but written "
        "apart, in its scan's order, which ClickHouse does not keep: such a tie for the smallest "
        "raises 0A000. A window, DISTINCT or FILTER is refused by name.",
    ),
    "sum": _Doc(
        "sum(interval)",
        "The sum of a group's intervals.",
        _TEXT_STATED + " A total past a field raises 22008 (interval out of range). PostgreSQL "
        "raises it where a running total overflows, which depends on its scan's order: where the "
        "rows of one sign alone reach a field's end and the total does not, 0A000 is raised. A "
        "window, DISTINCT or FILTER is refused by name.",
    ),
}
_SHIFT_STATED = (
    " The {} is read as a literal in ISO form or a NULL; any other is refused by name. The answer "
    "is held as DateTime64(6, 'UTC'): an answer, or the step of it after the months or after the "
    f"days, outside {instants.STAMP_BOUND} raises 0A000 ({{}}() of a value outside "
    f"{instants.STAMP_BOUND} has no exact form on clickhouse)."
)
for _name, _what, _summary in (
    ("timestamp_pl_interval", "timestamp", "A timestamp plus an interval (+)."),
    ("timestamp_mi_interval", "timestamp", "A timestamp less an interval (-)."),
    ("interval_pl_timestamp", "timestamp", "An interval plus a timestamp (+)."),
    ("date_pl_interval", "date", "A date plus an interval, a timestamp (+)."),
    ("date_mi_interval", "date", "A date less an interval, a timestamp (-)."),
    ("interval_pl_date", "date", "An interval plus a date, a timestamp (+)."),
):
    _types = ", ".join("interval" if k == "i" else _what for k in _ROWS[_name].kinds)
    _DOCS[_name] = _Doc(f"{_name}({_types})", _summary, _SHIFT_STATED.format(_what, _name))


def _isfinite_rewrite() -> Callable[[exp.Expr, str], exp.Expr | None]:
    """isfinite's rewrite: instants' row reads every isfinite by its pattern, so this row's pattern
    is read first, and a call of no interval is that row's."""
    of_intervals = _rewrite("isfinite")
    of_instants = next(
        rule.forms[_CLICKHOUSE]
        for feature in instants.FEATURES
        if feature.name == "isfinite(timestamptz)"
        for rule in feature.read.rules
    )
    assert isinstance(of_instants, Rewrite)

    def rewrite(node: exp.Expr, dialect: str) -> exp.Expr | None:
        return of_intervals(node, dialect) or of_instants.function(node, dialect)

    rewrite.__doc__ = of_intervals.__doc__
    rewrite.__qualname__ = rewrite.__name__ = of_intervals.__name__
    return rewrite


def _feature(name: str) -> Feature:
    doc = _DOCS[name]
    stated = _HELD_STATED + doc.stated
    if name == "isfinite":
        forms = {
            **{d: trino.KEPT for d in DIALECTS if d not in NATIVE},
            _CLICKHOUSE: Rewrite(_isfinite_rewrite()),
        }
        rule = Rule("isfinite($a)", forms, unlisted_keep=True)
    else:
        rule = Rule(None, {_CLICKHOUSE: Rewrite(_rewrite(name), mentions=_MENTIONS)})
    return Feature(
        doc.feature,
        FAMILY,
        doc.summary,
        read=Side(rules=(rule,), limitations={_CLICKHOUSE: (Limitation(stated),)}),
    )


#: The rows. isfinite's is read before instants' (polyfill.GRID lists this family first).
FEATURES: tuple[Feature, ...] = tuple(_feature(name) for name in _ROWS)

__all__ = ["FEATURES", "HELD", "interval_read", "interval_text"]
