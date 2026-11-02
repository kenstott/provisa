# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9b6e20-71ac-4c58-92d4-e5a10c7d8b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What PostgreSQL reads of a timestamptz in the session's zone, on the engines that do not (REQ-2171).

Telling Trino or ClickHouse the zone does not move an instant: a Trino value carries its own zone,
and ClickHouse's toHour, toDate, dateTrunc and toString go by the column's. So the zone is written
into each of c::text, c::date, extract and date_trunc as the statement is transpiled, and a transpiled
form belongs to the zone it was written for (``zone_cache_key``). On UTC only the text is written:
each engine's own is not PostgreSQL's, and the others already answer as PostgreSQL does.

Which values are instants is the value's type, which the engine knows and the statement's text does
not say, so each form asks the engine (typeof, toTypeName). A branch not taken must still type
check, so the instant is reached through its text, which every type has. Each engine reads what it
holds with no zone as a plain timestamp, written as it is.

to_json is not here: Trino refuses a cast of an instant to JSON while it types the statement, which
no branch can avoid (execute_trino names the refusal), and ClickHouse has no form of it."""

# Requirements: REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.core.session_time_zone import (
    StatementZone,
    clickhouse_spelling,
    current_zone,
    refused_zone,
    trino_spelling,
)

_MARKED = "provisa_"
_TEXT_CALLS = frozenset({"replace", "regexp_replace", "substr", "trim", "lower", "upper"})
_NO_INSTANT = (
    exp.Literal,
    exp.Null,
    exp.Boolean,
    exp.Placeholder,
    exp.Parameter,
    exp.Interval,
    exp.DPipe,
    exp.Concat,
    exp.Lower,
    exp.Upper,
    exp.Substring,
    exp.Trim,
    exp.Replace,
)
_MENTIONED = re.compile(r"::|\b(?:CAST|EXTRACT|DATE_PART|DATE_TRUNC|DATE)\b", re.IGNORECASE)
_TEXTS = (exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR)
_DAY = (exp.DataType.Type.DATE,)
#: What a zone cannot move: a count of seconds since the epoch, and a unit no offset is a part of.
_NOT_ZONED_PARTS = frozenset({"EPOCH"})
_NOT_ZONED_UNITS = frozenset(
    {"SECOND", "MILLISECOND", "MILLISECONDS", "MICROSECOND", "MICROSECONDS"}
)


def _clickhouse_declared(v: str) -> str:
    """The zone ``v``'s type declares, empty where it declares none."""
    return f"extract(toTypeName({v}), '''([^'']+)''')"


def _clickhouse_local(v: str, zone: str) -> str:
    declared = _clickhouse_declared(v)
    return (
        f"toTimeZone(parseDateTime64BestEffortOrNull(toString({v}), 6, "
        f"if({declared} = '', 'UTC', {declared})), '{zone}')"
    )


def _clickhouse_text(local: str) -> str:
    wall = rf"replaceRegexpOne(toString({local}), '([.][0-9]*[1-9])0+$', '\\1')"
    offset = rf"replaceRegexpOne(formatDateTime({local}, '%z'), '^(...)(..)$', '\\1:\\2')"
    return f"concat(replaceRegexpOne({wall}, '[.]0+$', ''), replaceRegexpOne({offset}, ':00$', ''))"


def _trino_text(local: str) -> str:
    wall = (
        f"regexp_replace(CAST(CAST({local} AS TIMESTAMP(6)) AS VARCHAR), "
        "'([.][0-9]*[1-9])0+$', '$1')"
    )
    offset = rf"regexp_replace(format_datetime({local}, 'ZZ'), '^Z$', '+00')"
    return f"concat(regexp_replace({wall}, '[.]0+$', ''), regexp_replace({offset}, ':00$', ''))"


def _trino_trunc(v: str, local: str, unit: str) -> str:
    """The session's start of ``unit``, as a value of ``v``'s own type: the other branch has that
    type, and one a date or a timestamp reads must not come back an instant."""
    return (
        f"date_add('second', date_diff('second', date_trunc('second', {local}), "
        f"date_trunc('{unit}', {local})), date_trunc('second', {v}))"
    )


@dataclass(frozen=True)
class _Engine:
    """An engine's reading of a value ``v``: whether it is an instant; the instant in a zone; and
    of that local instant its text and date, and of ``v`` the start of a unit."""

    name: str
    spelling: Callable[[StatementZone], str | None]
    is_instant: Callable[[str], str]
    local: Callable[[str, str], str]
    text: Callable[[str], str]
    day: Callable[[str], str]
    trunc: Callable[[str, str, str], str]


_ENGINES = {
    "trino": _Engine(
        name="Trino",
        spelling=trino_spelling,
        is_instant=lambda v: f"typeof({v}) LIKE 'timestamp(%) with time zone'",
        local=lambda v, zone: (
            f"CAST(CAST({v} AS VARCHAR) AS TIMESTAMP(6) WITH TIME ZONE) AT TIME ZONE '{zone}'"
        ),
        text=_trino_text,
        day=lambda local: f"CAST({local} AS DATE)",
        trunc=_trino_trunc,
    ),
    "clickhouse": _Engine(
        name="ClickHouse",
        spelling=clickhouse_spelling,
        is_instant=lambda v: f"{_clickhouse_declared(v)} != ''",
        local=_clickhouse_local,
        text=_clickhouse_text,
        day=lambda local: f"toDate32({local})",
        trunc=lambda _v, local, unit: f"toTimeZone(dateTrunc('{unit}', {local}), 'UTC')",
    ),
}


def _zone_named(engine: _Engine) -> str:
    zone = current_zone.get()
    if zone is None:
        return "UTC"
    named = engine.spelling(zone)
    if named is None:
        raise refused_zone(engine.name, zone)
    return named


def _may_be_instant(value: exp.Expr) -> bool:
    """False for what the statement itself says is no instant, which needs no asking."""
    if isinstance(value, exp.Paren):
        return _may_be_instant(value.this)
    if isinstance(value, exp.Cast):
        return value.to.is_type(exp.DataType.Type.TIMESTAMPTZ)
    if isinstance(value, exp.Anonymous):
        name = str(value.this).lower()
        return not (name.startswith(_MARKED) or name in _TEXT_CALLS)
    if isinstance(value, exp.Case):
        taken = [branch.args["true"] for branch in value.args["ifs"]] + [value.args.get("default")]
        return any(_may_be_instant(branch) for branch in taken if branch is not None)
    return not isinstance(value, _NO_INSTANT)


def _instant_form(node: exp.Expr, value: exp.Expr, engine: _Engine, dialect: str) -> str | None:
    """``node``'s answer for an instant ``value``; None where the zone cannot move it. A zone the
    engine has no spelling of is refused here, where a form is written in it."""
    is_text = isinstance(node, exp.Cast) and node.to.this in _TEXTS
    if current_zone.get() is None and not is_text:
        return None
    v = value.sql(dialect=dialect)
    if isinstance(node, exp.Cast) and is_text:
        written = engine.text(engine.local(v, _zone_named(engine)))
        if node.to.expressions:  # c::varchar(5) is the text, cut
            return f"CAST({written} AS {node.to.sql(dialect=dialect)})"
        return written
    if isinstance(node, exp.Date) or (isinstance(node, exp.Cast) and node.to.this in _DAY):
        return engine.day(engine.local(v, _zone_named(engine)))
    if isinstance(node, exp.Extract) and node.name.upper() not in _NOT_ZONED_PARTS:
        local = sqlglot.parse_one(engine.local(v, _zone_named(engine)), read=dialect)
        return exp.Extract(this=node.this.copy(), expression=local).sql(dialect=dialect)
    if isinstance(node, exp.TimestampTrunc) and node.text("unit").upper() not in _NOT_ZONED_UNITS:
        return engine.trunc(v, engine.local(v, _zone_named(engine)), node.text("unit").upper())
    return None


def _form(node: exp.Expr, engine: _Engine, dialect: str) -> exp.Expr | None:
    value = node.expression if isinstance(node, exp.Extract) else node.this
    if not _may_be_instant(value):
        return None
    instant = _instant_form(node, value, engine, dialect)
    if instant is None:
        return None
    asked = engine.is_instant(value.sql(dialect=dialect))
    return sqlglot.parse_one(
        f"CASE WHEN {asked} THEN {instant} ELSE {node.sql(dialect=dialect)} END", read=dialect
    )


def written(sql: str, tree: exp.Expr | None, dialect: str) -> exp.Expr | None:
    """``tree`` (the parse of ``sql``, or None for it not yet parsed) with each read of an instant
    in the session's zone, on ``dialect``; ``tree`` as it was where nothing is to be written."""
    engine = _ENGINES.get(dialect)
    if engine is None or _MENTIONED.search(sql) is None:
        return tree
    parsed = tree if tree is not None else sqlglot.parse_one(sql, read="postgres")
    wrote = False
    nodes = parsed.find_all(exp.Cast, exp.Date, exp.Extract, exp.TimestampTrunc)
    for node in reversed(list(nodes)):  # the deepest first: a form holds its value's form
        form = _form(node, engine, dialect)
        if form is not None:
            node.replace(form)
            wrote = True
    return parsed if wrote else tree
