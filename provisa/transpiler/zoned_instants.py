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
form belongs to the zone it was written for (``zone_cache_key``). On UTC only the text is written,
whose engine form is not PostgreSQL's, and on ClickHouse the date and the start of a unit as well:
its own are a Date and a DateTime, which reach Arrow as counts of days and seconds.

Which values are instants is the value's type, which the engine knows and the statement's text does
not say, so each form asks the engine (typeof, toTypeName). A branch not taken must still type
check, so the instant is reached through its text, which every type has. Each engine reads what it
holds with no zone as a plain timestamp, written as it is.

to_json of an instant on Trino is written here, where the statement says the value is one (a cast
to timestamptz, as the compiler states a registered column's type, or the clock): PostgreSQL's
text of it as a JSON string. Trino refuses a cast of an instant to JSON while it types the
statement, which no branch can avoid, so an instant the statement does not say is one is refused by
name there (execute_trino), and one it says is inside an array or a row is refused by name here,
as is one json_agg or json_build_object reads: Trino's forms of them cast it to JSON too.
ClickHouse has no form of to_json."""

# Requirements: REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp


from provisa.core.pg_error import PostgresError
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
_MENTIONED = re.compile(
    r"::|\b(?:CAST|EXTRACT|DATE_PART|DATE_TRUNC|DATE|TO_JSONB?|JSON_AGG|JSONB?_BUILD_OBJECT)\b",
    re.IGNORECASE,
)
_TEXTS = (exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR)
_JSON_CALLS = frozenset({"to_json", "to_jsonb"})
#: A json or jsonb function's answer is never an instant: its text is jsonb_out's to refuse.
_JSON_PREFIXES = ("json", "to_json")
_JSON_OBJECTS = frozenset({"json_build_object", "jsonb_build_object"})
_DAY = (exp.DataType.Type.DATE,)
#: What a zone cannot move: a count of seconds since the epoch, and a unit no offset is a part of.
_NOT_ZONED_PARTS = frozenset({"EPOCH"})
_NOT_ZONED_UNITS = frozenset(
    {"SECOND", "MILLISECOND", "MILLISECONDS", "MICROSECOND", "MICROSECONDS"}
)


def _clickhouse_declared(v: str) -> str:
    """The zone ``v``'s type declares, empty where it declares none: its type name's first quoted
    part. Not ClickHouse's extract(), which the function forms read as PostgreSQL's and refuse."""
    return f"arrayElement(splitByChar('''', toTypeName({v})), 2)"


def _clickhouse_local(v: str, zone: str) -> str:
    declared = _clickhouse_declared(v)
    return (
        f"toTimeZone(parseDateTime64BestEffortOrNull(toString({v}), 6, "
        f"if({declared} = '', 'UTC', {declared})), '{zone}')"
    )


def _clickhouse_two_digits(count: str) -> str:
    return f"substring(toString(100 + {count}), 2)"


def _clickhouse_offset(local: str) -> str:
    """The local instant's offset as PostgreSQL writes it: +HH, +HH:MM or +HH:MM:SS. From its count
    of seconds, not formatDateTime, which the function forms read as to_char and refuse."""
    seconds = f"timeZoneOffset({local})"
    whole = f"abs({seconds})"
    minutes = _clickhouse_two_digits(f"intDiv({whole} % 3600, 60)")
    return (
        f"concat(if({seconds} < 0, '-', '+'), {_clickhouse_two_digits(f'intDiv({whole}, 3600)')}, "
        f"if({whole} % 3600 = 0, '', concat(':', {minutes})), "
        f"if({whole} % 60 = 0, '', concat(':', {_clickhouse_two_digits(f'{whole} % 60')})))"
    )


def _clickhouse_text(local: str) -> str:
    wall = rf"replaceRegexpOne(toString({local}), '([.][0-9]*[1-9])0+$', '\\1')"
    return f"concat(replaceRegexpOne({wall}, '[.]0+$', ''), {_clickhouse_offset(local)})"


def _trino_wall(local: str) -> str:
    """The local instant's date and time, its fraction's trailing zeros dropped."""
    wall = (
        f"regexp_replace(CAST(CAST({local} AS TIMESTAMP(6)) AS VARCHAR), "
        "'([.][0-9]*[1-9])0+$', '$1')"
    )
    return f"regexp_replace({wall}, '[.]0+$', '')"


def _trino_text(local: str) -> str:
    offset = rf"regexp_replace(format_datetime({local}, 'ZZ'), '^Z$', '+00')"
    return f"concat({_trino_wall(local)}, regexp_replace({offset}, ':00$', ''))"


def _trino_json(local: str) -> str:
    """to_json's text of the instant: the T, and an offset's minutes, as a JSON string."""
    offset = rf"regexp_replace(format_datetime({local}, 'ZZ'), '^Z$', '+00:00')"
    return f"json_parse(concat('\"', replace({_trino_wall(local)}, ' ', 'T'), {offset}, '\"'))"


def _trino_trunc(v: str, local: str, unit: str, _zone: str) -> str:
    """The session's start of ``unit``, as a value of ``v``'s own type: the other branch has that
    type, and one a date or a timestamp reads must not come back an instant."""
    return (
        f"date_add('second', date_diff('second', date_trunc('second', {local}), "
        f"date_trunc('{unit}', {local})), date_trunc('second', {v}))"
    )


#: The units Trino's date_trunc has none of: a value held to the microsecond is its own start of a
#: microsecond, in any zone, as PostgreSQL answers.
_TRINO_OWN_START = frozenset({"MICROSECOND", "MICROSECONDS"})


def _trino_plain_trunc(v: str, unit: str) -> str | None:
    return v if unit in _TRINO_OWN_START else None


@dataclass(frozen=True)
class _Engine:
    """An engine's reading of a value ``v``: whether it is an instant; the instant in a zone; and
    of that local instant its text and date, and of ``v`` the start of a unit in that zone; and
    whether its own date and start of a unit are written on UTC too. ``plain_trunc`` is the start
    of a unit of a ``v`` that is no instant, None where the engine's own form answers."""

    name: str
    spelling: Callable[[StatementZone], str | None]
    is_instant: Callable[[str], str]
    local: Callable[[str, str], str]
    text: Callable[[str], str]
    day: Callable[[str], str]
    trunc: Callable[[str, str, str, str], str]
    plain_trunc: Callable[[str, str], str | None]
    own_day_differs: bool


#: A unit shorter than a minute, in microseconds: its start is the instant's count floored, in any
#: zone, as no zone's offset has a part shorter than a second (the tz database and ClickHouse hold
#: offsets in whole seconds), so no offset can move it. ClickHouse's dateTrunc and toStartOfSecond,
#: as sqlglot writes them, are a DateTime, which drops the fraction and reaches Arrow as a count of
#: seconds; and ClickHouse 24.3's dateTrunc refuses a millisecond and a microsecond, in a branch not
#: taken too, as it types every branch.
def _clickhouse_trunc(_v: str, local: str, unit: str, zone: str) -> str:
    """The session's start of ``unit`` as Provisa's instant, DateTime64(6, 'UTC'): the grid's
    builder (polyfill.clickhouse.truncated) of the wall clock in ``zone``."""
    from provisa.transpiler.polyfill import clickhouse  # read when called, as semantic_dialect's

    start = clickhouse.truncated(unit, sqlglot.parse_one(local, read="clickhouse"), zone)
    return f"toTimeZone({start.sql(dialect='clickhouse')}, 'UTC')"


def _clickhouse_plain_trunc(v: str, unit: str) -> str | None:
    """The start of sub-minute ``unit`` of a plain timestamp (or a date), by the grid's builder,
    its fraction kept. None for a longer unit, whose dateTrunc the same builder writes."""
    from provisa.transpiler.polyfill import clickhouse  # read when called, as semantic_dialect's

    if unit.lower() not in clickhouse.SUBMINUTE:
        return None
    return clickhouse.truncated(unit, sqlglot.parse_one(v, read="clickhouse")).sql(
        dialect="clickhouse"
    )


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
        plain_trunc=_trino_plain_trunc,
        own_day_differs=False,
    ),
    "clickhouse": _Engine(
        name="ClickHouse",
        spelling=clickhouse_spelling,
        is_instant=lambda v: f"{_clickhouse_declared(v)} != ''",
        local=_clickhouse_local,
        text=_clickhouse_text,
        day=lambda local: f"toDate32({local})",
        trunc=_clickhouse_trunc,
        plain_trunc=_clickhouse_plain_trunc,
        own_day_differs=True,
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
        return not (name.startswith((_MARKED, *_JSON_PREFIXES)) or name in _TEXT_CALLS)
    if isinstance(value, exp.Case):
        taken = [branch.args["true"] for branch in value.args["ifs"]] + [value.args.get("default")]
        return any(_may_be_instant(branch) for branch in taken if branch is not None)
    return not isinstance(value, _NO_INSTANT)


def _instant_form(node: exp.Expr, value: exp.Expr, engine: _Engine, dialect: str) -> str | None:
    """``node``'s answer for an instant ``value``; None where the zone cannot move it. A zone the
    engine has no spelling of is refused here, where a form is written in it."""
    is_text = isinstance(node, exp.Cast) and node.to.this in _TEXTS
    is_day = isinstance(node, exp.Date) or (isinstance(node, exp.Cast) and node.to.this in _DAY)
    written_on_utc = is_text or (engine.own_day_differs and (is_day or _is_trunc(node, engine)))
    if current_zone.get() is None and not written_on_utc:
        return None
    v = _written(value, dialect)
    if isinstance(node, exp.Cast) and is_text:
        return engine.text(engine.local(v, _zone_named(engine)))
    if is_day:
        return engine.day(engine.local(v, _zone_named(engine)))
    if isinstance(node, exp.Extract) and node.name.upper() not in _NOT_ZONED_PARTS:
        local = sqlglot.parse_one(engine.local(v, _zone_named(engine)), read=dialect)
        return exp.Extract(this=node.this.copy(), expression=local).sql(dialect=dialect)
    if _is_trunc(node, engine):
        zone = _zone_named(engine)
        return engine.trunc(v, engine.local(v, zone), node.text("unit").upper(), zone)
    return None


def _is_trunc(node: exp.Expr, engine: _Engine) -> bool:
    """A start of a unit the zone moves, or on an engine whose own is written for its type, any."""
    if not isinstance(node, exp.TimestampTrunc):
        return False
    return engine.own_day_differs or node.text("unit").upper() not in _NOT_ZONED_UNITS


def _plain_form(node: exp.Expr, value: exp.Expr, engine: _Engine, dialect: str) -> str | None:
    """``node``'s answer for a ``value`` that is no instant, None where the engine's own answers."""
    if not isinstance(node, exp.TimestampTrunc):
        return None
    return engine.plain_trunc(_written(value, dialect), node.text("unit").upper())


#: ClickHouse's instant, which a statement's timestamp is held as (transpile.clickhouse_semantics
#: writes every other): its DateTime holds no day before 1970.
CLICKHOUSE_INSTANT = "Nullable(DateTime64(6, 'UTC'))"
_TIMESTAMPS = (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ)


def _written(node: exp.Expr, dialect: str) -> str:
    """``node``'s SQL in ``dialect``: on ClickHouse each timestamp it casts to its DateTime64(6),
    as the statement's other timestamps are written, never its DateTime."""
    if dialect != "clickhouse":
        return node.sql(dialect=dialect)

    def held(part: exp.Expr) -> exp.Expr:
        if isinstance(part, exp.DataType) and part.this in _TIMESTAMPS:
            return exp.DataType.build(CLICKHOUSE_INSTANT, dialect="clickhouse")
        return part

    return node.copy().transform(held).sql(dialect=dialect)


def _form(node: exp.Expr, engine: _Engine, dialect: str) -> exp.Expr | None:
    value = node.expression if isinstance(node, exp.Extract) else node.this
    plain = _plain_form(node, value, engine, dialect)
    instant = _instant_form(node, value, engine, dialect) if _may_be_instant(value) else None
    if instant is None:
        return None if plain is None else sqlglot.parse_one(plain, read=dialect)
    asked = engine.is_instant(_written(value, dialect))
    otherwise = _written(node, dialect) if plain is None else plain
    form = sqlglot.parse_one(f"CASE WHEN {asked} THEN {instant} ELSE {otherwise} END", read=dialect)
    if isinstance(node, exp.Cast) and node.to.this in _TEXTS:
        # The text the statement says it is, which a function's overload reads (sha256(c::varchar)
        # is refused), and c::varchar(5) the text cut.
        return exp.Cast(this=form, to=node.to.copy())
    return form


def _said_instant(value: exp.Expr) -> bool:
    """Whether the statement itself says ``value`` is an instant."""
    if isinstance(value, exp.Paren):
        return _said_instant(value.this)
    if isinstance(value, exp.Cast):
        return value.to.is_type(exp.DataType.Type.TIMESTAMPTZ)
    return isinstance(value, exp.CurrentTimestamp)


def _holds_said_instant(value: exp.Expr) -> bool:
    """Whether ``value`` is an array or a row with an instant the statement says is one in it."""
    if isinstance(value, exp.Paren):
        return _holds_said_instant(value.this)
    row = isinstance(value, exp.Anonymous) and value.name.lower() == "row"
    if not (row or isinstance(value, (exp.Array, exp.Struct, exp.Tuple))):
        return False
    return any(_said_instant(m) or _holds_said_instant(m) for m in value.expressions)


def _json_of_instant(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """to_json(v) on Trino of an instant ``v`` the statement says is one, as PostgreSQL writes it;
    None for any other value, which Trino's own cast to JSON answers or refuses."""
    if not (isinstance(node, exp.Anonymous) and node.name.lower() in _JSON_CALLS):
        return None
    if dialect != "trino" or len(node.expressions) != 1:
        return None
    value = node.expressions[0]
    if _holds_said_instant(value):
        raise PostgresError(
            "0A000",
            f"{node.name.lower()} of a timestamptz in an array or a row is not available on Trino",
        )
    if not _said_instant(value):
        return None
    engine = _ENGINES[dialect]
    local = engine.local(_written(value, dialect), _zone_named(engine))
    return sqlglot.parse_one(_trino_json(local), read=dialect)


def _refuse_json_of_said_instant(tree: exp.Expr) -> None:
    """json_agg and json_build_object on Trino of an instant the statement says is one, refused by
    name: their forms there (JSON_PARSE, JSON_OBJECT) cast it to JSON, which Trino refuses."""
    for node in tree.find_all(exp.JSONArrayAgg, exp.Anonymous):
        if isinstance(node, exp.JSONArrayAgg):
            name = "json_agg"
            value = node.this.this if isinstance(node.this, exp.Order) else node.this
            values = [value]
        elif node.name.lower() in _JSON_OBJECTS:
            name = node.name.lower()
            values = node.expressions[1::2]
        else:
            continue
        if any(_said_instant(v) for v in values):
            raise PostgresError("0A000", f"{name} of a timestamptz is not available on Trino")


def written(sql: str, tree: exp.Expr | None, dialect: str) -> exp.Expr | None:
    """``tree`` (the parse of ``sql``, or None for it not yet parsed) with each read of an instant
    in the session's zone, on ``dialect``; ``tree`` as it was where nothing is to be written."""
    engine = _ENGINES.get(dialect)
    if engine is None or _MENTIONED.search(sql) is None:
        return tree
    parsed = tree if tree is not None else sqlglot.parse_one(sql, read="postgres")
    if dialect == "trino":
        _refuse_json_of_said_instant(parsed)
    wrote = False
    nodes = parsed.find_all(exp.Cast, exp.Date, exp.Extract, exp.TimestampTrunc)
    for node in reversed(list(nodes)):  # the deepest first: a form holds its value's form
        form = _form(node, engine, dialect)
        if form is not None:
            node.replace(form)
            wrote = True
    for node in list(parsed.find_all(exp.Anonymous)):
        json = _json_of_instant(node, dialect)
        if json is not None:
            node.replace(json)
            wrote = True
    return parsed if wrote else tree
