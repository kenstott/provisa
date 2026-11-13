# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e4c92-5a18-4f3d-9c26-e8d1a7f4b503
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's answers that differed from PostgreSQL's (REQ-2236; the function support measure,
run 43361055823, REQ-2171).

array_agg: ClickHouse's groupArray skips a NULL, and answers an empty array of no rows, where
PostgreSQL keeps each NULL and answers NULL of no rows. Its form aggregates each value in a tuple,
which groupArray keeps whatever it holds, and answers NULL where the aggregate counted no row. Of an
order (array_agg(x ORDER BY y)) it is refused by name: ClickHouse takes no ORDER BY in a call.

A NULL array: ClickHouse has no NULL of an array (an Array is never Nullable), so a call PostgreSQL
answers NULL for its NULL array argument is that NULL, read before any rule's pattern: a function
STRICT at its argument count, or one that tests its array for NULL first. A set-returning function
answers no rows of a NULL argument, not one NULL: it is left to its own row (generate_subscripts'
refuses it on ClickHouse), but unnest's, which is ClickHouse's arrayJoin of an empty array of the
type. max and min of a NULL array answer NULL too: each skips a NULL. A call sqlglot reads as a
typed node (array_to_string as ArrayToString) is read by the names that node answers to.

date_trunc: ClickHouse's dateTrunc answers a DateTime (a Date for a month or a year), which reaches
Arrow as its count of seconds and holds no day before 1970. Its start is a DateTime64(6) of the
value's zone: :func:`truncated`, the one builder every writer of it calls (this row, for a
statement's call; provisa.transpiler.semantic_dialect, for sqlglot's; zoned_instants, for an
instant's start in the session's zone).

A text cast to a date: ClickHouse reads a day its month does not have as a day of the next month
('2024-02-30' as 2024-03-01). A text the statement writes is read here: an ISO date (YYYY-MM-DD)
is that day, written for toDate32; a day that does not exist is PostgreSQL's error (22008), and
NULL for a cast that answers NULL on failure (pg_input_is_valid's check, accurateCastOrNull by the
time the grid reads it); any other text, and a year Date32 does not hold, is refused by name.

lag, lead, first_value, last_value and nth_value: ClickHouse answers the type's default (0, '')
where PostgreSQL answers NULL, past the partition or of an empty frame, and its first_value and
last_value skip a NULL. lag and lead are lagInFrame and leadInFrame over the whole partition (as
PostgreSQL's ignore the frame), each of the value as Nullable; first_value and last_value respect
NULLs; nth_value reads the value as Nullable. What a rule writes is not walked again, so a value
that holds a call is refused by name: each is a literal, a column or a cast of one.

current_schemas: ClickHouse has its own currentSchemas, which answers its database's name, where
PostgreSQL answers the search path, and NULL of a NULL. No form names PostgreSQL's search path on
ClickHouse: refused by name there, as on Trino."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import calendar
import functools
import re

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.server import plain
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement

FAMILY = "ClickHouse answers"

_ARRAY_AGG = "array_agg($a)"
_KEEPS_NULLS = "if(count() = 0, NULL, arrayMap(_t -> _t.1, groupArray(tuple($a))))"


def _aggregated(node: exp.Expr) -> exp.Expr | None:
    """The value a plain array_agg call aggregates; None for any other node, and for a call of an
    order, of DISTINCT, under a FILTER or over a window, whose form is not this row's."""
    if isinstance(node, exp.ArrayAgg):
        value = node.this
    elif isinstance(node, exp.Anonymous) and node.name.lower() == "array_agg":
        value = node.expressions[0] if len(node.expressions) == 1 else None
    else:
        return None
    if isinstance(value, exp.Order):
        raise Refused(
            Refusal(
                "0A000",
                "array_agg() of an order has no form on clickhouse: its aggregate takes none",
            )
        )
    if value is None or isinstance(value, exp.Distinct):
        return None
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    if isinstance(outer, (exp.Filter, exp.Window)):
        return None
    return value


def kept_nulls(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """array_agg of every value, a NULL kept, and NULL of no rows."""
    value = _aggregated(node)
    if value is None:
        return None
    return graft(replacement(_KEEPS_NULLS, dialect, _ARRAY_AGG), {"a": value})


ARRAY_AGG = Feature(
    "array_agg",
    FAMILY,
    "The values of an aggregate as an array, a NULL kept; NULL of no rows.",
    read=Side(rules=(Rule(_ARRAY_AGG, {"clickhouse": Rewrite(kept_nulls)}),)),
)

#: The functions not STRICT in pg_proc that answer NULL of a NULL array all the same: each tests
#: its array argument for NULL first (PostgreSQL 17's array_userfuncs.c and arrayfuncs.c).
_NULL_OF_A_NULL_ARRAY = frozenset(
    {"array_position", "array_positions", "array_remove", "array_replace", "array_to_string"}
)
#: The functions not STRICT in pg_proc that answer NULL where every argument is a NULL array:
#: array_cat answers the other argument where one is not (array_userfuncs.c, and its grid row).
_NULL_OF_NULL_ARRAYS = frozenset({"array_cat"})
#: The aggregates not STRICT in pg_proc whose answer of a NULL array is NULL: each skips a NULL,
#: and answers NULL of no value.
_OF_NO_VALUE = frozenset({"max", "min"})


@functools.cache
def _arities() -> tuple[frozenset[tuple[str, int]], frozenset[tuple[str, int]]]:
    """Each (name, argument count) whose every overload pg_proc lists as STRICT, and each of which
    an overload returns a set (proretset)."""
    from provisa.pgwire.catalog_pg17_procs import PROCS

    strict: dict[tuple[str, int], bool] = {}
    sets: set[tuple[str, int]] = set()
    for row in PROCS:
        key = (row[1], row[15])
        strict[key] = strict.get(key, True) and bool(row[11])
        if row[12]:
            sets.add(key)
    return frozenset(key for key, every in strict.items() if every), frozenset(sets)


def _is_null_array(value: exp.Expr) -> bool:
    """Whether an argument is a NULL cast to an array type."""
    node = value.unnest()
    return (
        isinstance(node, exp.Cast)
        and node.to.is_type(exp.DataType.Type.ARRAY)
        and isinstance(node.this.unnest(), exp.Null)
    )


def _called(target: exp.Expr, dialect: str) -> tuple[str, list[exp.Expr]] | None:
    """The name and arguments of ``target``, a call: as the translator's table reads it, or as
    the grid's rule it matches binds them (array_length is sqlglot's ArraySize)."""
    from provisa.transpiler.function_macros import call_of
    from provisa.transpiler.polyfill import GRID

    call = call_of(target)
    if call is not None:
        return call
    typed = trino.called(target) if isinstance(target, exp.Func) else None
    if typed is not None:  # a typed node sqlglot reads the name as (ArrayToString)
        names, args = typed
        strict, sets = _arities()
        known = _NULL_OF_A_NULL_ARRAY | _NULL_OF_NULL_ARRAYS | _OF_NO_VALUE
        for name in sorted(names):
            if name in known or (name, len(args)) in strict or (name, len(args)) in sets:
                return name, args
    try:
        applied = GRID.read(target, dialect)
    except Refused:
        return None  # a rewrite's refusal: the translator raises it as the row's
    if applied is None or applied.rule.pattern is None:
        return None
    return applied.feature.name, [applied.bindings[p] for p in applied.rule.holes()]


def null_of_a_null_array(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A call PostgreSQL answers NULL for its NULL array argument, as that NULL, named for the
    call; a set-returning call is left to its own row."""
    call = _called(node, dialect)
    if call is None:
        return None
    name, args = call
    strict, sets = _arities()
    if name == "unnest" and len(args) == 1 and _is_null_array(args[0]):
        # No rows: ClickHouse's arrayJoin of an empty array of the type.
        held = args[0].unnest()
        assert isinstance(held, exp.Cast)  # _is_null_array
        empty = exp.Cast(this=exp.Array(expressions=[]), to=held.to.copy())
        joined = exp.Anonymous(this="arrayJoin", expressions=[empty])
        return exp.alias_(joined, name)
    if (name, len(args)) in sets:
        return None
    if name in _NULL_OF_A_NULL_ARRAY:
        nulled = bool(args) and _is_null_array(args[0])
    elif name in _NULL_OF_NULL_ARRAYS:
        nulled = bool(args) and all(_is_null_array(a) for a in args)
    elif name in _OF_NO_VALUE:
        nulled = len(args) == 1 and _is_null_array(args[0])
    else:
        nulled = (name, len(args)) in strict and any(_is_null_array(a) for a in args)
    return exp.alias_(exp.null(), name) if nulled else None


NULL_ARRAY = Feature(
    "a NULL array",
    FAMILY,
    "A call PostgreSQL answers NULL for its NULL array argument, as that NULL.",
    read=Side(
        rules=(Rule(None, {"clickhouse": Rewrite(null_of_a_null_array, first=True)}),),
        limitations={
            "clickhouse": (
                Limitation(
                    "A set-returning call of a NULL array is its own row's: generate_subscripts "
                    "and pg_options_to_table are refused by name."
                ),
            )
        },
    ),
)

#: The units shorter than a minute, by the microseconds in one: dateTrunc keeps no fraction.
SUBMINUTE = {
    "second": 1_000_000,
    "millisecond": 1_000,
    "milliseconds": 1_000,
    "microsecond": 1,
    "microseconds": 1,
}


def truncated(unit: str, value: exp.Expr, zone: str = "UTC") -> exp.Expr:
    """The start of ``unit`` of ``value`` as DateTime64(6, ``zone``): dateTrunc's, or for a unit
    shorter than a minute the microseconds since the epoch floored to it; a NULL of it for no unit
    (a NULL part)."""
    if not unit:
        to = exp.DataType.build(f"Nullable(DateTime64(6, '{zone}'))", dialect="clickhouse")
        return exp.Cast(this=exp.null(), to=to)
    step = SUBMINUTE.get(unit.lower())
    if step is not None:
        micros = exp.Anonymous(
            this="toUnixTimestamp64Micro",
            expressions=[
                exp.Anonymous(this="toDateTime64", expressions=[value, exp.Literal.number(6)])
            ],
        )
        modulo = exp.Anonymous(
            this="positiveModulo", expressions=[micros.copy(), exp.Literal.number(step)]
        )
        floored = exp.Sub(this=micros, expression=modulo)
        return exp.Anonymous(
            this="fromUnixTimestamp64Micro", expressions=[floored, exp.Literal.string(zone)]
        )
    start = exp.Anonymous(this="dateTrunc", expressions=[exp.Literal.string(unit.lower()), value])
    return exp.Anonymous(
        this="toDateTime64", expressions=[start, exp.Literal.number(6), exp.Literal.string(zone)]
    )


def _unit_said(unit: exp.Expr | None) -> str | None:
    """A unit's text as the statement writes it ('' for NULL); None for one it does not write."""
    node = unit.unnest() if unit is not None else None
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    if isinstance(node, exp.Null):
        return ""
    if isinstance(node, exp.Var) or (isinstance(node, exp.Literal) and node.is_string):
        return node.name
    return None


def started(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """date_trunc of a timestamp: its start as DateTime64(6, 'UTC'), whatever the unit."""
    if not isinstance(node, exp.TimestampTrunc):
        return None
    unit = _unit_said(node.args.get("unit"))
    return None if unit is None else truncated(unit, node.this)


DATE_TRUNC = Feature(
    "date_trunc",
    FAMILY,
    "The start of a unit of a timestamp.",
    read=Side(rules=(Rule("date_trunc($u, $t)", {"clickhouse": Rewrite(started)}),)),
)

_ISO_DATE = re.compile(r"\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*", re.ASCII)
#: The years ClickHouse's Date32 holds.
_DATE32_YEARS = range(1900, 2300)


def _text_of(value: exp.Expr) -> str | None:
    """The text a statement writes, through casts of it to text; None for any other value."""
    node = value.unnest()
    while isinstance(node, exp.Cast) and node.to.is_type(*exp.DataType.TEXT_TYPES):
        node = node.this.unnest()
    return node.this if isinstance(node, exp.Literal) and node.is_string else None


def _date_cast(node: exp.Expr) -> tuple[exp.Expr, bool] | None:
    """The text a node casts to a date, and whether it answers NULL on failure; None for any other
    node."""
    if isinstance(node, (exp.Cast, exp.TryCast)) and node.to.is_type(exp.DataType.Type.DATE):
        return node.this, isinstance(node, exp.TryCast)
    if isinstance(node, exp.Anonymous) and node.name == "accurateCastOrNull":
        value, to = (node.expressions + [None, None])[:2]
        if isinstance(to, exp.Literal) and to.name.upper() == "DATE" and value is not None:
            return value, True
    return None


def dated(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A text the statement writes cast to a date: its day, PostgreSQL's error of one that does
    not exist, or a refusal by name of a text not in ISO form."""
    cast = _date_cast(node)
    text = None if cast is None else _text_of(cast[0])
    if cast is None or text is None:
        return None
    iso = _ISO_DATE.fullmatch(text)
    if iso is None or int(iso[1]) not in _DATE32_YEARS:
        raise Refused(
            Refusal(
                "0A000",
                f'date_in() of "{text}" has no form on {dialect}: only an ISO date '
                "(YYYY-MM-DD) of 1900 to 2299 is read",
            )
        )
    year, month, day = (int(part) for part in iso.groups())
    if not (1 <= month <= 12 and 1 <= day <= calendar.monthrange(year, month)[1]):
        if cast[1]:
            return exp.Cast(this=exp.null(), to=exp.DataType.build("DATE"))
        raise Refused(Refusal("22008", f'date/time field value out of range: "{text}"'))
    day_text = f"{year:04d}-{month:02d}-{day:02d}"
    return exp.Anonymous(this="toDate32", expressions=[exp.Literal.string(day_text)])


DATE = Feature(
    "date",
    FAMILY,
    "A text cast to a date.",
    read=Side(
        rules=(
            Rule(
                None,
                {"clickhouse": Rewrite(dated, mentions=r"::\s*date\b|\bas\s+date\b|\bdate\s+'")},
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A text the statement writes: an ISO date of 1900 to 2299, any other refused "
                    "by name. A text column's cast keeps ClickHouse's own read."
                ),
            )
        },
    ),
)

_IN_FRAME = {"lag": "lagInFrame", "lead": "leadInFrame"}
_RESPECTING = ("first_value", "last_value")
WINDOW_VALUES = (*_IN_FRAME, *_RESPECTING, "nth_value")


def _window_call(window: exp.Window) -> tuple[str, list[exp.Expr]] | None:
    """The value function a window computes and its arguments; None for any other, and for one
    that ignores NULLs."""
    call = window.this
    if isinstance(call, exp.RespectNulls):
        call = call.this
    if isinstance(call, exp.Dot):
        call = call.expression
    named = trino.called(call)
    if named is None:
        return None
    names = named[0] & set(WINDOW_VALUES)
    return (next(iter(names)), named[1]) if len(names) == 1 else None


def _is_null(value: exp.Expr) -> bool:
    """Whether a value is NULL, or a cast of NULL."""
    node = value.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    return isinstance(node, exp.Null)


def _null_of(value: exp.Expr) -> exp.Expr:
    """A NULL of ``value``'s type: nullIf of the value and itself."""
    return exp.Anonymous(this="nullIf", expressions=[_nullable(value), _nullable(value)])


def _nullable(value: exp.Expr) -> exp.Expr:
    return exp.Anonymous(this="toNullable", expressions=[value.copy()])


_WHOLE_PARTITION = exp.WindowSpec(
    kind="ROWS", start="UNBOUNDED", start_side="PRECEDING", end="UNBOUNDED", end_side="FOLLOWING"
)


def framed(function: str) -> Rewrite:
    """The rewrite of ``function`` over a window, NULL where ClickHouse answers its default."""

    def frame(node: exp.Expr, dialect: str) -> exp.Expr | None:
        if not isinstance(node, exp.Window):
            return None
        call = _window_call(node)
        if call is None or call[0] != function:
            return None
        name, args = call
        if len(args) > 1 and _is_null(args[1]):
            # PostgreSQL's offset or row number of NULL answers NULL (WinGetFuncArgCurrent's
            # isnull), which ClickHouse takes as no integer: a NULL of the value's type.
            return _null_of(args[0])
        if not args or not all(plain(arg) for arg in args):
            raise Refused(
                Refusal(
                    "0A000",
                    f"{name}() of a value holding a call has no form on {dialect}: "
                    "only a literal, a column or a cast of one",
                )
            )
        window = node.copy()
        value, rest = _nullable(args[0]), [arg.copy() for arg in args[1:]]
        if name in _IN_FRAME:
            window.set("this", exp.Anonymous(this=_IN_FRAME[name], expressions=[value, *rest]))
            window.set("spec", _WHOLE_PARTITION.copy())
        elif name in _RESPECTING:
            kind = exp.FirstValue if name == "first_value" else exp.LastValue
            window.set("this", exp.RespectNulls(this=kind(this=value)))
        else:
            window.set("this", exp.Anonymous(this="nth_value", expressions=[value, *rest]))
        return window

    frame.__doc__ = f"{function} over a window: NULL where ClickHouse answers its type's default."
    frame.__qualname__ = frame.__name__ = f"framed_{function}"
    return Rewrite(frame, mentions=rf"\b{function}\b")


HOLDS_A_CALL = Limitation(
    "Of a value holding a call, refused by name: what a rule writes is not walked again."
)
WINDOW_FEATURES = tuple(
    Feature(
        name,
        FAMILY,
        f"The value of {'a row of' if name not in _IN_FRAME else 'another row of'} a window.",
        read=Side(
            rules=(Rule(None, {"clickhouse": framed(name)}),),
            limitations={"clickhouse": (HOLDS_A_CALL,)},
        ),
    )
    for name in (*_RESPECTING, "nth_value")
)


def _constant_count(count: exp.Expr) -> int | None:
    """The integer a count of buckets the statement writes is, through casts; None otherwise."""
    node = count.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    negated = isinstance(node, exp.Neg)
    node = node.this.unnest() if negated else node
    if isinstance(node, exp.Literal) and not node.is_string and node.this.isdigit():
        return -int(node.this) if negated else int(node.this)
    return None


def bucketed(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """ntile over a window, as PostgreSQL's integer: every engine's is a bigint. On ClickHouse a
    NULL count of buckets answers NULL, as PostgreSQL's does; a count below 1 is PostgreSQL's
    error; any other constant count is ClickHouse's unsigned one over the whole partition (as
    PostgreSQL's ignores the frame); a count that is no constant is refused by name, as ClickHouse
    takes none."""
    if not (isinstance(node, exp.Window) and isinstance(node.this, exp.Ntile)):
        return None
    count = node.this.this
    if dialect != "clickhouse" or count is None:
        return exp.cast(node, "INT")
    if _is_null(count):
        return exp.cast(exp.null(), "INT")
    buckets = _constant_count(count)
    if buckets is None:
        raise Refused(
            Refusal(
                "0A000",
                "ntile() of a count that is no constant has no form on clickhouse: "
                "its ntile takes a constant",
            )
        )
    if buckets < 1:
        raise Refused(Refusal("22014", "argument of ntile must be greater than zero"))
    window = node.copy()
    window.this.set("this", exp.Literal.number(buckets))
    window.set("spec", _WHOLE_PARTITION.copy())
    if window.args.get("order") is None:  # ClickHouse's ntile reads a frame of an order only
        unordered = exp.Ordered(this=exp.Anonymous(this="tuple", expressions=[]))
        window.set("order", exp.Order(expressions=[unordered]))
    return exp.cast(window, "INT")


NTILE = Feature(
    "ntile",
    FAMILY,
    "The bucket of a row in a window, as an integer.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    dialect: Rewrite(bucketed, mentions=r"\bntile\b")
                    for dialect in DIALECTS
                    if dialect not in NATIVE
                },
            ),
        ),
        limitations={
            "clickhouse": (
                Limitation(
                    "A count of buckets that is no constant is refused by name: ClickHouse's "
                    "ntile takes a constant."
                ),
            )
        },
    ),
)


def _scale_of(value: str) -> str:
    """The scale of ``value``'s Decimal type (0 for any other): ClickHouse holds a numeric's scale
    in its type, and writes its text without trailing zeros."""
    return f"toUInt8OrZero(extract(toTypeName({value}), 'Decimal\\\\(\\\\d+, (\\\\d+)\\\\)'))"


#: numeric_mod: the remainder at 18 places, which keeps it exact, written at the larger scale of
#: its operands (toDecimalString writes a scale of 0 with its point, trimmed); by nullIf of the
#: divisor, as ClickHouse folds a constant remainder by 0 whichever branch is chosen (the old table's form cut the text, which ClickHouse writes without trailing zeros:
#: 1000 % 1000 answered '').
_NUMERIC_MOD = (
    "CASE WHEN $a IS NULL OR $b IS NULL THEN CAST(NULL AS Nullable(String)) "
    "WHEN $b = 0 THEN CAST(throwIf(1, 'postgresql-error 22012: division by zero"
    "<end of postgresql-error>') AS Nullable(String)) "
    "ELSE trimRight(toDecimalString(CAST($a AS Nullable(Decimal(38, 18))) % "
    "CAST(nullIf($b, 0) AS Nullable(Decimal(38, 18))), "
    f"greatest({_scale_of('$a')}, {_scale_of('$b')})), '.') END"
)

NUMERIC_MOD = Feature(
    "numeric_mod",
    FAMILY,
    "The remainder of one numeric divided by another, at the larger scale of the two.",
    read=Side(
        rules=(Rule("numeric_mod($a, $b)", {"clickhouse": _NUMERIC_MOD}),),
        limitations={
            "clickhouse": (
                Limitation(
                    "The scale is each operand's type's: a column holds one scale for all its "
                    "values, where PostgreSQL's numeric holds each value's own."
                ),
            )
        },
    ),
)

_SEARCH_PATH = (
    "Refused by name: ClickHouse's own currentSchemas answers its database, not PostgreSQL's "
    "search path."
)
#: Refused wherever a statement names it: unqualified, ClickHouse would answer its own.
_NO_SEARCH_PATH = Rewrite(
    trino.refusing(
        "current_schemas",
        "current_schemas: refused by name, as no form names the search path.",
        lambda *_: True,
    ).function,
    mentions=r"\bcurrent_schemas\b",
)
CURRENT_SCHEMAS = Feature(
    "current_schemas",
    FAMILY,
    "The schemas of the search path.",
    read=Side(
        rules=(Rule(None, dict.fromkeys((*trino.TRINO, "clickhouse"), _NO_SEARCH_PATH)),),
        limitations={
            **dict.fromkeys(
                trino.TRINO, (Limitation("Refused by name: Trino has no form of it."),)
            ),
            "clickhouse": (Limitation(_SEARCH_PATH),),
        },
    ),
)

FEATURES: tuple[Feature, ...] = (
    NULL_ARRAY,
    ARRAY_AGG,
    DATE_TRUNC,
    DATE,
    *WINDOW_FEATURES,
    NTILE,
    NUMERIC_MOD,
    CURRENT_SCHEMAS,
)
