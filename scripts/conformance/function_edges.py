# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b51c7-6d2a-4f94-a0c3-7b19e6d4f582
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The edge values the functions measure calls every overload with, beside its three samples
(REQ-2236, amended 2026-11-15, AN EDGE CASE RAISES; IT DOES NOT REFUSE THE FORM), and what the
polyfill grid says of an overload's bound.

Three samples say a form answers ordinary values. They do not say it answers the values at a
type's edge: an extreme, an empty value, multi-byte or combining text, or a value PostgreSQL itself
raises on. ``EDGES`` is the one table of those values, by argument type. function_support.py
writes the calls (``function_support.edge_calls``): each takes the overload's first sample row and
puts one edge value in one argument, NULL first where the overload has more than one argument (a
one-argument overload's NULL is its NULL call). At most ``EDGE_CALLS_MAX`` such calls per overload,
taken round-robin over the arguments in the table's order, so a two-argument overload has edges in
both. An aggregate or a window function reads the edge value in two of its rows.

An edge call reads at most ``EDGE_ROWS_MAX`` + 1 rows (``bounded``); one PostgreSQL answers with
more compares nothing. An edge call is compared as a sample is, and where PostgreSQL raises, the engine must raise an
error of the same SQLSTATE class: answering is a wrong answer. An engine that raises where
PostgreSQL answers has not answered wrongly; such an overload is served within its bound when its
samples are right, no edge call answers otherwise than PostgreSQL, and the polyfill grid's row of it
on that engine states a limitation (``grid_bounds``). The grid marks a row bounded where a
limitation says what it raises (``stated_raises``); the measure holds such an overload to raising
that SQLSTATE on at least one edge call.
"""

# Requirements: REQ-2236

from __future__ import annotations

import functools
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from provisa.transpiler.polyfill.grid import Limitation

#: The most edge calls one overload gets: with its NULL call, three samples and up to three column
#: calls, at most 15 statements per overload on each server.
EDGE_CALLS_MAX = 8

#: The most rows an edge call's answer is compared in: each call reads at most one more, and one
#: with more (generate_series between an integer's least and greatest) compares nothing.
EDGE_ROWS_MAX = 1000


def bounded(statement: str) -> str:
    """``statement`` reading at most ``EDGE_ROWS_MAX`` + 1 rows, so an edge that makes a set of
    billions of rows (or none that ends) costs no more than that on either server. A LIMIT of its
    own, not a derived table, which would describe its column by another route than the call's."""
    return f"{statement} LIMIT {EDGE_ROWS_MAX + 1}"


NULL = "null"
EMPTY = "empty"
LEAST = "least"
GREATEST = "greatest"
ZERO = "zero"
NEGATIVE = "negative"
MULTIBYTE = "multi-byte"
COMBINING = "combining"
SPECIAL = "special"
PAST = "past the type"  # PostgreSQL raises reading it


@dataclass(frozen=True)
class Edge:
    """One edge value of a type: what kind of edge it is, and its literal, cast where it is used.
    An edge of the kind ``PAST`` is one PostgreSQL raises on reading it."""

    kind: str
    literal: str


def _integer(least: str, greatest: str, past: str) -> tuple[Edge, ...]:
    return (
        Edge(LEAST, f"'{least}'"),
        Edge(GREATEST, f"'{greatest}'"),
        Edge(ZERO, "'0'"),
        Edge(PAST, f"'{past}'"),
    )


_TEXT = (
    Edge(EMPTY, "''"),
    Edge(MULTIBYTE, "'日本語テキスト😀'"),
    Edge(COMBINING, "'été'"),
    Edge(SPECIAL, "'  padded  '"),
)

_FLOAT_SPECIALS = (Edge(SPECIAL, "'NaN'"), Edge(SPECIAL, "'-Infinity'"), Edge(ZERO, "'-0'"))

_TIMESTAMP = (
    Edge(LEAST, "'4713-01-01 00:00:00 BC'"),
    Edge(GREATEST, "'294276-12-31 23:59:59.999999'"),
    Edge(SPECIAL, "'infinity'"),
    Edge(SPECIAL, "'-infinity'"),
    Edge(PAST, "'294277-01-01 00:00:00'"),
)

_TIMESTAMPTZ = (
    Edge(LEAST, "'4713-01-01 00:00:00+00 BC'"),
    Edge(GREATEST, "'294276-12-31 23:59:59.999999+00'"),
    Edge(SPECIAL, "'infinity'"),
    Edge(SPECIAL, "'-infinity'"),
    Edge(PAST, "'294277-01-01 00:00:00+00'"),
)

_UNBOUNDED_RANGE = Edge(SPECIAL, "'(,)'")

#: The edge values of each argument type, by the concrete type name function_support's samples
#: use, most telling first: the first ``EDGE_CALLS_MAX`` of an overload are what it is called with.
#: NULL is every type's, and is not listed. A type not here has NULL alone.
EDGES: dict[str, tuple[Edge, ...]] = {
    "smallint": _integer("-32768", "32767", "32768"),
    "integer": _integer("-2147483648", "2147483647", "2147483648"),
    "bigint": _integer("-9223372036854775808", "9223372036854775807", "9223372036854775808"),
    "oid": (Edge(GREATEST, "'4294967295'"), Edge(PAST, "'4294967296'")),
    "numeric": (
        Edge(ZERO, "'0'"),
        Edge(NEGATIVE, "'-0.000000000000000000001'"),
        Edge(GREATEST, "'123456789012345678901234567890.123456789012345678901234567890'"),
        Edge(SPECIAL, "'NaN'"),
    ),
    "real": (*_FLOAT_SPECIALS, Edge(GREATEST, "'3.4028235e+38'")),
    "double precision": (*_FLOAT_SPECIALS, Edge(GREATEST, "'1.7976931348623157e+308'")),
    "money": (
        Edge(LEAST, "'-92233720368547758.08'"),
        Edge(GREATEST, "'92233720368547758.07'"),
        Edge(ZERO, "'0'"),
    ),
    "text": _TEXT,
    "character varying": _TEXT,
    "character": _TEXT,
    "name": _TEXT,
    '"char"': (Edge(EMPTY, "''"),),
    "bytea": (Edge(EMPTY, "'\\x'"), Edge(SPECIAL, "'\\x00ff00'")),
    "date": (
        Edge(LEAST, "'4713-01-01 BC'"),
        Edge(GREATEST, "'5874897-12-31'"),
        Edge(SPECIAL, "'infinity'"),
        Edge(SPECIAL, "'-infinity'"),
        Edge(PAST, "'5874898-01-01'"),
    ),
    "time without time zone": (
        Edge(LEAST, "'00:00:00'"),
        Edge(GREATEST, "'24:00:00'"),
        Edge(SPECIAL, "'23:59:59.999999'"),
    ),
    "time with time zone": (
        Edge(LEAST, "'00:00:00+15:59'"),
        Edge(GREATEST, "'24:00:00-15:59'"),
    ),
    "timestamp without time zone": _TIMESTAMP,
    "timestamp with time zone": _TIMESTAMPTZ,
    "interval": (
        Edge(ZERO, "'00:00:00'"),
        Edge(NEGATIVE, "'-00:00:00.000001'"),
        Edge(GREATEST, "'178956970 years'"),
        Edge(LEAST, "'-178956970 years'"),
        Edge(PAST, "'178956971 years'"),
    ),
    "json": (
        Edge(EMPTY, "'{}'"),
        Edge(EMPTY, "'[]'"),
        Edge(MULTIBYTE, "'\"日本語😀\"'"),
        Edge(GREATEST, "'12345678901234567890123456789'"),
    ),
    "jsonb": (
        Edge(EMPTY, "'{}'"),
        Edge(EMPTY, "'[]'"),
        Edge(MULTIBYTE, "'\"日本語😀\"'"),
        Edge(GREATEST, "'12345678901234567890123456789'"),
    ),
    "inet": (
        Edge(LEAST, "'0.0.0.0/0'"),
        Edge(GREATEST, "'255.255.255.255'"),
        Edge(LEAST, "'::/0'"),
        Edge(GREATEST, "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'"),
    ),
    "cidr": (Edge(LEAST, "'0.0.0.0/0'"), Edge(LEAST, "'::/0'")),
    "bit varying": (Edge(EMPTY, "''"),),
    "tsvector": (Edge(EMPTY, "''"),),
    "xml": (Edge(EMPTY, "''"), Edge(MULTIBYTE, "'<a>日本語😀</a>'")),
    "pg_lsn": (Edge(LEAST, "'0/0'"), Edge(GREATEST, "'FFFFFFFF/FFFFFFFF'")),
    "integer[]": (
        Edge(GREATEST, "'{2147483647,-2147483648}'"),
        Edge(SPECIAL, "'{NULL}'"),
        Edge(SPECIAL, "'{{1,2},{3,4}}'"),
    ),
    "smallint[]": (Edge(GREATEST, "'{32767,-32768}'"), Edge(SPECIAL, "'{NULL}'")),
    "bigint[]": (
        Edge(GREATEST, "'{9223372036854775807,-9223372036854775808}'"),
        Edge(SPECIAL, "'{NULL}'"),
    ),
    "real[]": (Edge(SPECIAL, "'{NaN,-Infinity}'"),),
    "double precision[]": (Edge(SPECIAL, "'{NaN,-Infinity}'"),),
    "numeric[]": (Edge(SPECIAL, "'{NaN,0}'"),),
    "text[]": (
        Edge(MULTIBYTE, "'{\"\",日本語😀,é}'"),
        Edge(SPECIAL, "'{NULL}'"),
        Edge(SPECIAL, "'{{a,b},{c,d}}'"),
    ),
    "int4range": (Edge(GREATEST, "'[-2147483648,2147483647)'"), _UNBOUNDED_RANGE),
    "int8range": (
        Edge(GREATEST, "'[-9223372036854775808,9223372036854775807)'"),
        _UNBOUNDED_RANGE,
    ),
    "numrange": (_UNBOUNDED_RANGE,),
    "daterange": (Edge(SPECIAL, "'[-infinity,infinity]'"), _UNBOUNDED_RANGE),
    "tsrange": (_UNBOUNDED_RANGE,),
    "tstzrange": (_UNBOUNDED_RANGE,),
    # A shape holding NaN or an infinity: what the ClickHouse geometric rows say they raise on.
    "point": (Edge(SPECIAL, "'(NaN,1)'"), Edge(SPECIAL, "'(Infinity,1)'")),
    "line": (Edge(SPECIAL, "'{NaN,1,0}'"), Edge(SPECIAL, "'{1,-1,Infinity}'")),
    "lseg": (Edge(SPECIAL, "'[(0,0),(NaN,1)]'"), Edge(SPECIAL, "'[(0,0),(Infinity,1)]'")),
    "box": (Edge(SPECIAL, "'(NaN,1),(0,0)'"), Edge(SPECIAL, "'(Infinity,1),(0,0)'")),
    "path": (
        Edge(SPECIAL, "'((0,0),(NaN,1),(1,1))'"),
        Edge(SPECIAL, "'((0,0),(Infinity,1),(1,1))'"),
    ),
    "polygon": (
        Edge(SPECIAL, "'((0,0),(NaN,1),(1,1))'"),
        Edge(SPECIAL, "'((0,0),(Infinity,1),(1,1))'"),
    ),
    "circle": (Edge(SPECIAL, "'<(NaN,0),1>'"), Edge(SPECIAL, "'<(0,0),Infinity>'")),
    "int4multirange": (Edge(SPECIAL, "'{(,)}'"),),
    "int8multirange": (Edge(SPECIAL, "'{(,)}'"),),
    "nummultirange": (Edge(SPECIAL, "'{(,)}'"),),
}

#: The NULL edge, every type's: one argument NULL, the others the first sample row's.
NULL_EDGE = Edge(NULL, "NULL")


def edges_of(concrete: str, *, with_null: bool) -> tuple[Edge, ...]:
    """The edge values of the type ``concrete``, NULL first when ``with_null``."""
    return ((NULL_EDGE,) if with_null else ()) + EDGES.get(concrete, ())


#: The edge values of an argument given literals of its own (function_support.ARG_VALUES), by
#: function and position: a regular expression's pattern, whose empty pattern matches the empty
#: text everywhere (the ClickHouse regexp rows say it raises where it counts, places or splits).
#: These are called first, so the round robin reaches them whatever the arity.
_EMPTY_PATTERN = (Edge(EMPTY, "''"),)
GIVEN_EDGES: dict[tuple[str, int], tuple[Edge, ...]] = {
    (name, 1): _EMPTY_PATTERN
    for name in (
        "regexp_count",
        "regexp_instr",
        "regexp_like",
        "regexp_match",
        "regexp_matches",
        "regexp_replace",
        "regexp_split_to_array",
        "regexp_split_to_table",
        "regexp_substr",
    )
}


def given_edges(function: str, position: int) -> tuple[Edge, ...]:
    """The edge values of ``function``'s argument ``position`` where it is given literals of its
    own: ``GIVEN_EDGES``, none for any other."""
    return GIVEN_EDGES.get((function, position), ())


def chosen(per_position: list[tuple[int, tuple[Edge, ...]]]) -> list[tuple[int, Edge]]:
    """At most ``EDGE_CALLS_MAX`` (position, edge) pairs, round-robin over the positions: each
    position's first edge, then each one's second, and so on."""
    out: list[tuple[int, Edge]] = []
    depth = max((len(edges) for _, edges in per_position), default=0)
    for level in range(depth):
        for position, edges in per_position:
            if level < len(edges):
                out.append((position, edges[level]))
                if len(out) == EDGE_CALLS_MAX:
                    return out
    return out


def sqlstate_class(sqlstate: str) -> str:
    """An SQLSTATE's class: its first two characters."""
    return sqlstate[:2]


# --- what the polyfill grid says of an overload's bound ----------------------------------------

#: An SQLSTATE a limitation says its form raises, and the message it gives in parentheses after
#: it where it gives one: "raises 0A000 (f() has no exact form on clickhouse)", "raises
#: PostgreSQL's 22003". A message may hold one level of parentheses of its own ("f()").
_RAISES = re.compile(
    r"\braises?\s+(?:PostgreSQL's\s+)?([0-9A-Z]{5})\b(?:\s*\(((?:[^()]|\([^()]*\))*)\))?"
)

#: Of a call's statement on an engine: its grid row's limitations there, none when the grid has no
#: row of it or the row states none.
Bounds = Callable[[str, str], tuple[Limitation, ...]]


@dataclass(frozen=True)
class StatedError:
    """An error a limitation states: its SQLSTATE, and its message where the limitation gives it
    (None: the limitation names the SQLSTATE alone, and any message of it is the one stated)."""

    sqlstate: str
    message: str | None = None

    def matches(self, sqlstate: str, message: str) -> bool:
        return sqlstate == self.sqlstate and self.message in (None, message)


def raised_error(raised: Mapping[str, Any]) -> tuple[str, str]:
    """The SQLSTATE and the primary message of an edge call's raise (``edge_raised``)."""
    message = str(raised.get("error", "")).split("\n", 1)[0].removeprefix("ERROR:").strip()
    return str(raised.get("sqlstate", "")), message


def bound_errors(limitation: Limitation) -> tuple[StatedError, ...]:
    """The errors ``limitation``'s text says its form raises outside its bound: what makes a row
    bounded, and what its overloads are held to raising at an edge."""
    return tuple(
        StatedError(state, message or None) for state, message in _RAISES.findall(limitation.text)
    )


def stated_errors(limitation: Limitation) -> tuple[StatedError, ...]:
    """Every error ``limitation`` states: its refusal, and the raises its text names. An edge raise
    is within the bound of a limitation only where it is one of these (REQ-2236, amended
    2026-11-22: a limit excuses only the errors matching its own values and SQLSTATE)."""
    refusal = limitation.refusal
    own = (StatedError(refusal.sqlstate, refusal.message),) if refusal is not None else ()
    return own + bound_errors(limitation)


def excuses(limitation: Limitation, raised: Mapping[str, Any]) -> bool:
    """Whether the edge raise ``raised`` is an error ``limitation`` states."""
    sqlstate, message = raised_error(raised)
    return any(e.matches(sqlstate, message) for e in stated_errors(limitation))


def bound_excuses(limitation: Limitation, raised: Mapping[str, Any]) -> bool:
    """Whether the edge raise ``raised`` is an error ``limitation``'s text says its form raises
    outside its bound: what holds a bounded limitation (``bound_errors``)."""
    sqlstate, message = raised_error(raised)
    return any(e.matches(sqlstate, message) for e in bound_errors(limitation))


def stated_by_dialect(grid: Any, dialect: str) -> dict[tuple[str, str], str]:
    """The errors with a message any limitation of ``grid`` states on ``dialect``, by SQLSTATE and
    message, each to the feature whose limitation states it: a value's own reading (a literal past
    the engine's type) raises the error its own row states, whatever call it is an argument of."""
    out: dict[tuple[str, str], str] = {}
    for feature in grid.features:
        for limitation in feature.read.limitations.get(dialect, ()):
            for error in stated_errors(limitation):
                if error.message is not None:
                    out.setdefault((error.sqlstate, error.message), feature.name)
    return out


def stated_elsewhere(engine: str, raised: Mapping[str, Any]) -> bool:
    """Whether the polyfill grid states the edge raise ``raised``, its SQLSTATE and its message, in
    a limitation of any row on ``engine``'s dialect (``stated_by_dialect``)."""
    import polyfill_check

    dialect = polyfill_check.ENGINE_DIALECTS[engine]
    return raised_error(raised) in _grid_stated(dialect)


@functools.cache
def _grid_stated(dialect: str) -> dict[tuple[str, str], str]:
    import polyfill_check

    return stated_by_dialect(polyfill_check.GRID, dialect)


@dataclass(frozen=True)
class OfRecord:
    """A limit no edge call can reach, standing as a statement of record: the check does not hold
    its feature's overloads to raising it (REQ-2236, amended 2026-11-22). Named by the feature,
    the dialect and the error its limitation states, never by its wording."""

    feature: str
    dialect: str
    error: StatedError
    why: str


_PAST_2_40 = "a group of more than 2^40 values: no edge call reads that many rows"
#: The limits no edge call reaches, by feature and dialect.
STATEMENTS_OF_RECORD: tuple[OfRecord, ...] = (
    *(
        OfRecord(
            name,
            "clickhouse",
            StatedError(
                "0A000", f"{name} of more than {1 << 40} values has no exact form on clickhouse"
            ),
            _PAST_2_40,
        )
        for name in (
            "avg",
            "variance",
            "var_samp",
            "var_pop",
            "stddev",
            "stddev_samp",
            "stddev_pop",
        )
    ),
    OfRecord(
        "bpchar",
        "clickhouse",
        StatedError("0A000"),
        "a length past 10485760 characters: no edge call pads a text that far",
    ),
    OfRecord(
        "string_to_array",
        "clickhouse",
        StatedError("0A000"),
        "a NULL text that is no literal: every edge call writes its NULL as a literal",
    ),
)


def of_record(feature: str, dialect: str, limitation: Limitation) -> bool:
    """Whether ``limitation`` of ``feature`` on ``dialect`` is a statement of record."""
    errors = bound_errors(limitation)
    return any(
        r.feature == feature and r.dialect == dialect and r.error in errors
        for r in STATEMENTS_OF_RECORD
    )


def grid_bounds(statement: str, engine: str) -> tuple[Limitation, ...]:
    """The limitations the polyfill grid's row of the call ``statement`` makes states on
    ``engine``'s dialect, read as the translator reads it: the function unqualified, the statement
    parsed as PostgreSQL's, its first column's expression looked up. None when the grid has no row
    of it there; a rewrite function refusing the call for its values states that refusal. An
    engine polyfill_check.ENGINE_DIALECTS names no dialect of is a mistake in the run; one whose
    dialect is no column of the grid has no row there."""
    import polyfill_check
    import sqlglot
    from sqlglot.errors import ParseError

    from provisa.transpiler import transpile

    from provisa.transpiler.polyfill.notation import Refused

    dialect = polyfill_check.ENGINE_DIALECTS[engine]
    if polyfill_check.column_of(dialect) is None:
        # A dialect that is no column of the grid: it has no row there.
        return ()
    # pg_catalog."f"(...) as the translator reads it: the function unqualified, read again.
    try:
        unqualified = transpile._catalog_functions_unqualified(statement)
        node = sqlglot.parse_one(unqualified, read="postgres").expressions[0].unalias()
    except ParseError:
        # The translator reads no grid row of a call sqlglot cannot read unqualified
        # (json_object(text[]), normalize(text, text), date_add of three arguments): the grid
        # states nothing of it.
        return ()
    try:
        applied = polyfill_check.GRID.read(node, dialect)
    except Refused as refused:
        # A rewrite function refusing the call for its values: its row lists that refusal as a
        # limitation (notation.Refused), which is what the row states of the call.
        text = f"refused: {refused.refusal.sqlstate} {refused.refusal.message}"
        return (Limitation(text, refused.refusal),)
    if applied is None:
        return ()
    return tuple(applied.feature.read.limitations.get(dialect, ()))


def edge_value(label: str, statement: str) -> str:
    """The value an edge call of ``label`` (``<type> <kind>``) passed, as the statement wrote it
    and as a grid limitation states it (``<type> <literal>``); the label itself where no edge of
    its type and kind is in the statement."""
    for kind in sorted({e.kind for edges in EDGES.values() for e in edges}, key=len, reverse=True):
        if label.endswith(f" {kind}"):
            argtype = label[: -len(kind) - 1]
            for edge in EDGES.get(argtype, ()):
                if edge.kind == kind and edge.literal in statement:
                    return f"{argtype} {edge.literal}"
    return label


def stated_raises(limitations: tuple[Limitation, ...]) -> list[str]:
    """The SQLSTATEs the limitations say the form raises: what makes a row bounded."""
    return sorted({e.sqlstate for lim in limitations for e in bound_errors(lim)})
