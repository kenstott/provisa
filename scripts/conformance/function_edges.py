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

An edge call is compared as a sample is, and where PostgreSQL raises, the engine must raise an
error of the same SQLSTATE class: answering is a wrong answer. An engine that raises where
PostgreSQL answers has not answered wrongly; such an overload is served within its bound when its
samples are right, no edge call answers otherwise than PostgreSQL, and the polyfill grid's row of it
on that engine states a limitation (``grid_bounds``). The grid marks a row bounded where a
limitation says what it raises (``stated_raises``); the measure holds such an overload to raising
that SQLSTATE on at least one edge call.
"""

# Requirements: REQ-2236

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass

#: The most edge calls one overload gets: with its NULL call, three samples and up to three column
#: calls, at most 15 statements per overload on each server.
EDGE_CALLS_MAX = 8

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
    "int4multirange": (Edge(SPECIAL, "'{(,)}'"),),
    "int8multirange": (Edge(SPECIAL, "'{(,)}'"),),
    "nummultirange": (Edge(SPECIAL, "'{(,)}'"),),
}

#: The NULL edge, every type's: one argument NULL, the others the first sample row's.
NULL_EDGE = Edge(NULL, "NULL")


def edges_of(concrete: str, *, with_null: bool) -> tuple[Edge, ...]:
    """The edge values of the type ``concrete``, NULL first when ``with_null``."""
    return ((NULL_EDGE,) if with_null else ()) + EDGES.get(concrete, ())


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

#: An SQLSTATE a limitation says its form raises: "raises 0A000", "raises PostgreSQL's 22003".
_RAISES = re.compile(r"\braises?\s+(?:PostgreSQL's\s+)?([0-9A-Z]{5})\b")

#: Of a call's statement on an engine: the texts of its grid row's limitations there, none when the
#: grid has no row of it or the row states none.
Bounds = Callable[[str, str], tuple[str, ...]]

#: A function named in pg_catalog, as the measure writes its calls.
_QUALIFIED = re.compile(r'pg_catalog\."([^"]+)"')


def grid_bounds(statement: str, engine: str) -> tuple[str, ...]:
    """The limitations the polyfill grid's row of the call ``statement`` makes states on
    ``engine``'s dialect, read as the translator reads it: the function unqualified, the statement
    parsed as PostgreSQL's, its first column's expression looked up. None when the grid has no row
    of it there. An engine the grid has no dialect of is a mistake in the run."""
    import polyfill_check
    import sqlglot

    dialect = polyfill_check.ENGINE_DIALECTS[engine]
    parsed = sqlglot.parse_one(_QUALIFIED.sub(r'"\1"', statement), read="postgres")
    node = parsed.expressions[0]
    applied = polyfill_check.GRID.read(node.unalias(), dialect)
    if applied is None:
        return ()
    return tuple(lim.text for lim in applied.feature.read.limitations.get(dialect, ()))


def stated_raises(limitations: tuple[str, ...]) -> list[str]:
    """The SQLSTATEs the limitations say the form raises: what makes a row bounded."""
    return sorted({m for text in limitations for m in _RAISES.findall(text)})
