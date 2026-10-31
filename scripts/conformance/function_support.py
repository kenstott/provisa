# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2a94d1-0c6b-4f38-b5d2-91c3e4a7f608
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which pg_catalog functions work today, per federation engine (REQ-2167, THE GOAL IS THE
GUARANTEED LIST; REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED).

Run by .github/workflows/conformance-functions.yml, one leg per engine. Measurement only: it
writes no function form. ``measure`` calls every overload pg_proc lists in pg_catalog on a real
PostgreSQL and through Provisa over pgwire; ``merge`` joins the legs into function_support.json
and the guaranteed list.

Each overload gets one outcome per engine:

  pass          every call PostgreSQL answered, Provisa answered the same
  wrong_answer  a call compares unequal: its result type, its rows or their values
  error         PostgreSQL answered a call and Provisa did not (the engine's message is kept)
  not_callable  PostgreSQL itself answers no call of it
  not_reached   the leg ended, or its server went away, before this overload

A wrong answer outranks an error, which outranks a pass: the overload is as bad as its worst call.

The calls of an overload are the NULL call of pgwire_function_probe.py and, where ``value_calls``
can write them, up to three calls with real values. How far values go, exactly:

  * Only a function whose answer is a function of its arguments is given values: an immutable
    one, or a stable one of ``STABLE_FAMILIES``, compared under one TimeZone, DateStyle and
    IntervalStyle. Volatile functions are never given values, because that would run them for
    real (they are the ones that act: setval, pg_terminate_backend, lo_unlink) and their answers
    differ by nature (random, now). Stable catalog and session functions read server state that
    has no PostgreSQL counterpart, so they are not compared either.
  * Every argument must have a literal: ``SAMPLES`` by type, ``ARG_VALUES`` by function where the
    type's literals are not valid input. Pseudo-types take the concrete types of
    pgwire_function_probe.py. An overload taking a type with no sample gets no value call.
  * A value call PostgreSQL refuses compares nothing: that it also refuses on Provisa is not
    checked, nor is Provisa answering what PostgreSQL refuses.

``answers_checked`` is an overload with at least one value call PostgreSQL answered, its value
compared; the rest are ``only called`` and each says why (``unchecked``). A NULL call of a strict
function proves the call was served and its type, not its answer. Three literals, not a domain: a
function that differs only on values no sample reaches passes here.

The result is text format under one set of session settings. Rows compare as a multiset (no call
has an ORDER BY of its own to hold them to); values exactly, except float4 and float8, and their
arrays, within one part in 10^14 (REQ-2171). The NULL call of a stable or volatile function
compares its type alone.
"""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import argparse
import json
import math
import select
import sys
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import pq

import pgwire_catalog_types as catalog
import pgwire_function_probe as function_probe
from pgwire_function_probe import Function

#: The session settings both servers compare under.
SETTINGS = (
    "SET TimeZone TO 'UTC'",
    "SET DateStyle TO 'ISO, MDY'",
    "SET IntervalStyle TO 'postgres'",
    "SET extra_float_digits TO 1",
)

#: How long one call may take on one server before it is an error.
CALL_TIMEOUT_S = 30.0

#: How long a leg may run before what remains is not reached: the job's own limit would lose it all.
LEG_BUDGET_S = 6300.0

#: Calls of an overload given values: one aggregate or window statement reads all of them as rows.
VALUE_ROWS = 3

#: Floats compare within one part in this (REQ-2171).
FLOAT_TOLERANCE = 1e-14

#: The families whose stable functions depend on the session settings alone. Stable functions
#: elsewhere (System above all) read the catalog or the session.
STABLE_FAMILIES = frozenset(
    {
        "Aggregate and window",
        "Array",
        "Date",
        "Geometric",
        "JSON",
        "Network",
        "Numeric",
        "Range",
        "String",
        "Text search",
        "XML",
    }
)

#: Stable by pg_proc, but answers from the clock.
CLOCK_FUNCTIONS = frozenset({"age", "now", "transaction_timestamp", "statement_timestamp"})

_FLOAT_OIDS = frozenset({700, 701})
_FLOAT_ARRAY_OIDS = frozenset({1021, 1022})

_INTEGERS = ("3", "(-7)", "42")
_DECIMALS = ("0.5", "(-2.25)", "100")
_STRINGS = ("'abc'", "'Hello World'", "'12'")

#: Three literals of each type format_type names, cast to it where they are used. Enough to tell
#: a sign, a case and a digit string apart; the first of the floats is a valid fraction, which an
#: ordered-set aggregate's direct argument needs.
SAMPLES: dict[str, tuple[str, str, str]] = {
    "smallint": _INTEGERS,
    "integer": _INTEGERS,
    "bigint": ("3", "(-7)", "5000000000"),
    "numeric": ("2.5", "(-7.125)", "1000"),
    "real": _DECIMALS,
    "double precision": _DECIMALS,
    "boolean": ("true", "false", "true"),
    "text": _STRINGS,
    "character varying": _STRINGS,
    "character": _STRINGS,
    "name": _STRINGS,
    '"char"': ("'a'", "'B'", "'1'"),
    "bytea": ("'\\xdeadbeef'", "'abc'", "'\\x00ff'"),
    "date": ("'2024-06-15'", "'1999-12-31'", "'2000-02-29'"),
    "time without time zone": ("'12:30:45'", "'00:00:00'", "'23:59:59.5'"),
    "time with time zone": ("'12:30:45+02'", "'00:00:00+00'", "'23:59:59.5-05'"),
    "timestamp without time zone": (
        "'2024-06-15 12:30:45'",
        "'1999-12-31 23:59:59'",
        "'2000-02-29 00:00:00'",
    ),
    "timestamp with time zone": (
        "'2024-06-15 12:30:45+00'",
        "'1999-12-31 23:59:59+02'",
        "'2000-02-29 00:00:00-05'",
    ),
    "interval": ("'1 day 02:03:04'", "'3 mons'", "'-1 year 2 days'"),
    "uuid": (
        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
        "'00000000-0000-0000-0000-000000000000'",
        "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
    ),
    "json": ('\'{"a": 1, "b": [1, 2]}\'', "'[1, \"two\", null]'", "'3'"),
    "jsonb": ('\'{"a": 1, "b": [1, 2]}\'', "'[1, \"two\", null]'", "'3'"),
    "jsonpath": ("'$.a'", "'$[0]'", "'$'"),
    "inet": ("'192.168.0.1/24'", "'10.1.2.3'", "'::1'"),
    "cidr": ("'192.168.0.0/24'", "'10.0.0.0/8'", "'::1/128'"),
    "macaddr": ("'08:00:2b:01:02:03'", "'00:00:00:00:00:00'", "'ff:ff:ff:ff:ff:ff'"),
    "macaddr8": (
        "'08:00:2b:01:02:03:04:05'",
        "'00:00:00:00:00:00:00:00'",
        "'ff:ff:ff:ff:ff:ff:ff:ff'",
    ),
    "money": ("'12.34'", "'-5.00'", "'1000.00'"),
    "point": ("'(1,2)'", "'(0,0)'", "'(-3.5,4)'"),
    "line": ("'{1,2,3}'", "'{1,0,0}'", "'{0,1,-2}'"),
    "lseg": ("'[(0,0),(3,4)]'", "'[(1,1),(1,5)]'", "'[(-1,0),(2,0)]'"),
    "box": ("'(3,4),(0,0)'", "'(1,1),(-1,-1)'", "'(5,2),(2,1)'"),
    "path": ("'((0,0),(1,0),(1,1))'", "'[(0,0),(2,2)]'", "'((1,1),(4,1),(4,5))'"),
    "polygon": ("'((0,0),(1,0),(1,1))'", "'((0,0),(4,0),(4,4),(0,4))'", "'((1,1),(2,3),(3,1))'"),
    "circle": ("'<(0,0),1>'", "'<(1,2),3>'", "'<(-1,-1),0.5>'"),
    "bit": ("'1'", "'0'", "'1'"),
    "bit varying": ("'101'", "'1'", "'0011'"),
    "tsvector": ("'cat:1 fat:2'", "'a b c'", "'rat:3A'"),
    "tsquery": ("'cat'", "'a & b'", "'fat | rat'"),
    "xml": ("'<a>1</a>'", "'<b x=\"y\"/>'", "'<c><d>2</d></c>'"),
    "pg_lsn": ("'0/16B3748'", "'1/0'", "'FF/FFFFFFFF'"),
    "oid": ("12345", "0", "1"),
    "regconfig": ("'english'", "'simple'", "'english'"),
    "interval[]": ("'{1 day,2 hours}'", "'{}'", "'{1 mon}'"),
    "integer[]": ("'{1,2,3}'", "'{}'", "'{-7,NULL,42}'"),
    "smallint[]": ("'{1,2,3}'", "'{}'", "'{-7,NULL,42}'"),
    "bigint[]": ("'{1,2,3}'", "'{}'", "'{-7,NULL,42}'"),
    "real[]": ("'{0.25,0.5}'", "'{}'", "'{-1.5,100}'"),
    "double precision[]": ("'{0.25,0.5}'", "'{}'", "'{-1.5,100}'"),
    "numeric[]": ("'{0.25,0.5}'", "'{}'", "'{-1.5,100}'"),
    "text[]": ("'{a,b,c}'", "'{}'", "'{x,NULL,z}'"),
    "int4range": ("'[1,5)'", "'empty'", "'(,3]'"),
    "int8range": ("'[1,5)'", "'empty'", "'(,3]'"),
    "numrange": ("'[1.5,2.5)'", "'empty'", "'(,3]'"),
    "daterange": ("'[2024-01-01,2024-02-01)'", "'empty'", "'(,2000-01-01]'"),
    "tsrange": ("'[2024-01-01 00:00,2024-02-01 00:00)'", "'empty'", "'(,2000-01-01 00:00]'"),
    "tstzrange": (
        "'[2024-01-01 00:00+00,2024-02-01 00:00+00)'",
        "'empty'",
        "'(,2000-01-01 00:00+00]'",
    ),
    "int4multirange": ("'{[1,5),[7,9)}'", "'{}'", "'{(,3]}'"),
    "int8multirange": ("'{[1,5),[7,9)}'", "'{}'", "'{(,3]}'"),
    "nummultirange": ("'{[1.5,2.5)}'", "'{}'", "'{(,3]}'"),
}

_UNITS = ("'day'", "'month'", "'year'")
_FIELDS = ("'year'", "'month'", "'dow'")
_PATTERNS = ("'b'", "'[a-z]+'", "'^1'")
_ENCODINGS = ("'hex'", "'base64'", "'escape'")

_BOUNDS = ("'[)'", "'[]'", "'()'")
_START = ("1", "2", "1")

#: Where a function needs literals its argument's type cannot give, by name and argument position;
#: ``name(type)`` for the overloads whose first argument is that type. Raw literals, cast as
#: ``SAMPLES`` is.
ARG_VALUES: dict[str, dict[int, tuple[str, str, str]]] = {
    "date_trunc": {0: _UNITS},
    "date_part": {0: _FIELDS},
    "extract": {0: _FIELDS},
    "date_bin": {0: ("'15 minutes'", "'1 hour'", "'1 day'")},
    "to_char": {1: ("'YYYY-MM-DD'", "'HH24:MI:SS'", "'FM999.00'")},
    "to_date": {
        0: ("'2024-06-15'", "'15 Jun 2024'", "'1999'"),
        1: ("'YYYY-MM-DD'", "'DD Mon YYYY'", "'YYYY'"),
    },
    "to_timestamp": {
        0: ("'2024-06-15 12:30:45'", "'15 Jun 2024'", "'1999'"),
        1: ("'YYYY-MM-DD HH24:MI:SS'", "'DD Mon YYYY'", "'YYYY'"),
    },
    "to_number": {0: ("'12,345.5'", "'-7'", "'0042'"), 1: ("'99G999D9'", "'S9'", "'0000'")},
    "make_date": {0: ("2024", "1999", "2000"), 1: ("6", "12", "2"), 2: ("15", "31", "29")},
    "make_time": {0: ("12", "0", "23"), 1: ("30", "0", "59"), 2: ("45.5", "0", "59.5")},
    "make_timestamp": {
        0: ("2024", "1999", "2000"),
        1: ("6", "12", "2"),
        2: ("15", "31", "29"),
        3: ("12", "0", "23"),
        4: ("30", "0", "59"),
        5: ("45.5", "0", "59.5"),
    },
    "make_timestamptz": {
        0: ("2024", "1999", "2000"),
        1: ("6", "12", "2"),
        2: ("15", "31", "29"),
        3: ("12", "0", "23"),
        4: ("30", "0", "59"),
        5: ("45.5", "0", "59.5"),
    },
    "regexp_count": {1: _PATTERNS},
    "regexp_instr": {1: _PATTERNS},
    "regexp_like": {1: _PATTERNS},
    "regexp_match": {1: _PATTERNS},
    "regexp_matches": {1: _PATTERNS},
    "regexp_replace": {1: _PATTERNS},
    "regexp_split_to_array": {1: _PATTERNS},
    "regexp_split_to_table": {1: _PATTERNS},
    "regexp_substr": {1: _PATTERNS},
    "encode": {1: _ENCODINGS},
    "decode": {0: ("'deadbeef'", "'3q2+7w=='", "'abc'"), 1: _ENCODINGS},
    "convert_from": {1: ("'UTF8'", "'LATIN1'", "'UTF8'")},
    "convert_to": {1: ("'UTF8'", "'LATIN1'", "'UTF8'")},
    "convert": {1: ("'UTF8'", "'LATIN1'", "'UTF8'"), 2: ("'LATIN1'", "'UTF8'", "'LATIN1'")},
    "nth_value": {1: ("1", "2", "1")},
    "lag": {1: ("1", "2", "1")},
    "lead": {1: ("1", "2", "1")},
    "ntile": {0: ("2", "2", "2")},
    "percentile_cont": {0: ("0.25", "0.5", "0.75")},
    "percentile_disc": {0: ("0.25", "0.5", "0.75")},
    "width_bucket(numeric)": {1: ("0", "(-10)", "0"), 2: ("10", "10", "100"), 3: ("5", "4", "10")},
    "width_bucket(double precision)": {
        1: ("0", "(-10)", "0"),
        2: ("10", "10", "100"),
        3: ("5", "4", "10"),
    },
    "repeat": {1: ("2", "0", "3")},
    "lpad": {1: ("6", "2", "5")},
    "rpad": {1: ("6", "2", "5")},
    "set_byte": {1: ("0", "1", "0"), 2: ("65", "66", "67")},
    "get_byte": {1: ("0", "1", "0")},
    "set_bit": {1: ("0", "1", "0"), 2: ("1", "0", "1")},
    "get_bit": {1: ("0", "1", "0")},
    "split_part": {1: ("'b'", "' '", "'1'"), 2: ("1", "2", "1")},
    "json_extract_path": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "json_extract_path_text": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "jsonb_extract_path": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "jsonb_extract_path_text": {1: ("'{a}'", "'{0}'", "'{a}'")},
    "timezone(text)": {0: ("'UTC'", "'America/New_York'", "'Asia/Tokyo'")},
    "substring": {1: _START, 2: ("2", "3", "1")},
    "overlay": {2: _START, 3: ("1", "2", "1")},
    "like_escape": {1: ("'\\'", "'#'", "''")},
    "similar_escape": {1: ("'\\'", "'#'", "''")},
    "similar_to_escape": {1: ("'\\'", "'#'", "''")},
    "ts_headline": {3: ("'MaxWords=2, MinWords=1'", "'ShortWord=1'", "''")},
    "daterange": {2: _BOUNDS},
    "int4range": {2: _BOUNDS},
    "int8range": {2: _BOUNDS},
    "numrange": {2: _BOUNDS},
    "tsrange": {2: _BOUNDS},
    "tstzrange": {2: _BOUNDS},
}


@dataclass(frozen=True)
class Call:
    """One statement of an overload. ``answers``: a real answer, compared by value; ``values``:
    whether the rows compare at all (the NULL call of a stable or volatile function: no)."""

    sql: str
    answers: bool = False
    values: bool = True


def _cast(literal: str, argtype: str) -> str:
    concrete = function_probe.CONCRETE.get(argtype, argtype)
    return f"{literal}::{concrete}"


def _sample(fn: Function, position: int, row: int) -> str | None:
    """Row ``row``'s literal of ``fn``'s argument ``position``, cast; None if it has none."""
    argtype = fn.argtypes[position]
    first = fn.argtypes[0]
    given = (ARG_VALUES.get(f"{fn.name}({first})") or ARG_VALUES.get(fn.name) or {}).get(position)
    if given is None:
        samples = SAMPLES.get(function_probe.CONCRETE.get(argtype, argtype))
        if samples is None:
            return None
        given = samples
    return _cast(given[row], argtype)


def unchecked(fn: Function, family: str) -> str | None:
    """Why ``fn`` is given no values; None if it is."""
    if function_probe.UNCALLABLE in fn.argtypes:
        return function_probe.NO_CALL
    if fn.signature in function_probe.STATEMENTS:
        return "its one statement takes no value"
    if fn.volatility == "v":
        return "volatile"
    if fn.name in CLOCK_FUNCTIONS:
        return "answers from the clock"
    if fn.volatility == "s" and family not in STABLE_FAMILIES:
        return f"stable, of family {family}"
    missing = sorted(
        {
            function_probe.CONCRETE.get(t, t)
            for i, t in enumerate(fn.argtypes)
            if _sample(fn, i, 0) is None
        }
    )
    return f"no sample of {', '.join(missing)}" if missing else None


def _rows(fn: Function) -> list[list[str]]:
    return [
        [_sample(fn, i, row) or "" for i in range(len(fn.argtypes))] for row in range(VALUE_ROWS)
    ]


def _values(rows: Iterable[Iterable[str]]) -> str:
    return "VALUES " + ", ".join(f"({', '.join(row)})" for row in rows)


def _columns(count: int) -> list[str]:
    return [f"c{i + 1}" for i in range(count)]


def _aggregate(fn: Function) -> str:
    """An aggregate over the rows of a VALUES list, which reads no table and so reaches the engine."""
    rows = _rows(fn)
    named = f'pg_catalog."{fn.name}"'
    if fn.aggkind == "h":
        relation = "(VALUES (1), (2), (3)) AS t(c1)"
        return f"SELECT {named}({rows[0][0]}) WITHIN GROUP (ORDER BY c1) AS v FROM {relation}"
    if not fn.argtypes:
        return f"SELECT {function_probe.statement(fn, [])[len('SELECT ') :]} FROM (VALUES (1), (2), (3)) AS t(c1)"
    if fn.aggkind == "o":
        direct = ", ".join(rows[0][: fn.direct])
        ordered = [row[fn.direct :] for row in rows]
        columns = _columns(len(ordered[0]))
        relation = f"({_values(ordered)}) AS t({', '.join(columns)})"
        return (
            f"SELECT {named}({direct}) WITHIN GROUP (ORDER BY {', '.join(columns)}) AS v "
            f"FROM {relation}"
        )
    columns = _columns(len(fn.argtypes))
    relation = f"({_values(rows)}) AS t({', '.join(columns)})"
    # The order of an aggregate that keeps its rows' order is the call's to give.
    order = " ORDER BY c1" if "_agg" in fn.name else ""
    return f"SELECT {named}({', '.join(columns)}{order}) AS v FROM {relation}"


def _window(fn: Function) -> str:
    named = f'pg_catalog."{fn.name}"'
    rows = _rows(fn)
    numbered = [[str(i + 1), *row] for i, row in enumerate(rows)]
    columns = _columns(len(fn.argtypes) + 1)
    relation = f"({_values(numbered)}) AS t({', '.join(columns)})"
    return f"SELECT {named}({', '.join(columns[1:])}) OVER (ORDER BY c1) AS v FROM {relation}"


def value_calls(fn: Function, family: str) -> list[Call]:
    """The calls of ``fn`` with real values, none if ``unchecked`` says so."""
    if unchecked(fn, family) is not None:
        return []
    if fn.kind == "a":
        return [Call(_aggregate(fn), answers=True)]
    if fn.kind == "w":
        return [Call(_window(fn), answers=True)]
    if not fn.argtypes:
        return [Call(function_probe.statement(fn, []), answers=True)]
    calls = []
    for row in _rows(fn):
        if fn.variadic:
            row = [*row[:-1], f"VARIADIC {row[-1]}"]
        calls.append(Call(function_probe.statement(fn, row), answers=True))
    return calls


def calls(fn: Function, family: str) -> list[Call]:
    """Every call of ``fn``: its NULL call, then its value calls."""
    valued = value_calls(fn, family)
    null = function_probe.call(fn)
    if null is None or any(c.sql == null for c in valued):
        return valued
    return [Call(null, values=fn.volatility == "i"), *valued]


# --- one call on one server -------------------------------------------------------------------


class Session(catalog.Server):
    """A server under the session settings, whose every call has a deadline: a statement that
    hangs costs its own overload, not the leg."""

    def __init__(self, dsn: str) -> None:
        super().__init__(dsn)
        self._set = None
        self._ensure()

    def _ensure(self) -> None:
        super()._ensure()
        if self._set is not self.conn:
            self._set = self.conn
            for sql in SETTINGS:
                self.conn.pgconn.exec_(sql.encode())

    def answer(self, sql: str, timeout: float = CALL_TIMEOUT_S) -> dict[str, Any]:
        """One statement's every row as text: its fields and rows, or its error. A lost
        connection says so (``lost``): the next call reconnects, and raises if it cannot."""
        self._ensure()
        conn = self.conn.pgconn
        deadline = time.monotonic() + timeout
        last = None
        try:
            conn.send_query_params(sql.encode(), [], result_format=0)
            while True:
                conn.consume_input()
                if conn.is_busy():
                    left = deadline - time.monotonic()
                    if left <= 0 or not select.select([conn.socket], [], [], left)[0]:
                        self.conn.close()
                        return {"ok": False, "error": f"no answer in {timeout:.0f}s"}
                    continue
                res = conn.get_result()
                if res is None:
                    break
                if last is None or res.status != pq.ExecStatus.TUPLES_OK:
                    last = res
        except psycopg.Error as exc:
            self.conn.close()
            return {"ok": False, "error": f"connection: {exc}".strip(), "lost": True}
        if last is None:
            return {"ok": False, "error": "no result"}
        return _result(last)


def _result(res: Any) -> dict[str, Any]:
    if res.status not in (pq.ExecStatus.TUPLES_OK, pq.ExecStatus.COMMAND_OK):
        return {
            "ok": False,
            "error": (res.error_message or b"").decode(errors="replace").strip()[:400],
        }
    fields = [res.ftype(i) for i in range(res.nfields)]
    rows = [
        tuple(
            None if (v := res.get_value(r, c)) is None else v.decode(errors="replace")
            for c in range(res.nfields)
        )
        for r in range(res.ntuples)
    ]
    return {"ok": True, "fields": fields, "rows": rows}


# --- comparison -------------------------------------------------------------------------------


def _floats_equal(a: str, b: str) -> bool:
    try:
        x, y = float(a), float(b)
    except ValueError:
        return False
    if not (math.isfinite(x) and math.isfinite(y)):
        return False
    return abs(x - y) <= FLOAT_TOLERANCE * max(abs(x), abs(y))


def _value_equal(want: str | None, got: str | None, oid: int) -> bool:
    if want == got:
        return True
    if want is None or got is None:
        return False
    if oid in _FLOAT_OIDS:
        return _floats_equal(want, got)
    if oid in _FLOAT_ARRAY_OIDS:
        a, b = want.strip("{}").split(","), got.strip("{}").split(",")
        return len(a) == len(b) and all(
            x == y or _floats_equal(x, y) for x, y in zip(a, b, strict=True)
        )
    return False


def _order(row: tuple, fields: list[int]) -> tuple:
    """A float sorts by its value, so a last-digit difference does not misalign two multisets."""
    key: list[tuple[int, float, str]] = []
    for value, oid in zip(row, fields, strict=True):
        if value is None:
            key.append((1, 0.0, ""))
        elif oid in _FLOAT_OIDS:
            try:
                key.append((0, float(value), ""))
            except ValueError:
                key.append((0, 0.0, value))
        else:
            key.append((0, 0.0, value))
    return tuple(key)


def difference(want: dict[str, Any], got: dict[str, Any], *, values: bool) -> str | None:
    """How Provisa's answer differs from PostgreSQL's; None if it does not. ``values`` False
    compares the result types alone."""
    if want["fields"] != got["fields"]:
        return f"result type oids {got['fields']}, PostgreSQL {want['fields']}"
    if not values:
        return None
    fields = want["fields"]
    a = sorted(want["rows"], key=lambda r: _order(r, fields))
    b = sorted(got["rows"], key=lambda r: _order(r, fields))
    if len(a) != len(b):
        return f"{len(b)} rows, PostgreSQL {len(a)}"
    for x, y in zip(a, b, strict=True):
        for wv, gv, oid in zip(x, y, fields, strict=True):
            if not _value_equal(wv, gv, oid):
                return f"{gv!r}, PostgreSQL {wv!r}"
    return None


# --- one overload on one engine ---------------------------------------------------------------


def measure_overload(
    fn: Function, family: str, pg: Session, provisa: Session, classify: Any
) -> dict[str, Any]:
    """Every call of ``fn`` on both servers, as this engine's one outcome of it."""
    planned = calls(fn, family)
    entry: dict[str, Any] = {
        "function": fn.name,
        "overload": fn.overload,
        "family": family,
        "kind": fn.kind,
        "volatility": fn.volatility,
    }
    accepted = answered = 0
    wrong: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    refused: list[str] = []
    routes: set[str] = set()
    lost = False
    for call_ in planned:
        want = pg.answer(call_.sql)
        if not want["ok"] or not want["fields"]:
            refused.append(want.get("error") or "no result column")
            continue
        accepted += 1
        answered += call_.answers
        routes.add("catalog" if classify(call_.sql) == "INTERCEPT" else "engine")
        got = provisa.answer(call_.sql)
        if not got["ok"]:
            errors.append({"statement": call_.sql, "error": got["error"]})
            lost = lost or got.get("lost", False)
            continue
        differs = difference(want, got, values=call_.values)
        if differs is not None:
            wrong.append({"statement": call_.sql, "difference": differs})
    entry["calls"] = accepted
    entry["answers_compared"] = answered
    entry["answers_checked"] = answered > 0
    entry["served_by"] = "+".join(sorted(routes)) or None
    if not answered:
        why = unchecked(fn, family)
        entry["unchecked"] = why or "PostgreSQL refused every value call"
    if lost:
        entry["connection_lost"] = True
    if accepted == 0:
        entry["outcome"] = "not_callable"
        entry["detail"] = (
            f"PostgreSQL refuses the call: {refused[0]}" if refused else function_probe.NO_CALL
        )
    elif wrong:
        entry["outcome"] = "wrong_answer"
        entry["detail"] = wrong[0]
        entry["wrong_calls"] = len(wrong)
    elif errors:
        entry["outcome"] = "error"
        entry["detail"] = errors[0]
        entry["error_calls"] = len(errors)
    else:
        entry["outcome"] = "pass"
    return entry


def _classifier() -> Any:
    from provisa.pgwire.catalog import classify

    return classify


def measure(
    engine: str, pg_dsn: str, provisa_dsn: str, out_json: Path, out_md: Path, limit: int | None
) -> int:
    """One leg. Written whatever the leg reaches: what it did not reach is ``not_reached``."""
    pg = Session(pg_dsn)
    fns = catalog.pg_functions(pg)
    if limit is not None:
        fns = fns[:: max(1, len(fns) // limit)]
    declared = function_probe.families()
    classify = _classifier()
    provisa = Session(provisa_dsn)
    started = time.monotonic()
    overloads: list[dict[str, Any]] = []
    server_gone: str | None = None
    for i, fn in enumerate(fns):
        family = function_probe.family(fn, declared)
        if server_gone is None and time.monotonic() - started > LEG_BUDGET_S:
            server_gone = f"the leg ran out of its {LEG_BUDGET_S:.0f}s at {fn.signature}"
        if server_gone is None:
            try:
                overloads.append(measure_overload(fn, family, pg, provisa, classify))
            except psycopg.OperationalError as exc:
                server_gone = f"Provisa stopped answering at {fn.signature}: {exc}".strip()
        if server_gone is not None and len(overloads) == i:
            print(f"::error::{server_gone}")
            overloads.append(
                {
                    "function": fn.name,
                    "overload": fn.overload,
                    "family": family,
                    "kind": fn.kind,
                    "volatility": fn.volatility,
                    "outcome": "not_reached",
                    "detail": server_gone,
                }
            )
        if i % 200 == 0:
            print(f"{i}/{len(fns)} after {time.monotonic() - started:.0f}s", flush=True)
    report = {
        "engine": engine,
        "postgres_version": pg.rows("SHOW server_version")[0][0],
        "seconds": round(time.monotonic() - started, 1),
        "server_gone": server_gone,
        "counts": counts(overloads),
        "overloads": overloads,
    }
    out_json.write_text(json.dumps(report, indent=1) + "\n")
    out_md.write_text(engine_markdown(report) + "\n")
    print(engine_markdown(report))
    return 1 if server_gone else 0


# --- counts and the merged data file ----------------------------------------------------------

OUTCOMES = ("pass", "wrong_answer", "error", "not_callable", "not_reached")


def counts(overloads: list[dict[str, Any]]) -> dict[str, int]:
    """The outcomes, with a pass split by whether an answer was checked or it was only called."""
    tally = Counter(o["outcome"] for o in overloads)
    out = {name: tally.get(name, 0) for name in OUTCOMES}
    out["total"] = len(overloads)
    out["pass_answers_checked"] = sum(
        1 for o in overloads if o["outcome"] == "pass" and o.get("answers_checked")
    )
    out["pass_only_called"] = out["pass"] - out["pass_answers_checked"]
    # Passed without reaching the engine: the catalog DB answers it on every engine alike.
    out["pass_catalog_answered"] = sum(
        1 for o in overloads if o["outcome"] == "pass" and o.get("served_by") == "catalog"
    )
    return out


def _row(label: str, c: dict[str, int]) -> str:
    return (
        f"| {label} | {c['total']} | {c['pass_answers_checked']} | {c['pass_only_called']} "
        f"| {c['wrong_answer']} | {c['error']} | {c['not_callable']} | {c['not_reached']} |"
    )


_HEADER = (
    "| | overloads | pass, answers checked | pass, only called | wrong answer | error "
    "| not callable | not reached |",
    "|---|---:|---:|---:|---:|---:|---:|---:|",
)


def _families(overloads: list[dict[str, Any]]) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    by: dict[str, list[dict[str, Any]]] = {}
    for o in overloads:
        by.setdefault(o["family"], []).append(o)
    yield from sorted(by.items())


def engine_markdown(report: dict[str, Any]) -> str:
    """One leg's summary: the engine's counts, then each family's."""
    lines = [
        f"## pg_catalog functions on {report['engine']} (PostgreSQL {report['postgres_version']})",
        "",
        *_HEADER,
        _row(report["engine"], report["counts"]),
        *(_row(family, counts(group)) for family, group in _families(report["overloads"])),
        "",
    ]
    if report["server_gone"]:
        lines += [f"**{report['server_gone']}**", ""]
    return "\n".join(lines)


def _outcome(entry: dict[str, Any] | None) -> dict[str, Any]:
    """An overload's outcome on one engine, as the data file keeps it."""
    if entry is None:
        return {"outcome": "not_reached", "detail": "the engine's leg left no result"}
    kept: dict[str, Any] = {"outcome": entry["outcome"]}
    for key in ("detail", "served_by"):
        if entry.get(key) is not None:
            kept[key] = entry[key]
    return kept


def merged(reports: dict[str, dict[str, Any] | None]) -> dict[str, Any]:
    """The legs as one document. ``reports`` holds every engine the workflow runs, None for a leg
    that left no result: it counts as not reached, so nothing is guaranteed by its absence.

    An overload is on the guaranteed list when it passes on every engine, listed under
    ``answers_checked`` if an answer was compared and ``only_called`` if not. A function is
    guaranteed when each of its overloads PostgreSQL can call is."""
    by_engine = {
        engine: {
            (o["function"], o["overload"]): o for o in (report or {"overloads": []})["overloads"]
        }
        for engine, report in reports.items()
    }
    keys = sorted({key for overloads in by_engine.values() for key in overloads})
    functions: list[dict[str, Any]] = []
    for name, overload in keys:
        entries = {
            engine: overloads.get((name, overload)) for engine, overloads in by_engine.items()
        }
        any_entry = next(e for e in entries.values() if e is not None)
        functions.append(
            {
                "function": name,
                "overload": overload,
                "family": any_entry["family"],
                "kind": any_entry["kind"],
                "volatility": any_entry["volatility"],
                "answers_checked": any(
                    e is not None and e.get("answers_checked") for e in entries.values()
                ),
                "unchecked": next(
                    (e["unchecked"] for e in entries.values() if e and "unchecked" in e), None
                ),
                "outcomes": {engine: _outcome(entry) for engine, entry in entries.items()},
            }
        )
    guaranteed: list[dict[str, Any]] = [
        f for f in functions if all(o["outcome"] == "pass" for o in f["outcomes"].values())
    ]
    held = {(f["function"], f["overload"]) for f in guaranteed}
    callable_by_name: dict[str, list[dict[str, Any]]] = {}
    for f in functions:
        if not all(o["outcome"] == "not_callable" for o in f["outcomes"].values()):
            callable_by_name.setdefault(f["function"], []).append(f)
    missing = sorted(engine for engine, report in reports.items() if report is None)
    incomplete = sorted(
        engine for engine, report in reports.items() if report is None or report.get("server_gone")
    )
    per_engine = {
        engine: counts([by_engine[engine].get(key) or _absent() for key in keys])
        for engine in reports
    }
    return {
        "postgres_version": next((r["postgres_version"] for r in reports.values() if r), None),
        "engines": sorted(reports),
        "engines_without_a_result": missing,
        "engines_incomplete": incomplete,
        "comparison": (
            "text format under one TimeZone, DateStyle and IntervalStyle; rows as a multiset; "
            f"floats within one part in {1 / FLOAT_TOLERANCE:.0e}"
        ),
        "counts": per_engine,
        "guaranteed": {
            "answers_checked": sorted(
                f"{f['function']}({f['overload']})" for f in guaranteed if f["answers_checked"]
            ),
            "only_called": sorted(
                f"{f['function']}({f['overload']})" for f in guaranteed if not f["answers_checked"]
            ),
            "functions": sorted(
                name
                for name, overloads in callable_by_name.items()
                if all((f["function"], f["overload"]) in held for f in overloads)
            ),
        },
        "functions": functions,
    }


def _absent() -> dict[str, Any]:
    return {"outcome": "not_reached", "family": ""}


def merged_markdown(doc: dict[str, Any]) -> str:
    """The run's summary: per engine, per family, and the guaranteed list's size."""
    engines = doc["engines"]
    guaranteed = doc["guaranteed"]
    lines = [
        f"## pg_catalog functions per engine (PostgreSQL {doc['postgres_version']})",
        "",
        *_HEADER,
        *(_row(engine, doc["counts"][engine]) for engine in engines),
        "",
        f"**Guaranteed list:** {len(guaranteed['answers_checked'])} overloads pass on all "
        f"{len(engines)} engines with an answer checked, {len(guaranteed['only_called'])} more "
        f"pass only called; {len(guaranteed['functions'])} functions have every callable "
        "overload on the list.",
        "",
        "Comparison: " + doc["comparison"] + ". A pass counts as answers checked when a call with "
        "real values was compared, and as only called when it was not: such a pass proves the "
        "call is served and its type, not its answer.",
        "",
    ]
    answered = {engine: doc["counts"][engine]["pass_catalog_answered"] for engine in engines}
    lines += [
        "Passes the catalog DB answers, without reaching the engine: "
        + ", ".join(f"{engine} {n}" for engine, n in answered.items())
        + ".",
        "",
    ]
    if doc["engines_incomplete"]:
        lines += [f"**Incomplete legs:** {', '.join(doc['engines_incomplete'])}.", ""]
    lines += [
        "### Per family: pass (answers checked + only called) of callable overloads",
        "",
        "| family | overloads | " + " | ".join(engines) + " | on all |",
        "|---|---:|" + "---:|" * (len(engines) + 1),
    ]
    by_family: dict[str, list[dict[str, Any]]] = {}
    for f in doc["functions"]:
        by_family.setdefault(f["family"], []).append(f)
    for family, members in sorted(by_family.items()):
        cells = [
            _family_cell([f["outcomes"][engine]["outcome"] for f in members], members)
            for engine in engines
        ]
        on_all = sum(
            1 for f in members if all(o["outcome"] == "pass" for o in f["outcomes"].values())
        )
        lines.append(f"| {family} | {len(members)} | {' | '.join(cells)} | {on_all} |")
    lines.append("")
    return "\n".join(lines)


def _family_cell(outcomes: list[str], members: list[dict[str, Any]]) -> str:
    checked = sum(
        1 for o, f in zip(outcomes, members, strict=True) if o == "pass" and f["answers_checked"]
    )
    passed = sum(1 for o in outcomes if o == "pass")
    callable_ = sum(1 for o in outcomes if o != "not_callable")
    return f"{checked} + {passed - checked} of {callable_}"


def merge(engines: list[str], indir: Path, out: Path, out_md: Path) -> int:
    """``function-support-<engine>.json`` of each engine, anywhere under ``indir``, as one file.
    Fails when a leg left no result or stopped short: the list is then not a guarantee."""
    reports: dict[str, dict[str, Any] | None] = {}
    for engine in engines:
        found = sorted(indir.rglob(f"function-support-{engine}.json"))
        if len(found) > 1:
            raise SystemExit(f"{engine}: more than one result under {indir}: {found}")
        reports[engine] = json.loads(found[0].read_text()) if found else None
    doc = merged(reports)
    out.write_text(json.dumps(doc, indent=1) + "\n")
    text = merged_markdown(doc)
    out_md.write_text(text + "\n")
    print(text)
    return 1 if doc["engines_incomplete"] else 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    m = sub.add_parser("measure")
    m.add_argument("--engine", required=True)
    m.add_argument("--pg", required=True)
    m.add_argument("--provisa", required=True)
    m.add_argument("--out-json", required=True, type=Path)
    m.add_argument("--out-md", required=True, type=Path)
    m.add_argument("--limit", type=int, help="an even sample of about this many overloads")
    g = sub.add_parser("merge")
    g.add_argument("--engines", required=True, help="every engine of the run, comma separated")
    g.add_argument("--in", dest="indir", required=True, type=Path)
    g.add_argument("--out", required=True, type=Path)
    g.add_argument("--out-md", required=True, type=Path)
    args = parser.parse_args(argv[1:])
    if args.command == "measure":
        return measure(args.engine, args.pg, args.provisa, args.out_json, args.out_md, args.limit)
    return merge(args.engines.split(","), args.indir, args.out, args.out_md)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
