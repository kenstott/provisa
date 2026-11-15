# Copyright (c) 2026 Kenneth Stott
# Canary: 9579d636-4b4f-4d6b-a006-aad464e70171
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's forms carried over from DuckDB (REQ-2167, REQ-2236): an overload ClickHouse had no
form of, written on ClickHouse in the form DuckDB answers it with.

:data:`PORTED` lists those overloads, each by PostgreSQL's name and argument types. A call of one
is taken out of the statement before anything else reads it, written for DuckDB alone, as
``SELECT <call>``, and that one expression, read back in DuckDB's SQL, is written in ClickHouse's.
What ClickHouse writes of every other part of the statement is unchanged: a placeholder holds the
call's place until the statement is written, and is then replaced by the carried form.

A call is carried over only where DuckDB's form is one expression and calls no function DuckDB
alone has (a ``provisa_`` function of provisa.federation.duckdb_pg_scalars); otherwise it is left
where it was, to be refused by name as before. Each listed overload answered PostgreSQL's value on
the function support measure's calls and on its edge values
(tests/unit/polyfill/test_polyfill_clickhouse_carried.py); an overload the bulk port carried that
answered otherwise, or failed on the engine, is not listed, and stays refused by name."""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

import functools
import itertools
import re
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.transpiler.polyfill.grid import Feature, Rule, Side
from provisa.transpiler.polyfill.notation import Rewrite

_CLICKHOUSE = "clickhouse"

#: Each overload carried over: PostgreSQL's name of the function and the types of its arguments,
#: as pg_proc lists them. The comment is its family on the function support report.
PORTED: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("dexp", ("double precision",)),  # Numeric
    ("dlog1", ("double precision",)),  # Numeric
    ("text", ("xml",)),  # Other
    ("replace", ("text", "text", "text")),  # String
    ("tideq", ("tid", "tid")),  # Type operators
    ("tidge", ("tid", "tid")),  # Type operators
    ("tidgt", ("tid", "tid")),  # Type operators
    ("tidlarger", ("tid", "tid")),  # Type operators
    ("tidle", ("tid", "tid")),  # Type operators
    ("tidlt", ("tid", "tid")),  # Type operators
    ("tidne", ("tid", "tid")),  # Type operators
    ("tidsmaller", ("tid", "tid")),  # Type operators
)

#: The placeholder a carried call's place is held by, until the statement is written.
_PLACEHOLDER = "provisa_ported_"
_PLACED = re.compile(rf"\b{_PLACEHOLDER}(\d+)\(\)", re.IGNORECASE)


@functools.cache
def _said(pg_type: str) -> str | None:
    """A pg_proc argument type as said_type reads a statement's cast to it."""
    from provisa.transpiler.function_macros import said_type

    tree = sqlglot.parse_one(f"SELECT NULL::{pg_type}", read="postgres")
    assert isinstance(tree, exp.Select)
    return said_type(tree.selects[0])


@functools.cache
def _overloads() -> dict[tuple[str, int], tuple[tuple[str | None, ...], ...]]:
    table: dict[tuple[str, int], list[tuple[str | None, ...]]] = {}
    for name, types in PORTED:
        table.setdefault((name, len(types)), []).append(tuple(_said(t) for t in types))
    return {key: tuple(rows) for key, rows in table.items()}


#: What a statement calling a listed overload holds: one of their names.
_MENTIONS = (
    r"\b(?:"
    + "|".join(map(re.escape, sorted({name for name, _ in PORTED}, key=len, reverse=True)))
    + r")\b"
)
_MENTIONED = re.compile(_MENTIONS, re.IGNORECASE)


def _name_and_args(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    from provisa.transpiler import engine_refusals, function_macros

    if isinstance(node, exp.Anonymous):
        name = node.name.lower()
        return engine_refusals._NAMED.get(name, name), list(node.expressions)
    called = function_macros.call_of(node)
    if called is not None:
        return called
    if isinstance(node, exp.Func):
        found = engine_refusals._called(node)
        if found is not None:
            name, args = found
            return engine_refusals._NAMED.get(name, name), args
    return None


def _told(argument: exp.Expr) -> str | None:
    """The type the statement says of ``argument``: a minus sign keeps its operand's."""
    from provisa.transpiler.function_macros import said_type

    while isinstance(argument, (exp.Neg, exp.Paren)):
        argument = argument.this
    return said_type(argument)


def _fits(want: str | None, got: str) -> bool:
    return want is None or want == got.split("(")[0]


def is_ported(node: exp.Expr) -> bool:
    """Whether ``node`` calls an overload of :data:`PORTED`: its name, its argument count, and each
    argument's type, which the statement says (a cast, or a VALUES column's cast). An argument
    whose type is not said fits none: a call Provisa's own forms write holds such arguments, and
    keeps what ClickHouse has."""

    found = _name_and_args(node)
    if found is None:
        return False
    name, args = found
    rows = _overloads().get((name, len(args)))
    if rows is None:
        return False
    said = [_told(a) for a in args]
    told = [s for s in said if s is not None]
    if len(told) != len(said):
        return False
    return any(all(_fits(want, got) for want, got in zip(row, told)) for row in rows)


def duckdb_form(call: exp.Expr) -> exp.Expr | None:
    """DuckDB's form of ``call`` as one expression, or None where DuckDB's is not one expression
    ClickHouse can be given."""
    from provisa.transpiler.function_macros import FunctionRefused
    from provisa.transpiler.transpile import transpile

    try:
        written = transpile(f"SELECT {call.sql(dialect='postgres')} AS v", "duckdb")
        tree = sqlglot.parse_one(written, read="duckdb")
    except (FunctionRefused, SqlglotError, ValueError):
        return None
    if not isinstance(tree, exp.Select) or len(tree.selects) != 1:
        return None
    if any(tree.args.get(key) for key in ("from_", "from", "with_", "with", "where", "joins")):
        return None
    form = tree.selects[0].unalias()
    if any(f.name.lower().startswith("provisa_") for f in form.find_all(exp.Anonymous)):
        return None
    return form


def clickhouse_text(form: exp.Expr) -> str:
    """``form`` written in ClickHouse's SQL, with what ClickHouse would read otherwise said for it
    (transpile.clickhouse_semantics)."""
    from provisa.transpiler.transpile import clickhouse_semantics

    tree = clickhouse_semantics(exp.select(form.copy()))
    assert isinstance(tree, exp.Select)
    said = tree.selects[0]
    if not isinstance(said, (exp.Func, exp.Column, exp.Literal, exp.Paren)):
        said = exp.Paren(this=said)
    return said.sql(dialect=_CLICKHOUSE)


@dataclass
class HeldOut:
    """The carried calls of one statement, by placeholder number."""

    forms: dict[int, str] = field(default_factory=dict)

    def restored(self, written: str) -> str:
        """``written`` with each placeholder replaced by its call's ClickHouse form."""
        if not self.forms:
            return written
        return _PLACED.sub(lambda m: self.forms[int(m.group(1))], written)


def held_out(pg_sql: str, dialect: str) -> tuple[str, HeldOut]:
    """``pg_sql`` with each outermost call of a carried overload replaced by a placeholder, and
    the forms that replace them once the statement is written. Unchanged on any dialect but
    ClickHouse, and where no carried overload is called."""
    held = HeldOut()
    if dialect != _CLICKHOUSE or not _MENTIONED.search(pg_sql):
        return pg_sql, held
    try:
        tree = sqlglot.parse_one(pg_sql, read="postgres")
    except SqlglotError:
        return pg_sql, held
    numbers = itertools.count()

    def carried(node: exp.Expr) -> exp.Expr:
        target = node
        if (
            isinstance(node, exp.Dot)
            and isinstance(node.this, exp.Identifier)
            and node.this.name.lower() == "pg_catalog"
        ):
            target = node.expression
        if not is_ported(target):
            return node
        form = duckdb_form(target)
        if form is None:
            return node
        number = next(numbers)
        held.forms[number] = clickhouse_text(form)
        placeholder = exp.Anonymous(this=f"{_PLACEHOLDER}{number}", expressions=[])
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(placeholder, target.name or "v")
        return placeholder

    rewritten = tree.transform(carried)
    if not held.forms:
        return pg_sql, held
    return rewritten.sql(dialect="postgres"), held


def carried(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A call of a listed overload on ClickHouse, in the form DuckDB answers it with."""
    if dialect != _CLICKHOUSE or not is_ported(node):
        return None
    return duckdb_form(node)


def _listed() -> str:
    return ", ".join(f"{name}({', '.join(types)})" for name, types in PORTED)


#: The grid's row of the carried overloads: ClickHouse's form is the carrying function, and every
#: other dialect keeps what it had.
CARRIED = Feature(
    "carried_from_duckdb",
    "forms carried from DuckDB",
    "Overloads ClickHouse answers in the form DuckDB answers them with, each compared with "
    f"PostgreSQL's answer on its edge values: {_listed()}.",
    read=Side(rules=(Rule(None, {_CLICKHOUSE: Rewrite(carried, mentions=_MENTIONS)}),)),
)

FEATURES: tuple[Feature, ...] = (CARRIED,)
