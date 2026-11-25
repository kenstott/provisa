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

from provisa.core.pg_error import END, MARK
from provisa.transpiler.polyfill.grid import Feature, Rule, Side
from provisa.transpiler.polyfill.notation import Rewrite

_CLICKHOUSE = "clickhouse"

#: Each overload carried over: PostgreSQL's name of the function and the types of its arguments,
#: as pg_proc lists them. The comment is its family on the function support report.
PORTED: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("dexp", ("double precision",)),  # Numeric
    ("dlog1", ("double precision",)),  # Numeric
    ("text", ("xml",)),  # Other
    ("int8inc_any", ("bigint", '"any"')),  # Aggregate and window
    ("int8dec_any", ("bigint", '"any"')),  # Aggregate and window
    ("max", ("money",)),  # Aggregate and window
    ("min", ("money",)),  # Aggregate and window
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
    """A pg_proc argument type as said_type reads a statement's cast to it; None for "any", which
    any type the statement says fits."""
    from provisa.transpiler.function_macros import said_type

    if pg_type == '"any"':
        return None

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


def duckdb_form(call: exp.Expr, *, generated: bool) -> exp.Expr | None:
    """DuckDB's form of ``call`` as one expression, or None where DuckDB's is not one expression
    ClickHouse can be given. ``generated``: the origin of the statement ``call`` is of
    (``transpiler.transpile``)."""
    from provisa.transpiler.function_macros import FunctionRefused
    from provisa.transpiler.transpile import transpile

    try:
        written = transpile(
            f"SELECT {call.sql(dialect='postgres')} AS v", "duckdb", generated=generated
        )
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


#: PostgreSQL's integers: their names in its error, and their ranges.
_INTEGERS = {
    exp.DataType.Type.SMALLINT: ("smallint", 2**15),
    exp.DataType.Type.INT: ("integer", 2**31),
    exp.DataType.Type.BIGINT: ("bigint", 2**63),
}


def _past_its_integer(form: exp.Expr) -> str | None:
    """PostgreSQL's 22003 of a number cast to an integer it does not fit, which it raises as it
    reads the statement, wherever the cast stands: ClickHouse's raise of it, or None where the
    form holds no such cast. ClickHouse's CAST would wrap the number instead."""
    for node in form.find_all(exp.Cast):
        value = node.this
        if isinstance(node, exp.TryCast) or not isinstance(value, exp.Literal):
            continue
        named = _INTEGERS.get(node.to.this)
        if named is None or value.is_string or not value.is_int:
            continue
        if not -named[1] <= int(value.this) < named[1]:
            return f"throwIf(1, '{MARK} 22003: {named[0]} out of range{END}')"
    return None


def _adapted(node: exp.Expr) -> exp.Expr:
    """What DuckDB's form calls, as ClickHouse calls it: DuckDB's error() raises, as ClickHouse's
    throwIf does in the branch that runs; a TRY_CAST answers NULL where the value does not fit, as
    accurateCastOrNull does (ClickHouse's CAST of an integer wraps it)."""
    if isinstance(node, exp.Anonymous) and node.name.lower() == "error":
        return exp.Anonymous(this="throwIf", expressions=[exp.Literal.number(1), *node.expressions])
    if isinstance(node, exp.TryCast) and node.to.is_type(*exp.DataType.INTEGER_TYPES):
        named = node.to.sql(dialect=_CLICKHOUSE)
        return exp.Anonymous(
            this="accurateCastOrNull", expressions=[node.this, exp.Literal.string(named)]
        )
    return node


def clickhouse_text(form: exp.Expr) -> str:
    """``form`` written in ClickHouse's SQL, with what ClickHouse would read otherwise said for it
    (transpile.clickhouse_semantics)."""
    from provisa.transpiler.pg_money import exact_on_clickhouse
    from provisa.transpiler.transpile import clickhouse_semantics

    raised = _past_its_integer(form)
    if raised is not None:
        return raised
    # a number cast to a decimal is read as its text there: ClickHouse's number is a Float64
    adapted = form.copy().transform(exact_on_clickhouse).transform(_adapted)
    tree = clickhouse_semantics(exp.select(adapted))
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


def held_out(pg_sql: str, dialect: str, *, generated: bool) -> tuple[str, HeldOut]:
    """``pg_sql`` with each outermost call of a carried overload replaced by a placeholder, and
    the forms that replace them once the statement is written. Unchanged on any dialect but
    ClickHouse, and where no carried overload is called. ``generated``: the statement's origin
    (``transpiler.transpile``)."""
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
        form = duckdb_form(target, generated=generated)
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


def carried(node: exp.Expr, dialect: str, *, generated: bool) -> exp.Expr | None:
    """A call of a listed overload on ClickHouse, in the form DuckDB answers it with, read as a
    statement of origin ``generated`` reads it (``transpiler.transpile``)."""
    if dialect != _CLICKHOUSE or not is_ported(node):
        return None
    return duckdb_form(node, generated=generated)


def _carried_by_the_grid(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """``carried`` as the grid's ClickHouse form, which reads a call apart from its statement.
    REQ-2236: transpile's ``held_out`` carries each call it reaches first, with its statement's
    origin; a call the grid reads after it is one ``held_out`` left (no DuckDB form of one
    expression). The grid reads a call apart from any statement, so it states SQL as written."""
    return carried(node, dialect, generated=False)


def _listed() -> str:
    return ", ".join(f"{name}({', '.join(types)})" for name, types in PORTED)


#: The grid's row of the carried overloads: ClickHouse's form is the carrying function, and every
#: other dialect keeps what it had.
CARRIED = Feature(
    "carried_from_duckdb",
    "forms carried from DuckDB",
    "Overloads ClickHouse answers in the form DuckDB answers them with, each compared with "
    f"PostgreSQL's answer on its edge values: {_listed()}.",
    read=Side(
        rules=(Rule(None, {_CLICKHOUSE: Rewrite(_carried_by_the_grid, mentions=_MENTIONS)}),)
    ),
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 370
FEATURES: tuple[Feature, ...] = (CARRIED,)
