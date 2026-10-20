# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a read of a code table asks beyond its function's arguments, applied to the function's
output (REQ-2100).

A read of one code table is a statement over it. Its parameters go to the function as arguments;
everything else -- column filters, sorts, limits, projections, aggregates -- is applied here, by an
embedded DuckDB, to the rows the function returned, so a statement on any column never fails for
the code and its author implements none of it. Every column is named as the code declares it.

:func:`pushed` reads the values a statement gives the parameters a predicate is pushed to (the
function's pushdown mapping, provisa/codesource/pushdown.py): only top-level AND conjuncts of the
WHERE of the SELECT reading the table, each a comparison of a column with a literal. The pushed
predicate stays in the statement and is applied again here, so a function that ignores or loosens
a value cannot return wrong rows.

:func:`answer` runs the statement over the rows. The rows hold the function's output columns, and
one column per parameter, ``_nf_{name}``, holding the value the read gave it (null when none), so
a statement's own conditions on its parameters hold of every row.

This module is carried, as it is, by the front end of every built image and used by Provisa for a
dev-mode source; it imports nothing of Provisa's: the standard library, sqlglot, DuckDB and Arrow.
"""

# Requirements: REQ-2100
from __future__ import annotations

import datetime
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import duckdb
import pyarrow as pa
import sqlglot
from sqlglot import exp

#: The relation the statement's table is read as.
ROWS = "__provisa_code_rows"
IR_KEY = b"provisa.ir"

_COMPARISONS: dict[type, str] = {
    exp.EQ: "eq",
    exp.NEQ: "ne",
    exp.GT: "gt",
    exp.GTE: "gte",
    exp.LT: "lt",
    exp.LTE: "lte",
}
#: A comparison read with its literal on the left: ``5 < score`` is ``score > 5``.
_FLIPPED = {"eq": "eq", "ne": "ne", "gt": "lt", "gte": "lte", "lt": "gt", "lte": "gte"}


class _NotLiteral(Exception):
    pass


def _value(node: exp.Expr) -> Any:
    """A literal's Python value: a string, a number, a boolean, a date or timestamp literal."""
    if isinstance(node, exp.Neg):
        inner = _value(node.this)
        if isinstance(inner, (int, float, Decimal)):
            return -inner
        raise _NotLiteral
    if isinstance(node, exp.Boolean):
        return node.this
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        text = str(node.this)
        return float(text) if any(c in text for c in ".eE") else int(text)
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Literal) and node.this.is_string:
        kind = node.to.this
        if kind == exp.DataType.Type.DATE:
            return datetime.date.fromisoformat(node.this.this)
        if kind in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ):
            return datetime.datetime.fromisoformat(node.this.this)
        return node.this.this
    raise _NotLiteral


def _conjuncts(node: exp.Expr | None) -> list[exp.Expr]:
    if node is None:
        return []
    if isinstance(node, exp.Paren):
        return _conjuncts(node.this)
    if isinstance(node, exp.And):
        return _conjuncts(node.left) + _conjuncts(node.right)
    return [node]


def _reading(tree: exp.Expr, table: str) -> list[exp.Table]:
    return [t for t in tree.find_all(exp.Table) if t.name == table]


def _predicate(node: exp.Expr, names: set[str]) -> tuple[str, str, Any] | None:
    """``(column, op, value)`` for a conjunct comparing one of the table's columns to a literal."""

    def column(side: exp.Expr) -> str | None:
        if isinstance(side, exp.Column) and side.table in names | {""}:
            return side.name
        return None

    try:
        op = _COMPARISONS.get(type(node))
        if op is not None and isinstance(node, exp.Binary):
            left, right = column(node.left), column(node.right)
            if left is not None and right is None:
                return left, op, _value(node.right)
            if right is not None and left is None:
                return right, _FLIPPED[op], _value(node.left)
            return None
        if isinstance(node, exp.In) and not node.args.get("query"):
            name = column(node.this)
            if name is not None:
                return name, "in", [_value(e) for e in node.expressions]
        if isinstance(node, exp.Like):
            name = column(node.this)
            if name is not None:
                value = _value(node.expression)
                return (name, "like", value) if isinstance(value, str) else None
    except _NotLiteral:
        return None
    return None


def pushed(
    sql: str, dialect: str, table: str, pushdown: Mapping[str, Mapping[str, str]]
) -> dict[str, Any]:
    """The values the statement ``sql`` gives the parameters a predicate on ``table``'s columns is
    pushed to (``pushdown``: parameter -> ``{column, op}``), by parameter. A parameter two
    conjuncts fill takes the first: each conjunct must hold, so either value is safe."""
    if not pushdown:
        return {}
    wanted = {(m["column"], m["op"]): p for p, m in pushdown.items()}
    tree = sqlglot.parse_one(sql, read=dialect)
    out: dict[str, Any] = {}
    for read in _reading(tree, table):
        select = read.parent_select
        if select is None or select.args.get("joins"):
            continue
        names = {read.alias_or_name, read.name}
        for conjunct in _conjuncts(select.args["where"].this if select.args.get("where") else None):
            found = _predicate(conjunct, names)
            if found is None:
                continue
            column, op, value = found
            parameter = wanted.get((column, op))
            if parameter is not None and parameter not in out:
                out[parameter] = value
    return out


def _typed(rows: pa.Table) -> pa.Table:
    """The function's rows with each numeric field (exact digits on the wire) as a decimal wide
    enough for every value it holds."""
    columns, fields = [], []
    for field, column in zip(rows.schema, rows.columns, strict=True):
        if (field.metadata or {}).get(IR_KEY) != b"numeric":
            columns.append(column)
            fields.append(field)
            continue
        values = [None if v is None else Decimal(v) for v in column.to_pylist()]
        digits = [d for d in values if d is not None]
        scale = max((max(-int(d.as_tuple().exponent), 0) for d in digits), default=0)
        whole = max((d.adjusted() + 1 for d in digits if d != 0), default=1)
        kind = pa.decimal128(max(whole, 1) + scale, scale)
        columns.append(pa.array(values, type=kind))
        fields.append(pa.field(field.name, kind))
    return pa.Table.from_arrays(columns, schema=pa.schema(fields))


def with_arguments(
    rows: pa.Table, arguments: Mapping[str, Any], types: Mapping[str, pa.DataType]
) -> pa.Table:
    """``rows`` with a column per parameter, ``_nf_{name}``, every row holding the value the read
    gave it (null when it gave none)."""
    for name, kind in types.items():
        value = arguments.get(name)
        rows = rows.append_column(
            pa.field(f"_nf_{name}", kind), pa.array([value] * rows.num_rows, type=kind)
        )
    return rows


def answer(sql: str, dialect: str, table: str, rows: pa.Table) -> pa.Table:
    """``sql`` over ``rows``, the function's output as the statement's ``table``: every reference
    to the table reads them, under the name the statement gives it."""
    tree = sqlglot.parse_one(sql, read=dialect)
    for read in _reading(tree, table):
        alias = read.alias_or_name
        read.replace(exp.alias_(exp.to_table(ROWS), alias, table=True))
    for column in tree.find_all(exp.Column):
        # A column the statement qualifies by its table's schema or catalog is the table's.
        column.set("db", None)
        column.set("catalog", None)
    with duckdb.connect() as con:
        con.register(ROWS, _typed(rows))
        return con.execute(tree.sql(dialect="duckdb")).to_arrow_table()
