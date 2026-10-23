# Copyright (c) 2026 Kenneth Stott
# Canary: f775ac64-1b41-4475-b38c-f41eaf91317a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The values a read of a code table gives the parameters its function pushes a predicate to
(REQ-2100).

A read of one code table is a statement over it. Its parameters go to the function as arguments;
its rows are landed and the engine runs the whole statement over them (codesource.proxy), so a
statement on any column never fails for the code and its author implements none of it.

:func:`pushed` reads the values a statement gives the parameters a predicate is pushed to (the
function's pushdown mapping, provisa/codesource/pushdown.py): only top-level AND conjuncts of the
WHERE of the SELECT reading the table, each a comparison of a column with a literal. The pushed
predicate stays in the statement and the engine applies it again, so a function that ignores or
loosens a value cannot return wrong rows.
"""

# Requirements: REQ-2100
from __future__ import annotations

import datetime
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import sqlglot
from sqlglot import exp

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


def conjuncts(node: exp.Expr | None) -> list[exp.Expr]:
    if node is None:
        return []
    if isinstance(node, exp.Paren):
        return conjuncts(node.this)
    if isinstance(node, exp.And):
        return conjuncts(node.left) + conjuncts(node.right)
    return [node]


def _reading(tree: exp.Expr, table: str) -> list[exp.Table]:
    return [t for t in tree.find_all(exp.Table) if t.name == table]


def predicate(node: exp.Expr, names: set[str]) -> tuple[str, str, Any] | None:
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
        for conjunct in conjuncts(select.args["where"].this if select.args.get("where") else None):
            found = predicate(conjunct, names)
            if found is None:
                continue
            column, op, value = found
            parameter = wanted.get((column, op))
            if parameter is not None and parameter not in out:
                out[parameter] = value
    return out
