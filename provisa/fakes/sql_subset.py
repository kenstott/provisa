# Copyright (c) 2026 Kenneth Stott
# Canary: fae190db-09e7-4893-948a-f909d96b3abe
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The subset of SQL a ``sql`` or ``sql_group`` fake is written in (REQ-1494, THE SQL FAKE, THE
SQL_GROUP FAKE).

``sql(expression)``: one scalar expression over the current row's columns, in the governed dialect
-- arithmetic, comparison, conditional, string, date and time operations and the functions of
:data:`FUNCTIONS`. No subquery, no other table, no aggregate or window, and nothing whose result
can differ between two reads of the same row.

``sql_group(expression)`` adds :data:`WINDOW_FUNCTIONS` and :data:`AGGREGATES` over windows of the
same table (``SUM(amount) OVER (PARTITION BY customer_id ORDER BY id)``), and aggregates over a
parent's children through a declared relationship, the child's columns named through the
relationship (``SUM(lines.amount)``).
"""

# Requirements: REQ-1494

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from provisa.fakes.kinds import FakeRefused

DIALECT = "postgres"  # the governed dialect

# Operators and forms an expression may use.
_FORMS: tuple[type[exp.Expression], ...] = (
    exp.Column,
    exp.Identifier,
    exp.Literal,
    exp.Boolean,
    exp.Null,
    exp.Paren,
    exp.Tuple,
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.IntDiv,
    exp.Mod,
    exp.Neg,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Is,
    exp.In,
    exp.Between,
    exp.Like,
    exp.ILike,
    exp.And,
    exp.Or,
    exp.Not,
    exp.Case,
    exp.If,
    exp.Cast,
    exp.TryCast,
    exp.DataType,
    exp.DataTypeParam,
    exp.Interval,
    exp.Var,
    exp.DPipe,
)

#: The functions an expression may call, by the name it calls them.
FUNCTIONS: dict[str, type[exp.Expression]] = {
    "coalesce": exp.Coalesce,
    "nullif": exp.Nullif,
    "greatest": exp.Greatest,
    "least": exp.Least,
    "abs": exp.Abs,
    "round": exp.Round,
    "floor": exp.Floor,
    "ceil": exp.Ceil,
    "power": exp.Pow,
    "sqrt": exp.Sqrt,
    "ln": exp.Ln,
    "exp": exp.Exp,
    "sign": exp.Sign,
    "upper": exp.Upper,
    "lower": exp.Lower,
    "length": exp.Length,
    "substring": exp.Substring,
    "trim": exp.Trim,
    "replace": exp.Replace,
    "concat": exp.Concat,
    "lpad": exp.Pad,
    "left": exp.Left,
    "right": exp.Right,
    "date_trunc": exp.TimestampTrunc,  # how the governed dialect reads date_trunc
    "extract": exp.Extract,
    "date_add": exp.DateAdd,
    "date_diff": exp.DateDiff,
}

#: The window functions a ``sql_group`` expression may call, within ``OVER (...)``.
WINDOW_FUNCTIONS: dict[str, type[exp.Expression]] = {
    "row_number": exp.RowNumber,
    "rank": exp.Rank,
    "dense_rank": exp.DenseRank,
    "lag": exp.Lag,
    "lead": exp.Lead,
    "first_value": exp.FirstValue,
    "last_value": exp.LastValue,
}

#: The aggregates a ``sql_group`` expression may call, over a window or a parent's children.
AGGREGATES: dict[str, type[exp.Expression]] = {
    "sum": exp.Sum,
    "count": exp.Count,
    "avg": exp.Avg,
    "min": exp.Min,
    "max": exp.Max,
}

_WINDOW_FORMS: tuple[type[exp.Expression], ...] = (exp.Window, exp.Order, exp.Ordered, exp.Star)

# The function classes' names, for refusals.
_NAMED: dict[type, str] = {
    cls: name for table in (FUNCTIONS, WINDOW_FUNCTIONS, AGGREGATES) for name, cls in table.items()
}


@dataclass(frozen=True)
class Read:
    """What an expression reads: columns of its own row or table, and per relationship the child
    columns it aggregates."""

    columns: frozenset[str]
    children: dict[str, frozenset[str]]


def read(expression: str, *, group: bool) -> Read:
    """Parse ``expression`` and refuse by name anything outside the subset; return what it reads.
    Whether the columns exist is the caller's check (:func:`provisa.fakes.checks.check_table`)."""
    try:
        tree = sqlglot.parse_one(expression, read=DIALECT)
    except ParseError as exc:
        raise FakeRefused(f"cannot read {expression!r}: {exc}") from exc
    if isinstance(tree, (exp.Select, exp.Union, exp.Subquery)):
        raise FakeRefused(f"{expression!r} is a statement, not an expression")
    allowed: tuple[type[exp.Expression], ...] = _FORMS + tuple(FUNCTIONS.values())
    if group:
        allowed += _WINDOW_FORMS + tuple(WINDOW_FUNCTIONS.values()) + tuple(AGGREGATES.values())
    columns: set[str] = set()
    children: dict[str, set[str]] = {}
    for node in tree.walk():
        if isinstance(node, (exp.Subquery, exp.Select)):
            raise FakeRefused(f"{expression!r} holds a subquery")
        if isinstance(node, exp.Anonymous):
            raise FakeRefused(f"{expression!r} calls {node.name}(), which is not in the subset")
        if not isinstance(node, allowed):
            what = _NAMED.get(type(node), node.key)
            if not group and isinstance(
                node, tuple(AGGREGATES.values()) + tuple(WINDOW_FUNCTIONS.values()) + (exp.Window,)
            ):
                raise FakeRefused(
                    f"{expression!r} uses {what}, an aggregate or window, which only sql_group "
                    f"may use"
                )
            raise FakeRefused(f"{expression!r} uses {what}, which is not in the subset")
        if isinstance(node, exp.Column):
            if node.args.get("db") or node.args.get("catalog"):
                raise FakeRefused(f"{expression!r} names another table's column {node.sql()}")
            if node.table:
                if not group or not _in_aggregate(node):
                    raise FakeRefused(
                        f"{expression!r} names {node.sql()}: a child's column is read only within "
                        f"an aggregate of sql_group"
                    )
                children.setdefault(node.table, set()).add(node.name)
            else:
                columns.add(node.name)
    return Read(frozenset(columns), {k: frozenset(v) for k, v in children.items()})


def _in_aggregate(node: exp.Expression) -> bool:
    parent = node.parent
    while parent is not None:
        if isinstance(parent, tuple(AGGREGATES.values())):
            return not isinstance(parent.parent, exp.Window)
        parent = parent.parent
    return False
