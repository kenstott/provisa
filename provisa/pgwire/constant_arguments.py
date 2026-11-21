# Copyright (c) 2026 Kenneth Stott
# Canary: 6d3b1f8a-c927-4e05-8a41-f27c90de1b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The arguments of a server function pgwire evaluates itself (REQ-2167): pg_sleep's duration and
an advisory lock's key run on a DuckDB of their own, outside governance, where a subquery or a
table function could read the server's files. Only a constant expression is evaluated there."""

# Requirements: REQ-2167

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

_CONSTANT_NODES = (
    exp.Add,
    exp.Boolean,
    exp.Cast,
    exp.CurrentTimestamp,
    exp.DataType,
    exp.DataTypeParam,
    exp.Div,
    exp.Identifier,
    exp.IntDiv,
    exp.Interval,
    exp.Literal,
    exp.Mod,
    exp.Mul,
    exp.Neg,
    exp.Null,
    exp.Paren,
    exp.Sub,
    exp.Var,
)


#: The time, by each of PostgreSQL's names for it: within one evaluation, the same instant.
_NOW = frozenset({"now", "clock_timestamp", "statement_timestamp", "transaction_timestamp"})


def _constant_node(node: exp.Expr) -> bool:
    if isinstance(node, exp.Anonymous):
        return node.name.lower() in _NOW and not node.expressions
    if isinstance(node, exp.Dot):  # pg_catalog.now()
        return isinstance(node.this, exp.Identifier) and node.this.name.lower() == "pg_catalog"
    return isinstance(node, _CONSTANT_NODES)


def is_constant(expression: exp.Expr) -> bool:
    """Whether ``expression`` is literals, casts and arithmetic, and the time: nothing it reads."""
    return all(_constant_node(node) for node in expression.walk())


def evaluable(sql: str) -> str:
    """A constant expression (``is_constant_sql``) as the evaluating DuckDB reads it: each name of
    the time is now(), which DuckDB has, and no pg_catalog qualifier."""

    def now(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Anonymous):
            node = node.expression
        if isinstance(node, exp.Anonymous) and node.name.lower() in _NOW:
            return exp.Anonymous(this="now")
        return node

    select = sqlglot.parse_one(f"SELECT {sql}", read="postgres")
    assert isinstance(select, exp.Select)
    return select.selects[0].transform(now).sql(dialect="postgres")


def is_constant_sql(sql: str) -> bool:
    """``is_constant`` of an expression in PostgreSQL's dialect; False for what does not parse."""
    try:
        select = sqlglot.parse_one(f"SELECT {sql}", read="postgres")
    except SqlglotError:
        return False
    if not isinstance(select, exp.Select) or len(select.selects) != 1:
        return False
    return is_constant(select.selects[0])
