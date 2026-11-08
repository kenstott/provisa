# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6f2a84-7c31-4e59-b1a6-e4c89f15d372
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The type of a statement's uncast ``$N``, as PostgreSQL resolves it (REQ-2167): an unknown-typed
parameter takes the type of what it meets. Compared with a column or any typed expression, it is
that type; an assigned or inserted value is its column's; alone in a select list it is text.

Derived from registered metadata beside the result columns (:mod:`provisa.compiler.result_shape`).
A parameter the statement casts is the cast's, which it says itself, and one nothing here types is
left out: what asked decides what such a parameter is."""

# Requirements: REQ-2167

from __future__ import annotations

from typing import Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.compiler.result_shape import (
    UnderivableColumn,
    _lookup,
    _resolve_table,
    registered_money,
    table_columns,
    typed_statement,
)
from provisa.transpiler.native_subtype import COMPARISONS
from provisa.transpiler.pg_money import money_type, statement_columns

_PATTERNS = (exp.Like, exp.ILike)


def _index(parameter: exp.Parameter) -> int | None:
    number = parameter.this
    return int(number.this) if isinstance(number, exp.Literal) and number.is_int else None


def _met_by(parameter: exp.Expr) -> exp.Expr | None:
    """What ``parameter`` is compared with; None where it is compared with nothing."""
    node = parameter
    parent = node.parent
    while isinstance(parent, exp.Paren):
        node, parent = parent, parent.parent
    if isinstance(parent, (*COMPARISONS, *_PATTERNS)):
        return parent.expression if parent.this is node else parent.this
    if isinstance(parent, (exp.Between, exp.In)) and parent.this is not node:
        return parent.this
    return None


def _in_select_list(parameter: exp.Expr) -> bool:
    parent = parameter.parent
    if isinstance(parent, exp.Alias):
        parent = parent.parent
    return isinstance(parent, exp.Select)


def _uncast(tree: exp.Expr) -> list[exp.Parameter]:
    """The statement's parameters that are nowhere cast: a cast of ``$1`` says what ``$1`` is."""
    parameters = list(tree.find_all(exp.Parameter))
    cast = {_index(p) for p in parameters if isinstance(p.parent, exp.Cast)}
    return [p for p in parameters if _index(p) is not None and _index(p) not in cast]


def _query_types(
    tree: exp.Query, table_map: dict[str, int], ctx: Any, column_types: dict
) -> dict[int, str]:
    annotated, registered = typed_statement(tree.copy(), table_map, ctx, column_types)
    money_of = statement_columns(annotated, registered_money(registered))
    types: dict[int, str] = {}
    for parameter in _uncast(annotated):
        index = _index(parameter)
        met = _met_by(parameter)
        if index in types or index is None:
            continue
        if met is None:
            if _in_select_list(parameter):
                types[index] = "text"
            continue
        typed = money_type(met, money_of)
        derived = met.type
        if typed is None and derived is not None and derived.this != exp.DataType.Type.UNKNOWN:
            typed = derived.sql(dialect="postgres")
        if typed is not None:
            types[index] = typed
    return types


def _inserted(tree: exp.Insert, columns: dict[str, str]) -> dict[int, str]:
    """Each parameter of an INSERT's VALUES, as the column it is written to."""
    target = tree.this
    values = tree.expression
    if not isinstance(values, exp.Values):
        return {}
    names = (
        [c.name for c in target.expressions] if isinstance(target, exp.Schema) else list(columns)
    )
    types: dict[int, str] = {}
    for row in values.expressions:
        for name, value in zip(names, row.expressions):
            if isinstance(value, exp.Parameter) and (index := _index(value)) is not None:
                typed = _lookup(columns, name)
                if typed is not None:
                    types.setdefault(index, typed)
    return types


def _write_types(
    tree: exp.Expr, table_map: dict[str, int], ctx: Any, column_types: dict
) -> dict[int, str]:
    target = tree.find(exp.Table)
    table_id = _resolve_table(target, table_map) if target is not None else None
    if table_id is None:
        return {}
    columns = table_columns(table_id, ctx, column_types)
    types = _inserted(tree, columns) if isinstance(tree, exp.Insert) else {}
    for parameter in _uncast(tree):
        index = _index(parameter)
        met = _met_by(parameter)
        if index is not None and isinstance(met, exp.Column):
            typed = _lookup(columns, met.name)
            if typed is not None:
                types.setdefault(index, typed)
    return types


def parameter_types(
    governed_sql: str, table_map: dict[str, int], ctx: Any, column_types: dict
) -> dict[int, str]:
    """``$N`` -> its type, for each uncast parameter of ``governed_sql`` the statement types;
    the arguments are :func:`~provisa.compiler.result_shape.derive_result_shape`'s."""
    if "$" not in governed_sql:
        return {}
    try:
        tree = sqlglot.parse_one(governed_sql, read="postgres")
        if isinstance(tree, exp.Query):
            return _query_types(tree, table_map, ctx, column_types)
        if isinstance(tree, (exp.Insert, exp.Update, exp.Delete)):
            return _write_types(tree, table_map, ctx, column_types)
    except (SqlglotError, UnderivableColumn):
        return {}  # the statement says nothing the registry can read of its parameters
    return {}


__all__ = ["parameter_types"]
