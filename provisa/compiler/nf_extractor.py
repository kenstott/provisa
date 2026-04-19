# Copyright (c) 2026 Kenneth Stott
# Canary: 7e37b8c8-4be2-4dbe-8f10-41a1fa44cc69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extract _nf_* native filter conditions from SQL WHERE clauses.

Native filter columns use the _nf_ prefix convention (e.g. _nf_id, _nf_status).
In SQL and Cypher queries they appear as ordinary WHERE conditions; this module
strips them out before Trino execution and returns them as api_args for the
REST call Phase 1.

Handles both literal values and $N positional params (postgres dialect).
After extraction, remaining $N references are renumbered contiguously.
"""
from __future__ import annotations

import re
from typing import Any

import sqlglot
import sqlglot.expressions as exp

_NF_PREFIX = "_nf_"
_PARAM_RE = re.compile(r"\$(\d+)")


def _flatten_and(expr: exp.Expression) -> list[exp.Expression]:
    if isinstance(expr, exp.And):
        return _flatten_and(expr.left) + _flatten_and(expr.right)
    return [expr]


def _rebuild_and(conditions: list[exp.Expression]) -> exp.Expression | None:
    if not conditions:
        return None
    result = conditions[0]
    for c in conditions[1:]:
        result = exp.And(this=result, expression=c)
    return result


def _resolve_value(val_expr: exp.Expression, params: list[Any]) -> tuple[Any, int | None]:
    """Return (value, param_index_1based) from a value expression.

    param_index is set when the value came from a $N placeholder; None for literals.
    """
    if isinstance(val_expr, exp.Literal):
        if val_expr.is_string:
            return val_expr.this, None
        raw = val_expr.this
        return (float(raw) if "." in raw else int(raw)), None
    if isinstance(val_expr, exp.Parameter):
        try:
            idx = int(str(val_expr.this))
            value = params[idx - 1] if 0 < idx <= len(params) else None
            return value, idx
        except (ValueError, TypeError):
            pass
    return None, None


def extract_nf_args(sql: str, params: list[Any]) -> tuple[str, list[Any], dict[str, Any]]:
    """Extract _nf_* WHERE conditions from SQL (postgres dialect).

    Returns (clean_sql, clean_params, nf_args) where:
    - clean_sql has _nf_* conditions removed and $N refs renumbered
    - clean_params has consumed positional params removed
    - nf_args maps bare param name (e.g. "id") to its value
    """
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return sql, params, {}

    where = ast.find(exp.Where)
    if where is None:
        return sql, params, {}

    conditions = _flatten_and(where.this)
    nf_args: dict[str, Any] = {}
    consumed_indices: set[int] = set()
    keep: list[exp.Expression] = []

    for cond in conditions:
        nf_col: str | None = None
        val_expr: exp.Expression | None = None

        if isinstance(cond, exp.EQ):
            left, right = cond.left, cond.right
            if isinstance(left, exp.Column) and left.name.startswith(_NF_PREFIX):
                nf_col, val_expr = left.name[len(_NF_PREFIX):], right
            elif isinstance(right, exp.Column) and right.name.startswith(_NF_PREFIX):
                nf_col, val_expr = right.name[len(_NF_PREFIX):], left

        if nf_col is not None and val_expr is not None:
            value, param_idx = _resolve_value(val_expr, params)
            if value is not None or param_idx is not None:
                nf_args[nf_col] = value
                if param_idx is not None:
                    consumed_indices.add(param_idx)
                continue

        keep.append(cond)

    if not nf_args:
        return sql, params, {}

    new_condition = _rebuild_and(keep)
    if new_condition is None:
        where.pop()
    else:
        where.set("this", new_condition)

    clean_sql = ast.sql(dialect="postgres")

    clean_params = [v for i, v in enumerate(params, 1) if i not in consumed_indices]

    if consumed_indices and clean_params:
        old_to_new: dict[int, int] = {}
        new_idx = 1
        for old_idx in range(1, len(params) + 1):
            if old_idx not in consumed_indices:
                old_to_new[old_idx] = new_idx
                new_idx += 1
        clean_sql = _PARAM_RE.sub(lambda m: f"${old_to_new.get(int(m.group(1)), int(m.group(1)))}", clean_sql)

    return clean_sql, clean_params, nf_args


def find_api_table_names(sql: str) -> list[str]:
    """Return table names referenced in FROM/JOIN clauses of a SQL string."""
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return []
    return [tbl.name for tbl in ast.find_all(exp.Table) if tbl.name]
