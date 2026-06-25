# Copyright (c) 2026 Kenneth Stott
# Canary: 7e37b8c8-4be2-4dbe-8f10-41a1fa44cc69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extract _nf_* native param conditions from SQL WHERE clauses.

Native param columns use the _nf_ prefix (e.g. _nf_id).
In SQL and Cypher queries they appear as ordinary WHERE conditions; this module
strips them out before Trino execution and returns them as api_args for the
REST call Phase 1.

Handles both literal values and $N positional params (postgres dialect).
After extraction, remaining $N references are renumbered contiguously.
"""

# Requirements: REQ-009, REQ-264, REQ-301

from __future__ import annotations

import re
from typing import Any

import sqlglot
import sqlglot.expressions as exp

_NF_PREFIX = "_nf_"
_PARAM_RE = re.compile(r"\$(\d+)")


def _flatten_and(expr: exp.Expr) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if isinstance(expr, exp.And):
        return _flatten_and(expr.left) + _flatten_and(expr.right)
    return [expr]


def _rebuild_and(conditions: list[exp.Expr]) -> exp.Expr | None:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    if not conditions:
        return None
    result = conditions[0]
    for c in conditions[1:]:
        result = exp.And(this=result, expression=c)
    return result


def _resolve_value(val_expr: exp.Expr, params: list[Any]) -> tuple[Any, int | None]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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


def extract_nf_args(
    sql: str, params: list[Any]
) -> tuple[str, list[Any], dict[str, Any]]:  # REQ-301
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
    keep: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    for cond in conditions:
        nf_col: str | None = None
        val_expr: exp.Expr | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        if isinstance(cond, exp.EQ):
            left, right = cond.left, cond.right
            if isinstance(left, exp.Column) and left.name.startswith(_NF_PREFIX):
                nf_col, val_expr = left.name[len(_NF_PREFIX) :], right
            elif isinstance(right, exp.Column) and right.name.startswith(_NF_PREFIX):
                nf_col, val_expr = right.name[len(_NF_PREFIX) :], left

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
        clean_sql = _PARAM_RE.sub(
            lambda m: f"${old_to_new.get(int(m.group(1)), int(m.group(1)))}", clean_sql
        )

    return clean_sql, clean_params, nf_args


def find_api_table_names(sql: str) -> list[str]:
    """Return table names referenced in FROM/JOIN clauses of a SQL string."""
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return []
    return [tbl.name for tbl in ast.find_all(exp.Table) if tbl.name]


def left_join_table_names(sql: str) -> set[str]:
    """Return table names that appear in LEFT JOIN clauses."""
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return set()
    names: set[str] = set()
    for join in ast.find_all(exp.Join):
        if join.side and join.side.upper() == "LEFT":
            for tbl in join.find_all(exp.Table):
                if tbl.name:
                    names.add(tbl.name)
    return names


def drop_joined_table(sql: str, table_name: str) -> str:  # REQ-264
    """Remove any JOIN for *table_name* (any join type) and NULL-out its SELECT-list columns."""
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return sql

    for select in tree.find_all(exp.Select):
        joins = select.args.get("joins") or []
        pruned_aliases: set[str] = set()

        kept: list[exp.Join] = []
        for join in joins:
            has_target = any(tbl.name == table_name for tbl in join.find_all(exp.Table))
            if not has_target:
                kept.append(join)
                continue
            for tbl in join.find_all(exp.Table):
                alias = tbl.alias or tbl.name
                if alias:
                    pruned_aliases.add(alias)

        if len(kept) == len(joins):
            continue

        select.set("joins", kept)

        if pruned_aliases:
            new_exprs = []
            for expr in select.expressions:
                col = expr.this if isinstance(expr, exp.Alias) else expr
                tbl_ref = col.table if isinstance(col, exp.Column) else ""
                if tbl_ref in pruned_aliases:
                    alias = (
                        expr.alias
                        if isinstance(expr, exp.Alias)
                        else (col.name if isinstance(col, exp.Column) else "")
                    )
                    new_exprs.append(exp.alias_(exp.null(), alias) if alias else expr)
                else:
                    new_exprs.append(expr)
            select.set("expressions", new_exprs)

    return tree.sql(dialect="postgres")


# Keep old name as alias for callers that imported it before the rename.
drop_left_join_table = drop_joined_table


def drop_union_branches_for_table(sql: str, table_name: str) -> str:
    """Remove every UNION branch whose FROM clause references *table_name*.

    Works at any nesting depth (including inside CTEs).  Used when a GQL-remote
    table with unsatisfied required_args cannot be dropped as a JOIN.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return sql

    def _has_from_table(select: exp.Select) -> bool:  # pyright: ignore[reportPrivateImportUsage]
        from_clause = select.args.get("from_") or select.args.get("from")
        if from_clause is None:
            return False
        return any(t.name == table_name for t in from_clause.find_all(exp.Table))

    root = tree
    modified = False
    for union in list(tree.find_all(exp.Union)):
        left, right = union.this, union.expression
        left_match = isinstance(left, exp.Select) and _has_from_table(left)
        right_match = isinstance(right, exp.Select) and _has_from_table(right)
        if left_match and not right_match:
            replacement = right
        elif right_match and not left_match:
            replacement = left
        else:
            continue
        modified = True
        if union.parent is None:
            root = replacement
        else:
            union.replace(replacement)

    if not modified:
        return sql
    return root.sql(dialect="postgres")


def where_referenced_tables(sql: str) -> set[str]:
    """Return table names (or aliases) that appear in WHERE predicates (not JOIN ON conditions)."""
    try:
        ast = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:
        return set()
    names: set[str] = set()
    for where in ast.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            tbl = col.table
            if tbl:
                names.add(tbl)
    return names
