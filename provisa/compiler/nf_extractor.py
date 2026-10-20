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
strips them out before the engine execution and returns them as api_args for the
REST call Phase 1.

Handles both literal values and $N positional params (postgres dialect).
After extraction, remaining $N references are renumbered contiguously.
"""

# Requirements: REQ-009, REQ-264, REQ-301

from __future__ import annotations

import functools
import re
from collections.abc import Collection, Mapping
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


_NOT_LITERAL = object()


def _literal(val_expr: exp.Expr) -> Any:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """A literal's value -- a string, a number, a negated number -- or ``_NOT_LITERAL``."""
    if isinstance(val_expr, exp.Neg) and isinstance(val_expr.this, exp.Literal):
        inner = _literal(val_expr.this)
        return -inner if isinstance(inner, (int, float)) else _NOT_LITERAL
    if isinstance(val_expr, exp.Literal):
        if val_expr.is_string:
            return val_expr.this
        raw = val_expr.this
        return float(raw) if "." in raw or "e" in raw.lower() else int(raw)
    return _NOT_LITERAL


def _resolve_value(val_expr: exp.Expr, params: list[Any]) -> tuple[Any, int | None]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Return (value, param_index_1based) from a value expression.

    param_index is set when the value came from a $N placeholder; None for literals.
    """
    literal = _literal(val_expr)
    if literal is not _NOT_LITERAL:
        return literal, None
    if isinstance(val_expr, exp.Array):
        # REQ-1980: an array literal (a query vector, ARRAY[0.1, -0.2]) is a list of its items.
        items = [_literal(item) for item in val_expr.expressions]
        if all(item is not _NOT_LITERAL for item in items):
            return items, None
        return None, None
    if isinstance(val_expr, exp.Parameter):
        try:
            idx = int(str(val_expr.this))
            value = params[idx - 1] if 0 < idx <= len(params) else None
            return value, idx
        except (ValueError, TypeError):
            pass
    return None, None


def extract_nf_args(
    sql: str,
    params: list[Any],
    parameters: Collection[str],
    frozen: Mapping[str, str | None] | None = None,
) -> tuple[str, list[Any], dict[str, Any]]:  # REQ-301, REQ-1986, REQ-2031
    """Extract _nf_* WHERE conditions from SQL (postgres dialect).

    ``parameters`` are the parameter columns of the statement's context
    (``CompilationContext.native_filter_columns``): only a condition on one of them is a native
    argument. A ``_nf_`` column that is no parameter there -- a generated table's parameter in a
    Test (synthetic) environment, an ordinary column of its copy (REQ-1942) -- stays a filter.

    ``frozen``: the parameters of a table kept for good, each with the landed column its value
    filters (``materialization.frozen_parameter_filters``, REQ-2031). Its rows are landed, never
    fetched under an argument, so a condition on one is kept as a filter on that column; one with
    no such column is refused by name.

    Returns (clean_sql, clean_params, nf_args) where:
    - clean_sql has _nf_* conditions removed and $N refs renumbered
    - clean_params has consumed positional params removed
    - nf_args maps bare param name (e.g. "id") to its value
    """
    # Parse failure must fail loud: returning input skips NF-arg extraction.
    # A statement that names no ``_nf_`` column has nothing to extract: the loop below only ever
    # collects a condition whose column name starts with the prefix, and that name is in the text.
    if _NF_PREFIX not in sql:
        return sql, params, {}
    ast = sqlglot.parse_one(sql, dialect="postgres")

    where = ast.find(exp.Where)
    if where is None:
        return sql, params, {}

    landed = {k.lower(): v for k, v in (frozen or {}).items()}
    renamed = False
    for column in where.find_all(exp.Column):
        if column.name.lower() not in landed:
            continue
        target = landed[column.name.lower()]
        if target is None:
            raise ValueError(
                f"{column.name} is a parameter of a table read from its landed rows, and they hold"
                f" no column {column.name[len(_NF_PREFIX) :]!r} for it to filter"
            )
        column.set("this", exp.to_identifier(target, quoted=True))
        renamed = True
    known = {p.lower() for p in parameters if p.startswith(_NF_PREFIX)} - set(landed)
    conditions = _flatten_and(where.this)
    nf_args: dict[str, Any] = {}
    consumed_indices: set[int] = set()
    keep: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

    for cond in conditions:
        nf_col: str | None = None
        val_expr: exp.Expr | None = None  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__

        if isinstance(cond, exp.EQ):
            left, right = cond.left, cond.right
            if isinstance(left, exp.Column) and left.name.lower() in known:
                nf_col, val_expr = left.name[len(_NF_PREFIX) :], right
            elif isinstance(right, exp.Column) and right.name.lower() in known:
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
        return (ast.sql(dialect="postgres") if renamed else sql), params, {}

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


#: The suffix a read of a parameter table is renamed with when a statement reads that table more
#: than once under arguments of its own (:func:`separate_parameter_reads`).
_READ_SUFFIX = re.compile(r"__nf\d+$")


def read_base(table_name: str) -> str:
    """The registered table a read named by :func:`separate_parameter_reads` reads."""
    return _READ_SUFFIX.sub("", table_name)


def _own_sources(select: exp.Select) -> dict[str, exp.Table]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """The tables ``select`` itself reads in its FROM and JOINs, by the name a column qualifies
    them with (their alias, else their name)."""
    found: dict[str, exp.Table] = {}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    from_ = select.args.get("from") or select.args.get("from_")
    candidates = [from_.this] if from_ is not None else []
    candidates += [join.this for join in select.args.get("joins") or []]
    for node in candidates:
        if isinstance(node, exp.Table) and node.name:
            found[node.alias_or_name] = node
    return found


def separate_parameter_reads(
    sql: str, parameters: Collection[str]
) -> tuple[str, dict[str, tuple[str, dict[str, Any]]]]:  # REQ-1986, REQ-1983
    """A statement that reads parameter tables more than once, each read under literal arguments
    of its own -- the union of several views over one search table, each with its own phrase --
    names each such read for itself: ``<table>__nf<k>``, keeping the name its columns are
    qualified by, and its argument conditions taken out of its own SELECT's WHERE. Returns the
    statement and, for each read so named, (the table it reads, its arguments by bare parameter
    name), for the API stage to read each one under its own arguments.

    A statement with at most one such read is returned as it is, with nothing named:
    :func:`extract_nf_args` takes its arguments, as ever."""
    if _NF_PREFIX not in sql:
        return sql, {}
    known = {p.lower() for p in parameters if p.startswith(_NF_PREFIX)}
    ast = sqlglot.parse_one(sql, dialect="postgres")
    # (the read, its arguments, the conditions holding them, the WHERE they are in)
    reads: list[tuple[exp.Table, dict[str, Any], list[exp.Expr], exp.Where]] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    for select in ast.find_all(exp.Select):
        where = select.args.get("where")
        if where is None:
            continue
        sources = _own_sources(select)
        held: dict[int, tuple[exp.Table, dict[str, Any], list[exp.Expr]]] = {}  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        for cond in _flatten_and(where.this):
            if not isinstance(cond, exp.EQ):
                continue
            column, value = cond.left, cond.right
            if not (isinstance(column, exp.Column) and column.name.lower() in known):
                column, value = cond.right, cond.left
            if not (isinstance(column, exp.Column) and column.name.lower() in known):
                continue
            literal = _literal(value)
            if literal is _NOT_LITERAL:
                continue
            if column.table:
                table = sources.get(column.table)
            else:
                table = next(iter(sources.values())) if len(sources) == 1 else None
            if table is None:
                continue
            entry = held.setdefault(id(table), (table, {}, []))
            entry[1][column.name[len(_NF_PREFIX) :]] = literal
            entry[2].append(cond)
        reads += [(table, args, conds, where) for table, args, conds in held.values()]
    if len(reads) < 2:
        return sql, {}
    named: dict[str, tuple[str, dict[str, Any]]] = {}
    for k, (table, args, conds, where) in enumerate(reads, start=1):
        base = table.name
        if not table.alias:
            # Its columns are qualified by its name: it keeps that name as its alias.
            table.set("alias", exp.TableAlias(this=exp.to_identifier(base)))
        name = f"{base}__nf{k}"
        table.set("this", exp.to_identifier(name, quoted=table.this.quoted))
        named[name] = (base, args)
        keep = [c for c in _flatten_and(where.this) if all(c is not held_c for held_c in conds)]
        rebuilt = _rebuild_and(keep)
        if rebuilt is None:
            where.pop()
        else:
            where.set("this", rebuilt)
    return ast.sql(dialect="postgres"), named


@functools.lru_cache(maxsize=4096)
def _table_names(sql: str) -> tuple[str, ...]:
    ast = sqlglot.parse_one(sql, dialect="postgres")
    return tuple(tbl.name for tbl in ast.find_all(exp.Table) if tbl.name)


def find_api_table_names(sql: str) -> list[str]:  # REQ-599
    """Return table names referenced in FROM/JOIN clauses of a SQL string. A function of the text
    alone, asked on every execution of a statement (``would_materialize_optimize``), so the parse
    is kept per text."""
    return list(_table_names(sql))


def left_join_table_names(sql: str) -> set[str]:  # REQ-599
    """Return table names that appear in LEFT JOIN clauses."""
    ast = sqlglot.parse_one(sql, dialect="postgres")
    names: set[str] = set()
    for join in ast.find_all(exp.Join):
        if join.side and join.side.upper() == "LEFT":
            for tbl in join.find_all(exp.Table):
                if tbl.name:
                    names.add(tbl.name)
    return names


def drop_joined_table(sql: str, table_name: str) -> str:  # REQ-264
    """Remove any JOIN for *table_name* (any join type) and NULL-out its SELECT-list columns."""
    # Parse failure must fail loud: returning input skips the REQ-264 governance transform.
    tree = sqlglot.parse_one(sql, dialect="postgres")

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


def drop_union_branches_for_table(sql: str, table_name: str) -> str:  # REQ-599
    """Remove every UNION branch whose FROM clause references *table_name*.

    Works at any nesting depth (including inside CTEs).  Used when a GQL-remote
    table with unsatisfied required_args cannot be dropped as a JOIN.
    """
    # Parse failure must fail loud: returning input skips the union-branch governance transform.
    tree = sqlglot.parse_one(sql, dialect="postgres")

    def _has_from_table(select: exp.Select) -> bool:  # pyright: ignore[reportPrivateImportUsage]
        return any(t.name == table_name for t in select.find_all(exp.Table))

    root = tree
    modified = False

    # Process nested unions before the unions that contain them: `_has_from_table`
    # recurses into subqueries, so a branch that wraps a nested UNION (e.g. a
    # relationship-type union nested inside an outer Cypher UNION ALL branch)
    # would otherwise look like a whole-branch match on *table_name* the moment
    # *any* inner arm references it, dropping the entire outer branch instead of
    # just the one inner arm. Innermost-first ensures the inner arm is pruned
    # first, so the outer branch no longer references the table by the time it
    # is checked.
    def _depth(node: exp.Expression) -> int:
        depth = 0
        parent = node.parent
        while parent is not None:
            depth += 1
            parent = parent.parent
        return depth

    unions_deepest_first = sorted(tree.find_all(exp.Union), key=_depth, reverse=True)
    for union in unions_deepest_first:
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


def apply_dropped_tables(sql: str, dropped: dict[str, str]) -> str:  # REQ-599
    """Drop UNION branches for each table in *dropped*; raise the table's own reason for any
    that survive.

    ``dropped`` maps a table the query cannot address (unsatisfied required_args / path params)
    to a human reason; an unreachable remote or a failed fetch is never here -- it fails the query
    (REQ-1661, amended 2026-09-30). A table referenced inside a UNION
    (a relationship join, a multi-label sweep) is simply excluded from the sweep — the ordinary
    REQ-848/REQ-941 behavior. A table that is the SOLE FROM target has no branch to drop, would
    otherwise reach the engine unqualified, and surfaces as an opaque native catalog error
    instead of the caller-facing reason recorded here — so it raises loud instead (matches the
    pgwire ``_optimize_and_route`` no-op detection, generalized to every settled-pipeline caller).
    """
    for table_name, reason in dropped.items():
        sql = drop_union_branches_for_table(sql, table_name)
        if table_name in find_api_table_names(sql):
            raise ValueError(f"Table {table_name!r} {reason}")
    return sql


def where_referenced_tables(sql: str) -> set[str]:  # REQ-599
    """Return table names (or aliases) that appear in WHERE predicates (not JOIN ON conditions)."""
    ast = sqlglot.parse_one(sql, dialect="postgres")
    names: set[str] = set()
    for where in ast.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            tbl = col.table
            if tbl:
                names.add(tbl)
    return names
