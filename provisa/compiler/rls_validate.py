# Copyright (c) 2026 Kenneth Stott
# Canary: 6f1e2a9c-4b7d-4c3e-9a8f-2d5b7e1c0a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Write-time validation of an RLS predicate against the model (REQ-1676).

An RLS rule's ``filter_expr`` is semantic SQL. Before REQ-1676 its first parse was at query time
in :func:`provisa.compiler.rls._qualified_predicate`, and a predicate that did not parse failed
closed for the role: the administrator saw a successful save and every query for that role then
denied. This module is the same parse, run when the rule is saved, plus the resolution the query
path never does — every column must be a column of the table(s) the rule binds to, every table a
subquery reads must be one the model registers, and the expression must be boolean.

A column is named by its exposed SQL name (its alias when one is set, else the SQL-convention
form of the physical name) or by its physical name: both are names the pipeline binds today
(``CompilationContext.physical_to_sql``), and :mod:`provisa.api.admin.column_dependents` scans
for both. A table is named the same way, bare or qualified by its domain's SQL schema name.

Session-variable references (``current_setting('provisa.<var>')``) carry no column and are not
resolved here; they are resolved per request by the pipeline.
"""

# Requirements: REQ-041, REQ-1676

from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from provisa.compiler.naming import apply_sql_name, domain_to_sql_name


@dataclass(frozen=True)
class _Table:
    """A registered table as the validator sees it: its accepted names and its columns."""

    label: str  # the name reported in an error message
    names: frozenset[str]  # bare names this table answers to
    schemas: frozenset[str]  # qualifiers this table answers to (domain SQL schema, domain id)
    columns: dict[str, str] = field(default_factory=dict)  # accepted column name → data_type
    boolean_columns: frozenset[str] = frozenset()


def _table_from_row(row: dict) -> _Table:
    exposed = row.get("alias") or row["table_name"]
    names = {exposed, row["table_name"], apply_sql_name(exposed)}
    domain_id = row.get("domain_id") or ""
    schemas = {domain_id, domain_to_sql_name(domain_id)} if domain_id else set()
    columns: dict[str, str] = {}
    booleans: set[str] = set()
    for col in row.get("columns") or []:
        phys = col["column_name"]
        dtype = (col.get("data_type") or "").lower()
        for name in {phys, col.get("alias") or apply_sql_name(phys)}:
            columns[name] = dtype
            if dtype in ("boolean", "bool"):
                booleans.add(name)
    return _Table(
        label=exposed,
        names=frozenset(names),
        schemas=frozenset(schemas),
        columns=columns,
        boolean_columns=frozenset(booleans),
    )


def _resolve_table(ref: exp.Table, registry: list[_Table]) -> _Table | None:
    for t in registry:
        if ref.name in t.names and (not ref.db or ref.db in t.schemas):
            return t
    return None


def _unwrap(node: exp.Expression) -> exp.Expression:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _is_boolean(node: exp.Expression, targets: list[_Table]) -> bool:
    node = _unwrap(node)
    if isinstance(node, (exp.Predicate, exp.Connector, exp.Boolean)):
        return True
    if isinstance(node, exp.Not):
        return True
    if isinstance(node, exp.Cast):
        return node.to.this == exp.DataType.Type.BOOLEAN
    if isinstance(node, exp.Column) and not node.table:
        return all(node.name in t.boolean_columns for t in targets)
    return False


def _enclosing_select(node: exp.Expression, root: exp.Expression) -> exp.Select | None:
    """The nearest SELECT above ``node`` inside the predicate, or None at predicate top level."""
    parent = node.parent
    while parent is not None and parent is not root:
        if isinstance(parent, exp.Select):
            return parent
        parent = parent.parent
    return None


def _scope_of(select: exp.Select, registry: list[_Table]) -> dict[str, _Table]:
    """Qualifier → table for every relation a subquery SELECT reads."""
    scope: dict[str, _Table] = {}
    for ref in select.find_all(exp.Table):
        if ref.parent_select is not select:
            continue
        resolved = _resolve_table(ref, registry)
        if resolved is None:
            continue
        scope[ref.alias or ref.name] = resolved
    return scope


def _target_for_qualifier(qualifier: str, targets: list[_Table]) -> _Table | None:
    for t in targets:
        if qualifier in t.names:
            return t
    return None


def _check_column_in_all(col_name: str, targets: list[_Table]) -> str | None:
    for t in targets:
        if col_name not in t.columns:
            return f"column {col_name!r} is not a column of {t.label!r}"
    return None


def _check_column(  # allow-cc: one branch per scope the column can bind in
    col: exp.Column,
    root: exp.Expression,
    targets: list[_Table],
    registry: list[_Table],
) -> str | None:
    select = _enclosing_select(col, root)
    scope = _scope_of(select, registry) if select is not None else {}
    name = col.name
    if col.table:
        bound = scope.get(col.table) or _target_for_qualifier(col.table, targets)
        if bound is None:
            return f"column qualifier {col.table!r} names no table the predicate can see"
        if name not in bound.columns:
            return f"column {name!r} is not a column of {bound.label!r}"
        return None
    if scope and any(name in t.columns for t in scope.values()):
        return None
    return _check_column_in_all(name, targets)


def validate_rls_predicate(  # REQ-1676
    filter_expr: str,
    targets: list[dict],
    registry: list[dict],
) -> str | None:
    """Return why ``filter_expr`` cannot be saved as an RLS rule, or None when it can.

    ``targets`` are the registered-table rows (with ``columns``) the rule binds to — one for a
    table-level rule, every table of the domain for a domain-level rule. A bare column must exist
    on every target because a domain rule is applied to every table of the domain. ``registry``
    is every registered table, for the tables a subquery may read.
    """
    if not filter_expr or not filter_expr.strip():
        return "RLS predicate is empty"
    try:
        parsed = sqlglot.parse_one(filter_expr, read="postgres")
    except SqlglotError as e:
        return f"RLS predicate does not parse: {e}"
    if parsed is None:
        return "RLS predicate is empty"
    tree = cast("exp.Expression", parsed)  # sqlglot stub types parse_one as Expr
    if isinstance(tree, (exp.Select, exp.Union, exp.Command, exp.Insert, exp.Update, exp.Delete)):
        return f"RLS predicate must be a boolean expression, not a {type(tree).__name__.upper()}"

    target_tables = [_table_from_row(r) for r in targets]
    registry_tables = [_table_from_row(r) for r in registry]

    if not _is_boolean(tree, target_tables):
        return (
            "RLS predicate must be a boolean expression, got "
            f"{type(_unwrap(tree)).__name__}: {_unwrap(tree).sql(dialect='postgres')}"
        )

    defined = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
    for ref in tree.find_all(exp.Table):
        if ref.name in defined:
            continue
        if _resolve_table(ref, registry_tables) is None:
            shown = f"{ref.db}.{ref.name}" if ref.db else ref.name
            return f"RLS predicate reads table {shown!r}, which the model does not register"

    for col in tree.find_all(exp.Column):
        problem = _check_column(col, tree, target_tables, registry_tables)
        if problem is not None:
            return f"RLS predicate: {problem}"
    return None
