# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3d5e19-c2b8-4f60-9e14-0b6c8d2f5a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Operations on a subtyped column, marked for its native semantics (REQ-2171).

A subtyped column is stored and read as its base (an inet as text). Where a sort, a filter, a
join or one of the type's own operators touches it, what it means is the native type's: an inet
orders by address, not as text; a range overlaps another by its bounds. Each such use of the
column in the physical statement is wrapped in the marker ``provisa.transpiler.native_subtype``
resolves once the routed dialect is known -- into the dialect's native type, an emulation, or a
refusal by name. A column read as a value (a select list, a function's argument) is left as it
is: its value is its text form. This runs in both physical rewrites, beside the datasource cast
(REQ-1965), so it is derived with the governed statement and kept with it.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.core.ir_types import SUBTYPES
from provisa.transpiler.native_subtype import (
    COMPARISONS,
    OPERATORS,
    is_spatial_call,
    marked,
    subtype_marker,
)

# (schema, physical table) lowercased -> {column name as emitted SQL spells it, lowercased: its
# subtype}, for every registered column whose subtype has semantics of its own.
SubtypeColumns = dict[tuple[str, str], dict[str, str]]

# The semantics a comparison, a membership or an ordering on a column changes.
_ORDERED = frozenset({"address", "mac", "range"})


def _resolve(
    col: exp.Column, tables: dict[str, tuple[str, str]], subtype_columns: SubtypeColumns
) -> str | None:
    name = col.name.lower()
    if col.table:
        phys = tables.get(col.table.lower())
        return (subtype_columns.get(phys) or {}).get(name) if phys else None
    # Unqualified: SQL binds it to the one in-scope table that has it.
    owners = [
        subtype_columns[phys][name]
        for phys in set(tables.values())
        if name in subtype_columns.get(phys, {})
    ]
    return owners[0] if len(owners) == 1 else None


def _touches(col: exp.Column, subtype: str) -> bool:
    """Whether ``col`` is used where its subtype's semantics decide the answer."""
    semantics = SUBTYPES[subtype].semantics
    node: exp.Expr = col
    parent = node.parent
    while isinstance(parent, exp.Paren):
        node, parent = parent, parent.parent
    if isinstance(parent, OPERATORS):
        return True
    if semantics == "spatial":
        return is_spatial_call(parent)
    if semantics not in _ORDERED:
        return False
    if isinstance(parent, exp.Ordered):
        return True
    if isinstance(parent, (*COMPARISONS, exp.Between)):
        return True
    return isinstance(parent, exp.In) and parent.this is node


def mark_subtype_operations(sql: str, subtype_columns: SubtypeColumns) -> str:
    """``sql`` (PostgreSQL dialect, physical table names) with every sort, filter, join or
    operator use of a subtyped column wrapped in the native-semantics marker. Idempotent, and a
    statement that names no affected table is returned as given, unparsed."""
    if not subtype_columns:
        return sql
    lowered = sql.lower()
    if not any(table in lowered for (_schema, table) in subtype_columns):
        return sql
    from provisa.compiler.sql_rewrite import _scope_tables

    tree = sqlglot.parse_one(sql, read="postgres")
    if not any(
        (t.db.lower(), t.name.lower()) in subtype_columns for t in tree.find_all(exp.Table) if t.db
    ):
        return sql
    scopes: dict[int, dict[str, tuple[str, str]]] = {}
    changed = False
    for col in list(tree.find_all(exp.Column)):
        if isinstance(col.this, exp.Star) or marked(col.parent) is not None:
            continue
        scope = col.find_ancestor(exp.Select, exp.Update, exp.Delete)
        if scope is None:
            continue
        tables = scopes.setdefault(id(scope), _scope_tables(scope))
        subtype = _resolve(col, tables, subtype_columns)
        if subtype is None or not _touches(col, subtype):
            continue
        col.replace(subtype_marker(col.copy(), subtype))
        changed = True
    return tree.sql(dialect="postgres") if changed else sql


__all__ = ["SubtypeColumns", "mark_subtype_operations"]
