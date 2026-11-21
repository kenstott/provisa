# Copyright (c) 2026 Kenneth Stott
# Canary: bb814c71-2a1d-4f84-bc29-515176749017
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The first governance rule: every object a statement touches is in the person's mask
(REQ-2219).

The mask is narrowed into the role's governance context where the context is read
(:func:`narrow_governance`), so what cannot be named is what cannot be read: a star reads the
columns the mask leaves. What a statement names outside it is refused here, before any other rule
reads it (:func:`entitlement_violations`). The mask itself is built above the compiler, which
reads it through the methods this module calls.
"""

# Requirements: REQ-2219

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlglot.expressions as exp

from provisa.compiler.sql_validator import ValidationViolation

if TYPE_CHECKING:
    from provisa.compiler.stage2 import GovernanceContext


def narrow_governance(gov: GovernanceContext, mask: Any) -> GovernanceContext:
    """``gov`` narrowed to ``mask``. ``gov`` is its reader's own copy; a full mask leaves it."""
    if mask is None or mask.full:
        return gov
    narrowed: dict[int, frozenset[str] | None] = dict(gov.visible_columns)
    for table_id, columns in gov.all_columns.items():
        if not mask.knows_table(table_id):
            continue
        base = gov.visible_columns.get(table_id)
        names = [n for n, _type in columns] if base is None else base
        narrowed[table_id] = frozenset(n for n in names if mask.has_column(table_id, n))
    gov.visible_columns = narrowed
    gov.entitlement_mask = mask
    return gov


def _statement_ctes(tree: exp.Expr) -> set[str]:  # pyright: ignore[reportPrivateImportUsage]
    return {c.alias_or_name for c in tree.find_all(exp.CTE)}


def _is_cte(node: exp.Table, ctes: set[str]) -> bool:
    return not node.db and node.name in ctes


def _column_violation(
    col: exp.Column, alias_to_tid: dict[str, int], gov: GovernanceContext, mask: Any
) -> ValidationViolation | None:
    from provisa.compiler.stage2 import registered_name

    if col.table:
        tid = alias_to_tid.get(col.table)
        holders = [] if tid is None else [tid]
    else:
        holders = [
            tid
            for tid in dict.fromkeys(alias_to_tid.values())
            if any(
                n == registered_name(gov, tid, col.name) for n, _t in gov.all_columns.get(tid, [])
            )
        ]
    # A table outside the mask is refused as a table; its columns need not be named too.
    holders = [t for t in holders if mask.knows_table(t) and mask.has_table(t)]
    if holders and not any(mask.has_column(t, registered_name(gov, t, col.name)) for t in holders):
        return ValidationViolation(
            "E002", f"Column {col.name!r} is outside this person's entitlements"
        )
    return None


def _scope(select: exp.Select, scopes: dict[int, dict[str, int]]) -> dict[str, int]:
    """Every alias ``select`` reads, its own over any enclosing select's: a correlated subquery
    walks a relationship through an outer alias."""
    outer = select.find_ancestor(exp.Select)
    enclosing = {} if outer is None else _scope(outer, scopes)
    return {**enclosing, **scopes.get(id(select), {})}


def _join_violations(
    select: exp.Select, alias_to_tid: dict[str, int], gov: GovernanceContext, mask: Any
) -> list[ValidationViolation]:
    from provisa.compiler.stage2 import registered_name

    out: list[ValidationViolation] = []
    conditions = [join.args.get("on") for join in select.args.get("joins") or []]
    conditions.append(select.args.get("where"))
    for condition in conditions:
        if condition is None:
            continue
        for eq in condition.find_all(exp.EQ):
            left, right = eq.left, eq.right
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                continue
            a, b = alias_to_tid.get(left.table), alias_to_tid.get(right.table)
            if a is None or b is None:
                continue
            key = (a, b, registered_name(gov, a, left.name), registered_name(gov, b, right.name))
            if mask.knows_relationship(key) and not mask.has_relationship(*key):
                out.append(
                    ValidationViolation(
                        "E003",
                        f"The relationship between {left.sql()} and {right.sql()} is "
                        "outside this person's entitlements",
                    )
                )
    return out


def entitlement_violations(
    written: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    expanded: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    gov: GovernanceContext,
) -> list[ValidationViolation]:
    """Everything the statement touches outside the mask. ``written`` is the statement as the
    client wrote it, ``expanded`` the same with its views replaced by their SQL: the mask must
    hold each view named and each table it reads, and every column the statement itself names."""
    from provisa.compiler.stage2 import _alias_for, _get_tables_from_select, _table_id_for_node

    mask = gov.entitlement_mask
    if mask is None or mask.full:
        return []
    out: list[ValidationViolation] = []
    named: set[tuple[str, str]] = set()
    for tree in (written, expanded):
        ctes = _statement_ctes(tree)
        for node in tree.find_all(exp.Table):
            if _is_cte(node, ctes):
                continue
            tid = _table_id_for_node(node, gov)
            if tid is None or not mask.knows_table(tid) or mask.has_table(tid):
                continue
            if (node.db, node.name) not in named:
                named.add((node.db, node.name))
                out.append(
                    ValidationViolation(
                        "E001", f"Table {node.name!r} is outside this person's entitlements"
                    )
                )
    ctes = _statement_ctes(written)
    scopes = {
        id(select): {
            _alias_for(t): tid
            for t, tid in _get_tables_from_select(select, gov)
            if tid is not None and not _is_cte(t, ctes)
        }
        for select in written.find_all(exp.Select)
    }
    for select in written.find_all(exp.Select):
        alias_to_tid = scopes[id(select)]
        if alias_to_tid:
            for col in select.find_all(exp.Column):
                if col.find_ancestor(exp.Select) is not select:
                    continue
                found = _column_violation(col, alias_to_tid, gov, mask)
                if found is not None and found not in out:
                    out.append(found)
        scope = _scope(select, scopes)
        if scope:
            out.extend(v for v in _join_violations(select, scope, gov, mask) if v not in out)
    out.extend(v for v in _write_violations(written, gov, mask) if v not in out)
    return out


def _write_violations(
    written: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    gov: GovernanceContext,
    mask: Any,
) -> list[ValidationViolation]:
    """The columns a write names outside the mask: its INSERT list, or its SET and WHERE."""
    from provisa.compiler.stage2 import _table_id_for_node, registered_name

    out: list[ValidationViolation] = []
    for write in written.find_all(exp.Insert, exp.Update):
        target = write.find(exp.Table)
        tid = None if target is None else _table_id_for_node(target, gov)
        if tid is None or not mask.has_table(tid):
            continue
        if isinstance(write, exp.Insert):
            schema = write.this if isinstance(write.this, exp.Schema) else None
            names = [] if schema is None else [i.name for i in schema.expressions]
        else:
            names = [
                c.name for c in write.find_all(exp.Column) if c.find_ancestor(exp.Select) is None
            ]
        for name in names:
            if not mask.has_column(tid, registered_name(gov, tid, name)):
                out.append(
                    ValidationViolation(
                        "E002", f"Column {name!r} is outside this person's entitlements"
                    )
                )
    return out
