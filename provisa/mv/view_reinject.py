# Copyright (c) 2026 Kenneth Stott
# Canary: 56e34392-9ec2-42bf-951f-afba7638c049
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governed statement answered from a fresh materialized view (REQ-849, REQ-882, REQ-1586).

The one place a materialized view is put back into a statement: the pipeline's optimize stage,
after governance, on the statement lowered to catalog-physical -- every surface's statement,
SQL and compiled alike. Having run after governance, the rewrite sees the role's row filters
and masks as part of the statement: a predicate or a mask over a column the view keeps is
carried onto the view's column; one over anything else leaves the statement to its tables.

Two kinds of view answer:

- A **join-pattern view** holds its left table's columns as they are and each other table's
  as ``"<table>__<column>"`` (``refresh.join_pattern_sql``). A statement whose FROM is the
  view's left table, joined to its right table on the view's columns with the view's join type
  (or through its junction, with its edge-type predicate), reads the view instead: fully, when
  those are its only joins, or partly, keeping its other joins against the view. An inner-join
  view also answers a statement rooted at its right table.
- An **aggregate view**: see ``mv.aggregate_catalog``.

A statement is rewritten only when every column it reads of a covered table is qualified by
that table's alias, and reads none of them through ``*``: anything else is left as it is.

A view's own build is never answered by that view (:func:`building`): its definition is what
it holds, so a build would read back what the view held before it, and an empty first build
would keep it empty.
"""

# Requirements: REQ-849, REQ-882, REQ-1586

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

if TYPE_CHECKING:
    from provisa.compiler.sql_types import CompilationContext
    from provisa.mv.models import MVDefinition

log = logging.getLogger(__name__)

Relation = tuple[str, str, str]  # (catalog, schema, table), lowercased

#: The view whose build the current statement is, if it is one (:func:`building`).
_building: ContextVar[str | None] = ContextVar("view_reinject_building", default=None)


@contextmanager
def building(mv_id: str) -> Iterator[None]:
    """The statements planned in this block are the build of view ``mv_id``: it answers none of
    them."""
    token = _building.set(mv_id)
    try:
        yield
    finally:
        _building.reset(token)


@dataclass(frozen=True)
class Reinjected:
    """A statement rewritten to read a view: its SQL, the view, and how it matched."""

    sql: str
    mv: MVDefinition
    match: str  # "full", "partial", "aggregate"


def _relation(table: exp.Table) -> Relation:
    return (table.catalog.lower(), table.db.lower(), table.name.lower())


def _physical_of(ctx: CompilationContext) -> dict[tuple[str, str, str], Relation]:
    """Each registered table the context names, by its identity (source, schema, registered
    name), as its catalog-physical relation."""
    return {
        (m.source_id, m.schema_name, m.original_table_name or m.table_name): (
            m.catalog_name.lower(),
            m.schema_name.lower(),
            m.table_name.lower(),
        )
        for m in ctx.tables.values()
    }


@dataclass(frozen=True)
class _Join:
    node: exp.Join
    table: exp.Table
    alias: str
    kind: str  # "inner" or "left"
    on: tuple[tuple[str, str], tuple[str, str]] | None  # ((alias, column), (alias, column))


def _kind(join: exp.Join) -> str | None:
    side = (join.side or "").lower()
    kind = (join.kind or "").lower()
    if side == "left" and kind in ("", "outer"):
        return "left"
    if not side and kind in ("", "inner"):
        return "inner"
    return None  # right, full, cross, semi...


def _equi(on: exp.Expression | None) -> tuple[tuple[str, str], tuple[str, str]] | None:
    if not isinstance(on, exp.EQ):
        return None
    left, right = on.this, on.expression
    if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
        return None
    if not (left.table and right.table):
        return None
    return (left.table, left.name.lower()), (right.table, right.name.lower())


def _joins(select: exp.Select) -> list[_Join] | None:
    out: list[_Join] = []
    for join in select.args.get("joins") or []:
        table = join.this
        kind = _kind(join)
        if not isinstance(table, exp.Table) or kind is None or join.args.get("using"):
            return None
        out.append(_Join(join, table, table.alias_or_name, kind, _equi(join.args.get("on"))))
    return out


def _pair(on, a: str, a_col: str, b: str, b_col: str) -> bool:
    """Whether ``on`` equates ``a.a_col`` and ``b.b_col``, either way round."""
    if on is None:
        return False
    return {on[0], on[1]} == {(a, a_col), (b, b_col)}


def _has_type_predicate(select: exp.Select, alias: str, column: str, value: str) -> bool:
    """The statement keeps only the junction rows of the view's edge type."""
    where = select.args.get("where")
    if where is None:
        return False
    conjuncts = list(where.this.flatten()) if isinstance(where.this, exp.And) else [where.this]
    for c in conjuncts:
        if (
            isinstance(c, exp.EQ)
            and isinstance(c.this, exp.Column)
            and c.this.table == alias
            and c.this.name.lower() == column.lower()
            and isinstance(c.expression, exp.Literal)
            and c.expression.is_string
            and c.expression.this == value
        ):
            return True
    return False


def _covered(
    select: exp.Select,
    root: exp.Table,
    joins: list[_Join],
    mv: MVDefinition,
    physical: dict[tuple[str, str, str], Relation],
) -> tuple[dict[str, str], list[int]] | None:
    """For a join-pattern view that answers ``select``: alias -> the table name its columns
    take the prefix of ("" for the view's left table, unprefixed), and the indexes of the joins
    it covers. None when it does not answer."""
    jp = mv.join_pattern
    assert jp is not None
    try:
        bound = dict(zip(mv.source_tables, mv.inputs, strict=True))
        rel = {
            name: physical[(i.source_id, i.schema_name, i.table_name)] for name, i in bound.items()
        }
    except (KeyError, ValueError):
        return None  # a table of the view this role's context does not name
    left, right = rel.get(jp.left_table), rel.get(jp.right_table)
    root_rel, root_alias = _relation(root), root.alias_or_name
    if jp.is_junction:
        via = rel.get(jp.via_table or "")
        if root_rel != left or via is None:
            return None
        for i, first in enumerate(joins):
            if _relation(first.table) != via or first.kind != jp.join_type:
                continue
            if not _pair(first.on, root_alias, jp.left_column.lower(), first.alias,
                         (jp.via_left_column or "").lower()):  # fmt: skip
                continue
            for j, second in enumerate(joins):
                if j == i or _relation(second.table) != right or second.kind != jp.join_type:
                    continue
                if not _pair(second.on, first.alias, (jp.via_right_column or "").lower(),
                             second.alias, jp.right_column.lower()):  # fmt: skip
                    continue
                if jp.via_type_column and not _has_type_predicate(
                    select, first.alias, jp.via_type_column, jp.via_type_value or ""
                ):
                    continue
                return (
                    {root_alias: "", first.alias: jp.via_table or "", second.alias: jp.right_table},
                    [i, j],
                )
        return None
    if root_rel == left:
        far, far_name, near_name = right, jp.right_table, ""
    elif root_rel == right and jp.join_type == "inner" and len(joins) == 1:
        # An inner join reads the same rows from either end; the view's columns keep their
        # names, so the root takes the right table's prefix and the join the left's none.
        far, far_name, near_name = left, "", jp.right_table
    else:
        return None
    for i, join in enumerate(joins):
        if _relation(join.table) != far or join.kind != jp.join_type:
            continue
        near_col = jp.left_column if near_name == "" else jp.right_column
        far_col = jp.right_column if near_name == "" else jp.left_column
        if _pair(join.on, root_alias, near_col.lower(), join.alias, far_col.lower()):
            return {root_alias: near_name, join.alias: far_name}, [i]
    return None


def _rewrite_join(
    select: exp.Select, root: exp.Table, joins: list[_Join], prefixes: dict[str, str],
    covered: list[int], stored: str,
) -> str | None:  # fmt: skip
    """``select`` reading the view at ``stored`` (under the root's alias) for the covered
    tables, or None when a column of one is read unqualified or through ``*``."""
    for star in select.find_all(exp.Star):
        if not isinstance(star.parent, exp.Count):
            return None  # the view's columns are named apart from its tables'
    for column in select.find_all(exp.Column):
        if not column.table and len(joins) > 0:
            return None  # which table it reads is not said
    # Each selected column keeps the name it is read back by, whatever the view calls it.
    select.set(
        "expressions",
        [
            exp.alias_(e, e.name, quoted=True)
            if isinstance(e, exp.Column) and e.table in prefixes
            else e
            for e in select.expressions
        ],
    )
    alias = root.alias_or_name
    for column in list(select.find_all(exp.Column)):
        prefix = prefixes.get(column.table)
        if prefix is None:
            continue
        name = f"{prefix}__{column.name}" if prefix else column.name
        column.replace(exp.column(name, table=alias, quoted=True))
    for i in sorted(covered, reverse=True):
        joins[i].node.pop()
    view = sqlglot.parse_one(stored, into=exp.Table, read="postgres")
    view.set("alias", exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
    root.replace(view)
    return select.sql(dialect="postgres")


def reinject(sql: str, fresh_mvs: list[MVDefinition], ctx: CompilationContext) -> Reinjected | None:
    """``sql`` -- a governed read lowered to catalog-physical -- rewritten to read the fresh
    view that answers it, or None. A join-pattern view that covers every join wins over one
    that covers fewer; among partial ones, the widest; then an aggregate view."""
    from provisa.mv.aggregate_catalog import rewrite_aggregate_query

    # A stale view answers nothing, nor a view its own build.
    built = _building.get()
    fresh_mvs = [mv for mv in fresh_mvs if mv.is_fresh and mv.id != built]
    if not fresh_mvs:
        return None
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except (SqlglotError, RecursionError):
        return None
    if not isinstance(tree, exp.Select):
        return None
    root = tree.args.get("from_")
    root = root.this if root is not None else None
    if not isinstance(root, exp.Table) or not root.catalog:
        return None
    joins = _joins(tree)
    if joins is None:
        return None
    if joins:
        physical = _physical_of(ctx)
        best: tuple[MVDefinition, dict[str, str], list[int]] | None = None
        for mv in fresh_mvs:
            if mv.join_pattern is None:
                continue
            found = _covered(tree, root, joins, mv, physical)
            if found is not None and (best is None or len(found[1]) > len(best[2])):
                best = (mv, *found)
                if len(found[1]) == len(joins):
                    break
        if best is None:
            return None
        mv, prefixes, covered = best
        rewritten = _rewrite_join(tree, root, joins, prefixes, covered, mv.stored_ref())
        if rewritten is None:
            return None
        match = "full" if len(covered) == len(joins) else "partial"
        log.info("view %s answers a statement (%s join match)", mv.id, match)
        return Reinjected(rewritten, mv, match)
    hit = rewrite_aggregate_query(sql, _relation(root), fresh_mvs)
    if hit is None:
        return None
    return Reinjected(hit[0], hit[1], "aggregate")
