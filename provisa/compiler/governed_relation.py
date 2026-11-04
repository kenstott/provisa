# Copyright (c) 2026 Kenneth Stott
# Canary: 3c6f0b52-9e1d-4a87-b4f3-61d2e7a8c915
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading a governed statement through its governed relations (REQ-039).

Stage 2 puts the role's governed relation in the place of every registered table a statement
reads: a derived table, under the table's alias, whose SELECT reads that one table and exposes
only the columns the role is served, masked, with the row rule applied
(``stage2._governed_relation``). The governed statement passes between stages as text, so the
passes after governance -- result typing, row-level bounds, ``_nf_`` extraction, materialized-view
rewrite -- recognise a governed relation by its shape, here, and read its table through it.

The shape: a ``Subquery`` with an alias, whose body is a plain SELECT (no join, grouping,
DISTINCT, window, ordering or limit) of named projections from either the table itself under the
same alias, or another derived table of the same shape under the same alias (the faked and
member projections of the chain stage 2 builds). A derived table a role writes in that shape is
read the same way: it is only ever read here, after stage 2 governed what is inside it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot.expressions as exp

#: What a governed relation's SELECT may not carry: each changes which rows or values it holds.
_NOT_A_RELATION = (
    "joins",
    "laterals",
    "group",
    "having",
    "distinct",
    "order",
    "limit",
    "offset",
    "qualify",
    "windows",
    "with_",
    "kind",
    "pivots",
)


def _folded(identifier: exp.Expr | None) -> str:
    if not isinstance(identifier, exp.Identifier):
        return ""
    return identifier.name if identifier.quoted else identifier.name.lower()


def _alias(node: exp.Expr) -> str:
    alias = node.args.get("alias")
    return _folded(alias.this) if isinstance(alias, exp.TableAlias) else ""


@dataclass
class ReadThrough:
    """A governed relation read as its table.

    ``table`` is the registered table it reads, ``alias`` the name the statement reads it by,
    ``columns`` each exposed column's value over ``table`` (resolved through the chain, so a
    column served as it is reads as a bare column of ``table``), and ``predicates`` the row rule
    each level applies, over ``table``'s columns."""

    relation: exp.Subquery
    table: exp.Table
    alias: str
    columns: dict[str, exp.Expr] = field(default_factory=dict)
    predicates: list[exp.Expr] = field(default_factory=list)

    def column(self, name: str) -> str | None:
        """The physical column of ``table`` the relation exposes as ``name`` unchanged -- served as
        it is, neither masked nor computed. None when it exposes no such column, or a value that is
        not the column's own."""
        value = self.columns.get(name)
        if value is None:
            value = next((v for k, v in self.columns.items() if k.lower() == name.lower()), None)
        if isinstance(value, exp.Column) and not isinstance(value.this, exp.Star):
            return value.name
        return None

    def over_table(self, predicate: exp.Expr, *, sole: bool) -> exp.Expr:
        """``predicate``, read where the relation stands, as a predicate over its table: each
        column of the relation (qualified by its alias, or unqualified when it is the one relation
        its SELECT reads) as the table's column it serves as it is, under the same alias. A column
        it serves masked or computed reads as NULL, so no bound is taken from it -- a mask's value
        is not the column's."""

        def swap(n: exp.Expr) -> exp.Expr:
            if not isinstance(n, exp.Column) or isinstance(n.this, exp.Star):
                return n
            if n.table != self.alias and not (sole and not n.table):
                return n
            physical = self.column(n.name)
            if physical is None:
                return exp.Null()
            return exp.column(physical, table=self.alias)

        return predicate.copy().transform(swap)


def _level(select: exp.Select, alias: str) -> tuple[dict[str, exp.Expr], exp.Expr | None] | None:
    """One level of the chain: its projections by exposed name, and its FROM item. None when the
    SELECT is not a plain projection of one relation under ``alias``."""
    if any(select.args.get(k) for k in _NOT_A_RELATION):
        return None
    source = select.args.get("from_")
    if source is None or not isinstance(source.this, (exp.Table, exp.Subquery)):
        return None
    if _alias(source.this) != alias and not (
        isinstance(source.this, exp.Table) and not _alias(source.this) and source.this.name == alias
    ):
        return None
    projections: dict[str, exp.Expr] = {}
    for projection in select.expressions:
        if isinstance(projection, exp.Alias):
            projections[projection.alias] = projection.this
        elif isinstance(projection, exp.Column) and not isinstance(projection.this, exp.Star):
            projections[projection.name] = projection
        else:
            return None
    return projections, source.this


def read_through(node: exp.Expr | None) -> ReadThrough | None:
    """``node`` read as the registered table its governed relation reads, or None when ``node``
    is not a derived table of that shape."""
    if not isinstance(node, exp.Subquery) or not isinstance(node.this, exp.Select):
        return None
    alias = _alias(node)
    if not alias:
        return None
    levels: list[tuple[dict[str, exp.Expr], exp.Select]] = []
    current: exp.Expr | None = node
    while isinstance(current, exp.Subquery):
        if not isinstance(current.this, exp.Select):
            return None
        level = _level(current.this, alias)
        if level is None:
            return None
        projections, source = level
        levels.append((projections, current.this))
        current = source
    if not isinstance(current, exp.Table) or not current.name:
        return None
    # Innermost first: each level's values and row rule over the base table, an inner level's
    # bare column replaced by the value the level below exposes under its name.
    below: dict[str, exp.Expr] | None = None
    predicates: list[exp.Expr] = []
    for projections, select in reversed(levels):
        where = select.args.get("where")
        if where is not None:
            predicates.append(
                where.this.copy() if below is None else _over(where.this, alias, below)
            )
        below = (
            {name: value.copy() for name, value in projections.items()}
            if below is None
            else {name: _over(value, alias, below) for name, value in projections.items()}
        )
    return ReadThrough(node, current, alias, below or {}, predicates)


def _over(value: exp.Expr, alias: str, below: dict[str, exp.Expr]) -> exp.Expr:
    """``value`` with each column of the level below replaced by what that level exposes."""

    def swap(n: exp.Expr) -> exp.Expr:
        if (
            isinstance(n, exp.Column)
            and not isinstance(n.this, exp.Star)
            and (not n.table or n.table == alias)
            and n.name in below
        ):
            return below[n.name].copy()
        return n

    return value.copy().transform(swap)


def base_table(node: exp.Expr | None) -> exp.Table | None:
    """The table a FROM or JOIN item reads: the item itself, or the table its governed relation
    reads. None for any other item."""
    if isinstance(node, exp.Table):
        return node
    relation = read_through(node)
    return None if relation is None else relation.table


def relation_of(table: exp.Table) -> ReadThrough | None:
    """The outermost governed relation that reads ``table`` as its base, or None when ``table``
    stands in the statement as itself."""
    found: ReadThrough | None = None
    node: exp.Expr | None = table.parent
    while node is not None:
        if isinstance(node, exp.Subquery):
            rt = read_through(node)
            if rt is None or rt.table is not table:
                break
            found = rt
        elif not isinstance(node, (exp.From, exp.Select)):
            break
        node = node.parent
    return found


def relations(tree: exp.Expr) -> list[ReadThrough]:
    """Every governed relation in ``tree``, outermost of each chain only."""
    out: list[ReadThrough] = []
    for table in tree.find_all(exp.Table):
        rt = relation_of(table)
        if rt is not None:
            out.append(rt)
    return out
