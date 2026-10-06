# Copyright (c) 2026 Kenneth Stott
# Canary: c671e2a7-4451-4a31-801c-0f456dea7c06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The row-spanning synthetic rules in generation (REQ-1494, REQ-1939, GENERATION IN PASSES).

A column whose synthetic rule is ``sql_group`` or ``sequence`` first draws its row's value as any
column does -- from its fake, else its profile -- and the rule is then computed over the generated
rows, in the governed dialect:

* ``sql_group(expression)``: the expression over the table's rows, ``self`` naming the drawn
  value; a window over a partition is computed in the table's own statement, and an aggregate
  over a parent's children (``SUM(lines.amount)``, a relationship's name and its child's column)
  joins the generated child table, grouped by the column that refers to the parent;
* ``sequence((states), entity, order)``: each entity's rows, in order, take the states in turn
  from the first; the table's rows are generated with no entity holding more rows than states.

A rule reading children is computed in the second pass, once every table's rows exist; the
columns whose fakes name a rule's column are computed after it, in the same statement.
"""

# Requirements: REQ-1494, REQ-1939

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from provisa.fakes.kinds import Sequence, Sql


@dataclass(frozen=True)
class ChildLink:
    """A child the rule reads by its relationship's name: the parent's column it refers to, and
    the child's column that refers to it."""

    name: str
    child_table: str  # the child's table as the dataset names it
    parent_column: str
    child_column: str


@dataclass(frozen=True)
class GroupColumn:
    name: str
    sql_type: str
    rule: Sql | Sequence
    children: tuple[ChildLink, ...] = ()


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def child_placeholder(child_table: str) -> str:
    """The name a statement gives a generated child table until its engine address is known."""
    return _q(f"__provisa_child__{child_table}")


def reads(g: GroupColumn) -> frozenset[str]:
    """The table's own columns the rule reads (``self`` excluded)."""
    if isinstance(g.rule, Sequence):
        return frozenset({g.rule.entity, g.rule.order})
    tree = sqlglot.parse_one(g.rule.expression, read="postgres")
    return frozenset(c.name for c in tree.find_all(exp.Column) if not c.table and c.name != "self")


def reads_children(g: GroupColumn) -> bool:
    return isinstance(g.rule, Sql) and bool(g.children)


def _sequence_sql(g: GroupColumn, columns: list[str], alias: str) -> str:
    rule = g.rule
    assert isinstance(rule, Sequence)
    a = _q(alias)
    # Ties in the order are broken by every other column, so the same seed gives the same states.
    tie = ", ".join(f"{a}.{_q(c)}" for c in columns if c not in (rule.order, g.name))
    order = f"{a}.{_q(rule.order)}" + (f", {tie}" if tie else "")
    n = f"ROW_NUMBER() OVER (PARTITION BY {a}.{_q(rule.entity)} ORDER BY {order})"
    whens = " ".join(
        f"WHEN {i} THEN '{s.replace(chr(39), chr(39) * 2)}'" for i, s in enumerate(rule.states, 1)
    )
    return f"CAST(CASE {n} {whens} END AS {g.sql_type})"


def _sql_group_sql(g: GroupColumn, alias: str) -> tuple[str, list[str]]:
    """The rule's expression over the level aliased ``alias``, and the joins its aggregates over
    children take."""
    rule = g.rule
    assert isinstance(rule, Sql)
    links = {c.name: c for c in g.children}
    tree = sqlglot.parse_one(rule.expression, read="postgres")
    joins: list[str] = []
    for agg in list(tree.find_all(exp.AggFunc)):
        child_cols = [c for c in agg.find_all(exp.Column) if c.table]
        if not child_cols:
            continue
        names = {c.table for c in child_cols}
        assert len(names) == 1, "an aggregate reads one child"  # the sql subset's check
        link = links[next(iter(names))]
        inner = agg.copy()
        for c in inner.find_all(exp.Column):
            c.set("table", exp.to_identifier("ch"))
        sub = f"__ag{len(joins)}"
        joins.append(
            f"LEFT JOIN (SELECT ch.{_q(link.child_column)} AS {_q('__k')}, "
            f"{inner.sql(dialect='postgres')} AS {_q('__a')} "
            f"FROM {child_placeholder(link.child_table)} AS ch "
            f"GROUP BY ch.{_q(link.child_column)}) AS {_q(sub)} "
            f"ON {_q(alias)}.{_q(link.parent_column)} = {_q(sub)}.{_q('__k')}"
        )
        value: exp.Expression = exp.column("__a", table=sub, quoted=True)
        if isinstance(agg, exp.Count):
            value = exp.Coalesce(this=value, expressions=[exp.Literal.number(0)])
        if agg is tree:
            tree = value  # the rule is the aggregate itself: replacing the root replaces nothing
        else:
            agg.replace(value)
    for c in list(tree.find_all(exp.Column)):
        if c.table:
            continue  # a joined aggregate's column
        name = g.name if c.name == "self" else c.name
        c.replace(exp.column(name, table=alias, quoted=True))
    return f"CAST({tree.sql(dialect='postgres')} AS {g.sql_type})", joins


def group_levels(
    level: str, alias: str, columns: list[str], groups: list[GroupColumn], keep: tuple[str, ...]
) -> str:
    """``level`` with each rule of ``groups`` computed over it in reference order, one level each,
    every column of ``columns`` and ``keep`` carried through. ``groups`` holds only the rules this
    statement computes."""
    done: set[str] = set()
    names = {g.name for g in groups}
    pending = list(groups)
    while pending:
        ready = [g for g in pending if not (reads(g) & names) - done]
        assert ready, "the rules name one another in a cycle"  # refused when they are saved
        for g in ready:
            if isinstance(g.rule, Sequence):
                value, joins = _sequence_sql(g, columns, alias), []
            else:
                value, joins = _sql_group_sql(g, alias)
            a = _q(alias)
            cols = [f"{value} AS {_q(c)}" if c == g.name else f"{a}.{_q(c)}" for c in columns] + [
                f"{a}.{_q(k)}" for k in keep
            ]
            level = f"SELECT {', '.join(cols)} FROM ({level}) AS {a} {' '.join(joins)}".rstrip()
            done.add(g.name)
        pending = [g for g in pending if g.name not in done]
    return level
