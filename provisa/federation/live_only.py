# Copyright (c) 2026 Kenneth Stott
# Canary: df6f1d92-0d5d-4288-986d-9479e9edbfd8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A live-only source's part of a statement the engine answers (REQ-2000).

A live-only source type (``core.source_registry.LIVE_ONLY_TYPES``) has no replica. When a statement
reading one of its tables is answered by the engine -- it joins another source, or computes over
one of the table's columns, as a mask does -- the table's part of the statement is cut from it and
read from the source for that statement alone, through the source's direct driver, and its rows
handed to the engine as a search table's are. The part is the table's columns, the conditions of
the statement on the table alone (its row rules among them), and, where the table is read alone
in its SELECT, that SELECT's similarity ordering and LIMIT. A part with no condition and no top-k
would read the whole table, a replica by another name, and is refused, as is a part the source's
driver cannot state exactly: never replicated.

Stage 2 reads every registered table through the role's governed relation (REQ-039,
``compiler.governed_relation``), so the table stands in the statement inside a derived table and
the statement's conditions and top-k sit around it. The part is cut where the governed relation
stands: its row rules, and the statement's conditions and top-k on it read through it as the
table's columns. A condition or ordering on a column the relation serves masked or computed is not
one on the table's column, and is not the part's.
"""

# Requirements: REQ-2000

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

from provisa.compiler.governed_relation import ReadThrough, relation_of

_SIMILARITY = "cosine_similarity"


class LiveReadRefused(ValueError):
    """A live-only table's part of a statement that cannot be read live, by the table's name."""

    def __init__(self, table: str, reason: str) -> None:
        super().__init__(f"Table {table!r} is read live from its source: {reason}")
        self.table = table
        self.reason = reason


@dataclass(frozen=True)
class Part:
    """The statement's read of one live-only table: a single-table SELECT in PostgreSQL text."""

    table: dict
    sql: str


def live_only_tables(state: Any, table_ids: tuple[int, ...]) -> list[dict]:
    """The tables the statement reads whose source type is live-only, except search tables (read
    under their arguments, ``api.data.materialization``)."""
    from provisa.core.source_registry import is_live_only

    ids = set(table_ids)
    types = getattr(state, "source_types", None) or {}
    return [
        t
        for t in getattr(state, "tables", None) or []
        if t.get("id") in ids
        and is_live_only(types.get(t["source_id"]))
        and not t.get("query_template")
    ]


def _joins(select: exp.Select) -> list[exp.Join]:
    return list(select.args.get("joins") or [])


_Place = tuple[exp.Select, exp.Expr, exp.Join | None, ReadThrough | None]


def _place(table: exp.Table) -> _Place | None:
    """Where ``table`` stands in its SELECT: the SELECT, the FROM or JOIN item that reads it (the
    table itself, or the governed relation that reads it), the join (None: the FROM), and the
    governed relation (None: the table stands as itself)."""
    relation = relation_of(table)
    item: exp.Expr = table if relation is None else relation.relation
    holder = item.parent
    if isinstance(holder, exp.From) and isinstance(holder.parent, exp.Select):
        return holder.parent, item, None, relation
    if isinstance(holder, exp.Join) and isinstance(holder.parent, exp.Select):
        if holder.this is item:
            return holder.parent, item, holder, relation
    return None


def _nullable(select: exp.Select, join: exp.Join | None) -> bool:
    """Whether the table read at ``join`` (None: the FROM) is on the side an outer join pads."""
    sides = [(j.side or "").upper() for j in _joins(select)]
    if "FULL" in sides:
        return True
    if join is None:
        return "RIGHT" in sides
    return (join.side or "").upper() == "LEFT"


def _conjuncts(node: exp.Expr | None) -> list[exp.Expr]:
    if node is None:
        return []
    if isinstance(node, exp.And):
        return _conjuncts(node.left) + _conjuncts(node.right)
    if isinstance(node, exp.Paren):
        return _conjuncts(node.this)
    return [node]


def _alone(node: exp.Expr, alias: str, single: bool) -> bool:
    """Whether ``node`` reads the table under ``alias`` and nothing else, with no parameter (the
    engine binds those) and no subquery."""
    columns = list(node.find_all(exp.Column))
    if not columns:
        return False
    if node.find(exp.Placeholder) is not None or node.find(exp.Parameter) is not None:
        return False
    if node.find(exp.Select) is not None:
        return False
    return all(c.table == alias or (not c.table and single) for c in columns)


def _unqualified(node: exp.Expr) -> exp.Expr:
    copy = node.copy()
    for column in copy.find_all(exp.Column):
        column.set("table", None)
    return copy


def _through(node: exp.Expr, relation: ReadThrough | None) -> exp.Expr | None:
    """``node``, a condition or ordering over the item read under ``relation``'s alias, over the
    table's own columns, unqualified: each column the relation serves as it is read as the table's
    column. None when it reads a column the relation serves masked or computed."""
    if relation is None:
        return _unqualified(node)
    copy = node.copy()
    for column in list(copy.find_all(exp.Column)):
        physical = relation.column(column.name)
        if physical is None:
            return None
        column.replace(exp.column(physical, quoted=True))
    return copy


def _top_k(select: exp.Select) -> tuple[exp.Expr, exp.Expr] | None:
    """The SELECT's ``ORDER BY cosine_similarity(...) DESC`` and LIMIT, when it reads one table
    with nothing between them and the rows it reads."""
    if _joins(select):
        return None
    for part in ("group", "having", "distinct", "windows", "offset"):
        if select.args.get(part):
            return None
    if select.find(exp.AggFunc) is not None:
        return None
    order, limit = select.args.get("order"), select.args.get("limit")
    if order is None or limit is None or len(order.expressions) != 1:
        return None
    ordered = order.expressions[0]
    call = ordered.this
    if not (
        isinstance(call, exp.Anonymous)
        and str(call.this).lower() == _SIMILARITY
        and ordered.args.get("desc")
    ):
        return None
    return ordered, limit.expression


def cut(sql: str, table: dict) -> Part:
    """``table``'s part of the governed physical ``sql``, or :class:`LiveReadRefused`."""
    name = table["table_name"]
    tree = sqlglot.parse_one(sql, read="postgres")
    places = [
        place
        for tbl in tree.find_all(exp.Table)
        if tbl.name == name
        for place in [_place(tbl)]
        if place is not None
    ]
    if len(places) != 1:
        raise LiveReadRefused(name, "the statement reads the table more than once")
    select, item, join, relation = places[0]
    alias = item.alias_or_name
    single = not _joins(select)
    if _nullable(select, join):
        held = _conjuncts(join.args.get("on")) if join is not None else []
    else:
        # An inner join's ON condition holds as a WHERE condition does.
        held = _conjuncts((select.args.get("where") or exp.Where()).this)
        for inner in _joins(select):
            if not inner.side:
                held += _conjuncts(inner.args.get("on"))
    pushed = [_through(c, relation) for c in held if _alone(c, alias, single)]
    # The relation's row rules hold of every row it reads, padded or not.
    rules = [] if relation is None else relation.predicates
    pushed += [_unqualified(c) for r in rules for c in _conjuncts(r) if _alone(c, alias, True)]
    pushed = [c for c in pushed if c is not None]
    top = _top_k(select) if not _nullable(select, join) else None
    if top is not None:
        ordered = _through(top[0], relation)
        top = None if ordered is None else (ordered, top[1])
    if not pushed and top is None:
        raise LiveReadRefused(
            name, "the statement reads it with no condition, key or top-k, the whole table"
        )
    columns = [
        exp.column(c["column_name"], quoted=True)
        for c in table["columns"]
        if c.get("native_filter_type") is None
    ]
    part = exp.select(*columns).from_(exp.table_(name, quoted=True))
    if pushed:
        part = part.where(exp.and_(*pushed))
    if top is not None:
        ordered, limit = top
        part = part.order_by(ordered).limit(limit)
    return Part(table, part.sql(dialect="postgres"))


async def read(state: Any, part: Part) -> Any:
    """``part`` read from its source through the source's direct driver, as the rows a search
    table lands (``mongodb.pipeline_table.PipelineRead``)."""
    from provisa.core.ir_types import to_ir
    from provisa.mongodb.pipeline_table import PipelineRead

    table = part.table
    driver = state.source_pools.get(table["source_id"])
    reason = await driver.reason(part.sql, table)
    if reason is not None:
        raise LiveReadRefused(table["table_name"], reason)
    result = await driver.execute(part.sql)
    names = result.column_names
    columns = {c["column_name"]: c for c in table["columns"]}
    return PipelineRead(
        rows=[dict(zip(names, row)) for row in result.rows],
        columns=[(n, to_ir(columns[n]["data_type"])) for n in names],
    )
