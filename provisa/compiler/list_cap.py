# Copyright (c) 2026 Kenneth Stott
# Canary: da4438d5-2281-450e-b596-debb702d8970
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role's row limit bounds every list a read nests, as it bounds the read's rows (REQ-005).

A list a statement aggregates -- ``json_agg`` / ``JSON_ARRAYAGG``: an aggregate or group-by's
nodes, a to-many relationship's rows; ``array_agg``: a Cypher ``collect()``; ``ARRAY(SELECT ...)``:
a Cypher ``COLLECT { }`` -- holds at most the limit's number of entries (a distinct list, of
distinct values). Its counts
and other aggregates stay over every row, and the statement's own LIMIT is governance's as before
(stage2). The rewrite is one, of the governed statement's tree, whatever compiled it:

- A list beside a grouping or other aggregates numbers each group's rows in its source --
  ``ROW_NUMBER() OVER (PARTITION BY <the group's keys>)`` -- and aggregates those within the
  limit: ``json_agg(e) FILTER (WHERE __list_rn <= n)``.
- A list that is its select's one value (a relationship's correlated subquery) reads at most the
  limit's rows: ``(SELECT json_agg(_t) FROM (SELECT e AS _t FROM ... LIMIT n) _sub)``, the
  compiler's own limited form; one already limited is held to the smaller.

- ``ARRAY(SELECT ...)`` reads at most the limit's rows (its own LIMIT, where smaller, stands).

A list in any other shape is refused by name, never left unbounded. A list of exactly the limit's
entries says so, as it may have more (:func:`at_ceiling`, REQ-1350).
"""

# Requirements: REQ-005, REQ-1350

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import sqlglot.expressions as exp

from provisa.core.statement_warnings import ServerWarning

#: The column a capped list's source numbers its rows in.
ROW_NUMBER = "__list_rn"


class ListNotCappable(ValueError):
    """A list the statement aggregates in a shape the cap has no form for; the message names it."""


def _is_list(node: exp.Expr) -> bool:
    """A list aggregate: json_agg / JSON_ARRAYAGG, or array_agg (a Cypher collect())."""
    return isinstance(node, (exp.JSONArrayAgg, exp.ArrayAgg)) or (
        isinstance(node, exp.Anonymous) and node.name.lower() in ("json_arrayagg", "json_agg")
    )


def _array_of_rows(node: exp.Expr) -> exp.Select | None:
    """The rows of ``ARRAY(SELECT ...)`` (a Cypher COLLECT { }), or None for any other node."""
    if isinstance(node, exp.Array) and len(node.expressions) == 1:
        (only,) = node.expressions
        if isinstance(only, exp.Select):
            return only
    return None


def _hold_to(select: exp.Select, limit: int) -> None:
    """``select`` reads at most ``limit`` rows: its own LIMIT, where smaller, stands."""
    own = select.args.get("limit")
    if own is None:
        select.limit(limit, copy=False)
        return
    value = own.expression
    if not (isinstance(value, exp.Literal) and not value.is_string):
        raise ListNotCappable("a list limited by a value not written in it")
    if int(value.name) > limit:
        select.limit(limit, copy=False)


def _own_select(node: exp.Expr) -> exp.Select | None:
    parent = node.parent
    while parent is not None and not isinstance(parent, exp.Select):
        parent = parent.parent
    return parent


def _aggregates_beside(select: exp.Select, lists: list[exp.Expr]) -> bool:
    """Whether ``select`` groups, or aggregates anything but its lists."""
    if select.args.get("group"):
        return True
    for projection in select.expressions:
        for agg in projection.find_all(exp.AggFunc):
            if any(agg is lst or lst in agg.walk() for lst in lists):
                continue
            if _own_select(agg) is select:
                return True
    return False


#: A registered table's columns, as governance knows them; None for a source it does not.
ColumnsOf = Callable[[exp.Table], "list[str] | None"]


def _no_columns(_table: exp.Table) -> list[str] | None:
    return None


def cap_lists(tree: exp.Expr, limit: int, columns_of: ColumnsOf = _no_columns) -> bool:
    """Bound each list ``tree`` aggregates at ``limit`` entries, in place. Returns whether any
    list was bounded. :class:`ListNotCappable` for a list in a shape the cap has no form for.
    ``columns_of`` names a joined table's columns: a list beside other aggregates over joined
    tables reads them from the one source its rows are numbered in."""
    by_select: dict[int, tuple[exp.Select, list[exp.Expr]]] = {}
    arrays = 0
    for node in list(tree.walk()):
        rows = _array_of_rows(node)
        if rows is not None:
            _hold_to(rows, limit)
            arrays += 1
        elif _is_list(node):
            select = _own_select(node)
            if select is None:
                raise ListNotCappable("a list aggregated outside any select")
            by_select.setdefault(id(select), (select, []))[1].append(node)
    for select, lists in by_select.values():
        if _aggregates_beside(select, lists):
            _number_the_source(select, lists, limit, columns_of)
        else:
            _limit_the_rows(select, lists, limit)
    return bool(by_select) or arrays > 0


def _defines(select: exp.Select) -> set[str]:
    """The table aliases ``select``'s FROM and JOINs name."""
    source = select.args.get("from_") or select.args.get("from")
    held = [source.this] if source is not None else []
    held += [j.this for j in select.args.get("joins") or []]
    return {t.alias_or_name for t in held}


def _read_from_one_source(select: exp.Select, columns_of: ColumnsOf) -> list[exp.Expr]:
    """``select``'s FROM and JOINs as one derived source, each table's column ``t.c`` read as
    ``_ls."t__c"``: the columns the numbered source carries. Each reference the select makes to a
    joined table -- correlated subqueries' among them -- is rewritten to it; ``t.*`` is each of
    the table's columns. Returns the source's columns."""
    source = select.args["from_"] if select.args.get("from_") else select.args["from"]
    tables = [source.this, *(j.this for j in select.args.get("joins") or [])]
    carried: list[exp.Expr] = []
    by_alias: dict[str, list[str]] = {}
    for table in tables:
        columns = columns_of(table) if isinstance(table, exp.Table) else None
        if columns is None:
            raise ListNotCappable(
                "a list beside other aggregates over a joined source of no known columns"
            )
        by_alias[table.alias_or_name] = columns
        for column in columns:
            carried.append(
                exp.alias_(
                    exp.column(column, table=table.alias_or_name, quoted=True),
                    f"{table.alias_or_name}__{column}",
                    quoted=True,
                )
            )

    def outer(node: exp.Column) -> bool:
        """Whether ``node``'s table is resolved by ``select`` (not a nested select's own)."""
        parent = node.parent
        while parent is not None:
            if isinstance(parent, exp.Select) and node.table in _defines(parent):
                return parent is select
            parent = parent.parent
        return False

    parts: list[exp.Expr] = list(select.expressions)
    parts += [select.args[k] for k in ("group", "having", "order") if select.args.get(k)]
    found = [
        node
        for part in parts
        for node in part.walk()
        if isinstance(node, exp.Column) and node.table in by_alias and outer(node)
    ]
    for node in found:
        if isinstance(node.this, exp.Star):
            group = node.parent
            if not isinstance(group, exp.Group):
                raise ListNotCappable("a table's every column beside a capped list")
            expanded = [
                exp.column(f"{node.table}__{c}", table="_ls", quoted=True)
                for c in by_alias[node.table]
            ]
            group.set("expressions", [*(e for e in group.expressions if e is not node), *expanded])
            continue
        node.replace(exp.column(f"{node.table}__{node.name}", table="_ls", quoted=True))
    return carried


def _number_the_source(
    select: exp.Select, lists: list[exp.Expr], limit: int, columns_of: ColumnsOf
) -> None:
    """``json_agg(e)`` beside a grouping or other aggregates: the rows are numbered per group in
    the select's source, and the list aggregates those within the limit."""
    source = select.args.get("from_") or select.args.get("from")
    if source is None:
        raise ListNotCappable("a list beside other aggregates read from no table")
    table = source.this
    joined = bool(select.args.get("joins"))
    alias = "_ls" if joined else table.alias_or_name
    held = {t.alias_or_name: t for t in [table, *(j.this for j in select.args.get("joins") or [])]}
    keys: list[exp.Expr] = []
    for key in select.args["group"].expressions if select.args.get("group") else []:
        if isinstance(key, exp.Column) and isinstance(key.this, exp.Star):
            # A table's every column: each is a key the rows are partitioned by.
            joined_table = held.get(key.table)
            names = columns_of(joined_table) if isinstance(joined_table, exp.Table) else None
            if names is None:
                raise ListNotCappable("a grouping by every column of a source of no known columns")
            keys += [exp.column(name, table=key.table, quoted=True) for name in names]
        else:
            keys.append(key.copy())
    values = [
        lst.this.expressions[0].copy() if isinstance(lst.this, exp.Distinct) else None
        for lst in lists
    ]
    original_from = table.copy()
    original_joins = [j.copy() for j in select.args.get("joins") or []]
    carried = _read_from_one_source(select, columns_of) if joined else [exp.Star()]
    numbered = exp.Window(this=exp.RowNumber(), partition_by=keys or None)
    columns: list[exp.Expr] = [*carried, exp.alias_(numbered, ROW_NUMBER)]
    within: list[exp.Expr] = []
    for i, lst in enumerate(lists):
        number = ROW_NUMBER
        value = values[i]
        if value is not None:
            # A distinct list's entries are its distinct values: those are what is numbered.
            if not isinstance(value, (exp.Column, exp.Dot)):
                raise ListNotCappable("a distinct list of computed values beside other aggregates")
            number = f"{ROW_NUMBER}{i}"
            ranked = exp.Window(
                this=exp.Anonymous(this="DENSE_RANK", expressions=[]),
                partition_by=keys or None,
                order=exp.Order(expressions=[exp.Ordered(this=value)]),
            )
            columns.append(exp.alias_(ranked, number))
        within.append(
            exp.LTE(this=exp.column(number, table=alias), expression=exp.Literal.number(limit))
        )
    inner = exp.select(*columns).from_(original_from)
    for join in original_joins:
        inner = inner.join(join)
    where = select.args.get("where")
    if where is not None:
        inner = inner.where(where.this.copy())
        select.set("where", None)
    source.set("this", exp.alias_(exp.Subquery(this=inner), alias, table=True))
    select.set("joins", None)
    for lst, bound in zip(lists, within, strict=True):
        if isinstance(lst.parent, exp.Filter):
            # A list already filtered (collect() skips nulls) holds what both conditions keep.
            kept = lst.parent.expression
            kept.set("this", exp.And(this=exp.Paren(this=kept.this.copy()), expression=bound))
        else:
            lst.replace(exp.Filter(this=lst.copy(), expression=exp.Where(this=bound)))


def _limit_the_rows(select: exp.Select, lists: list[exp.Expr], limit: int) -> None:
    """A list that is its select's one value: the select reads at most the limit's rows."""
    if len(lists) != 1 or len(select.expressions) != 1:
        raise ListNotCappable("a select of several lists")
    (lst,) = lists
    if lst.args.get("order") is not None or isinstance(lst.this, exp.Order):
        raise ListNotCappable("an ordered list")
    source = select.args.get("from_") or select.args.get("from")
    if source is None:
        raise ListNotCappable("a list read from no table")
    held = source.this
    if (
        isinstance(held, exp.Subquery)
        and isinstance(held.this, exp.Select)
        and held.this.args.get("limit") is not None
    ):
        # Already the limited form: the smaller of its own limit and the role's.
        _hold_to(held.this, limit)
        return
    entry = lst.this.copy()
    distinct = isinstance(entry, exp.Distinct)
    if distinct:
        # A distinct list's entries are its distinct values: at most the limit's of those.
        (entry,) = entry.expressions
    inner = exp.select(exp.alias_(entry, "_t")).from_(held.copy())
    if distinct:
        inner = inner.distinct()
    for join in select.args.get("joins") or []:
        inner = inner.join(join.copy())
    where = select.args.get("where")
    if where is not None:
        inner = inner.where(where.this.copy())
    inner = inner.limit(limit)
    lst.set("this", exp.column("_t"))
    source.set("this", exp.alias_(exp.Subquery(this=inner), "_sub", table=True))
    select.set("joins", None)
    select.set("where", None)


def at_ceiling(rows: list[tuple], limit: int) -> bool:
    """Whether any list ``rows`` hold, at any depth, has exactly ``limit`` entries."""

    def full(value: Any) -> bool:
        if isinstance(value, str) and value[:1] in "[{":
            try:
                value = json.loads(value)
            except ValueError:
                return False
        if isinstance(value, list):
            return len(value) == limit or any(full(v) for v in value)
        if isinstance(value, dict):
            return any(full(v) for v in value.values())
        return False

    return any(full(cell) for row in rows for cell in row)


def list_at_ceiling_warning(limit: int, kind: str) -> ServerWarning:
    """What an answer holding a list of exactly the row limit's entries says (REQ-1350)."""
    return ServerWarning(
        code="statement.list_at_ceiling",
        params={"limit": limit, "kind": kind},
        message=(
            f"A list holds the maximum of {limit} entries the row limit allows for this read "
            f"({kind}); it may have more."
        ),
    )
