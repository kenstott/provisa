"""A role's row limit bounds every list a read nests, as it bounds the read's rows (REQ-005).

A list a statement aggregates -- ``json_agg`` / ``JSON_ARRAYAGG``: an aggregate or group-by's
nodes, a to-many relationship's rows -- holds at most the limit's number of entries. Its counts
and other aggregates stay over every row, and the statement's own LIMIT is governance's as before
(stage2). The rewrite is one, of the governed statement's tree, whatever compiled it:

- A list beside a grouping or other aggregates numbers each group's rows in its source --
  ``ROW_NUMBER() OVER (PARTITION BY <the group's keys>)`` -- and aggregates those within the
  limit: ``json_agg(e) FILTER (WHERE __list_rn <= n)``.
- A list that is its select's one value (a relationship's correlated subquery) reads at most the
  limit's rows: ``(SELECT json_agg(_t) FROM (SELECT e AS _t FROM ... LIMIT n) _sub)``, the
  compiler's own limited form; one already limited is held to the smaller.

A list in any other shape is refused by name, never left unbounded. A list of exactly the limit's
entries says so, as it may have more (:func:`at_ceiling`, REQ-1350).
"""

# Requirements: REQ-005, REQ-1350

from __future__ import annotations

import json
from typing import Any

import sqlglot.expressions as exp

from provisa.core.statement_warnings import ServerWarning

#: The column a capped list's source numbers its rows in.
ROW_NUMBER = "__list_rn"


class ListNotCappable(ValueError):
    """A list the statement aggregates in a shape the cap has no form for; the message names it."""


def _is_list(node: exp.Expr) -> bool:
    return isinstance(node, exp.JSONArrayAgg) or (
        isinstance(node, exp.Anonymous) and node.name.lower() in ("json_arrayagg", "json_agg")
    )


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


def cap_lists(tree: exp.Expr, limit: int) -> bool:
    """Bound each list ``tree`` aggregates at ``limit`` entries, in place. Returns whether any
    list was bounded. :class:`ListNotCappable` for a list in a shape the cap has no form for."""
    by_select: dict[int, tuple[exp.Select, list[exp.Expr]]] = {}
    for node in list(tree.walk()):
        if _is_list(node):
            select = _own_select(node)
            if select is None:
                raise ListNotCappable("a list aggregated outside any select")
            by_select.setdefault(id(select), (select, []))[1].append(node)
    for select, lists in by_select.values():
        if _aggregates_beside(select, lists):
            _number_the_source(select, lists, limit)
        else:
            _limit_the_rows(select, lists, limit)
    return bool(by_select)


def _number_the_source(select: exp.Select, lists: list[exp.Expr], limit: int) -> None:
    """``json_agg(e)`` beside a grouping or other aggregates: the rows are numbered per group in
    the select's source, and the list aggregates those within the limit."""
    source = select.args.get("from_") or select.args.get("from")
    if source is None or select.args.get("joins"):
        raise ListNotCappable("a list beside other aggregates over joined or no tables")
    table = source.this
    alias = table.alias_or_name
    keys = [
        e.copy() for e in (select.args["group"].expressions if select.args.get("group") else [])
    ]
    numbered = exp.Window(this=exp.RowNumber(), partition_by=keys or None)
    inner = exp.select(exp.Star(), exp.alias_(numbered, ROW_NUMBER)).from_(table.copy())
    where = select.args.get("where")
    if where is not None:
        inner = inner.where(where.this.copy())
        select.set("where", None)
    source.set("this", exp.alias_(exp.Subquery(this=inner), alias, table=True))
    within = exp.LTE(this=exp.column(ROW_NUMBER, table=alias), expression=exp.Literal.number(limit))
    for lst in lists:
        if isinstance(lst.parent, exp.Filter):
            raise ListNotCappable("a list already filtered")
        lst.replace(exp.Filter(this=lst.copy(), expression=exp.Where(this=within.copy())))


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
    own = held.this.args.get("limit") if isinstance(held, exp.Subquery) else None
    if isinstance(held, exp.Subquery) and isinstance(held.this, exp.Select) and own is not None:
        # Already the limited form: the smaller of its own limit and the role's.
        value = own.expression
        if not (isinstance(value, exp.Literal) and not value.is_string):
            raise ListNotCappable("a list limited by a value not written in it")
        if int(value.name) > limit:
            held.this.limit(limit, copy=False)
        return
    entry = lst.this.copy()
    inner = exp.select(exp.alias_(entry, "_t")).from_(held.copy())
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
