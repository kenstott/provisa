# Copyright (c) 2026 Kenneth Stott
# Canary: f24db85d-3c5a-4f39-9539-f88ff541bcdf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An aggregate query answered from a materialized view that pre-aggregates its table
(REQ-198, REQ-199, REQ-882).

Nothing is declared: a fresh materialized view whose SQL is a single-table ``GROUP BY`` of
decomposable aggregates serves the aggregate queries it can answer exactly. Its grouping
columns are its *dimensions*; each of its aggregates, a *measure*:

- ``SUM(c)``, ``COUNT(*)``, ``COUNT(c)``, ``MIN(c)`` and ``MAX(c)`` -- each stored under its own
  output name. ``AVG``, ``COUNT(DISTINCT ...)`` or any expression in a view's select list make it
  serve none: neither re-aggregates.

A query is answered from such a view only when it reads that one table, groups by a subset of
the view's dimensions, filters (and orders) only on them, and asks only for aggregates the view's
measures re-aggregate exactly:

- ``SUM(c)`` -> ``SUM(<sum of c>)``; ``COUNT(*)`` -> ``COALESCE(SUM(<count>), 0)`` as bigint;
  ``COUNT(c)`` -> ``COALESCE(SUM(<count of c>), 0)`` as bigint; ``MIN``/``MAX`` -> themselves over the
  stored ``MIN``/``MAX``;
- ``AVG(c)`` -> the stored sum of ``c`` over its stored count, as ``numeric``: only from a stored
  ``SUM(c)`` and ``COUNT(c)``.

Anything else is not rewritten: a join, ``DISTINCT``, ``HAVING``, a window, a subquery, a column
the view does not keep as a dimension. The statement is the role's governed one, and the view was
computed over every row: a row filter every column of which is a dimension is carried onto the
view, exact as each of its rows is one combination of dimension values (REQ-882, amended
2026-11-04); a filter on any other column, or with a subquery, is a column the view does not keep
or a second table, and is not rewritten. A mask stands in its column's place as an expression or a
constant: a masked measure is no stored measure, and a masked dimension grouped on is no
dimension, so neither is rewritten; a masked dimension only filtered on is evaluated over the
view's dimension values as over the table's.
"""

# Requirements: REQ-198, REQ-199, REQ-882

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

if TYPE_CHECKING:
    from provisa.mv.models import MVDefinition

log = logging.getLogger(__name__)

#: (catalog, schema, table), lowercased: the one table a view pre-aggregates.
Relation = tuple[str, str, str]
#: (aggregate, column lowercased or "*").
Measure = tuple[str, str]

_STORED = {exp.Sum: "sum", exp.Min: "min", exp.Max: "max", exp.Count: "count"}


@dataclass(frozen=True)
class AggregateShape:
    """What a pre-aggregating view keeps: its table, dimensions and measures, each by the
    column of the view that holds it."""

    relation: Relation
    dims: dict[str, str]  # base column (lowercased) -> view column
    measures: dict[Measure, str]  # measure -> view column


def _plain_column(node: exp.Expression) -> str | None:
    return node.name.lower() if isinstance(node, exp.Column) and node.name else None


def _measure_of(node: exp.Expression) -> Measure | None:
    """The measure an aggregate call is, when it is one a view can store and re-aggregate."""
    kind = _STORED.get(type(node))
    if kind is None:
        return None
    arg = node.this
    if isinstance(arg, exp.Distinct):
        return None
    if kind == "count" and isinstance(arg, exp.Star):
        return ("count", "*")
    column = _plain_column(arg) if arg is not None else None
    return (kind, column) if column is not None else None


def _single_table_select(tree: Any) -> exp.Table | None:
    """The one table a plain SELECT reads, or None for anything else."""
    if not isinstance(tree, exp.Select):
        return None
    if tree.args.get("joins") or tree.args.get("distinct") or tree.args.get("having"):
        return None
    if tree.find(exp.Window) is not None or tree.find(exp.Subquery) is not None:
        return None
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1:
        return None
    return tables[0]


@lru_cache(maxsize=1024)
def derive_shape(sql: str) -> AggregateShape | None:
    """What the view defined by ``sql`` (its catalog-physical SQL) pre-aggregates, or None when
    it is not a single-table ``GROUP BY`` of storable measures."""
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except (SqlglotError, RecursionError):
        return None
    table = _single_table_select(tree)
    if table is None or not table.catalog:
        return None
    if tree.args.get("where") or tree.args.get("order") or tree.args.get("limit"):
        return None
    group = tree.args.get("group")
    grouped = [_plain_column(g) for g in (group.expressions if group else [])]
    if any(g is None for g in grouped):
        return None
    dims: dict[str, str] = {}
    measures: dict[Measure, str] = {}
    for selected in tree.expressions:
        name = selected.alias_or_name
        node = selected.this if isinstance(selected, exp.Alias) else selected
        column = _plain_column(node)
        if column is not None:
            if column not in grouped:
                return None
            dims[column] = name
            continue
        measure = _measure_of(node)
        if measure is None or not isinstance(selected, exp.Alias):
            return None  # an expression, or an aggregate with no name to read it back by
        measures[measure] = name
    if set(grouped) - dims.keys() or not measures:
        return None
    relation = (table.catalog.lower(), table.db.lower(), table.name.lower())
    return AggregateShape(relation=relation, dims=dims, measures=measures)


def _quoted(name: str) -> exp.Column:
    return exp.Column(this=exp.Identifier(this=name, quoted=True))


def _reaggregate(node: exp.Expression, shape: AggregateShape) -> exp.Expression | None:
    """The query's aggregate ``node`` re-aggregated over the view's measures, or None when the
    view does not hold what it needs."""
    if isinstance(node, exp.Avg):
        column = _plain_column(node.this)
        total = shape.measures.get(("sum", column)) if column else None
        count = shape.measures.get(("count", column)) if column else None
        if total is None or count is None:
            return None
        return exp.Div(
            this=exp.Cast(this=exp.Sum(this=_quoted(total)), to=exp.DataType.build("numeric")),
            expression=exp.Nullif(
                this=exp.Sum(this=_quoted(count)), expression=exp.Literal.number(0)
            ),
        )
    measure = _measure_of(node)
    if measure is None:
        return None
    stored = shape.measures.get(measure)
    if stored is None:
        return None
    if measure[0] == "count":
        # A count summed is numeric, and NULL over no rows: as COUNT itself, a bigint and 0.
        return exp.Cast(
            this=exp.Coalesce(
                this=exp.Sum(this=_quoted(stored)), expressions=[exp.Literal.number(0)]
            ),
            to=exp.DataType.build("bigint"),
        )
    return {"sum": exp.Sum, "min": exp.Min, "max": exp.Max}[measure[0]](this=_quoted(stored))


_AGGREGATES = (exp.Sum, exp.Min, exp.Max, exp.Count, exp.Avg)


def rewrite_over(sql: str, relation: Relation, shape: AggregateShape, stored: str) -> str | None:
    """``sql`` (a single-table aggregate over ``relation``) read from the view stored at
    ``stored`` instead, or None when the view cannot answer it exactly."""
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except (SqlglotError, RecursionError):
        return None
    table = _single_table_select(tree)
    if table is None or shape.relation != relation:
        return None
    if tree.find(*_AGGREGATES) is None:
        return None
    group = tree.args.get("group")
    for g in group.expressions if group else []:
        if _plain_column(g) not in shape.dims:
            return None

    # Name each output as the query named it: an unnamed aggregate or column keeps the name
    # PostgreSQL gives it (its function's, its column's), which its reader reads it back by.
    named = []
    for selected in tree.expressions:
        if isinstance(selected, _AGGREGATES):
            selected = exp.alias_(selected, selected.key, quoted=True)
        elif isinstance(selected, exp.Column):
            selected = exp.alias_(selected, selected.name, quoted=True)
        named.append(selected)
    tree.set("expressions", named)
    # The columns read outside an aggregate -- selected, grouped, filtered, ordered on -- are the
    # view's dimensions; then each aggregate is re-aggregated over its measures.
    for column in list(tree.find_all(exp.Column)):
        if column.find_ancestor(*_AGGREGATES) is not None:
            continue
        dim = shape.dims.get(column.name.lower())
        if dim is None:
            return None
        column.replace(_quoted(dim))
    for aggregate in list(tree.find_all(*_AGGREGATES)):
        if aggregate.find_ancestor(*_AGGREGATES) is not None:
            return None  # an aggregate of an aggregate
        swapped = _reaggregate(aggregate, shape)
        if swapped is None:
            return None
        aggregate.replace(swapped)
    table.replace(sqlglot.parse_one(stored, into=exp.Table, read="postgres"))
    return tree.sql(dialect="postgres")


def rewrite_aggregate_query(
    sql: str, relation: Relation, fresh_mvs: list[MVDefinition]
) -> tuple[str, MVDefinition] | None:  # REQ-882
    """``sql`` -- a statement over ``relation`` -- read from the first fresh view that answers it
    exactly, with that view; None when none does."""
    for mv in fresh_mvs:
        if not mv.sql or mv.join_pattern is not None:
            continue
        shape = derive_shape(mv.sql)
        if shape is None or shape.relation != relation:
            continue
        rewritten = rewrite_over(sql, relation, shape, mv.stored_ref())
        if rewritten is not None:
            log.info("aggregate view %s answers a query over %s", mv.id, ".".join(relation))
            return f"/* aggregate_mv: {mv.id} */\n{rewritten}", mv
    return None
