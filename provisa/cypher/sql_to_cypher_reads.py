# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7b5e90-6c1a-4f83-b4e2-9a0c8f3d61b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship field's grouped read, read by the SQL -> Cypher translator as the nested field it
is (#362). The compiler writes each field as a derived table of the target's rows keyed by the join
column, LEFT JOINed back by key (``compiler.sql_selection.rel_read``); the translator reads a nested
field as the subquery of the field's rows correlated with its parent's row, the same relationship
stated per row. Each read is restated so before the translator reads the statement:

- a to-one read (the head of each key's rows) is ``(SELECT json_object(...) FROM ... WHERE
  key = parent LIMIT 1)``;
- a list read is ``(SELECT json_agg(json_object(...)) FROM ... WHERE key = parent)``, and with a
  cap ``(SELECT json_agg(_t) FROM (SELECT json_object(...) AS _t FROM ... WHERE key = parent
  LIMIT n) _sub)``.

Nested reads are restated inside their parent's first. A join of any other shape is left as it
is."""

# Requirements: REQ-345, REQ-2236

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.compiler.sql_selection import READ_KEY, READ_ROW, READ_ROWS, READ_VALUE


def _keys(join: exp.Join) -> list[tuple[str, exp.Expr]] | None:
    """Each (read key name, the parent's value it equals) of a read's LEFT JOIN."""
    alias = join.this.alias
    on = join.args.get("on")
    if on is None:
        return None
    conjuncts = list(on.flatten()) if isinstance(on, exp.And) else [on]
    pairs = []
    for conjunct in conjuncts:
        if not isinstance(conjunct, exp.EQ):
            return None
        left = conjunct.left
        if not (
            isinstance(left, exp.Column) and left.table == alias and left.name.startswith(READ_KEY)
        ):
            return None
        pairs.append((left.name, conjunct.right))
    return pairs


def _named(select: exp.Select) -> dict[str, exp.Expr]:
    return {p.alias_or_name: p.unalias() for p in select.expressions}


def _correlated(rows: exp.Select, keys: list[tuple[str, exp.Expr]], value: exp.Expr) -> exp.Select:
    """``SELECT value FROM <rows' FROM and joins> WHERE <rows' key> = <parent> AND ...``."""
    named = _named(rows)
    select = exp.Select(expressions=[value.copy()])
    select.set("from_", (rows.args.get("from_") or rows.args["from"]).copy())
    select.set("joins", [j.copy() for j in rows.args.get("joins") or []] or None)
    for name, parent in keys:
        select.where(exp.EQ(this=named[name].copy(), expression=parent.copy()), copy=False)
    where = rows.args.get("where")
    if where is not None:
        select.where(where.this.copy(), copy=False)
    return select


def _as_subquery(join: exp.Join) -> exp.Subquery | None:
    """The correlated subquery ``join``'s grouped read states, or None for any other join."""
    read = join.this.this if isinstance(join.this, exp.Subquery) else None
    keys = _keys(join) if read is not None and join.side.upper() == "LEFT" else None
    if read is None or keys is None or not isinstance(read, exp.Select):
        return None
    source = (read.args.get("from_") or read.args["from"]).this
    if isinstance(source, exp.Subquery) and source.alias == READ_ROWS:
        rows = source.this
        restate(rows)
        value = _named(rows)[READ_VALUE]
        cap = read.args["where"].this
        if not (isinstance(cap, (exp.EQ, exp.LTE)) and cap.left.name == READ_ROW):
            return None
        if isinstance(cap, exp.EQ):
            return exp.Subquery(this=_correlated(rows, keys, value).limit(1, copy=False))
        listed = _correlated(rows, keys, exp.alias_(value, "_t")).limit(
            cap.expression.copy(), copy=False
        )
        outer = exp.Select(expressions=[exp.JSONArrayAgg(this=exp.column("_t"))]).from_(
            exp.alias_(exp.Subquery(this=listed), "_sub", table=True), copy=False
        )
        return exp.Subquery(this=outer)
    restate(read)
    return exp.Subquery(this=_correlated(read, keys, _named(read)[READ_VALUE]))


def restate(select: exp.Select) -> None:
    """``select`` with each grouped read joined to it restated as its correlated subquery, in
    place."""
    kept: list[exp.Join] = []
    for join in select.args.get("joins") or []:
        subquery = _as_subquery(join)
        if subquery is None:
            kept.append(join)
            continue
        alias = join.this.alias
        for column in list(select.find_all(exp.Column)):
            if column.table == alias and column.name == READ_VALUE:
                column.replace(subquery.copy())
    select.set("joins", kept or None)
