# Copyright (c) 2026 Kenneth Stott
# Canary: 4a92e1d6-07b3-4c8f-b5e2-1f6d83a0c975
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A QuickBooks entity's table read through the query endpoint (REQ-2197), and a statement's
conditions on it lowered into QuickBooks' query language.

A table's query is ``SELECT * FROM <Entity>``, with ``WHERE`` conditions where the statement gives
them; a read sends it a page at a time, ``STARTPOSITION <n> MAXRESULTS <m>`` after it.

Pushed: the top-level AND conjuncts of the WHERE of the one SELECT reading the table (no join),
each a comparison of one filterable field with literals, as QuickBooks takes it -- ``=``, ``<``,
``>``, ``<=``, ``>=`` and ``IN`` on a number, a date or a time; ``=`` and ``IN`` on text and on a
reference's id; ``=`` on a boolean; ``LIKE`` on the text fields QuickBooks matches, with ``%``
as its only wildcard. QuickBooks has no ``OR``, no ``NOT`` and no ``!=``: a condition with one of
them, or on a field it does not filter, is left to the engine. Every condition stays in the
statement and is applied again to the rows, so a pushed one only narrows what is fetched. The
LIMIT is pushed only when every conjunct was and the SELECT does nothing else that needs rows
beyond it (grouping, aggregates, DISTINCT, ORDER BY, OFFSET).
"""

# Requirements: REQ-2197
from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import sqlglot
from sqlglot import exp

from provisa.codehost.residual import conjuncts, predicate
from provisa.quickbooks.entities import (
    BOOLEAN,
    DATE,
    INTEGER,
    NUMBER,
    REF,
    TEXT,
    TIMESTAMP,
    Column,
    Entity,
    columns_of,
)

#: The argument a narrowed query is sent as: part of the call, so its answer is kept under it.
QUERY_ARG = "query"
#: The argument that bounds how many rows a read takes, where the statement's LIMIT was pushed.
MAX_ROWS_ARG = "max_rows"
#: The most rows QuickBooks answers one query with.
MAX_PAGE = 1000

_OPS = {"eq": "=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
_ORDERED = (NUMBER, INTEGER, DATE, TIMESTAMP)


def entity_query(entity: Entity, conditions: list[str] | tuple[str, ...] = ()) -> str:
    """The query that reads ``entity``'s rows that meet ``conditions`` (already in QuickBooks'
    language). An entity with an ``Active`` flag is read whole, its inactive records too, unless a
    condition names ``Active``: QuickBooks otherwise answers only the active ones."""
    where = list(conditions)
    if entity.has_active and not any(c.startswith("Active ") for c in where):
        where.append("Active IN (true, false)")
    query = f"SELECT * FROM {entity.name}"
    if where:
        query += " WHERE " + " AND ".join(where)
    return query


def page_query(query: str, start: int, size: int) -> str:
    """``query`` asking for the page of ``size`` rows that starts at row ``start`` (from 1)."""
    if not 1 <= size <= MAX_PAGE:
        raise ValueError(f"a QuickBooks page holds 1 to {MAX_PAGE} rows, not {size}")
    if start < 1:
        raise ValueError(f"a QuickBooks page starts at row 1 or later, not {start}")
    return f"{query} STARTPOSITION {start} MAXRESULTS {size}"


def _text(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _literal(kind: str, value: Any) -> str | None:
    """``value`` as a literal a field of ``kind`` is compared with, or None when it is not one."""
    if kind in (TEXT, REF):
        return _text(value) if isinstance(value, str) else None
    if kind in (NUMBER, INTEGER):
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            return None
        return repr(value) if isinstance(value, float) else str(value)
    if kind == BOOLEAN:
        return ("true" if value else "false") if isinstance(value, bool) else None
    if kind == DATE:
        if isinstance(value, dt.datetime):
            return None  # a time compared with a date: the engine decides what it means
        if isinstance(value, dt.date):
            return _text(value.isoformat())
        return _text(value) if isinstance(value, str) and _is_date(value) else None
    if kind == TIMESTAMP:
        if isinstance(value, dt.datetime):
            return _text(value.isoformat())
        return _text(value) if isinstance(value, str) and _is_time(value) else None
    return None


def _is_date(value: str) -> bool:
    try:
        dt.date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _is_time(value: str) -> bool:
    try:
        dt.datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _like(pattern: Any) -> str | None:
    """A LIKE pattern QuickBooks matches as SQL does: ``%`` its only wildcard. A pattern with
    ``_`` (any one character) or an escape is not one."""
    if not isinstance(pattern, str) or "_" in pattern or "\\" in pattern:
        return None
    return _text(pattern)


def condition(column: Column, op: str, value: Any) -> str | None:
    """One conjunct in QuickBooks' language, or None when QuickBooks cannot take it."""
    if not column.filterable:
        return None
    field = column.path
    if op == "like":
        pattern = _like(value) if column.like else None
        return None if pattern is None else f"{field} LIKE {pattern}"
    if op == "in":
        if column.kind == BOOLEAN or not isinstance(value, list) or not value:
            return None
        literals = [_literal(column.kind, v) for v in value]
        if None in literals:
            return None
        return f"{field} IN ({', '.join(lit for lit in literals if lit is not None)})"
    sql_op = _OPS.get(op)
    if sql_op is None or (sql_op != "=" and column.kind not in _ORDERED):
        return None
    literal = _literal(column.kind, value)
    return None if literal is None else f"{field} {sql_op} {literal}"


def _limit(select: exp.Select) -> int | None:
    limit = select.args.get("limit")
    if limit is None:
        return None
    node = limit.expression
    if isinstance(node, exp.Literal) and not node.is_string and str(node.this).isdigit():
        return int(node.this)
    return None


def _limit_pushable(select: exp.Select) -> bool:
    """Whether the first rows the table gives are the rows ``select`` returns: it groups,
    aggregates, de-duplicates, orders and skips nothing."""
    if any(select.args.get(k) for k in ("group", "having", "distinct", "order", "offset")):
        return False
    return not any(e.find(exp.AggFunc, exp.Window) for e in select.expressions)


def narrowed(entity: Entity, sql: str, table: str, columns: Mapping[str, Column]) -> dict[str, Any]:
    """The arguments a read of ``entity``'s table sends to answer the statement ``sql``
    (postgres): its query with the conditions pushed (:data:`QUERY_ARG`), and the most rows it
    takes where the LIMIT was pushed (:data:`MAX_ROWS_ARG`). ``columns`` maps each column's SQL
    name to its column. Empty when nothing is pushed."""
    tree = sqlglot.parse_one(sql, read="postgres")
    reads = [t for t in tree.find_all(exp.Table) if t.name == table]
    if len(reads) != 1:
        return {}  # read twice, the rows fetched serve both reads
    read = reads[0]
    select = read.parent_select
    if not isinstance(select, exp.Select) or select.args.get("joins"):
        return {}
    source = select.args.get("from_") or select.args.get("from")
    if source is None or source.this is not read:
        return {}
    names = {read.alias_or_name, read.name}
    where = select.args.get("where")
    pushed: list[str] = []
    every_one = True
    for conjunct in conjuncts(where.this if where is not None else None):
        found = predicate(conjunct, names)
        lowered = None
        if found is not None and found[0] in columns:
            lowered = condition(columns[found[0]], found[1], found[2])
        if lowered is None:
            every_one = False
        else:
            pushed.append(lowered)
    limit = _limit(select) if every_one and _limit_pushable(select) else None
    arguments: dict[str, Any] = {}
    if pushed:
        arguments[QUERY_ARG] = entity_query(entity, pushed)
    if limit is not None:
        arguments[MAX_ROWS_ARG] = limit
    return arguments


def sql_columns(entity: Entity) -> dict[str, Column]:
    """``entity``'s columns by the name a statement calls them."""
    from provisa.compiler.naming import apply_sql_name

    return {apply_sql_name(c.name): c for c in columns_of(entity)}
