# Copyright (c) 2026 Kenneth Stott
# Canary: 8037d943-7414-4c30-b539-23819ae009be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's conditions and LIMIT on a SPARQL table, pushed into the table's SELECT
(REQ-2138), so the endpoint answers only the rows the statement can read.

Pushed: the top-level AND conjuncts of the WHERE of the one SELECT reading the table (no join),
each a comparison or IN of one column with literals -- a text column by equality (``STR(?v)``, so
an IRI and a literal compare by their text, as their rows hold them), a number by any comparison,
a boolean by equality. The conditions stay in the statement and are applied again to the rows,
so a pushed condition only ever narrows what is fetched. The LIMIT is pushed only when every
conjunct was and the SELECT does nothing else that needs rows beyond it (grouping, aggregates,
DISTINCT, ORDER BY, OFFSET).

The table's own SELECT becomes a subquery: ``SELECT * WHERE { { <its SELECT> } FILTER(...) }
LIMIT n``, its PREFIX and BASE declarations kept in front.
"""

# Requirements: REQ-2138

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import sqlglot
from sqlglot import exp

from provisa.api_source.models import ApiColumnType, ApiEndpoint
from provisa.codehost.residual import conjuncts, predicate

#: The form field a SPARQL 1.1 query is posted under, and the argument a narrowed query is
#: sent as (``caller.prepare_call``): part of the call, so the answer is kept under it.
QUERY_ARG = "query"

_SQL_OPS = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
_NUMBERS = (ApiColumnType.integer, ApiColumnType.number)
_PROLOGUE = re.compile(
    r"\A(?:\s*(?:#[^\n]*\n|PREFIX\s+[\w.-]*:\s*<[^>]*>|BASE\s+<[^>]*>))*\s*", re.IGNORECASE
)


def _text(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def _term(kind: ApiColumnType, value: Any) -> str | None:
    """``value`` as a SPARQL term a column of ``kind`` compares with, or None when it is not one."""
    if kind == ApiColumnType.string:
        return _text(value) if isinstance(value, str) else None
    if kind in _NUMBERS:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            return None
        return repr(value) if isinstance(value, float) else str(value)
    if kind == ApiColumnType.boolean and isinstance(value, bool):
        return "true" if value else "false"
    return None


def _filter(var: str, kind: ApiColumnType, op: str, value: Any) -> str | None:
    """The FILTER expression of one pushed conjunct, or None when it is not pushed."""
    if kind not in _NUMBERS and op not in ("eq", "ne", "in"):
        return None
    subject = f"STR(?{var})" if kind == ApiColumnType.string else f"?{var}"
    if op == "in":
        terms = [_term(kind, v) for v in value]
        if not terms or None in terms:
            return None
        return f"{subject} IN ({', '.join(t for t in terms if t is not None)})"
    term = _term(kind, value)
    sql_op = _SQL_OPS.get(op)
    if term is None or sql_op is None:
        return None
    return f"{subject} {sql_op} {term}"


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


def narrowed_query(
    query_template: str, sql: str, table: str, columns: Mapping[str, tuple[str, ApiColumnType]]
) -> str | None:
    """``query_template`` narrowed by what the statement ``sql`` (postgres) asks of ``table``:
    ``columns`` maps each column's SQL name to its SPARQL variable and type. None when nothing is
    pushed."""
    tree = sqlglot.parse_one(sql, read="postgres")
    reads = [t for t in tree.find_all(exp.Table) if t.name == table]
    if len(reads) != 1:
        return None  # read twice, the rows fetched serve both reads
    read = reads[0]
    select = read.parent_select
    if not isinstance(select, exp.Select) or select.args.get("joins"):
        return None
    source = select.args.get("from_") or select.args.get("from")
    if source is None or source.this is not read:
        return None
    names = {read.alias_or_name, read.name}
    where = select.args.get("where")
    filters: list[str] = []
    every_one = True
    for conjunct in conjuncts(where.this if where is not None else None):
        found = predicate(conjunct, names)
        pushed = None
        if found is not None and found[0] in columns:
            var, kind = columns[found[0]]
            pushed = _filter(var, kind, found[1], found[2])
        if pushed is None:
            every_one = False
        else:
            filters.append(pushed)
    limit = _limit(select) if every_one and _limit_pushable(select) else None
    if not filters and limit is None:
        return None
    prologue = _PROLOGUE.match(query_template)
    head = prologue.group(0) if prologue else ""
    body = query_template[len(head) :].strip().rstrip(";")
    narrowed = f"{head}SELECT * WHERE {{\n  {{ {body} }}\n"
    narrowed += "".join(f"  FILTER({f})\n" for f in filters)
    narrowed += "}"
    if limit is not None:
        narrowed += f"\nLIMIT {limit}"
    return narrowed


def pushed_arguments(endpoint: ApiEndpoint, sql: str, table: str) -> dict[str, str]:
    """The argument a SPARQL table's call is sent with to answer the statement ``sql``: its query
    narrowed (``QUERY_ARG``), or none when nothing is pushed."""
    from provisa.compiler.naming import apply_sql_name

    if endpoint.response_normalizer != "sparql_bindings" or not endpoint.query_template:
        return {}
    columns = {
        apply_sql_name(c.name): (c.name, c.type) for c in endpoint.columns if c.param_type is None
    }
    narrowed = narrowed_query(endpoint.query_template, sql, table, columns)
    return {} if narrowed is None else {QUERY_ARG: narrowed}
