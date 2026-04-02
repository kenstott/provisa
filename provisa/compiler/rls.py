# Copyright (c) 2025 Kenneth Stott
# Canary: 98081871-58f3-442f-911a-b2a85cbdf709
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Inject RLS WHERE clauses into compiled SQL per role (REQ-040, REQ-041).

Applied every request after SQL compilation, before transpilation.
RLS filter expressions are stored per (table_id, role_id) in the config DB.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from provisa.compiler.sql_gen import CompiledQuery, CompilationContext


@dataclass
class RLSContext:
    """RLS rules for a specific role, keyed by table_id."""

    # table_id → filter expression (raw SQL predicate)
    rules: dict[int, str]

    @staticmethod
    def empty() -> RLSContext:
        return RLSContext(rules={})

    def has_rules(self) -> bool:
        return bool(self.rules)


def build_rls_context(rls_rules: list[dict], role_id: str) -> RLSContext:
    """Build an RLSContext from DB rows for a specific role.

    Args:
        rls_rules: list of dicts with {table_id, role_id, filter_expr}.
        role_id: the role to filter for.
    """
    rules = {}
    for rule in rls_rules:
        if rule["role_id"] == role_id:
            rules[rule["table_id"]] = rule["filter_expr"]
    return RLSContext(rules=rules)


def inject_rls(
    compiled: CompiledQuery,
    ctx: CompilationContext,
    rls: RLSContext,
) -> CompiledQuery:
    """Inject RLS WHERE clauses into a compiled query.

    For each table referenced in the query that has an RLS rule,
    the filter expression is ANDed into the WHERE clause.

    Returns a new CompiledQuery with the modified SQL.
    """
    if not rls.has_rules():
        return compiled

    # Find which tables in the query have RLS rules
    filters: list[str] = []

    # Check root table
    root_table = ctx.tables.get(compiled.root_field)
    if root_table and root_table.table_id in rls.rules:
        filter_expr = rls.rules[root_table.table_id]
        # Qualify column refs with alias if the query uses aliases
        if _has_alias(compiled.sql):
            filter_expr = _qualify_filter(filter_expr, "t0")
        filters.append(f"({filter_expr})")

    # Check joined tables — find their aliases from the SQL
    for (type_name, field_name), join_meta in ctx.joins.items():
        if type_name == root_table.type_name and join_meta.target.table_id in rls.rules:
            filter_expr = rls.rules[join_meta.target.table_id]
            alias = _find_join_alias(compiled.sql, join_meta.target.table_name)
            if alias:
                filter_expr = _qualify_filter(filter_expr, alias)
            filters.append(f"({filter_expr})")

    if not filters:
        return compiled

    rls_clause = " AND ".join(filters)
    sql = _inject_where(compiled.sql, rls_clause)

    return CompiledQuery(
        sql=sql,
        params=compiled.params,
        root_field=compiled.root_field,
        columns=compiled.columns,
        sources=compiled.sources,
    )


def _has_alias(sql: str) -> bool:
    """Check if the SQL uses table aliases (t0, t1, ...)."""
    return '"t0"' in sql


def _qualify_filter(filter_expr: str, alias: str) -> str:
    """Prefix unqualified column references in a filter with a table alias.

    Simple heuristic: any word that looks like a column name (starts with letter,
    not already qualified with a dot prefix) gets prefixed.
    This handles simple filters like `region = 'us-east'`.
    """
    # Don't re-qualify already-qualified refs
    if f'"{alias}".' in filter_expr:
        return filter_expr

    # Match bare identifiers that are likely column names
    # (word chars not preceded by a dot or quote, not a SQL keyword)
    sql_keywords = {
        "and", "or", "not", "is", "null", "in", "like", "between",
        "true", "false", "current_setting", "select", "from", "where",
    }

    def _replace(m: re.Match) -> str:
        word = m.group(0)
        if word.lower() in sql_keywords:
            return word
        # Check if preceded by a dot (already qualified) or single quote (string literal)
        start = m.start()
        if start > 0 and filter_expr[start - 1] in (".", "'"):
            return word
        return f'"{alias}".{word}'

    return re.sub(r'\b([a-zA-Z_]\w*)\b', _replace, filter_expr)


def _find_join_alias(sql: str, table_name: str) -> str | None:
    """Find the alias used for a joined table in the SQL."""
    # Pattern: JOIN "schema"."table" "tN"
    pattern = rf'"[^"]*"\."{re.escape(table_name)}"\s+"(t\d+)"'
    m = re.search(pattern, sql)
    return m.group(1) if m else None


def _inject_where(sql: str, rls_clause: str) -> str:
    """Inject an RLS clause into SQL, merging with existing WHERE if present."""
    # Find WHERE position (case-insensitive, word boundary)
    where_match = re.search(r'\bWHERE\b', sql, re.IGNORECASE)
    if where_match:
        # Insert RLS after WHERE, before existing conditions
        pos = where_match.end()
        return f"{sql[:pos]} {rls_clause} AND{sql[pos:]}"

    # No WHERE — insert before ORDER BY, LIMIT, or end
    for keyword in (r'\bORDER\s+BY\b', r'\bLIMIT\b', r'\bOFFSET\b'):
        m = re.search(keyword, sql, re.IGNORECASE)
        if m:
            return f"{sql[:m.start()]}WHERE {rls_clause} {sql[m.start():]}"

    return f"{sql} WHERE {rls_clause}"
