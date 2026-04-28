# Copyright (c) 2026 Kenneth Stott
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
from dataclasses import dataclass, field

from provisa.compiler.sql_gen import CompiledQuery, CompilationContext
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)


@dataclass
class RLSContext:
    """RLS rules for a specific role, keyed by table_id or domain_id."""

    # table_id → filter expression (raw SQL predicate)
    rules: dict[int, str]
    # domain_id → filter expression (applies to all tables in that domain)
    domain_rules: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def empty() -> RLSContext:
        return RLSContext(rules={}, domain_rules={})

    def has_rules(self) -> bool:
        return bool(self.rules) or bool(self.domain_rules)


def build_rls_context(rls_rules: list[dict], role_id: str) -> RLSContext:
    """Build an RLSContext from DB rows for a specific role.

    Args:
        rls_rules: list of dicts with {table_id, domain_id, role_id, filter_expr}.
        role_id: the role to filter for.
    """
    rules: dict[int, str] = {}
    domain_rules: dict[str, str] = {}
    for rule in rls_rules:
        if rule["role_id"] != role_id:
            continue
        if rule.get("domain_id"):
            domain_rules[rule["domain_id"]] = rule["filter_expr"]
        elif rule.get("table_id") is not None:
            rules[rule["table_id"]] = rule["filter_expr"]
    return RLSContext(rules=rules, domain_rules=domain_rules)


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
    with _tracer.start_as_current_span("rls.inject") as span:
        if not rls.has_rules():
            span.set_attribute("rls.rules_applied", 0)
            return compiled

        # Find which tables in the query have RLS rules
        filters: list[str] = []

        def _rule_for_table(table_id: int, domain_id: str) -> str | None:
            if table_id in rls.rules:
                return rls.rules[table_id]
            if domain_id and domain_id in rls.domain_rules:
                return rls.domain_rules[domain_id]
            return None

        # Check root table
        root_table = ctx.tables.get(compiled.root_field)
        if root_table:
            filter_expr = _rule_for_table(root_table.table_id, root_table.domain_id)
            if filter_expr:
                # Qualify column refs with alias if the query uses aliases
                if _has_alias(compiled.sql):
                    filter_expr = _qualify_filter(filter_expr, "t0")
                filters.append(f"({filter_expr})")

        # Check joined tables — find their aliases from the SQL
        for (type_name, field_name), join_meta in ctx.joins.items():
            if root_table and type_name == root_table.type_name:
                filter_expr = _rule_for_table(join_meta.target.table_id, join_meta.target.domain_id)
                if filter_expr:
                    alias = _find_join_alias(compiled.sql, join_meta.target.table_name)
                    if alias:
                        filter_expr = _qualify_filter(filter_expr, alias)
                    filters.append(f"({filter_expr})")

        span.set_attribute("rls.rules_applied", len(filters))
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


_SQL_KEYWORDS = {
    "and", "or", "not", "is", "null", "in", "like", "between",
    "true", "false", "current_setting", "select", "from", "where",
    "exists", "case", "when", "then", "else", "end", "as", "on",
    "join", "left", "right", "inner", "outer", "cross", "full",
    "group", "by", "order", "having", "limit", "offset", "union",
    "all", "distinct", "with",
}


def _qualify_filter(filter_expr: str, alias: str) -> str:
    """Prefix bare column names in filter_expr with "alias".

    Handles string literals correctly — quoted values are skipped so
    words inside single quotes are never treated as column references.
    """
    if f'"{alias}".' in filter_expr:
        return filter_expr

    def _replace(m: re.Match) -> str:
        # Group 1 matches a single-quoted string literal — return unchanged
        if m.group(1) is not None:
            return m.group(1)
        word = m.group(2)
        if word.lower() in _SQL_KEYWORDS:
            return word
        start = m.start(2)
        if start > 0 and filter_expr[start - 1] in (".", "'"):
            return word
        return f'"{alias}".{word}'

    # Match single-quoted strings first (to skip them), then bare identifiers
    return re.sub(r"('(?:[^'\\]|\\.)*')|(\b[a-zA-Z_]\w*\b)", _replace, filter_expr)


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
