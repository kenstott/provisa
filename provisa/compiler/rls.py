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

# Requirements: REQ-038, REQ-039, REQ-040, REQ-041, REQ-402, REQ-403

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

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


def build_rls_context(rls_rules: list[dict], role_id: str) -> RLSContext:  # REQ-041, REQ-402
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


def inject_rls(  # REQ-038, REQ-040, REQ-041, REQ-402, REQ-403
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

        def _rule_for_table(table_id: int, domain_id: str) -> str | None:
            if table_id in rls.rules:
                return rls.rules[table_id]
            if domain_id and domain_id in rls.domain_rules:
                return rls.domain_rules[domain_id]
            return None

        tree = sqlglot.parse_one(compiled.sql, read="postgres")
        # Map each physical table_name to the alias it carries in the query (its own name when
        # unaliased) — read from the AST, so any alias convention works, not just "t0"/"tN".
        alias_by_table = _alias_by_table(tree)  # pyright: ignore[reportArgumentType]  # sqlglot stub

        # Build a qualified predicate AST per applicable table.
        predicates: list[exp.Expression] = []
        root_table = ctx.tables.get(compiled.root_field)
        if root_table:
            filter_expr = _rule_for_table(root_table.table_id, root_table.domain_id)
            if filter_expr:
                predicates.append(
                    _qualified_predicate(filter_expr, alias_by_table.get(root_table.table_name))
                )

        for (type_name, _), join_meta in ctx.joins.items():
            if root_table and type_name == root_table.type_name:
                filter_expr = _rule_for_table(join_meta.target.table_id, join_meta.target.domain_id)
                if filter_expr:
                    predicates.append(
                        _qualified_predicate(
                            filter_expr, alias_by_table.get(join_meta.target.table_name)
                        )
                    )

        span.set_attribute("rls.rules_applied", len(predicates))
        if not predicates:
            return compiled

        select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
        if select is None:
            raise ValueError("inject_rls: query has no SELECT to attach the RLS predicate to")
        for pred in predicates:
            select.where(pred, copy=False)  # ANDs into the existing WHERE at the right position

        sql = tree.sql(dialect="postgres")

        from provisa.observability.stage_trace import trace_stage

        trace_stage("govern.rls", sql)
        return CompiledQuery(
            sql=sql,
            params=compiled.params,
            root_field=compiled.root_field,
            columns=compiled.columns,
            sources=compiled.sources,
        )


def _alias_by_table(tree: exp.Expression) -> dict[str, str]:
    """Map each table's physical name to the alias it carries in the query (its own name when
    unaliased). Read structurally from the AST — no regex over the SQL text."""
    out: dict[str, str] = {}
    for tbl in tree.find_all(exp.Table):
        out.setdefault(tbl.name, tbl.alias or tbl.name)
    return out


def _qualified_predicate(filter_expr: str, alias: str | None) -> exp.Expression:
    """Parse a raw SQL predicate and qualify every unqualified column with ``alias`` (AST).

    Structural, not textual: string literals and keywords are their own node types, so only
    genuine Column nodes are touched — no regex, no keyword denylist. Already-qualified columns
    keep their table. Column identifiers are rendered quoted to match the compiler convention.
    """
    pred = sqlglot.parse_one(filter_expr, read="postgres")
    if alias:
        for col in pred.find_all(exp.Column):
            if not col.table:
                col.set("table", exp.to_identifier(alias, quoted=True))
            if isinstance(col.this, exp.Identifier):
                col.this.set("quoted", True)
    return pred  # pyright: ignore[reportReturnType]  # sqlglot stub types parse_one as Expr


def _qualify_filter(filter_expr: str, alias: str) -> str:
    """Qualify bare columns in ``filter_expr`` with ``alias`` and return the SQL text."""
    return _qualified_predicate(filter_expr, alias).sql(dialect="postgres")


def _inject_where(sql: str, rls_clause: str) -> str:
    """AND ``rls_clause`` into a query's WHERE (AST): parse, attach the predicate to the outer
    SELECT — sqlglot places it before ORDER BY/LIMIT and merges an existing WHERE — reserialize."""
    tree = sqlglot.parse_one(sql, read="postgres")
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        raise ValueError("_inject_where: query has no SELECT to attach the predicate to")
    select.where(rls_clause, dialect="postgres", copy=False)
    return tree.sql(dialect="postgres")
