# Copyright (c) 2026 Kenneth Stott
# Canary: 0853c191-82dd-45b0-9317-468bbfdca129
# (run scripts/canary_stamp.py on this file after creating it)

"""Stage 2: SQL governance transformer (REQ-263, REQ-264).

Applies RLS, column visibility, masking, and LIMIT ceiling to raw SQL
using SQLGlot. Input: plain SQL string. Output: governed SQL string.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.sql_gen import CompilationContext
from provisa.security.masking import MaskingRule, build_mask_expression


# --------------------------------------------------------------------------- #
# GovernanceContext                                                            #
# --------------------------------------------------------------------------- #

@dataclass
class GovernanceContext:
    """Governance parameters for a single request/role."""

    # table_id → RLS filter expression (already role-filtered)
    rls_rules: dict[int, str] = field(default_factory=dict)
    # (table_id, col_name) → (MaskingRule, data_type)
    masking_rules: dict[tuple[int, str], tuple] = field(default_factory=dict)
    # table_id → visible column names (None = all visible)
    visible_columns: dict[int, frozenset[str] | None] = field(default_factory=dict)
    # "schema.table" or "table" → table_id
    table_map: dict[str, int] = field(default_factory=dict)
    # table_id → [(col_name, data_type)]
    all_columns: dict[int, list[tuple[str, str]]] = field(default_factory=dict)
    limit_ceiling: int | None = None
    sample_size: int | None = None


# --------------------------------------------------------------------------- #
# Builder                                                                     #
# --------------------------------------------------------------------------- #

def build_governance_context(
    role_id: str,
    rls_context,
    masking_rules,
    ctx: CompilationContext,
    tables: list[dict],
) -> GovernanceContext:
    """Build GovernanceContext from server state for a given role.

    Args:
        role_id: The requesting role.
        rls_context: RLSContext with .rules: dict[int, str].
        masking_rules: MaskingRules = dict[(table_id, role_id), dict[col, (rule, dtype)]].
        ctx: CompilationContext with .tables: dict[str, TableMeta].
        tables: Raw table dicts from state, each with
                {id, columns: [{column_name, visible_to: [role_ids], data_type}]}.
    """
    gov = GovernanceContext()

    # RLS rules
    gov.rls_rules = dict(rls_context.rules) if rls_context else {}

    # Masking rules — flatten to (table_id, col_name) → (rule, dtype)
    for (table_id, r_id), col_map in masking_rules.items():
        if r_id != role_id:
            continue
        for col_name, (rule, dtype) in col_map.items():
            gov.masking_rules[(table_id, col_name)] = (rule, dtype)

    # Build table_map, visible_columns, all_columns from raw tables
    for tbl in tables:
        table_id = tbl["id"]
        cols = tbl.get("columns", [])

        # all_columns
        gov.all_columns[table_id] = [
            (c["column_name"], c.get("data_type", "varchar")) for c in cols
        ]

        # visible_columns
        visible: set[str] = set()
        all_visible = True
        for c in cols:
            visible_to = c.get("visible_to")
            if visible_to is None:
                visible.add(c["column_name"])
            elif role_id in visible_to:
                visible.add(c["column_name"])
            else:
                all_visible = False
        gov.visible_columns[table_id] = None if all_visible else frozenset(visible)

    # table_map from compilation context — physical and semantic refs
    from provisa.compiler.naming import domain_to_sql_name
    for meta in ctx.tables.values():
        key_full = f"{meta.schema_name}.{meta.table_name}"
        key_short = meta.table_name
        key_semantic = f"{domain_to_sql_name(meta.domain_id)}.{meta.field_name}"
        gov.table_map[key_full] = meta.table_id
        gov.table_map[key_short] = meta.table_id
        gov.table_map[key_semantic] = meta.table_id

    return gov


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

_SQL_KEYWORDS = {
    "and", "or", "not", "is", "null", "in", "like", "between",
    "true", "false", "current_setting", "select", "from", "where",
    "exists", "case", "when", "then", "else", "end", "as", "on",
    "join", "left", "right", "inner", "outer", "cross", "full",
    "group", "by", "order", "having", "limit", "offset", "union",
    "all", "distinct", "with",
}

_LIMIT_RE = re.compile(r'\bLIMIT\s+(\d+)', re.IGNORECASE)


def _qualify_filter(filter_expr: str, alias: str) -> str:
    """Prefix bare column names in filter_expr with "alias"."""
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


def _table_id_for_node(table_node: exp.Table, gov_ctx: GovernanceContext) -> int | None:
    """Resolve a SQLGlot Table node to a table_id."""
    db = table_node.db
    name = table_node.name
    if db:
        full = f"{db}.{name}"
        if full in gov_ctx.table_map:
            return gov_ctx.table_map[full]
    return gov_ctx.table_map.get(name)


def _collect_alias_map(select_node: exp.Select) -> dict[str, int]:
    """Return alias → table_id for FROM + JOIN tables in a SELECT."""
    alias_map: dict[str, int] = {}
    return alias_map  # populated by caller after _table_id_for_node calls


def _get_tables_from_select(
    select_node: exp.Select,
    gov_ctx: GovernanceContext,
) -> list[tuple[exp.Table, int | None]]:
    """Return (table_node, table_id) for each table referenced in FROM/JOINs."""
    results: list[tuple[exp.Table, int | None]] = []
    from_clause = select_node.args.get("from_") or select_node.args.get("from")
    if from_clause:
        for tbl in from_clause.find_all(exp.Table):
            results.append((tbl, _table_id_for_node(tbl, gov_ctx)))
    for join in select_node.args.get("joins") or []:
        for tbl in join.find_all(exp.Table):
            results.append((tbl, _table_id_for_node(tbl, gov_ctx)))
    return results


def _alias_for(table_node: exp.Table) -> str:
    """Return alias or table name for a table node."""
    return table_node.alias or table_node.name


# --------------------------------------------------------------------------- #
# Core governance transformer                                                 #
# --------------------------------------------------------------------------- #

def _govern_select(node: exp.Select, gov_ctx: GovernanceContext) -> exp.Select:
    """Apply visibility, masking, and RLS to one SELECT node."""
    table_refs = _get_tables_from_select(node, gov_ctx)
    if not table_refs:
        return node

    # Build alias → (table_id, table_node) mapping
    alias_to_tid: dict[str, int] = {}
    for tbl, tid in table_refs:
        if tid is not None:
            alias_to_tid[_alias_for(tbl)] = tid

    if not alias_to_tid:
        return node

    # --- Rewrite SELECT projection ---
    new_exprs: list[exp.Expression] = []
    existing_exprs = node.expressions

    for expr in existing_exprs:
        if isinstance(expr, exp.Star):
            # Expand SELECT * using all_columns, filtered by visibility
            expanded = _expand_star(alias_to_tid, gov_ctx)
            new_exprs.extend(expanded)
        elif isinstance(expr, exp.Column) and isinstance(expr.table, str):
            if not _is_column_visible(expr, alias_to_tid, gov_ctx):
                pass  # drop invisible column
            else:
                new_exprs.append(_maybe_mask_column(expr, alias_to_tid, gov_ctx))
        elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Column):
            col = expr.this
            if not _is_column_visible(col, alias_to_tid, gov_ctx):
                pass  # drop invisible column
            else:
                masked = _maybe_mask_column(col, alias_to_tid, gov_ctx)
                if masked is not col:
                    new_exprs.append(exp.Alias(this=masked, alias=expr.alias))
                else:
                    new_exprs.append(expr)
        else:
            new_exprs.append(expr)

    node = node.select(*new_exprs, append=False)

    # --- Inject RLS WHERE predicates ---
    rls_filters: list[str] = []
    for tbl, tid in table_refs:
        if tid is None or tid not in gov_ctx.rls_rules:
            continue
        filter_expr = gov_ctx.rls_rules[tid]
        tbl_alias = _alias_for(tbl)
        filter_expr = _qualify_filter(filter_expr, tbl_alias)
        rls_filters.append(f"({filter_expr})")

    if rls_filters:
        for rls_filter in rls_filters:
            node = node.where(rls_filter, dialect="postgres", append=True)

    return node


def _is_column_visible(
    col: exp.Column,
    alias_to_tid: dict[str, int],
    gov_ctx: GovernanceContext,
) -> bool:
    """Return True if column passes visibility check."""
    tbl_ref = col.table
    col_name = col.name
    if not tbl_ref:
        # Unqualified — check all tables
        for tid in alias_to_tid.values():
            vis = gov_ctx.visible_columns.get(tid)
            if vis is not None and col_name not in vis:
                return False
        return True
    tid = alias_to_tid.get(tbl_ref)
    if tid is None:
        return True
    vis = gov_ctx.visible_columns.get(tid)
    return vis is None or col_name in vis


def _maybe_mask_column(
    col: exp.Column,
    alias_to_tid: dict[str, int],
    gov_ctx: GovernanceContext,
) -> exp.Expression:
    """Return a mask expression if column has a masking rule, else return col unchanged."""
    tbl_ref = col.table
    col_name = col.name

    if tbl_ref:
        tid = alias_to_tid.get(tbl_ref)
        if tid is None:
            return col
        entry = gov_ctx.masking_rules.get((tid, col_name))
        if entry:
            rule, dtype = entry
            col_sql = col.sql(dialect="postgres")
            mask_expr_str = build_mask_expression(rule, col_sql, dtype)
            return sqlglot.parse_one(mask_expr_str, read="postgres")
        # Visibility check
        vis = gov_ctx.visible_columns.get(tid)
        if vis is not None and col_name not in vis:
            return exp.Null()
        return col
    else:
        # Unqualified column — check all tables
        for tid in alias_to_tid.values():
            entry = gov_ctx.masking_rules.get((tid, col_name))
            if entry:
                rule, dtype = entry
                col_sql = col.sql(dialect="postgres")
                mask_expr_str = build_mask_expression(rule, col_sql, dtype)
                return sqlglot.parse_one(mask_expr_str, read="postgres")
        return col


def _expand_star(
    alias_to_tid: dict[str, int],
    gov_ctx: GovernanceContext,
) -> list[exp.Expression]:
    """Expand SELECT * to explicit columns, filtered by visibility and masked."""
    result: list[exp.Expression] = []
    for alias, tid in alias_to_tid.items():
        cols = gov_ctx.all_columns.get(tid, [])
        vis = gov_ctx.visible_columns.get(tid)
        for col_name, dtype in cols:
            if vis is not None and col_name not in vis:
                continue
            col_expr = exp.Column(
                this=exp.Identifier(this=col_name, quoted=True),
                table=exp.Identifier(this=alias, quoted=True),
            )
            entry = gov_ctx.masking_rules.get((tid, col_name))
            if entry:
                rule, col_dtype = entry
                col_sql = col_expr.sql(dialect="postgres")
                mask_sql = build_mask_expression(rule, col_sql, col_dtype)
                masked = sqlglot.parse_one(mask_sql, read="postgres")
                result.append(
                    exp.Alias(
                        this=masked,
                        alias=exp.Identifier(this=col_name, quoted=True),
                    )
                )
            else:
                result.append(col_expr)
    return result


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #

def apply_governance(sql: str, gov_ctx: GovernanceContext) -> str:
    """Apply governance (RLS, masking, visibility, LIMIT) to raw SQL.

    Returns governed SQL string.
    """
    tree = sqlglot.parse_one(sql, read="postgres")

    def _transform(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Select):
            return _govern_select(node, gov_ctx)
        return node

    tree = tree.transform(_transform)

    governed = tree.sql(dialect="postgres")

    # Apply LIMIT ceiling
    if gov_ctx.limit_ceiling is not None:
        governed = _apply_limit_ceiling(governed, gov_ctx.limit_ceiling)
    elif gov_ctx.sample_size is not None:
        governed = _apply_limit_ceiling(governed, gov_ctx.sample_size)

    return governed


def _apply_limit_ceiling(sql: str, ceiling: int) -> str:
    """Inject or cap LIMIT to ceiling."""
    m = _LIMIT_RE.search(sql)
    if m:
        existing = int(m.group(1))
        if existing > ceiling:
            return sql[:m.start()] + f"LIMIT {ceiling}" + sql[m.end():]
        return sql
    sql = sql.rstrip().rstrip(";")
    return f"{sql} LIMIT {ceiling}"


def extract_sources(
    sql: str,
    gov_ctx: GovernanceContext,
    ctx: CompilationContext,
) -> set[str]:
    """Parse SQL and return source_ids involved (for routing).

    Matches table names against the CompilationContext to find source_ids.
    """
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return set()

    sources: set[str] = set()
    for tbl in tree.find_all(exp.Table):
        name = tbl.name
        db = tbl.db
        full = f"{db}.{name}" if db else name

        tid = gov_ctx.table_map.get(full) or gov_ctx.table_map.get(name)
        if tid is None:
            continue
        # Find source_id from ctx
        for meta in ctx.tables.values():
            if meta.table_id == tid:
                sources.add(meta.source_id)
                break

    return sources
