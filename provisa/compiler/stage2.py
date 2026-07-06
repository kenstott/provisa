# Copyright (c) 2026 Kenneth Stott
# Canary: 0853c191-82dd-45b0-9317-468bbfdca129
# (run scripts/canary_stamp.py on this file after creating it)

"""Stage 2: SQL governance transformer (REQ-263, REQ-264).

Applies RLS, column visibility, masking, and LIMIT ceiling to raw SQL
using SQLGlot. Input: plain SQL string. Output: governed SQL string.
"""

# Requirements: REQ-002, REQ-005, REQ-038, REQ-040, REQ-262, REQ-263, REQ-264, REQ-265, REQ-266, REQ-267

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.cte_utils import physical_tables
from provisa.compiler.rls import _qualify_filter
from provisa.compiler.sql_gen import CompilationContext
from provisa.security.masking import build_mask_expression


# --------------------------------------------------------------------------- #
# GovernanceContext                                                            #
# --------------------------------------------------------------------------- #


@dataclass
class GovernanceContext:  # REQ-263, REQ-264, REQ-265
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
    # Role-level row ceiling (REQ-005) — applies to the whole query regardless of tables.
    limit_ceiling: int | None = None
    # Per-table row ceiling (REQ-005) — applied only when that table is referenced.
    table_ceilings: dict[int, int] = field(default_factory=dict)
    sample_size: int | None = None


# --------------------------------------------------------------------------- #
# Builder                                                                     #
# --------------------------------------------------------------------------- #


def resolve_row_cap(
    role: dict | None, explicit: int | None = None
) -> int | None:  # REQ-005, REQ-263
    """Resolve the row cap (REQ-005) — the single cap path for every transport.

    An explicit role/table ``max_rows`` always wins. A role holding the FULL_RESULTS
    capability gets **no default row limit at all** (``None``); every other role —
    including an unknown/None role — receives the configured ``default_row_limit``
    (env ``PROVISA_DEFAULT_ROW_LIMIT``, default 10000).
    """
    if explicit is not None:
        return int(explicit)
    from provisa.security.rights import Capability, has_capability

    if role and has_capability(role, Capability.FULL_RESULTS):
        return None
    from provisa.compiler.sql_gen import _get_default_row_limit

    return _get_default_row_limit()


def build_governance_context(  # REQ-002, REQ-005, REQ-040, REQ-263, REQ-265
    role_id: str,
    rls_context,
    masking_rules,
    ctx: CompilationContext,
    tables: list[dict],
    role: dict | None = None,
) -> GovernanceContext:
    """Build GovernanceContext from server state for a given role.

    Args:
        role_id: The requesting role.
        rls_context: RLSContext with .rules: dict[int, str].
        masking_rules: MaskingRules = dict[(table_id, role_id), dict[col, (rule, dtype)]].
        ctx: CompilationContext with .tables: dict[str, TableMeta].
        tables: Raw table dicts from state, each with
                {id, columns: [{column_name, visible_to: [role_ids], data_type}],
                 max_rows: int | None}.
        role: The requesting role's config dict (carries ``max_rows``, REQ-005).
              When None, no role-level ceiling is applied.
    """
    gov = GovernanceContext()

    # RLS rules
    gov.rls_rules = dict(rls_context.rules) if rls_context else {}

    # Row cap (REQ-005): role-level ceiling. Explicit role `max_rows` wins; otherwise a
    # role without the FULL_RESULTS capability (or an unknown role) gets the default cap.
    gov.limit_ceiling = resolve_row_cap(role, role.get("max_rows") if role else None)

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

        # per-table ceiling (REQ-005)
        tbl_max = tbl.get("max_rows")
        if tbl_max is not None:
            gov.table_ceilings[table_id] = int(tbl_max)

    # table_map from compilation context — semantic refs only
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_gen import semantic_table_name

    for meta in ctx.tables.values():
        key_semantic = f"{domain_to_sql_name(meta.domain_id)}.{semantic_table_name(meta)}"
        key_short = semantic_table_name(meta)
        gov.table_map[key_semantic] = meta.table_id
        gov.table_map[key_short] = meta.table_id

    # Also allow domain.original_table_name and schema.original_table_name refs
    # (e.g. "meta.registered_tables", "public.registered_tables")
    # ctx.tables have aliased names; raw tables have the original pre-alias names.
    for tbl in tables:
        domain_id = tbl.get("domain_id") or ""
        original_name = tbl.get("table_name") or ""
        schema_name = tbl.get("schema_name") or ""
        tbl_id = tbl["id"]
        if domain_id and original_name:
            gov.table_map[f"{domain_to_sql_name(domain_id)}.{original_name}"] = tbl_id
        if schema_name and original_name:
            gov.table_map[f"{schema_name}.{original_name}"] = tbl_id

    return gov


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _table_id_for_node(table_node: exp.Table, gov_ctx: GovernanceContext) -> int | None:
    """Resolve a SQLGlot Table node to a table_id."""
    db = table_node.db
    name = table_node.name
    if db:
        full = f"{db}.{name}"
        if full in gov_ctx.table_map:
            return gov_ctx.table_map[full]
    return gov_ctx.table_map.get(name)


def _get_tables_from_select(
    select_node: exp.Select,
    gov_ctx: GovernanceContext,
) -> list[tuple[exp.Table, int | None]]:
    """Return (table_node, table_id) for each direct table in FROM/JOINs.

    Does NOT recurse into subqueries — inner tables inside UNION ALL / derived
    tables are governed when their own SELECT node is visited.
    """

    def _is_inside_subquery(node: exp.Expression) -> bool:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        current = node.parent
        while current is not None and current is not select_node:
            if isinstance(current, exp.Subquery):
                return True
            current = current.parent
        return False

    results: list[tuple[exp.Table, int | None]] = []
    from_clause = select_node.args.get("from_") or select_node.args.get("from")
    if from_clause:
        for tbl in from_clause.find_all(exp.Table):
            if not _is_inside_subquery(tbl):
                results.append((tbl, _table_id_for_node(tbl, gov_ctx)))
    for join in select_node.args.get("joins") or []:
        for tbl in join.find_all(exp.Table):
            if not _is_inside_subquery(tbl):
                results.append((tbl, _table_id_for_node(tbl, gov_ctx)))
    return results


def _alias_for(table_node: exp.Table) -> str:
    """Return alias or table name for a table node."""
    return table_node.alias or table_node.name


# --------------------------------------------------------------------------- #
# Core governance transformer                                                 #
# --------------------------------------------------------------------------- #


def _govern_select(
    node: exp.Select, gov_ctx: GovernanceContext
) -> exp.Select:  # REQ-040, REQ-263, REQ-264
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
    new_exprs: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    existing_exprs = node.expressions

    for expr in existing_exprs:
        if isinstance(expr, exp.Star):
            # Expand SELECT * using all_columns, filtered by visibility.
            # Fall back to keeping * when column metadata is unavailable.
            expanded = _expand_star(alias_to_tid, gov_ctx)
            if expanded:
                new_exprs.extend(expanded)
            else:
                new_exprs.append(expr)
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
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
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
) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Expand SELECT * to explicit columns, filtered by visibility and masked."""
    result: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    for alias, tid in alias_to_tid.items():
        cols = gov_ctx.all_columns.get(tid, [])
        vis = gov_ctx.visible_columns.get(tid)
        for col_name, _ in cols:
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


def apply_governance(
    sql: str, gov_ctx: GovernanceContext
) -> str:  # REQ-002, REQ-038, REQ-263, REQ-264, REQ-266, REQ-267
    """Apply governance (RLS, masking, visibility, LIMIT) to raw SQL.

    Returns governed SQL string.
    """
    from provisa.observability.stage_trace import trace_stage

    trace_stage("govern.in", sql)
    tree = sqlglot.parse_one(sql, read="postgres")

    def _transform(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        if isinstance(node, exp.Select):
            return _govern_select(node, gov_ctx)
        return node

    tree = tree.transform(_transform)

    # SQLGlot's transform may visit parent Select nodes before their WHERE-clause
    # subquery children when UNION/CTE structures are present (REQ-264).  Do a
    # second bottom-up pass over any remaining Subquery nodes so that every
    # physical table reference inside IN/EXISTS/correlated subqueries is governed.
    for subq in list(tree.find_all(exp.Subquery)):
        inner = subq.this
        if isinstance(inner, exp.Select):
            governed_inner = _govern_select(inner, gov_ctx)
            if governed_inner is not inner:
                subq.set("this", governed_inner)

    governed = tree.sql(dialect="postgres")

    # Apply LIMIT ceiling (REQ-005): most restrictive of the role-level ceiling and
    # any per-table ceiling on a table referenced by the query.
    ceiling = _effective_ceiling(tree, gov_ctx)
    if ceiling is not None:
        governed = _apply_limit_ceiling(governed, ceiling)
    elif gov_ctx.sample_size is not None:
        governed = _apply_limit_ceiling(governed, gov_ctx.sample_size)

    trace_stage("govern.out", governed)
    return governed


def _effective_ceiling(tree, gov_ctx: GovernanceContext) -> int | None:  # REQ-005, REQ-263
    """Smallest applicable row ceiling: role-level plus per-table for referenced tables."""
    candidates: list[int] = []
    if gov_ctx.limit_ceiling is not None:
        candidates.append(gov_ctx.limit_ceiling)
    if gov_ctx.table_ceilings:
        for tbl_node in tree.find_all(exp.Table):
            tid = _table_id_for_node(tbl_node, gov_ctx)
            if tid is not None and tid in gov_ctx.table_ceilings:
                candidates.append(gov_ctx.table_ceilings[tid])
    return min(candidates) if candidates else None


def apply_row_cap(sql: str, cap: int | None) -> str:  # REQ-005
    """Inject or cap a query's LIMIT to ``cap`` (no-op when ``cap`` is None)."""
    return sql if cap is None else _apply_limit_ceiling(sql, cap)


def _apply_limit_ceiling(sql: str, ceiling: int) -> str:
    """Bound a query's row count by ``ceiling`` (AST — inspect the LIMIT node, never regex the text).

    A literal LIMIT above the ceiling is lowered; a missing LIMIT is added. A non-literal LIMIT
    (parameter or expression, whose value is unknown at govern time) is wrapped in a subquery with a
    constant outer LIMIT, so the ceiling still bounds the result — min(inner, ceiling) — and stays
    valid on engines that reject expressions in LIMIT.
    """
    tree = sqlglot.parse_one(sql, read="postgres")
    node = tree if isinstance(tree, (exp.Select, exp.Union)) else tree.find(exp.Select)
    if node is None:
        raise ValueError("_apply_limit_ceiling: query has no SELECT/UNION to bound")

    limit = node.args.get("limit")
    if limit is None:
        node.limit(ceiling, copy=False)
        return tree.sql(dialect="postgres")

    val = limit.expression
    if isinstance(val, exp.Literal) and not val.is_string:
        if int(val.name) > ceiling:
            node.limit(ceiling, copy=False)
            return tree.sql(dialect="postgres")
        return sql  # within the ceiling — leave the SQL byte-identical

    sub = exp.Subquery(this=tree, alias=exp.TableAlias(this=exp.to_identifier("_govern_capped")))
    return exp.select("*").from_(sub).limit(ceiling).sql(dialect="postgres")


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
    for tbl in physical_tables(tree):
        name = tbl.name
        db = tbl.db
        full = f"{db}.{name}" if db else name

        tid = gov_ctx.table_map.get(full) or gov_ctx.table_map.get(name)
        if tid is None:
            continue
        for meta in ctx.tables.values():
            if meta.table_id == tid:
                sources.add(meta.source_id)
                break

    return sources
