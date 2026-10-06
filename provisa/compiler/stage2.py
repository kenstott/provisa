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
from typing import Any

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
    # The role this context was built for, and what it may write (compiler/write_admission.py):
    # whether it holds the ``write`` right (REQ-868), and per table the columns whose
    # ``writable_by`` names it (REQ-663). A column that declares none is writable by nobody.
    role_id: str = ""
    can_write: bool = False
    writable_columns: dict[int, frozenset[str]] = field(default_factory=dict)
    # The data writes each table's source can take (executor/write_capability.py), from the
    # table's record. A table with no entry offers none.
    write_ops: dict[int, frozenset[str]] = field(default_factory=dict)
    # REQ-1494: per table, the columns this role reads as fakes (by the name a statement uses),
    # each read through the table's faked projection; and the fingerprint naming the platform key
    # the engine computes them under.
    fake_columns: dict[int, dict[str, Any]] = field(default_factory=dict)
    fake_fingerprint: str | None = None


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


def build_governance_context(  # REQ-002, REQ-005, REQ-040, REQ-263, REQ-265, REQ-971
    role_id: str,
    rls_context,
    masking_rules,
    ctx: CompilationContext,
    tables: list[dict],
    role: dict,
    relationships: list[dict] | None = None,
    source_types: dict[str, str] | None = None,
    engine=None,
) -> GovernanceContext:
    """Build GovernanceContext from server state for a given role.

    Args:
        role_id: The requesting role.
        rls_context: RLSContext with .rules: dict[int, str].
        masking_rules: MaskingRules = dict[(table_id, role_id), dict[col, (rule, dtype)]].
        ctx: CompilationContext with .tables: dict[str, TableMeta].
        tables: Raw table dicts from state, each with
                {id, source_id, columns: [{column_name, visible_to: [role_ids], data_type}],
                 max_rows: int | None}.
        role: The requesting role's own dict — REQUIRED. Its capabilities, domain scope and
              ``max_rows`` (REQ-005) are what governance decides from; there is no default role,
              because a missing one would have to be read as either nothing or everything.
        relationships: The user-defined relationship registry dicts (int source/target table ids),
              used for REQ-1132 row-level meta scoping. When None, meta rows are confined to the
              role's directly-accessible tables with NO 1-hop neighbour expansion (fail-closed).
        source_types: {source_id: source_type}, e.g. {"sales-pg": "postgresql"}. Together with
              ``engine``, enables the REQ-971 mask pushdown/streaming decision below. When either
              is None, no capability check runs — the mask expression is inlined unconditionally
              (matches every connector registered today, all of which declare predicate_pushdown).
        engine: The bound FederationEngine, used to read each masked table's connector
              ``Capability`` (REQ-897) via ``connector_pushdown``.
    """
    if role is None:
        raise ValueError(
            f"build_governance_context needs the acting role's dict for {role_id!r}: governance "
            "is decided from the role, and a missing role is an error, never a default"
        )
    gov = GovernanceContext()
    gov.role_id = role_id

    # RLS rules
    gov.rls_rules = dict(rls_context.rules) if rls_context else {}

    # Row cap (REQ-005): role-level ceiling. Explicit role `max_rows` wins; otherwise a
    # role without the FULL_RESULTS capability (or an unknown role) gets the default cap.
    gov.limit_ceiling = resolve_row_cap(role, role.get("max_rows"))

    # Masking rules — flatten to (table_id, col_name) → (rule, dtype)
    for (table_id, r_id), col_map in masking_rules.items():
        if r_id != role_id:
            continue
        for col_name, (rule, dtype) in col_map.items():
            gov.masking_rules[(table_id, col_name)] = (rule, dtype)
    _bind_fakes(gov)

    # Column visibility is each column's visible_to grant and nothing above it: no capability sees
    # every column regardless (REQ-1327). The lockdown domains (ops) need an explicit grant; the
    # meta domain is governed by the tiered rule below.
    from provisa.security.rights import (
        GOVERNANCE_META_COLUMNS,
        META_DOMAIN_ID,
        META_ROW_SCOPED_VIEWS,
        Capability,
        compute_meta_row_scope,
        has_capability,
    )

    _has_view_gov = has_capability(role, Capability.VIEW_GOVERNANCE)
    gov.can_write = has_capability(role, Capability.WRITE)
    # table_id → domain_id, so meta (catalog) tables can be governed by the tiered rule (REQ-1132)
    # rather than their static seed (meta columns are seeded visible_to: [] = nobody).
    _domain_by_tid: dict[int | None, str | None] = {
        getattr(tm, "table_id", None): getattr(tm, "domain_id", None)
        for tm in getattr(ctx, "tables", {}).values()
    }

    # Build table_map, visible_columns, all_columns from raw tables
    for tbl in tables:
        table_id = tbl["id"]
        cols = tbl.get("columns", [])

        # all_columns
        gov.all_columns[table_id] = [
            (c["column_name"], c.get("data_type", "varchar")) for c in cols
        ]
        gov.writable_columns[table_id] = frozenset(
            c["column_name"] for c in cols if role_id in (c.get("writable_by") or [])
        )
        if "write_ops" in tbl:
            gov.write_ops[table_id] = frozenset(tbl["write_ops"])

        # visible_columns — None means "all visible" (no V003 filtering for this table)
        if _domain_by_tid.get(table_id) == META_DOMAIN_ID:
            # Meta (catalog) domain: CORE/structural columns are visible for discovery; GOVERNANCE
            # columns (visible_to, masks, view_sql, …) require the view_governance capability. Which
            # META ROWS a role sees (its readable tables + 1-hop neighbours vs the whole catalog with
            # a meta grant) is enforced by row-level scoping (REQ-1132), not here.
            gov.visible_columns[table_id] = frozenset(
                c["column_name"]
                for c in cols
                if _has_view_gov or c["column_name"] not in GOVERNANCE_META_COLUMNS
            )
        else:
            from provisa.compiler.schema_gen import _LOCKDOWN_DOMAINS

            _tbl_domain = tbl.get("domain_id")
            visible: set[str] = set()
            all_visible = True
            for c in cols:
                visible_to = c.get("visible_to")
                # REQ-1730 gap: the DB's visible_to column is JSON NOT NULL (schema_org.py), so a
                # freshly-registered, ungranted column is an EMPTY LIST, never a true SQL NULL —
                # `visible_to is None` never actually fires against real data, silently rejecting
                # every such column instead of applying schema_gen.py's own documented contract
                # ("visible_to=[] means unrestricted (visible to all roles)", _build_visible_tables
                # above) — the two governance checks disagreed on the exact same input, verified
                # live: schema_gen.py's precomputed schema correctly listed a grpc_remote table's
                # columns as visible, while this function's V003 check rejected every one of them.
                if not visible_to and _tbl_domain not in _LOCKDOWN_DOMAINS:
                    visible.add(c["column_name"])
                # REQ-1742 gap: "*" is the codebase's "everyone" sentinel (Metric.visible_to,
                # core/models.py, defaults to it; schema_gen.py's metrics branch already
                # special-cases it) but this column-visibility check never did — a literal
                # `role_id in visible_to` treats ["*"] as "visible only to a role named '*'",
                # silently rejecting every real role even after a successful grant.
                elif visible_to and ("*" in visible_to or role_id in visible_to):
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
    from provisa.compiler.sql_rewrite import semantic_table_name

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

    # REQ-1132: row-level meta scoping. A DEFAULT-tier role sees meta ROWS only for its reachable
    # neighbourhood (directly-accessible tables + 1-hop semantic neighbours); admins and meta-grant
    # roles see all rows (scope None → no filter). The scope is injected as an RLS predicate on the
    # two described-table meta views (registered_tables_meta.id / table_columns_meta.table_id), so it
    # flows through the SAME apply_governance WHERE-injection as every other RLS rule — one code path.
    meta_scope = compute_meta_row_scope(role, tables, relationships)
    if meta_scope is not None:
        id_list = ",".join(str(i) for i in sorted(meta_scope)) or "-1"  # empty → matches no row
        for tm in ctx.tables.values():
            if getattr(tm, "domain_id", None) != META_DOMAIN_ID:
                continue
            scope_col = META_ROW_SCOPED_VIEWS.get(getattr(tm, "table_name", ""))
            if scope_col is None:
                continue
            predicate = f"{scope_col} IN ({id_list})"
            existing = gov.rls_rules.get(tm.table_id)
            # AND with any pre-existing rule (e.g. tenant RLS) — never replace another guard.
            gov.rls_rules[tm.table_id] = (
                f"({existing}) AND ({predicate})" if existing else predicate
            )

    # REQ-971: mask pushdown/streaming decision. When a masked table's connector cannot push
    # the predicate down and cannot stream, fail loud here rather than silently inlining an
    # unsupported expression that a downstream connector cannot evaluate.
    if source_types is not None and engine is not None:
        from provisa.federation.promote import plan_mask_evaluation

        source_id_by_table_id = {tbl["id"]: tbl.get("source_id") for tbl in tables}
        masked_table_ids = {tid for (tid, _) in gov.masking_rules}
        for tid in masked_table_ids:
            source_id = source_id_by_table_id.get(tid)
            if not source_id:
                continue
            source_type = source_types.get(source_id)
            if not source_type:
                continue
            cap = engine.connector_pushdown(source_type)
            plan_mask_evaluation(cap, can_stream=False)

    return gov


def _bind_fakes(gov: GovernanceContext) -> None:  # REQ-1494
    """The role's faked columns, by table, and the fingerprint of the key they are computed
    under. A faked column is read through its table's faked projection, never substituted per
    reference."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.fakes.checks import family
    from provisa.fakes.kinds import parse
    from provisa.fakes.read_sql import Column
    from provisa.security.masking import MaskType

    for (tid, col_name), (rule, dtype) in gov.masking_rules.items():
        if rule.mask_type != MaskType.fake:
            continue
        name = apply_sql_name(col_name)
        gov.fake_columns.setdefault(tid, {})[name] = Column(
            name,
            dtype,
            family(dtype),
            parse(rule.fake),
            rule.fake_stable,
            rule.fake_measured,
            rule.fake_stable_version,
        )
    if gov.fake_columns:
        from provisa.fakes.digest import fingerprint, platform_key

        gov.fake_fingerprint = fingerprint(platform_key())


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

    # --- REQ-1494: a table with faked columns is read through its faked projection, which applies
    # the row filter to the real values and defines each fake once; every use of a faked column
    # below reads the projection's fake.
    projected: set[int] = set()
    for tbl, tid in table_refs:
        if tid is not None and tid in gov_ctx.fake_columns:
            _project_faked(node, tbl, tid, gov_ctx)
            projected.add(id(tbl))

    # --- Every other reference to a governed column (expressions, WHERE, JOIN ON, GROUP BY,
    # HAVING, ORDER BY, windows): the role computes only over what it can see. Done before the
    # row filters are added below, which are the policy and read the real values.
    _govern_references(node, alias_to_tid, gov_ctx)

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
                masked = _maybe_mask_column(expr, alias_to_tid, gov_ctx)
                if masked is expr:
                    new_exprs.append(expr)
                else:
                    masked.meta[_GOVERNED] = True
                    # The masked value keeps the column's name: a client reads `email`, not a
                    # nameless expression (`?column?`). The identifier is the column's own, so it
                    # is quoted exactly as the statement wrote it.
                    new_exprs.append(exp.Alias(this=masked, alias=expr.this.copy()))
        elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Column):
            col = expr.this
            if not _is_column_visible(col, alias_to_tid, gov_ctx):
                pass  # drop invisible column
            else:
                masked = _maybe_mask_column(col, alias_to_tid, gov_ctx)
                if masked is not col:
                    masked.meta[_GOVERNED] = True
                    new_exprs.append(exp.Alias(this=masked, alias=expr.alias))
                else:
                    new_exprs.append(expr)
        else:
            new_exprs.append(expr)

    node = node.select(*new_exprs, append=False)

    # --- Inject RLS WHERE predicates ---
    rls_filters: list[str] = []
    for tbl, tid in table_refs:
        if tid is None or tid not in gov_ctx.rls_rules or id(tbl) in projected:
            continue
        filter_expr = gov_ctx.rls_rules[tid]
        tbl_alias = _alias_for(tbl)
        # REQ-1686: a session-variable term takes the compared column's type.
        column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
        filter_expr = _qualify_filter(filter_expr, tbl_alias, column_types)
        rls_filters.append(f"({filter_expr})")

    if rls_filters:
        for rls_filter in rls_filters:
            node = node.where(rls_filter, dialect="postgres", append=True)

    return node


#: Marks an expression governance put in a column's place (a mask, or NULL for a hidden column),
#: so a later pass over the same statement does not govern what is inside it again.
_GOVERNED = "provisa_governed"


def _project_faked(
    node: exp.Select, tbl: exp.Table, tid: int, gov_ctx: GovernanceContext
) -> None:  # REQ-1494
    """Put the faked projection of ``tbl`` in its place in ``node``, under the same alias."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.fakes.checks import family
    from provisa.fakes.projection import faked_projection

    alias = _alias_for(tbl)
    base = tbl.copy()
    base.set("alias", None)
    row_filter = None
    if tid in gov_ctx.rls_rules:
        column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
        row_filter = _qualify_filter(gov_ctx.rls_rules[tid], alias, column_types)
    columns = [
        (apply_sql_name(name), dtype, family(dtype)) for name, dtype in gov_ctx.all_columns[tid]
    ]
    assert gov_ctx.fake_fingerprint is not None  # bound with the faked columns
    sql = faked_projection(
        base.sql(dialect="postgres"),
        alias,
        columns,
        gov_ctx.fake_columns[tid],
        gov_ctx.fake_fingerprint,
        row_filter,
    )
    projection = sqlglot.parse_one(f"SELECT * FROM {sql} AS {_quote(alias)}", read="postgres")
    subquery = projection.args["from_"].this
    subquery.meta[_GOVERNED] = True  # its own references are the projection's, already governed
    # A reference qualified by the table's schema now names the projection by its alias alone.
    for col in node.find_all(exp.Column):
        if col.table == tbl.name and col.args.get("db") is not None and alias == tbl.name:
            col.set("db", None)
            col.set("catalog", None)
    tbl.replace(subquery)


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _already_governed(col: exp.Column) -> bool:
    parent = col.parent
    while parent is not None:
        if parent.meta.get(_GOVERNED):
            return True
        parent = parent.parent
    return False


def _govern_references(
    node: exp.Select, alias_to_tid: dict[str, int], gov_ctx: GovernanceContext
) -> None:
    """Replace, in place, every reference to a masked column with its mask and every reference
    to a hidden column with NULL — anywhere in ``node`` except its bare select-list columns,
    which the projection rewrite names and drops. A column of this SELECT's tables referenced
    from a nested SELECT by its alias (a correlated subquery) is replaced too."""
    top_level = {id(e) for e in node.expressions}
    for col in list(node.find_all(exp.Column)):
        parent = col.parent
        if id(col) in top_level or (
            isinstance(parent, exp.Alias) and id(parent) in top_level and parent.this is col
        ):
            continue
        if _already_governed(col):
            continue
        if col.find_ancestor(exp.Select) is not node and col.table not in alias_to_tid:
            continue  # a nested SELECT's own column: governed when that SELECT is
        if not _is_column_visible(col, alias_to_tid, gov_ctx):
            replacement: exp.Expr = exp.Null()  # pyright: ignore[reportPrivateImportUsage]
        else:
            replacement = _maybe_mask_column(col, alias_to_tid, gov_ctx)
            if replacement is col:
                continue
        replacement.meta[_GOVERNED] = True
        _replace_reference(col, replacement)


def _replace_reference(col: exp.Column, replacement: exp.Expr) -> None:  # pyright: ignore[reportPrivateImportUsage]
    """Put ``replacement`` where ``col`` is. A constant cannot stand as an ORDER BY or GROUP BY
    key on every engine (PostgreSQL reads a bare constant there as a position): an ordering by a
    constant orders nothing and is dropped; a grouping key keeps its place as a typed value."""
    parent = col.parent
    constant = isinstance(replacement, (exp.Literal, exp.Null, exp.Boolean))
    if constant and isinstance(parent, exp.Ordered) and parent.this is col:
        order = parent.parent
        parent.pop()
        if isinstance(order, exp.Order) and not order.expressions:
            order.pop()
        return
    if constant and isinstance(parent, exp.Group):
        typed = exp.cast(replacement, "VARCHAR")
        typed.meta[_GOVERNED] = True
        col.replace(typed)
        return
    col.replace(replacement)


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
        if entry and not _is_fake(entry[0]):
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
            if entry and not _is_fake(entry[0]):
                rule, dtype = entry
                col_sql = col.sql(dialect="postgres")
                mask_expr_str = build_mask_expression(rule, col_sql, dtype)
                return sqlglot.parse_one(mask_expr_str, read="postgres")
        return col


def _is_fake(rule: Any) -> bool:
    """A fake mask: read through the table's faked projection, not substituted (REQ-1494)."""
    from provisa.security.masking import MaskType

    return rule.mask_type == MaskType.fake


def _expand_star(
    alias_to_tid: dict[str, int],
    gov_ctx: GovernanceContext,
) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Expand SELECT * to explicit columns, filtered by visibility and masked."""
    from provisa.compiler.naming import apply_sql_name

    result: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    for alias, tid in alias_to_tid.items():
        cols = gov_ctx.all_columns.get(tid, [])
        vis = gov_ctx.visible_columns.get(tid)
        for col_name, _ in cols:
            if vis is not None and col_name not in vis:
                continue
            # REQ-301: the physical column as materialized (e.g. an API-backed table's
            # engine-cache CTE) is canonicalized via apply_sql_name, same as the compiled
            # (GraphQL/Cypher) path's ctx.physical_to_sql — reference it under that name here
            # too, or an OpenAPI-sourced camelCase catalog name (photoUrls) never matches the
            # snake_case CTE column it was materialized under (photo_urls).
            sql_col_name = apply_sql_name(col_name)
            col_expr = exp.Column(
                this=exp.Identifier(this=sql_col_name, quoted=True),
                table=exp.Identifier(this=alias, quoted=True),
            )
            entry = gov_ctx.masking_rules.get((tid, col_name))
            if entry and not _is_fake(entry[0]):
                rule, col_dtype = entry
                col_sql = col_expr.sql(dialect="postgres")
                mask_sql = build_mask_expression(rule, col_sql, col_dtype)
                masked = sqlglot.parse_one(mask_sql, read="postgres")
                result.append(
                    exp.Alias(
                        this=masked,
                        alias=exp.Identifier(this=sql_col_name, quoted=True),
                    )
                )
            else:
                result.append(col_expr)
    return result


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def _govern_selects(tree: exp.Expr, gov_ctx: GovernanceContext) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """Govern every SELECT in ``tree`` — the outer one, and each CTE, derived table, scalar or
    IN subquery, EXISTS body and set-operation branch — deepest first, each once. (A transform
    that replaced the outer SELECT never reached the SELECTs inside it, so a CTE or an EXISTS
    over another table ran with none of the reader's rules.)"""
    for select in reversed(list(tree.find_all(exp.Select))):
        governed_select = _govern_select(select, gov_ctx)
        if governed_select is select:
            continue
        if select is tree:
            tree = governed_select
        else:
            select.replace(governed_select)
    return tree


def govern_fragment(sql: str, gov_ctx: GovernanceContext) -> str:
    """``sql`` — a read that becomes part of a larger statement (a view's body) — with the
    role's row filters, masks and column visibility applied. No row cap and no sampling: those
    bound the statement the fragment is put into."""
    return _govern_selects(sqlglot.parse_one(sql, read="postgres"), gov_ctx).sql(dialect="postgres")


def apply_governance(
    sql: str,
    gov_ctx: GovernanceContext,
    session_vars: dict[str, str] | None = None,
    params: list | None = None,
) -> str:  # REQ-002, REQ-038, REQ-263, REQ-264, REQ-266, REQ-267
    """Apply governance (RLS, masking, visibility, LIMIT) to raw SQL — and, to a data write, its
    admission (compiler/write_admission.py: the write right, the columns' ``writable_by``, the
    role's row filter on the rows it touches and on the rows it leaves behind).

    ``session_vars`` are the request's session variables and ``params`` its bound values; a
    write's row-filter check reads both. A read's are resolved by the pipeline after this stage.

    Returns governed SQL string.
    """
    from provisa.compiler.write_admission import admit_write, is_write
    from provisa.observability.stage_trace import trace_stage

    trace_stage("govern.in", sql)
    tree = sqlglot.parse_one(sql, read="postgres")
    writes = is_write(tree)  # pyright: ignore[reportArgumentType]  # sqlglot stub types parse_one as Expr
    if writes:
        tree = admit_write(tree, gov_ctx, session_vars, params)  # pyright: ignore[reportArgumentType]

    tree = _govern_selects(tree, gov_ctx)

    governed = tree.sql(dialect="postgres")

    # Apply LIMIT ceiling (REQ-005): most restrictive of the role-level ceiling and
    # any per-table ceiling on a table referenced by the query.
    # A ceiling bounds the rows a READ returns; a write returns none, and what it may write
    # is its admission's to decide.
    ceiling = None if writes else _effective_ceiling(tree, gov_ctx)
    if ceiling is not None:
        governed = _apply_limit_ceiling(governed, ceiling)
    elif gov_ctx.sample_size is not None and not writes:
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
    # Parse failure must fail loud: a swallowed error mis-routes by yielding no sources.
    tree = sqlglot.parse_one(sql, read="postgres")

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


def reduce_sources_for_routing(  # REQ-863
    sql: str,
    gov_ctx: GovernanceContext,
    ctx: CompilationContext,
    inlined_table_names: set[str],
) -> set[str]:
    """Routing source set AFTER the post-governance optimization stage (REQ-863).

    The optimization stage may REMOVE sources — hot/API tables inlined as VALUES CTEs, or
    union-pruned. Routing (decide_route) MUST consume this reduced set, not the pre-optimization
    governed source set, so a query whose second source is fully inlined collapses to a single
    live source and routes DIRECT instead of federated.

    A source is dropped only when every one of its tables referenced by the query was inlined or
    pruned; a source with any remaining live table stays. ``inlined_table_names`` are physical
    optimizer-side names; the query may carry the same tables under semantic names, so identity is
    reconciled through ``table_id`` (table_map keys both name forms to the same id).
    """
    tree = sqlglot.parse_one(sql, read="postgres")

    inlined_tids: set[int] = set()
    unresolved: set[str] = set()
    for nm in inlined_table_names:
        tid = gov_ctx.table_map.get(nm) or gov_ctx.table_map.get(nm.split(".")[-1])
        if tid is not None:
            inlined_tids.add(tid)
        else:
            unresolved.add(nm.split(".")[-1])
    # The optimizer names a table by its physical name; table_map keys semantic names only, so a
    # table registered under a display alias (get_inventory shown as inventory) is matched by
    # its physical name here.
    for meta in ctx.tables.values():
        if meta.table_name in unresolved or meta.original_table_name in unresolved:
            inlined_tids.add(meta.table_id)

    live: set[str] = set()
    for tbl in physical_tables(tree):
        name = tbl.name
        db = tbl.db
        full = f"{db}.{name}" if db else name
        tid = gov_ctx.table_map.get(full) or gov_ctx.table_map.get(name)
        if tid is None or tid in inlined_tids:
            continue
        for meta in ctx.tables.values():
            if meta.table_id == tid:
                live.add(meta.source_id)
                break

    return live
