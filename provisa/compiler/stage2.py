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
from provisa.compiler.sql_where import SERVED_WITH_EVERY_TABLE
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
    # REQ-1959: tables outside the role's domains that it is served anyway, because they publish
    # a column to it. A direct read of one is not a domain violation (V001); the columns it may
    # read there are the published ones (visible_columns).
    public_tables: frozenset[int] = frozenset()
    # "schema.table" or "table" → table_id
    table_map: dict[str, int] = field(default_factory=dict)
    # table_id → [(col_name, data_type)]
    all_columns: dict[int, list[tuple[str, str]]] = field(default_factory=dict)
    # Role-level row ceiling (REQ-005) — applies to the whole query regardless of tables.
    limit_ceiling: int | None = None
    # Per-table row ceiling (REQ-005) — applied only when that table is referenced.
    table_ceilings: dict[int, int] = field(default_factory=dict)
    # REQ-1949: the ceiling that bounds the statement last governed with this context, when a
    # ceiling and not the statement's own LIMIT is what bounds it (compiler/row_limit.RowLimit).
    row_limit: Any = None
    sample_size: int | None = None
    # The role this context was built for, and what it may write (compiler/write_admission.py):
    # whether it holds the ``write`` right (REQ-868), and per table its write grant for each
    # operation (REQ-2074). A role with no grant for an operation may not perform it.
    role_id: str = ""
    can_write: bool = False
    write_grants: dict[int, dict[str, dict]] = field(default_factory=dict)
    # The data writes each table's source can take (executor/write_capability.py), from the
    # table's record. A table with no entry offers none.
    write_ops: dict[int, frozenset[str]] = field(default_factory=dict)
    # Whether a write to each table can return the rows it wrote, from the same record: a
    # RETURNING clause on a table that cannot is refused at admission.
    write_returns_rows: dict[int, bool] = field(default_factory=dict)
    # The statement forms a write to each table does not carry, from the same record.
    write_refused_forms: dict[int, frozenset[str]] = field(default_factory=dict)
    # REQ-1494: per table, the columns this role reads as fakes (by the name a statement uses),
    # each read through the table's faked projection; and the fingerprint naming the platform key
    # the engine computes them under.
    fake_columns: dict[int, dict[str, Any]] = field(default_factory=dict)
    fake_fingerprint: str | None = None
    # REQ-1942: per table with kept mutations (a Reversible environment), its change log's
    # address and primary key; the table is read with the log applied.
    kept: dict[int, tuple[str, list[str]]] = field(default_factory=dict)
    # REQ-421/423: per table, its embedding columns (by the name a statement uses) and each one's
    # model; and the registered models' dimensions, which the pipeline sets for the statement.
    embedding_columns: dict[int, dict[str, str]] = field(default_factory=dict)
    embedding_dims: dict[str, int] = field(default_factory=dict)
    # Per table, each published column name (lowercased) that is not its physical name, to that
    # physical name (sql_rewrite.published_columns): a statement is governed before its published
    # names are lowered, and the grants and masks above are by physical name.
    published: dict[int, dict[str, str]] = field(default_factory=dict)
    # (table id, physical name) -> the name the column is published under (physical_to_sql).
    published_name: dict[tuple[int, str], str] = field(default_factory=dict)
    # The joins the registered relationships approve, (table, table, column, column) by physical
    # name (sql_validator.approved_joins): a join key the role cannot select is still read by the
    # engine in such a join, never returned (REQ-603).
    join_keys: set[tuple[int, int, str, str]] = field(default_factory=set)
    # REQ-1620: a meta-role's members, per table (compiler.rls.MemberReads). A table with a column
    # its members read differently is read through its member projection (_project_members).
    member_reads: dict[int, Any] = field(default_factory=dict)


def registered_name(gov_ctx: GovernanceContext, table_id: int, name: str) -> str:  # REQ-040
    """The registered (physical) name of the column of ``table_id`` a statement reads as
    ``name``: its published name resolved as lowering resolves it; a physical name as it is."""
    return gov_ctx.published.get(table_id, {}).get(name.lower(), name)


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
    if ctx.refusal is not None:
        # REQ-1942: a Test (fake) environment holding a sensitive column with no fake serves no
        # statement until each has one.
        raise PermissionError(ctx.refusal)
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
    _with_member_reads(gov, rls_context)
    _bind_fakes(gov)
    gov.kept = dict(ctx.kept)  # REQ-1942
    from provisa.compiler.sql_rewrite import published_columns
    from provisa.compiler.sql_validator import approved_joins

    gov.published = published_columns(ctx)
    gov.published_name = dict(ctx.physical_to_sql)
    gov.join_keys = approved_joins(ctx)

    # Column visibility is each column's visible_to grant and nothing above it: no capability sees
    # every column regardless (REQ-1327). The lockdown domains (ops) need an explicit grant; the
    # meta domain is governed by the tiered rule below.
    from provisa.security.rights import (
        GOVERNANCE_META_COLUMNS,
        META_DOMAIN_ID,
        META_ROW_SCOPED_VIEWS,
        Capability,
        column_served,
        compute_meta_row_scope,
        has_capability,
        reaches_domain,
        served_across_domains,
    )

    _has_view_gov = has_capability(role, Capability.VIEW_GOVERNANCE)
    gov.can_write = has_capability(role, Capability.WRITE)
    # table_id → domain_id, so meta (catalog) tables can be governed by the tiered rule (REQ-1132)
    # rather than their static seed (meta columns are seeded visible_to: [] = nobody).
    _domain_by_tid: dict[int | None, str | None] = {
        getattr(tm, "table_id", None): getattr(tm, "domain_id", None)
        for tm in getattr(ctx, "tables", {}).values()
    }

    _public: set[int] = set()
    # Build table_map, visible_columns, all_columns from raw tables
    for tbl in tables:
        table_id = tbl["id"]
        cols = tbl.get("columns", [])

        # all_columns
        gov.all_columns[table_id] = [
            (c["column_name"], c.get("data_type", "varchar"))
            for c in cols
            # A column the relation a statement reads does not hold is not expanded from `*`: a
            # remote table's reference column, or an API parameter (a native filter column, an
            # input the payload never carries -- REQ-301).
            if (table_id, c["column_name"]) not in ctx.columns_outside_relation
            and not c.get("native_filter_type")
        ]
        embeddings = {
            c["column_name"]: c["embedding_model"]
            for c in cols
            if c.get("embedding") and c.get("embedding_model")
        }
        if embeddings:
            gov.embedding_columns[table_id] = embeddings
        gov.write_grants[table_id] = {
            g["operation"]: g for g in tbl.get("write_grants") or [] if g["role"] == role_id
        }
        if "write_ops" in tbl:
            gov.write_ops[table_id] = frozenset(tbl["write_ops"])
            gov.write_returns_rows[table_id] = tbl["write_returns_rows"]
            gov.write_refused_forms[table_id] = frozenset(tbl["write_refused_forms"])

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
            # ONE rule with the schema build (security.rights.column_served): the column's grant
            # — empty is everyone, "*", or the role; an explicit grant only in a lockdown domain
            # and on a restricted column — and, in a domain the role does not reach, only a
            # published column (REQ-1959).
            _tbl_domain = tbl.get("domain_id")
            # A table registered with no domain is gated by none, as the direct-read check
            # (V001) treats it.
            _reaches = not _tbl_domain or reaches_domain(role["domain_access"], _tbl_domain)
            _subject = {"id": role_id}
            visible = {
                c["column_name"]
                for c in cols
                if column_served(_subject, _tbl_domain, c, reaches=_reaches)
            }
            all_visible = len(visible) == len(cols)
            if not _reaches and served_across_domains(_subject, tbl):
                _public.add(table_id)
            reads = gov.member_reads.get(table_id)
            if reads is not None:
                # REQ-1620: a column any member reading the table is served; on the rows only
                # members not served it admit, the member projection reads it as NULL.
                from provisa.compiler.rls import HIDDEN

                visible = {c for c, ts in reads.columns.items() if any(t[0] != HIDDEN for t in ts)}
                all_visible = len(visible) == len(cols)
            gov.visible_columns[table_id] = None if all_visible else frozenset(visible)

        # per-table ceiling (REQ-005)
        tbl_max = tbl.get("max_rows")
        if tbl_max is not None:
            gov.table_ceilings[table_id] = int(tbl_max)

    gov.public_tables = frozenset(_public)

    # table_map from compilation context — semantic refs only
    from provisa.compiler.naming import (
        physical_relation_name,
        semantic_relation_names,
        stored_relation_names,
    )
    from provisa.compiler.sql_rewrite import semantic_table_name

    for meta in ctx.tables.values():
        for said in semantic_relation_names(meta.domain_id, semantic_table_name(meta)):
            gov.table_map[said] = meta.table_id
        # And its catalog-physical name: a view's body is read into a statement lowered to it,
        # and two sources may hold tables of one schema.table (naming.table_id_of).
        gov.table_map[
            physical_relation_name(meta.catalog_name, meta.schema_name, meta.table_name)
        ] = meta.table_id

    # Also allow domain.original_table_name and schema.original_table_name refs
    # (e.g. "meta.registered_tables", "public.registered_tables")
    # ctx.tables have aliased names; raw tables have the original pre-alias names.
    for tbl in tables:
        for said in stored_relation_names(
            tbl.get("domain_id") or "", tbl.get("schema_name") or "", tbl.get("table_name") or ""
        ):
            gov.table_map[said] = tbl["id"]

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


def _with_member_reads(gov: GovernanceContext, rls_context) -> None:  # REQ-1620
    """A meta-role reads each row as the member(s) whose filter admitted it. A column every member
    reading the table treats alike is masked as they read it, through the role's masking rules; a
    column they treat differently is read through the table's member projection, so it has no
    masking rule of the role's own."""
    from provisa.security.masking import MaskType

    member_reads = getattr(rls_context, "member_reads", None) or {}
    gov.member_reads = dict(member_reads)
    for tid, reads in member_reads.items():
        for column, treatments in reads.columns.items():
            gov.masking_rules.pop((tid, column), None)
            if reads.uniform(column):
                rank, rule, dtype = treatments[0]
                if rule is not None:
                    gov.masking_rules[(tid, column)] = (rule, dtype)
                continue
            # A column faked for some members is faked in the table's faked projection, which the
            # member projection reads for those members (and the real value for the others).
            # Members faking it by different mask entries all read the earlier entry's fake: the
            # projection computes one fake per column, and neither group reads a real value.
            fakes = sorted(
                (t for t in treatments if t[1] is not None and t[1].mask_type == MaskType.fake),
                key=lambda t: t[0],
            )
            if fakes:
                gov.masking_rules[(tid, column)] = (fakes[0][1], fakes[0][2])


def _bind_fakes(gov: GovernanceContext) -> None:  # REQ-1494
    """The role's faked columns, by table, and the fingerprint of the key they are computed
    under. A faked column is read through its table's faked projection, never substituted per
    reference."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.fakes.checks import family
    from provisa.fakes.kinds import parse
    from provisa.fakes.read_sql import Column
    from provisa.security.masking import MaskType

    from provisa.fakes.measurement import MEASURING

    if MEASURING.get():
        # REQ-1942: the model build's measurement of what fakes compute from reads the real values;
        # its faked columns are read as they are, never substituted (see _is_fake).
        return
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
    """Resolve a SQLGlot Table node to a table_id (``naming.table_id_of``)."""
    from provisa.compiler.naming import table_id_of

    return table_id_of(table_node, gov_ctx.table_map)


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
    node: exp.Select, gov_ctx: GovernanceContext, *, final: bool = False
) -> exp.Select:  # REQ-040, REQ-263, REQ-264
    """Apply visibility, masking, and RLS to one SELECT node.

    ``final``: this SELECT's projection is the statement's answer, so a numeric column the model
    records no size for is projected as floating point (:func:`_as_read`, REQ-1961)."""
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
    # REQ-1620: per alias read through a member projection, the columns whose real value an
    # approved join reads (REQ-603), each under its internal name.
    real_keys: dict[str, dict[str, str]] = {}
    for tbl, tid in table_refs:
        faked = None
        members = None if tid is None else _differing_members(tid, gov_ctx)
        carried = None
        if members is not None:
            assert tid is not None
            # REQ-1620: each member's flag and the real values its members read, computed on the
            # real row -- before any fake -- and carried through the projections below.
            carried = _member_carried(tid, members, gov_ctx, _alias_for(tbl))
        if tid is not None and tid in gov_ctx.fake_columns:
            faked = _project_faked(node, tbl, tid, gov_ctx, carried)
            projected.add(id(tbl))
        current: exp.Expr | None = faked  # pyright: ignore[reportPrivateImportUsage]
        if tid is not None and tid in gov_ctx.kept:
            # REQ-1942: the environment's kept mutations, over the faked rows where there are
            # fakes -- a written row is never faked again.
            current = _project_kept(node, tbl, faked, tid, gov_ctx, members)
            projected.add(id(tbl))
        if members is not None:
            assert tid is not None
            # REQ-1620: each row read as the member(s) whose filter admitted it.
            real_keys[_alias_for(tbl)] = _project_members(
                node, tbl, current, tid, members, gov_ctx, carried
            )
            projected.add(id(tbl))
    if projected:
        _read_projected_names(
            node,
            {_alias_for(t): tid for t, tid in table_refs if tid is not None and id(t) in projected},
            table_refs,
            gov_ctx,
        )

    # --- REQ-423/426: a similarity search names an embedding column the role is served, and is
    # completed with its model and dimensions -- before a hidden column becomes NULL below, so a
    # search of one is refused by name rather than scoring nothing.
    from provisa.vector.similarity import govern_calls

    if any(True for _ in node.find_all(exp.Anonymous)):
        govern_calls(
            node,
            alias_to_tid,
            gov_ctx.embedding_columns,
            gov_ctx.embedding_dims,
            lambda col: _is_column_visible(col, alias_to_tid, gov_ctx),
            gov_ctx.role_id,
        )

    # --- Every other reference to a governed column (expressions, WHERE, JOIN ON, GROUP BY,
    # HAVING, ORDER BY, windows): the role computes only over what it can see. Done before the
    # row filters are added below, which are the policy and read the real values.
    _govern_references(node, alias_to_tid, gov_ctx, real_keys)

    # --- Rewrite SELECT projection ---
    new_exprs: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    existing_exprs = node.expressions

    for expr in existing_exprs:
        if isinstance(expr, exp.Star):
            # A star is the role's served columns of every governed table it covers, each masked
            # as the column named alone is. It is never kept as ``*``: that reads every column.
            new_exprs.extend(_expand_star(alias_to_tid, gov_ctx, final=final))
        elif isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
            # ``t.*``: the same, for the one table it names. A derived relation's star reads what
            # that relation's own governed SELECT projects.
            tid = alias_to_tid.get(expr.table)
            if tid is None:
                new_exprs.append(expr)
            else:
                new_exprs.extend(_expand_star({expr.table: tid}, gov_ctx, final=final))
        elif isinstance(expr, exp.Column) and isinstance(expr.table, str):
            if not _is_column_visible(expr, alias_to_tid, gov_ctx):
                pass  # drop invisible column
            else:
                masked = _maybe_mask_column(expr, alias_to_tid, gov_ctx)
                read = _as_read(masked, expr, alias_to_tid, gov_ctx) if final else masked
                if read is expr:
                    new_exprs.append(expr)
                else:
                    read.meta[_GOVERNED] = True
                    # The masked value keeps the column's name: a client reads `email`, not a
                    # nameless expression (`?column?`). The identifier is the column's own, so it
                    # is quoted exactly as the statement wrote it.
                    new_exprs.append(exp.Alias(this=read, alias=expr.this.copy()))
        elif isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Column):
            col = expr.this
            if not _is_column_visible(col, alias_to_tid, gov_ctx):
                pass  # drop invisible column
            else:
                masked = _maybe_mask_column(col, alias_to_tid, gov_ctx)
                read = _as_read(masked, col, alias_to_tid, gov_ctx) if final else masked
                if read is not col:
                    read.meta[_GOVERNED] = True
                    new_exprs.append(exp.Alias(this=read, alias=expr.args["alias"].copy()))
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


def _read_projected_names(
    node: exp.Select,
    projected: dict[str, int],
    table_refs: list[tuple[exp.Table, int | None]],
    gov_ctx: GovernanceContext,
) -> None:  # REQ-1494, REQ-1942
    """Name, IN PLACE, each reference in ``node`` to a column of a projected table (``projected``:
    alias -> table id) as the projection names it (``apply_sql_name`` of the physical name). A
    statement names a column by its published name, and lowering resolves a published name only
    on a table, not on the projection that now stands in its place. A bare select-list column
    renamed keeps its published name as its output name. An unqualified name is renamed only
    where this SELECT reads that one table, as lowering resolves it only where one source of its
    scope publishes it."""
    from provisa.compiler.naming import apply_sql_name

    sole = len(table_refs) == 1 and len(projected) == 1
    top_level = {id(e) for e in node.expressions}
    for col in list(node.find_all(exp.Column)):
        if isinstance(col.this, exp.Star) or _already_governed(col):
            continue
        if col.table:
            tid = projected.get(col.table)
        else:
            tid = next(iter(projected.values())) if sole else None
            if tid is not None and col.find_ancestor(exp.Select) is not node:
                tid = None  # a nested SELECT's own unqualified column
        if tid is None:
            continue
        physical = registered_name(gov_ctx, tid, col.name)
        named = apply_sql_name(physical)
        if physical == col.name or named == col.name:
            continue
        published = col.name
        col.set("this", exp.Identifier(this=named, quoted=True))
        if id(col) in top_level:
            col.replace(exp.alias_(col.copy(), published, quoted=True))


def _project_faked(
    node: exp.Select,
    tbl: exp.Table,
    tid: int,
    gov_ctx: GovernanceContext,
    carried: dict[str, tuple[str, str]] | None = None,
) -> exp.Subquery:  # REQ-1494
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
        None if carried is None else {n: base_expr for n, (base_expr, _) in carried.items()},
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
    return subquery


def _project_kept(
    node: exp.Select,
    tbl: exp.Table,
    faked: exp.Subquery | None,
    tid: int,
    gov_ctx: GovernanceContext,
    members: list[str] | None = None,
) -> exp.Subquery:  # REQ-1942
    """Put ``tbl`` -- or ``faked``, its faked projection already in its place -- with its change
    log applied in its place in ``node``, under the same alias. The row filter reads the table's
    rows and the log's versions alike."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.core.env_changes import overlay_sql

    alias = _alias_for(tbl)
    log, key = gov_ctx.kept[tid]
    column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
    rls = gov_ctx.rls_rules.get(tid)
    if faked is not None:
        base = faked.this.sql(dialect="postgres")  # the row filter is inside the projection
    else:
        plain = tbl.copy()
        plain.set("alias", None)
        base = f"SELECT * FROM {plain.sql(dialect='postgres')} AS {_quote(alias)}"
        if members is not None:
            # REQ-1620: the member flags and real values, computed on the table's real rows.
            base = (
                f"SELECT {_quote(alias)}.*, "
                + ", ".join(
                    f"{e} AS {_quote(n)}"
                    for n, (e, _) in _member_carried(tid, members, gov_ctx, alias).items()
                )
                + f" FROM {plain.sql(dialect='postgres')} AS {_quote(alias)}"
            )
        if rls is not None:
            base += f" WHERE {_qualify_filter(rls, alias, column_types)}"
    sql = overlay_sql(
        base,
        log,
        [apply_sql_name(k) for k in key],
        [apply_sql_name(name) for name, _ in gov_ctx.all_columns[tid]],
        None if rls is None else _qualify_filter(rls, "l", column_types),
        # REQ-1620: the same flags and real values over a written row's version.
        None
        if members is None
        else {
            n: log_expr
            for n, (_, log_expr) in _member_carried(tid, members, gov_ctx, alias).items()
        },
    )
    projection = sqlglot.parse_one(f"SELECT * FROM ({sql}) AS {_quote(alias)}", read="postgres")
    subquery = projection.args["from_"].this
    subquery.meta[_GOVERNED] = True  # its own references are already governed
    for col in node.find_all(exp.Column):
        if col.table == tbl.name and col.args.get("db") is not None and alias == tbl.name:
            col.set("db", None)
            col.set("catalog", None)
    (faked if faked is not None else tbl).replace(subquery)
    return subquery


def _differing_members(tid: int, gov_ctx: GovernanceContext) -> list[str] | None:  # REQ-1620
    """The columns of ``tid`` the acting meta-role's members read differently, or None when there
    are none (a single role, or members that read every column alike)."""
    reads = gov_ctx.member_reads.get(tid)
    if reads is None:
        return None
    differing = [c for c in reads.columns if not reads.uniform(c)]
    return differing or None


def _member_carried(
    tid: int, differing: list[str], gov_ctx: GovernanceContext, alias: str
) -> dict[str, tuple[str, str]]:  # REQ-1620
    """What a member projection reads off the real row, by internal name: each member's flag
    (``__m_<i>``, its own row filter) and the real value of each column the members read
    differently (``__real_<name>``) -- each as an expression over ``alias`` and over a change log's
    version (``l``)."""
    from provisa.compiler.naming import apply_sql_name

    reads = gov_ctx.member_reads[tid]
    column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
    carried: dict[str, tuple[str, str]] = {}
    for i, flag in enumerate(reads.flags):
        carried[f"__m_{i}"] = (
            ("TRUE", "TRUE")
            if flag == "TRUE"
            else (
                f"({_qualify_filter(flag, alias, column_types)})",
                f"({_qualify_filter(flag, 'l', column_types)})",
            )
        )
    for name in differing:
        sql_name = _quote(apply_sql_name(name))
        carried[f"__real_{apply_sql_name(name)}"] = (f"{_quote(alias)}.{sql_name}", f"l.{sql_name}")
    return carried


def _project_members(
    node: exp.Select,
    tbl: exp.Table,
    current: exp.Expr | None,  # pyright: ignore[reportPrivateImportUsage]
    tid: int,
    differing: list[str],
    gov_ctx: GovernanceContext,
    carried: dict[str, tuple[str, str]] | None,
) -> dict[str, str]:  # REQ-1620
    """Put ``tbl`` -- or ``current``, the faked or kept projection already in its place, which
    carries the member flags and real values (:func:`_member_carried`) -- read row by row as the
    members of the acting meta-role read it, in its place in ``node``, under the same alias.
    Returns the internal name of the real value of each column an approved join may read.

    Each member's own row filter is evaluated once per row, on the real row, as a flag; the rows
    are those the role's filter (the OR of the members') admits. A column the members read
    differently is read through the treatment of the least restrictive group of members whose
    flag holds for the row -- read as it is, then each mask in the column's mask-list order, then
    not served (NULL) -- so a row is never read through a mask or grant of a member that did not
    admit it. A faked treatment reads the faked value; any other reads the real one. Masks are
    applied here, at the table, before any join or aggregate reads the column; an approved join
    reads the real key (REQ-603)."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.compiler.rls import HIDDEN
    from provisa.security.masking import MaskType

    reads = gov_ctx.member_reads[tid]
    alias = _alias_for(tbl)
    column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
    carried = carried or _member_carried(tid, differing, gov_ctx, alias)
    if current is not None:
        scan = f"SELECT * FROM ({current.this.sql(dialect='postgres')}) AS {_quote(alias)}"
    else:
        plain = tbl.copy()
        plain.set("alias", None)
        scan = (
            f"SELECT {_quote(alias)}.*, "
            + ", ".join(f"{e} AS {_quote(n)}" for n, (e, _) in carried.items())
            + f" FROM {plain.sql(dialect='postgres')} AS {_quote(alias)}"
        )
        if tid in gov_ctx.rls_rules:
            scan += f" WHERE {_qualify_filter(gov_ctx.rls_rules[tid], alias, column_types)}"
    flag_names = [f"__m_{i}" for i in range(len(reads.flags))]
    keys = {c1 for (t1, _t2, c1, _c2) in gov_ctx.join_keys if t1 == tid}
    real_keys: dict[str, str] = {}
    projections: list[str] = []
    for name, _dtype in gov_ctx.all_columns.get(tid, []):
        sql_name = apply_sql_name(name)
        quoted = _quote(sql_name)
        if name not in differing:
            projections.append(quoted)
            continue
        real = _quote(f"__real_{sql_name}")
        treatments = reads.columns[name]
        branches: list[str] = []
        for rank in sorted({t[0] for t in treatments}):
            group = [i for i, t in enumerate(treatments) if t[0] == rank]
            _, rule, dtype = treatments[group[0]]
            if rank == HIDDEN:
                value = "NULL"
            elif rule is None:
                value = real
            elif rule.mask_type == MaskType.fake:
                value = quoted  # the faked projection's value (one fake per column)
            else:
                value = build_mask_expression(rule, real, dtype)
            branches.append(f"WHEN {' OR '.join(flag_names[i] for i in group)} THEN {value}")
            if any(reads.flags[i] == "TRUE" for i in group):
                break  # every row is admitted by this group: no later branch is read
        projections.append(f"CASE {' '.join(branches)} ELSE NULL END AS {quoted}")
        if name in keys:
            projections.append(f"{real} AS {real}")
            real_keys[sql_name] = f"__real_{sql_name}"
    sql = f"SELECT {', '.join(projections)} FROM ({scan}) AS {_quote(alias)}"
    projection = sqlglot.parse_one(f"SELECT * FROM ({sql}) AS {_quote(alias)}", read="postgres")
    subquery = projection.args["from_"].this
    subquery.meta[_GOVERNED] = True  # its own references are already governed
    for col in node.find_all(exp.Column):
        if col.table == tbl.name and col.args.get("db") is not None and alias == tbl.name:
            col.set("db", None)
            col.set("catalog", None)
    (current if current is not None else tbl).replace(subquery)
    return real_keys


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
    node: exp.Select,
    alias_to_tid: dict[str, int],
    gov_ctx: GovernanceContext,
    real_keys: dict[str, dict[str, str]] | None = None,
) -> None:
    """Replace, in place, every reference to a masked column with its mask and every reference
    to a hidden column with NULL — anywhere in ``node`` except its bare select-list columns,
    which the projection rewrite names and drops. A column of this SELECT's tables referenced
    from a nested SELECT by its alias (a correlated subquery) is replaced too."""
    top_level = {id(e) for e in node.expressions}
    outputs = {e.alias_or_name.lower() for e in node.expressions if e.alias_or_name}
    for col in list(node.find_all(exp.Column)):
        parent = col.parent
        if id(col) in top_level or (
            isinstance(parent, exp.Alias) and id(parent) in top_level and parent.this is col
        ):
            continue
        if _already_governed(col):
            continue
        if _names_an_output(col, node, outputs):
            continue  # the statement's own output column: what it reads is governed there
        if col.find_ancestor(exp.Select) is not node and col.table not in alias_to_tid:
            continue  # a nested SELECT's own column: governed when that SELECT is
        real = (real_keys or {}).get(col.table, {}).get(col.name)
        if real is not None and approved_join_key(col, gov_ctx, alias_to_tid):
            # REQ-603, REQ-1620: read through a member projection, an approved join reads the
            # real key the projection carries under its internal name, not the value it serves.
            col.set("this", exp.Identifier(this=real, quoted=True))
            col.meta[_GOVERNED] = True
            continue
        if not _is_column_visible(col, alias_to_tid, gov_ctx):
            replacement: exp.Expr = exp.Null()  # pyright: ignore[reportPrivateImportUsage]
        else:
            replacement = _maybe_mask_column(col, alias_to_tid, gov_ctx)
            if replacement is col:
                continue
        if approved_join_key(col, gov_ctx):
            # A registered relationship's key, hidden or masked: the engine joins on the real
            # value, which is not returned (REQ-603).
            continue
        replacement.meta[_GOVERNED] = True
        _replace_reference(col, replacement)


def _names_an_output(col: exp.Column, node: exp.Select, outputs: set[str]) -> bool:
    """Whether ``col`` is an ORDER BY key of ``node`` naming one of its output columns by its
    output name (``SUM(x) AS revenue ... ORDER BY revenue``): the statement's own name, governed
    where the output is, so there is nothing to hide or mask in it. In ORDER BY an output name
    is read before a table's column of that name (PostgreSQL's rule); GROUP BY reads the table's
    first, so a GROUP BY key is governed as the column it is."""
    if col.table or col.name.lower() not in outputs:
        return False
    clause = col.find_ancestor(exp.Order, exp.Select)
    return isinstance(clause, exp.Order) and clause.parent is node


def _unwrap_cast(node: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    while isinstance(node, exp.Cast):
        node = node.this
    return node


def _table_in_scope(alias: str, select: exp.Select, gov_ctx: GovernanceContext) -> int | None:
    """The registered table ``alias`` names where ``select`` reads it: a table of ``select``,
    else of the nearest enclosing SELECT that has one by that name (a correlated reference)."""
    scope: exp.Expr | None = select  # pyright: ignore[reportPrivateImportUsage]
    while scope is not None:
        if isinstance(scope, exp.Select):
            for tbl, tid in _get_tables_from_select(scope, gov_ctx):
                if _alias_for(tbl) == alias:
                    return tid
        scope = scope.parent
    return None


def approved_join_key(
    col: exp.Column, gov_ctx: GovernanceContext, alias_to_tid: dict[str, int] | None = None
) -> bool:  # REQ-603
    """Whether ``col`` is one side of a registered relationship's join condition: an equality
    (each side optionally cast) in a WHERE or JOIN ON of the two columns a relationship approves
    (``GovernanceContext.join_keys``). The role reaches the related rows through it; its value is
    read by the engine to join and is not returned."""
    side: exp.Expr = col  # pyright: ignore[reportPrivateImportUsage]
    while isinstance(side.parent, exp.Cast):
        side = side.parent
    eq = side.parent
    if not isinstance(eq, exp.EQ):
        return False
    clause = eq.find_ancestor(exp.Where, exp.Join, exp.Select)
    if not isinstance(clause, (exp.Where, exp.Join)):
        return False
    select = clause.find_ancestor(exp.Select)
    other = _unwrap_cast(eq.right if eq.left is side else eq.left)
    if select is None or not isinstance(other, exp.Column) or not col.table or not other.table:
        return False
    # ``alias_to_tid``: the tables of the SELECT being governed, by alias, as they were before a
    # projection took a table's place (REQ-1620).
    known = alias_to_tid or {}
    tid = known[col.table] if col.table in known else _table_in_scope(col.table, select, gov_ctx)
    other_tid = (
        known[other.table]
        if other.table in known
        else _table_in_scope(other.table, select, gov_ctx)
    )
    if tid is None or other_tid is None:
        return False
    return (
        tid,
        other_tid,
        registered_name(gov_ctx, tid, col.name),
        registered_name(gov_ctx, other_tid, other.name),
    ) in gov_ctx.join_keys


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
    if col_name in SERVED_WITH_EVERY_TABLE:
        return True  # a virtual column, served with every table its reader reads
    if not tbl_ref:
        # Unqualified — check all tables
        for tid in alias_to_tid.values():
            vis = gov_ctx.visible_columns.get(tid)
            if vis is not None and registered_name(gov_ctx, tid, col_name) not in vis:
                return False
        return True
    tid = alias_to_tid.get(tbl_ref)
    if tid is None:
        return True
    vis = gov_ctx.visible_columns.get(tid)
    return vis is None or registered_name(gov_ctx, tid, col_name) in vis


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
        entry = gov_ctx.masking_rules.get((tid, registered_name(gov_ctx, tid, col_name)))
        if entry and not _is_fake(entry[0]):
            rule, dtype = entry
            col_sql = col.sql(dialect="postgres")
            mask_expr_str = build_mask_expression(rule, col_sql, dtype)
            return sqlglot.parse_one(mask_expr_str, read="postgres")
        # Visibility check: the one rule (_is_column_visible)
        if not _is_column_visible(col, alias_to_tid, gov_ctx):
            return exp.Null()
        return col
    else:
        # Unqualified column — check all tables
        for tid in alias_to_tid.values():
            entry = gov_ctx.masking_rules.get((tid, registered_name(gov_ctx, tid, col_name)))
            if entry and not _is_fake(entry[0]):
                rule, dtype = entry
                col_sql = col.sql(dialect="postgres")
                mask_expr_str = build_mask_expression(rule, col_sql, dtype)
                return sqlglot.parse_one(mask_expr_str, read="postgres")
        return col


def _registered_column(
    col: exp.Column, alias_to_tid: dict[str, int], gov_ctx: GovernanceContext
) -> tuple[int, str, str] | None:
    """(table id, registered column name, registered type) of the registered column ``col``
    reads, by the name the statement uses for it; None when it reads none."""
    from provisa.compiler.naming import apply_sql_name

    tids = [alias_to_tid[col.table]] if col.table in alias_to_tid else []
    if not col.table:
        tids = list(alias_to_tid.values())
    for tid in tids:
        registered = registered_name(gov_ctx, tid, col.name)
        for name, dtype in gov_ctx.all_columns.get(tid, []):
            if registered in (name, apply_sql_name(name)):
                return tid, name, dtype
    return None


def _as_read(
    projected: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    col: exp.Column,
    alias_to_tid: dict[str, int],
    gov_ctx: GovernanceContext,
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """``projected`` — what the statement's answer holds for the column ``col`` (the column, or
    its mask) — as a read of the column returns it (REQ-1961).

    A numeric column the model records no precision and scale for is floating point on every
    read path. The cast is made here, in the statement, so the source or engine converts and the
    value arrives as its own nearest double; nothing is converted afterwards. It wraps what a
    constant mask produces (still the column's number) and a fake (read through the table's
    faked projection); a regex or truncate mask has already made the column something else, and
    is left as it is. Only the answer's own projection is cast: a reference anywhere else in
    the statement — a filter, a join, an aggregate or any other expression over the column —
    computes on the exact value.
    """
    from provisa.core.catalog_types import is_undeclared_numeric
    from provisa.security.masking import MaskType

    registered = _registered_column(col, alias_to_tid, gov_ctx)
    if registered is None or not is_undeclared_numeric(registered[2]):
        return projected
    if projected is not col:
        entry = gov_ctx.masking_rules.get((registered[0], registered[1]))
        if entry is None or entry[0].mask_type != MaskType.constant:
            return projected
    return exp.Cast(this=projected, to=exp.DataType.build("double precision", dialect="postgres"))


def _is_fake(rule: Any) -> bool:
    """A fake mask: read through the table's faked projection, not substituted (REQ-1494)."""
    from provisa.security.masking import MaskType

    return rule.mask_type == MaskType.fake


def _expand_star(
    alias_to_tid: dict[str, int],
    gov_ctx: GovernanceContext,
    *,
    final: bool = False,
) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Expand SELECT * to explicit columns, filtered by visibility and masked — and, in the
    statement's own answer (``final``), each read as a read of the column returns it
    (:func:`_as_read`)."""
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
            # The answer names the column as the catalog publishes it (physical_to_sql: its alias
            # or its convention name). A column the compiler records no published name for (a
            # context not built for this role's tables) is named as it is referenced -- today's
            # convention name (REQ-301).
            out_name = gov_ctx.published_name.get((tid, col_name), sql_col_name)
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
                if final:
                    masked = _as_read(masked, col_expr, {alias: tid}, gov_ctx)
                result.append(
                    exp.Alias(
                        this=masked,
                        alias=exp.Identifier(this=out_name, quoted=True),
                    )
                )
            else:
                read = _as_read(col_expr, col_expr, {alias: tid}, gov_ctx) if final else col_expr
                if read is col_expr and out_name == sql_col_name:
                    result.append(col_expr)
                else:
                    read.meta[_GOVERNED] = True
                    result.append(
                        exp.Alias(this=read, alias=exp.Identifier(this=out_name, quoted=True))
                    )
    if not result:
        # Nothing of the tables the star covers is served to the role: refused as a named hidden
        # column is (V003), never read as every column.
        names = ", ".join(sorted(alias_to_tid))
        raise PermissionError(f"no column of {names} is visible to this role")
    return result


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def _is_the_answer(select: exp.Select, tree: exp.Expr) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether ``select``'s projection is the statement's own answer: the statement itself, or
    a branch of the set operation the statement is."""
    node: Any = select
    while node is not tree:
        node = node.parent
        if not isinstance(node, exp.SetOperation):
            return False
    return True


def _govern_selects(
    tree: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    gov_ctx: GovernanceContext,
    *,
    answers: bool = False,
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
    """Govern every SELECT in ``tree`` — the outer one, and each CTE, derived table, scalar or
    IN subquery, EXISTS body and set-operation branch — deepest first, each once. (A transform
    that replaced the outer SELECT never reached the SELECTs inside it, so a CTE or an EXISTS
    over another table ran with none of the reader's rules.)

    ``answers``: ``tree`` is a read whose rows are the answer (not a fragment of a larger
    statement, and not a write), so its own projection is read as REQ-1961 says."""
    for select in reversed(list(tree.find_all(exp.Select))):
        final = answers and isinstance(tree, exp.Query) and _is_the_answer(select, tree)
        governed_select = _govern_select(select, gov_ctx, final=final)
        if governed_select is select:
            continue
        if select is tree:
            tree = governed_select
        else:
            select.replace(governed_select)
    return tree


def _govern_returning(tree: exp.Expr, gov_ctx: GovernanceContext) -> None:  # pyright: ignore[reportPrivateImportUsage]
    """The rows a write returns (``RETURNING``) are read like a SELECT's: a star is the role's
    served columns, masked; a named column the role is not served is refused (V003); a served
    column is masked, and any other expression computes over the masked values. Without this a
    write handed back what the role may not read -- ``DELETE ... RETURNING *`` every column."""
    from provisa.compiler.write_admission import _target  # noqa: PLC0415

    returning = tree.args.get("returning")
    if returning is None:
        return
    table, _listed = _target(tree)  # pyright: ignore[reportArgumentType]  # an admitted write
    tid = _table_id_for_node(table, gov_ctx)
    if tid is None:
        return  # an unregistered target is refused by the write's admission
    alias_to_tid = {_alias_for(table): tid, table.name: tid}
    governed: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    for expr in returning.expressions:
        if isinstance(expr, exp.Star) or (
            isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star)
        ):
            governed.extend(_expand_star({_alias_for(table): tid}, gov_ctx))
            continue
        col = expr.this if isinstance(expr, exp.Alias) else expr
        for ref in list(expr.find_all(exp.Column)) if not isinstance(expr, exp.Column) else [expr]:
            if not _is_column_visible(ref, alias_to_tid, gov_ctx):
                raise PermissionError(f"[V003] Column {ref.name!r} is not visible to this role")
        if isinstance(col, exp.Column):
            masked = _maybe_mask_column(col, alias_to_tid, gov_ctx)
            if masked is col:
                governed.append(expr)
            else:
                name = expr.args["alias"].copy() if isinstance(expr, exp.Alias) else col.this.copy()
                governed.append(exp.Alias(this=masked, alias=name))
            continue
        for ref in list(expr.find_all(exp.Column)):
            masked = _maybe_mask_column(ref, alias_to_tid, gov_ctx)
            if masked is not ref:
                ref.replace(masked)
        governed.append(expr)
    returning.set("expressions", governed)


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
    admission (compiler/write_admission.py: the write right and the role's write grant for the
    operation -- its columns, presets, rows and check, REQ-2074).

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
        _govern_returning(tree, gov_ctx)

    tree = _govern_selects(tree, gov_ctx, answers=not writes)

    governed = tree.sql(dialect="postgres")

    # Apply LIMIT ceiling (REQ-005): most restrictive of the role-level ceiling and
    # any per-table ceiling on a table referenced by the query.
    # A ceiling bounds the rows a READ returns; a write returns none, and what it may write
    # is its admission's to decide.
    bound = None if writes else _effective_ceiling(tree, gov_ctx)
    gov_ctx.row_limit = None
    if bound is not None:
        from provisa.compiler.row_limit import binds

        if binds(governed, bound.limit):
            gov_ctx.row_limit = bound
        governed = _apply_limit_ceiling(governed, bound.limit)
    elif gov_ctx.sample_size is not None and not writes:
        governed = _apply_limit_ceiling(governed, gov_ctx.sample_size)

    trace_stage("govern.out", governed)
    return governed


def _effective_ceiling(tree, gov_ctx: GovernanceContext):  # REQ-005, REQ-263, REQ-1949
    """The smallest applicable row ceiling and whose it is -- the role's, or a table's the
    statement reads (compiler/row_limit.RowLimit); None when none applies. The role's wins a tie."""
    from provisa.compiler.row_limit import ROLE, TABLE, RowLimit

    candidates: list[RowLimit] = []
    if gov_ctx.limit_ceiling is not None:
        candidates.append(RowLimit(gov_ctx.limit_ceiling, ROLE))
    if gov_ctx.table_ceilings:
        for tbl_node in tree.find_all(exp.Table):
            tid = _table_id_for_node(tbl_node, gov_ctx)
            if tid is not None and tid in gov_ctx.table_ceilings:
                candidates.append(RowLimit(gov_ctx.table_ceilings[tid], TABLE))
    return min(candidates, key=lambda c: c.limit) if candidates else None


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
        tid = _table_id_for_node(tbl, gov_ctx)
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
        tid = _table_id_for_node(tbl, gov_ctx)
        if tid is None or tid in inlined_tids:
            continue
        for meta in ctx.tables.values():
            if meta.table_id == tid:
                live.add(meta.source_id)
                break

    return live
