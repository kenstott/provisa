# Copyright (c) 2026 Kenneth Stott
# Canary: 0853c191-82dd-45b0-9317-468bbfdca129
# (run scripts/canary_stamp.py on this file after creating it)

"""Stage 2: SQL governance transformer (REQ-263, REQ-264).

Applies RLS, column visibility, masking, and LIMIT ceiling to raw SQL
using SQLGlot. Input: plain SQL string. Output: governed SQL string.
"""

# Requirements: REQ-002, REQ-005, REQ-038, REQ-040, REQ-262, REQ-263, REQ-264, REQ-265, REQ-266, REQ-267

from __future__ import annotations

import secrets
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler.cte_utils import physical_tables
from provisa.compiler.governed_relation import keep_each_read
from provisa.compiler.rls import Rule, all_of, every_row, qualified_filter
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import extracts_paths, stored_path
from provisa.compiler.sql_where import SERVED_WITH_EVERY_TABLE
from provisa.security.masking import mask_expression


# --------------------------------------------------------------------------- #
# GovernanceContext                                                            #
# --------------------------------------------------------------------------- #


@dataclass
class GovernanceContext:  # REQ-263, REQ-264, REQ-265
    """Governance parameters for a single request/role."""

    # table_id → row rule (already role-filtered), an expression (REQ-913): a rule given as text
    # is a rule entering, read once where it is stored (compiler.rls.RuleMap)
    rls_rules: dict[int, Rule] = field(default_factory=dict)
    # (table_id, col_name) → (MaskingRule, data_type)
    masking_rules: dict[tuple[int, str], tuple] = field(default_factory=dict)
    # table_id → visible column names (None = all visible)
    visible_columns: dict[int, frozenset[str] | None] = field(default_factory=dict)
    # REQ-2190: the table ids the role's meta rows are scoped to, as an array literal, which a
    # statement's settings tell its catalog views. None where no row filter applies.
    meta_scope: str | None = None
    # REQ-1959: tables outside the role's domains that it is served anyway, because they publish
    # a column to it. A direct read of one is not a domain violation (V001); the columns it may
    # read there are the published ones (visible_columns).
    public_tables: frozenset[int] = frozenset()
    # "schema.table" or "table" → table_id
    table_map: dict[str, int] = field(default_factory=dict)
    # table_id → [(col_name, data_type)]
    all_columns: dict[int, list[tuple[str, str]]] = field(default_factory=dict)
    # table_id → its parameter columns (``_nf_<name>``), [(col_name, data_type)]: an argument of
    # the read and no column of its rows, so in a governed relation only where a statement names
    # it (REQ-039, REQ-2099, REQ-2100).
    parameter_columns: dict[int, list[tuple[str, str]]] = field(default_factory=dict)
    # Role-level row ceiling (REQ-005) — applies to the whole query regardless of tables.
    limit_ceiling: int | None = None
    # Per-table row ceiling (REQ-005) — applied only when that table is referenced.
    table_ceilings: dict[int, int] = field(default_factory=dict)
    # REQ-1949: the ceiling that bounds the statement last governed with this context, when a
    # ceiling and not the statement's own LIMIT is what bounds it (compiler/row_limit.RowLimit).
    row_limit: Any = None
    # REQ-005: the ceiling that bounds each list the statement last governed with this context
    # nests (compiler/list_cap), when it nests any; None otherwise.
    list_limit: Any = None
    sample_size: int | None = None
    # The role this context was built for, and what it may write (compiler/write_admission.py):
    # whether it holds the ``write`` right (REQ-868), and per table its write grant for each
    # operation (REQ-2074). A role with no grant for an operation may not perform it.
    role_id: str = ""
    can_write: bool = False
    write_grants: dict[int, dict[str, dict]] = field(default_factory=dict)
    # REQ-2188: the role's U and D rules: "update" or "delete" -> table_id -> predicate. ANDed into
    # the write beside its grant; they admit nothing the grant does not.
    write_rules: dict[str, dict[int, Rule]] = field(default_factory=dict)
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
    kept: dict[int, tuple[tuple[str, ...], list[str]]] = field(default_factory=dict)
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
    # REQ-2219: the person's mask over this role's schema, when it narrows anything
    # (compiler/entitlement_rule.py); None when nothing is narrowed.
    entitlement_mask: Any = None
    # Per table, its registered primary key's columns (CompilationContext.pk_columns): the key a
    # write that reads beyond its target matches its rows by (REQ-039).
    pk_columns: dict[int, list[str]] = field(default_factory=dict)
    # REQ-151, REQ-152: per table on a SQL database, each column registered at a path in a JSON
    # column (by physical name) -> that JSON column and the keys into it (sql_types.stored_path).
    # The governed relation reads such a table as its source stores it (_read_stored).
    stored_paths: dict[int, dict[str, tuple[str, list[str]]]] = field(default_factory=dict)
    # REQ-039, REQ-1942: the write the statement last governed with this context, admitted (its
    # grant's rows and the role's U or D rule in its WHERE) as the role wrote it, before what it
    # reads was moved over the governed relation; None for a read. A Reversible environment keeps
    # a write by reading its rows as a governed read of its own (pgwire/kept_mutations), which
    # this form gives it.
    admitted_write: str | None = None

    def __setattr__(self, name: str, value: Any) -> None:
        from provisa.compiler.rls import GrantMap, RuleMap, rule_maps

        # A rule or grant entering as a plain dict is read (REQ-913); a read-only view of one
        # already read (pgwire.governed_plan's shared context) is kept as it is.
        if isinstance(value, dict):
            if name == "rls_rules" and not isinstance(value, RuleMap):
                value = RuleMap(value)
            elif name == "write_rules":
                value = rule_maps(value)
            elif name == "write_grants" and not isinstance(value, GrantMap):
                value = GrantMap(value)
        super().__setattr__(name, value)


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


def _readable_term_predicate(column: str, domains: frozenset[str]) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """REQ-1593: ``column`` (a term's domains, as |a|b|) names a term the caller may read by
    glossary.readable_term: one with no domains, an enterprise ("*") one, or one in a domain of
    ``domains``."""
    test: exp.Expr = exp.Is(this=exp.column(column), expression=exp.Null())  # pyright: ignore[reportPrivateImportUsage]
    for d in ("*", *sorted(domains)):
        found = exp.StrPosition(this=exp.column(column), substr=exp.Literal.string(f"|{d}|"))
        test = exp.Or(this=test, expression=exp.GT(this=found, expression=exp.Literal.number(0)))
    return exp.paren(test)


def _holds(column: str, value: str) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """``column`` (a set, as |a|b|) holds ``value``."""
    found = exp.StrPosition(this=exp.column(column), substr=exp.Literal.string(f"|{value}|"))
    return exp.GT(this=found, expression=exp.Literal.number(0))


def tag_row_predicate(role: dict, readable_in: frozenset[str] | None) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # REQ-2208
    """Which rows of a tags relation (security.rights.TAG_ROW_VIEWS) ``role`` reads, as the tag
    admin reads assignments (api.admin.schema_query._readable_assignments), from the relation's
    read columns (api._meta_views._tag_instances_sql).

    Within ``readable_in`` (None: domains gate nothing): every domain of ``read_all_domains`` (a
    table's, a relationship's two tables', a command's, a product's, the domains a row rule decides
    rows in; NULL when the object is not there); one of ``read_any_domains`` when it names any (a
    source's allowed domains); and ``read_term_domains`` as a glossary term is read (a term's, a
    library document's). A row rule's tag is read with access_config; a role's with
    user_management, or by the role itself; a metric's by a role it is granted to. A meta-role
    reads no role's or metric's tag but by those rights: its members are not in its role."""
    from provisa.security.rights import Capability, has_capability, is_meta_role_id

    kind = exp.column("object_kind")
    terms: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    if readable_in is not None:
        stripped: exp.Expr = exp.column("read_all_domains")  # pyright: ignore[reportPrivateImportUsage]
        for d in sorted(readable_in):
            stripped = exp.Anonymous(
                this="REPLACE",
                expressions=[stripped, exp.Literal.string(f"|{d}|"), exp.Literal.string("|")],
            )
        terms.append(exp.EQ(this=stripped, expression=exp.Literal.string("|")))
        any_of: exp.Expr = exp.EQ(  # pyright: ignore[reportPrivateImportUsage]
            this=exp.column("read_any_domains"), expression=exp.Literal.string("|")
        )
        for d in sorted(readable_in):
            any_of = exp.Or(this=any_of, expression=_holds("read_any_domains", d))
        terms.append(exp.paren(any_of))
        terms.append(_readable_term_predicate("read_term_domains", frozenset(readable_in)))
    if not has_capability(role, Capability.ACCESS_CONFIG):
        terms.append(exp.NEQ(this=kind.copy(), expression=exp.Literal.string("rls_rule")))
    own = [] if is_meta_role_id(role["id"]) else [role["id"]]
    if not has_capability(role, Capability.USER_MANAGEMENT):
        not_role: exp.Expr = exp.NEQ(  # pyright: ignore[reportPrivateImportUsage]
            this=kind.copy(), expression=exp.Literal.string("role")
        )
        if own:
            not_role = exp.Or(
                this=not_role,
                expression=_in_list(exp.column("object_id"), [exp.Literal.string(own[0])]),
            )
        terms.append(exp.paren(not_role))
    not_metric: exp.Expr = exp.NEQ(  # pyright: ignore[reportPrivateImportUsage]
        this=kind.copy(), expression=exp.Literal.string("metric")
    )
    for r in own:
        not_metric = exp.Or(this=not_metric, expression=_holds("read_roles", r))
    terms.append(exp.paren(not_metric))
    return exp.and_(*terms, copy=False)


def library_row_predicate(  # REQ-2208
    role: dict, readable_in: frozenset[str] | None, relation: str
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """Which rows of a library relation (security.rights.LIBRARY_ROW_VIEWS) ``role`` reads: those
    whose document it reads, as a library document is read (``read_document_domains``, the
    glossary's rule), and of a relation whose rows may name an object, a row naming one
    (``object_key`` set) only where it reads the tag on that object (:func:`tag_row_predicate`)."""
    from provisa.security.rights import LIBRARY_ROW_VIEWS, META_DOMAIN_ID

    terms: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    if readable_in is not None:
        terms.append(_readable_term_predicate("read_document_domains", frozenset(readable_in)))
    if LIBRARY_ROW_VIEWS[(META_DOMAIN_ID, relation)]:
        no_object = exp.Is(this=exp.column("object_key"), expression=exp.Null())
        terms.append(
            exp.paren(
                exp.Or(this=no_object, expression=exp.paren(tag_row_predicate(role, readable_in)))
            )
        )
    if not terms:
        return exp.true()
    return exp.and_(*terms, copy=False)


def _in_list(column: Rule, values: Sequence[Rule]) -> exp.In:
    return exp.In(this=column, expressions=list(values))


def _not_in(row: Sequence[Rule], rows: Sequence[Sequence[Rule]]) -> exp.Not:
    """``(row...) NOT IN ((value...), ...)``."""
    return exp.Not(
        this=exp.In(
            this=exp.Tuple(expressions=list(row)),
            expressions=[exp.Tuple(expressions=list(r)) for r in rows],
        )
    )


def _and_rule(gov: GovernanceContext, tid: int, rule: exp.Expr) -> None:  # pyright: ignore[reportPrivateImportUsage]
    """AND ``rule`` with any rule already on ``tid`` (a tenant rule, say): never replace a guard."""
    held = all_of([gov.rls_rules.get(tid), rule])
    assert held is not None  # ``rule`` is one of them
    gov.rls_rules[tid] = held


def context_table_map(ctx: Any) -> dict[str, int]:
    """Every name a statement gives a table of ``ctx``, to its id: what governance reads a
    statement's references by, and so what anything else reading one must read it by."""
    from provisa.compiler.naming import physical_relation_name, semantic_relation_names
    from provisa.compiler.ontology_classes import is_class_table_id
    from provisa.compiler.sql_rewrite import semantic_table_name

    table_map: dict[str, int] = {}
    for meta in ctx.tables.values():
        for said in semantic_relation_names(meta.domain_id, semantic_table_name(meta)):
            # REQ-2163: a Class is named only under the ontology schema. A table is grounded in
            # a Class named for it (REQ-2141), so the bare name stays the table's.
            if is_class_table_id(meta.table_id) and "." not in said:
                continue
            table_map[said] = meta.table_id
        # And its catalog-physical name: a view's body is read into a statement lowered to it,
        # and two sources may hold tables of one schema.table (naming.table_id_of).
        table_map[physical_relation_name(meta.catalog_name, meta.schema_name, meta.table_name)] = (
            meta.table_id
        )
        # And its physical schema.table, which a statement lowered without a catalog names it by.
        # For most tables that is also the stored name below; for one read through a physical
        # alias (a meta view, an ops log view, a Kafka topic's table) it is not, and without it
        # such a read is refused as unregistered.
        table_map[f"{meta.schema_name}.{meta.table_name}"] = meta.table_id
    # And, unqualified, its field name: the domain-prefixed ``sa__orders`` a domain_prefix schema
    # exposes, which the validator admits as naming the table (sql_validator). Unresolved, it was
    # read ungoverned under that name wherever the route happened to find one. A semantic name
    # another table holds stays that table's; a Class is named only under the ontology schema.
    for meta in ctx.tables.values():
        if not is_class_table_id(meta.table_id):
            table_map.setdefault(meta.field_name, meta.table_id)
    return table_map


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
    source_hints: dict[str, dict[str, str]] | None = None,
    draft_reach: Callable[[], frozenset[str] | None] | None = None,
) -> GovernanceContext:
    """Build GovernanceContext from server state for a given role.

    ``draft_reach``: asked only where the model table is in this compilation, the domains whose
    drafts' rows of it the role reads (security.rights.draft_reach): None for every domain, empty
    for none. Not given: none.

    Args:
        role_id: The requesting role.
        rls_context: RLSContext with .rules: table id -> row rule (an expression).
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
              is None, no capability check runs; the mask expression is inlined either way, and
              the engine computes whatever part of it the source cannot.
        engine: The bound FederationEngine, used to read each masked table's connector
              ``Capability`` (REQ-897) via ``connector_pushdown``.
        source_hints: {source_id: federation_hints}: each source's settings, which
              ``connector_pushdown`` reads with its type (REQ-2191).
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
    gov.write_rules = dict(rls_context.write_rules) if rls_context else {}  # REQ-2188

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
    gov.pk_columns = dict(getattr(ctx, "pk_columns", None) or {})  # REQ-039
    from provisa.compiler.sql_rewrite import published_columns
    from provisa.compiler.sql_validator import approved_joins

    gov.published = published_columns(ctx)
    gov.published_name = dict(ctx.physical_to_sql)
    gov.join_keys = approved_joins(ctx)

    # Column visibility is each column's visible_to grant and nothing above it: no capability sees
    # every column regardless (REQ-1327). The lockdown domains (ops) need an explicit grant; the
    # meta domain is governed by the tiered rule below.
    from provisa.security.rights import (
        GLOSSARY_SCOPED_VIEWS,
        GOVERNANCE_META_COLUMNS,
        META_CATALOG_ROW_SCOPES,
        META_DOMAIN_ID,
        LISTED_NAMES_VIEW,
        META_ROW_SCOPED_VIEWS,
        ONTOLOGY_DOMAIN_ID,
        SYSTEM_DOMAINS,
        Capability,
        acts_in_domain,
        column_served,
        DRAFT_ROWS_VIEW,
        compute_meta_row_scope,
        has_capability,
        reaches_domain,
        served_across_domains,
        unlisted_columns,
    )

    from provisa.transpiler.metadata_settings import scope_ids
    from provisa.transpiler.metadata_views import NEUTRAL_VIEW_NAMES, SCOPE_COLUMN

    _has_view_gov = has_capability(role, Capability.VIEW_GOVERNANCE)
    gov.can_write = has_capability(role, Capability.WRITE)
    # table_id → domain_id, so meta (catalog) tables can be governed by the tiered rule (REQ-1132)
    # rather than their static seed (meta columns are seeded visible_to: [] = nobody).
    _domain_by_tid: dict[int | None, str | None] = {
        getattr(tm, "table_id", None): getattr(tm, "domain_id", None)
        for tm in getattr(ctx, "tables", {}).values()
    }

    # REQ-151, REQ-152: table_id → its source's type, which says how a path column is served.
    _source_type_by_tid: dict[Any, str] = {
        getattr(tm, "table_id", None): getattr(tm, "source_type", "")
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
            # REQ-2190: a catalog view's scope is meta's, no column of PostgreSQL's relation.
            and c["column_name"] != SCOPE_COLUMN
        ]
        gov.parameter_columns[table_id] = [
            (c["column_name"], c.get("data_type", "varchar"))
            for c in cols
            if c.get("native_filter_type")
        ]
        # A table the compilation does not hold is read by no statement it compiles.
        paths = {c["column_name"]: c["path"] for c in cols if c.get("path")}
        if (
            paths
            and table_id in _source_type_by_tid
            and extracts_paths(_source_type_by_tid[table_id], tbl.get("table_name", str(table_id)))
        ):
            gov.stored_paths[table_id] = {n: stored_path(n, p) for n, p in paths.items()}
        embeddings = {
            c["column_name"]: c["embedding_model"]
            for c in cols
            if c.get("embedding") and c.get("embedding_model")
        }
        if embeddings:
            gov.embedding_columns[table_id] = embeddings
        # A capability opening a system domain brings no write: its write grants, if any are
        # stored, are not the role's here (admit_write reads this and no domain).
        _home = tbl.get("domain_id") or ""
        _writes = _home not in SYSTEM_DOMAINS or acts_in_domain(role["domain_access"], _home)
        gov.write_grants[table_id] = {
            g["operation"]: g
            for g in (tbl.get("write_grants") or [] if _writes else [])
            if g["role"] == role_id
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
            # REQ-1132: read with no meta grant, as a column published across domains is.
            if not reaches_domain(role["domain_access"], META_DOMAIN_ID) and served_across_domains(
                {"id": role_id}, tbl
            ):
                _public.add(table_id)
        else:
            # ONE rule with the schema build (security.rights.column_served): the column's grant
            # — empty is everyone, "*", or the role; an explicit grant only in a lockdown domain
            # and on a restricted column — and, in a domain the role does not reach, only a
            # published column (REQ-1959).
            _tbl_domain = tbl.get("domain_id")
            # A table registered with no domain is gated by none, as the direct-read check
            # (V001) treats it. The ontology schema's rows are scoped by the glossary's own rule
            # below (REQ-2163).
            _reaches = not _tbl_domain or reaches_domain(role["domain_access"], _tbl_domain)
            _subject = {"id": role_id}
            visible = {
                c["column_name"]
                for c in cols
                if column_served(_subject, _tbl_domain, c, reaches=_reaches)
            }
            # A parameter column is an argument, not data: it carries no grant of its own, and a
            # role served the table's data may pass it (REQ-1614, as the schema build serves it),
            # or its condition would read NULL and the argument never reach the source (REQ-1986).
            if visible - {c["column_name"] for c in cols if c.get("native_filter_type")}:
                visible |= {c["column_name"] for c in cols if c.get("native_filter_type")}
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

    # REQ-2163: a Class table's column is served where one of the columns it is bound to is, so its
    # visibility is read off its tables' (provisa.compiler.ontology_classes).
    from provisa.compiler.ontology_classes import served_class_columns

    for tbl in tables:
        if "class_bindings" in tbl:
            served = served_class_columns(tbl, gov.visible_columns.__getitem__)
            gov.visible_columns[tbl["id"]] = (
                None if len(served) == len(tbl["columns"]) else frozenset(served)
            )
    gov.public_tables = frozenset(_public)

    # table_map from compilation context — semantic refs only
    from provisa.compiler.naming import stored_relation_names

    gov.table_map.update(context_table_map(ctx))

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
    # REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN: a draft's row is read by a role holding table
    # registration in its domain (draft_reach), and by no other; decided only where the model
    # table is in this compilation, the one table it is about.
    _draft_rows = [
        tm
        for tm in {tm.table_id: tm for tm in ctx.tables.values()}.values()
        if getattr(tm, "domain_id", None) == META_DOMAIN_ID
        and getattr(tm, "table_name", "") == DRAFT_ROWS_VIEW
    ]
    reach: frozenset[str] | None = (
        draft_reach() if _draft_rows and draft_reach is not None else frozenset()
    )
    # The drafts' rows it reads, beside the rows of its scope: every one, or those of its domains.
    drafts_read: exp.Expr | None = (  # pyright: ignore[reportPrivateImportUsage]
        exp.column("draft")
        if reach is None
        else exp.and_(
            exp.column("draft"),
            _in_list(exp.column("domain_id"), [exp.Literal.string(d) for d in sorted(reach)]),
            copy=False,
        )
        if reach
        else None
    )
    if meta_scope is not None:
        ids = sorted(meta_scope) or [-1]  # empty → matches no row
        # A setting's value, told to the statement's catalog views: an array, not SQL.
        gov.meta_scope = "{" + ",".join(str(i) for i in ids) + "}"
        # A table is in ctx.tables under each name it has; its scope is one predicate.
        by_id = {tm.table_id: tm for tm in ctx.tables.values()}
        for tm in by_id.values():
            if getattr(tm, "domain_id", None) != META_DOMAIN_ID:
                continue
            scope_cols = META_ROW_SCOPED_VIEWS.get(getattr(tm, "table_name", ""))
            catalog_scope = META_CATALOG_ROW_SCOPES.get(getattr(tm, "table_name", ""))
            if scope_cols is not None:
                terms: list[exp.Expr] = [  # pyright: ignore[reportPrivateImportUsage]
                    exp.In(this=exp.column(col), query=exp.Subquery(this=scope_ids()))
                    if tm.table_name in NEUTRAL_VIEW_NAMES
                    else _in_list(exp.column(col), [exp.Literal.number(i) for i in ids])
                    for col in scope_cols
                ]
            elif catalog_scope is not None:
                # REQ-2190: a row about no registered table is every role's: its NULL stays
                # true, and the scope is never an empty list (-1 above).
                terms = [
                    exp.Coalesce(
                        this=exp.In(
                            this=exp.paren(expr.copy()), query=exp.Subquery(this=scope_ids())
                        ),
                        expressions=[exp.true()],
                    )
                    for expr in catalog_scope
                ]
            else:
                continue
            scoped = exp.and_(*terms, copy=False)
            if tm.table_name == DRAFT_ROWS_VIEW and drafts_read is not None:
                # A draft's row is in no role's reach (it is in no role's tables): the drafts
                # the role is served are read beside the rows of its scope.
                scoped = exp.or_(scoped, drafts_read.copy(), copy=False)
            _and_rule(gov, tm.table_id, scoped)
    # REQ-1921: of the model table's draft rows, only those the role is served, whatever its
    # scope (the whole catalog's included).
    for tm in _draft_rows if reach is not None else ():
        if drafts_read is None:
            _and_rule(gov, tm.table_id, exp.not_(exp.column("draft")))
        elif meta_scope is None:
            _and_rule(gov, tm.table_id, exp.or_(exp.not_(exp.column("draft")), drafts_read.copy()))

    # REQ-2190: a column the relation a statement reads does not hold is no column of the catalog's
    # either; which those are is decided in code, so no view can say.
    from provisa.compiler.naming import apply_sql_name as _sql_name

    def _as(column: str, dtype: str) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
        return exp.cast(exp.column(column), dtype)

    outside = set(ctx.columns_outside_relation) | unlisted_columns(role, tables)
    for view, row in (
        ("catalog_pg_attribute", [_as("attrelid", "BIGINT"), _as("attname", "TEXT")]),
        (
            "is_columns",
            [
                exp.Add(
                    this=_as("provisa_table_id", "BIGINT"), expression=exp.Literal.number(16384)
                ),
                _as("column_name", "TEXT"),
            ],
        ),
    ):
        if not outside:
            break
        pairs = [
            [
                exp.Literal.number(16384 + table_id),
                exp.Literal.string(ctx.physical_to_sql.get((table_id, name)) or _sql_name(name)),
            ]
            for table_id, name in sorted(outside)
        ]
        for tm in ctx.tables.values():
            if getattr(tm, "domain_id", None) != META_DOMAIN_ID or tm.table_name != view:
                continue
            _and_rule(gov, tm.table_id, _not_in([e.copy() for e in row], pairs))
    # REQ-2153: and a listed table's other columns are named to no role that only lists it.
    unlisted = unlisted_columns(role, tables)
    if unlisted:
        pairs = [
            [exp.Literal.number(table_id), exp.Literal.string(name)]
            for table_id, name in sorted(unlisted)
        ]
        for tm in ctx.tables.values():
            if getattr(tm, "domain_id", None) != META_DOMAIN_ID or tm.table_name not in (
                "table_columns_meta",
                "column_details",
            ):
                continue
            row = [exp.column("table_id"), exp.column("column_name")]
            _and_rule(gov, tm.table_id, _not_in(row, pairs))

    # REQ-1593, REQ-2163: the ontology schema's definition views answer what the glossary API
    # does -- a term is read where glossary.readable_term admits it -- by the same WHERE-injection.
    from provisa.core import domain_policy
    from provisa.core.env_authority import domains_within

    readable_in = None if domain_policy.single_domain() else domains_within(role["domain_access"])
    if readable_in is not None:
        for tm in ctx.tables.values():
            if getattr(tm, "domain_id", None) != ONTOLOGY_DOMAIN_ID:
                continue
            scope_cols = GLOSSARY_SCOPED_VIEWS.get(getattr(tm, "table_name", ""))
            if scope_cols is None:
                continue
            _and_rule(
                gov,
                tm.table_id,
                exp.and_(
                    *(_readable_term_predicate(col, readable_in) for col in scope_cols),
                    copy=False,
                ),
            )

    # REQ-2208 (THE TAGS REPORT): a tag on an object is read where the tag admin reads it.
    from provisa.security.rights import TAG_ROW_VIEWS

    tag_relations = {
        tm.table_id: tm
        for tm in ctx.tables.values()
        if (getattr(tm, "domain_id", None), getattr(tm, "table_name", "")) in TAG_ROW_VIEWS
    }
    if tag_relations:
        tag_rows = tag_row_predicate(role, readable_in)
        for tid in tag_relations:
            _and_rule(gov, tid, tag_rows.copy())

    # REQ-2208 (decision 2026-11-13): a library relation's row is read where its document is, and
    # where the tag on the object it names is.
    from provisa.security.rights import LIBRARY_ROW_VIEWS

    for tm in ctx.tables.values():
        key = (getattr(tm, "domain_id", None), getattr(tm, "table_name", ""))
        if key in LIBRARY_ROW_VIEWS:
            _and_rule(gov, tm.table_id, library_row_predicate(role, readable_in, key[1]))

    # REQ-2153: a listed name is the reader's only for a domain it does not reach -- what it reaches
    # is in registered_tables_meta / table_columns_meta -- by the same WHERE-injection.
    for tm in ctx.tables.values():
        if getattr(tm, "domain_id", None) != META_DOMAIN_ID:
            continue
        if getattr(tm, "table_name", "") != LISTED_NAMES_VIEW:
            continue
        if readable_in is None:
            # the reader reaches every domain: nothing is outside its reach
            predicate: exp.Expr = exp.EQ(  # pyright: ignore[reportPrivateImportUsage]
                this=exp.Literal.number(1), expression=exp.Literal.number(0)
            )
        elif readable_in:
            reached = [exp.Literal.string(d) for d in sorted(readable_in)]
            predicate = exp.Not(this=_in_list(exp.column("listed_domain"), reached))
        else:
            continue  # the reader reaches no domain: every listed name is outside its reach
        _and_rule(gov, tm.table_id, predicate)

    # REQ-971: mask pushdown/streaming decision, read from each masked table's connector
    # capability. A mask is a projection expression in this governed statement, and the statement
    # is executed by the engine: a mask the source cannot evaluate is computed by the engine as it
    # streams the result (REQ-971's engine-side stage; REQ-1494: every fake is computed by the
    # serving engine). Nothing in the tier buffers the unmasked relation, so every mask streams.
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
            from provisa.core.models import Source

            hints = (source_hints or {}).get(source_id, {})
            cap = engine.connector_pushdown(Source.settings_view(source_id, source_type, hints))
            plan_mask_evaluation(cap, can_stream=True)

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
            resolved = reads.resolved(column)
            if resolved is not None:
                # Every row reads it one way: masked as that treatment reads it, as a single
                # role's column is, with no member projection.
                rank, rule, dtype = resolved
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


def flatten_leftmost_join_groups(tree: exp.Expr) -> None:  # REQ-039
    """Unwrap, in place, an unaliased parenthesised join group that is a SELECT's first FROM item
    -- recursively, the group's own leftmost item -- into that SELECT's FROM and joins, in order.
    Joins associate left to right, so ``((a JOIN b ON ..) LEFT JOIN c ON ..)`` (the form BI tools
    write) is ``a JOIN b ON .. LEFT JOIN c ON ..``, and its relations are governed as any other.
    A group elsewhere -- the right side of a join, or under an alias -- is left as it is
    (:func:`parenthesised_join_tables` refuses it)."""
    for frm in list(tree.find_all(exp.From)):
        select = frm.parent
        if not isinstance(select, exp.Select):
            continue
        while True:
            group = frm.this
            if (
                not isinstance(group, exp.Subquery)
                # a derived table's query; a nested group's parentheses are a Subquery (a Query)
                or (isinstance(group.this, exp.Query) and not isinstance(group.this, exp.Subquery))
                or {k for k, v in group.args.items() if v} - {"this", "joins"}
            ):
                break
            inner = group.this
            joins = [
                *(inner.args.get("joins") or []),
                *(group.args.get("joins") or []),
                *(select.args.get("joins") or []),
            ]
            inner.set("joins", None)
            frm.set("this", inner)
            select.set("joins", joins)


def parenthesised_join_tables(  # REQ-039
    tree: exp.Expr, gov_ctx: GovernanceContext
) -> list[exp.Table]:
    """The first registered relation of each parenthesised FROM item that is no query and that
    :func:`flatten_leftmost_join_groups` leaves -- a group on the right side of a join, ``a JOIN
    (bank.customer CROSS JOIN v) ON ..``, or one under an alias, ``(bank.customer) x``. Neither the
    column checks nor this stage see a relation inside one, so its hidden columns, masks and row
    rules would go unapplied: it is refused until every registered-relation reference is governed
    by construction. A table of a query nested in the group, or a CTE, is not the group's own."""
    from provisa.compiler.cte_utils import names_a_cte_in_scope

    found: list[exp.Table] = []
    for group in tree.find_all(exp.Subquery):
        if isinstance(group.this, exp.Query):
            continue
        for tbl in group.find_all(exp.Table):
            if _within_query(tbl, group) or _table_id_for_node(tbl, gov_ctx) is None:
                continue
            if not tbl.db and names_a_cte_in_scope(tbl):
                continue
            found.append(tbl)
            break
    return found


def parenthesised_join_message(tbl: exp.Table) -> str:
    return (
        f"Relation {tbl.sql(dialect='postgres')!r} is in a parenthesised join group, which is "
        "not supported: write the join without the parentheses"
    )


def _within_query(node: exp.Expr, stop: exp.Expr) -> bool:
    """Whether a query lies between ``node`` and its ancestor ``stop``: ``node`` is that query's
    own, not ``stop``'s."""
    cur = node.parent
    while cur is not None and cur is not stop:
        if isinstance(cur, exp.Query):
            return True
        cur = cur.parent
    return False


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


#: Marks an expression governance put in a column's place (a mask, or NULL for a hidden column),
#: so a later pass over the same statement does not govern what is inside it again.
_GOVERNED = "provisa_governed"

#: The prefix of the internal name a governed relation carries a column's real value under, for
#: an approved join on it (REQ-603) or a write's key: random per process, as ``VIEW_MARK`` is, so
#: no statement a role writes can name it.
KEY_MARK = f"__key_{secrets.token_hex(8)}__"

#: The meta key a governed relation's derived table carries its :class:`_Relation` under.
_RELATION = "provisa_relation"


@dataclass
class _Relation:  # REQ-039
    """What a governed relation exposes: each column by the name it is exposed under, with its
    physical name, in the table's order; which of them the reader is not served (exposed only
    inside a view body); and each column carried as its real value, by its internal name."""

    tid: int
    alias: str
    columns: list[tuple[str, str]]
    hidden: frozenset[str] = frozenset()
    keys: dict[str, str] = field(default_factory=dict)

    def exposed(self, name: str, quoted: bool, gov_ctx: GovernanceContext) -> str | None:
        """The exposed name of the column a reference reads as ``name``: by its exposed name as
        PostgreSQL folds an unquoted one, its published name or its physical name. None when the
        relation exposes no such column."""
        from provisa.compiler.naming import apply_sql_name

        folded = name if quoted else name.lower()
        physical = registered_name(gov_ctx, self.tid, name)
        for out, phys in self.columns:
            if folded == out or (not quoted and folded == out.lower()):
                return out
        for out, phys in self.columns:
            if physical in (phys, apply_sql_name(phys)) or folded == apply_sql_name(phys).lower():
                return out
        return None


def _relation(node: exp.Expr | None) -> _Relation | None:  # pyright: ignore[reportPrivateImportUsage]
    return None if node is None else node.meta.get(_RELATION)


def _key_name(physical: str) -> str:
    from provisa.compiler.naming import apply_sql_name

    return f"{KEY_MARK}{apply_sql_name(physical)}"


def _masked(gov_ctx: GovernanceContext, tid: int, physical: str) -> bool:
    entry = gov_ctx.masking_rules.get((tid, physical))
    return entry is not None and not _is_fake(entry[0])


def _served(gov_ctx: GovernanceContext, tid: int, physical: str) -> bool:
    vis = gov_ctx.visible_columns.get(tid)
    return vis is None or physical in vis


def _parameters_named(
    tbl: exp.Table, alias: str, tid: int, gov_ctx: GovernanceContext
) -> list[str]:  # REQ-039, REQ-2099, REQ-2100
    """The parameter columns of table ``tid`` the statement holding ``tbl`` names, under ``alias``
    or unqualified, and the role is served. A parameter is no column of the rows, so a statement
    that does not name one never has it from ``*``; one it names outside its WHERE conjuncts (a
    join on the words a search ran for, a read of an attached code table) is read through the
    relation, and one that is extracted is taken back out (``nf_extractor``)."""
    parameters = gov_ctx.parameter_columns.get(tid)
    if not parameters:
        return []
    alias_folded = alias.lower()
    named = {
        c.name.lower()
        for c in tbl.root().find_all(exp.Column)
        if not c.table or c.table.lower() == alias_folded
    }
    return [name for name, _ in parameters if name.lower() in named and _served(gov_ctx, tid, name)]


def _pass_up_chain(source: exp.Expr, name: str) -> None:  # pyright: ignore[reportPrivateImportUsage]  # REQ-039
    """Give each SELECT of the chain under a governed relation (the faked, kept and member
    projections, REQ-1494/1942/1620) the column ``name`` of what it reads, IN PLACE, so the
    relation can read it: those projections give the table's registered columns alone."""
    while isinstance(source, exp.Subquery) and isinstance(source.this, exp.Select):
        level = source.this
        source = level.args["from_"].this
        level.select(
            exp.alias_(
                exp.column(name, table=source.alias_or_name or None, quoted=True), name, quoted=True
            ),
            copy=False,
        )


def _governed_relation(
    tbl: exp.Table,
    tid: int,
    gov_ctx: GovernanceContext,
    *,
    view_body: bool = False,
    keys: set[str] | frozenset[str] = frozenset(),
) -> exp.Subquery:  # REQ-039, REQ-040, REQ-603, REQ-1494, REQ-1620, REQ-1942
    """Put the role's governed relation of ``tbl`` in its place, under the alias the statement
    gave it (or its own name): a derived table exposing only the columns the role is served, each
    masked as the role's masks say, with the role's row rule applied to the real values. A column
    the role is not served is not in it, so no reference anywhere can read it.

    It is the last stage of one chain on the base table: the faked projection (REQ-1494), the
    kept-mutation overlay (REQ-1942) and the member projection (REQ-1620) each apply the row rule
    and keep every column; this stage keeps the served ones and masks them.

    ``view_body``: ``tbl`` is read inside an administrator's view body, whose clauses may use a
    column hidden from the reader (REQ-039): every column is exposed, masked, and the body's
    select lists are judged afterwards (:func:`_judge_view_selects`). ``keys``: the columns the
    statement reads the real value of in an approved join (REQ-603) or as a write's key, carried
    under their internal names (:data:`KEY_MARK`), which no star, whole-row read, NATURAL or USING
    join ever includes."""
    from provisa.compiler.naming import apply_sql_name

    alias = _alias_for(tbl)
    given = tbl.args.get("alias")
    renamed = list(given.args.get("columns") or []) if given is not None else []
    attached = {k: tbl.args.get(k) for k in ("joins", "laterals", "pivots")}
    stand = tbl.copy()
    for k in ("alias", *attached):
        stand.set(k, None)
    stand.set("alias", _table_alias(alias))
    holder = exp.select("*").from_(stand)
    stand = holder.args["from_"].this

    members = _differing_members(tid, gov_ctx)
    carried = None if members is None else _member_carried(tid, members, gov_ctx, alias)
    current: exp.Subquery | None = None
    if tid in gov_ctx.fake_columns:
        current = _project_faked(stand, tid, gov_ctx, carried)
    if tid in gov_ctx.kept:
        current = _project_kept(stand, current, tid, gov_ctx, members)
    if members is not None:
        current = _project_members(stand, current, tid, members, gov_ctx, carried)
    source = holder.args["from_"].this

    projections: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    columns: list[tuple[str, str]] = []
    hidden: set[str] = set()
    names = iter(renamed)
    for physical, dtype in gov_ctx.all_columns.get(tid, []):
        served = _served(gov_ctx, tid, physical)
        if not served and not view_body:
            continue
        sql_name = apply_sql_name(physical)
        out = gov_ctx.published_name.get((tid, physical), sql_name)
        as_named = next(names, None)
        if as_named is not None:
            out = as_named.name if as_named.quoted else as_named.name.lower()
        # A column registered at a path in its document is read by its own name: a source whose
        # connector resolves the path holds it so (Trino's live MongoDB catalog holds the
        # document instead, and the routed statement reads the path there,
        # sql_rewrite.read_document_paths); a SQL database's table is read as it stores it
        # (_read_stored), the path extracted from its JSON column.
        value: exp.Expr = exp.column(sql_name, table=alias, quoted=True)  # pyright: ignore[reportPrivateImportUsage]
        entry = gov_ctx.masking_rules.get((tid, physical))
        if entry is not None and not _is_fake(entry[0]):
            rule, mask_type = entry
            value = mask_expression(rule, value, mask_type)
        projections.append(exp.alias_(value, out, quoted=True))
        columns.append((out, physical))
        if not served:
            hidden.add(physical)
    extra = list(names)
    if extra:
        raise ValueError(
            f'table "{alias}" has {len(columns)} columns available but '
            f"{len(columns) + len(extra)} columns specified"
        )
    if not columns:
        # Nothing of the table is served to the role: refused as a named hidden column is (V003),
        # never a relation with no columns.
        raise PermissionError(f"no column of {alias} is visible to this role")
    for name in _parameters_named(tbl, alias, tid, gov_ctx):
        projections.append(
            exp.alias_(exp.column(name, table=alias, quoted=True), name, quoted=True)
        )
        columns.append((name, name))
        _pass_up_chain(source, name)
    carried_keys: dict[str, str] = {}
    for physical in sorted(keys):
        sql_name = apply_sql_name(physical)
        real = f"__real_{sql_name}" if members is not None and physical in members else sql_name
        carried_keys[physical] = _key_name(physical)
        projections.append(
            exp.alias_(exp.column(real, table=alias, quoted=True), _key_name(physical), quoted=True)
        )
    body = exp.select(*projections).from_(source)
    if current is None:
        row_filter = _row_filter(gov_ctx, tid, alias)
        if row_filter is not None:
            body = body.where(row_filter)
    relation = exp.Subquery(this=body, alias=given.copy() if given is not None else None)
    if relation.args.get("alias") is None:
        relation.set("alias", exp.TableAlias(this=tbl.this.copy()))
    relation.args["alias"].set("columns", None)
    for k, v in attached.items():
        if v:
            relation.set(k, v)
    relation.meta[_GOVERNED] = True
    relation.meta[_RELATION] = _Relation(
        tid, _folded(relation.args["alias"].this), columns, frozenset(hidden), carried_keys
    )
    if tid in gov_ctx.stored_paths:
        _read_stored(relation.this, tbl, tid, gov_ctx)
    tbl.replace(relation)
    return relation


def _read_stored(body: exp.Expr, tbl: exp.Table, tid: int, gov_ctx: GovernanceContext) -> None:  # pyright: ignore[reportPrivateImportUsage]  # REQ-151, REQ-152
    """Each read of ``tbl`` inside ``body`` -- the governed relation of a table on a SQL database
    holding a column at a path in a JSON column -- as the table its source stores, IN PLACE: a
    derived table under the same alias exposing every column by its name, a path column as the
    extraction from its JSON column (PostgreSQL's ``->``/``->>``; the engine's transpile writes
    its own). Every level above reads the path column by its name, as every read names it."""
    from provisa.compiler.naming import apply_sql_name

    paths = gov_ctx.stored_paths[tid]
    for node in [t for t in body.find_all(exp.Table) if _same_table(t, tbl)]:
        alias = _alias_for(node)
        stored: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
        for physical, _ in gov_ctx.all_columns.get(tid, []):
            name = apply_sql_name(physical)
            if physical in paths:
                source, keys = paths[physical]
                value = _extracted(
                    exp.column(apply_sql_name(source), table=alias, quoted=True), keys
                )
                stored.append(exp.alias_(value, name, quoted=True))
            else:
                stored.append(exp.column(name, table=alias, quoted=True))
        read = exp.select(*stored).from_(_plain(node, alias))
        node.replace(_derived(read, alias))


def _same_table(node: exp.Table, tbl: exp.Table) -> bool:
    """Whether ``node`` names the relation ``tbl`` names, whatever alias either carries."""
    return all(node.args.get(k) == tbl.args.get(k) for k in ("this", "db", "catalog"))


def _extracted(column: exp.Column, keys: list[str]) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # REQ-151
    """The value at ``keys`` in the JSON ``column``, as text: ``->`` to each inner key and ``->>``
    at the last, as PostgreSQL reads it."""
    value: exp.Expr = column  # pyright: ignore[reportPrivateImportUsage]
    for i, key in enumerate(keys):
        path = exp.JSONPath(expressions=[exp.JSONPathRoot(), exp.JSONPathKey(this=key)])
        kind = exp.JSONExtractScalar if i == len(keys) - 1 else exp.JSONExtract
        value = kind(this=value, expression=path, only_json_types=True)
    return value


def _folded(identifier: exp.Identifier | None) -> str:
    if identifier is None:
        return ""
    return identifier.name if identifier.quoted else identifier.name.lower()


def _names_a_cte(tbl: exp.Table) -> bool:  # REQ-039
    """Whether ``tbl`` reads a CTE in its lexical scope, not a registered relation: an unqualified
    name matching a CTE of a WITH it is under -- in that WITH's main body, every CTE; in a CTE's
    body, the CTEs before it (every one, under RECURSIVE). ``bank.customer`` is never a CTE."""
    if tbl.args.get("db") is not None or tbl.args.get("catalog") is not None:
        return False
    if not isinstance(tbl.this, exp.Identifier):
        return False
    name = _folded(tbl.this)
    path: list[exp.Expr] = [tbl]  # pyright: ignore[reportPrivateImportUsage]
    node = tbl.parent
    while node is not None:
        with_ = node.args.get("with_")
        if isinstance(with_, exp.With):
            ctes = list(with_.expressions)
            inside = next((c for c in ctes if any(p is c for p in path)), None)
            if inside is None:
                visible = ctes
            elif with_.args.get("recursive"):
                visible = ctes
            else:
                visible = ctes[: ctes.index(inside)]
            if any(_folded(c.args["alias"].this) == name for c in visible):
                return True
        path.append(node)
        node = node.parent
    return False


def _inside_governed(node: exp.Expr) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    parent = node.parent
    while parent is not None:
        if parent.meta.get(_GOVERNED):
            return True
        parent = parent.parent
    return False


def _in_view_body(node: exp.Expr) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether ``node`` is inside an administrator's view body, marked where it was expanded
    (``view_expand.VIEW_MARK``), which no statement a role writes can name."""
    from provisa.compiler.view_expand import marked_view

    parent = node.parent
    while parent is not None:
        if isinstance(parent, exp.Subquery) and marked_view(parent.alias) is not None:
            return True
        parent = parent.parent
    return False


def _write_target(tbl: exp.Table) -> bool:
    """Whether ``tbl`` is the table a write writes (its target, not a table it reads)."""
    from provisa.compiler.write_admission import WRITES

    parent = tbl.parent
    if isinstance(parent, exp.Schema) and isinstance(parent.parent, exp.Insert):
        return parent.parent.this is parent
    if isinstance(parent, exp.TruncateTable):
        return True
    return isinstance(parent, WRITES) and parent.this is tbl


def _approved_keys(
    tree: exp.Expr,
    gov_ctx: GovernanceContext,  # pyright: ignore[reportPrivateImportUsage]
) -> tuple[dict[int, set[str]], list[tuple[exp.Column, exp.Table, str]]]:  # REQ-603
    """The columns whose real value an approved join of the statement reads and the relation
    would not otherwise expose as it is (hidden, masked, or read differently by a meta-role's
    members): per table node, its physical names; and each such reference with its table."""
    needed: dict[int, set[str]] = {}
    refs: list[tuple[exp.Column, exp.Table, str]] = []
    if not gov_ctx.join_keys:
        return needed, refs
    for col in list(tree.find_all(exp.Column)):
        if not col.table or _inside_governed(col) or not approved_join_key(col, gov_ctx):
            continue
        select = col.find_ancestor(exp.Select)
        found = _table_node_in_scope(col.table, select, gov_ctx) if select is not None else None
        if found is None:
            continue
        tbl, tid = found
        physical = registered_name(gov_ctx, tid, col.name)
        members = _differing_members(tid, gov_ctx) or []
        if (
            _served(gov_ctx, tid, physical)
            and not _masked(gov_ctx, tid, physical)
            and physical not in members
        ):
            continue  # the relation serves its real value as it is
        needed.setdefault(id(tbl), set()).add(physical)
        refs.append((col, tbl, physical))
    return needed, refs


def _table_node_in_scope(
    alias: str, select: exp.Select, gov_ctx: GovernanceContext
) -> tuple[exp.Table, int] | None:
    """The registered table node ``alias`` names where ``select`` reads it (as
    :func:`_table_in_scope`), with its id."""
    scope: exp.Expr | None = select  # pyright: ignore[reportPrivateImportUsage]
    while scope is not None:
        if isinstance(scope, exp.Select):
            for tbl, tid in _get_tables_from_select(scope, gov_ctx):
                if _alias_for(tbl) == alias and tid is not None:
                    return tbl, tid
        scope = scope.parent
    return None


def _govern_relations(
    tree: exp.Expr,
    gov_ctx: GovernanceContext,  # pyright: ignore[reportPrivateImportUsage]
) -> None:  # REQ-039
    """Replace, IN PLACE, every reference to a registered relation anywhere in ``tree`` -- a
    parenthesised join, a subquery, a CTE body, a LATERAL item, a set-operation branch, a write's
    FROM or USING -- with the role's governed relation, alias kept. Not replaced: a CTE reference,
    a table inside what governance itself put in (a row rule, a mask, a projection), and the
    table a write writes (``_govern_write`` governs it)."""
    needed, refs = _approved_keys(tree, gov_ctx)
    for tbl in list(tree.find_all(exp.Table)):
        if _inside_governed(tbl) or _names_a_cte(tbl) or _write_target(tbl):
            continue
        tid = _table_id_for_node(tbl, gov_ctx)
        if tid is None:
            continue
        _governed_relation(
            tbl, tid, gov_ctx, view_body=_in_view_body(tbl), keys=needed.get(id(tbl), set())
        )
    for col, _tbl, physical in refs:
        # The approved join reads the real key the relation carries under its internal name.
        col.set("this", exp.Identifier(this=_key_name(physical), quoted=True))
        col.meta[_GOVERNED] = True


def _from_items(select: exp.Expr) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]
    """The FROM items of ``select`` (a SELECT, or a write's FROM/USING), a parenthesised join's
    items included."""
    items: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    pending: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    from_ = select.args.get("from_")
    if from_ is not None:
        pending.append(from_.this)
    pending.extend(j.this for j in select.args.get("joins") or [])
    pending.extend(select.args.get("using") or [])
    while pending:
        item = pending.pop(0)
        if isinstance(item, exp.Subquery) and not isinstance(item.this, exp.Query):
            pending.insert(0, item.this)  # a parenthesised join: its items, not one relation
        items.append(item)
        pending[1:1] = [j.this for j in item.args.get("joins") or []]
    return items


def _resolve(col: exp.Column, gov_ctx: GovernanceContext) -> tuple[_Relation, str] | None:
    """The governed relation ``col`` reads and the name it exposes the column under: the
    nearest scope with a relation of its qualifier, or, unqualified, one exposing its name."""
    if isinstance(col.this, exp.Star):
        return None
    quoted = bool(col.this.quoted) if isinstance(col.this, exp.Identifier) else False
    qualifier = _folded(col.args.get("table"))
    scope = col.find_ancestor(exp.Select)
    while scope is not None:
        for item in _from_items(scope):
            relation = _relation(item)
            if qualifier:
                if (
                    _folded(item.args["alias"].this if item.args.get("alias") else None)
                    != qualifier
                ):
                    continue
                if relation is None:
                    return None
                out = relation.exposed(col.name, quoted, gov_ctx)
                return None if out is None else (relation, out)
            if relation is not None:
                out = relation.exposed(col.name, quoted, gov_ctx)
                if out is not None:
                    return relation, out
        scope = scope.parent.find_ancestor(exp.Select) if scope.parent is not None else None
    return None


def _hides(relation: _Relation, col: exp.Column, gov_ctx: GovernanceContext) -> bool:
    """Whether ``col`` names a registered column of ``relation``'s table the reader is not served."""
    physical = registered_name(gov_ctx, relation.tid, col.name)
    registered = {c for c, _ in gov_ctx.all_columns.get(relation.tid, [])}
    return physical in registered and not _served(gov_ctx, relation.tid, physical)


def _holds_served(relation: _Relation, col: exp.Column, gov_ctx: GovernanceContext) -> bool:
    """Whether ``col`` names a registered column of ``relation``'s table the reader is served."""
    physical = registered_name(gov_ctx, relation.tid, col.name)
    registered = {c for c, _ in gov_ctx.all_columns.get(relation.tid, [])}
    return physical in registered and _served(gov_ctx, relation.tid, physical)


def _unserved(relation: _Relation, col: exp.Column, gov_ctx: GovernanceContext) -> bool:
    """Whether the reader is not served ``col`` of ``relation``'s table, as the column check
    (V003) judges it: the table serves the role a subset of its columns, and not this name -- a
    hidden column, or one the table does not have, alike."""
    vis = gov_ctx.visible_columns.get(relation.tid)
    return vis is not None and registered_name(gov_ctx, relation.tid, col.name) not in vis


def _may_supply(item: exp.Expr, name: str) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether a FROM item that is no governed relation may have a column ``name``: yes unless
    its columns are known and ``name`` is not among them."""
    alias = item.args.get("alias")
    columns = alias.args.get("columns") if isinstance(alias, exp.TableAlias) else None
    if columns:
        return any(c.name.lower() == name for c in columns)
    query = item.this if isinstance(item, exp.Subquery) else None
    if isinstance(query, exp.Query) and "*" not in query.named_selects:
        return name in {n.lower() for n in query.named_selects}
    return True


def _names_an_output(col: exp.Column, select: exp.Select, clause: type[exp.Expr]) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether ``col`` is a key of ``select``'s ``clause`` (ORDER BY or GROUP BY) naming one of
    its outputs: a real alias, or a bare column's own name. A string literal's ``?column?``
    names none."""
    found = col.find_ancestor(clause, exp.Select)
    if not isinstance(found, clause) or found.parent is not select:
        return False
    name = col.name.lower()
    for e in select.expressions:
        if isinstance(e, exp.Alias) and e.alias.lower() == name:
            return True
        if (
            isinstance(e, exp.Column)
            and not isinstance(e.this, exp.Star)
            and e.name.lower() == name
        ):
            return True
    return False


def _refuse_unserved(col: exp.Column, gov_ctx: GovernanceContext) -> None:  # REQ-039
    """Refuse (V003), naming it, before anything is sent, a reference no relation in scope
    exposes and that a governed relation in its scope does not serve the reader (as the column
    check judges it: hidden, or a name the table does not have, alike, so the refusal tells
    nothing of which), in every clause of the reader's own SQL. Left to the engine: a name
    another FROM item of that scope may supply (a derived table, VALUES, a CTE, a function), a
    column the role is served that the relation does not expose (a parameter, REQ-1986), the key
    governance itself carries (``KEY_MARK``), an ORDER BY key naming an output -- PostgreSQL
    reads the output first -- and a GROUP BY key naming an output that is no hidden input column
    (it reads the input first)."""
    if isinstance(col.this, exp.Star) or col.name in SERVED_WITH_EVERY_TABLE:
        return
    if col.name.startswith(KEY_MARK) or col.find_ancestor(exp.Lambda) is not None:
        return
    qualifier = _folded(col.args.get("table"))
    first = col.find_ancestor(exp.Select)
    if first is not None and not qualifier and _names_an_output(col, first, exp.Order):
        return
    name = col.name.lower()
    scope = first
    while scope is not None:
        items = _from_items(scope)
        if qualifier:
            for item in items:
                alias = item.args.get("alias")
                if _folded(alias.this if alias else None) != qualifier:
                    continue
                relation = _relation(item)
                if relation is not None and _unserved(relation, col, gov_ctx):
                    raise PermissionError(
                        f"[V003] Column {qualifier}.{col.name} is not visible to this role"
                    )
                return
        else:
            if any(_may_supply(i, name) for i in items if _relation(i) is None):
                return
            relations = [r for i in items if (r := _relation(i)) is not None]
            if any(_hides(r, col, gov_ctx) for r in relations):
                raise PermissionError(f"[V003] Column {col.name!r} is not visible to this role")
            if any(_unserved(r, col, gov_ctx) for r in relations):
                if first is not None and scope is first and _names_an_output(col, first, exp.Group):
                    return
                raise PermissionError(f"[V003] Column {col.name!r} is not visible to this role")
            if any(_holds_served(r, col, gov_ctx) for r in relations):
                return  # the role is served it; the relation leaves it out (a parameter)
        scope = scope.parent.find_ancestor(exp.Select) if scope.parent is not None else None


def _bind_references(tree: exp.Expr, gov_ctx: GovernanceContext) -> None:  # pyright: ignore[reportPrivateImportUsage]
    """Name, IN PLACE, each reference to a governed relation's column as the relation exposes it
    (a published name, as PostgreSQL would fold it, or the physical one). A bare select-list
    column renamed keeps the name it was written with as its output name."""
    for col in list(tree.find_all(exp.Column)):
        if _inside_governed(col) or col.meta.get(_GOVERNED):
            continue
        found = _resolve(col, gov_ctx)
        if found is None:
            _refuse_unserved(col, gov_ctx)
            continue
        _relation_, out = found
        identifier = col.this
        if identifier.quoted and identifier.name == out:
            continue
        if not identifier.quoted and identifier.name.lower() == out:
            continue
        col.set("this", exp.Identifier(this=out, quoted=True))
        parent = col.parent
        if isinstance(parent, exp.Select) and any(e is col for e in parent.expressions):
            col.replace(exp.Alias(this=col.copy(), alias=identifier.copy()))


def _whole_row(relation: _Relation) -> exp.Subquery:
    """A whole-row read of ``relation`` as its served columns alone: a record of them, named."""
    row = exp.select(
        *(
            exp.column(out, table=relation.alias, quoted=True)
            for out, physical in relation.columns
            if physical not in relation.hidden
        )
    )
    inner = exp.Subquery(this=row, alias=_table_alias("_row"))
    return exp.Subquery(this=exp.select(exp.column("_row", quoted=True)).from_(inner))


def _private_columns(relation: _Relation) -> bool:
    """Whether ``relation`` exposes a column a star or a whole-row read must not: a carried key,
    or a column hidden from the reader inside a view body."""
    return bool(relation.keys or relation.hidden)


def _govern_whole_rows(tree: exp.Expr) -> None:  # pyright: ignore[reportPrivateImportUsage]
    """A whole-row read (``SELECT c``, ``row_to_json(c)``, ``to_jsonb(c.*)``) of a relation that
    carries a private column reads its served columns alone."""
    for col in list(tree.find_all(exp.Column)):
        if _inside_governed(col) or col.meta.get(_GOVERNED):
            continue
        parent = col.parent
        in_list = isinstance(parent, exp.Select) and any(e is col for e in parent.expressions)
        if isinstance(col.this, exp.Star):
            if in_list or not col.table:
                continue  # a select-list star is expanded with the rest (_expand_stars)
            name = _folded(col.args.get("table"))
        elif not col.table:
            name = _folded(col.this)
        else:
            continue
        scope = col.find_ancestor(exp.Select)
        while scope is not None:
            hits = [
                item
                for item in _from_items(scope)
                if item.args.get("alias") is not None and _folded(item.args["alias"].this) == name
            ]
            if hits:
                relation = _relation(hits[0])
                if (
                    relation is not None
                    and _private_columns(relation)
                    and (isinstance(col.this, exp.Star) or not _exposes(relation, name))
                ):
                    replacement = _whole_row(relation)
                    replacement.meta[_GOVERNED] = True
                    if in_list:
                        col.replace(exp.alias_(replacement, name, quoted=True))
                    else:
                        col.replace(replacement)
                break
            scope = scope.parent.find_ancestor(exp.Select) if scope.parent is not None else None


def _exposes(relation: _Relation, name: str) -> bool:
    return any(out == name or out.lower() == name for out, _ in relation.columns)


def _star_columns(
    relation: _Relation, gov_ctx: GovernanceContext, *, final: bool
) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]
    """A star's columns of ``relation``: the served ones, each as the answer reads it."""
    result: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    for out, physical in relation.columns:
        if physical in relation.hidden:
            continue
        col = exp.column(out, table=relation.alias, quoted=True)
        read = _answer_read(col, relation, physical, gov_ctx) if final else col
        if read is col:
            result.append(col)
        else:
            read.meta[_GOVERNED] = True
            result.append(exp.alias_(read, out, quoted=True))
    return result


def _answer_read(
    col: exp.Column, relation: _Relation, physical: str, gov_ctx: GovernanceContext
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """``col`` -- a reference to the relation's column ``physical`` in the statement's answer --
    as a read of the column returns it (REQ-1961): a numeric the model records no size for is
    read as floating point, its mask too when the mask keeps it a number (a constant)."""
    from provisa.core.catalog_types import is_undeclared_numeric
    from provisa.security.masking import MaskType

    dtype = dict(gov_ctx.all_columns.get(relation.tid, [])).get(physical)
    if dtype is None or not is_undeclared_numeric(dtype):
        return col
    entry = gov_ctx.masking_rules.get((relation.tid, physical))
    if entry is not None and not _is_fake(entry[0]) and entry[0].mask_type != MaskType.constant:
        return col
    return exp.Cast(this=col, to=exp.DataType.build("double precision", dialect="postgres"))


def _merging_joins(select: exp.Select) -> bool:
    return any(
        j.args.get("using")
        or (j.args.get("method") or "").upper() == "NATURAL"
        or j.args.get("kind") == "NATURAL"
        for j in select.args.get("joins") or []
    ) or any(
        isinstance(item, exp.Subquery) and not isinstance(item.this, exp.Query)
        for item in _from_items(select)
    )


def _expand_stars(
    tree: exp.Expr,
    gov_ctx: GovernanceContext,
    *,
    answers: bool,  # pyright: ignore[reportPrivateImportUsage]
) -> None:  # REQ-039, REQ-603, REQ-1961
    """A select-list star over a governed relation that carries a private column (a key, or a
    column hidden from the reader in a view body) is the relation's served columns; so is one in
    the statement's answer over a relation a read of which converts a column (REQ-1961). Any other
    star is left as written: the relations it covers expose only what the role is served."""
    for select in list(tree.find_all(exp.Select)):
        if _inside_governed(select):
            continue
        final = answers and isinstance(tree, exp.Query) and _is_the_answer(select, tree)
        if not any(
            isinstance(e, exp.Star) or (isinstance(e, exp.Column) and isinstance(e.this, exp.Star))
            for e in select.expressions
        ):
            continue
        items = _from_items(select)
        expanded: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
        changed = False
        for expr in select.expressions:
            if isinstance(expr, exp.Star):
                covered = items
            elif isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                qualifier = _folded(expr.args.get("table"))
                covered = [
                    i
                    for i in items
                    if i.args.get("alias") is not None
                    and _folded(i.args["alias"].this) == qualifier
                ]
            else:
                expanded.append(expr)
                continue
            relations = [_relation(i) for i in covered]
            private = any(r is not None and _private_columns(r) for r in relations)
            converts = final and any(
                r is not None
                and any(
                    _answer_read(exp.column(o), r, p, gov_ctx).__class__ is exp.Cast
                    for o, p in r.columns
                )
                for r in relations
            )
            if not (private or converts):
                expanded.append(expr)
                continue
            if isinstance(expr, exp.Star) and _merging_joins(select):
                if private:
                    raise PermissionError(
                        "[V006] a star over a NATURAL, USING or parenthesised join of a relation "
                        "read through a relationship's key is not supported: name the columns"
                    )
                expanded.append(expr)
                continue
            changed = True
            for item, relation in zip(covered, relations):
                if relation is not None:
                    expanded.extend(_star_columns(relation, gov_ctx, final=final))
                elif item.args.get("alias") is not None:
                    expanded.append(
                        exp.Column(this=exp.Star(), table=item.args["alias"].this.copy())
                    )
                else:
                    raise PermissionError(
                        "[V006] a star over an unaliased relation beside a governed one is not "
                        "supported: alias it or name the columns"
                    )
        if changed:
            select.set("expressions", expanded)


def _read_answer_columns(
    tree: exp.Expr,
    gov_ctx: GovernanceContext,  # pyright: ignore[reportPrivateImportUsage]
) -> None:  # REQ-1961
    """Each bare column of the statement's answer that reads a governed relation's column, read
    as a read of the column returns it (:func:`_answer_read`), under the name it had."""
    if not isinstance(tree, exp.Query):
        return
    for select in list(tree.find_all(exp.Select)):
        if _inside_governed(select) or not _is_the_answer(select, tree):
            continue
        for expr in list(select.expressions):
            col = expr.this if isinstance(expr, exp.Alias) else expr
            if not isinstance(col, exp.Column) or col.meta.get(_GOVERNED):
                continue
            found = _resolve(col, gov_ctx)
            if found is None:
                continue
            relation, out = found
            physical = dict(relation.columns)[out]
            read = _answer_read(col.copy(), relation, physical, gov_ctx)
            if read.__class__ is not exp.Cast:
                continue
            read.meta[_GOVERNED] = True
            name = expr.args["alias"].copy() if isinstance(expr, exp.Alias) else col.this.copy()
            expr.replace(exp.Alias(this=read, alias=name))


def _judge_view_selects(tree: exp.Expr, gov_ctx: GovernanceContext) -> None:  # pyright: ignore[reportPrivateImportUsage]
    """REQ-039: in an administrator's view body only its select lists are judged against the
    reader. A column hidden from the reader read there is NULL; its other clauses read it."""
    for select in list(tree.find_all(exp.Select)):
        if _inside_governed(select) or not _in_view_body(select):
            continue
        for expr in list(select.expressions):
            for col in (
                list(expr.find_all(exp.Column)) if not isinstance(expr, exp.Column) else [expr]
            ):
                if col.find_ancestor(exp.Select) is not select or col.meta.get(_GOVERNED):
                    continue
                found = _resolve(col, gov_ctx)
                if found is None:
                    continue
                relation, out = found
                if dict(relation.columns)[out] not in relation.hidden:
                    continue
                null = exp.Null()
                null.meta[_GOVERNED] = True
                if col is expr:
                    expr.replace(exp.alias_(null, col.name, quoted=bool(col.this.quoted)))
                elif isinstance(expr, exp.Alias) and expr.this is col:
                    col.replace(null)
                else:
                    col.replace(null)


def unmark_views(sql: str) -> str:  # REQ-039
    """``sql`` -- a statement whose views were expanded marked (``view_read.semantic_statement``)
    -- without the marks: the text every use other than governance reads, so the mark is never
    logged, audited or keyed on."""
    return _unmark_views(sqlglot.parse_one(sql, read="postgres")).sql(dialect="postgres")


def _unmark_views(tree: exp.Expr) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """The statement without the marks its view bodies were expanded under: each
    ``SELECT * FROM (<body>) AS "<mark><view>"`` is the body itself, so the mark is never in the
    statement that runs, is logged or is explained (RV597b M3)."""
    from provisa.compiler.view_expand import marked_view

    for sub in list(tree.find_all(exp.Subquery)):
        if marked_view(sub.alias) is None:
            continue
        wrapper = sub.parent.parent if isinstance(sub.parent, exp.From) else None
        if (
            isinstance(wrapper, exp.Select)
            and len(wrapper.expressions) == 1
            and isinstance(wrapper.expressions[0], exp.Star)
            and not wrapper.args.get("where")
            and not wrapper.args.get("joins")
        ):
            if wrapper is tree:
                tree = sub.this
            else:
                wrapper.replace(sub.this)
        else:
            sub.set("alias", _table_alias("_view"))
    return tree


def _table_alias(alias: str) -> exp.TableAlias:
    return exp.TableAlias(this=exp.to_identifier(alias, quoted=True))


def _derived(query: exp.Query, alias: str) -> exp.Subquery:
    """``query`` as a derived table under ``alias``, marked as governance's own: the references
    inside it are already governed, and no later pass governs them again."""
    subquery = exp.Subquery(this=query, alias=_table_alias(alias))
    subquery.meta[_GOVERNED] = True
    return subquery


def _plain(tbl: exp.Table, alias: str) -> exp.Table:
    """``tbl`` as the base relation of a projection: the same table, under ``alias``."""
    plain = tbl.copy()
    plain.set("alias", _table_alias(alias))
    return plain


def _row_filter(gov_ctx: GovernanceContext, tid: int, alias: str) -> exp.Expr | None:  # pyright: ignore[reportPrivateImportUsage]
    """The role's row rule on ``tid``, qualified to ``alias``; None when it has none."""
    if tid not in gov_ctx.rls_rules:
        return None
    column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
    return qualified_filter(gov_ctx.rls_rules[tid], alias, column_types)


def _project_faked(
    tbl: exp.Table,
    tid: int,
    gov_ctx: GovernanceContext,
    carried: dict[str, tuple[exp.Expr, exp.Expr]] | None = None,
) -> exp.Subquery:  # REQ-1494
    """Put the faked projection of ``tbl`` in its place, under the same alias."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.fakes.checks import family
    from provisa.fakes.projection import faked_projection

    alias = _alias_for(tbl)
    base = tbl.copy()
    base.set("alias", None)
    columns = [
        (apply_sql_name(name), dtype, family(dtype)) for name, dtype in gov_ctx.all_columns[tid]
    ]
    assert gov_ctx.fake_fingerprint is not None  # bound with the faked columns
    projection = faked_projection(
        base,
        alias,
        columns,
        gov_ctx.fake_columns[tid],
        gov_ctx.fake_fingerprint,
        _row_filter(gov_ctx, tid, alias),
        None if carried is None else {n: base_expr for n, (base_expr, _) in carried.items()},
    )
    subquery = _derived(projection, alias)
    tbl.replace(subquery)
    return subquery


def _row_columns(gov_ctx: GovernanceContext, tid: int, alias: str) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]
    """Every column of ``tid``'s row read under ``alias``, each by name: a level of the chain
    names the columns it reads off the table, never ``*``, so the relation of the table an engine
    reads gives each as it serves it (a path column, sql_rewrite.read_document_paths) and the
    columns no level above reads are dropped (governed_relation.keep_read)."""
    from provisa.compiler.naming import apply_sql_name

    return [
        exp.column(apply_sql_name(name), table=alias, quoted=True)
        for name, _ in gov_ctx.all_columns.get(tid, [])
    ]


def _with_carried(
    query: exp.Select,
    row: list[exp.Expr],  # pyright: ignore[reportPrivateImportUsage]
    carried: dict[str, tuple[exp.Expr, exp.Expr]],
) -> exp.Select:  # REQ-1620
    """``query`` -- a read of a table's rows -- with every column of the row (``row``) and the
    member flags and real values ``carried`` names, each computed on the real row."""
    return query.select(
        *(c.copy() for c in row),
        *(exp.alias_(e.copy(), n, quoted=True) for n, (e, _) in carried.items()),
        append=False,
    )


def _project_kept(
    tbl: exp.Table,
    faked: exp.Subquery | None,
    tid: int,
    gov_ctx: GovernanceContext,
    members: list[str] | None = None,
) -> exp.Subquery:  # REQ-1942
    """Put ``tbl`` -- or ``faked``, its faked projection already in its place -- with its change
    log applied in its place, under the same alias. The row filter reads the table's rows and the
    log's versions alike."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.core.env_changes import overlay

    alias = _alias_for(tbl)
    log, key = gov_ctx.kept[tid]
    column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
    rls = gov_ctx.rls_rules.get(tid)
    if faked is not None:
        base: exp.Query = faked.this  # the row filter is inside the projection
    else:
        row = _row_columns(gov_ctx, tid, alias)
        base = exp.select(*row).from_(_plain(tbl, alias))
        if members is not None:
            # REQ-1620: the member flags and real values, computed on the table's real rows.
            base = _with_carried(base, row, _member_carried(tid, members, gov_ctx, alias))
        row_filter = _row_filter(gov_ctx, tid, alias)
        if row_filter is not None:
            base = base.where(row_filter)
    applied = overlay(
        base,
        log,
        [apply_sql_name(k) for k in key],
        [apply_sql_name(name) for name, _ in gov_ctx.all_columns[tid]],
        None if rls is None else qualified_filter(rls, "l", column_types),
        # REQ-1620: the same flags and real values over a written row's version.
        None
        if members is None
        else {
            n: log_expr
            for n, (_, log_expr) in _member_carried(tid, members, gov_ctx, alias).items()
        },
    )
    subquery = _derived(applied, alias)
    (faked if faked is not None else tbl).replace(subquery)
    return subquery


def _differing_members(tid: int, gov_ctx: GovernanceContext) -> list[str] | None:  # REQ-1620
    """The columns of ``tid`` the acting meta-role's members read differently row by row, or None
    when there are none (a single role, or members whose reads of every column are settled for
    every row: MemberReads.resolved) -- then the table is read as it is, with no member projection."""
    reads = gov_ctx.member_reads.get(tid)
    if reads is None:
        return None
    differing = [c for c in reads.columns if reads.resolved(c) is None]
    return differing or None


def _member_branches(reads: Any, column: str) -> tuple[list[tuple[list[int], int]], int | None]:
    """How the member projection reads ``column``: the WHEN branches, each the members whose
    flags select it and the treatment it reads, in order from the least restrictive; and the
    treatment the remaining rows read (a group holding a member that admits every row), or None
    when they read NULL. A group after one admitting every row is never reached."""
    treatments = reads.columns[column]
    branches: list[tuple[list[int], int]] = []
    for rank in sorted({t[0] for t in treatments}):
        group = [i for i, t in enumerate(treatments) if t[0] == rank]
        if any(every_row(reads.flags[i]) for i in group):
            return branches, group[0]
        branches.append((group, group[0]))
    return branches, None


def _member_carried(
    tid: int, differing: list[str], gov_ctx: GovernanceContext, alias: str
) -> dict[str, tuple[exp.Expr, exp.Expr]]:  # pyright: ignore[reportPrivateImportUsage]  # REQ-1620
    """What a member projection reads off the real row, by internal name: each member's flag
    (``__m_<i>``, its own row filter) and the real value of each column the members read
    differently (``__real_<name>``) -- each as an expression over ``alias`` and over a change log's
    version (``l``)."""
    from provisa.compiler.naming import apply_sql_name

    reads = gov_ctx.member_reads[tid]
    column_types = {name: dtype for name, dtype in gov_ctx.all_columns.get(tid, [])}
    carried: dict[str, tuple[exp.Expr, exp.Expr]] = {}  # pyright: ignore[reportPrivateImportUsage]
    # Only the flags a WHEN branch reads; a member admitting every row is the branches' ELSE.
    read = sorted(
        {i for c in differing for group, _ in _member_branches(reads, c)[0] for i in group}
    )
    for i in read:
        flag = reads.flags[i]
        carried[f"__m_{i}"] = (
            exp.paren(qualified_filter(flag, alias, column_types)),
            exp.paren(qualified_filter(flag, "l", column_types)),
        )
    for name in differing:
        sql_name = apply_sql_name(name)
        carried[f"__real_{sql_name}"] = (
            exp.column(sql_name, table=alias, quoted=True),
            exp.column(sql_name, table="l", quoted=True),
        )
    return carried


def _project_members(
    tbl: exp.Table,
    current: exp.Subquery | None,
    tid: int,
    differing: list[str],
    gov_ctx: GovernanceContext,
    carried: dict[str, tuple[exp.Expr, exp.Expr]] | None,
) -> exp.Subquery:  # REQ-1620
    """Put ``tbl`` -- or ``current``, the faked or kept projection already in its place, which
    carries the member flags and real values (:func:`_member_carried`) -- read row by row as the
    members of the acting meta-role read it, in its place, under the same alias. Each column an
    approved join may read is carried as well, as its real value under ``__real_<name>``, which
    the governed relation passes on only for a join that reads it (:func:`_governed_relation`).

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
    carried = carried or _member_carried(tid, differing, gov_ctx, alias)
    if current is not None:
        # The level below gives every column of the row and then ``carried``, by name.
        scan: exp.Select = exp.select(
            *_row_columns(gov_ctx, tid, alias),
            *(exp.column(n, table=alias, quoted=True) for n in carried),
        ).from_(_derived(current.this, alias))
    else:
        scan = _with_carried(
            exp.select("*").from_(_plain(tbl, alias)), _row_columns(gov_ctx, tid, alias), carried
        )
        row_filter = _row_filter(gov_ctx, tid, alias)
        if row_filter is not None:
            scan = scan.where(row_filter)
    flag_names = [f"__m_{i}" for i in range(len(reads.flags))]
    keys = {c1 for (t1, _t2, c1, _c2) in gov_ctx.join_keys if t1 == tid}
    projections: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    for name, _dtype in gov_ctx.all_columns.get(tid, []):
        sql_name = apply_sql_name(name)
        if name not in differing:
            projections.append(exp.column(sql_name, quoted=True))
            continue
        real = f"__real_{sql_name}"
        treatments = reads.columns[name]

        def _value(member: int, real: str = real, sql_name: str = sql_name) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
            rank, rule, dtype = treatments[member]
            if rank == HIDDEN:
                return exp.Null()
            if rule is None:
                return exp.column(real, quoted=True)
            if rule.mask_type == MaskType.fake:
                return exp.column(sql_name, quoted=True)  # the faked projection's value
            return mask_expression(rule, exp.column(real, quoted=True), dtype)

        whens, rest = _member_branches(reads, name)
        case = exp.Case(
            ifs=[
                exp.If(
                    this=exp.or_(*(exp.column(flag_names[i], quoted=True) for i in group)),
                    true=_value(member),
                )
                for group, member in whens
            ],
            default=exp.Null() if rest is None else _value(rest),
        )
        projections.append(exp.alias_(case, sql_name, quoted=True))
        if name in keys:
            projections.append(exp.alias_(exp.column(real, quoted=True), real, quoted=True))
    subquery = _derived(exp.select(*projections).from_(_derived(scan, alias)), alias)
    (current if current is not None else tbl).replace(subquery)
    return subquery


def _already_governed(col: exp.Column) -> bool:
    parent = col.parent
    while parent is not None:
        if parent.meta.get(_GOVERNED):
            return True
        parent = parent.parent
    return False


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
            return mask_expression(rule, col, dtype)
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
                return mask_expression(rule, col, dtype)
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
                masked = mask_expression(rule, col_expr, col_dtype)
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
    """Govern ``tree`` by construction (REQ-039): every registered relation it reads, wherever it
    stands, is replaced by the role's governed relation (:func:`_govern_relations`), so a column
    the role is not served is in no relation the statement reads, and a masked one is its mask
    in every clause. Then each reference is named as its relation exposes it, a whole-row read or
    star over a relation carrying a private column reads the served columns, and a view body's
    select lists are judged against the reader.

    ``answers``: ``tree`` is a read whose rows are the answer (not a fragment of a larger
    statement, and not a write), so its own projection is read as REQ-1961 says."""
    from provisa.compiler.sql_rewrite import bind_schema_qualified_columns
    from provisa.vector.similarity import govern_calls

    # REQ-2204: a column the statement qualifies by its table's schema is read by the table's
    # name before any table is put behind a relation under that name.
    bind_schema_qualified_columns(tree)
    # REQ-423/426: a similarity search names an embedding column the role is served, and is
    # completed with its model and dimensions -- read off the tables as the statement names them.
    for select in list(tree.find_all(exp.Select)):
        if not any(True for _ in select.find_all(exp.Anonymous)):
            continue
        alias_to_tid = {
            _alias_for(t): tid
            for t, tid in _get_tables_from_select(select, gov_ctx)
            if tid is not None
        }
        if alias_to_tid:
            govern_calls(
                select,
                alias_to_tid,
                gov_ctx.embedding_columns,
                gov_ctx.embedding_dims,
                lambda col, a=alias_to_tid: _is_column_visible(col, a, gov_ctx),
                gov_ctx.role_id,
            )
    _govern_relations(tree, gov_ctx)
    _govern_whole_rows(tree)
    _bind_references(tree, gov_ctx)
    _expand_stars(tree, gov_ctx, answers=answers)
    if answers:
        _read_answer_columns(tree, gov_ctx)
    _judge_view_selects(tree, gov_ctx)
    return _unmark_views(tree)


def _refuse_hidden(col: exp.Column, tid: int, gov_ctx: GovernanceContext) -> None:
    if not _served(gov_ctx, tid, registered_name(gov_ctx, tid, col.name)):
        raise PermissionError(f"[V003] Column {col.name!r} is not visible to this role")


def _served_returning(target: exp.Table, tid: int, gov_ctx: GovernanceContext) -> exp.Returning:
    """The rows a write returns, as the role reads them: its served columns, masked."""
    from provisa.compiler.naming import apply_sql_name

    alias = _alias_for(target)
    columns: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    for physical, dtype in gov_ctx.all_columns.get(tid, []):
        if not _served(gov_ctx, tid, physical):
            continue
        value: exp.Expr = exp.column(apply_sql_name(physical), table=alias, quoted=True)  # pyright: ignore[reportPrivateImportUsage]
        entry = gov_ctx.masking_rules.get((tid, physical))
        if entry is not None and not _is_fake(entry[0]):
            value = mask_expression(entry[0], value, entry[1])
        out = gov_ctx.published_name.get((tid, physical), apply_sql_name(physical))
        columns.append(exp.alias_(value, out, quoted=True))
    if not columns:
        raise PermissionError(f"no column of {alias} is visible to this role")
    return exp.Returning(expressions=columns)


def _read_returned(
    tree: exp.Expr,
    write: exp.Expr,
    target: exp.Table,
    tid: int,
    gov_ctx: GovernanceContext,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """The write's RETURNING read over the rows it returns as the role reads them: the write
    returns its served columns, masked, under an internal name, and the role's RETURNING list is
    read over that, where a column the role is not served does not exist. A write at the root
    becomes ``WITH <w> AS (<write> RETURNING <served>) SELECT <its list> FROM <w>``; one already
    in that shape (``WITH w AS (<write> RETURNING ...) SELECT ... FROM w``) keeps it."""
    returning = write.args["returning"]
    listed = [e.copy() for e in returning.expressions]
    write.set("returning", _served_returning(target, tid, gov_ctx))
    alias = _alias_for(target)
    name = f"{KEY_MARK}w"
    read = exp.select(*listed).from_(
        exp.Table(this=exp.to_identifier(name, quoted=True), alias=_table_alias(alias))
    )
    own = write.args.get("with_")
    hoisted = list(own.expressions) if own is not None else []
    write.set("with_", None)
    if write is tree:
        written = exp.CTE(this=write, alias=_table_alias(name))
        read.set("with_", exp.With(expressions=[*hoisted, written]))
        return read
    cte = write.parent
    assert isinstance(cte, exp.CTE)
    with_ = cte.parent
    assert isinstance(with_, exp.With)
    ctes = list(with_.expressions)
    at = next(i for i, c in enumerate(ctes) if c is cte)
    written = exp.CTE(this=write.pop(), alias=_table_alias(name))
    cte.set("this", read)
    with_.set("expressions", [*ctes[:at], *hoisted, written, *ctes[at:]])
    return tree


def _plain_write(write: exp.Expr, role_where: exp.Where | None) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether every expression the role wrote in an UPDATE or DELETE reads only its target: no
    FROM or USING, no WITH and no subquery. Then every column it names is the target's, decided
    here, and the write is governed as written; otherwise its expressions are read in a leading
    CTE over the governed relation (:func:`_keyed_write`)."""
    if write.args.get("from_") or write.args.get("using") or write.args.get("joins"):
        return False
    if write.args.get("with_") is not None:
        return False
    parts = [*(write.expressions if isinstance(write, exp.Update) else []), role_where]
    parts.append(write.args.get("returning"))
    return not any(p is not None and p.find(exp.Query) is not None for p in parts)


def _govern_plain_write(
    write: exp.Expr,
    role_where: exp.Where | None,
    target: exp.Table,
    tid: int,
    gov_ctx: GovernanceContext,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Where | None:
    """An UPDATE or DELETE whose expressions read only its target, governed in place: a column the
    role is not served is refused (V003) and a masked one is its mask. Returns the role's WHERE."""
    alias = _alias_for(target)
    names = {_folded(target.this), alias.lower(), alias}
    values = [e.expression for e in write.expressions] if isinstance(write, exp.Update) else []
    for part in [*values, role_where]:
        if part is None:
            continue
        for col in list(part.find_all(exp.Column)) if not isinstance(part, exp.Column) else [part]:
            if col.table and _folded(col.args.get("table")) not in names:
                continue
            _refuse_hidden(col, tid, gov_ctx)
            physical = registered_name(gov_ctx, tid, col.name)
            if (tid, physical) in gov_ctx.masking_rules and _is_fake(
                gov_ctx.masking_rules[(tid, physical)][0]
            ):
                raise PermissionError(
                    f"[V006] a write may not read the faked column {col.name!r}: its value is not "
                    "the row's"
                )
            if _masked(gov_ctx, tid, physical):
                masked = _maybe_mask_column(col, {alias: tid, col.table or alias: tid}, gov_ctx)
                masked.meta[_GOVERNED] = True
                if part is col:
                    values[values.index(col)] = masked
                    for e in write.expressions:
                        if e.expression is col:
                            e.set("expression", masked)
                else:
                    col.replace(masked)
    # The rows it may write are its grant's (admission: the grant's rows, which are the role's
    # read row rule unless the grant states its own, and its U or D rule).
    return role_where


def _keyed_write(
    write: exp.Expr,
    role_where: exp.Where | None,
    target: exp.Table,
    tid: int,
    gov_ctx: GovernanceContext,  # pyright: ignore[reportPrivateImportUsage]
) -> exp.Where:
    """An UPDATE or DELETE whose expressions read beyond its target, governed by construction:
    the role's WHERE, SET values and FROM or USING are read in a leading CTE over the role's
    governed relation of the target (a CTE body cannot see the statement it leads), giving each
    row's key and new values; the write itself holds only compiler-written expressions -- the
    key match and the grant (added by admission). Returns the WHERE it matches the keys by."""
    from provisa.compiler.write_admission import WriteFormNotSupported

    form = "update_from" if isinstance(write, exp.Update) else "delete_using"
    if form in gov_ctx.write_refused_forms.get(tid, frozenset()):
        raise WriteFormNotSupported(target.name, form)
    key = list(gov_ctx.pk_columns.get(tid) or (gov_ctx.kept[tid][1] if tid in gov_ctx.kept else []))
    if not key:
        raise PermissionError(
            f"[V006] {target.name!r} has no registered primary key: a write that reads a "
            "subquery or another relation cannot be governed without one"
        )
    alias = _alias_for(target)
    holder = exp.select("*").from_(_plain(target, alias))
    relation = _governed_relation(holder.args["from_"].this, tid, gov_ctx, keys=set(key))
    rel = _relation(relation)
    assert rel is not None
    name = f"{KEY_MARK}k"
    outputs: list[exp.Expr] = [  # pyright: ignore[reportPrivateImportUsage]
        exp.alias_(exp.column(rel.keys[k], table=rel.alias, quoted=True), f"k{i}", quoted=True)
        for i, k in enumerate(key)
    ]
    if isinstance(write, exp.Update):
        assignments = list(write.expressions)
        for i, assignment in enumerate(assignments):
            outputs.append(exp.alias_(assignment.expression.copy(), f"v{i}", quoted=True))
            assignment.set("expression", exp.column(f"v{i}", table=name, quoted=True))
    body = exp.select(*outputs).from_(relation)
    items = []
    from_ = write.args.get("from_")
    if from_ is not None:
        items.append(from_.this)
    items.extend(write.args.get("using") or [])
    for item in items:
        body = body.join(item.copy(), join_type="CROSS")
    for join in write.args.get("joins") or []:
        body.append("joins", join.copy())
    if role_where is not None:
        body = body.where(role_where.this.copy())
    ref = exp.Table(this=exp.to_identifier(name, quoted=True))
    if isinstance(write, exp.Update):
        write.set("from_", exp.From(this=ref))
        write.set("joins", None)
    else:
        write.set("using", [ref])
    cte = exp.CTE(this=body, alias=_table_alias(name))
    own = write.args.get("with_")
    write.set("with_", exp.With(expressions=[*(own.expressions if own is not None else []), cte]))
    return exp.Where(
        this=exp.and_(
            *(
                exp.EQ(
                    this=exp.column(k, table=alias, quoted=True),
                    expression=exp.column(f"k{i}", table=name, quoted=True),
                )
                for i, k in enumerate(key)
            )
        )
    )


def _govern_conflict(write: exp.Insert, target: exp.Table) -> None:
    """``INSERT ... ON CONFLICT DO UPDATE`` may read the row it inserts (``EXCLUDED``) and
    constants, never the existing row: a column of it is refused, so no conflict update reads a
    value the role is not served (REQ-039)."""
    conflict = write.args.get("conflict")
    if conflict is None:
        return
    parts = [e.expression for e in conflict.expressions if isinstance(e, exp.EQ)]
    parts.append(conflict.args.get("where"))
    for part in parts:
        if part is None:
            continue
        if part.find(exp.Query) is not None:
            raise PermissionError("[V006] ON CONFLICT DO UPDATE may not read a subquery")
        for col in list(part.find_all(exp.Column)) if not isinstance(part, exp.Column) else [part]:
            if _folded(col.args.get("table")) != "excluded":
                raise PermissionError(
                    f"[V003] ON CONFLICT DO UPDATE may read EXCLUDED and constants, not the "
                    f"existing row's {col.name!r}"
                )


def _govern_merge(
    write: exp.Merge, target: exp.Table, tid: int, gov_ctx: GovernanceContext
) -> None:
    """A MERGE's source is a read, governed as any is; its WHEN conditions and values may read
    the source and constants, not the target row; its ON may read the target's served columns."""
    alias = _alias_for(target)
    names = {_folded(target.this), alias.lower(), alias}
    on = write.args.get("on")
    if on is not None:
        if on.find(exp.Query) is not None:
            raise PermissionError("[V006] a MERGE's ON may not read a subquery")
        for col in on.find_all(exp.Column):
            if not col.table:
                raise PermissionError(f"[V006] qualify {col.name!r} in the MERGE's ON")
            if _folded(col.args.get("table")) in names:
                _refuse_hidden(col, tid, gov_ctx)
    whens = write.args.get("whens")
    for col in whens.find_all(exp.Column) if whens is not None else []:
        if not col.table or _folded(col.args.get("table")) in names:
            if isinstance(col.parent, exp.EQ) and col.parent.this is col:
                continue  # the column a WHEN sets
            if isinstance(col.parent, exp.Tuple) and isinstance(col.parent.parent, exp.Insert):
                continue  # a column a WHEN NOT MATCHED inserts
            raise PermissionError(
                f"[V003] a MERGE's WHEN may read its source and constants, not the target's "
                f"{col.name!r}"
            )


def _govern_write(
    tree: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    write: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    gov_ctx: GovernanceContext,
    session_vars: dict[str, str] | None,
    params: list | None,
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # REQ-039, REQ-2074, REQ-2188
    """Admit ``write`` (the statement's one write, at the root or as ``WITH w AS (<write>
    RETURNING ...)``) and govern what it reads (REQ-039): the target stays the base table, since
    a derived table cannot be written, with the grant's rows and the role's U or D rule in its
    WHERE (admission); what the role wrote is read over the governed relation; its RETURNING is
    read over the served, masked columns of the rows it wrote. Returns the statement."""
    from provisa.compiler.write_admission import _target, admit_write

    role_where = write.args.get("where") if isinstance(write, (exp.Update, exp.Delete)) else None
    if role_where is not None:
        write.set("where", None)
    admitted = admit_write(write, gov_ctx, session_vars, params)  # pyright: ignore[reportArgumentType]
    kept = admitted.copy()
    if role_where is not None:
        terms = [role_where.this.copy()]
        if kept.args.get("where") is not None:
            terms.append(kept.args["where"].this)
        kept.set("where", exp.Where(this=exp.and_(*terms)))
    gov_ctx.admitted_write = kept.sql(dialect="postgres")
    if admitted is not write:
        if write is tree:
            tree = admitted
        else:
            write.replace(admitted)
        write = admitted
    target, _listed = _target(write)  # pyright: ignore[reportArgumentType]
    tid = _table_id_for_node(target, gov_ctx)
    assert tid is not None  # admitted
    returning = write.args.get("returning")
    reads = returning is not None and returning.find(exp.Query) is not None
    if isinstance(write, exp.Update):
        # REQ-039: a column the role is not served cannot be set.
        for assignment in write.expressions:
            if isinstance(assignment, exp.EQ) and isinstance(assignment.this, exp.Column):
                _refuse_hidden(assignment.this, tid, gov_ctx)
    if isinstance(write, (exp.Update, exp.Delete)):
        granted = write.args.get("where")
        if _plain_write(write, role_where):
            where = _govern_plain_write(write, role_where, target, tid, gov_ctx)
        else:
            where = _keyed_write(write, role_where, target, tid, gov_ctx)
            reads = returning is not None
        terms = [w.this for w in (where, granted) if w is not None]
        write.set("where", exp.Where(this=exp.and_(*terms)) if terms else None)
    elif isinstance(write, exp.Insert):
        _govern_conflict(write, target)
    elif isinstance(write, exp.Merge):
        _govern_merge(write, target, tid, gov_ctx)
    if returning is None:
        return tree
    if reads:
        return _read_returned(tree, write, target, tid, gov_ctx)
    _govern_returning(write, gov_ctx)
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

    Returns governed SQL string: the statement read once and written once (:func:`govern`).
    """
    from provisa.observability.stage_trace import trace_stage

    trace_stage("govern.in", sql)
    # REQ-913: the statement as the client (or a compiler stage) wrote it, read once here.
    tree = sqlglot.parse_one(sql, read="postgres")
    governed = govern(tree, gov_ctx, session_vars, params).sql(dialect="postgres")
    trace_stage("govern.out", governed)
    return governed


def govern(
    tree: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    gov_ctx: GovernanceContext,
    session_vars: dict[str, str] | None = None,
    params: list | None = None,
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]  # REQ-039, REQ-913
    """:func:`apply_governance` on a statement already a tree: the governed statement, as a tree.
    ``tree`` is governed in place; the statement returned may be a new root (a ceiling put round
    a LIMIT it cannot lower)."""
    from provisa.compiler.sql_rewrite import bind_schema_qualified_columns
    from provisa.compiler.write_admission import the_write
    from provisa.core.session_terms import expand_list_settings

    flatten_leftmost_join_groups(tree)
    hidden = parenthesised_join_tables(tree, gov_ctx)
    if hidden:
        raise PermissionError(f"[V006] {parenthesised_join_message(hidden[0])}")
    # REQ-039: a statement holds at most one write, at its root or as WITH w AS (<write>
    # RETURNING ...) SELECT ... FROM w; it is a write wherever it stands.
    write = the_write(tree)  # pyright: ignore[reportArgumentType]  # sqlglot stub types parse_one as Expr
    writes = write is not None
    gov_ctx.admitted_write = None
    if write is not None:
        bind_schema_qualified_columns(tree)
        tree = _govern_write(tree, write, gov_ctx, session_vars, params)

    tree = _govern_selects(tree, gov_ctx, answers=not writes)
    # REQ-2188: a variable of several values reads as a list in an IN; the rest are resolved by the
    # pipeline after this stage.
    expand_list_settings(tree, session_vars or {})

    # Apply LIMIT ceiling (REQ-005): most restrictive of the role-level ceiling and
    # any per-table ceiling on a table referenced by the query.
    # A ceiling bounds the rows a READ returns; a write returns none, and what it may write
    # is its admission's to decide.
    bound = None if writes else _effective_ceiling(tree, gov_ctx)
    gov_ctx.row_limit = None
    gov_ctx.list_limit = None
    if bound is not None:
        from provisa.compiler.list_cap import cap_lists

        def columns_of(table: exp.Expr) -> list[str] | None:  # pyright: ignore[reportPrivateImportUsage]
            relation = _relation(table)
            if relation is not None:
                # REQ-039: a governed relation's columns are the ones it serves.
                return [
                    out for out, physical in relation.columns if physical not in relation.hidden
                ]
            if not isinstance(table, exp.Table):
                return None
            tid = _table_id_for_node(table, gov_ctx)
            if tid is None or tid not in gov_ctx.all_columns:
                return None
            return [name for name, _type in gov_ctx.all_columns[tid]]

        # REQ-005 (amended 2026-10-24): and each list the read nests, at the same ceiling.
        if cap_lists(tree, bound.limit, columns_of):
            gov_ctx.list_limit = bound

    if bound is not None:
        from provisa.compiler.row_limit import binds_tree

        if binds_tree(tree, bound.limit):
            gov_ctx.row_limit = bound
        tree = _limit_ceiling(tree, bound.limit)
    elif gov_ctx.sample_size is not None and not writes:
        tree = _limit_ceiling(tree, gov_ctx.sample_size)
    if not writes:
        # Last, once every pass that reads a relation's columns has: a read's governed relations
        # serve only the outputs it reads, so an engine, and a live source asked through it,
        # reads no other column (a path column it does not read is not extracted at all).
        keep_each_read(
            tree,
            [(s, r.alias) for s in tree.find_all(exp.Subquery) if (r := _relation(s)) is not None],
        )
    return tree


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
    """:func:`_limit_ceiling` on a statement given as text (a compiled statement); one already
    within the ceiling is returned byte-identical."""
    tree = sqlglot.parse_one(sql, read="postgres")
    bounded = _limit_ceiling(tree.copy(), ceiling)
    return sql if bounded == tree else bounded.sql(dialect="postgres")


def _limit_ceiling(tree: exp.Expr, ceiling: int) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """Bound a query's row count by ``ceiling`` (AST — inspect the LIMIT node, never regex the
    text), in place; the statement returned is a new root where a subquery is put round it.

    A literal LIMIT above the ceiling is lowered; a missing LIMIT is added. A non-literal LIMIT
    (parameter or expression, whose value is unknown at govern time) is wrapped in a subquery with a
    constant outer LIMIT, so the ceiling still bounds the result — min(inner, ceiling) — and stays
    valid on engines that reject expressions in LIMIT.
    """
    node = tree if isinstance(tree, (exp.Select, exp.Union)) else tree.find(exp.Select)
    if node is None:
        raise ValueError("_limit_ceiling: query has no SELECT/UNION to bound")

    limit = node.args.get("limit")
    if limit is None:
        node.limit(ceiling, copy=False)
        return tree
    val = limit.expression
    if isinstance(val, exp.Literal) and not val.is_string:
        if int(val.name) > ceiling:
            node.limit(ceiling, copy=False)
        return tree  # within the ceiling — left as it is

    sub = exp.Subquery(this=tree, alias=exp.TableAlias(this=exp.to_identifier("_govern_capped")))
    return exp.select("*").from_(sub).limit(ceiling)


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
