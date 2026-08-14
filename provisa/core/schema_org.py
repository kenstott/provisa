# Copyright (c) 2026 Kenneth Stott
# Canary: 61874ab7-c7ab-492e-bde1-a1396435b684
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLAlchemy Core metadata for the **tenant control plane** (per-org data model).

Its counterpart is the **platform control plane** (global registry) in
``provisa/core/schema_admin.py``.

Mirrors the post-migration shape of ``provisa/core/schema.sql`` (the per-org
tables) plus the audit log from ``provisa/audit/query_log.py``. Portable types
replace PG-specific ones so the same metadata can target PostgreSQL, SQLite
(>=3.35), and MySQL 8:

- ``JSONB``           -> :class:`sqlalchemy.JSON`
- ``TEXT[]``          -> :class:`sqlalchemy.JSON` (list-as-JSON)
- ``SERIAL``          -> :class:`sqlalchemy.Integer` autoincrement PK
- ``BIGSERIAL``       -> :class:`sqlalchemy.BigInteger` autoincrement PK
- ``TIMESTAMPTZ``     -> ``DateTime(timezone=True)``
- ``UUID``            -> :class:`sqlalchemy.Uuid`
- ``DOUBLE PRECISION``-> :class:`sqlalchemy.Float`

Org isolation is handled at runtime via ``schema_translate_map`` (see
``provisa/core/database.py``); tables are defined schema-agnostic here.

Cross-model references to admin-model tables (``orgs``, and ``roles`` from the
admin ``org_invites`` row) are kept as plain columns, not ForeignKeys, because
the two models may live in separate schemas/engines and FKs cannot span them.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    false,
    func,
    true,
)

metadata = MetaData()


sources = Table(
    "sources",
    metadata,
    Column("id", Text, primary_key=True),
    Column("type", Text, nullable=False),
    Column("host", Text, nullable=False, server_default=""),
    Column("port", Integer, nullable=False, server_default="0"),
    Column("database", Text, nullable=False, server_default=""),
    Column("username", Text, nullable=False, server_default=""),
    Column("dialect", Text, nullable=False, server_default=""),
    Column("cache_enabled", Boolean, nullable=False, server_default=true()),
    Column("cache_ttl", Integer),
    Column("prefer_materialized", Boolean, nullable=False, server_default=false()),
    Column("load_protected", Boolean, nullable=False, server_default=false()),  # REQ-1141
    Column("off_peak_window", Text),  # REQ-1141
    Column("off_peak_tz", Text, nullable=False, server_default="UTC"),  # REQ-1141
    Column("gql_naming_convention", Text),
    Column("path", Text),
    Column("allowed_domains", JSON, nullable=False, default=list, server_default="[]"),
    Column("description", Text, nullable=False, server_default=""),
    Column("mapping", JSON, nullable=False, default=dict, server_default="{}"),
    # Connection extras the typed columns cannot carry (warehouse account/http_path, remote-schema
    # override, Exasol server-certificate fingerprint) — Source.federation_hints.
    Column("federation_hints", JSON, nullable=False, default=dict, server_default="{}"),
    Column("cdc", JSON),
    Column("change_signal", Text, nullable=False, server_default="ttl"),  # REQ-929
)

domains = Table(
    "domains",
    metadata,
    Column("id", Text, primary_key=True),
    Column("description", Text, nullable=False, server_default=""),
    Column("steward", Text),  # REQ-609: designated steward; NULL = pending
    Column("graphql_alias", Text),
    Column("org_id", Text),  # cross-model ref -> admin.orgs
    Column("tenant_id", Uuid),
)

naming_rules = Table(
    "naming_rules",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("pattern", Text, nullable=False),
    Column("replacement", Text, nullable=False),
)

registered_tables = Table(
    "registered_tables",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, ForeignKey("sources.id", ondelete="CASCADE"), nullable=False),
    Column("domain_id", Text, ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
    Column("schema_name", Text, nullable=False),
    Column("table_name", Text, nullable=False),
    Column("alias", Text),
    Column("description", Text),
    Column("cache_ttl", Integer),
    Column("prefer_materialized", Boolean),
    Column("load_protected", Boolean),  # REQ-1141: NULL = inherit source
    Column("off_peak_window", Text),  # REQ-1141
    Column("off_peak_tz", Text),  # REQ-1141
    Column("gql_naming_convention", Text),
    Column("watermark_column", Text),
    Column("change_signal", Text),  # REQ-929: override source change signal; NULL = inherit
    Column("probe_query", Text),  # REQ-929: source-native freshness probe
    Column("probe_type", Text),  # REQ-982: input-probe method; NULL = resolve per source class
    Column("column_presets", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-1093: table-level UNIQUE constraints — [{name, columns:[...]}], seeded from source introspection.
    Column("unique_constraints", JSON, nullable=False, default=list, server_default="[]"),
    Column("view_sql", Text),
    # REQ-1443: the data-quality contract a checker table's rows are the scan results of.
    Column("dq_contract", Text),
    # REQ-1318: declarative metric-composed view spec ({metrics, dimensions, filters});
    # NULL for ordinary tables/free-hand views. view_sql holds the generated SELECT.
    Column("view_metrics", JSON),
    Column("data_product", Boolean, nullable=False, server_default=false()),
    Column("materialize", Boolean, nullable=False, server_default=false()),
    Column("mv_refresh_interval", Integer, nullable=False, server_default="300"),
    # REQ-963 live-MV debounce (event-loop path). quiet=0 → real-time recompute.
    Column("mv_debounce_quiet", Float, nullable=False, server_default="0"),
    Column("mv_debounce_max_delay", Float, nullable=False, server_default="5"),
    # REQ-879: MV cross-instance consistency tier — "shared" (fleet-coordinated refresh) or
    # "distributed" (per-instance).
    Column("mv_consistency", Text, nullable=False, server_default="shared"),
    # REQ-957: inline preprocess(rows, ctx) hook source; NULL = identity.
    Column("mv_preprocess", Text),
    # REQ-1162: append-only bitemporal materialization. NULL = ordinary MV; "snapshot"|"delta".
    # mv_bitemporal_key is the business-key column list a version belongs to (required for delta).
    Column("mv_bitemporal_mode", Text),
    Column("mv_bitemporal_key", JSON),
    # REQ-965/969/970: MV persistence outcome + row identity + incremental maintenance.
    Column("mv_persist", Text, nullable=False, server_default="replace"),
    Column("mv_primary_key", JSON),
    Column("mv_incremental", Boolean, nullable=False, server_default=false()),
    # REQ-961/962/1168: PERIODIC calendar trigger (snapshot schedule). mv_calendar names a registered
    # calendar; mv_grain is a nesting grain ("daily".."annual") OR an nth-weekday recurrence
    # ("3WE"/"LFR"). mv_allowed_lateness (s) extends the claim deadline; mv_expected_events is the
    # preflight freshness contract (NULL = all SQL-lineage inputs); mv_business_day_grain gates window
    # existence on business days. All NULL/false = not periodic (the NRT/live default).
    Column("mv_calendar", Text),
    Column("mv_grain", Text),
    Column("mv_allowed_lateness", Float, nullable=False, server_default="0"),
    Column("mv_expected_events", JSON),
    Column("mv_business_day_grain", Boolean, nullable=False, server_default=false()),
    # REQ-1320: star/vault modeling role retained after entity/fact lowering
    # ("dimension" | "fact" | NULL) and the originating entity's history mode.
    Column("modeling_role", Text),
    Column("modeling_history", Text),
    Column("enable_aggregates", Boolean, nullable=False, server_default=false()),
    Column("enable_group_by", Boolean, nullable=False, server_default=false()),
    Column("live", JSON),
    Column("tenant_id", Uuid),
    Column("l1_cluster", Integer),
    Column("l2_cluster", Integer),
    Column("l3_cluster", Integer),
    Column("clusters_computed_at", DateTime(timezone=True)),
    UniqueConstraint("source_id", "schema_name", "table_name"),
)

table_columns = Table(
    "table_columns",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("column_name", Text, nullable=False),
    Column("visible_to", JSON, nullable=False, default=list, server_default="[]"),
    Column("alias", Text),
    Column("description", Text),
    Column("path", Text),
    Column("data_type", Text),
    Column("writable_by", JSON, nullable=False, default=list, server_default="[]"),
    Column("unmasked_to", JSON, nullable=False, default=list, server_default="[]"),
    Column("mask_type", Text),
    Column("mask_pattern", Text),
    Column("mask_replace", Text),
    Column("mask_value", Text),
    Column("mask_precision", Text),
    Column("is_primary_key", Boolean, nullable=False, server_default=false()),
    Column("native_filter_type", Text),
    Column("is_foreign_key", Boolean, nullable=False, server_default=false()),
    Column("is_alternate_key", Boolean, nullable=False, server_default=false()),
    Column("object_fields", JSON, nullable=False, default=list, server_default="[]"),
    Column("scope", Text, nullable=False, server_default="domain"),
    Column("gql_selection", Text),
    Column("tenant_id", Uuid),
    UniqueConstraint("table_id", "column_name"),
    CheckConstraint(
        "mask_type IN ('regex', 'constant', 'truncate')", name="table_columns_mask_type_check"
    ),
)

relationships = Table(
    "relationships",
    metadata,
    Column("id", Text, primary_key=True),
    Column(
        "source_table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("target_table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE")),
    Column("source_column", Text, nullable=False),
    Column("target_column", Text),
    Column("cardinality", Text, nullable=False),
    Column("materialize", Boolean, nullable=False, server_default=false()),
    Column("refresh_interval", Integer, nullable=False, server_default="300"),
    Column("target_function_name", Text),
    Column("function_arg", Text),
    Column("alias", Text),
    Column("graphql_alias", Text),
    Column("disable_cypher", Boolean, nullable=False, server_default=false()),
    Column("hide_target_meta", Boolean, nullable=False, server_default=false()),  # REQ-1132
    Column("source_json_key", Text),
    Column("owner", Text),
    Column("version", Integer, nullable=False, server_default="1"),
    Column("needs_review", Boolean, nullable=False, server_default=false()),
    Column("tenant_id", Uuid),
    UniqueConstraint("source_table_id", "alias", name="relationships_source_alias_unique"),
    CheckConstraint(
        "cardinality IN ('many-to-one', 'one-to-many')", name="relationships_cardinality_check"
    ),
)

# REQ-1317: governed metric definitions — named aggregates with query-time grain.
# from_fact marks a metric auto-registered from a fact spec's measure (REQ-1320).
# V1: schema-defined, no migration.
metrics = Table(
    "metrics",
    metadata,
    Column("name", Text, primary_key=True),
    Column("expression", Text, nullable=False),
    Column("datatype", Text),
    Column("description", Text),
    Column("ai_context", Text),  # REQ-1319: definition text for AI consumers
    Column("visible_to", JSON, nullable=False, default=lambda: ["*"], server_default='["*"]'),
    Column("from_fact", Text),
)

roles = Table(
    "roles",
    metadata,
    Column("id", Text, primary_key=True),
    Column("capabilities", JSON, nullable=False, default=list, server_default="[]"),
    Column("domain_access", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-1174: per-role rate + query-complexity limits {requests_per_second, max_query_depth,
    # max_query_nodes, max_query_time_ms, ...}. None/absent = unlimited.
    Column("rate_limit", JSON),
    Column("parent_role_id", Text, ForeignKey("roles.id")),
    Column("org_id", Text),  # cross-model ref -> admin.orgs
    Column("tenant_id", Uuid),
)

rls_rules = Table(
    "rls_rules",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE")),
    Column("domain_id", Text, ForeignKey("domains.id", ondelete="CASCADE")),
    Column("role_id", Text, ForeignKey("roles.id", ondelete="CASCADE"), nullable=False),
    Column("filter_expr", LargeBinary, nullable=False),  # REQ-686: encrypted at rest (BYTEA)
    Column("tenant_id", Uuid),
    UniqueConstraint("table_id", "role_id"),
    UniqueConstraint("domain_id", "role_id", name="rls_rules_domain_role_key"),
)

# REQ-1373/REQ-1375: org-level tag registry — one registry, per-tag applies_to scopes;
# system tags seeded/immutable, user tags annotation-only. V1: schema-defined, no migration.
tags = Table(
    "tags",
    metadata,
    Column("id", Text, primary_key=True),
    Column("description", Text, nullable=False, server_default=""),
    Column("applies_to", JSON, nullable=False, default=list, server_default="[]"),
    Column("is_system", Boolean, nullable=False, server_default=false()),
    # Per-tag assignment-field policy: hidden | optional | required.
    Column("reason_policy", Text, nullable=False, server_default="optional"),
    Column("expires_policy", Text, nullable=False, server_default="optional"),
    # REQ-1467: whether assignments carry a "{tag}:{value}" parameter.
    Column("param_policy", Text, nullable=False, server_default="none"),
    Column("tenant_id", Uuid),
    CheckConstraint(
        "reason_policy IN ('hidden', 'optional', 'required')",
        name="tags_reason_policy_check",
    ),
    CheckConstraint(
        "expires_policy IN ('hidden', 'optional', 'required')",
        name="tags_expires_policy_check",
    ),
    CheckConstraint(
        "param_policy IN ('none', 'required')",
        name="tags_param_policy_check",
    ),
)

# REQ-1467: the permitted parameter values for a parameterized tag, maintainer-editable.
# Its own table rather than a column on `tags` because the parameterized tag that motivates it
# — `entity` — is a system tag, and system tags are code-defined with no row here (see the
# comment on SYSTEM_TAGS). This keeps that invariant while still giving the mutable half a home.
# tag_id is the BASE id ("entity"), never an assigned "entity:customer".
tag_param_values = Table(
    "tag_param_values",
    metadata,
    Column("tag_id", Text, primary_key=True),
    Column("value", Text, primary_key=True),
    Column("description", Text, nullable=False, server_default=""),
    Column("tenant_id", Uuid),
)

# REQ-1377: tag assignments to sources/tables/columns/relationships. object_key is the
# canonical dedup identity; typed FK columns cascade cleanup with the tagged object.
tag_assignments = Table(
    "tag_assignments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    # No FK: system tags are code-defined (models.SYSTEM_TAGS) with no row to reference.
    Column("tag_id", Text, nullable=False),
    # REQ-1467: tag_id with the parameter stripped ("entity:customer" -> "entity"). Stored, not
    # derived at read time, because the uniqueness rule below is stated over it: one column's
    # values are entity names of ONE type, so entity:customer and entity:employee on the same
    # column is a contradiction, and a UNIQUE over the full tag_id accepts both.
    Column("base_tag_id", Text, nullable=False),
    Column("object_type", Text, nullable=False),
    Column("source_id", Text, ForeignKey("sources.id", ondelete="CASCADE")),
    Column("table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE")),
    Column("column_name", Text),
    Column("relationship_id", Text, ForeignKey("relationships.id", ondelete="CASCADE")),
    Column("command_name", Text),  # tracked function/webhook name; no FK (two registries)
    Column("object_key", Text, nullable=False),
    Column("reason", Text),  # required for 'deprecated' (enforced at the mutation layer)
    Column("expires_on", Text),  # ISO date; planned removal for 'deprecated', typed for reporting
    Column("tenant_id", Uuid),
    UniqueConstraint("base_tag_id", "object_key"),
    CheckConstraint(
        "object_type IN ('source', 'table', 'column', 'relationship', 'command')",
        name="tag_assignments_object_type_check",
    ),
)

# REQ-1385/REQ-1389: vendor-side identity bindings captured at publish time — the catalog's
# own id (Atlas guid, OpenMetadata UUID, Collibra asset UUID, DataHub URN) per published
# asset, keyed by the canonical Provisa URN, so a physical re-address rebinds the SAME
# catalog entity instead of trusting the vendor's name-keyed upsert to find it.
catalog_bindings = Table(
    "catalog_bindings",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("provider", Text, nullable=False),
    Column("semantic_uri", Text, nullable=False),
    Column("vendor_ref", Text, nullable=False),
    Column("physical_key", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("tenant_id", Uuid),
    UniqueConstraint("provider", "semantic_uri"),
)

# REQ-1387: business glossary. Terms are the normalized vocabulary derived from physical
# field names; rooted terms hold refs, abstract terms hold none. A term losing its last
# physical ref is removed unless an abstract term is connected to the rooted graph through
# it, in which case it is deprecated (kept) so no abstract term is left dangling.
glossary_terms = Table(
    "glossary_terms",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("definition", Text),
    Column("is_abstract", Boolean, nullable=False, server_default=false()),
    Column("deprecated", Boolean, nullable=False, server_default=false()),
    Column("export_excluded", Boolean, nullable=False, server_default=false()),
    Column("tenant_id", Uuid),
    UniqueConstraint("name"),
)

# Physical ref = (table_id, column_name): the stable column identity — table_columns.id is
# NOT stable across the table upsert's wholesale column replace. One term per physical field.
glossary_term_refs = Table(
    "glossary_term_refs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("term_id", Integer, ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False),
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    Column("column_name", Text, nullable=False),
    Column("tenant_id", Uuid),
    UniqueConstraint("table_id", "column_name"),
)

# Closed, enum-typed relationship set — free-form edge types are not permitted (REQ-1387).
glossary_term_edges = Table(
    "glossary_term_edges",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "from_term_id", Integer, ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False
    ),
    Column(
        "to_term_id", Integer, ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False
    ),
    Column("rel_type", Text, nullable=False),
    Column("tenant_id", Uuid),
    UniqueConstraint("from_term_id", "to_term_id", "rel_type"),
    CheckConstraint(
        "rel_type IN ('KIND_OF', 'RELATED_TO', 'PART_OF', 'SYNONYM_OF', "
        "'VALID_VALUE_OF', 'DERIVED_FROM', 'REPLACES', 'PREFERRED_TERM_FOR', "
        "'TRANSLATION_OF', 'ANTONYM_OF')",
        name="glossary_term_edges_rel_type_check",
    ),
)

# People who can answer questions about the term or who authored its definition.
glossary_term_experts = Table(
    "glossary_term_experts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("term_id", Integer, ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False),
    Column("user_id", Text, nullable=False),
    Column("kind", Text, nullable=False, server_default="expert"),
    Column("tenant_id", Uuid),
    UniqueConstraint("term_id", "user_id"),
    CheckConstraint("kind IN ('expert', 'author')", name="glossary_term_experts_kind_check"),
)

materialized_views = Table(
    "materialized_views",
    metadata,
    Column("id", Text, primary_key=True),
    Column("source_tables", JSON, nullable=False),
    Column("target_catalog", Text, nullable=False),
    Column("target_schema", Text, nullable=False),
    Column("target_table", Text, nullable=False),
    Column("refresh_interval", Integer, nullable=False, server_default="300"),
    Column("enabled", Boolean, nullable=False, server_default=true()),
    Column("join_pattern", JSON),
    Column("custom_sql", Text),
    Column("expose_in_sdl", Boolean, nullable=False, server_default=false()),
    Column("sdl_config", JSON),
    Column("status", Text, nullable=False, server_default="stale"),
    Column("last_refresh_at", DateTime(timezone=True)),
    Column("row_count", Integer),
    Column("last_error", Text),
    # REQ-879: authoritative SHARED refresh-coordination state for a load-balanced fleet.
    # writer = the instance owning the in-flight refresh; lease_until = when its claim expires
    # (a crashed refresher's lease times out so the MV is reclaimable). The version stamps are
    # the REQ-862 dedup key: a claim skips when materialized_input_version already == target.
    Column("writer", Text),
    Column("lease_until", DateTime(timezone=True)),
    Column("materialized_definition_version", Text),
    Column("materialized_input_version", Text),
    Column("snapshot_id", Text),
    # REQ-961/962: temporal-processing declaration. calendar/grain name the shared, versioned
    # boundary source that yields [start,end) windows; allowed_lateness (seconds) extends the claim
    # deadline past window.end; expected_events is the freshness contract (inputs that must be
    # fresh-through window.end; NULL = default to all SQL-lineage inputs); business_day_grain gates
    # window existence on the calendar's business days. All NULL/false = a non-temporal MV.
    Column("calendar", Text),
    Column("grain", Text),
    Column("allowed_lateness", Integer, nullable=False, server_default="0"),
    Column("expected_events", JSON),
    Column("business_day_grain", Boolean, nullable=False, server_default=false()),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "status IN ('fresh', 'stale', 'refreshing', 'disabled')",
        name="materialized_views_status_check",
    ),
)

# REQ-962: named, shared, VERSIONED calendars — the temporal-window boundary source. An MV declares
# (calendar, grain) and the calendar deterministically yields [start,end) windows. The holiday/
# business-day set is captured per version so a replay reproduces the same window existence. V1:
# schema-defined, no migration.
calendars = Table(
    "calendars",
    metadata,
    Column("name", Text, primary_key=True),
    Column("version", Text, primary_key=True),
    # gregorian | fiscal | retail_445
    Column("base_system", Text, nullable=False, server_default="gregorian"),
    Column("tz", Text, nullable=False, server_default="UTC"),  # IANA zone (DST-aware boundaries)
    Column("fiscal_anchor_month", Integer, nullable=False, server_default="1"),
    Column("fiscal_anchor_day", Integer, nullable=False, server_default="1"),
    Column("retail_anchor", Date),  # retail_445: the reference retail-year start date
    Column("week_start", Integer, nullable=False, server_default="0"),  # 0 = Monday
    Column("holidays", JSON, default=list, server_default="[]"),  # ISO dates, versioned/immutable
    Column("weekend", JSON, default=list, server_default="[5, 6]"),  # weekday ints (Sat, Sun)
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "base_system IN ('gregorian', 'fiscal', 'retail_445')",
        name="calendars_base_system_check",
    ),
)

mv_refresh_log = Table(
    "mv_refresh_log",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mv_id", Text, ForeignKey("materialized_views.id", ondelete="CASCADE"), nullable=False),
    Column("status", Text, nullable=False),
    Column("row_count", Integer),
    Column("duration_ms", Integer),
    Column("error", Text),
    # Column-level lineage / provenance stamps (REQ-862) — store-independent.
    Column("definition_version", Text),
    Column("input_version", Text),
    Column("input_version_kind", Text),
    Column("trace_id", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("status IN ('success', 'failure')", name="mv_refresh_log_status_check"),
)

# REQ-877: opt-in per-MV ROW-LEVEL delta ledger — the row-level companion to the column-level
# lineage (REQ-862). Append-only; keyed by (mv_id, refresh_version) and never stamped on the target
# table, so it is store-independent (works for an Iceberg or RDB target). Each refresh that changed
# rows appends one event per changed key: change_type (insert/update/delete), the key (row_key JSON
# text), the row hashes (old/new — change detection over the hash-excluded projection), and the row
# VALUES (old/new — the value-delta tier that enables full-content point-in-time reconstruction,
# REQ-878). A delete carries old_values only; an insert new_values only.
mv_delta_ledger = Table(
    "mv_delta_ledger",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("mv_id", Text, ForeignKey("materialized_views.id", ondelete="CASCADE"), nullable=False),
    Column("refresh_version", Integer, nullable=False),
    Column("definition_version", Text),
    Column("trace_id", Text),
    Column("change_type", Text, nullable=False),
    Column("row_key", Text, nullable=False),
    Column("old_hash", Text),
    Column("new_hash", Text),
    Column("old_values", JSON),
    Column("new_values", JSON),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "change_type IN ('insert', 'update', 'delete')",
        name="mv_delta_ledger_change_type_check",
    ),
)

relationship_candidates = Table(
    "relationship_candidates",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_table_id", Integer, ForeignKey("registered_tables.id"), nullable=False),
    Column("target_table_id", Integer, ForeignKey("registered_tables.id"), nullable=False),
    Column("source_column", Text, nullable=False),
    Column("target_column", Text, nullable=False),
    Column("cardinality", Text, nullable=False),
    Column("confidence", Float, nullable=False),
    Column("reasoning", Text),
    Column("suggested_name", Text),
    Column("status", Text, nullable=False, server_default="suggested"),
    Column("scope", Text, nullable=False),
    Column("rejection_reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("source_table_id", "source_column", "target_table_id", "target_column"),
    CheckConstraint(
        "status IN ('suggested', 'accepted', 'rejected', 'expired')",
        name="relationship_candidates_status_check",
    ),
)

kafka_sources = Table(
    "kafka_sources",
    metadata,
    Column("id", Text, primary_key=True),
    Column("bootstrap_servers", Text, nullable=False),
    Column("schema_registry_url", Text),
    Column("auth_type", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

kafka_topics = Table(
    "kafka_topics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, ForeignKey("kafka_sources.id", ondelete="CASCADE"), nullable=False),
    Column("topic", Text, nullable=False),
    Column("table_name", Text, nullable=False, unique=True),
    Column("schema_source", Text, nullable=False, server_default="registry"),
    Column("value_format", Text, nullable=False, server_default="json"),
    Column("columns", JSON, nullable=False, default=list, server_default="[]"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("source_id", "topic"),
    CheckConstraint(
        "schema_source IN ('registry', 'manual', 'sample')", name="kafka_topics_schema_source_check"
    ),
    CheckConstraint(
        "value_format IN ('json', 'avro', 'protobuf')", name="kafka_topics_value_format_check"
    ),
)

kafka_sinks = Table(
    "kafka_sinks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("query_stable_id", Text, nullable=False, unique=True),
    Column("topic", Text, nullable=False),
    Column("key_column", Text),
    Column("value_format", Text, nullable=False, server_default="json"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

api_sources = Table(
    "api_sources",
    metadata,
    Column("id", Text, primary_key=True),
    Column("type", Text, nullable=False),
    Column("base_url", Text, nullable=False),
    Column("spec_url", Text),
    # REQ-686: API auth config (keys/tokens) encrypted at rest, decrypted before use.
    Column("auth", LargeBinary),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "type IN ('openapi', 'graphql_api', 'grpc_api')", name="api_sources_type_check"
    ),
)

api_endpoints = Table(
    "api_endpoints",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, ForeignKey("api_sources.id", ondelete="CASCADE"), nullable=False),
    Column("path", Text, nullable=False),
    Column("method", Text, nullable=False, server_default="GET"),
    Column("table_name", Text, nullable=False, unique=True),
    Column("columns", JSON, nullable=False),
    Column("ttl", Integer, nullable=False, server_default="300"),
    Column("response_root", Text),
    Column("error_path", Text),
    Column("pk_column", Text),
    Column("pagination", JSON),
    Column("max_concurrency", Integer),
    Column("default_params", JSON),
    Column("promotions", JSON, nullable=False, default=list, server_default="[]"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

creation_requests = Table(
    "creation_requests",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("request_type", Text, nullable=False),
    Column("capability", Text, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("requested_by", Text),
    Column("status", Text, nullable=False, server_default="pending"),
    Column("rejection_reason", Text),
    Column("resolved_by", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("resolved_at", DateTime(timezone=True)),
    Column("approvals", JSON, nullable=False, default=list, server_default="[]"),
    Column("required_approvals", Integer, nullable=False, server_default="1"),
    CheckConstraint(
        "status IN ('pending', 'executed', 'rejected')", name="creation_requests_status_check"
    ),
)

api_endpoint_candidates = Table(
    "api_endpoint_candidates",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, ForeignKey("api_sources.id", ondelete="CASCADE"), nullable=False),
    Column("path", Text, nullable=False),
    Column("method", Text, nullable=False, server_default="GET"),
    Column("table_name", Text),
    Column("columns", JSON, nullable=False),
    Column("status", Text, nullable=False, server_default="discovered"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("source_id", "path", "method"),
    CheckConstraint(
        "status IN ('discovered', 'registered', 'rejected')",
        name="api_endpoint_candidates_status_check",
    ),
)

live_query_state = Table(
    "live_query_state",
    metadata,
    Column("source", Text, nullable=False),
    Column("output_type", Text, nullable=False),
    Column("last_watermark", Text),
    Column("last_polled_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("status", Text, nullable=False, server_default="active"),
    PrimaryKeyConstraint("source", "output_type"),
)

tracked_functions = Table(
    "tracked_functions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False, unique=True),
    Column("source_id", Text, nullable=False, server_default=""),
    Column("schema_name", Text, nullable=False, server_default="public"),
    Column("function_name", Text, nullable=False, server_default=""),
    Column("returns", Text, nullable=False, server_default=""),
    Column("arguments", JSON, nullable=False, default=list, server_default="[]"),
    Column("visible_to", JSON, nullable=False, default=list, server_default="[]"),
    Column("writable_by", JSON, nullable=False, default=list, server_default="[]"),
    Column("domain_id", Text, nullable=False, server_default=""),
    Column("description", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("kind", Text, nullable=False, server_default="mutation"),
    Column("return_schema", JSON),
    # REQ-1159: canonical IR-typed output dataset contract [{name,type}]; return_schema projects it to GraphQL.
    Column("output_columns", JSON),
    # REQ-885: implementation kind + swappable binding, decoupled from addressing.
    Column("impl_kind", Text, nullable=False, server_default="source_procedure"),
    Column("binding", JSON, nullable=False, default=dict, server_default="{}"),
    Column("materialize", Boolean, nullable=False, server_default=false()),
)

tracked_webhooks = Table(
    "tracked_webhooks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False, unique=True),
    Column("url", Text, nullable=False, server_default=""),
    Column("method", Text, nullable=False, server_default="POST"),
    Column("timeout_ms", Integer, nullable=False, server_default="5000"),
    Column("returns", Text),
    Column("inline_return_type", JSON, nullable=False, default=list, server_default="[]"),
    Column("arguments", JSON, nullable=False, default=list, server_default="[]"),
    Column("visible_to", JSON, nullable=False, default=list, server_default="[]"),
    Column("domain_id", Text, nullable=False, server_default=""),
    Column("description", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("kind", Text, nullable=False, server_default="mutation"),
)

table_meta_links = Table(
    "table_meta_links",
    metadata,
    Column(
        "source_table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "target_table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        nullable=False,
    ),
)

file_source_mtimes = Table(
    "file_source_mtimes",
    metadata,
    Column(
        "table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("source_mtime", Float, nullable=False),
    Column("synced_at", Float, nullable=False),
)

# REQ-1349: per-org overrides layered over the deployment config file. One row per top-level
# config key the org owns (ai_models, vector_models, nl); see provisa/core/org_settings.py.
org_settings = Table(
    "org_settings",
    metadata,
    Column("key", Text, primary_key=True),
    Column("value", JSON, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_by", Text),
)

# REQ-1395: per-org secrets (e.g. Anthropic API key), encrypted at rest via
# provisa.encryption.runtime.encryption_service() — same pattern as api_sources.auth.
# Kept out of org_settings because that table's `value` column is unencrypted JSON.
org_secrets = Table(
    "org_secrets",
    metadata,
    Column("key", Text, primary_key=True),
    Column("value_enc", LargeBinary, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_by", Text),
)

user_role_assignments = Table(
    "user_role_assignments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Text, nullable=False),
    Column("role_id", Text, ForeignKey("roles.id", ondelete="CASCADE"), nullable=False),
    Column("domain_id", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("user_id", "role_id", "domain_id"),
)

# REQ-1439: tenant-local mirror of the platform user directory, so an ops report can put a
# person's name next to the IdP subject it logs. See the matching block in schema.sql.
user_directory = Table(
    "user_directory",
    metadata,
    Column("user_id", Text, primary_key=True),
    Column("display_name", Text),
    Column("email", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

node_ids = Table(
    "node_ids",
    metadata,
    Column(
        "id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    ),
    Column("composite_id", Text, nullable=False, unique=True),
    Column("label", Text, nullable=False),
    Column("properties", JSON, nullable=False, default=dict, server_default="{}"),
)

rel_ids = Table(
    "rel_ids",
    metadata,
    Column(
        "id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    ),
    Column("composite_id", Text, nullable=False, unique=True),
    Column("rel_type", Text, nullable=False),
    Column("properties", JSON, nullable=False, default=dict, server_default="{}"),
)

# Append-only SOC2 audit log (per-org). PG enforces immutability via CREATE RULE;
# on Tier-2 backends this is enforced by triggers/app-level (see Phase 3).
query_audit_log = Table(
    "query_audit_log",
    metadata,
    Column(
        "id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    ),
    # The tenant is the ORG, and an org id is text ("default", "kstott") — not a UUID. Typing this
    # column UUID meant the only value the audit pipeline could ever store was NULL.
    Column("tenant_id", Text),
    Column("user_id", Text, nullable=False),
    Column("role_id", Text, nullable=False),
    Column("query_hash", Text, nullable=False),
    # Encrypted query text (REQ-689) — decrypted only on authorised admin reads.
    Column("query_text_enc", LargeBinary),
    Column("table_ids", JSON, nullable=False, default=list, server_default="[]"),
    Column("source", Text, nullable=False),
    Column("status_code", Integer, nullable=False),
    Column("duration_ms", Integer, nullable=False),
    # REQ-886: correlation id of the UDF invocation this row was written under, joining the
    # audit row back to the engine-side UDF trace. Null for non-UDF queries.
    Column("trace_id", Text),
    Column("logged_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# SLA telemetry (REQ-074, REQ-506) — DDL in provisa/audit/sla_monitor.py.
query_sla_log = Table(
    "query_sla_log",
    metadata,
    Column(
        "id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    ),
    Column("tenant_id", Uuid),
    Column("duration_ms", Integer, nullable=False),
    Column("status_code", Integer, nullable=False),
    Column("window_start", DateTime(timezone=True), nullable=False),
    Column("recorded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Source catalog cache (REQ-464) — DDL in provisa/discovery/catalog_cache.py.
source_catalog_cache = Table(
    "source_catalog_cache",
    metadata,
    Column("source_id", Text, nullable=False),
    Column("schema_name", Text, nullable=False),
    Column("table_name", Text, nullable=False),
    Column("column_names", JSON, nullable=False, default=list, server_default="[]"),
    Column("comment", Text),
    Column("indexed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("source_id", "schema_name", "table_name"),
)


# --- Event substrate (REQ-940/941): the control-plane event table as a transactional outbox. ---
# The fleet-shared change-event queue. Injectors POST events here (in the same tx as the state
# change → atomic); table processors CLAIM their work via event_status; repeaters fanout-read events
# by id cursor. Two tables: `events` (posted once) + `event_status` (one row per event × dependent
# table = the fanout work item, exactly-once claim/lease). V1: schema-defined, no migration.

events = Table(
    "events",
    metadata,
    # id is the ordering key AND the repeater replay cursor.
    Column(
        "id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    ),
    # the node/table this event is ABOUT (a data source table or an MV).
    Column("source_table", Text, nullable=False),
    # delta (upsert by PK) | append (insert) | replace (delete+insert) | warn (advise) | error (halt)
    Column("event_type", Text, nullable=False),
    # cursor / changed rows (bounded to jsonb; over the ceiling degrade to replace) / warn|error detail.
    Column("payload", JSON, default=dict, server_default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "event_type IN ('delta','append','replace','warn','error','quarantine')",
        name="events_event_type_check",
    ),
)

# Per-node freshness state (REQ-981/982): the content hash of the last land (output gate) and the last
# probe token (input probe baseline). One row per node; upserted on each successful land. V1:
# schema-defined, no migration.
node_freshness_state = Table(
    "node_freshness_state",
    metadata,
    Column("node", Text, primary_key=True),  # the source-table / MV node key
    Column("content_hash", Text),  # REQ-981: hash of the last landed replace-shaped content
    Column("probe_token", Text),  # REQ-982: last probe token (watermark/hash/count baseline)
    # REQ-961: the observed per-node refresh state the freshness CONTRACT pulls at fire time. A
    # periodic MV's expected input is "fresh-through window.end" iff its last refresh SUCCEEDED
    # (last_refresh_ok) AND covered the boundary (last_refresh_at >= window.end). NULL last_refresh_at
    # = the node never refreshed → itself an outage (never assume fresh).
    Column("last_refresh_at", DateTime(timezone=True)),
    Column("last_refresh_ok", Boolean),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

event_status = Table(
    "event_status",
    metadata,
    # the fanout work item: one row per (event, dependent node) — dispatched from the SQLGlot lineage.
    Column("event_id", BigInteger, ForeignKey("events.id", ondelete="CASCADE"), nullable=False),
    Column("dependent_table", Text, nullable=False),
    # unclaimed → claimed (heartbeat-leased) → completed; a stale heartbeat reverts to unclaimed.
    Column("claim_status", Text, nullable=False, server_default="unclaimed"),
    Column("processor_name", Text),  # the lease owner while claimed; the REQ-959 ownership-CAS key
    Column("heartbeat_at", DateTime(timezone=True)),  # lease; stale → reaper reclaims
    # REQ-959: per-claim fire-by deadline. reclaimable = (heartbeat lapsed) OR (deadline+grace passed
    # AND not completed) — the second catches a stuck-but-alive owner a heartbeat cannot. NULL = no
    # deadline (reclaim on heartbeat lapse only).
    Column("deadline", DateTime(timezone=True)),
    Column("completed_at", DateTime(timezone=True)),
    PrimaryKeyConstraint("event_id", "dependent_table"),
    CheckConstraint(
        "claim_status IN ('unclaimed','claimed','completed')",
        name="event_status_claim_status_check",
    ),
)

# REQ-983 preserved snapshots: a point-in-time dataset MATERIALIZED-AND-SEALED because it is NOT
# reconstructible from current state + retained event history — the one PIT form that genuinely
# departs from the NRT ideal (contrast the reconstructible PIT of REQ-958/965/967). DECLARED (never
# inferred) and MUST be why-tagged (``reason`` — why the data cannot be reconstructed). One row per
# sealed snapshot; the row IS the immutability record — a second seal of the same name fails loud.
# V1: schema-defined, no migration.
preserved_snapshots = Table(
    "preserved_snapshots",
    metadata,
    Column("name", Text, primary_key=True),  # the declared snapshot identity
    Column(
        "reason", Text, nullable=False
    ),  # REQ-983 mandatory why-tag (non-reconstructible reason)
    Column("location", Text, nullable=False),  # the sealed store-table location
    Column("content_hash", Text, nullable=False),  # the frozen content digest (immutability proof)
    Column("window_id", Text),  # optional calendar-addressable period this snapshot froze
    Column("sealed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-1303/REQ-1308: the org's administrative trail — grants, revocations, offboardings and role
# changes performed against THIS org, including a platform_admin's intervention. Distinct from
# query_audit_log, which records data access. Mirrors the schema.sql DDL of the same name.
admin_audit_log = Table(
    "admin_audit_log",
    metadata,
    Column(
        "id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    ),
    Column("action", Text, nullable=False),
    Column("actor_id", Text, nullable=False),
    Column("subject_id", Text, nullable=False),
    Column("detail", JSON, nullable=False, default=dict, server_default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
