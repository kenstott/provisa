# Copyright (c) 2026 Kenneth Stott
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
    DateTime,
    Float,
    ForeignKey,
    Integer,
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
    Column("gql_naming_convention", Text),
    Column("path", Text),
    Column("allowed_domains", JSON, nullable=False, default=list, server_default="[]"),
    Column("description", Text, nullable=False, server_default=""),
    Column("mapping", JSON, nullable=False, default=dict, server_default="{}"),
    Column("cdc", JSON),
)

domains = Table(
    "domains",
    metadata,
    Column("id", Text, primary_key=True),
    Column("description", Text, nullable=False, server_default=""),
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
    Column("governance", Text, nullable=False, server_default="pre-approved"),
    Column("alias", Text),
    Column("description", Text),
    Column("cache_ttl", Integer),
    Column("gql_naming_convention", Text),
    Column("watermark_column", Text),
    Column("column_presets", JSON, nullable=False, default=list, server_default="[]"),
    Column("view_sql", Text),
    Column("data_product", Boolean, nullable=False, server_default=false()),
    Column("materialize", Boolean, nullable=False, server_default=false()),
    Column("mv_refresh_interval", Integer, nullable=False, server_default="300"),
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

roles = Table(
    "roles",
    metadata,
    Column("id", Text, primary_key=True),
    Column("capabilities", JSON, nullable=False, default=list, server_default="[]"),
    Column("domain_access", JSON, nullable=False, default=list, server_default="[]"),
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
    Column("filter_expr", Text, nullable=False),
    Column("tenant_id", Uuid),
    UniqueConstraint("table_id", "role_id"),
    UniqueConstraint("domain_id", "role_id", name="rls_rules_domain_role_key"),
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
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "status IN ('fresh', 'stale', 'refreshing', 'disabled')",
        name="materialized_views_status_check",
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
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("status IN ('success', 'failure')", name="mv_refresh_log_status_check"),
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
    Column("auth", JSON),
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

node_ids = Table(
    "node_ids",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("composite_id", Text, nullable=False, unique=True),
    Column("label", Text, nullable=False),
    Column("properties", JSON, nullable=False, default=dict, server_default="{}"),
)

rel_ids = Table(
    "rel_ids",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("composite_id", Text, nullable=False, unique=True),
    Column("rel_type", Text, nullable=False),
    Column("properties", JSON, nullable=False, default=dict, server_default="{}"),
)

# Append-only SOC2 audit log (per-org). PG enforces immutability via CREATE RULE;
# on Tier-2 backends this is enforced by triggers/app-level (see Phase 3).
query_audit_log = Table(
    "query_audit_log",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("tenant_id", Uuid),
    Column("user_id", Text, nullable=False),
    Column("role_id", Text, nullable=False),
    Column("query_hash", Text, nullable=False),
    Column("table_ids", JSON, nullable=False, default=list, server_default="[]"),
    Column("source", Text, nullable=False),
    Column("status_code", Integer, nullable=False),
    Column("duration_ms", Integer, nullable=False),
    Column("logged_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
