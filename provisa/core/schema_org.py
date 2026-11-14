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

import functools

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

from provisa.core.pgvector import Vector

metadata = MetaData()


# REQ-1921/1922: the stores an org keeps its data in, per region. A store is a connection URL (a
# secret reference); an org region names, for each kind of data, the store it is kept in there.
def _retirement() -> list[Column]:
    """REQ-2248: a model object deleted is retired, not removed -- its row, its name and its file
    are kept, out of service, with why, who and when, in the shape a glossary term's retirement
    takes (``retired``, ``status_reason``, ``status_by``, ``status_at``)."""
    return [
        Column("retired", Boolean, nullable=False, server_default=false()),
        Column("status_reason", Text),
        Column("status_by", Text),
        Column("status_at", DateTime(timezone=True)),
    ]


stores = Table(
    "stores",
    metadata,
    Column("id", Text, primary_key=True),
    Column("url", Text, nullable=False),
    # REQ-1922: the engine kind (an engine-builder key) of a store a region names as its engine;
    # a URL does not identify one (fabric and synapse are both mssql, trino and trino-byo one
    # scheme). None on a store no region uses as its engine.
    Column("kind", Text),
    # REQ-1491: whether this environment has supplied the store's URL (a copy carries the row,
    # never the binding).
    Column(
        "binding", Text, nullable=False, server_default="own"
    ),  # REQ-1942: own | copied | unbound
    CheckConstraint("binding IN ('own', 'copied', 'unbound')", name="stores_binding_check"),
)

org_regions = Table(
    "org_regions",
    metadata,
    Column("id", Text, primary_key=True),  # a platform region this org selects
    Column("engine", Text, nullable=False),
    Column("replicas", Text, nullable=False),
    Column("views", Text, nullable=False),
    Column("cache", Text, nullable=False),
    Column("state", Text, nullable=False),
    Column("record", Text, nullable=False),
)

# REQ-1919: a configuration file seeds the model store once. The row is written when the seed is
# applied; while it exists a restart, redeploy or reload never applies the file again.
model_seed = Table(
    "model_seed",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("seeded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("id = 1", name="model_seed_id_check"),
)

# The layout this schema was given (db.layout_digest): written once schema.sql and the audit schema
# have both run, and read before either runs again.
schema_layout = Table(
    "schema_layout",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("digest", Text, nullable=False),
    CheckConstraint("id = 1", name="schema_layout_id_check"),
)

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
    Column("replicate", Integer),  # REQ-826: NULL = global threshold; -1 never, N hot, 0 always
    Column("region", Text),  # REQ-1921: the admin form's default region for its new tables
    Column("load_protected", Boolean, nullable=False, server_default=false()),  # REQ-1141
    Column("off_peak_window", Text),  # REQ-1141
    Column("off_peak_tz", Text, nullable=False, server_default="UTC"),  # REQ-1141
    Column("max_live_concurrency", Integer),  # REQ-1909: NULL = no cap
    Column("sentinel_path", Text),  # REQ-1148
    Column("download_from", Text),  # REQ-2113
    Column("freshness_gate", Boolean, nullable=False, server_default=false()),  # REQ-860
    Column("gql_naming_convention", Text),
    Column("path", Text),
    Column("allowed_domains", JSON, nullable=False, default=list, server_default="[]"),
    Column("description", Text, nullable=False, server_default=""),
    Column("mapping", JSON, nullable=False, default=dict, server_default="{}"),
    # Connection extras the typed columns cannot carry (warehouse account/http_path, remote-schema
    # override, Exasol server-certificate fingerprint) — Source.federation_hints.
    Column("federation_hints", JSON, nullable=False, default=dict, server_default="{}"),
    # REQ-1923: the values a branded source's setup was given (Shopify: store, api_version) --
    # Source.brand_setup.
    Column("brand_setup", JSON, nullable=False, default=dict, server_default="{}"),
    Column("cdc", JSON(none_as_null=True)),
    Column("change_signal", Text, nullable=False, server_default="ttl"),  # REQ-929
    # REQ-1491: whether this environment has supplied this source's connection values. A copy
    # carries the row and never the binding, and an empty host is not an absent one — the
    # connection builder reads it as localhost — so an unbound source is MARKED, and the query
    # path refuses a query naming it rather than dialling whatever is local to the node.
    Column(
        "binding", Text, nullable=False, server_default="own"
    ),  # REQ-1942: own | copied | unbound | synthetic
    CheckConstraint(
        "binding IN ('own', 'copied', 'unbound', 'synthetic')", name="sources_binding_check"
    ),
    # REQ-1695: the source's password as a SECRET REFERENCE, never a credential. A literal typed
    # into the Sources form is put into the org vault (provisa.core.secrets_store) and what lands
    # here is the ``${secret:NAME}`` that names it; a reference the operator typed themselves is
    # stored verbatim. Empty means the source needs no password. Resolution happens where every
    # other connection secret's does — at the use point, inside the bound org.
    Column("password_ref", Text, nullable=False, server_default=""),
    # REQ-1942: set while the row is bound to the environment's synthetic store (binding
    # ``synthetic``): {schema, tables, parameters, model_type} -- the store schema holding its
    # generated tables, the registered tables generated there as [schema_name, table_name], the
    # required parameters generated as ordinary columns ("schema.table" -> {column: type}), and
    # the type the model gives the source, which ``type`` gives up to the store's while bound so.
    # The store itself is resolved by the platform: no credential of it is written here.
    Column("synthetic", JSON(none_as_null=True)),
    # REQ-1919: the rest of a source's settings, held here so the store alone owns the model.
    Column("base_url", Text),
    Column("pool_min", Integer, nullable=False, server_default="1"),
    Column("pool_max", Integer, nullable=False, server_default="5"),
    Column("use_pgbouncer", Boolean, nullable=False, server_default=false()),
    Column("pgbouncer_port", Integer, nullable=False, server_default="6432"),
    Column("producer_command", JSON(none_as_null=True)),
    Column("cache_catalog", Text),
    Column("cache_schema", Text, nullable=False, server_default="api_cache"),
    Column("approval_hook", Boolean, nullable=False, server_default=false()),
    Column("is_system", Boolean, nullable=False, server_default=false()),
)

domains = Table(
    "domains",
    metadata,
    Column("id", Text, primary_key=True),
    Column("description", Text, nullable=False, server_default=""),
    Column("steward", Text),  # REQ-609: designated steward; NULL = pending
    Column("graphql_alias", Text),
    # REQ-1363: the domain this one sits under (a business unit and its organizations); NULL =
    # no parent. No foreign key: a load or a deploy writes the whole tree at once, in no order
    # a self-reference could rely on. What holds it is the model store — a parent with children
    # is refused deletion (repositories.integrity), and an unknown parent or a cycle is refused
    # by name wherever a tree is written (security.domain_tree).
    Column("parent_domain_id", Text),
    Column("org_id", Text),  # cross-model ref -> admin.orgs
    Column("tenant_id", Uuid),
)

data_products = Table(  # REQ-1634
    "data_products",
    metadata,
    Column("id", Text, primary_key=True),
    Column("domain_id", Text, ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
    Column("name", Text, nullable=False),
    Column("owner_role", Text),
    Column("team_role", Text),
    Column("purpose", Text, nullable=False, server_default=""),
    Column("limitations", Text, nullable=False, server_default=""),
    Column("usage", Text, nullable=False, server_default=""),
    Column("version", Text),
    # REQ-1634 STATUS: draft, live, deprecated or retired, each asserted by an action; why, the
    # planned retirement of a deprecated product, who changed it when, and the product a retired
    # one's members moved to.
    Column("status", Text, nullable=False, server_default="draft"),
    Column("status_reason", Text),
    Column("retire_on", Date),
    Column("status_by", Text),
    Column("status_at", DateTime(timezone=True)),
    Column("successor_id", Text, ForeignKey("data_products.id", ondelete="SET NULL")),
    Column("support", Text),
    Column("support_contact", Text),  # REQ-1635: Horizon Catalog listing manifest requirement
    Column("custom_properties", JSON, nullable=False, default=dict, server_default="{}"),
    # REQ-2118: when the product was created, its ODPS productCreatedTs; set on insert, never
    # written by a save.
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # REQ-2219: who created it, set on insert only; it grants nothing.
    Column("created_by", Text),
    # REQ-2180: the freshness every contract of the product promises, in seconds.
    Column("freshness", Integer),
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
    # REQ-1907: role -> TTL seconds; effective TTL = max(cache_ttl, role_ttl(role)).
    Column("role_ttl", JSON, nullable=False, default=dict, server_default="{}"),
    Column("replicate", Integer),  # REQ-826: NULL = inherit source
    Column("region", Text),  # REQ-1921: the org region its data lives in; NULL = none
    # REQ-1921: out of service while set — read and written nowhere, offered in no schema, copied
    # nowhere; a table or view registered through the admin starts so.
    Column("draft", Boolean, nullable=False, default=False, server_default=false()),
    Column("load_protected", Boolean),  # REQ-1141: NULL = inherit source
    Column("off_peak_window", Text),  # REQ-1141
    Column("off_peak_tz", Text),  # REQ-1141
    # REQ-318: how the table is read page by page (provisa.core.paging); NULL = not paged here.
    # none_as_null: an unpaged table (None) is SQL NULL, never the JSON value null.
    Column("pagination", JSON(none_as_null=True)),
    # REQ-2061: the values of the parameters the table's source takes by name, fixed for every
    # read (a GraphQL root field's arguments, an OpenAPI operation's parameters); NULL = none.
    Column("arguments", JSON(none_as_null=True)),
    # REQ-2062: a SQL predicate over the table's own columns, applied when its replica lands.
    Column("landing_filter", Text),
    # REQ-2224: what a read does with nothing landed (provisa.core.models.Table.read_wait).
    Column("read_wait", Integer),
    Column("gql_naming_convention", Text),
    Column("watermark_column", Text),
    Column("change_signal", Text),  # REQ-929: override source change signal; NULL = inherit
    Column("probe_query", Text),  # REQ-929: source-native freshness probe
    Column("probe_type", Text),  # REQ-982: input-probe method; NULL = resolve per source class
    # REQ-1093: table-level UNIQUE constraints — [{name, columns:[...]}], seeded from source introspection.
    Column("unique_constraints", JSON, nullable=False, default=list, server_default="[]"),
    Column("view_sql", Text),
    # REQ-1443: the data-quality contract a checker table's rows are the scan results of.
    Column("dq_contract", Text),
    # REQ-1443 clause 10: the contract's checks, parsed at registration; none_as_null: a table
    # with no contract stores SQL NULL, which the dq_rules view skips.
    Column("dq_rules", JSON(none_as_null=True)),
    # REQ-1934: the Data Profiler source this table is a member of; NULL for none.
    Column("profiler_source_id", Text),
    # REQ-1318: declarative metric-composed view spec ({metrics, dimensions, filters});
    # NULL for ordinary tables/free-hand views. view_sql holds the generated SELECT.
    Column("view_metrics", JSON(none_as_null=True)),
    # REQ-2130: the glossary term this table's rows are instances of; one per table (schema.sql).
    Column("class_term_id", Integer, ForeignKey("glossary_terms.id", ondelete="SET NULL")),
    # REQ-2141: derived by sync, not declared by config or grounded by a person (schema.sql).
    Column("class_term_derived", Boolean, nullable=False, server_default=false()),
    Column("materialize", Boolean, nullable=False, server_default=false()),
    Column("row_materialize", Boolean, nullable=False, server_default=false()),  # REQ-1865
    Column(
        "file_glob", Text
    ),  # REQ-788: one logical table over a glob of files; NULL = single file
    Column("source_file_column", Text),  # REQ-788: opt-in column carrying each row's file path
    # REQ-1981: the table's format where its source reports one (an Athena table: "iceberg" or
    # "hive"), recorded when it is registered; it decides the writes the table takes.
    Column("table_format", Text),
    # REQ-2230: the statement that defines the table in its source (a Snowflake Dynamic Table's),
    # recorded when it is registered; what it reads joins the lineage graph.
    Column("source_definition", Text),
    # REQ-1954: a ServiceNow table's import set: the staging table of the transform map its
    # inserts go through. NULL = its inserts are written to the table directly.
    Column("import_set", Text),
    Column(
        "delta", JSON(none_as_null=True)
    ),  # REQ-874: incremental-reload (delta) declaration; NULL = whole rebuild only
    Column("mv_refresh_interval", Integer, nullable=False, server_default="300"),
    # REQ-963 live-MV debounce (event-loop path). quiet=0 → real-time recompute.
    Column("mv_debounce_quiet", Float, nullable=False, server_default="0"),
    Column("mv_debounce_max_delay", Float, nullable=False, server_default="5"),
    # REQ-1733: push-source landing debounce (kafka/websocket CDC — consume_cdc_into_store), the
    # SAME quiet+max_delay shape as mv_debounce_* above but for the CDC streaming path, not the
    # poll/MV tick loop — they are independent knobs on independent mechanisms. quiet=0 → land
    # every message as it arrives (no batching).
    Column("push_debounce_quiet", Float, nullable=False, server_default="0"),
    Column("push_debounce_max_delay", Float, nullable=False, server_default="5"),
    # REQ-879: MV cross-instance consistency tier — "shared" (fleet-coordinated refresh) or
    # "distributed" (per-instance).
    Column("mv_consistency", Text, nullable=False, server_default="shared"),
    # REQ-957: inline preprocess(rows, ctx) hook source; NULL = identity.
    Column("mv_preprocess", Text),
    # REQ-2133: the view's Hold on failing checks option, a built-in preflight beside mv_preprocess.
    Column("mv_hold_on_failing_checks", Boolean, nullable=False, server_default=false()),
    # REQ-2109: the bitemporal declaration of a table or view, all three or none: the business-key
    # column list, the valid-time column and the recorded-time column.
    Column("business_key", JSON(none_as_null=True)),
    Column("valid_time", Text),
    Column("recorded_time", Text),
    # REQ-965/969/970: MV persistence outcome + row identity + incremental maintenance.
    Column("mv_persist", Text, nullable=False, server_default="replace"),
    Column("mv_primary_key", JSON(none_as_null=True)),
    Column("mv_incremental", Boolean, nullable=False, server_default=false()),
    # REQ-961/962/1168: PERIODIC calendar trigger (snapshot schedule). mv_calendar names a registered
    # calendar; mv_grain is a nesting grain ("daily".."annual") OR an nth-weekday recurrence
    # ("3WE"/"LFR"). mv_allowed_lateness (s) extends the claim deadline; mv_expected_events is the
    # preflight freshness contract (NULL = all SQL-lineage inputs); mv_business_day_grain gates window
    # existence on business days. All NULL/false = not periodic (the NRT/live default).
    Column("mv_calendar", Text),
    Column("mv_grain", Text),
    Column("mv_allowed_lateness", Float, nullable=False, server_default="0"),
    Column("mv_expected_events", JSON(none_as_null=True)),
    Column("mv_business_day_grain", Boolean, nullable=False, server_default=false()),
    # REQ-1320: star/vault modeling role retained after entity/fact lowering
    # ("dimension" | "fact" | NULL) and the originating entity's history mode.
    Column("modeling_role", Text),
    Column("modeling_history", Text),
    Column("enable_aggregates", Boolean, nullable=False, server_default=false()),
    Column("enable_group_by", Boolean, nullable=False, server_default=false()),
    Column("live", JSON(none_as_null=True)),
    Column("tenant_id", Uuid),
    Column("l1_cluster", Integer),
    Column("l2_cluster", Integer),
    Column("l3_cluster", Integer),
    Column("clusters_computed_at", DateTime(timezone=True)),
    # REQ-1919: the rest of a table's settings, held here so the store alone owns the model.
    Column("approval_hook", Boolean, nullable=False, server_default=false()),  # REQ-204/247
    Column("hot", Boolean),  # NULL = auto-detect
    Column("promotions", JSON, nullable=False, default=list, server_default="[]"),  # REQ-119
    Column("query_template", Text),  # REQ-1668/1683: neo4j / sparql tables
    Column("relay_pagination", Boolean),  # NULL = inherit
    # REQ-2248: deleting it retires it, as a term is retired: why, who, when (schema.sql).
    *_retirement(),
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
    Column("domain_id", Text, ForeignKey("domains.id", ondelete="CASCADE")),
    Column("column_name", Text, nullable=False),
    Column("visible_to", JSON, nullable=False, default=list, server_default="[]"),
    Column("alias", Text),
    Column("description", Text),
    Column("path", Text),
    Column("data_type", Text),
    Column("masks", JSON, nullable=False, default=list, server_default="[]"),  # REQ-2059
    Column("is_primary_key", Boolean, nullable=False, server_default=false()),
    # REQ-2190: the source declares the column NOT NULL, as recorded when it was registered.
    Column("not_null", Boolean, nullable=False, server_default=false()),
    Column("native_filter_type", Text),
    # Whether the parameter is required by its source; NULL where it is not recorded.
    Column("native_filter_required", Boolean),
    # REQ-1986: a steward made the parameter required for the table.
    Column("required_by_steward", Boolean, nullable=False, server_default=false()),
    Column("is_foreign_key", Boolean, nullable=False, server_default=false()),
    Column("is_alternate_key", Boolean, nullable=False, server_default=false()),
    Column("object_fields", JSON, nullable=False, default=list, server_default="[]"),
    Column("scope", Text, nullable=False, server_default="domain"),
    Column("gql_selection", Text),
    # A remote GraphQL column's object type, and whether it holds a list of them (#245).
    Column("gql_object_type", Text),
    Column("gql_is_list", Boolean, nullable=False, server_default=false()),
    Column("epoch_unit", Text),  # REQ-1908: epoch-number storage unit of a temporal column
    # REQ-1965: the hidden attributes of a natively array/struct/map column, from its source.
    Column("array_depth", Integer, nullable=False, server_default="0"),
    Column("element_type", Text),
    Column("native_type", Text),
    Column("native_platform", Text),  # whose spelling native_type is
    # REQ-2171: the subtype the column was registered as (inet, money, ...); data_type is its base.
    Column("subtype", Text),
    Column("fake", Text),  # REQ-1494: the column's kind of fake, as declared
    Column("fake_stable", Boolean, nullable=False, server_default=false()),  # REQ-1494
    # REQ-1494: the portable definition version a stable fake is pinned to; NULL when not stable
    Column("fake_stable_version", Integer),
    Column("synthetic_rule", Text),  # REQ-1494, REQ-1939: laid over the fake, for generation
    # REQ-2094: why the column's fake and synthetic rule are what they are (provisa.judgments.why).
    Column("fake_why", JSON(none_as_null=True)),
    Column("embedding", Boolean, nullable=False, server_default=false()),  # REQ-421
    Column("embedding_model", Text),  # REQ-421
    Column("embedding_source_column", Text),  # REQ-421
    Column("encrypted", Boolean, nullable=False, server_default=false()),  # REQ-1919
    # REQ-2248: deleting it retires it, as a term is retired: why, who, when (schema.sql).
    *_retirement(),
    Column("tenant_id", Uuid),
    UniqueConstraint("table_id", "column_name"),
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
    # REQ-2141: the rows this edge joins are the same entity (repositories/identity.py).
    Column("same_entity", Boolean, nullable=False, server_default=false()),
    # REQ-1586: junction declaration — see schema.sql for the shape and the completeness checks.
    Column("via_table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE")),
    Column("via_source_column", Text),
    Column("via_target_column", Text),
    Column("via_type_column", Text),
    Column("via_type_value", Text),
    Column("via_label_source", Text),
    CheckConstraint(
        "via_label_source IN ('column', 'table', 'fixed')",
        name="relationships_via_label_source_check",
    ),
    Column("source_json_key", Text),
    # REQ-2099: the target's parameters this edge feeds, {parameter: source column}.
    Column("parameters", JSON, nullable=False, default=dict, server_default="{}"),
    Column("owner", Text),
    Column("version", Integer, nullable=False, server_default="1"),
    Column("needs_review", Boolean, nullable=False, server_default=false()),
    # REQ-351: the effective names, derived and stored once (core.relationship_names).
    Column("cypher_type", Text),
    Column("graphql_field", Text),
    # REQ-2248: deleting it retires it, as a term is retired: why, who, when (schema.sql).
    *_retirement(),
    Column("tenant_id", Uuid),
    UniqueConstraint("source_table_id", "alias", name="relationships_source_alias_unique"),
    CheckConstraint(
        "cardinality IN ('many-to-one', 'one-to-many', 'one-to-one')",
        name="relationships_cardinality_check",
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
    Column("visible_to", JSON, nullable=False, default=list, server_default="[]"),
    Column("from_fact", Text),
    # REQ-2243: the glossary term the metric names (schema.sql).
    Column("term_id", Integer, ForeignKey("glossary_terms.id", ondelete="SET NULL")),
    # REQ-2248: deleting it retires it, as a term is retired: why, who, when (schema.sql).
    *_retirement(),
)

roles = Table(
    "roles",
    metadata,
    Column("id", Text, primary_key=True),
    # REQ-2080: what the role is for, as an access review reads it. Required on every write
    # (models.Role); a standard role's is written by db.assert_standard_roles (REQ-2155).
    Column("description", Text, nullable=False, server_default=""),
    Column("capabilities", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-1602: rights the role does NOT hold, whose surfaces it is shown anyway -- disabled and
    # badged. A role defined by subtraction (the sandbox, REQ-1597) is a demonstration, and an
    # absent feature demonstrates nothing. Disjoint from `capabilities` by construction.
    Column("demonstrated", JSON, nullable=False, default=list, server_default="[]"),
    Column("domain_access", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-1921: with data_residency, the region values the grant covers (regions, "no_region").
    Column("residency_values", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-1682: the role's constant session values {name: value}, what
    # current_setting('provisa.<name>') reads when the request binds nothing of that name.
    Column("session_vars", JSON, nullable=False, default=dict, server_default="{}"),
    # REQ-1174: per-role rate + query limits {requests_per_second, max_query_complexity,
    # max_query_time_ms, ...}. None/absent = unlimited.
    Column("rate_limit", JSON(none_as_null=True)),
    # REQ-2155: the roles this one inherits from, in declared order; its capabilities are the
    # union of its own and every ancestor's (security.inheritance).
    Column("parents", JSON, nullable=False, default=list, server_default="[]"),
    Column("max_rows", Integer),  # REQ-005: the role's result-size ceiling; NULL = none of its own
    # A grant of this role unused for this many days is removed (auth.role_grants); NULL = never.
    Column("inactivity_days", Integer),
    Column("relationship_guard", Boolean, nullable=False, server_default=true()),  # REQ-1919
    # REQ-2219: what a holder gets with no list, {kind: [identifier, ...]}, written from
    # domain_access when that is set; narrowed afterwards. Absent kinds are the whole role.
    Column("default_entitlements", JSON(none_as_null=True)),
    Column("org_id", Text),  # cross-model ref -> admin.orgs
    Column("tenant_id", Uuid),
)

# REQ-2219: the entitlement store. Append-only: a change appends a row whose valid_from ends the
# row before it for the same principal, role, kind and capability; ``removed`` ends it with none.
entitlements = Table(
    "entitlements",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("source", Text, nullable=False),
    Column("principal", Text),  # NULL on a role's own rows (source role)
    Column("role_id", Text, nullable=False),
    Column("kind", Text, nullable=False),
    Column("capability", Text),
    Column("identifiers", JSON, nullable=False, default=list, server_default="[]"),
    Column("removed", Boolean, nullable=False, server_default=false()),
    Column("reason", Text),
    Column("set_by", Text),
    Column("valid_from", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("recorded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("source IN ('idp', 'provisa', 'role')", name="entitlements_source_check"),
)

# REQ-2219: one row per expiry rule on a person's grant; ``ends`` is {capability: [identifier,
# ...]}, empty for the whole grant. Append-only: ``removed`` withdraws the rule named by rule_key.
entitlement_expiries = Table(
    "entitlement_expiries",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("rule_key", Text, nullable=False),
    Column("principal", Text, nullable=False),
    Column("role_id", Text, nullable=False),
    Column("ends_at", DateTime(timezone=True), nullable=False),
    Column("ends", JSON, nullable=False, default=dict, server_default="{}"),
    Column("list_kind", Text),  # the list it was set with; re-setting that list withdraws it
    Column("removed", Boolean, nullable=False, server_default=false()),
    Column("reason", Text),
    Column("set_by", Text),
    Column("valid_from", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("recorded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-2219, ONE API, THREE WAYS TO USE IT: a list granted by typed email to a person not yet seen.
# Append-only, keyed by (email, role_id, kind): the last row is the grant's state. ``outcome`` is
# NULL while it waits; ``principal`` names whom a sign-in attached it to, or was refused for.
entitlement_pending = Table(
    "entitlement_pending",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("email", Text, nullable=False),
    Column("role_id", Text, nullable=False),
    Column("kind", Text, nullable=False),
    Column("identifiers", JSON, nullable=False, default=list, server_default="[]"),
    Column("expires_at", DateTime(timezone=True)),
    Column("outcome", Text),
    Column("principal", Text),
    Column("reason", Text, nullable=False),
    Column("set_by", Text, nullable=False),
    Column("valid_from", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("recorded_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "outcome IN ('ended', 'attached', 'refused')", name="entitlement_pending_outcome_check"
    ),
)

# REQ-2074: what each role may write to each table, per operation. rows_expr / check_expr are row
# predicates, encrypted at rest as rls_rules.filter_expr is (REQ-686); NULL is the role's read row
# filter on the table.
table_write_grants = Table(
    "table_write_grants",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("role_id", Text, ForeignKey("roles.id", ondelete="CASCADE"), nullable=False),
    Column("operation", Text, nullable=False),
    Column("columns", JSON, nullable=False, default=list, server_default="[]"),
    Column("rows_expr", LargeBinary),
    Column("check_expr", LargeBinary),
    Column("presets", JSON, nullable=False, default=list, server_default="[]"),
    Column(
        "why", JSON(none_as_null=True)
    ),  # REQ-2094: why the grant is what it is (provisa.judgments.why)
    Column("tenant_id", Uuid),
    CheckConstraint(
        "operation IN ('insert', 'update', 'delete')", name="table_write_grants_operation"
    ),
    UniqueConstraint("table_id", "role_id", "operation"),
)

rls_rules = Table(
    "rls_rules",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE")),
    Column("domain_id", Text, ForeignKey("domains.id", ondelete="CASCADE")),
    Column("role_id", Text, ForeignKey("roles.id", ondelete="CASCADE"), nullable=False),
    Column("filter_expr", LargeBinary, nullable=False),  # REQ-686: encrypted at rest (BYTEA)
    # REQ-2081: what the rule is for, as an access review reads it. Required on every write.
    Column("description", Text, nullable=False, server_default=""),
    Column("action_name", Text),  # REQ-1679: a tracked function/webhook target, by name
    # REQ-1384: a user tag — the rule applies to every table carrying it. No ON DELETE: a tag
    # that carries a row filter is refused deletion (repositories.integrity), because removing
    # the filter with the tag would widen what its role reads without anyone saying so.
    Column("tag_id", Text, ForeignKey("tags.id")),
    Column(
        "why", JSON(none_as_null=True)
    ),  # REQ-2094: why the rule is what it is (provisa.judgments.why)
    # REQ-2188: the rights the one predicate narrows: R reading a row, U updating it, D deleting
    # it, in that order. A right is in at most one rule of a role on a target; no constraint can
    # say so across rows, so the save does (repositories.rls.upsert).
    Column("rights", Text, nullable=False, server_default="R"),
    Column("tenant_id", Uuid),
    CheckConstraint("rights IN ('R', 'U', 'D', 'RU', 'RD', 'UD', 'RUD')", name="rls_rules_rights"),
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
    # REQ-1943: the Sensitive data option.
    Column("sensitive", Boolean, nullable=False, server_default=false()),
    # REQ-2208: each assignment must cite a library document or section; one that does not is a
    # gap, never refused.
    Column("requires_citation", Boolean, nullable=False, server_default=false()),
    # REQ-2208 (THE TAGS REPORT): required on every object of the kinds applies_to lists.
    Column("required", Boolean, nullable=False, server_default=false()),
    # REQ-2208 (A TAG IS OFFERED BY DOMAIN): offered on objects of these domains, as a source's
    # allowed_domains; empty = every domain.
    Column("allowed_domains", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-2248: deleting it retires it, as a term is retired: why, who, when (schema.sql).
    *_retirement(),
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
    Column("product_id", Text, ForeignKey("data_products.id", ondelete="CASCADE")),  # REQ-1634
    # REQ-2145: the glossary term a tag is on (pii on a Property term).
    Column("term_id", Integer, ForeignKey("glossary_terms.id", ondelete="CASCADE")),
    # REQ-2208: a row rule, a role or a governed metric the tag is on.
    Column("rls_rule_id", Integer, ForeignKey("rls_rules.id", ondelete="CASCADE")),
    Column("role_id", Text, ForeignKey("roles.id", ondelete="CASCADE")),
    Column("metric_name", Text, ForeignKey("metrics.name", ondelete="CASCADE", onupdate="CASCADE")),
    Column("object_key", Text, nullable=False),
    Column("reason", Text),  # required for 'deprecated' (enforced at the mutation layer)
    Column("expires_on", Text),  # ISO date; planned removal for 'deprecated', typed for reporting
    # REQ-2094: the rest of the assignment's "why" -- ``reason`` above is its reason. What it is
    # for, the named bases it cites, a review-by date (asks for a look; it does not end the
    # assignment as ``expires_on`` does), and who decided it, when and on what profile evidence,
    # set by the server at each save that changes the assignment or its why.
    Column("purpose", Text),
    Column("bases", JSON(none_as_null=True)),
    Column("review_on", Text),  # ISO date
    Column("decided_by", Text),
    Column("decided_at", DateTime(timezone=True)),
    Column("evidence", JSON(none_as_null=True)),
    Column("tenant_id", Uuid),
    UniqueConstraint("base_tag_id", "object_key"),
    CheckConstraint(
        "object_type IN ('source', 'table', 'column', 'relationship', 'command', 'product', "
        "'term', 'rls_rule', 'role', 'metric')",
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
    # REQ-2118: the export target (its id in the org's metadata_export.targets), not the provider:
    # two targets of one provider keep their bindings apart.
    Column("target", Text, nullable=False),
    Column("semantic_uri", Text, nullable=False),
    Column("vendor_ref", Text, nullable=False),
    Column("physical_key", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("tenant_id", Uuid),
    UniqueConstraint("target", "semantic_uri"),
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
    # REQ-2243: the term's calculation method (schema.sql).
    Column("calculation_method", Text),
    Column("deprecated", Boolean, nullable=False, server_default=false()),
    # Curator-set soft delete: bound but out of service. See schema.sql for the full rule.
    Column("retired", Boolean, nullable=False, server_default=false()),
    Column("export_excluded", Boolean, nullable=False, server_default=false()),
    # REQ-1387 (STATUS IS HUMAN): a steward approved the term (schema.sql).
    Column("approved", Boolean, nullable=False, server_default=false()),
    # REQ-2144: the asserted parts of a term's status (schema.sql).
    Column("deprecation_asserted", Boolean, nullable=False, server_default=false()),
    Column("status_reason", Text),
    Column("retire_on", Date),
    Column("status_by", Text),
    Column("status_at", DateTime(timezone=True)),
    # REQ-2237: the IRI of the imported concept the term is (schema.sql).
    Column("iri", Text),
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

# REQ-1591: a term's DECLARED domains — written only for an abstract term (which holds no refs
# to derive from) and as the stamp left when a rooted term's last ref departs. A term's domains
# are its refs' domains while it has refs, and these otherwise. See schema.sql for the full rule.
glossary_term_domains = Table(
    "glossary_term_domains",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("term_id", Integer, ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False),
    Column("domain_id", Text, nullable=False),
    Column("tenant_id", Uuid),
    UniqueConstraint("term_id", "domain_id"),
)

# REQ-2237: what an imported ontology states about a term that the term does not model, one row a
# statement (provisa/ontology_import/kept.py). See schema.sql.
glossary_term_statements = Table(
    "glossary_term_statements",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("term_id", Integer, ForeignKey("glossary_terms.id", ondelete="CASCADE"), nullable=False),
    Column("ordinal", Integer, nullable=False),
    Column("kind", Text, nullable=False),
    Column("statement", Text, nullable=False),
    Column("tenant_id", Uuid),
    UniqueConstraint("term_id", "ordinal"),
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
    Column("join_pattern", JSON(none_as_null=True)),
    Column("custom_sql", Text),
    Column("expose_in_sdl", Boolean, nullable=False, server_default=false()),
    Column("sdl_config", JSON(none_as_null=True)),
    # REQ-1922: the view's DEFINITION, shared by the org's regions. Each region's build of it —
    # status, the fleet's claim on it, what it was built from — is that region's own:
    # ``mv_build_state`` in the region's state store.
    # REQ-961/962: temporal-processing declaration. calendar/grain name the shared, versioned
    # boundary source that yields [start,end) windows; allowed_lateness (seconds) extends the claim
    # deadline past window.end; expected_events is the freshness contract (inputs that must be
    # fresh-through window.end; NULL = default to all SQL-lineage inputs); business_day_grain gates
    # window existence on the calendar's business days. All NULL/false = a non-temporal MV.
    Column("calendar", Text),
    Column("grain", Text),
    Column("allowed_lateness", Integer, nullable=False, server_default="0"),
    Column("expected_events", JSON(none_as_null=True)),
    Column("business_day_grain", Boolean, nullable=False, server_default=false()),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-879, REQ-1922: a region's build of a materialized view — the record of what that region
# built (``region``: the one that built it) and the fleet's refresh coordination for it. Kept in
# the region's STATE store: each region builds its own copy of a view that names no region, and
# only its home builds one that names a region; one row shared by every region would have them
# contend for one claim and report one status. writer = the instance owning the in-flight refresh;
# lease_until = when its claim expires (a crashed refresher's lease times out so the view is
# reclaimable). The version stamps are the REQ-862 dedup key: a claim skips when
# materialized_input_version already == target. No foreign key crosses to the model
# (integrity.remove_parts and the region's prune remove these with their view).
mv_build_state = Table(
    "mv_build_state",
    metadata,
    Column("mv_id", Text, primary_key=True),
    Column("region", Text, nullable=False),
    Column("status", Text, nullable=False, server_default="stale"),
    Column("last_refresh_at", DateTime(timezone=True)),
    Column("row_count", Integer),
    Column("last_error", Text),
    Column("writer", Text),
    Column("lease_until", DateTime(timezone=True)),
    Column("materialized_definition_version", Text),
    Column("materialized_input_version", Text),
    Column("snapshot_id", Text),
    # REQ-2015: the indexes the view's standing landing asked for and how each stands, as a
    # replica's are (``replica_state.index_state``): {"for", "table", "wanted", "built", "failed"}.
    Column("index_state", JSON(none_as_null=True)),
    CheckConstraint(
        "status IN ('fresh', 'stale', 'refreshing', 'disabled')",
        name="mv_build_state_status_check",
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
    Column(
        "holidays", JSON(none_as_null=True), default=list, server_default="[]"
    ),  # ISO dates, versioned/immutable
    Column(
        "weekend", JSON(none_as_null=True), default=list, server_default="[5, 6]"
    ),  # weekday ints (Sat, Sun)
    # REQ-2111: the local time a day closes at ("18:00"); NULL = local midnight.
    Column("day_close", Text),
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
    # REQ-1922: kept in the region's state store, apart from the model; no foreign key crosses
    # the two (integrity.remove_parts and the region's prune remove these with their owner).
    Column("mv_id", Text, nullable=False),
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
    # REQ-1922: kept in the region's state store, apart from the model; no foreign key crosses
    # the two (integrity.remove_parts and the region's prune remove these with their owner).
    Column("mv_id", Text, nullable=False),
    Column("refresh_version", Integer, nullable=False),
    Column("definition_version", Text),
    Column("trace_id", Text),
    Column("change_type", Text, nullable=False),
    Column("row_key", Text, nullable=False),
    Column("old_hash", Text),
    Column("new_hash", Text),
    Column("old_values", JSON(none_as_null=True)),
    Column("new_values", JSON(none_as_null=True)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "change_type IN ('insert', 'update', 'delete')",
        name="mv_delta_ledger_change_type_check",
    ),
)

# REQ-2129: the model's column lineage edges, folded from its compiled views and statement
# commands (provisa.lineage.edges). Derived state: each model build replaces every row with the
# edges the federation lineage graph holds, and nothing else writes it. An end that is a
# registered column carries that column's table and stored name, so the meta views join it.
lineage_edges = Table(
    "lineage_edges",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_relation", Text, nullable=False),
    Column("source_column", Text, nullable=False),
    Column("target_relation", Text, nullable=False),
    Column("target_column", Text, nullable=False),
    Column("via", Text, nullable=False),
    Column("via_kind", Text, nullable=False),
    Column("source_table_id", Integer),
    Column("source_column_name", Text),
    Column("target_table_id", Integer),
    Column("target_column_name", Text),
    CheckConstraint("via_kind IN ('view', 'command')", name="lineage_edges_via_kind_check"),
)

# REQ-2229: what OpenLineage events reported: one row per dataset of a job's latest run. Kept apart
# from lineage_edges, which each model build replaces whole.
openlineage_run_datasets = Table(
    "openlineage_run_datasets",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_namespace", Text, nullable=False),
    Column("job_name", Text, nullable=False),
    Column("run_id", Text, nullable=False),
    Column("event_time", DateTime(timezone=True), nullable=False),
    Column("direction", Text, nullable=False),
    Column("dataset_namespace", Text, nullable=False),
    Column("dataset_name", Text, nullable=False),
    Column("column_lineage", JSON, nullable=False, default=dict, server_default="{}"),
    CheckConstraint(
        "direction IN ('input', 'output')", name="openlineage_run_datasets_direction_check"
    ),
    UniqueConstraint(
        "job_namespace",
        "job_name",
        "run_id",
        "direction",
        "dataset_namespace",
        "dataset_name",
        name="openlineage_run_datasets_key",
    ),
)

# REQ-2125: the governed table each checker table's contract scans, as the model build resolved it
# (provisa.dq.registration.write_checker_targets). Derived state: each build replaces every row.
checker_targets = Table(
    "checker_targets",
    metadata,
    Column("table_id", Integer, primary_key=True),
    Column("checked_table_id", Integer, nullable=False),
)

# REQ-194: the names table of the last model version built
# (provisa.core.repositories.model_names.write_names_table). Derived state: each build replaces every row.
model_names = Table(
    "model_names",
    metadata,
    Column("object_kind", Text, primary_key=True),
    Column("object_id", Text, primary_key=True),
    Column("surface", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("physical_name", Text, nullable=False),
    Column("label", Text, nullable=False),
    Column("domain_id", Text, nullable=False, server_default=""),
    Column("table_id", Integer),
    Column("is_alias", Boolean, nullable=False, server_default=false()),
    Column("warnings", JSON, nullable=False, server_default="[]"),
)

# REQ-194: the name clashes the last model build refused, each naming both objects.
model_name_clashes = Table(
    "model_name_clashes",
    metadata,
    Column("surface", Text, primary_key=True),
    Column("name", Text, primary_key=True),
    Column("holder", Text, nullable=False),
    Column("newcomer", Text, primary_key=True),
    Column("message", Text, nullable=False),
)

# REQ-2132: each checker table's latest scan -- its id, time and outcome (pass, or fail when any
# check did not pass), written in the scan's own transaction (provisa.dq.scan.record_latest_run).
# A table with no row has never run. The dry run writes none.
checker_latest_runs = Table(
    "checker_latest_runs",
    metadata,
    Column("table_id", Integer, primary_key=True),
    Column("scan_id", Text, nullable=False),
    Column("scan_time", DateTime(timezone=True), nullable=False),
    Column("outcome", Text, nullable=False),
    CheckConstraint("outcome IN ('pass', 'fail')", name="checker_latest_runs_outcome_check"),
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

# REQ-2221: the connectors this organization offers: the model's ``connectors:`` block, promoted
# with the model. path and build_command are the CLI's; the server keeps them and acts on neither.
connectors = Table(
    "connectors",
    metadata,
    Column("name", Text, primary_key=True),
    Column("git", Text, nullable=False),
    Column("ref", Text, nullable=False),
    Column("commit", Text, nullable=False, quote=True),
    Column("path", Text, nullable=False, server_default=""),
    Column("build_command", Text, nullable=False, server_default=""),
    Column("language", Text, nullable=False),
    Column("image", Text, nullable=False),
    Column("digest", Text, nullable=False),
    Column("code_hash", Text, nullable=False),
    Column("arch", Text, nullable=False),
    Column("requires", JSON(none_as_null=True), nullable=False, default=list, server_default="[]"),
    Column("description", Text, nullable=False, server_default=""),
    Column("icon", Text, nullable=False, server_default=""),
    Column("settings", JSON(none_as_null=True), nullable=False, default=list, server_default="[]"),
    CheckConstraint("language IN ('node', 'python', 'go')", name="connectors_language_check"),
    CheckConstraint("arch IN ('amd64', 'arm64')", name="connectors_arch_check"),
)

kafka_sources = Table(
    "kafka_sources",
    metadata,
    Column("id", Text, primary_key=True),
    Column("bootstrap_servers", Text, nullable=False, server_default=""),
    Column("schema_registry_url", Text),
    Column("auth_type", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # REQ-1919: the source as its configuration wrote it (topics, windows, discriminators, auth;
    # a credential stays its reference).
    Column("spec", JSON(none_as_null=True)),
    # REQ-1491: whether this environment has supplied this source's connection values. A copy
    # carries the row and never the binding, and an empty host is not an absent one — the
    # connection builder reads it as localhost — so an unbound source is MARKED, and the query
    # path refuses a query naming it rather than dialling whatever is local to the node.
    Column(
        "binding", Text, nullable=False, server_default="own"
    ),  # REQ-1942: own | copied | unbound
    CheckConstraint("binding IN ('own', 'copied', 'unbound')", name="kafka_sources_binding_check"),
)

# REQ-2228: what is held of a dbt source's manifest: its definitions, never the document.
dbt_manifests = Table(
    "dbt_manifests",
    metadata,
    Column("source_id", Text, ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True),
    Column("definitions", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-2228, REQ-858: what dbt's run results last said of each node it builds. built_at is when dbt
# last built it, kept through a later run that failed.
dbt_node_runs = Table(
    "dbt_node_runs",
    metadata,
    Column("source_id", Text, ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True),
    Column("unique_id", Text, primary_key=True),
    Column("status", Text, nullable=False),
    Column("generated_at", DateTime(timezone=True), nullable=False),
    Column("built_at", DateTime(timezone=True)),
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

api_sources = Table(
    "api_sources",
    metadata,
    Column("id", Text, primary_key=True),
    Column("type", Text, nullable=False),
    Column("base_url", Text, nullable=False, server_default=""),
    Column("spec_url", Text),
    # REQ-686: API auth config (keys/tokens) encrypted at rest, decrypted before use.
    Column("auth", LargeBinary),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # REQ-1491: whether this environment has supplied this source's connection values. A copy
    # carries the row and never the binding, and an empty host is not an absent one — the
    # connection builder reads it as localhost — so an unbound source is MARKED, and the query
    # path refuses a query naming it rather than dialling whatever is local to the node.
    Column(
        "binding", Text, nullable=False, server_default="own"
    ),  # REQ-1942: own | copied | unbound
    CheckConstraint("binding IN ('own', 'copied', 'unbound')", name="api_sources_binding_check"),
    CheckConstraint(
        "type IN ('openapi', 'graphql_api', 'grpc_api', 'neo4j', 'sparql', 'quickbooks')",
        name="api_sources_type_check",
    ),
)

api_endpoints = Table(
    "api_endpoints",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, ForeignKey("api_sources.id", ondelete="CASCADE"), nullable=False),
    Column("path", Text, nullable=False),
    Column("method", Text, nullable=False, server_default="GET"),
    # A table is named within its source: two sources may each register a table of one name.
    Column("table_name", Text, nullable=False),
    Column("columns", JSON, nullable=False),
    Column("ttl", Integer, nullable=False, server_default="300"),
    Column("response_root", Text),
    Column("error_path", Text),
    Column("pk_column", Text),
    # REQ-318: a copy of the table's paging; NULL = not paged (SQL NULL, never JSON null).
    Column("pagination", JSON(none_as_null=True)),
    Column("max_concurrency", Integer),
    Column("default_params", JSON(none_as_null=True)),
    Column("promotions", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-1668: query-API endpoint shape (neo4j) — NULL on plain REST endpoints.
    Column("body_encoding", Text),
    Column("query_template", Text),
    Column("response_normalizer", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("source_id", "table_name", name="api_endpoints_source_table_key"),
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
    # REQ-2217, REQ-2216, REQ-2215: see the matching block in schema.sql.
    Column("environment", Text, nullable=False, server_default="prod"),
    Column("addressed_roles", JSON, nullable=False, default=list, server_default="[]"),
    Column("addressed_members", JSON, nullable=False, default=list, server_default="[]"),
    Column("unresolved_roles", JSON, nullable=False, default=list, server_default="[]"),
    CheckConstraint(
        "status IN ('pending', 'executed', 'rejected')", name="creation_requests_status_check"
    ),
)

# REQ-2216: approve and decide calls already answered, by Idempotency-Key. See schema.sql.
idempotent_calls = Table(
    "idempotent_calls",
    metadata,
    Column("scope", Text, primary_key=True),
    Column("actor", Text, primary_key=True),
    Column("call_key", Text, primary_key=True),
    Column("response", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
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
    Column("domain_id", Text, nullable=False, server_default=""),
    Column("description", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("kind", Text, nullable=False, server_default="mutation"),
    Column("return_schema", JSON(none_as_null=True)),
    # REQ-1159: canonical IR-typed output dataset contract [{name,type}]; return_schema projects it to GraphQL.
    Column("output_columns", JSON(none_as_null=True)),
    # REQ-885: implementation kind + swappable binding, decoupled from addressing.
    Column("impl_kind", Text, nullable=False, server_default="source_procedure"),
    Column("binding", JSON, nullable=False, default=dict, server_default="{}"),
    Column("materialize", Boolean, nullable=False, server_default=false()),
    # REQ-1924: each call is approved by the approval hook before it runs.
    Column("requires_approval", Boolean, nullable=False, server_default=false()),
    # REQ-1924, REQ-871: "schema.table" of the registered table the command writes, when known.
    Column("writes_table", Text),
    # REQ-2168: the command's name per language {language: {name?, exclude?}}.
    Column("language_bindings", JSON, nullable=False, default=dict, server_default="{}"),
)


# REQ-1634: data-product membership, many-to-many: one row per product and member table (or view),
# and one per product and member command (schema.sql).
data_product_tables = Table(
    "data_product_tables",
    metadata,
    Column("product_id", Text, ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False),
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    PrimaryKeyConstraint("product_id", "table_id"),
)

data_product_commands = Table(
    "data_product_commands",
    metadata,
    Column("product_id", Text, ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False),
    Column(
        "command_name",
        Text,
        ForeignKey("tracked_functions.name", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
    ),
    PrimaryKeyConstraint("product_id", "command_name"),
)

# REQ-2180 OFFERS: a product's offers (see schema.sql).
data_product_offers = Table(
    "data_product_offers",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("product_id", Text, ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False),
    Column("name", Text, nullable=False),
    Column("role_id", Text, nullable=False),
    Column("data_level", Text, nullable=False, server_default="full"),
    Column("sample_views", JSON, nullable=False, default=list, server_default="[]"),
    Column("dq_results", Boolean, nullable=False, server_default=true()),
    Column("profiles", Boolean, nullable=False, server_default=false()),
    Column("open", Boolean, nullable=False, server_default=false()),
    Column("duration_days", Integer),
    Column("version", Integer, nullable=False, server_default="1"),
    Column("published_by", Text),
    Column("review", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("product_id", "name"),
    CheckConstraint(
        "data_level IN ('full', 'sample', 'synthetic')",
        name="data_product_offers_data_level_check",
    ),
)

# REQ-2180: each offer's contract for each dataset of its product, versioned on its own.
data_product_offer_contracts = Table(
    "data_product_offer_contracts",
    metadata,
    Column(
        "offer_id",
        Integer,
        ForeignKey("data_product_offers.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    Column("contract", JSON, nullable=False),
    PrimaryKeyConstraint("offer_id", "table_id"),
)

# REQ-2180: a bid, or the cancel of an accepted one, as the request it fired through the outbound
# request hook, and its outcome (see schema.sql).
data_product_bids = Table(
    "data_product_bids",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("request_id", Text, nullable=False, unique=True),
    Column("kind", Text, nullable=False, server_default="bid"),
    Column("cancels", Integer, ForeignKey("data_product_bids.id", ondelete="CASCADE")),
    Column("product_id", Text, ForeignKey("data_products.id", ondelete="CASCADE"), nullable=False),
    Column("offer_id", Integer, ForeignKey("data_product_offers.id", ondelete="SET NULL")),
    Column("offer_version", Integer, nullable=False),
    Column("terms", JSON, nullable=False),
    Column("requester", Text, nullable=False),
    Column("reason", Text, nullable=False),
    Column("hook", Text, nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", Text, nullable=False, server_default="pending"),
    Column("decided_by", Text),
    Column("decided_at", DateTime(timezone=True)),
    Column("decline_reason", Text),
    # none_as_null: a bid with nothing granted (None) is SQL NULL, never the JSON value null.
    Column("granted", JSON(none_as_null=True)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("kind IN ('bid', 'cancel')", name="data_product_bids_kind_check"),
    CheckConstraint(
        "status IN ('pending', 'accepted', 'declined', 'withdrawn', 'cancelled')",
        name="data_product_bids_status_check",
    ),
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
    # REQ-209, REQ-1924: each call is approved by the approval hook before the webhook is called.
    Column("requires_approval", Boolean, nullable=False, server_default=false()),
)

# REQ-1003, REQ-1004: the org's scheduled triggers. Model rows (REQ-1919): the org schema is the org,
# so a trigger belongs to the org whose model holds it and runs bound to it; versioned and
# branchable like every definition, and only prod's are scheduled.
scheduled_triggers = Table(
    "scheduled_triggers",
    metadata,
    Column("id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    # When it fires: a cron schedule, or (REQ-2054) a registered table's change or event-loop
    # event -- one of the two.
    Column("cron", Text),
    Column("on_table", Text),  # "source_id.schema.table"
    Column("on_event", Text),  # change | error | quarantine
    Column("kind", Text, nullable=False),
    # kind "webhook": the URL it calls (a config trigger), or the org's tracked webhook by name,
    # resolved when it fires (an admin-made one).
    Column("url", Text),
    Column("webhook_name", Text),
    Column("args", JSON, nullable=False, default=dict, server_default="{}"),
    # kind "sql" (REQ-1003/1004): the statement and the role of this org it runs as.
    Column("sql", Text),
    # kind "command" (REQ-2054): the registered command it runs, as ``role``.
    Column("command", Text),
    Column("role", Text),
    Column("enabled", Boolean, nullable=False, server_default=true()),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "(kind = 'sql' AND sql IS NOT NULL AND role IS NOT NULL)"
        " OR (kind = 'webhook' AND (url IS NOT NULL OR webhook_name IS NOT NULL))"
        " OR (kind = 'command' AND command IS NOT NULL AND role IS NOT NULL)",
        name="scheduled_triggers_kind_fields",
    ),
    CheckConstraint(
        "(cron IS NOT NULL AND on_table IS NULL)"
        " OR (cron IS NULL AND on_table IS NOT NULL AND on_event IS NOT NULL)",
        name="scheduled_triggers_when",
    ),
)

# REQ-1939: an environment's synthetic datasets. Each names (table, profile run) pairs, a scale and a
# seed; generated, its tables are written to a store schema of its own and, in this environment
# only, read in place of the tables' bindings. Environment-local: never copied or promoted.
synthetic_datasets = Table(
    "synthetic_datasets",
    metadata,
    Column("id", Text, primary_key=True),
    Column("seed", BigInteger, nullable=False),
    Column("scale", Float, nullable=False),
    # defined | generating | generated | failed
    Column("status", Text, nullable=False),
    Column("store_schema", Text, nullable=False),
    Column("error", Text),
    # REQ-1942: why the report on its generated rows failed -- the report's own outcome, apart
    # from the generation's; NULL when it did not fail.
    Column("report_error", Text),
    Column("generated_at", DateTime(timezone=True)),
    # REQ-1939: conditional child counts by relationship ([{relationship, condition, count}]) and
    # the statements checked after generation ([statement, ...])
    Column("fanout_conditions", JSON, nullable=False, default=list, server_default="[]"),
    Column("assertions", JSON, nullable=False, default=list, server_default="[]"),
    # REQ-1939, DIFFERENTIAL PRIVACY: the dataset's privacy budget ε; NULL when not private
    Column("private_epsilon", Float),
    # REQ-1939, NOT TOO CLOSE TO A REAL ROW: threshold and draws; both NULL when not checked
    Column("closeness_threshold", Float),
    Column("closeness_draws", Integer),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-1942: what each source of a Test (synthetic) environment was bound to before generating
# bound it to the synthetic store -- its type, binding and connection columns -- so dropping the
# dataset or leaving the mode restores exactly that. Environment-local: never copied or promoted.
# REQ-1942: each finished whole-model generation of the environment -- the rows it generated and
# the seconds it took -- which the next generation's time is estimated from. Environment-local.
synthetic_generations = Table(
    "synthetic_generations",
    metadata,
    Column("finished_at", DateTime(timezone=True), primary_key=True),
    Column("generated_rows", BigInteger, nullable=False),
    Column("seconds", Float, nullable=False),
)

# REQ-2094: named bases a judgment can cite -- a policy, regulation or position the data cannot
# show. Declaring one changed sets ``changed_at``, which dates every judgment citing it decided
# before then.
judgment_bases = Table(
    "judgment_bases",
    metadata,
    Column("id", Text, primary_key=True),
    Column("name", Text, nullable=False, unique=True),
    Column("description", Text),
    Column("owner", Text),
    Column("changed_at", DateTime(timezone=True)),
    Column("changed_by", Text),
    Column("change_note", Text),
)

# REQ-2094: the review queue -- one row per (object, judgment, finding kind), written only by
# provisa.judgments.findings.record_finding. A finding seen again refreshes ``last_seen``; one
# no longer seen is resolved.
judgment_findings = Table(
    "judgment_findings",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("object_type", Text, nullable=False),
    Column("object_key", Text, nullable=False),
    Column(
        "judgment", Text, nullable=False
    ),  # tag | fake | mask | row_rule | write_grant | posture
    # wrong | dated_by_data | dated_by_context | undecided | worth_a_look
    Column("category", Text, nullable=False),
    Column("finding", Text, nullable=False),
    Column("observed", JSON(none_as_null=True)),
    Column("first_seen", DateTime(timezone=True), nullable=False),
    Column("last_seen", DateTime(timezone=True), nullable=False),
    Column("resolved_at", DateTime(timezone=True)),
    Column("resolved_by", Text),
    Column("resolution", Text),
    UniqueConstraint("object_type", "object_key", "judgment", "finding"),
)

# REQ-2091: the adequacy checks Provisa runs on each sensitive column's fake and synthetic rule,
# one row per check per run, so the sensitive_coverage view reads each check's latest. A result is
# a number and a flag, never a value: ``measure`` is a share or a count, ``detail`` says why.
fake_adequacy_checks = Table(
    "fake_adequacy_checks",
    metadata,
    Column(
        "table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("column_name", Text, primary_key=True),
    Column("check_name", Text, primary_key=True),  # leak_rate | kind_mismatch | copies_real
    Column("checked_at", DateTime(timezone=True), primary_key=True),
    Column("status", Text, nullable=False),  # passed | failed | not_run
    Column("measure", Float),
    Column("detail", Text, nullable=False),
)

synthetic_source_bindings = Table(
    "synthetic_source_bindings",
    metadata,
    Column("source_id", Text, ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True),
    Column("previous", JSON, nullable=False),
)

synthetic_dataset_tables = Table(
    "synthetic_dataset_tables",
    metadata,
    Column(
        "dataset_id",
        Text,
        ForeignKey("synthetic_datasets.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    # A table reads one dataset's copy at most, so it belongs to one dataset.
    Column(
        "table_id",
        Integer,
        ForeignKey("registered_tables.id", ondelete="CASCADE"),
        primary_key=True,
        unique=True,
    ),
    # The environment holding the profile run (typically prod), and the run.
    Column("profile_env", Text, nullable=False),
    Column("run_id", Text, nullable=False),
    # The table's own scale; NULL takes the dataset's.
    Column("scale", Float),
)

# REQ-1934 PROPOSED CONSTRAINTS: the operator's decision on a constraint a profile proposed --
# accepted (as proposed or edited), or dismissed so it is not proposed as new again. An accepted one
# is checked by every later profile run of the table and binds synthetic generation (REQ-1939).
profiler_constraints = Table(
    "profiler_constraints",
    metadata,
    Column("id", Text, primary_key=True),
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    # kind|column|other: one decision per constraint of a table.
    Column("signature", Text, nullable=False),
    Column("kind", Text, nullable=False),  # not_null | unique | value_set | range | ordering
    Column("column_name", Text, nullable=False),
    Column("other_column", Text),
    # The same columns by their registered (physical) names, for synthetic generation (REQ-1939).
    Column("physical_column", Text, nullable=False),
    Column("physical_other_column", Text),
    Column("definition", Text, nullable=False),  # JSON
    Column("evidence", Text, nullable=False),
    Column("share", Float),
    Column("sampled", Boolean, nullable=False),
    Column("status", Text, nullable=False),  # accepted | dismissed
    Column("run_id", Text, nullable=False),  # the run that proposed it
    Column("decided_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("table_id", "signature"),
)

# REQ-2147: a steward's decision on a classification a profile suggested for a column -- accepted
# (the column linked to the kind's Property term; for a sensitive kind marked pii), or rejected so
# it is not suggested again for that column and kind.
profiler_classifications = Table(
    "profiler_classifications",
    metadata,
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    Column("physical_column", Text, nullable=False),  # the column's registered name
    Column("kind", Text, nullable=False),  # a kind of provisa/profiler/classification_kinds.yaml
    Column("status", Text, nullable=False),  # accepted | rejected
    Column("evidence", Text, nullable=False),  # the suggestion's evidence when it was decided
    Column("run_id", Text, nullable=False),  # the run that suggested it
    Column("decided_by", Text),
    Column("decided_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("table_id", "physical_column", "kind"),
)

# REQ-2147 (CONTAINED VALUES ARE HINTS): a scan of a table's text columns for sensitive kinds held
# inside their values, asked for by a person and run in the background -- running, then succeeded
# or failed with why.
profiler_sensitive_scans = Table(
    "profiler_sensitive_scans",
    metadata,
    Column("scan_id", Text, primary_key=True),
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    Column("status", Text, nullable=False),  # running | succeeded | failed
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("finished_at", DateTime(timezone=True)),
    Column("sampled_rows", Integer),  # the rows the scan read
    Column("scanned_columns", Integer),  # the text columns it scanned
    Column("requested_by", Text),
    Column("error", Text),
)

# REQ-2147 (CONTAINED VALUES ARE HINTS): a column's latest hint per kind -- the share of its sampled
# values a kind was found inside. Only the counts: the matched text is never stored.
profiler_sensitive_hints = Table(
    "profiler_sensitive_hints",
    metadata,
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    Column("physical_column", Text, nullable=False),  # the column's registered name
    Column("kind", Text, nullable=False),  # a kind of provisa/profiler/classification_kinds.yaml
    Column("scan_id", Text, nullable=False),
    Column("values_tested", Integer, nullable=False),
    Column("values_matched", Integer, nullable=False),
    Column("share", Float, nullable=False),
    Column("evidence", Text, nullable=False),  # the hint in words
    PrimaryKeyConstraint("table_id", "physical_column", "kind"),
)

# REQ-2189: a saved, named arrangement of the ERD -- the bar's options, and per domain and table
# what is selected, collapsed and where it stands. Only the arrangement: columns and relationships
# are read from the live model, and a table or domain goes from every layout when it goes.
erd_layouts = Table(
    "erd_layouts",
    metadata,
    Column("name", Text, primary_key=True),
    Column("options", JSON, nullable=False),  # columnDetail, edgeRouting, gridSnap, showOrphans
    Column("updated_by", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# A domain the layout shows: whether the focus names it, whether it is collapsed, and its box (the
# centre and size) as it was saved.
erd_layout_domains = Table(
    "erd_layout_domains",
    metadata,
    Column("layout_name", Text, ForeignKey("erd_layouts.name", ondelete="CASCADE"), nullable=False),
    Column("domain_id", Text, ForeignKey("domains.id", ondelete="CASCADE"), nullable=False),
    Column("selected", Boolean, nullable=False, server_default=false()),
    Column("collapsed", Boolean, nullable=False, server_default=false()),
    Column("x", Float),
    Column("y", Float),
    Column("w", Float),
    Column("h", Float),
    PrimaryKeyConstraint("layout_name", "domain_id"),
)

# A table the layout holds, by id so that a rename keeps its place: whether the focus names it,
# and its position; none for a table in a domain that was collapsed when it was saved.
erd_layout_tables = Table(
    "erd_layout_tables",
    metadata,
    Column("layout_name", Text, ForeignKey("erd_layouts.name", ondelete="CASCADE"), nullable=False),
    Column(
        "table_id", Integer, ForeignKey("registered_tables.id", ondelete="CASCADE"), nullable=False
    ),
    Column("selected", Boolean, nullable=False, server_default=false()),
    Column("x", Float),
    Column("y", Float),
    PrimaryKeyConstraint("layout_name", "table_id"),
)

# REQ-1939: how close a generated dataset came to the profiles it was drawn from.
synthetic_report = Table(
    "synthetic_report",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "dataset_id", Text, ForeignKey("synthetic_datasets.id", ondelete="CASCADE"), nullable=False
    ),
    Column("table_name", Text, nullable=False),
    Column("column_name", Text),
    Column("measure", Text, nullable=False),
    Column("source_value", Float),
    Column("synthetic_value", Float),
    Column("delta", Float),
    Column("note", Text),
)

# REQ-1742 gap: grpc_remote_router.py's _register_schema has always written its per-table
# registration log here via a raw INSERT (ON CONFLICT (source_id, table_name)) — but this table
# was never defined anywhere in the codebase (confirmed: no other file references
# "provisa_sources" at all), so every grpc_remote registration that reached this INSERT failed
# with "no such table: provisa_sources" before this was added. Nothing else in the codebase reads
# from it; it exists purely as this router's own registration record.
provisa_sources = Table(
    "provisa_sources",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source_id", Text, nullable=False),
    Column("source_type", Text, nullable=False),
    Column("table_name", Text, nullable=False),
    Column("column_defs", Text, nullable=False, server_default=""),
    Column("namespace", Text, nullable=False, server_default=""),
    Column("domain_id", Text, nullable=False, server_default=""),
    Column("extra", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("source_id", "table_name"),
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
    # REQ-1922: kept in the region's state store, apart from the model; no foreign key crosses
    # the two (integrity.remove_parts and the region's prune remove these with their owner).
    Column("table_id", Integer, primary_key=True),
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

# REQ-1914: the config stamp of this plane -- one row per kind ("model", "build", "settings"),
# advanced by trigger in the same transaction as any write to a table of that kind (see
# provisa/core/config_stamp.py). Every process reloads when the stored stamp differs from the one
# it loaded. V1: schema-defined, no migration.
config_stamp = Table(
    "config_stamp",
    metadata,
    Column("kind", Text, primary_key=True),
    Column("stamp", BigInteger, nullable=False),
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
    # A time-limited grant, its last use and the notices sent before it goes (auth.role_grants).
    # See the matching block in schema.sql.
    Column("expires_at", DateTime(timezone=True)),
    Column("last_used_at", DateTime(timezone=True)),
    Column("expiry_notified_at", DateTime(timezone=True)),
    Column("inactive_notified_at", DateTime(timezone=True)),
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
    Column("email_opt_in", Boolean, nullable=False, server_default=true()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-1382: attributes the org records for a member; where the identity provider supplies the
# same name, the org's record wins (provisa.auth.attributes). See schema.sql.
user_attributes = Table(
    "user_attributes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("value", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("user_id", "name"),
)

# REQ-1383: what each user's identity provider supplied at their last sign-in -- the scalar claims,
# by normalized name, so an administrator sees which attributes are the provider's and which the
# org's record overrides. Rewritten at sign-in when it changed; never administered.
user_idp_attributes = Table(
    "user_idp_attributes",
    metadata,
    Column("user_id", Text, primary_key=True),
    Column("name", Text, primary_key=True),
    Column("value", Text, nullable=False),
    Column("seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-2219 (IDENTITY PROVIDER ROLES ARE MIRRORED): the role claims each user's identity provider
# sent at their last sign-in, as (role, domain) pairs, where roles come from its claims. Rewritten
# at sign-in when they changed; never administered.
user_idp_roles = Table(
    "user_idp_roles",
    metadata,
    Column("user_id", Text, primary_key=True),
    Column("role_id", Text, primary_key=True),
    Column("domain_id", Text, primary_key=True),
    Column("seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# REQ-1330: who granted/revoked/verified consent to mail an address, and when. See the matching
# block in schema.sql.
email_send_authority_audit = Table(
    "email_send_authority_audit",
    metadata,
    Column("id", Uuid, primary_key=True, server_default=func.gen_random_uuid()),
    Column("email_address", Text, nullable=False),
    Column("user_id", Text),
    Column("granted_by", Text),
    Column("org_id", Text),
    Column("action", Text, nullable=False),
    CheckConstraint(
        "action IN ('granted', 'revoked', 'verified')",
        name="email_send_authority_audit_action_check",
    ),
    Column("reason", Text),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

node_ids = Table(
    "node_ids",
    metadata,
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    Column("composite_id", Text, nullable=False, unique=True),
    Column("label", Text, nullable=False),
    Column("properties", JSON, nullable=False, default=dict, server_default="{}"),
)

rel_ids = Table(
    "rel_ids",
    metadata,
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    Column("composite_id", Text, nullable=False, unique=True),
    Column("rel_type", Text, nullable=False),
    Column("properties", JSON, nullable=False, default=dict, server_default="{}"),
)

# Append-only SOC2 audit log (per-org). PG enforces immutability via CREATE RULE;
# on Tier-2 backends this is enforced by triggers/app-level (see Phase 3).
query_audit_log = Table(
    "query_audit_log",
    metadata,
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    # The tenant is the ORG, and an org id is text ("default", "kstott") — not a UUID. Typing this
    # column UUID meant the only value the audit pipeline could ever store was NULL.
    Column("tenant_id", Text),
    Column("user_id", Text, nullable=False),
    Column("role_id", Text, nullable=False),
    Column("query_hash", Text, nullable=False),
    # Encrypted query text (REQ-689) — decrypted only on authorised admin reads.
    Column("query_text_enc", LargeBinary),
    Column("table_ids", JSON, nullable=False, default=list, server_default="[]"),
    # Each of table_ids' tables by its qualified name (source.schema.table) as it was when the
    # statement ran, in the same order: a table renamed or deleted later still reads as what was
    # accessed. Written by every writer; no default.
    Column("table_names", JSON, nullable=False),
    Column("source", Text, nullable=False),
    Column("status_code", Integer, nullable=False),
    Column("duration_ms", Integer, nullable=False),
    # How the statement was answered — cache | direct | engine (api for an API-source GraphQL
    # field) — and the rows its terminal delivered. Both NULL for a refused statement.
    Column("route", Text),
    Column("row_count", Integer),
    # REQ-886: correlation id of the UDF invocation this row was written under, joining the
    # audit row back to the engine-side UDF trace. Null for non-UDF queries.
    Column("trace_id", Text),
    # Provenance (provisa/audit/provenance.py): the model stamp the statement was governed under
    # (REQ-1914); what was enforced on it (row filters with the names of the session variables
    # they read, masks by kind, row caps, a write's table and columns); why it took its route and
    # the sources it read; how old the rows it was answered with were (null: read live).
    Column("model_stamp", BigInteger),
    # The environment repository's commit (REQ-1524) the model at model_stamp equals — only when
    # that is proven; NULL otherwise, and then ``enforced`` lists the visible columns too.
    Column("model_commit", Text),
    Column("enforced", JSON(none_as_null=True)),
    Column("route_reason", Text),
    Column("sources", JSON(none_as_null=True)),
    Column("data_age", JSON(none_as_null=True)),
    # REQ-1922: the region whose data answered the statement — the answering node's, unless the
    # statement read only what another region keeps, in place there (its replicas, its views).
    Column("region", Text, nullable=False),
    # REQ-1163: the times the statement read bitemporal views as of (on their clock; a range as
    # ``start/end``) and as recorded at, ISO 8601; NULL where it read them now.
    Column("as_of", Text),
    Column("as_recorded", Text),
    Column("logged_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Service-level breaches (REQ-506). A row is opened when a measure goes past its limit and closed
# when it comes back; the per-query record the measures are computed from is query_audit_log.
sla_breach_log = Table(
    "sla_breach_log",
    metadata,
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    Column("tenant_id", Text),
    # "latency" (the p99 past sla.p99_limit_ms) or "availability" (below sla.availability_min_pct).
    Column("kind", Text, nullable=False),
    # The measure when the breach opened, and the latest while it lasts.
    Column("observed", Float, nullable=False),
    Column("last_observed", Float, nullable=False),
    # The limit in force when the breach opened.
    Column("limit_value", Float, nullable=False),
    Column("window_start", DateTime(timezone=True), nullable=False),
    Column("window_end", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("resolved_at", DateTime(timezone=True)),
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
    # REQ-1954: the heading the source lists the table under (a ServiceNow table's theme).
    Column("group_name", Text),
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
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    # the node/table this event is ABOUT (a data source table or an MV).
    Column("source_table", Text, nullable=False),
    # delta (upsert by PK) | append (insert) | replace (delete+insert) | warn (advise) | error (halt)
    Column("event_type", Text, nullable=False),
    # cursor / changed rows (bounded to jsonb; over the ceiling degrade to replace) / warn|error detail.
    Column("payload", JSON(none_as_null=True), default=dict, server_default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint(
        "event_type IN ('delta','append','replace','warn','error','quarantine','row_refresh')",  # REQ-1865
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

# REQ-1923: where the last completed read of a mail source reached, a token for each mailbox
# (``scope`` '') or each of its folders (``scope`` the folder's id). ``kind`` is 'probe' (what
# the read saw, kept only to ask "changed since?") or 'delta' (where an incremental read
# continues from). A token is opaque: compared, or handed back to the provider. Written when the
# build that read to it is recorded as completed (``federation.mail_sync_state``). V1:
# schema-defined, no migration.
mail_sync_state = Table(
    "mail_sync_state",
    metadata,
    Column("source_id", Text, primary_key=True),
    Column("account", Text, primary_key=True),
    Column("scope", Text, primary_key=True),
    Column("kind", Text, primary_key=True),
    Column("token", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

# REQ-2054: each trigger's last run -- a scheduled one, or one run on a table's change or event --
# keyed by the trigger, with when it ran, how it ended and, when it did not succeed, why. One row
# per trigger, replaced by each run. V1: schema-defined, no migration.
trigger_runs = Table(
    "trigger_runs",
    metadata,
    Column("trigger_id", Text, primary_key=True),
    Column("ran_at", DateTime(timezone=True), nullable=False),
    Column("outcome", Text, nullable=False),  # succeeded | refused | failed
    Column("error", Text),
    # What fired it: the table and event of an on-change or on-event run; None for a schedule.
    Column("fired_by", Text),
)

# REQ-1912 / REQ-826 replica state: one row per replica of a source table, shared by every worker
# and instance. ``promoted`` is the automatic-promotion decision (a Hot-N table past its
# threshold); the replica build's own state is added to this same row. Keyed by the table's
# registered identity, source first. V1: schema-defined, no migration.
replica_state = Table(
    "replica_state",
    metadata,
    Column("source_id", Text, primary_key=True),
    Column("schema_name", Text, primary_key=True),
    Column("table_name", Text, primary_key=True),
    Column("promoted", Boolean, nullable=False, server_default=false()),
    Column("promoted_at", DateTime(timezone=True)),
    # REQ-1915: the replica's build, one record every worker and instance reads. A replica
    # exists when ``completed_at`` is set, whatever ``build_state`` says: a refresh that is
    # requested, running or failed leaves the previous replica readable.
    # idle → requested → building → idle (completed) | failed.
    Column("build_state", Text, nullable=False, server_default="idle"),
    Column("requested_at", DateTime(timezone=True)),
    Column("requested_reason", Text),  # model | definition | hot | refresh | operator | read
    Column("build_started_at", DateTime(timezone=True)),
    Column("build_holder", Text),  # host:pid of the process building it
    Column("build_method", Text),  # data_replicator.Method
    Column("rows_copied", BigInteger),  # progress of the running build; the total once complete
    # The rows of the last build that copied the whole table, kept while the next build runs
    # (``rows_copied`` starts again from 0 then): what a running build's progress is measured
    # against. An estimate -- the table may have grown or shrunk since. NULL: no such build yet.
    Column("completed_rows", BigInteger),
    Column("completed_at", DateTime(timezone=True)),  # NULL: there is no replica
    Column("next_refresh_at", DateTime(timezone=True)),  # NULL: refreshed only on request
    Column("content_hash", Text),  # order-independent hash of the last completed build
    # The store the last completed build was written into (a digest of the engine and its
    # store's address). A replica recorded for another store is not one this engine can read.
    Column("built_store", Text),
    # What the last completed build was built from: a digest of the table's replica address,
    # columns, key and its source's connection identity, and the columns it has. A model that
    # no longer matches the digest asks for a rebuild; the columns say whether the standing
    # replica can still answer the model meanwhile.
    Column("definition_hash", Text),
    Column("built_columns", JSON(none_as_null=True)),
    # The model stamp (REQ-1914) of the process that asked for the build: a node whose loaded
    # model is older than this has not seen the table yet and must not retire its replica.
    Column("model_stamp", BigInteger),
    Column("load_kind", Text),  # data_replicator.TargetLoad of the running or last build
    # What a running build is waiting out, when it is: {"by", "seconds", "until"} -- its
    # source's service is throttling it and asked for that long. NULL: it is not waiting.
    Column("build_wait", JSON(none_as_null=True)),
    # How far a running build that lands a part at a time has got: {"done", "total", "unit"}
    # (mailboxes read to their end, of the mailboxes the source names). NULL: not such a build.
    Column("build_progress", JSON(none_as_null=True)),
    # Set when the model stops declaring this replica; it is dropped once every node has had
    # time to reload and every statement already addressed at it has ended. NULL: in use.
    Column("retired_at", DateTime(timezone=True)),
    Column("last_error", Text),
    # REQ-1350: the failure's stable code and params, when its cause is one Provisa names
    # (``replica_errors``); NULL for a driver's or a source's own error.
    Column("last_error_code", Text),
    Column("last_error_params", JSON(none_as_null=True)),
    # What the last COMPLETED build had to say of the copy it made: a list of
    # data_replicator.BuildNote as {"code", "params"} -- a code the UI words in its own
    # language, and its particulars. Empty: it had nothing to say; NULL: no build has
    # completed. Set by each completed build; a failed build leaves it as it was.
    Column("build_notes", JSON(none_as_null=True)),
    # When the table's probe last found the source unchanged since this copy was built
    # (REQ-855: an unchanged token keeps the rows). The copy is current as of then; cleared by
    # each completed build. NULL: not verified since the build.
    Column("verified_at", DateTime(timezone=True)),
    Column("failed_at", DateTime(timezone=True)),
    # Builds that have failed in a row since the last one that completed. A replica that keeps
    # failing is retried for as long as it is declared, after ``replication.retry_interval``
    # doubled for each of these up to ``replication.retry_interval_max`` (replica_state
    # .RetryPolicy): the wait is computed from this count and ``failed_at``, nothing else is stored.
    Column("failed_attempts", Integer, nullable=False, server_default="0"),
    # Why a requested build was not started on the last pass that tried it, as a code
    # (``replica_errors.WAITING``: the engine at its job cap, the source at its live-read
    # cap); NULL once it starts.
    Column("waiting_on", Text),
    # REQ-1861: a table that follows its source's change feed, while the feed's listener is
    # down: since when, and the server's own reason. NULL once the listener is watching again.
    Column("feed_down_since", DateTime(timezone=True)),
    Column("feed_error", Text),
    Column(
        "delta_cursor", JSON(none_as_null=True)
    ),  # REQ-874: the stored delta cursor (max cursor-field of the last applied delta)
    Column(
        "delta_skipped", Text
    ),  # REQ-874: why the last build was a whole rebuild, not a delta; NULL = delta applied
    # REQ-2015: the indexes of the standing replica, built after it lands: {"for": the landing
    # (completed_at) they belong to, "wanted": [{name, columns, primary}], "built": [name],
    # "failed": {name: {error, code, columns}}}. NULL: no landing has asked for any.
    Column("index_state", JSON(none_as_null=True)),
    CheckConstraint(
        "build_state IN ('idle','requested','building','failed')",
        name="replica_state_build_state_check",
    ),
)

# REQ-1926: STATE, not model -- the store schema each session holds its temporary tables in,
# with the last time the worker that owns the session said it is alive. It is how a worker finds
# the schemas a session that is gone left behind (its worker was killed, the store was
# unreachable as it closed), and drops them. Read and written only by
# ``provisa.pgwire.temp_keeper``; never copied to another environment (``env_classes``).
session_temp_schemas = Table(
    "session_temp_schemas",
    metadata,
    Column("store_schema", Text, primary_key=True),
    Column("session_id", Text, nullable=False),
    Column("beat_at", DateTime(timezone=True), nullable=False),
)

# REQ-2176: STATE, not model -- each principal's ephemeral graph session in this environment: what
# the session knows of the tables its store schema holds (each table's shape and serials), its
# generation and its ephemeral graph's index, read for each statement of the principal under a lock
# and written back when the statement changed it. ``roles``, ``assigned`` and ``rights`` are the
# rights it was made under: a session whose principal's rights changed is ended. Read and written
# only by ``provisa.api.graph_session``; never copied to another environment (``env_classes``).
graph_sessions = Table(
    "graph_sessions",
    metadata,
    Column("user_id", Text, primary_key=True),
    Column("generation", BigInteger, nullable=False),
    Column("stored", Boolean, nullable=False),  # its store schema exists
    Column("tables", JSON, nullable=False),  # [{name, columns, serials}]
    Column("graph", JSON, nullable=False),
    Column("roles", JSON, nullable=False),  # the role claims the principal held
    Column("assigned", JSON, nullable=False),  # of those, the ones the org assigned it
    Column("rights", Text, nullable=False),  # the fingerprint of its rights then
    Column("used_at", DateTime(timezone=True), nullable=False),
)

# The replica builds that ended: one row a build that completed or failed, appended as its
# replica_state record is written. Bounded per replica: its last REPLICA_BUILD_LOG_KEEP builds
# (federation/replica_state.py), pruned as each is written. V1: schema-defined, no migration.
replica_build_log = Table(
    "replica_build_log",
    metadata,
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    Column("source_id", Text, nullable=False),
    Column("schema_name", Text, nullable=False),
    Column("table_name", Text, nullable=False),
    Column("started_at", DateTime(timezone=True)),  # NULL: the build's start was not recorded
    Column("finished_at", DateTime(timezone=True), nullable=False),
    Column("outcome", Text, nullable=False),
    Column("method", Text),  # engine_statement | stream_batches | delta
    Column("rows_copied", BigInteger()),  # a completed build's rows
    Column("error_code", Text),  # a failed build's cause, when it is a named one
    Column("error_message", Text),  # a failed build's error, in English
    CheckConstraint("outcome IN ('completed','failed')", name="replica_build_log_outcome_check"),
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
# REQ-2105: what each code source's calls to Provisa's LLM service used and cost, one row a call;
# a source's monthly cap is read against the sum of its month's rows.
code_source_llm_usage = Table(
    "code_source_llm_usage",
    metadata,
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    Column("source_id", Text, nullable=False),
    Column("month", Text, nullable=False),  # YYYY-MM, UTC
    Column("model", Text, nullable=False),
    Column("input_tokens", BigInteger(), nullable=False, server_default="0"),
    Column("output_tokens", BigInteger(), nullable=False, server_default="0"),
    Column("cache_read_tokens", BigInteger(), nullable=False, server_default="0"),
    Column("cache_write_tokens", BigInteger(), nullable=False, server_default="0"),
    Column("cost_usd", Float, nullable=True),  # None: the model has no polly.model_prices entry
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)


# REQ-1174 (amended 2026-10-27): what each role's LLM calls used, counted once per role whoever acts
# as it: per-minute buckets kept an hour, per-hour a week, per-day 31 days (security.llm_budget).
role_llm_usage = Table(
    "role_llm_usage",
    metadata,
    Column("role_id", Text, primary_key=True),
    Column("grain", Text, primary_key=True),  # minute | hour | day
    Column("bucket", DateTime(timezone=True), primary_key=True),  # the bucket's start, UTC
    Column("tokens", BigInteger(), nullable=False, server_default="0"),
    Column("usd", Float, nullable=False, server_default="0"),
)


# REQ-2098: a code source's image builds: each one's state, what it made (a pushed image, or an
# archive kept on the node that built it), and why it failed.
code_source_builds = Table(
    "code_source_builds",
    metadata,
    Column("id", Text, primary_key=True),
    Column("source_id", Text, nullable=False),
    Column("code_hash", Text, nullable=False),
    Column("arch", Text, nullable=False),  # amd64 | arm64
    Column("registry_id", Text, nullable=True),  # pushed there; None: downloaded
    Column("status", Text, nullable=False),  # building | done | failed
    Column("image_ref", Text, nullable=True),
    Column("digest", Text, nullable=True),
    Column("archive_path", Text, nullable=True),
    Column("node", Text, nullable=True),  # the node that holds the archive
    Column("error", Text, nullable=True),
    Column("started_by", Text, nullable=True),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("finished_at", DateTime(timezone=True), nullable=True),
)


# REQ-2106: a code source's own cache, shared by every worker of it in every process; a value is
# JSON, gone at expires_at (None: kept until deleted or replaced).
code_source_cache = Table(
    "code_source_cache",
    metadata,
    Column("source_id", Text, primary_key=True),
    Column("key", Text, primary_key=True),
    Column("value", Text, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=True),
)

# REQ-2106: a code source's own vector collection, which its code builds and searches; never a
# governed table's data. ``vector`` is the embedding as a JSON array of floats.
code_source_vectors = Table(
    "code_source_vectors",
    metadata,
    Column("source_id", Text, primary_key=True),
    Column("item_id", Text, primary_key=True),
    Column("vector", Text, nullable=False),
    Column("text", Text, nullable=True),
    Column("item_metadata", Text, nullable=False, server_default="{}"),
)


# REQ-2185: the org's reference library -- documents its assistants draw on, each with a status
# (proposed or kept) and domains (a list, or "*" for the whole org, as a glossary term's). A
# document's content is held as its passages, each embedded by the org's vector model;
# ``vector`` is the embedding, a pgvector column of any dimension (provisa.core.pgvector), under one
# partial HNSW index per dimension in use (provisa.reference_library.store).
reference_documents = Table(
    "reference_documents",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", Text, nullable=False),
    Column("source_kind", Text, nullable=False),  # file | url
    Column("source", Text, nullable=False),  # the file's name, or the URL
    Column("media_type", Text, nullable=False),
    Column("content_sha256", Text, nullable=False),
    Column("status", Text, nullable=False),  # proposed | kept
    Column("embedding_model", Text, nullable=False),
    Column("added_by", Text, nullable=True),
    Column("added_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("refreshed_at", DateTime(timezone=True), nullable=True),
    Column("replaced_at", DateTime(timezone=True), nullable=True),
    # REQ-2185 (ADD NOW, INDEX IN THE BACKGROUND): the document's index status. A document exists
    # from the moment it is added; extraction and indexing run in a background job and report here.
    # The default is ready because a document written any other way has its passages already.
    Column("index_status", Text, nullable=False, server_default="ready"),  # indexing|ready|failed
    Column("index_error", Text, nullable=True),
    Column("index_step", Text, nullable=True),  # reading | indexing
    Column("index_done", Integer, nullable=True),
    Column("index_total", Integer, nullable=True),
    Column("index_launch", Text, nullable=True),
    Column("index_progress_at", DateTime(timezone=True), nullable=True),
    # The uploaded bytes, kept only until the document is ready, so a failed one can be retried:
    # the library keeps a document's passages, not its file.
    Column("pending_content", LargeBinary, nullable=True),
    # Why terms were not looked for once it was ready (no term-finding model), else null.
    Column("terms_note", Text, nullable=True),
    Column("tenant_id", Uuid),
    CheckConstraint("source_kind IN ('file', 'url')", name="reference_documents_kind_check"),
    CheckConstraint("status IN ('proposed', 'kept')", name="reference_documents_status_check"),
)

# REQ-2185: a library document's domains; "*" is the whole org and stands alone.
reference_document_domains = Table(
    "reference_document_domains",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "document_id",
        Integer,
        ForeignKey("reference_documents.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("domain_id", Text, nullable=False),
    Column("tenant_id", Uuid),
    UniqueConstraint("document_id", "domain_id"),
)

# REQ-2185: a library document's passages, in order, each with where it sits (a section heading,
# a page) and its embedding.
reference_passages = Table(
    "reference_passages",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "document_id",
        Integer,
        ForeignKey("reference_documents.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("ordinal", Integer, nullable=False),
    Column("section", Text, nullable=True),
    Column("page", Integer, nullable=True),
    Column("text", Text, nullable=False),
    Column("vector", Vector(), nullable=False),
    Column("tenant_id", Uuid),
    UniqueConstraint("document_id", "ordinal"),
)

# REQ-2208: the registry's tags on library documents and their sections, kept with the library --
# the environment's own, as the documents are -- rather than in tag_assignments, which travels with
# the model: a tag follows its object's environment class. The columns are a tag assignment's; a
# section is named by its heading, or by its page when it has none (a PDF's pages).
reference_document_tags = Table(
    "reference_document_tags",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("tag_id", Text, nullable=False),
    Column("base_tag_id", Text, nullable=False),  # REQ-1467: the parameter stripped
    Column("object_type", Text, nullable=False),
    Column(
        "document_id",
        Integer,
        ForeignKey("reference_documents.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("section_heading", Text),
    Column("section_page", Integer),
    Column("object_key", Text, nullable=False),
    Column("reason", Text),
    Column("expires_on", Text),
    Column("purpose", Text),  # REQ-2094: the rest of the assignment's why, as tag_assignments'
    Column("bases", JSON(none_as_null=True)),
    Column("review_on", Text),
    Column("decided_by", Text),
    Column("decided_at", DateTime(timezone=True)),
    Column("evidence", JSON(none_as_null=True)),
    Column("tenant_id", Uuid),
    UniqueConstraint("base_tag_id", "object_key"),
    CheckConstraint(
        "object_type IN ('document', 'section')",
        name="reference_document_tags_object_type_check",
    ),
)

# REQ-2208: citations of reference library documents and their sections, one shape for whatever
# cites the library: a tag's definition (citer_kind "tag", object_key "") and each assignment of a
# tag (citer_kind "tag_assignment", object_key the assignment's). The section is named by heading or
# page, the whole document by neither; cite_key is that name. text_sha256 is of the section's text
# when cited, so a revised section flags its citer for review (provisa.reference_library.citations).
# The document's title and source are kept, so a citation outlives the document.
reference_citations = Table(
    "reference_citations",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("citer_kind", Text, nullable=False),
    Column("tag_id", Text),  # the BASE tag id
    Column("object_key", Text, nullable=False, server_default=""),
    Column(
        "document_id",
        Integer,
        ForeignKey("reference_documents.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("document_title", Text, nullable=False),
    Column("document_source", Text, nullable=False),
    Column("section_heading", Text),
    Column("section_page", Integer),
    Column("cite_key", Text, nullable=False),
    Column("text_sha256", Text, nullable=False),
    Column("cited_by", Text),
    Column("cited_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("tenant_id", Uuid),
    UniqueConstraint("citer_kind", "tag_id", "object_key", "cite_key"),
    CheckConstraint(
        "citer_kind IN ('tag', 'tag_assignment')",
        name="reference_citations_citer_kind_check",
    ),
)

# REQ-2185 (GRAPH RETRIEVAL): each in-service glossary term's embedding -- its name and definition,
# embedded by the org's vector model -- that governed retrieval finds the terms nearest a question
# by. ``digest`` is of the model and the text embedded, so a term whose name or definition changed,
# or a change of model, is embedded again (provisa.reference_library.terms). ``vector`` is held as
# a passage's is, under one partial HNSW index per dimension in use.
glossary_term_vectors = Table(
    "glossary_term_vectors",
    metadata,
    Column(
        "term_id",
        Integer,
        ForeignKey("glossary_terms.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("embedding_model", Text, nullable=False),
    Column("digest", Text, nullable=False),
    Column("vector", Vector(), nullable=False),
    Column("tenant_id", Uuid),
)

# REQ-2179: the glossary's scans of reference library documents for terms. A scan keeps the
# document's title and source as they were, so its citations outlive the document (document_id is
# cleared when the document is removed, and the citations are then marked as citing a removed
# document). ``domains`` is the steward's working domains as a JSON list, given to the terms the
# scan's drafts become; ``summary`` a JSON object of what was read and what was left out, by why.
glossary_term_scans = Table(
    "glossary_term_scans",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "document_id",
        Integer,
        ForeignKey("reference_documents.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("document_title", Text, nullable=False),
    Column("document_source", Text, nullable=False),
    Column("domains", Text, nullable=False),
    Column("status", Text, nullable=False),  # running | done | failed
    Column("error", Text, nullable=True),
    Column("summary", Text, nullable=True),
    Column("started_by", Text, nullable=True),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("finished_at", DateTime(timezone=True), nullable=True),
    # What a running scan is doing: the model it asks, the step it is on (reading | merging), the
    # batch of the batches it reads, what the role's LLM budget held a call's output to, the launch
    # it runs in and when it last moved on (provisa/term_finding/queue.py).
    Column("model", Text, nullable=True),
    Column("step", Text, nullable=True),
    Column("batch", Integer, nullable=True),
    Column("batches", Integer, nullable=True),
    Column("budget_limit", Integer, nullable=True),
    Column("launch", Text, nullable=True),
    Column("progress_at", DateTime(timezone=True), nullable=True),
    # REQ-2202: a scan of a registered table, not a library document -- a JSON object naming its
    # domain, table, id column and text column; null for a document's scan.
    Column("source_table", Text, nullable=True),
    Column("tenant_id", Uuid),
    CheckConstraint(
        "status IN ('running', 'done', 'failed')", name="glossary_term_scans_status_check"
    ),
)

# REQ-2179: the review queue -- each candidate term a scan proposes, as a draft until the steward
# accepts, merges or rejects it. ``proposal`` is what the scan proposes: a new term, or a merge into
# the glossary term it duplicates or names again (match_term_id, match_kind). ``payload`` is a JSON
# object: the candidate's source passages, variants (each proposed as a synonym or not),
# conflicting explanations side by side, homonyms and every glossary term it matched. ``term_id``
# is the term an accepted draft became or a merged one was merged into.
glossary_term_drafts = Table(
    "glossary_term_drafts",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "scan_id",
        Integer,
        ForeignKey("glossary_term_scans.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("ordinal", Integer, nullable=False),
    Column("name", Text, nullable=False),
    Column("category", Text, nullable=False),
    Column("sense", Text, nullable=False),
    Column("definition", Text, nullable=True),
    Column("definition_kind", Text, nullable=False),  # explicit | synthesized | missing
    Column("proposal", Text, nullable=False),  # new | merge
    Column(
        "match_term_id",
        Integer,
        ForeignKey("glossary_terms.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("match_kind", Text, nullable=True),  # duplicate | synonym
    Column("payload", Text, nullable=False),
    Column("status", Text, nullable=False),  # draft | accepted | merged | rejected
    Column(
        "term_id",
        Integer,
        ForeignKey("glossary_terms.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("decided_by", Text, nullable=True),
    Column("decided_at", DateTime(timezone=True), nullable=True),
    Column("tenant_id", Uuid),
    UniqueConstraint("scan_id", "ordinal"),
    CheckConstraint(
        "definition_kind IN ('explicit', 'synthesized', 'missing')",
        name="glossary_term_drafts_definition_check",
    ),
    CheckConstraint("proposal IN ('new', 'merge')", name="glossary_term_drafts_proposal_check"),
    CheckConstraint(
        "status IN ('draft', 'accepted', 'merged', 'rejected')",
        name="glossary_term_drafts_status_check",
    ),
)

# REQ-2237: an ontology imported as an alignment (provisa/ontology_import, schema.sql). ``domains``
# is the steward's working domains as a JSON list, given to the terms the import creates;
# ``summary`` a JSON object of what the file held and what was left out, by why.
glossary_ontology_imports = Table(
    "glossary_ontology_imports",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", Text, nullable=False),
    Column("file_name", Text, nullable=False),
    Column("format", Text, nullable=False),  # ossie | turtle | xml | nt | json-ld | archive
    Column("domains", Text, nullable=False),
    Column("summary", Text, nullable=False),
    Column("imported_by", Text, nullable=True),
    Column("imported_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("applied_by", Text, nullable=True),
    Column("applied_at", DateTime(timezone=True), nullable=True),
    Column("tenant_id", Uuid),
    CheckConstraint(
        "format IN ('ossie', 'turtle', 'xml', 'nt', 'json-ld', 'archive')",
        name="glossary_ontology_imports_format_check",
    ),
)

# REQ-2237: each class, property, value type or concept an import holds. ``payload`` is a JSON
# object: the item's source keys (its IRIs or Ossie names, several when one name folds them) and
# its alternative labels. ``term_id`` is the term an applied item became or was aligned with.
glossary_ontology_items = Table(
    "glossary_ontology_items",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "import_id",
        Integer,
        ForeignKey("glossary_ontology_imports.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("ordinal", Integer, nullable=False),
    Column("name", Text, nullable=False),
    Column("kind", Text, nullable=False),  # class | property | value | concept | individual
    Column("definition", Text, nullable=True),
    Column("iri", Text, nullable=True),
    Column("payload", Text, nullable=False),
    Column("status", Text, nullable=False),  # pending | excluded | applied
    Column(
        "term_id",
        Integer,
        ForeignKey("glossary_terms.id", ondelete="SET NULL"),
        nullable=True,
    ),
    Column("tenant_id", Uuid),
    UniqueConstraint("import_id", "ordinal"),
    CheckConstraint(
        "kind IN ('class', 'property', 'value', 'concept', 'individual')",
        name="glossary_ontology_items_kind_check",
    ),
    CheckConstraint(
        "status IN ('pending', 'excluded', 'applied')",
        name="glossary_ontology_items_status_check",
    ),
)

# REQ-2237: an alignment between an imported item and a glossary term (``term_id``) or another
# item of the import (``other_item_id``): SAME_AS (the item is that term) or a relationship type,
# read from the item when ``item_is_source``. ``basis`` says why it was proposed (or that a steward
# added it); ``status`` is proposed until a steward approves or rejects it, and written once
# applied.
glossary_ontology_alignments = Table(
    "glossary_ontology_alignments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "import_id",
        Integer,
        ForeignKey("glossary_ontology_imports.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "item_id",
        Integer,
        ForeignKey("glossary_ontology_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "term_id",
        Integer,
        ForeignKey("glossary_terms.id", ondelete="CASCADE"),
        nullable=True,
    ),
    Column(
        "other_item_id",
        Integer,
        ForeignKey("glossary_ontology_items.id", ondelete="CASCADE"),
        nullable=True,
    ),
    Column("rel_type", Text, nullable=False),
    Column("item_is_source", Boolean, nullable=False),
    Column("basis", Text, nullable=False),
    Column("status", Text, nullable=False),  # proposed | approved | rejected | written
    Column("use_definition", Boolean, nullable=False, server_default=false()),
    Column("decided_by", Text, nullable=True),
    Column("decided_at", DateTime(timezone=True), nullable=True),
    Column("tenant_id", Uuid),
    CheckConstraint(
        "rel_type IN ('SAME_AS', 'SYNONYM_OF', 'KIND_OF', 'PART_OF', 'VALID_VALUE_OF', "
        "'RELATED_TO')",
        name="glossary_ontology_alignments_rel_type_check",
    ),
    CheckConstraint(
        "status IN ('proposed', 'approved', 'rejected', 'written')",
        name="glossary_ontology_alignments_status_check",
    ),
    CheckConstraint(
        "(term_id IS NULL) <> (other_item_id IS NULL)",
        name="glossary_ontology_alignments_end_check",
    ),
)


admin_audit_log = Table(
    "admin_audit_log",
    metadata,
    Column("id", BigInteger(), primary_key=True, autoincrement=True),
    Column("action", Text, nullable=False),
    Column("actor_id", Text, nullable=False),
    Column("subject_id", Text, nullable=False),
    Column("detail", JSON, nullable=False, default=dict, server_default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)


# REQ-2006: every source of the DDL that lands in an org's control-plane schema from code, by its
# path in this package: its tables (beside this metadata, which ddl_fingerprint compiles), views,
# functions and triggers. A demo template kept across a cloud patch is trusted only when the code
# that built it had the same text here (no migrations: one built by older code lacks what is new,
# e.g. relation "config_stamp" does not exist). A module that runs DDL is in this list or in the
# guard's list of DDL that never lands in an org's schema
# (tests/unit/test_demo_template_warm_start.py), so a new source cannot be missed.
ORG_SCHEMA_DDL_SOURCES: tuple[str, ...] = (
    "core/schema.sql",
    "core/db.py",
    "core/config_stamp.py",
    "core/env_changes.py",
    "core/env_copy.py",
    "audit/query_log.py",
    "mv/schema.sql",
    "api/_meta_views.py",
    "api/_ontology_views.py",
    "api/_ontology_quality_views.py",
    "api/_pg_catalog_views.py",
    "api/startup_seed.py",
    "grpc/field_numbering.py",
    "live/watermark.py",
    "profiler/schema.py",
    "profiler/carry.py",
    "dq/scan.py",
    "ingest/ddl.py",
    "federation/materialize_exec.py",
    "federation/query_residency.py",
    "federation/row_filter.py",
    "fakes/pg_functions.py",
    "transpiler/metadata_server_forms.py",
)


def ddl_sources() -> dict[str, bytes]:
    """The text of each of :data:`ORG_SCHEMA_DDL_SOURCES`, by its path."""
    from pathlib import Path

    package = Path(__file__).resolve().parents[1]
    return {path: (package / path).read_bytes() for path in ORG_SCHEMA_DDL_SOURCES}


def fingerprint_of(tables: MetaData, sources: dict[str, bytes]) -> str:
    """The first 16 hex of SHA-256 over an org schema's DDL: each of ``tables``' CREATE TABLE and
    CREATE INDEX, compiled for PostgreSQL in name order, and the text of each of ``sources`` (the
    modules and scripts that run the rest of it), in path order."""
    import hashlib

    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex, CreateTable

    dialect = postgresql.dialect()
    digest = hashlib.sha256()
    for name in sorted(tables.tables):
        table = tables.tables[name]
        digest.update(str(CreateTable(table).compile(dialect=dialect)).encode())
        for index in sorted(table.indexes, key=lambda i: str(i.name)):
            digest.update(str(CreateIndex(index).compile(dialect=dialect)).encode())
    for path in sorted(sources):
        digest.update(path.encode() + b"\0" + sources[path])
    return digest.hexdigest()[:16]


@functools.cache
def ddl_fingerprint() -> str:
    """REQ-2006: the shape of what this code creates in an org's schema (this metadata and
    :data:`ORG_SCHEMA_DDL_SOURCES`). There are no migrations: a demo template built by code of
    another shape keeps that code's tables, so its readiness record names this
    (demo_templates.readiness_version), and a cloud patch keeps only the templates recorded under
    the pushed code's (deploy-cloud.sh's reset_sandbox_state reads it from that code)."""
    return fingerprint_of(metadata, ddl_sources())
