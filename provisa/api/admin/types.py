# Copyright (c) 2026 Kenneth Stott
# Canary: f60382a1-5df1-4ae3-aca5-e4e0d6139efc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Strawberry types mirroring Pydantic config models."""

from __future__ import annotations

import strawberry

# Requirements: REQ-012, REQ-013, REQ-019, REQ-040, REQ-041, REQ-042, REQ-135, REQ-247, REQ-252, REQ-262, REQ-402


@strawberry.type
class SourceCdcConfigType:  # REQ-824
    bootstrap_servers: str
    topic_prefix: str
    schema_registry_url: str | None = None
    consumer_group_id: str | None = (
        None  # REQ-931: None = inherit Provisa-level cdc_consumer_group_id
    )


@strawberry.type
class SourceType:  # REQ-012
    id: str
    type: str
    host: str
    port: int
    database: str
    username: str
    dialect: str
    cache_enabled: bool
    cache_ttl: int | None
    prefer_materialized: bool
    load_protected: bool = False  # REQ-1141: scheduled-refresh-only load protection
    off_peak_window: str | None = None  # REQ-1141: "HH:MM-HH:MM" maintenance window
    off_peak_tz: str = "UTC"  # REQ-1141: IANA zone for the window
    gql_naming_convention: str | None = None
    path: str | None = None
    allowed_domains: list[str] = strawberry.field(default_factory=list)
    description: str = ""
    mapping_json: str = "{}"
    change_signal: str = "ttl"  # REQ-929: source default change signal (inherited by its tables)
    cdc: SourceCdcConfigType | None = None  # REQ-824: source-level CDC transport


@strawberry.type
class DomainType:  # REQ-533
    id: str
    description: str
    graphql_alias: str | None = None


@strawberry.type
class ColumnPresetType:
    column: str
    source: str
    name: str | None
    value: str | None
    data_type: str | None = None


@strawberry.type
class UniqueConstraintType:  # REQ-1093
    name: str
    columns: list[str]


@strawberry.type
class LiveOutputConfigType:  # REQ-565
    type: str  # "sse" | "kafka"
    topic: str | None = None
    key_column: str | None = None
    bootstrap_servers: str | None = None


@strawberry.type
class LiveKafkaParamsType:  # REQ-813
    topic: str
    format: str = "json"
    key_column: str | None = None


@strawberry.type
class LiveDeliveryConfigType:  # REQ-565, REQ-813
    strategy: str = "poll"  # poll | native | debezium | kafka
    watermark_column: str | None = None
    poll_interval: int = 10
    kafka: LiveKafkaParamsType | None = None
    query_id: str | None = None
    outputs: list[LiveOutputConfigType] = strawberry.field(default_factory=list)


@strawberry.type
class RefreshPolicySummaryType:  # REQ-1143
    """Server-derived plain-English summary of a table's effective refresh/serving policy, computed
    per (source, engine) from the same resolution the planner uses. ``serving`` ∈
    live|scheduled|cache|frozen; ``warning`` is a non-null misconfiguration note."""

    text: str
    serving: str
    warning: str | None = None


@strawberry.type
class RegisteredTableType:  # REQ-013, REQ-014, REQ-016, REQ-135
    id: int
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    alias: str | None
    description: str | None
    cache_ttl: int | None
    prefer_materialized: bool | None
    load_protected: bool | None  # REQ-1141: NULL = inherit source
    off_peak_window: str | None  # REQ-1141: per-table window override
    off_peak_tz: str | None  # REQ-1141: per-table window zone override
    gql_naming_convention: str | None
    watermark_column: str | None
    columns: list[TableColumnType]
    column_presets: list[ColumnPresetType] = strawberry.field(default_factory=list)
    unique_constraints: list[UniqueConstraintType] = strawberry.field(
        default_factory=list
    )  # REQ-1093
    api_endpoint: str | None = None
    view_sql: str | None = None
    change_signal: str | None = None  # REQ-929: override source change signal; None = inherit
    probe_query: str | None = None  # REQ-929: source-native freshness probe
    probe_type: str | None = None  # REQ-982: input-probe method; None = resolve per source class
    materialize: bool = False
    mv_refresh_interval: int = 300
    mv_debounce_quiet: float = 0.0  # REQ-963: seconds of quiet before firing; 0 = real-time
    mv_debounce_max_delay: float = 5.0  # REQ-963: staleness cap under continuous churn
    mv_consistency: str = (
        "shared"  # REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    )
    mv_preprocess: str | None = None  # REQ-957: inline preprocess(rows, ctx) hook source
    mv_bitemporal_mode: str | None = None  # REQ-1162: None | "snapshot" | "delta"
    mv_bitemporal_key: list[str] = strawberry.field(default_factory=list)  # REQ-1162: business key
    mv_persist: str = "replace"  # REQ-965: replace | append | upsert
    mv_primary_key: list[str] = strawberry.field(default_factory=list)  # REQ-970: row identity
    mv_incremental: bool = False  # REQ-969: incremental maintenance
    data_product: bool = False
    enable_aggregates: bool = False
    enable_group_by: bool = False
    can_deploy_to_db: bool = False
    live: LiveDeliveryConfigType | None = None

    @strawberry.field
    async def refresh_policy_summary(self) -> RefreshPolicySummaryType | None:  # REQ-1143
        """The effective refresh/serving policy summary, DERIVED SERVER-SIDE from the same planner
        resolution (federate + resolve_refresh_policy) per (source, engine). Returns None when the
        federation engine is not yet available (startup); never re-derives the decision tree in the
        client. Resolved lazily — only clients that request the field pay for it."""
        from provisa.api.admin._refresh_summary import summarize_table_policy

        return await summarize_table_policy(self)


@strawberry.type
class TableColumnType:  # REQ-040, REQ-041, REQ-393, REQ-399
    id: int
    column_name: str
    visible_to: list[str]
    writable_by: list[str]
    unmasked_to: list[str]
    mask_type: str | None
    mask_pattern: str | None
    mask_replace: str | None
    mask_value: str | None
    mask_precision: str | None
    alias: str | None
    computed_sql_alias: str
    description: str | None
    data_type: str | None = None
    native_filter_type: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_alternate_key: bool = False
    scope: str = "domain"


@strawberry.type
class AvailableTableType:  # REQ-533
    name: str
    comment: str | None


@strawberry.type
class AvailableColumnType:  # REQ-533
    name: str
    data_type: str
    comment: str | None
    native_filter_type: str | None = None
    is_primary_key: bool = False


@strawberry.type
class RelationshipType:  # REQ-019, REQ-020, REQ-158, REQ-413
    id: str
    source_table_id: int
    target_table_id: int | None
    source_table_name: str
    source_domain_id: str
    target_table_name: str
    source_column: str
    target_column: str | None
    cardinality: str
    materialize: bool
    refresh_interval: int
    target_function_name: str | None
    function_arg: str | None
    alias: str | None = None
    graphql_alias: str | None = None
    computed_cypher_alias: str | None = None
    disable_cypher: bool = False

    @strawberry.field
    def auto_suggested(self) -> bool:
        return self.id.startswith("fk__")


@strawberry.type
class RoleType:  # REQ-042
    id: str
    capabilities: list[str]
    domain_access: list[str]


@strawberry.type
class RLSRuleType:  # REQ-041, REQ-402
    id: int
    table_id: int | None
    domain_id: str | None
    role_id: str
    filter_expr: str


# --- Input types for mutations ---


@strawberry.input
class SourceCdcConfigInput:  # REQ-824
    bootstrap_servers: str
    topic_prefix: str
    schema_registry_url: str | None = None
    consumer_group_id: str | None = (
        None  # REQ-931: None = inherit Provisa-level cdc_consumer_group_id
    )


@strawberry.input
class SourceInput:  # REQ-012
    id: str
    type: str
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""
    path: str | None = None
    description: str = ""
    allowed_domains: list[str] = strawberry.field(default_factory=list)
    mapping_json: str | None = None
    change_signal: str = "ttl"  # REQ-929: source default change signal
    load_protected: bool = False  # REQ-1141: scheduled-refresh-only load protection
    off_peak_window: str | None = None  # REQ-1141: "HH:MM-HH:MM" maintenance window
    off_peak_tz: str = "UTC"  # REQ-1141: IANA zone for the window
    cdc: SourceCdcConfigInput | None = None  # REQ-824: source-level CDC transport


@strawberry.input
class DomainInput:  # REQ-533
    id: str
    description: str = ""
    graphql_alias: str | None = None


@strawberry.input
class ColumnInput:  # REQ-040, REQ-041, REQ-393, REQ-399
    name: str
    visible_to: list[str]
    writable_by: list[str] = strawberry.field(default_factory=list)
    unmasked_to: list[str] = strawberry.field(default_factory=list)
    mask_type: str | None = None
    mask_pattern: str | None = None
    mask_replace: str | None = None
    mask_value: str | None = None
    mask_precision: str | None = None
    alias: str | None = None
    description: str | None = None
    # Canonical IR data-type (REQ-846) a steward assigned during schema discovery. Authoritative for
    # a manually-defined column (a non-SQL source the engine can't introspect); the landing write
    # face maps IR → the store's physical type. Null when the type is filled by introspection.
    data_type: str | None = None
    native_filter_type: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_alternate_key: bool = False
    scope: str = "domain"


@strawberry.input
class ColumnPresetInput:  # REQ-533
    column: str
    source: str
    name: str | None = None
    value: str | None = None
    data_type: str | None = None


@strawberry.input
class UniqueConstraintInput:  # REQ-1093
    name: str
    columns: list[str]


@strawberry.input
class LiveOutputConfigInput:  # REQ-565
    type: str  # "sse" | "kafka"
    topic: str | None = None
    key_column: str | None = None
    bootstrap_servers: str | None = None


@strawberry.input
class LiveKafkaParamsInput:  # REQ-813
    topic: str
    format: str = "json"
    key_column: str | None = None


@strawberry.input
class LiveDeliveryConfigInput:  # REQ-565, REQ-813
    strategy: str = "poll"  # poll | native | debezium | kafka
    watermark_column: str | None = None
    poll_interval: int = 10
    kafka: LiveKafkaParamsInput | None = None
    query_id: str | None = None
    outputs: list[LiveOutputConfigInput] = strawberry.field(default_factory=list)


@strawberry.input
class TableInput:  # REQ-013, REQ-016, REQ-133, REQ-135, REQ-252
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    columns: list[ColumnInput]
    alias: str | None = None
    description: str | None = None
    watermark_column: str | None = None
    column_presets: list[ColumnPresetInput] = strawberry.field(default_factory=list)
    unique_constraints: list[UniqueConstraintInput] = strawberry.field(
        default_factory=list
    )  # REQ-1093
    view_sql: str | None = None
    load_protected: bool | None = None  # REQ-1141: NULL = inherit source load protection
    off_peak_window: str | None = None  # REQ-1141: per-table "HH:MM-HH:MM" window override
    off_peak_tz: str | None = None  # REQ-1141: per-table window zone override
    change_signal: str | None = None  # REQ-929: override source change signal; None = inherit
    probe_query: str | None = None  # REQ-929: source-native freshness probe
    probe_type: str | None = None  # REQ-982: input-probe method; None = resolve per source class
    materialize: bool = False
    mv_refresh_interval: int = 300
    mv_debounce_quiet: float = 0.0  # REQ-963: seconds of quiet before firing; 0 = real-time
    mv_debounce_max_delay: float = 5.0  # REQ-963: staleness cap under continuous churn
    mv_consistency: str = (
        "shared"  # REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    )
    mv_preprocess: str | None = None  # REQ-957: inline preprocess(rows, ctx) hook source
    mv_bitemporal_mode: str | None = None  # REQ-1162: None | "snapshot" | "delta"
    mv_bitemporal_key: list[str] = strawberry.field(default_factory=list)  # REQ-1162: business key
    mv_persist: str = "replace"  # REQ-965: replace | append | upsert
    mv_primary_key: list[str] = strawberry.field(default_factory=list)  # REQ-970: row identity
    mv_incremental: bool = False  # REQ-969: incremental maintenance
    data_product: bool = False
    enable_aggregates: bool = False
    enable_group_by: bool = False
    discover: bool = False  # REQ-252: infer columns from the live NoSQL source at registration
    live: LiveDeliveryConfigInput | None = None  # REQ-565: live delivery config


@strawberry.input
class RelationshipInput:  # REQ-019, REQ-020, REQ-158, REQ-413
    id: str
    source_table_id: str  # table name (resolved to ID)
    target_table_id: str = ""  # empty for computed relationships
    source_column: str
    target_column: str = ""  # empty for computed relationships
    cardinality: str
    materialize: bool = False
    refresh_interval: int = 300
    target_function_name: str | None = None
    function_arg: str | None = None
    alias: str | None = None  # e.g. WORKS_FOR; unique per (source_table, alias)
    graphql_alias: str | None = None
    disable_cypher: bool = False  # when True, exclude from Cypher graph edges
    record_candidate: bool = False  # when True, also insert accepted relationship_candidates record


@strawberry.input
class RoleInput:  # REQ-042
    id: str
    capabilities: list[str]
    domain_access: list[str]


@strawberry.input
class RLSRuleInput:  # REQ-041, REQ-402
    table_id: str | None = None  # table name (resolved to ID); mutually exclusive with domain_id
    domain_id: str | None = None  # domain ID for domain-level rules
    role_id: str = ""
    filter_expr: str = ""


@strawberry.type
class MVType:  # REQ-135, REQ-158, REQ-159, REQ-160
    id: str
    source_tables: list[str]
    target_table: str
    refresh_interval: int
    enabled: bool
    status: str
    last_refresh_at: float | None
    row_count: int | None
    last_error: str | None


@strawberry.type
class CacheStatsType:
    total_keys: int
    hit_count: int
    miss_count: int
    store_type: str
    # Redis-only operational stats (None for the noop store, which exposes none of them).
    used_memory_bytes: int | None = None
    max_memory_bytes: int | None = None
    evicted_keys: int | None = None
    expired_keys: int | None = None
    connected_clients: int | None = None
    ops_per_sec: int | None = None


@strawberry.type
class CacheTableStatType:
    table_id: int
    cached_entries: int


@strawberry.type
class HotTableStatType:
    table_name: str
    catalog: str
    schema_name: str
    row_count: int
    is_api: bool
    loaded: bool


@strawberry.type
class MaterializeStoreInfoType:
    engine_name: str
    # None when the engine has no materialization store configured yet — the panel still shows the
    # engine + MV count; the UI renders storeRef only when present.
    store_ref: str | None
    mv_count: int


@strawberry.type
class ProtocolHealthType:
    """Liveness of a separate socket listener (gRPC, Arrow Flight, pgwire, bolt).

    ``status`` is one of "running" | "down" | "disabled". "disabled" means the protocol was
    never started (no bound port); "down"/"running" come from a TCP-connect liveness probe.
    """

    name: str
    status: str
    port: int | None


@strawberry.type
class SystemHealthType:
    engine_connected: bool
    engine_worker_count: int
    engine_active_workers: int
    metadata_pool_size: int  # tenant metadata-DB pool; -1 = pool doesn't track size
    metadata_pool_free: int
    metadata_dialect: str  # sqlalchemy dialect of the metadata DB (postgresql, sqlite, …)
    cache_mode: str  # "disabled" | "embedded" | "server"
    cache_connected: bool
    protocols: list[ProtocolHealthType]
    mv_refresh_loop_running: bool


@strawberry.type
class ScheduledTaskType:  # REQ-533
    id: str
    name: str
    cron_expression: str
    webhook_url: str | None
    kind: str  # REQ-1003: "webhook" | "sql"
    sql: str | None  # REQ-1003: SQL statement for a SQL trigger
    enabled: bool
    last_run_at: str | None
    next_run_at: str | None


@strawberry.type
class MutationResult:  # REQ-533
    success: bool
    message: str


# --- Compile / Submit types ---


@strawberry.type
class ColumnAliasType:
    field_name: str
    column: str


@strawberry.type
class EnforcementType:  # REQ-038, REQ-040, REQ-041, REQ-263
    rls_filters_applied: list[str]
    columns_excluded: list[str]
    schema_scope: str
    masking_applied: list[str]
    ceiling_applied: str | None
    route: str


@strawberry.type
class CompileQueryResult:  # REQ-262, REQ-263, REQ-267
    sql: str
    semantic_sql: str
    engine_sql: str | None
    direct_sql: str | None
    route: str
    route_reason: str
    sources: list[str]
    root_field: str
    canonical_field: str
    column_aliases: list[ColumnAliasType]
    enforcement: EnforcementType
    optimizations: list[str]
    warnings: list[str]
    compiled_cypher: str | None
    cypher_error: str | None


@strawberry.input
class CompileQueryInput:
    query: str
    role: str
    variables: strawberry.scalars.JSON | None = None
    flat_sql: bool = False
    flat_cypher: bool = False
    node_only_cypher: bool = False
