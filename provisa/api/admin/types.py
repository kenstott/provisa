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


@strawberry.type
class SourceType:
    id: str
    type: str
    host: str
    port: int
    database: str
    username: str
    dialect: str
    cache_enabled: bool
    cache_ttl: int | None
    naming_convention: str | None
    path: str | None


@strawberry.type
class DomainType:
    id: str
    description: str


@strawberry.type
class ColumnPresetType:
    column: str
    source: str
    name: str | None
    value: str | None


@strawberry.type
class RegisteredTableType:
    id: int
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    governance: str
    alias: str | None
    description: str | None
    cache_ttl: int | None
    naming_convention: str | None
    watermark_column: str | None
    columns: list[TableColumnType]
    column_presets: list[ColumnPresetType] = strawberry.field(default_factory=list)


@strawberry.type
class TableColumnType:
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
    description: str | None
    native_filter_type: str | None = None


@strawberry.type
class AvailableTableType:
    name: str
    comment: str | None


@strawberry.type
class AvailableColumnType:
    name: str
    data_type: str
    comment: str | None
    native_filter_type: str | None = None


@strawberry.type
class RelationshipType:
    id: str
    source_table_id: int
    target_table_id: int | None
    source_table_name: str
    target_table_name: str
    source_column: str
    target_column: str | None
    cardinality: str
    materialize: bool
    refresh_interval: int
    target_function_name: str | None
    function_arg: str | None
    alias: str | None


@strawberry.type
class RoleType:
    id: str
    capabilities: list[str]
    domain_access: list[str]


@strawberry.type
class RLSRuleType:
    id: int
    table_id: int
    role_id: str
    filter_expr: str


# --- Input types for mutations ---


@strawberry.input
class SourceInput:
    id: str
    type: str
    host: str = ""
    port: int = 0
    database: str = ""
    username: str = ""
    password: str = ""
    path: str | None = None


@strawberry.input
class DomainInput:
    id: str
    description: str = ""


@strawberry.input
class ColumnInput:
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
    native_filter_type: str | None = None


@strawberry.input
class ColumnPresetInput:
    column: str
    source: str
    name: str | None = None
    value: str | None = None


@strawberry.input
class TableInput:
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    governance: str
    columns: list[ColumnInput]
    alias: str | None = None
    description: str | None = None
    watermark_column: str | None = None
    column_presets: list[ColumnPresetInput] = strawberry.field(default_factory=list)


@strawberry.input
class RelationshipInput:
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


@strawberry.input
class RoleInput:
    id: str
    capabilities: list[str]
    domain_access: list[str]


@strawberry.input
class RLSRuleInput:
    table_id: str  # table name (resolved to ID)
    role_id: str
    filter_expr: str


@strawberry.type
class GovernedQueryType:
    id: int
    query_text: str
    compiled_sql: str
    status: str
    stable_id: str | None
    developer_id: str | None
    approved_by: str | None
    sink_topic: str | None
    sink_trigger: str | None
    sink_key_column: str | None
    business_purpose: str | None
    use_cases: str | None
    data_sensitivity: str | None
    refresh_frequency: str | None
    expected_row_count: str | None
    owner_team: str | None
    expiry_date: str | None
    visible_to: list[str]
    schedule_cron: str | None
    schedule_output_type: str | None
    schedule_output_format: str | None
    schedule_destination: str | None
    compiled_cypher: str | None


@strawberry.type
class MVType:
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


@strawberry.type
class SystemHealthType:
    trino_connected: bool
    trino_worker_count: int
    trino_active_workers: int
    pg_pool_size: int
    pg_pool_free: int
    cache_connected: bool
    flight_server_running: bool
    mv_refresh_loop_running: bool


@strawberry.type
class ScheduledTaskType:
    id: str
    name: str
    cron_expression: str
    webhook_url: str | None
    enabled: bool
    last_run_at: str | None
    next_run_at: str | None


@strawberry.type
class MutationResult:
    success: bool
    message: str


# --- Compile / Submit types ---

@strawberry.type
class ColumnAliasType:
    field_name: str
    column: str


@strawberry.type
class EnforcementType:
    rls_filters_applied: list[str]
    columns_excluded: list[str]
    schema_scope: str
    masking_applied: list[str]
    ceiling_applied: str | None
    route: str


@strawberry.type
class CompileQueryResult:
    sql: str
    semantic_sql: str
    trino_sql: str | None
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


@strawberry.input
class CompileQueryInput:
    query: str
    role: str
    variables: strawberry.scalars.JSON | None = None


@strawberry.type
class SubmitQueryResult:
    query_id: int
    operation_name: str
    message: str


@strawberry.input
class SinkInput:
    topic: str
    trigger: str = "change_event"
    key_column: str | None = None


@strawberry.input
class ScheduleInput:
    cron: str
    output_type: str | None = None
    output_format: str | None = None
    destination: str | None = None


@strawberry.input
class SubmitQueryInput:
    query: str
    role: str
    variables: strawberry.scalars.JSON | None = None
    compiled_cypher: str | None = None
    sink: SinkInput | None = None
    schedule: ScheduleInput | None = None
    business_purpose: str | None = None
    use_cases: str | None = None
    data_sensitivity: str | None = None
    refresh_frequency: str | None = None
    expected_row_count: str | None = None
    owner_team: str | None = None
    expiry_date: str | None = None
