# Copyright (c) 2025 Kenneth Stott
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


@strawberry.type
class DomainType:
    id: str
    description: str


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
    columns: list[TableColumnType]


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


@strawberry.type
class AvailableTableType:
    name: str
    comment: str | None


@strawberry.type
class AvailableColumnType:
    name: str
    data_type: str
    comment: str | None


@strawberry.type
class RelationshipType:
    id: str
    source_table_id: int
    target_table_id: int
    source_table_name: str
    target_table_name: str
    source_column: str
    target_column: str
    cardinality: str
    materialize: bool
    refresh_interval: int


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
    host: str
    port: int
    database: str
    username: str
    password: str


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


@strawberry.input
class RelationshipInput:
    id: str
    source_table_id: str  # table name (resolved to ID)
    target_table_id: str
    source_column: str
    target_column: str
    cardinality: str
    materialize: bool = False
    refresh_interval: int = 300


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
class PersistedQueryType:
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


@strawberry.type
class MutationResult:
    success: bool
    message: str
