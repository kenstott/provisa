# Copyright (c) 2025 Kenneth Stott
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
    alias: str | None
    description: str | None


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


@strawberry.input
class TableInput:
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    governance: str
    columns: list[ColumnInput]


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
class MutationResult:
    success: bool
    message: str
