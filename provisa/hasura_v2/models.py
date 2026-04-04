# Copyright (c) 2025 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Intermediate data models for Hasura v2 metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class HasuraPermission:
    role: str
    columns: list[str] = field(default_factory=list)
    filter: dict[str, Any] = field(default_factory=dict)
    allow_aggregations: bool = False
    check: dict[str, Any] = field(default_factory=dict)


@dataclass
class HasuraRelationship:
    name: str
    rel_type: str  # "object" or "array"
    remote_table: str
    remote_schema: str
    column_mapping: dict[str, str] = field(default_factory=dict)


@dataclass
class HasuraComputedField:
    name: str
    function_name: str
    function_schema: str = "public"
    table_argument: str | None = None


@dataclass
class HasuraEventTrigger:
    name: str
    table_name: str
    table_schema: str
    webhook: str
    operations: list[str] = field(default_factory=list)
    retry_conf: dict[str, Any] = field(default_factory=dict)


@dataclass
class HasuraTable:
    name: str
    schema_name: str = "public"
    custom_name: str | None = None
    custom_column_names: dict[str, str] = field(default_factory=dict)
    custom_root_fields: dict[str, str] = field(default_factory=dict)
    select_permissions: list[HasuraPermission] = field(default_factory=list)
    insert_permissions: list[HasuraPermission] = field(default_factory=list)
    update_permissions: list[HasuraPermission] = field(default_factory=list)
    delete_permissions: list[HasuraPermission] = field(default_factory=list)
    object_relationships: list[HasuraRelationship] = field(default_factory=list)
    array_relationships: list[HasuraRelationship] = field(default_factory=list)
    computed_fields: list[HasuraComputedField] = field(default_factory=list)
    event_triggers: list[HasuraEventTrigger] = field(default_factory=list)
    is_enum: bool = False
    configuration: dict[str, Any] = field(default_factory=dict)


@dataclass
class HasuraFunction:
    name: str
    schema_name: str = "public"
    volatility: str = "VOLATILE"
    exposed_as: str = "mutation"


@dataclass
class HasuraActionDefinition:
    kind: str = "synchronous"  # synchronous or asynchronous
    handler: str = ""
    action_type: str = "mutation"  # mutation or query
    arguments: list[dict[str, Any]] = field(default_factory=list)
    output_type: str = ""


@dataclass
class HasuraAction:
    name: str
    definition: HasuraActionDefinition = field(default_factory=HasuraActionDefinition)
    permissions: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class HasuraCronTrigger:
    name: str
    webhook: str
    schedule: str  # cron expression
    include_in_metadata: bool = True
    enabled: bool = True


@dataclass
class HasuraInheritedRole:
    role_name: str
    role_set: list[str] = field(default_factory=list)


@dataclass
class HasuraRemoteSchema:
    name: str
    definition: dict[str, Any] = field(default_factory=dict)


@dataclass
class HasuraSource:
    name: str
    kind: str = "postgres"
    connection_info: dict[str, Any] = field(default_factory=dict)
    tables: list[HasuraTable] = field(default_factory=list)
    functions: list[HasuraFunction] = field(default_factory=list)


@dataclass
class HasuraMetadata:
    version: int = 3
    sources: list[HasuraSource] = field(default_factory=list)
    actions: list[HasuraAction] = field(default_factory=list)
    cron_triggers: list[HasuraCronTrigger] = field(default_factory=list)
    inherited_roles: list[HasuraInheritedRole] = field(default_factory=list)
    remote_schemas: list[HasuraRemoteSchema] = field(default_factory=list)
