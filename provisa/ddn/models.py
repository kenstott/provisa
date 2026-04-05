# Copyright (c) 2026 Kenneth Stott
# Canary: a51c8652-2f9f-45df-aa98-e04ec4793c27
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Intermediate data models for DDN (Hasura v3) HML metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DDNConnector:
    """A DataConnectorLink definition."""

    name: str
    subgraph: str = ""
    url: str = ""
    # Scalar type mappings from connector to GraphQL
    scalar_type_map: dict[str, str] = field(default_factory=dict)
    # Schema from introspection result
    schema_info: dict[str, Any] = field(default_factory=dict)


@dataclass
class DDNFieldMapping:
    """Maps a GraphQL field name to a physical column name."""

    graphql_field: str
    column: str


@dataclass
class DDNTypeMapping:
    """Connector type mapping for an ObjectType."""

    connector_name: str
    # Source type name in the connector (physical table name)
    source_type: str
    field_mappings: list[DDNFieldMapping] = field(default_factory=list)


@dataclass
class DDNObjectType:
    """An ObjectType definition (schema for a model)."""

    name: str
    subgraph: str = ""
    # field_name -> graphql_type
    fields: dict[str, str] = field(default_factory=dict)
    # Type mappings to data connector
    type_mappings: list[DDNTypeMapping] = field(default_factory=list)


@dataclass
class DDNModel:
    """A Model definition (exposes an ObjectType as queryable)."""

    name: str
    subgraph: str = ""
    object_type: str = ""
    # Source connector + collection
    connector_name: str = ""
    collection: str = ""
    # Filterable/orderable fields
    filter_expression_type: str | None = None
    order_by_expression: str | None = None
    # Aggregate expression
    aggregate_expression: str | None = None
    # GraphQL type/query naming
    graphql_type_name: str | None = None
    graphql_select_many: str | None = None
    graphql_select_unique: str | None = None


@dataclass
class DDNRelationship:
    """A Relationship definition (Object or Array)."""

    name: str
    subgraph: str = ""
    source_type: str = ""  # source ObjectType name
    target_model: str = ""  # target Model name
    rel_type: str = ""  # "Object" or "Array"
    # field_name -> target_field_name
    field_mapping: dict[str, str] = field(default_factory=dict)


@dataclass
class DDNTypePermission:
    """TypePermissions — controls which fields a role can see."""

    type_name: str
    subgraph: str = ""
    role: str = ""
    allowed_fields: list[str] = field(default_factory=list)


@dataclass
class DDNModelPermission:
    """ModelPermissions — controls row-level access for a role."""

    model_name: str
    subgraph: str = ""
    role: str = ""
    filter: dict[str, Any] = field(default_factory=dict)


@dataclass
class DDNAggregateExpression:
    """An AggregateExpression definition."""

    name: str
    subgraph: str = ""
    operand_type: str = ""
    # List of enabled aggregate functions (count, sum, avg, etc.)
    count_enabled: bool = False
    count_distinct: bool = False
    # field_name -> list of aggregate functions
    aggregatable_fields: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class DDNCommand:
    """A Command definition (function or procedure)."""

    name: str
    subgraph: str = ""
    connector_name: str = ""
    # "function" or "procedure"
    command_type: str = ""
    # Physical function/procedure name in connector
    source_name: str = ""
    # Return type
    return_type: str = ""
    # Arguments: name -> type
    arguments: dict[str, str] = field(default_factory=dict)
    # GraphQL root field name
    graphql_root_field: str | None = None


@dataclass
class DDNMetadata:
    """Aggregated DDN HML metadata from a project directory."""

    connectors: list[DDNConnector] = field(default_factory=list)
    object_types: list[DDNObjectType] = field(default_factory=list)
    models: list[DDNModel] = field(default_factory=list)
    relationships: list[DDNRelationship] = field(default_factory=list)
    type_permissions: list[DDNTypePermission] = field(default_factory=list)
    model_permissions: list[DDNModelPermission] = field(default_factory=list)
    aggregate_expressions: list[DDNAggregateExpression] = field(default_factory=list)
    commands: list[DDNCommand] = field(default_factory=list)
    subgraphs: set[str] = field(default_factory=set)
    # Kinds that were skipped
    skipped_kinds: dict[str, int] = field(default_factory=dict)
