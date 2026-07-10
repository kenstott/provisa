# Copyright (c) 2026 Kenneth Stott
# Canary: fe7dee37-1a51-4599-a719-d5e9249736c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL schema-generation dataclasses (REQ-007, REQ-008).

SchemaInput (public generation input) and _TableInfo (internal resolved table).
Extracted from schema_gen.py.
"""

from dataclasses import dataclass, field

from graphql import GraphQLField

from provisa.compiler.introspect import ColumnMetadata


@dataclass
class SchemaInput:
    """All data needed to generate a GraphQL schema for one role."""

    tables: list[dict]  # from table_repo.list_all() — includes "columns" sub-list
    relationships: list[dict]  # from rel_repo.list_all()
    column_types: dict[int, list[ColumnMetadata]]  # table_id → the engine column metadata
    naming_rules: list[dict]  # [{pattern, replacement}]
    role: dict  # from role_repo.get()
    domains: list[dict]  # from domain_repo.list_all()
    source_types: dict[str, str] | None = None  # source_id → type (for mutation eligibility)
    source_catalogs: dict[str, str] | None = None  # source_id → the engine catalog name
    domain_prefix: bool = False  # prepend domain_id__ to all names
    physical_table_map: dict[str, str] | None = None  # virtual → physical table name
    relay_pagination: bool = False  # global opt-in for _connection fields
    functions: list[dict] = field(default_factory=list)  # tracked DB functions
    webhooks: list[dict] = field(default_factory=list)  # tracked webhooks
    enum_types: dict = field(default_factory=dict)  # pg_name → GraphQLEnumType (REQ-221)
    root_table_ids: set[int] | None = (
        None  # if set, only these tables get root query fields; others are type-defs only
    )
    gql_object_columns: dict[str, dict[str, list[str]]] = field(
        default_factory=dict
    )  # {table_name: {col_name: [sub_fields]}}
    governed_gql_types: set[str] = field(
        default_factory=set
    )  # GQL type names backed by governed tables
    gql_governed_object_cols: set[tuple[int, str]] = field(
        default_factory=set
    )  # (table_id, col_name) pairs where the GQL OBJECT type is a governed registered table


@dataclass
class _TableInfo:
    """Internal: resolved table info for schema generation."""

    table_id: int
    field_name: str  # snake_case GraphQL field name
    type_name: str  # PascalCase GraphQL type name
    domain_id: str
    source_id: str
    schema_name: str
    table_name: str  # original DB table name
    visible_columns: list[dict]  # [{column_name, visible_to, alias?, description?}]
    column_metadata: dict[str, ColumnMetadata]  # column_name → metadata
    native_filter_columns: list[dict] = field(
        default_factory=list
    )  # [{column_name, native_filter_type}]
    alias: str | None = None  # explicit GraphQL name override
    description: str | None = None  # GraphQL type/field description
    gql_convention_override: str | None = None  # None = use naming.active_gql_convention()
    relay_pagination: bool = False  # resolved relay flag for this table
    gql_fields: dict[str, GraphQLField] = field(default_factory=dict)
    enable_aggregates: bool = False  # REQ-653: table-level opt-in for _aggregate root field
    enable_group_by: bool = False  # REQ-653: table-level opt-in for _group_by root field
