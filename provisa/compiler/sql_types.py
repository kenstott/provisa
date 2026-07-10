# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compiler IR dataclasses for sql_gen (REQ-009, REQ-066).

Physical table/join metadata, compilation context, and compiled-query result.
Extracted from sql_gen.py; pure data structures, no compilation logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from provisa.compiler.introspect import ColumnMetadata


# Protocol mirroring schema_gen._TableInfo — avoids a circular import while
# giving pyright full attribute visibility on the visible-table objects.
class _TableInfoProto(Protocol):
    table_id: int
    field_name: str
    type_name: str
    domain_id: str
    source_id: str
    schema_name: str
    table_name: str
    visible_columns: list[dict]
    column_metadata: dict[str, ColumnMetadata]
    native_filter_columns: list[dict]
    alias: str | None


# --- Compilation context (built from SchemaInput alongside schema) ---


@dataclass(frozen=True)
class TableMeta:
    """Physical table metadata for a GraphQL root query field."""

    table_id: int
    field_name: str  # snake_case GraphQL field name
    type_name: str  # PascalCase GraphQL type name
    source_id: str
    catalog_name: str  # the engine catalog name (source_id with hyphens → underscores)
    schema_name: str
    table_name: str  # post-alias physical name (e.g. "registered_tables_meta")
    domain_id: str = ""  # semantic domain name (as JDBC clients see it)
    column_presets: list = field(default_factory=list)
    source_type: str = ""  # source type string (e.g. "iceberg", "postgresql") for time-travel
    original_table_name: str = ""  # pre-alias name (e.g. "registered_tables"); empty if no alias
    display_name: str = ""  # user-visible name (DB alias); empty → derived from field_name


@dataclass(frozen=True)
class JoinMeta:
    """Join metadata for a relationship field on a GraphQL type."""

    source_column: str
    target_column: str
    source_column_type: str  # the engine data type (e.g. "integer", "varchar")
    target_column_type: str  # the engine data type on target side
    target: TableMeta
    cardinality: str  # "many-to-one" or "one-to-many"
    cypher_alias: str | None = None  # Cypher rel type override (e.g. OPENED_BY)
    disable_cypher: bool = False  # when True, suppress this edge in the Cypher graph
    source_constant: int | str | None = (
        None  # when set, use as literal join value instead of source column
    )
    source_json_key: str | None = None  # when set, extract key from JSON object column via ->>'key'
    source_expr: str | None = (
        None  # when set, use as raw SQL expression; {alias} is replaced with the current alias
    )
    target_expr: str | None = (
        None  # when set, use as raw SQL expression on target side; {alias} replaced with join alias
    )
    default_limit: int | None = None  # when set, wrap join target in a LIMIT subquery
    child_src_val: str | None = (
        None  # when set, propagate as parent_src_val to child joins instead of sub_src
    )


@dataclass
class CompilationContext:
    """Maps GraphQL names to physical table/join metadata."""

    # Root query field_name → TableMeta
    tables: dict[str, TableMeta] = field(default_factory=dict)
    # (source_type_name, relationship_field_name) → JoinMeta
    joins: dict[tuple[str, str], JoinMeta] = field(default_factory=dict)
    # (table_id, graphql_field_name) → path expression (e.g. "payload.order_id")
    column_paths: dict[tuple[int, str], str] = field(default_factory=dict)
    # table_id → [(col_name, column_type)] for aggregate column metadata
    aggregate_columns: dict[int, list[tuple[str, str]]] = field(default_factory=dict)
    # table_id → user-designated PK column names (informational; empty = heuristic only)
    pk_columns: dict[int, list[str]] = field(default_factory=dict)
    # (table_id, gql_field_name) → physical_column_name (only when they differ)
    exposed_to_physical: dict[tuple[int, str], str] = field(default_factory=dict)
    # (table_id, physical_column_name) → sql_exposed_name (alias direct, or apply_sql_name(phys))
    physical_to_sql: dict[tuple[int, str], str] = field(default_factory=dict)
    # table_id → set of column names that require native API params (_nf_ prefix)
    native_filter_columns: dict[int, dict[str, str]] = field(default_factory=dict)
    # table_id → {virtual_col_name → literal_value}
    virtual_columns: dict[int, dict[str, str]] = field(default_factory=dict)
    # (table_id, col_name) pairs where the column is a GQL OBJECT stored as JSON
    gql_json_columns: set[tuple[int, str]] = field(default_factory=set)
    # (table_id, col_name) pairs where the GQL OBJECT type is itself a governed (registered) table —
    # these columns are excluded from HOT materialization and must not appear in domain UNION branches
    gql_governed_object_cols: set[tuple[int, str]] = field(default_factory=set)
    # table_name → {gql_field_name: gql_selection_string} for undeclared graphql_remote OBJECT fields
    gql_remote_extra_selections: dict[str, dict[str, str]] = field(default_factory=dict)


# --- Compiled query result ---


@dataclass(frozen=True)
class ColumnRef:
    """A column in the SELECT list with serialization metadata."""

    alias: str | None  # table alias ("t0") or None if no joins
    column: str  # physical column name
    field_name: str  # GraphQL field name
    nested_in: str | None  # relationship field name, or None for root
    cardinality: str | None = None  # "many-to-one", "one-to-many", or None for root
    is_agg: bool = False  # True when emitted as ARRAY_AGG correlated subquery


@dataclass
class CompiledQuery:
    """Result of compiling a single GraphQL root query field."""

    sql: str
    params: list
    root_field: str  # GraphQL root field name (alias if present, else schema name)
    columns: list[ColumnRef]
    sources: set[str]  # source_ids involved (for routing)
    canonical_field: str = ""  # original schema field name before alias substitution
    # Cursor pagination fields (connection queries only)
    is_connection: bool = False
    is_backward: bool = False
    sort_columns: list[str] = field(default_factory=list)
    page_size: int | None = None
    has_cursor: bool = False
    # Aggregate + nodes: plain SELECT for nodes field (issue #12)
    nodes_sql: str | None = None
    nodes_columns: list[ColumnRef] | None = None
    nodes_params: list = field(default_factory=list)
    # Alias for the "aggregate" response key (e.g. "derived: aggregate" → "derived")
    agg_alias: str = "aggregate"
    # Native filter args for API-routed sources (path/query params extracted from GQL args)
    api_args: dict = field(default_factory=dict)
    # Python-level row limit applied after grouping (used when LATERAL ops joins are present)
    result_limit: int | None = None
    # Aggregate MV routing fields (REQ-198/199)
    is_aggregate: bool = False
    agg_columns: list[str] = field(default_factory=list)
    table: str = ""
    filters: list[str] = field(default_factory=list)
    # Group-by query fields (REQ-654)
    is_group_by: bool = False
    group_by_columns: list[str] = field(default_factory=list)
    # table_name → {gql_field_name: gql_selection_string} for undeclared graphql_remote OBJECT fields
    gql_remote_extra_selections: dict[str, dict[str, str]] = field(default_factory=dict)

    def with_sql(self, new_sql: str) -> "CompiledQuery":
        """Return a copy of this CompiledQuery with sql replaced."""
        import dataclasses

        return dataclasses.replace(self, sql=new_sql)
