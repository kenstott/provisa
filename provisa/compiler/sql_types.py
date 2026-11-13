# Copyright (c) 2026 Kenneth Stott
# Canary: b33068e1-9915-4246-8b7a-99769cf3eea5
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

import functools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from provisa.compiler.native_nested import NestedColumns
    from provisa.compiler.native_subtype import SubtypeColumns
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
    metrics: list[dict]  # REQ-1319: role-visible single-table metrics for this table
    write_ops: frozenset[str]  # REQ-209: the operations the table's source takes


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
    source_type: str = ""  # source type string (e.g. "iceberg", "postgresql") for time-travel
    original_table_name: str = ""  # pre-alias name (e.g. "registered_tables"); empty if no alias
    display_name: str = ""  # user-visible name (DB alias); empty → derived from field_name


def extracts_paths(source_type: str, table: str) -> bool:  # REQ-151, REQ-152
    """Whether a path column of ``table``, on a source of ``source_type``, is served as the
    extraction from its JSON column (a SQL database) rather than by its name (a source whose
    connector resolves the path), as the type declares (``core.source_registry.PATH_COLUMNS``).
    A table whose source type is not known, or declares neither, cannot say which, and is
    refused."""
    from provisa.core.source_registry import PATHS_EXTRACTED, path_columns

    if not source_type:
        raise ValueError(
            f"table {table!r} registers a column at a path, and its source's type is not known: "
            "a path column is served by its name or extracted from its JSON column by the type"
        )
    served = path_columns(source_type)
    if served is None:
        raise ValueError(
            f"table {table!r} registers a column at a path, and its source's type "
            f"{source_type!r} serves no path column: its connector neither resolves the path nor "
            "holds the document in a JSON column it is extracted from"
        )
    return served == PATHS_EXTRACTED


def stored_path(column: str, path: str) -> tuple[str, list[str]]:  # REQ-151, REQ-152
    """The JSON column and the keys into it a path column registered as ``column`` at ``path``
    reads, on a SQL database: ``source.key...`` reads ``source``; a one-key path reads that key
    of the column itself (the column is the JSON column, exposed under its alias)."""
    first, *keys = path.split(".")
    return (column, [first]) if not keys else (first, keys)


def key_list(stored: str) -> tuple[str, ...]:
    """REQ-1586: split a stored key declaration into its ordered columns.

    A junction end and the relationship end it pairs with are each declared as a comma-separated
    ordered list, so a composite foreign key is mapped by listing its columns in order. A
    single-column end is the same declaration with one entry.
    """
    return tuple(part.strip() for part in stored.split(",") if part.strip())


@dataclass(frozen=True)
class JunctionMeta:
    """REQ-1586: the associative table a junction-backed relationship traverses through.

    ``source_columns``/``target_columns`` are the junction's own two foreign keys, each an ordered
    list of one or more columns, paired positionally against the relationship's own source and
    target key lists. ``type_column``/``type_value``
    discriminate a junction that carries several relationship types. ``attributes`` maps exposed
    property name to physical column for every junction column that is not one of those keys;
    those are the relationship's attributes.
    """

    table: TableMeta
    source_columns: tuple[str, ...]
    target_columns: tuple[str, ...]
    type_column: str | None = None
    type_value: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()
    #: The type column's exposed name: what a row's own type is read as (r.relType) where a hop
    #: reads every row of the junction, whatever its type.
    type_attribute: str | None = None
    # REQ-1586: which nomination names the exposed type — "column", "table" or "fixed".
    label_source: str = ""
    label_fixed: str = ""  # the cypher_alias, carried here so "fixed" can be resolved standalone


@dataclass(frozen=True)
class JoinMeta:
    """Join metadata for a relationship field on a GraphQL type."""

    source_column: str
    target_column: str
    source_column_type: str  # the engine data type (e.g. "integer", "varchar")
    target_column_type: str  # the engine data type on target side
    target: TableMeta
    cardinality: str  # "many-to-one", "one-to-many", or "one-to-one"
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
    via: "JunctionMeta | None" = None  # REQ-1586: set on a junction-backed edge, None on FK/PK


@dataclass
class CompilationContext:
    """Maps GraphQL names to physical table/join metadata."""

    # REQ-1942: why every statement compiled for this environment is refused (a Test (fake)
    # environment showing a sensitive column with no fake); None when it serves.
    refusal: str | None = None
    # REQ-2074: the role this context is built for, and per table the operations its source takes.
    # The schema offers a mutation only where both the source takes it and the role holds a grant
    # for it; these say which of the two an absent one lacks.
    role_id: str | None = None
    write_ops: dict[int, frozenset[str]] = field(default_factory=dict)
    # REQ-1942: per table with kept mutations (a Reversible environment), its change log's
    # address and primary key columns.
    kept: dict[int, tuple[tuple[str, ...], list[str]]] = field(default_factory=dict)

    # Root query field_name → TableMeta
    tables: dict[str, TableMeta] = field(default_factory=dict)
    # REQ-1921: every name a statement could give a draft table this role would see (field name,
    # table name, alias, schema.table) → the table's name. In no schema; refused as draft.
    draft_names: dict[str, str] = field(default_factory=dict)
    # REQ-1921: each of those names to the draft's registered table id, for the role that reads
    # a draft (A DRAFT IS SERVED TO ORG_ADMIN: admin/table_read.drafts_a_statement_names).
    draft_ids: dict[str, int] = field(default_factory=dict)
    # REQ-1132: each name of a system domain's table the role holds no capability for -> the domain.
    missing_capability: dict[str, str] = field(default_factory=dict)
    # REQ-1132: each name of a table the role can only list -> the table.
    listed_only: dict[str, str] = field(default_factory=dict)
    # (source_type_name, relationship_field_name) → JoinMeta
    joins: dict[tuple[str, str], JoinMeta] = field(default_factory=dict)
    # REQ-1908: (schema, physical table) lowercased → {column lowercased: (epoch unit, data_type)}
    # for every registered temporal column its source stores as an epoch number.
    epoch_columns: dict[tuple[str, str], dict[str, tuple[str, str]]] = field(default_factory=dict)
    # REQ-1965: (schema, physical table) lowercased → {column lowercased: its hidden attributes}
    # for every registered column that is natively an array, a struct or a map at its source.
    nested_columns: "NestedColumns" = field(default_factory=dict)
    # REQ-2171: (schema, physical table) lowercased → {column lowercased: its subtype} for every
    # registered column whose subtype has semantics of its own (an inet, a range, a geometry).
    subtype_columns: "SubtypeColumns" = field(default_factory=dict)
    # REQ-2171: the registered columns of a subtype held as text, read through its read form.
    read_columns: "SubtypeColumns" = field(default_factory=dict)
    # REQ-2171: the registered timestamptz columns, which to_json is told it reads.
    instant_columns: "SubtypeColumns" = field(default_factory=dict)
    # REQ-1915: the tables replicated row by row on the bound engine (flagged, and the engine
    # cannot attach their source). A to-one relationship into one compiles to a JOIN.
    row_level_tables: frozenset[int] = frozenset()
    # table_id → [(col_name, column_type)] for aggregate column metadata
    aggregate_columns: dict[int, list[tuple[str, str]]] = field(default_factory=dict)
    # table_id → user-designated PK column names (informational; empty = heuristic only)
    pk_columns: dict[int, list[str]] = field(default_factory=dict)
    # REQ-1093: table_id → declared UNIQUE constraints [(name, [col, ...]), ...] (source-introspected)
    unique_constraints: dict[int, list[tuple[str, list[str]]]] = field(default_factory=dict)
    # (table_id, gql_field_name) → physical_column_name (only when they differ)
    exposed_to_physical: dict[tuple[int, str], str] = field(default_factory=dict)
    # (table_id, physical_column_name) → sql_exposed_name (alias direct, or apply_sql_name(phys))
    physical_to_sql: dict[tuple[int, str], str] = field(default_factory=dict)
    # (table_id, physical_column_name) → the GraphQL field name the row type gives it (alias, or
    # the naming convention applied): the name an aggregate's result fields use too.
    physical_to_gql: dict[tuple[int, str], str] = field(default_factory=dict)
    # table_id → set of column names that require native API params (_nf_ prefix)
    native_filter_columns: dict[int, dict[str, str]] = field(default_factory=dict)
    # table_id → {virtual_col_name → literal_value}
    virtual_columns: dict[int, dict[str, str]] = field(default_factory=dict)
    # table_id → {virtual_col_name → (key column, {key value → literal})}: a virtual column whose
    # value differs per row, answered by the row's key -- meta.registered_tables' table_query_name,
    # each registration's name as queried, by table id.
    row_virtual_columns: dict[int, dict[str, tuple[str, dict[int, str]]]] = field(
        default_factory=dict
    )
    # REQ-1867: (table_id, physical column name lowercased) for every registered column typed json
    # or jsonb: the only columns whose text is written as the JSON it holds.
    json_columns: set[tuple[int, str]] = field(default_factory=set)
    # REQ-1867: (table_id, physical column name lowercased) for every registered column typed an
    # integer: written as a JSON number, by its type.
    integer_columns: set[tuple[int, str]] = field(default_factory=set)
    # (table_id, col_name) pairs where the column is a GQL OBJECT stored as JSON
    gql_json_columns: set[tuple[int, str]] = field(default_factory=set)
    # (table_id, col_name) pairs where the GQL OBJECT type is itself a governed (registered) table —
    # these columns are excluded from HOT materialization and must not appear in domain UNION branches
    gql_governed_object_cols: set[tuple[int, str]] = field(default_factory=set)
    # (table_id, col_name) pairs registered for a table but not held by the relation a statement
    # reads (graphql_remote.relation_columns): the SQL plane neither lists nor expands them
    columns_outside_relation: set[tuple[int, str]] = field(default_factory=set)
    # table_name → {gql_field_name: gql_selection_string} for undeclared graphql_remote OBJECT fields
    gql_remote_extra_selections: dict[str, dict[str, str]] = field(default_factory=dict)
    # REQ-1319: (table_id, metric_name) → inlined physical SQL aggregate expression, so the
    # aggregate compiler selects a metric exactly like a sum/avg column (same expansion the
    # raw-SQL pipeline applies via metric_expand).
    metric_exprs: dict[tuple[int, str], str] = field(default_factory=dict)


# --- Compiled query result ---


# Asked for every column of every table each role's context is built from: a type is answered
# from the memo.
@functools.lru_cache(maxsize=1024)
def is_json_type(data_type: str | None) -> bool:
    """REQ-1867: a column type whose values are JSON -- PostgreSQL's json and jsonb, as a
    registration or an engine names them."""
    return data_type is not None and data_type.strip().lower() in ("json", "jsonb")


#: REQ-1867: the integer types, as a registration or an engine (PostgreSQL, DuckDB, Trino) names
#: them. A DECIMAL with no fraction is not one: it is written as a DECIMAL is.
_INTEGER_TYPES = frozenset(
    {
        "int2", "int4", "int8", "int16", "int32", "int64", "int128",
        "smallint", "integer", "int", "bigint", "tinyint", "hugeint",
        "utinyint", "usmallint", "uinteger", "ubigint", "uhugeint",
        "smallserial", "serial", "bigserial", "serial2", "serial4", "serial8",
    }
)  # fmt: skip


@functools.lru_cache(maxsize=1024)
def is_integer_type(data_type: str | None) -> bool:
    """REQ-1867: a column type whose values are integers, written as JSON numbers by the type --
    not by how a reader represented the value (a DuckDB HUGEINT read through Arrow is a Decimal)."""
    return data_type is not None and data_type.strip().lower() in _INTEGER_TYPES


@dataclass(frozen=True)
class ColumnRef:
    """A column in the SELECT list with serialization metadata."""

    alias: str | None  # table alias ("t0") or None if no joins
    column: str  # physical column name
    field_name: str  # GraphQL field name
    nested_in: str | None  # relationship field name, or None for root
    cardinality: str | None = None  # "many-to-one", "one-to-many", "one-to-one", or None for root
    is_agg: bool = False  # True when emitted as ARRAY_AGG correlated subquery
    # REQ-1867: the value is JSON -- a column typed json or jsonb, or a relationship the statement
    # builds as one JSON value -- and is written as the object, array or scalar it holds. Any other
    # text is written as the string it is, even when it reads as JSON.
    json: bool = False
    # REQ-1867: the key paths ("items", "items.product") inside a JSON value whose strings are
    # JSON text the engine wrote: a nested relationship or declared object, which Trino's
    # json_format turns into text. Every other string inside the value is a string.
    json_text: frozenset[str] = frozenset()
    # REQ-1867: the column is typed an integer and is written as a JSON number, whatever a reader
    # represented its value as (a DuckDB HUGEINT read through Arrow is a Decimal).
    integer: bool = False


class StatementSources(set[str]):
    """The source ids a compiled statement reads — what routing takes — together with the
    registered tables it reads them through (REQ-826). The compiler adds a table where it emits
    one, so the statement's tables are known without a second reading of its SQL: the operator's
    floor and the replica refresh are judged by the tables a statement reads, never by its
    sources' other tables.

    A compile-time accumulator only: ``CompiledQuery`` carries a plain ``set`` of the source ids
    and a ``frozenset`` of the table ids, so a compiled query copies and pickles as plain data
    (a kept GraphQL plan is a deep copy of its compiled queries)."""

    def __init__(self, table: "TableMeta") -> None:
        super().__init__({table.source_id})
        self.table_ids: set[int] = {table.table_id}

    def add_table(self, table: "TableMeta") -> None:
        self.add(table.source_id)
        self.table_ids.add(table.table_id)

    def merge(self, other: "StatementSources") -> None:
        self.update(other)
        self.table_ids |= other.table_ids


@dataclass
class CompiledQuery:
    """Result of compiling a single GraphQL root query field."""

    sql: str
    params: list
    root_field: str  # GraphQL root field name (alias if present, else schema name)
    columns: list[ColumnRef]
    sources: set[str]  # source_ids involved (for routing)
    # REQ-826: the registered tables the statement reads (StatementSources.table_ids). Empty for
    # a statement that reads none (rewritten onto a materialized view, a Kafka window).
    table_ids: frozenset[int] = frozenset()
    canonical_field: str = ""  # original schema field name before alias substitution
    # Cursor pagination fields (connection queries only)
    is_connection: bool = False
    is_backward: bool = False
    sort_columns: list[str] = field(default_factory=list)
    page_size: int | None = None
    has_cursor: bool = False
    # An aggregate or group-by root that selects its nodes (issue #12): one statement, whose row
    # (each group's, under group-by) holds them as a column beside the aggregates
    # (aggregates._nodes_column).
    has_nodes: bool = False
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
