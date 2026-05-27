# Copyright (c) 2026 Kenneth Stott
# Canary: a06089bd-b453-4daa-8692-7fcbd909b7b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Walk validated GraphQL AST → PG-style SQL (REQ-009, REQ-066).

Single SQL statement per query field. No resolver chain, no N+1.
Double-quoted identifiers, $1-style positional parameters.
Table aliases (t0, t1, ...) used when JOINs are present.
"""

from __future__ import annotations

import fnmatch as _fnmatch
import os as _os
import re as _re

# Hard cap on rows returned when the caller supplies no explicit LIMIT.
# Resolved at query time from state.server_limits; falls back to env var then 10000.
def _get_default_row_limit() -> int:
    try:
        from provisa.api.app import state
        return state.server_limits.get("default_row_limit", int(_os.environ.get("PROVISA_DEFAULT_ROW_LIMIT", "10000")))
    except Exception:
        return int(_os.environ.get("PROVISA_DEFAULT_ROW_LIMIT", "10000"))

from dataclasses import dataclass, field
from provisa.otel_compat import get_tracer as _get_tracer

_tracer = _get_tracer(__name__)

from graphql import (
    BooleanValueNode,
    DocumentNode,
    EnumValueNode,
    FieldNode,
    FloatValueNode,
    IntValueNode,
    ListValueNode,
    ObjectValueNode,
    OperationDefinitionNode,
    StringValueNode,
    VariableNode,
)

from provisa.compiler.naming import rel_field_name as _rel_field_name, source_to_catalog
from provisa.compiler.params import ParamCollector
from provisa.cache.warm_tables import QueryCounter
from provisa.core.models import TIME_TRAVEL_SOURCES

# Module-level query counter for warm-table tracking (REQ-AD5)
query_counter = QueryCounter()

_VIRTUAL_COLS = frozenset({"_name_", "_domain_"})


# --- Compilation context (built from SchemaInput alongside schema) ---


@dataclass(frozen=True)
class TableMeta:
    """Physical table metadata for a GraphQL root query field."""

    table_id: int
    field_name: str  # snake_case GraphQL field name
    type_name: str  # PascalCase GraphQL type name
    source_id: str
    catalog_name: str  # Trino catalog name (source_id with hyphens → underscores)
    schema_name: str
    table_name: str  # post-alias physical name (e.g. "registered_tables_meta")
    domain_id: str = ""  # semantic domain name (as JDBC clients see it)
    column_presets: list = field(default_factory=list)
    source_type: str = ""  # source type string (e.g. "iceberg", "postgresql") for time-travel
    original_table_name: str = ""  # pre-alias name (e.g. "registered_tables"); empty if no alias


@dataclass(frozen=True)
class JoinMeta:
    """Join metadata for a relationship field on a GraphQL type."""

    source_column: str
    target_column: str
    source_column_type: str  # Trino data type (e.g. "integer", "varchar")
    target_column_type: str  # Trino data type on target side
    target: TableMeta
    cardinality: str  # "many-to-one" or "one-to-many"
    cypher_alias: str | None = None  # Cypher rel type override (e.g. OPENED_BY)
    disable_cypher: bool = False  # when True, suppress this edge in the Cypher graph
    source_constant: int | str | None = None  # when set, use as literal join value instead of source column
    source_json_key: str | None = None  # when set, extract key from JSON object column via ->>'key'
    source_expr: str | None = None  # when set, use as raw SQL expression; {alias} is replaced with the current alias
    target_expr: str | None = None  # when set, use as raw SQL expression on target side; {alias} replaced with join alias
    default_limit: int | None = None  # when set, wrap join target in a LIMIT subquery
    child_src_val: str | None = None  # when set, propagate as parent_src_val to child joins instead of sub_src


@dataclass
class CompilationContext:
    """Maps GraphQL names to physical table/join metadata."""

    # Root query field_name → TableMeta
    tables: dict[str, TableMeta] = field(default_factory=dict)
    # (source_type_name, relationship_field_name) → JoinMeta
    joins: dict[tuple[str, str], JoinMeta] = field(default_factory=dict)
    # (table_id, graphql_field_name) → path expression (e.g. "payload.order_id")
    column_paths: dict[tuple[int, str], str] = field(default_factory=dict)
    # table_id → [(col_name, trino_type)] for aggregate column metadata
    aggregate_columns: dict[int, list[tuple[str, str]]] = field(default_factory=dict)
    # table_id → user-designated PK column names (informational; empty = heuristic only)
    pk_columns: dict[int, list[str]] = field(default_factory=dict)
    # (table_id, gql_field_name) → physical_column_name (only when they differ)
    gql_to_physical: dict[tuple[int, str], str] = field(default_factory=dict)
    # table_id → set of column names that require native API params (_nf_ prefix)
    native_filter_columns: dict[int, set[str]] = field(default_factory=dict)
    # table_id → {virtual_col_name → literal_value}
    virtual_columns: dict[int, dict[str, str]] = field(default_factory=dict)
    # (table_id, col_name) pairs where the column is a GQL OBJECT stored as JSON
    gql_json_columns: set[tuple[int, str]] = field(default_factory=set)


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


# --- Build CompilationContext from SchemaInput ---


def _lookup_column_type(
    si: object,
    table_id: int,
    column_name: str,
    catalog: str | None = None,
    schema: str | None = None,
    table: str | None = None,
) -> str:
    """Look up a column's Trino data type.

    Checks compile-time column_types first; falls back to schema_service
    live Trino query when catalog/schema/table are provided.
    """
    from provisa.compiler.schema_gen import SchemaInput
    assert isinstance(si, SchemaInput)
    col_metas = si.column_types.get(table_id, [])
    for meta in col_metas:
        if meta.column_name == column_name:
            return meta.data_type
    if catalog and schema and table:
        from provisa.compiler import schema_service
        return schema_service.get_column_type(catalog, schema, table, column_name)
    return "varchar"


def build_context(si: object) -> CompilationContext:
    """Build CompilationContext from a SchemaInput.

    This mirrors the logic in schema_gen._build_visible_tables and _assign_names
    to produce the same field_name/type_name mapping.
    """
    from provisa.compiler.naming import domain_gql_alias, apply_convention
    from provisa.compiler.schema_gen import SchemaInput, _build_visible_tables, _assign_names

    assert isinstance(si, SchemaInput)
    ctx = CompilationContext()

    tables = _build_visible_tables(si)
    if not tables:
        return ctx

    domain_alias_map = {
        d["id"]: domain_gql_alias(d["id"], d.get("graphql_alias"))
        for d in si.domains
        if domain_gql_alias(d["id"], d.get("graphql_alias"))
    }
    _assign_names(tables, si.naming_rules, domain_prefix=si.domain_prefix, domain_alias_map=domain_alias_map)

    table_lookup = {t.table_id: t for t in tables}

    physical_map = getattr(si, "physical_table_map", None) or {}
    table_preset_map = {td["id"]: list(td.get("column_presets") or []) for td in si.tables}

    for t in tables:
        physical_name = physical_map.get(t.table_name, t.table_name)
        src_type = (si.source_types or {}).get(t.source_id, "")
        meta = TableMeta(
            table_id=t.table_id,
            field_name=t.field_name,
            type_name=t.type_name,
            source_id=t.source_id,
            catalog_name=(si.source_catalogs or {}).get(t.source_id) or source_to_catalog(t.source_id),
            schema_name=t.schema_name,
            table_name=physical_name,
            domain_id=t.domain_id,
            column_presets=table_preset_map.get(t.table_id, []),
            source_type=src_type,
            original_table_name=t.table_name if physical_name != t.table_name else "",
        )
        ctx.tables[t.field_name] = meta
        from provisa.compiler.naming import domain_to_sql_name as _d2s
        _field_part = t.field_name.split("__", 1)[1] if "__" in t.field_name else t.field_name
        ctx.virtual_columns[t.table_id] = {
            "_name_": f"{_d2s(t.domain_id)}.{_field_part}",
            "_domain_": t.domain_id,
        }
        # Register aggregate variant pointing to same TableMeta
        ctx.tables[f"{t.field_name}_aggregate"] = meta
        # Register connection variant for cursor pagination
        ctx.tables[f"{t.field_name}_connection"] = meta

        # Store column metadata for aggregate compilation.
        # Exclude GQL object blob columns covered by a FK relationship — those columns
        # are not physical SQL columns in the materialized Trino table.
        _gql_obj_cols = si.gql_object_columns.get(t.table_name, {})
        _src_rels = [
            r for r in si.relationships
            if r.get("source_table_id") == t.table_id and r.get("source_column")
        ]
        _covered_blobs = {
            blob_col
            for blob_col in _gql_obj_cols
            if any(r["source_column"].lower().startswith(blob_col.lower()) for r in _src_rels)
        }
        col_info = []
        for col in t.visible_columns:
            col_name = col["column_name"]
            if col_name in _covered_blobs:
                continue
            col_meta = t.column_metadata.get(col_name.lower())
            if col_meta:
                col_info.append((col_name, col_meta.data_type))
        ctx.aggregate_columns[t.table_id] = col_info

        # Store user-designated PK columns
        ctx.pk_columns[t.table_id] = [
            col["column_name"] for col in t.visible_columns
            if col.get("is_primary_key")
        ]

        # Store native filter column names
        ctx.native_filter_columns[t.table_id] = {
            nfc["column_name"] for nfc in t.native_filter_columns
        }

        # Populate column paths for JSON extraction and gql→physical mapping
        convention = si.naming_convention
        for col in t.visible_columns:
            col_path = col.get("path")
            phys = col["column_name"]
            gql = col.get("alias") or apply_convention(phys, convention) or phys
            if gql != phys:
                ctx.gql_to_physical[(t.table_id, gql)] = phys
            if col_path:
                ctx.column_paths[(t.table_id, gql)] = col_path

        for col_name in (si.gql_object_columns.get(t.table_name) or {}):
            ctx.gql_json_columns.add((t.table_id, col_name))

    # Build join metadata from visible relationships
    for rel in si.relationships:
        src_id = rel["source_table_id"]
        tgt_id = rel["target_table_id"]
        if src_id not in table_lookup or tgt_id not in table_lookup:
            continue
        # Remote-managed relationships with no inferred FK columns cannot be SQL-joined
        if not rel.get("source_column") and not rel.get("target_function_name"):
            continue

        src_info = table_lookup[src_id]
        tgt_info = table_lookup[tgt_id]

        _tgt_physical_name = physical_map.get(tgt_info.table_name, tgt_info.table_name)
        tgt_meta = TableMeta(
            table_id=tgt_info.table_id,
            field_name=tgt_info.field_name,
            type_name=tgt_info.type_name,
            source_id=tgt_info.source_id,
            catalog_name=source_to_catalog(tgt_info.source_id),
            schema_name=tgt_info.schema_name,
            table_name=_tgt_physical_name,
            domain_id=tgt_info.domain_id,
            original_table_name=tgt_info.table_name if _tgt_physical_name != tgt_info.table_name else "",
        )

        # Look up column types for the join columns; fall back to schema_service on miss
        src_col_type = _lookup_column_type(
            si, src_id, rel["source_column"],
            catalog=source_to_catalog(src_info.source_id),
            schema=src_info.schema_name,
            table=src_info.table_name,
        )
        tgt_col_type = _lookup_column_type(
            si, tgt_id, rel["target_column"],
            catalog=source_to_catalog(tgt_info.source_id),
            schema=tgt_info.schema_name,
            table=tgt_info.table_name,
        )

        # The relationship field on the source type uses alias if set, else computed rel name
        join_field_name = rel.get("graphql_alias") or _rel_field_name(
            tgt_info.field_name, rel["cardinality"]
        )
        ctx.joins[(src_info.type_name, join_field_name)] = JoinMeta(
            source_column=rel["source_column"],
            target_column=rel["target_column"],
            source_column_type=src_col_type,
            target_column_type=tgt_col_type,
            target=tgt_meta,
            cardinality=rel["cardinality"],
            cypher_alias=rel.get("alias") or None,
            disable_cypher=rel.get("disable_cypher", False),
            source_json_key=rel.get("source_json_key") or None,
        )

    # Inject synthetic _meta join: every non-meta table → meta:registered_tables
    meta_rt = next(
        (t for t in tables if t.domain_id == "meta" and t.table_name == "registered_tables"),
        None,
    )
    if meta_rt:
        meta_tgt = TableMeta(
            table_id=meta_rt.table_id,
            field_name=meta_rt.field_name,
            type_name=meta_rt.type_name,
            source_id=meta_rt.source_id,
            catalog_name=(si.source_catalogs or {}).get(meta_rt.source_id) or source_to_catalog(meta_rt.source_id),
            schema_name=meta_rt.schema_name,
            table_name=physical_map.get(meta_rt.table_name, meta_rt.table_name),
            domain_id=meta_rt.domain_id,
        )
        for t in tables:
            if t.domain_id == "meta":
                continue
            ctx.joins[(t.type_name, "_meta")] = JoinMeta(
                source_column="__table_id__",
                target_column="id",
                source_column_type="text",
                target_column_type="text",
                target=meta_tgt,
                cardinality="many-to-one",
                cypher_alias="HAS_TABLE",
                source_constant=t.table_id,  # kept for Cypher path only (RelationshipMapping)
                source_expr=f"VARCHAR '{t.domain_id}.{t.table_name}'",  # stable SQL constant
                target_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
                child_src_val=f"VARCHAR '{t.domain_id}.{t.table_name}'",
            )

    # Inject synthetic traversal joins: registered_tables → ops tables
    # that carry a `table_name` FK column (e.g. traces, queries).
    _IMPLICIT_DOMAINS = {"meta", "ops"}
    _ops_targets: list[tuple[TableMeta, str]] = []  # (ops_tgt, _base)
    for ops_t in tables:
        if ops_t.domain_id not in _IMPLICIT_DOMAINS or ops_t.domain_id == "meta":
            continue
        if not any(c["column_name"] == "table_name" for c in ops_t.visible_columns):
            continue
        ops_tgt = TableMeta(
            table_id=ops_t.table_id,
            field_name=ops_t.field_name,
            type_name=ops_t.type_name,
            source_id=ops_t.source_id,
            catalog_name=(si.source_catalogs or {}).get(ops_t.source_id) or source_to_catalog(ops_t.source_id),
            schema_name=ops_t.schema_name,
            table_name=physical_map.get(ops_t.table_name, ops_t.table_name),
            domain_id=ops_t.domain_id,
        )
        _base = ops_t.field_name.split("__", 1)[1] if "__" in ops_t.field_name else ops_t.field_name
        _ops_targets.append((ops_tgt, _base))
        join_field = f"_{_base}"
        if meta_rt:
            ctx.joins[(meta_rt.type_name, join_field)] = JoinMeta(
                source_column="table_name",
                source_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
                target_column="table_name",
                target_expr='CONCAT({alias}."domain_id", \'.\', {alias}."table_name")',
                source_column_type="text",
                target_column_type="text",
                target=ops_tgt,
                cardinality="one-to-many",
                cypher_alias=f"HAS_{_base.upper()}",
                default_limit=10,
            )

    return ctx


# --- AST value extraction ---


def _extract_value(node: object, variables: dict | None) -> object:
    """Extract a Python value from a GraphQL AST value node."""
    if isinstance(node, StringValueNode):
        return node.value
    if isinstance(node, IntValueNode):
        return int(node.value)
    if isinstance(node, FloatValueNode):
        return float(node.value)
    if isinstance(node, BooleanValueNode):
        return node.value
    if isinstance(node, EnumValueNode):
        return node.value
    if isinstance(node, ListValueNode):
        return [_extract_value(v, variables) for v in node.values]
    if isinstance(node, ObjectValueNode):
        return {
            f.name.value: _extract_value(f.value, variables)
            for f in node.fields
        }
    if isinstance(node, VariableNode):
        var_name = node.name.value
        if variables and var_name in variables:
            return variables[var_name]
        raise ValueError(f"Variable ${var_name} not provided")
    raise ValueError(f"Unsupported value node type: {type(node).__name__}")


def _q(name: str) -> str:
    """Double-quote a SQL identifier."""
    return f'"{name}"'


# --- WHERE clause compilation ---


_ISO_DATE_RE = _re.compile(
    r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)


def _timestamp_literal_or_param(val: object, collector) -> str:
    """Return a TIMESTAMP literal if val is an ISO date, otherwise a parameter.

    Accepts: 2000-01-01, 2000-01-01T00:00:00, 2000-01-01 00:00:00,
             2000-01-01T00:00:00Z, 2000-01-01T00:00:00+05:30
    With timezone → TIMESTAMP '...' WITH TIME ZONE
    Without → TIMESTAMP '...'
    """
    if isinstance(val, str) and _ISO_DATE_RE.match(val):
        normalized = val.replace("T", " ")
        # Check for timezone suffix
        tz_match = _re.search(r"(Z|[+-]\d{2}:?\d{2})$", normalized)
        if tz_match:
            tz = tz_match.group(1)
            base = normalized[:tz_match.start()].strip()
            if tz == "Z":
                tz = "UTC"
            return f"TIMESTAMP '{base} {tz}'"
        return f"TIMESTAMP '{normalized}'"
    return collector.add(val)


def _compile_where(
    where_obj: dict,
    collector: ParamCollector,
    alias: str | None,
    virtual_vals: dict[str, str] | None = None,
) -> str:
    """Compile a where input object to a SQL WHERE clause fragment."""
    parts: list[str] = []

    for key, value in where_obj.items():
        if key == "_and":
            sub_parts = [_compile_where(sub, collector, alias, virtual_vals) for sub in value]
            parts.append(f"({' AND '.join(sub_parts)})")
            continue
        if key == "_or":
            sub_parts = [_compile_where(sub, collector, alias, virtual_vals) for sub in value]
            parts.append(f"({' OR '.join(sub_parts)})")
            continue

        # Virtual column: resolve at compile time — no physical column exists
        if key in _VIRTUAL_COLS and virtual_vals is not None:
            vv = virtual_vals.get(key, "")
            filter_obj = value
            for op, val in filter_obj.items():
                if op == "eq":
                    parts.append("TRUE" if vv == str(val) else "FALSE")
                elif op == "neq":
                    parts.append("TRUE" if vv != str(val) else "FALSE")
                elif op == "gt":
                    parts.append("TRUE" if vv > str(val) else "FALSE")
                elif op == "gte":
                    parts.append("TRUE" if vv >= str(val) else "FALSE")
                elif op == "lt":
                    parts.append("TRUE" if vv < str(val) else "FALSE")
                elif op == "lte":
                    parts.append("TRUE" if vv <= str(val) else "FALSE")
                elif op == "in":
                    parts.append("TRUE" if vv in [str(v) for v in val] else "FALSE")
                elif op == "like":
                    pattern = str(val).replace("%", "*").replace("_", "?")
                    parts.append("TRUE" if _fnmatch.fnmatch(vv, pattern) else "FALSE")
                elif op == "is_null":
                    parts.append("FALSE")
            continue

        # Column filter: key is column name, value is filter object
        col = _q(key) if alias is None else f"{_q(alias)}.{_q(key)}"
        filter_obj = value
        for op, val in filter_obj.items():
            if op == "eq":
                rhs = _timestamp_literal_or_param(val, collector)
                parts.append(f"{col} = {rhs}")
            elif op == "neq":
                rhs = _timestamp_literal_or_param(val, collector)
                parts.append(f"{col} != {rhs}")
            elif op == "gt":
                rhs = _timestamp_literal_or_param(val, collector)
                parts.append(f"{col} > {rhs}")
            elif op == "gte":
                rhs = _timestamp_literal_or_param(val, collector)
                parts.append(f"{col} >= {rhs}")
            elif op == "lt":
                rhs = _timestamp_literal_or_param(val, collector)
                parts.append(f"{col} < {rhs}")
            elif op == "lte":
                rhs = _timestamp_literal_or_param(val, collector)
                parts.append(f"{col} <= {rhs}")
            elif op == "in":
                placeholders = [collector.add(v) for v in val]
                parts.append(f"{col} IN ({', '.join(placeholders)})")
            elif op == "like":
                placeholder = collector.add(val)
                parts.append(f"{col} LIKE {placeholder}")
            elif op == "is_null":
                if val:
                    parts.append(f"{col} IS NULL")
                else:
                    parts.append(f"{col} IS NOT NULL")

    return " AND ".join(parts) if parts else "TRUE"


# --- ORDER BY compilation ---


_DIRECTION_SQL = {
    "asc": "ASC",
    "desc": "DESC",
    "asc_nulls_first": "ASC NULLS FIRST",
    "asc_nulls_last": "ASC NULLS LAST",
    "desc_nulls_first": "DESC NULLS FIRST",
    "desc_nulls_last": "DESC NULLS LAST",
}


def _compile_order_by(
    order_by_list: list[dict],
    alias: str | None,
) -> str:
    """Compile order_by input list to SQL ORDER BY clause.

    Hasura v2 format: each item is {column_name: direction} where direction
    is one of: asc, desc, asc_nulls_first, asc_nulls_last, desc_nulls_first,
    desc_nulls_last.
    """
    parts: list[str] = []
    for item in order_by_list:
        for col_name, direction in item.items():
            sql_dir = _DIRECTION_SQL.get(direction)
            if sql_dir is None:
                raise ValueError(f"Unknown order direction: {direction!r}")
            col = _q(col_name) if alias is None else f"{_q(alias)}.{_q(col_name)}"
            parts.append(f"{col} {sql_dir}")
    return ", ".join(parts)


# --- Type coercion for cross-source JOINs ---

_NUMERIC_TYPES = {"tinyint", "smallint", "integer", "int", "bigint", "real", "double", "decimal", "numeric"}
_STRING_TYPES = {"varchar", "char", "text", "varbinary", "uuid"}
_TEMPORAL_TYPES = {"date", "time", "timestamp", "time with time zone", "timestamp with time zone"}


def _base_type(trino_type: str) -> str:
    """Normalize parameterized types: varchar(100) → varchar, decimal(10,2) → decimal."""
    return trino_type.lower().split("(")[0].strip()


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Check if two Trino types are implicitly coercible (no CAST needed)."""
    a, b = _base_type(type_a), _base_type(type_b)
    if a == b:
        return True
    for group in (_NUMERIC_TYPES, _STRING_TYPES, _TEMPORAL_TYPES):
        if a in group and b in group:
            return True
    return False


def _common_cast_type(type_a: str, type_b: str) -> str:
    """Pick a common type to CAST both sides to when types are incompatible."""
    a, b = _base_type(type_a), _base_type(type_b)
    # If one side is string, cast the other to VARCHAR
    if a in _STRING_TYPES:
        return "VARCHAR"
    if b in _STRING_TYPES:
        return "VARCHAR"
    # Numeric vs temporal — use VARCHAR as safe fallback
    return "VARCHAR"


def _sql_str_literal(val: str) -> str:
    """Escape and quote a string as a SQL VARCHAR literal."""
    return "VARCHAR '" + val.replace("'", "''") + "'"


def _join_column_expr_for(
    alias: str | None, column: str, my_type: str, other_type: str
) -> str:
    """Build a column expression, adding CAST only when types are incompatible."""
    col = _q(column) if alias is None else f'{_q(alias)}.{_q(column)}'
    if _types_compatible(my_type, other_type):
        return col
    cast_type = _common_cast_type(my_type, other_type)
    return f'CAST({col} AS {cast_type})'


def _join_column_expr(alias: str, column: str, my_type: str, other_type: str) -> str:
    return _join_column_expr_for(alias, column, my_type, other_type)


# --- Table reference helpers ---


def _table_ref(meta: TableMeta, use_catalog: bool) -> str:
    """Build a fully qualified table reference."""
    if use_catalog:
        return f'{_q(meta.catalog_name)}.{_q(meta.schema_name)}.{_q(meta.table_name)}'
    return f'{_q(meta.schema_name)}.{_q(meta.table_name)}'


def _semantic_table_ref(meta: TableMeta) -> str:
    """Semantic table reference: domain_schema.table_name (as JDBC clients see it)."""
    from provisa.compiler.naming import domain_to_sql_name
    # Strip domain prefix (e.g. "ci__") from field_name — the domain is already the schema name
    table = meta.field_name.split("__", 1)[1] if "__" in meta.field_name else meta.field_name
    return f'{_q(domain_to_sql_name(meta.domain_id))}.{_q(table)}'


def _apply_replacements(sql: str, replacements: dict[str, str]) -> str:
    """Apply replacements to sql, longest match first (single-pass, no substring clobbering)."""
    if not replacements:
        return sql
    keys_sorted = sorted(replacements, key=len, reverse=True)
    import re as _re

    pattern = _re.compile("|".join(_re.escape(k) for k in keys_sorted))
    return pattern.sub(lambda m: replacements[m.group(0)], sql)


def make_semantic_sql(sql: str, ctx: CompilationContext) -> str:
    """Replace physical table refs with semantic (domain.field_name) refs."""
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        replacements[_table_ref(meta, use_catalog=False)] = _semantic_table_ref(meta)
        replacements[_table_ref(meta, use_catalog=True)] = _semantic_table_ref(meta)
    return _apply_replacements(sql, replacements)


def normalize_table_refs(sql: str, ctx: CompilationContext) -> str:
    """Qualify and quote all table references using CompilationContext.

    For each exp.Table node in the parsed SQL:
    - Already schema-qualified (schema.table or "schema"."table") → re-emit quoted.
    - Unqualified (table only) + unique match in ctx → add schema + quote.
    - Unqualified + ambiguous (multiple schemas) → leave unchanged.
    - Unqualified + no match → leave unchanged (governance will reject).
    """
    import sqlglot
    import sqlglot.expressions as exp

    # Build lookup structures from ctx
    # table_name (lower) → list of (schema_name, table_name) physical pairs
    by_name: dict[str, list[tuple[str, str]]] = {}
    # (schema_lower, name_lower) → (schema_name, table_name) canonical pair
    by_schema_name: dict[tuple[str, str], tuple[str, str]] = {}

    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        nl = meta.table_name.lower()
        sl = meta.schema_name.lower()
        by_name.setdefault(nl, []).append((meta.schema_name, meta.table_name))
        by_schema_name[(sl, nl)] = (meta.schema_name, meta.table_name)
        # Also map pre-alias name → physical name (e.g. "registered_tables" → "registered_tables_meta")
        if meta.original_table_name:
            orig_nl = meta.original_table_name.lower()
            by_name.setdefault(orig_nl, []).append((meta.schema_name, meta.table_name))
            by_schema_name[(sl, orig_nl)] = (meta.schema_name, meta.table_name)
        # Map domain-name schema variant → physical (e.g. "shelter"."shelter__animal_breeds" → "graphql_remote"."shelter__animal_breeds")
        if meta.domain_id:
            from provisa.compiler.naming import domain_to_sql_name
            domain_sl = domain_to_sql_name(meta.domain_id).lower()
            if domain_sl != sl:
                by_schema_name[(domain_sl, nl)] = (meta.schema_name, meta.table_name)
                if meta.original_table_name:
                    by_schema_name[(domain_sl, orig_nl)] = (meta.schema_name, meta.table_name)

    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql

    def _rewrite(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if not isinstance(node, exp.Table):
            return node
        name = node.name
        db = node.db  # schema
        alias = node.alias

        if db:
            # Already schema-qualified — just ensure quoting
            canonical = by_schema_name.get((db.lower(), name.lower()))
            if canonical:
                schema_q, table_q = canonical
            else:
                schema_q, table_q = db, name
        else:
            # Unqualified — try unique match
            matches = by_name.get(name.lower(), [])
            if len(matches) == 1:
                schema_q, table_q = matches[0]
            else:
                return node  # ambiguous or unknown — leave unchanged

        new_tbl = exp.Table(
            this=exp.Identifier(this=table_q, quoted=True),
            db=exp.Identifier(this=schema_q, quoted=True),
            alias=exp.TableAlias(this=exp.Identifier(this=alias)) if alias else None,
        )
        return new_tbl

    tree = tree.transform(_rewrite)
    return tree.sql(dialect="postgres")


def rewrite_semantic_to_physical(sql: str, ctx: CompilationContext) -> str:
    """Replace semantic (domain.field_name) refs with physical (schema.table) refs."""
    from provisa.compiler.naming import domain_to_sql_name, to_snake_case
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        semantic = _semantic_table_ref(meta)
        physical = _table_ref(meta, use_catalog=False)
        replacements[semantic] = physical
        # Also handle snake_case variant of the semantic ref (e.g. "meta"."registered_tables")
        # _semantic_table_ref strips the domain prefix then keeps camelCase; users may write snake_case
        if "__" in meta.field_name:
            table_part = meta.field_name.split("__", 1)[1]
        else:
            table_part = meta.field_name
        snake_part = to_snake_case(table_part)
        if snake_part != table_part:
            domain_sql = domain_to_sql_name(meta.domain_id)
            replacements[f'{_q(domain_sql)}.{_q(snake_part)}'] = physical
    sql = _apply_replacements(sql, replacements)
    return normalize_table_refs(sql, ctx)


def _all_table_metas(ctx: CompilationContext) -> list[TableMeta]:
    """Return all TableMeta instances from ctx: root tables plus all join targets."""
    metas: list[TableMeta] = list(ctx.tables.values())
    for jm in ctx.joins.values():
        metas.append(jm.target)
    return metas


def rewrite_semantic_to_trino_physical(sql: str, ctx: CompilationContext) -> str:
    """Replace semantic and physical table refs with Trino catalog-qualified refs.

    Handles both semantic refs (domain.field_name, produced by make_semantic_sql for root
    tables) and physical refs without catalog (schema.table, left by make_semantic_sql for
    join targets that are not in ctx.tables).
    """
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str, str]] = set()
    for meta in _all_table_metas(ctx):
        key = (meta.catalog_name, meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        semantic = _semantic_table_ref(meta)
        physical_no_catalog = _table_ref(meta, use_catalog=False)
        physical_with_catalog = _table_ref(meta, use_catalog=True)
        replacements[semantic] = physical_with_catalog
        # Also replace bare physical refs (e.g. "signals"."queries") that make_semantic_sql
        # did not convert because join targets are not in ctx.tables.
        if physical_no_catalog not in replacements:
            replacements[physical_no_catalog] = physical_with_catalog
    return _apply_replacements(sql, replacements)


def qualify_with_catalogs(sql: str, ctx: CompilationContext) -> str:
    """Add catalog prefix to physical table refs: "schema"."table" → "catalog"."schema"."table"."""
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    for meta in _all_table_metas(ctx):
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        replacements[_table_ref(meta, use_catalog=False)] = _table_ref(meta, use_catalog=True)
    return _apply_replacements(sql, replacements)


# --- Main compilation ---


def _has_joins(field_node: FieldNode, ctx: CompilationContext, type_name: str) -> bool:
    """Check if any selected field is a relationship (requires JOIN)."""
    if not field_node.selection_set:
        return False
    for sel in field_node.selection_set.selections:
        if isinstance(sel, FieldNode):
            if (type_name, sel.name.value) in ctx.joins:
                return True
    return False


_NESTED_DB_ARGS = frozenset({"where", "order_by", "limit", "offset", "distinct_on"})
# Args that require LATERAL JOIN — limit is handled inside ARRAY_AGG subquery
_LATERAL_FORCE_ARGS = frozenset({"where", "order_by", "offset", "distinct_on"})


def _has_nested_db_args(field_node: FieldNode) -> bool:
    return any(arg.name.value in _NESTED_DB_ARGS for arg in field_node.arguments)


def _has_lateral_force_args(field_node: FieldNode) -> bool:
    return any(arg.name.value in _LATERAL_FORCE_ARGS for arg in field_node.arguments)


def _explicit_limit(field_node: FieldNode, variables: dict | None) -> int | None:
    for arg in field_node.arguments:
        if arg.name.value == "limit":
            val = _extract_value(arg.value, variables)
            if isinstance(val, int) and not isinstance(val, bool) and val > 0:
                return val
    return None


def _extract_non_negative_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _field_args(field_node: FieldNode, variables: dict | None) -> dict:
    return {
        arg.name.value: _extract_value(arg.value, variables)
        for arg in field_node.arguments
    }


def _lateral_join(
    field_node: FieldNode,
    join_meta: JoinMeta,
    join_alias: str,
    src_expr: str,
    collector: ParamCollector,
    variables: dict | None,
    use_catalog: bool,
) -> str:
    args = _field_args(field_node, variables)
    if join_meta.target_expr is not None:
        _lat_tgt_expr = join_meta.target_expr.replace("{alias}", _q(join_alias))
    else:
        _lat_tgt_expr = _join_column_expr_for(
            None,
            join_meta.target_column,
            join_meta.target_column_type,
            join_meta.source_column_type,
        )
    sql = (
        f'LEFT JOIN LATERAL (SELECT * FROM {_table_ref(join_meta.target, use_catalog)}'
        f' WHERE {_lat_tgt_expr} = {src_expr}'
    )
    if "where" in args:
        where_sql = _compile_where(
            args["where"],
            collector,
            None,
            None,
        )
        sql += f" AND ({where_sql})"
    if "distinct_on" in args:
        distinct_cols = args["distinct_on"]
        if isinstance(distinct_cols, str):
            distinct_cols = [distinct_cols]
        distinct_prefix = ", ".join(_q(c) for c in distinct_cols)
        sql = sql.replace("SELECT *", f"SELECT DISTINCT ON ({distinct_prefix}) *", 1)
    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        sql += f" ORDER BY {_compile_order_by(order_by_val, None)}"
    limit_value = args.get("limit", join_meta.default_limit)
    if limit_value is not None:
        limit_value = _extract_non_negative_int(limit_value, "limit")
        sql += f" LIMIT {collector.add(limit_value)}"
    if "offset" in args:
        offset_value = _extract_non_negative_int(args["offset"], "offset")
        sql += f" OFFSET {collector.add(offset_value)}"
    return (
        f'{sql}) {_q(join_alias)} ON TRUE'
    )


def _emit_agg_subqueries(
    selections,
    ctx: CompilationContext,
    type_name: str,
    table_meta: TableMeta,
    from_clause: str,
    where_expr: str,
    extra_joins: str,
    current_alias: str,
    nesting_path: str,
    cardinality: str | None,
    agg_limit: int | None,
    use_catalog: bool,
    alias_counter: int,
    select_parts: list[str],
    columns: list[ColumnRef],
    sources: set[str],
    variables: dict | None = None,
) -> int:
    """Emit correlated ARRAY_AGG subqueries for all scalars at any depth.

    For scalars: emits one ARRAY_AGG correlated subquery per leaf field.
    For sub-relationships: extends the JOIN chain and recurses.
    """
    for sel in selections:
        if not isinstance(sel, FieldNode):
            continue
        name = sel.name.value
        key = sel.alias.value if sel.alias else name
        join_key = (type_name, name)

        if join_key in ctx.joins:
            if not sel.selection_set:
                continue
            sub_join_meta = ctx.joins[join_key]
            sub_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(sub_join_meta.target.source_id)

            if sub_join_meta.source_constant is not None:
                sub_src = (
                    _sql_str_literal(sub_join_meta.source_constant)
                    if isinstance(sub_join_meta.source_constant, str)
                    else str(sub_join_meta.source_constant)
                )
            elif sub_join_meta.source_expr is not None:
                sub_src = sub_join_meta.source_expr.replace("{alias}", _q(current_alias))
            elif sub_join_meta.source_json_key:
                sub_src = (
                    f'CAST({_q(current_alias)}.{_q(sub_join_meta.source_column)} AS JSON)'
                    f'>>\'{sub_join_meta.source_json_key}\''
                )
            else:
                sub_src = _join_column_expr(
                    current_alias, sub_join_meta.source_column,
                    sub_join_meta.source_column_type, sub_join_meta.target_column_type,
                )
            if sub_join_meta.target_expr is not None:
                sub_tgt = sub_join_meta.target_expr.replace("{alias}", _q(sub_alias))
            elif sub_join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(sub_join_meta.target.table_id) or {}).get(
                    sub_join_meta.target_column, ""
                )
                sub_tgt = _sql_str_literal(_tvc)
            else:
                sub_tgt = _join_column_expr(
                    sub_alias, sub_join_meta.target_column,
                    sub_join_meta.target_column_type, sub_join_meta.source_column_type,
                )
            new_join = (
                f'JOIN {_table_ref(sub_join_meta.target, use_catalog)} {_q(sub_alias)}'
                f' ON {sub_tgt} = {sub_src}'
            )
            new_extra = f"{extra_joins} {new_join}".strip() if extra_joins else new_join
            sub_limit = _explicit_limit(sel, variables) or sub_join_meta.default_limit
            alias_counter = _emit_agg_subqueries(
                sel.selection_set.selections, ctx,
                sub_join_meta.target.type_name, sub_join_meta.target,
                from_clause, where_expr, new_extra, sub_alias,
                f"{nesting_path}.{key}", sub_join_meta.cardinality,
                sub_limit, use_catalog, alias_counter,
                select_parts, columns, sources, variables,
            )
        else:
            phys_col = ctx.gql_to_physical.get((table_meta.table_id, name), name)
            col_alias = nesting_path.replace(".", "__") + "__" + key
            if extra_joins and agg_limit is not None:
                select_parts.append(
                    f'(SELECT ARRAY_AGG({_q(current_alias)}.{_q(phys_col)})'
                    f' FROM (SELECT {_q(current_alias)}.{_q(phys_col)}'
                    f' FROM {from_clause} {extra_joins}'
                    f' WHERE {where_expr}'
                    f' LIMIT {agg_limit}))'
                    f' AS {_q(col_alias)}'
                )
            elif extra_joins:
                select_parts.append(
                    f'(SELECT ARRAY_AGG({_q(current_alias)}.{_q(phys_col)})'
                    f' FROM {from_clause} {extra_joins}'
                    f' WHERE {where_expr})'
                    f' AS {_q(col_alias)}'
                )
            elif agg_limit is not None:
                select_parts.append(
                    f'(SELECT ARRAY_AGG({_q(phys_col)})'
                    f' FROM (SELECT {_q(phys_col)}'
                    f' FROM {from_clause}'
                    f' WHERE {where_expr}'
                    f' LIMIT {agg_limit}))'
                    f' AS {_q(col_alias)}'
                )
            else:
                select_parts.append(
                    f'(SELECT ARRAY_AGG({_q(current_alias)}.{_q(phys_col)})'
                    f' FROM {from_clause}'
                    f' WHERE {where_expr})'
                    f' AS {_q(col_alias)}'
                )
            columns.append(ColumnRef(
                alias=current_alias,
                column=phys_col,
                field_name=key,
                nested_in=nesting_path,
                cardinality=cardinality,
                is_agg=True,
            ))
    return alias_counter


def _extract_json_blob_kv(sels, blob_base: str) -> list[str]:
    """Recursively build KEY/VALUE pairs by extracting fields from a JSON blob column."""
    pairs: list[str] = []
    for ss in sels:
        if not isinstance(ss, FieldNode):
            continue
        sn = ss.name.value
        sk = ss.alias.value if ss.alias else sn
        if ss.selection_set:
            sub_pairs = _extract_json_blob_kv(ss.selection_set.selections, f"{blob_base}->'{sn}'")
            if sub_pairs:
                pairs.append(f"KEY '{sk}' VALUE json_object({', '.join(sub_pairs)})")
        else:
            pairs.append(f"KEY '{sk}' VALUE {blob_base}->>\'{sn}\'")
    return pairs


def _build_rel_json_kv(
    selections,
    ctx: CompilationContext,
    type_name: str,
    table_meta: TableMeta,
    table_alias: str,
    use_catalog: bool,
    alias_counter: int,
    sources: set[str],
    variables: dict | None,
    parent_src_val: str | None = None,
) -> tuple[list[str], int]:
    """Build KEY/VALUE pairs for json_object(KEY k VALUE v, ...) for a relationship.

    Returns (kv_pairs_list, alias_counter) where each element is
    "KEY 'key' VALUE expr" — suitable for joining with commas inside json_object.
    Nested relationships produce correlated subqueries at value positions.
    Uses SQL-standard json_object syntax so sqlglot transpiles correctly to Trino.
    """
    kv_pairs: list[str] = []
    for sel in selections:
        if not isinstance(sel, FieldNode):
            continue
        name = sel.name.value
        key = sel.alias.value if sel.alias else name
        join_key = (type_name, name)

        if join_key in ctx.joins:
            if not sel.selection_set:
                continue
            sub_join_meta = ctx.joins[join_key]
            sub_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(sub_join_meta.target.source_id)

            if sub_join_meta.source_expr is not None:
                if parent_src_val is not None:
                    # Trino rejects doubly-nested correlated subqueries. When the parent
                    # join resolved to a constant (e.g. 'pet-store.pets'), use that value
                    # here so the child subquery's WHERE clause contains no outer reference.
                    sub_src = parent_src_val
                else:
                    sub_src = sub_join_meta.source_expr.replace("{alias}", _q(table_alias))
            elif sub_join_meta.source_constant is not None:
                sub_src = (
                    _sql_str_literal(sub_join_meta.source_constant)
                    if isinstance(sub_join_meta.source_constant, str)
                    else str(sub_join_meta.source_constant)
                )
            elif sub_join_meta.source_json_key:
                sub_src = (
                    f'CAST({_q(table_alias)}.{_q(sub_join_meta.source_column)} AS JSON)'
                    f'>>\'{sub_join_meta.source_json_key}\''
                )
            else:
                sub_src = _join_column_expr(
                    table_alias, sub_join_meta.source_column,
                    sub_join_meta.source_column_type, sub_join_meta.target_column_type,
                )
            if sub_join_meta.target_expr is not None:
                sub_tgt = sub_join_meta.target_expr.replace("{alias}", _q(sub_alias))
            elif sub_join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(sub_join_meta.target.table_id) or {}).get(
                    sub_join_meta.target_column, ""
                )
                sub_tgt = _sql_str_literal(_tvc)
            else:
                sub_tgt = _join_column_expr(
                    sub_alias, sub_join_meta.target_column,
                    sub_join_meta.target_column_type, sub_join_meta.source_column_type,
                )
            sub_from = f"{_table_ref(sub_join_meta.target, use_catalog)} {_q(sub_alias)}"
            sub_where = f"{sub_tgt} = {sub_src}"
            sub_limit = _explicit_limit(sel, variables) or sub_join_meta.default_limit
            # Prefer child_src_val (explicit varchar constant) over sub_src. Fall back to
            # sub_src for non-integer joins; block propagation for integer joins to avoid
            # type mismatches in child varchar source_expr comparisons.
            _next_parent_src = (
                sub_join_meta.child_src_val
                if sub_join_meta.child_src_val is not None
                else (sub_src if sub_join_meta.source_column_type != "integer" else None)
            )
            sub_expr, alias_counter = _build_rel_json_expr(
                sel.selection_set.selections, ctx,
                sub_join_meta.target.type_name, sub_join_meta.target,
                sub_alias, sub_from, sub_where,
                sub_join_meta.cardinality, sub_limit,
                use_catalog, alias_counter, sources, variables,
                parent_src_val=_next_parent_src,
            )
            kv_pairs.append(f"KEY '{key}' VALUE {sub_expr}")
        else:
            if sel.selection_set and (table_meta.table_id, name) in ctx.gql_json_columns:
                blob_base = f"{_q(table_alias)}.{_q(name)}"
                sub_kv = _extract_json_blob_kv(sel.selection_set.selections, blob_base)
                if sub_kv:
                    kv_pairs.append(f"KEY '{key}' VALUE json_object({', '.join(sub_kv)})")
            else:
                phys_col = ctx.gql_to_physical.get((table_meta.table_id, name), name)
                kv_pairs.append(f"KEY '{key}' VALUE {_q(table_alias)}.{_q(phys_col)}")

    return kv_pairs, alias_counter


def _build_rel_json_expr(
    selections,
    ctx: CompilationContext,
    type_name: str,
    table_meta: TableMeta,
    table_alias: str,
    from_clause: str,
    where_expr: str,
    cardinality: str | None,
    agg_limit: int | None,
    use_catalog: bool,
    alias_counter: int,
    sources: set[str],
    variables: dict | None = None,
    parent_src_val: str | None = None,
) -> tuple[str, int]:
    """Build one correlated JSON subquery for a relationship.

    many-to-one  → (SELECT json_object(...) FROM ... WHERE ... LIMIT 1)
    one-to-many  → (SELECT json_agg(json_object(...)) FROM ... WHERE ...)
    one-to-many with agg_limit →
        (SELECT json_agg(_t) FROM (SELECT json_object(...) AS _t FROM ... WHERE ... LIMIT n) _sub)
    Returns (sql_expr, alias_counter).
    """
    kv_pairs, alias_counter = _build_rel_json_kv(
        selections, ctx, type_name, table_meta, table_alias,
        use_catalog, alias_counter, sources, variables,
        parent_src_val=parent_src_val,
    )
    jbo = f"json_object({', '.join(kv_pairs)})"

    if cardinality == "many-to-one":
        expr = (
            f"(SELECT {jbo}"
            f" FROM {from_clause}"
            f" WHERE {where_expr}"
            f" LIMIT 1)"
        )
    elif agg_limit is not None:
        expr = (
            f"(SELECT json_agg(_t)"
            f" FROM (SELECT {jbo} AS _t"
            f" FROM {from_clause}"
            f" WHERE {where_expr}"
            f" LIMIT {agg_limit}) _sub)"
        )
    else:
        expr = (
            f"(SELECT json_agg({jbo})"
            f" FROM {from_clause}"
            f" WHERE {where_expr})"
        )

    return expr, alias_counter


def _collect_nested_columns(
    selections,
    parent_alias: str,
    parent_type_name: str,
    parent_table: TableMeta,
    nesting_path: str,
    ctx: CompilationContext,
    select_parts: list[str],
    columns: list[ColumnRef],
    join_clauses: list[str],
    sources: set[str],
    alias_counter: int,
    use_catalog: bool,
    collector: ParamCollector,
    variables: dict | None,
    cardinality: str | None = None,
    flat: bool = False,
) -> tuple[int, bool]:
    """Recursively collect columns and JOINs from nested selections."""
    has_lateral = False
    for nested_sel in selections:
        if not isinstance(nested_sel, FieldNode):
            continue
        nested_name = nested_sel.name.value
        nested_join_key = (parent_type_name, nested_name)

        if nested_join_key in ctx.joins:
            # This nested field is itself a relationship → add another JOIN
            nested_join_meta = ctx.joins[nested_join_key]
            nested_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(nested_join_meta.target.source_id)

            if nested_join_meta.source_constant is not None:
                src_expr = _sql_str_literal(nested_join_meta.source_constant) if isinstance(nested_join_meta.source_constant, str) else str(nested_join_meta.source_constant)
            elif nested_join_meta.source_column in _VIRTUAL_COLS:
                _svc = (ctx.virtual_columns.get(parent_table.table_id) or {}).get(nested_join_meta.source_column, "")
                src_expr = _sql_str_literal(_svc)
            elif nested_join_meta.source_json_key:
                src_expr = f'CAST({_q(parent_alias)}.{_q(nested_join_meta.source_column)} AS JSON)->>\'{nested_join_meta.source_json_key}\''
            else:
                src_expr = _join_column_expr(
                    parent_alias, nested_join_meta.source_column,
                    nested_join_meta.source_column_type, nested_join_meta.target_column_type,
                )
            if nested_join_meta.target_expr is not None:
                tgt_expr = nested_join_meta.target_expr.replace("{alias}", _q(nested_alias))
            elif nested_join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(nested_join_meta.target.table_id) or {}).get(nested_join_meta.target_column, "")
                tgt_expr = _sql_str_literal(_tvc)
            else:
                tgt_expr = _join_column_expr(
                    nested_alias, nested_join_meta.target_column,
                    nested_join_meta.target_column_type, nested_join_meta.source_column_type,
                )
            nested_key = nested_sel.alias.value if nested_sel.alias else nested_name
            _use_agg = not flat and not _has_lateral_force_args(nested_sel)
            if (nested_join_meta.default_limit is not None or _has_lateral_force_args(nested_sel)) and not _use_agg:
                if nested_join_meta.default_limit is not None:
                    has_lateral = True
                join_clauses.append(
                    _lateral_join(
                        nested_sel,
                        nested_join_meta,
                        nested_alias,
                        src_expr,
                        collector,
                        variables,
                        use_catalog,
                    )
                )
            elif _use_agg and nested_sel.selection_set:
                _agg_limit = _explicit_limit(nested_sel, variables) or nested_join_meta.default_limit
                _from_clause = f"{_table_ref(nested_join_meta.target, use_catalog)} {_q(nested_alias)}"
                _where_expr = f"{tgt_expr} = {src_expr}"
                alias_counter = _emit_agg_subqueries(
                    nested_sel.selection_set.selections, ctx,
                    nested_join_meta.target.type_name, nested_join_meta.target,
                    _from_clause, _where_expr, "", nested_alias,
                    f"{nesting_path}.{nested_key}", nested_join_meta.cardinality,
                    _agg_limit, use_catalog, alias_counter,
                    select_parts, columns, sources, variables,
                )
            else:
                join_clauses.append(
                    f'LEFT JOIN {_table_ref(nested_join_meta.target, use_catalog)}'
                    f' {_q(nested_alias)}'
                    f' ON {src_expr} = {tgt_expr}'
                )

            sub_path = f"{nesting_path}.{nested_name}"
            if nested_sel.selection_set and not _use_agg:
                alias_counter, _child_lateral = _collect_nested_columns(
                    nested_sel.selection_set.selections,
                    nested_alias,
                    nested_join_meta.target.type_name,
                    nested_join_meta.target,
                    sub_path,
                    ctx,
                    select_parts,
                    columns,
                    join_clauses,
                    sources,
                    alias_counter,
                    use_catalog,
                    collector,
                    variables,
                    cardinality=nested_join_meta.cardinality,
                    flat=flat,
                )
                has_lateral |= _child_lateral
        else:
            # GQL OBJECT column stored as JSON — expand sub-selections recursively via -> / ->>
            if nested_sel.selection_set and (parent_table.table_id, nested_name) in ctx.gql_json_columns:
                def _emit_json_cols(
                    sels,
                    json_base: str,
                    col_prefix: str,
                    nesting: str,
                ) -> None:
                    for ss in sels:
                        if not isinstance(ss, FieldNode):
                            continue
                        sn = ss.name.value
                        sk = ss.alias.value if ss.alias else sn
                        if ss.selection_set:
                            _emit_json_cols(
                                ss.selection_set.selections,
                                f"{json_base}->'{sn}'",
                                f"{col_prefix}__{sn}",
                                f"{nesting}.{sn}",
                            )
                        else:
                            expr = f"{json_base}->>\'{sn}\'"
                            col_alias = f"{col_prefix}__{sn}"
                            select_parts.append(f'{expr} AS {_q(col_alias)}')
                            columns.append(ColumnRef(
                                alias=parent_alias,
                                column=col_alias,
                                field_name=sk,
                                nested_in=nesting,
                                cardinality=cardinality,
                            ))
                _emit_json_cols(
                    nested_sel.selection_set.selections,
                    f'{_q(parent_alias)}.{_q(nested_name)}',
                    nested_name,
                    f"{nesting_path}.{nested_name}",
                )
                continue
            # Scalar column from the parent join
            nested_response_key = nested_sel.alias.value if nested_sel.alias else nested_name
            nested_phys = ctx.gql_to_physical.get((parent_table.table_id, nested_name), nested_name)
            if nested_phys in _VIRTUAL_COLS:
                _nvc = (ctx.virtual_columns.get(parent_table.table_id) or {}).get(nested_phys, "")
                col_expr = _sql_str_literal(_nvc)
            else:
                col_expr = f'{_q(parent_alias)}.{_q(nested_phys)}'
            if nested_sel.alias:
                select_parts.append(f'{col_expr} AS {_q(nested_response_key)}')
            else:
                select_parts.append(col_expr)
            columns.append(ColumnRef(
                alias=parent_alias,
                column=nested_phys,
                field_name=nested_response_key,
                nested_in=nesting_path,
                cardinality=cardinality,
            ))
    return alias_counter, has_lateral


def _compile_root_field(
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
    flat: bool = False,
) -> CompiledQuery:
    """Compile a single root query field to SQL."""
    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    use_aliases = _has_joins(field_node, ctx, table.type_name)
    root_alias: str | None = "t0" if use_aliases else None
    alias_counter = 1
    has_lateral_ops_joins = False

    # Collect SELECT columns and JOINs
    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    join_clauses: list[str] = []

    assert field_node.selection_set is not None
    for sel in field_node.selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue

        sel_name = sel.name.value
        join_key = (table.type_name, sel_name)

        if join_key in ctx.joins:
            # Relationship field → JOIN
            assert root_alias is not None  # joins only exist when use_aliases=True
            join_meta = ctx.joins[join_key]
            join_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(join_meta.target.source_id)

            if join_meta.source_expr is not None:
                src_expr = join_meta.source_expr.replace("{alias}", _q(root_alias))
            elif join_meta.source_constant is not None:
                src_expr = _sql_str_literal(join_meta.source_constant) if isinstance(join_meta.source_constant, str) else str(join_meta.source_constant)
            elif join_meta.source_column in _VIRTUAL_COLS:
                _svc = (ctx.virtual_columns.get(table.table_id) or {}).get(join_meta.source_column, "")
                src_expr = _sql_str_literal(_svc)
            elif join_meta.source_json_key:
                src_expr = f'CAST({_q(root_alias)}.{_q(join_meta.source_column)} AS JSON)->>\'{join_meta.source_json_key}\''
            else:
                src_expr = _join_column_expr(
                    root_alias, join_meta.source_column,
                    join_meta.source_column_type, join_meta.target_column_type,
                )
            if join_meta.target_expr is not None:
                tgt_expr = join_meta.target_expr.replace("{alias}", _q(join_alias))
            elif join_meta.target_column in _VIRTUAL_COLS:
                _tvc = (ctx.virtual_columns.get(join_meta.target.table_id) or {}).get(join_meta.target_column, "")
                tgt_expr = _sql_str_literal(_tvc)
            else:
                tgt_expr = _join_column_expr(
                    join_alias, join_meta.target_column,
                    join_meta.target_column_type, join_meta.source_column_type,
                )
            _use_agg = not flat and not _has_lateral_force_args(sel)
            if (join_meta.default_limit is not None or _has_lateral_force_args(sel)) and not _use_agg:
                if join_meta.default_limit is not None:
                    has_lateral_ops_joins = True
                join_clauses.append(
                    _lateral_join(
                        sel,
                        join_meta,
                        join_alias,
                        src_expr,
                        collector,
                        variables,
                        use_catalog,
                    )
                )
                if sel.selection_set:
                    alias_counter, _child_lateral = _collect_nested_columns(
                        sel.selection_set.selections,
                        join_alias,
                        join_meta.target.type_name,
                        join_meta.target,
                        sel_name,
                        ctx,
                        select_parts,
                        columns,
                        join_clauses,
                        sources,
                        alias_counter,
                        use_catalog,
                        collector,
                        variables,
                        cardinality=join_meta.cardinality,
                        flat=flat,
                    )
                    has_lateral_ops_joins |= _child_lateral
            elif _use_agg and sel.selection_set:
                _agg_limit = _explicit_limit(sel, variables) or join_meta.default_limit
                _from_clause = f"{_table_ref(join_meta.target, use_catalog)} {_q(join_alias)}"
                _where_expr = f"{tgt_expr} = {src_expr}"
                _rel_key = sel.alias.value if sel.alias else sel_name
                _pv = (
                    join_meta.child_src_val
                    if join_meta.child_src_val is not None
                    else (src_expr if join_meta.source_column_type != "integer" else None)
                )
                json_expr, alias_counter = _build_rel_json_expr(
                    sel.selection_set.selections, ctx,
                    join_meta.target.type_name, join_meta.target,
                    join_alias, _from_clause, _where_expr,
                    join_meta.cardinality, _agg_limit,
                    use_catalog, alias_counter, sources, variables,
                    parent_src_val=_pv,
                )
                select_parts.append(f"{json_expr} AS {_q(_rel_key)}")
                columns.append(ColumnRef(
                    alias=join_alias, column=_rel_key, field_name=_rel_key,
                    nested_in=None, cardinality=join_meta.cardinality, is_agg=True,
                ))
            else:
                join_clauses.append(
                    f'LEFT JOIN {_table_ref(join_meta.target, use_catalog)}'
                    f' {_q(join_alias)}'
                    f' ON {src_expr} = {tgt_expr}'
                )
                if sel.selection_set:
                    alias_counter, _child_lateral = _collect_nested_columns(
                        sel.selection_set.selections,
                        join_alias,
                        join_meta.target.type_name,
                        join_meta.target,
                        sel_name,
                        ctx,
                        select_parts,
                        columns,
                        join_clauses,
                        sources,
                        alias_counter,
                        use_catalog,
                        collector,
                        variables,
                        cardinality=join_meta.cardinality,
                        flat=flat,
                    )
                    has_lateral_ops_joins |= _child_lateral
        else:
            # GQL OBJECT column stored as JSON — expand sub-selections recursively via -> / ->>
            if sel.selection_set and (table.table_id, sel_name) in ctx.gql_json_columns:
                def _emit_root_json_cols(
                    sels,
                    json_base: str,
                    col_prefix: str,
                    nesting: str,
                ) -> None:
                    for ss in sels:
                        if not isinstance(ss, FieldNode):
                            continue
                        sn = ss.name.value
                        sk = ss.alias.value if ss.alias else sn
                        if ss.selection_set:
                            _emit_root_json_cols(
                                ss.selection_set.selections,
                                f"{json_base}->'{sn}'",
                                f"{col_prefix}__{sn}",
                                f"{nesting}.{sn}",
                            )
                        else:
                            expr = f"{json_base}->>\'{sn}\'"
                            col_alias = f"{col_prefix}__{sn}"
                            select_parts.append(f'{expr} AS {_q(col_alias)}')
                            columns.append(ColumnRef(
                                alias=root_alias,
                                column=col_alias,
                                field_name=sk,
                                nested_in=nesting,
                                cardinality=None,
                            ))
                if use_aliases:
                    assert root_alias is not None
                    base = f'{_q(root_alias)}.{_q(sel_name)}'
                else:
                    base = _q(sel_name)
                _emit_root_json_cols(sel.selection_set.selections, base, sel_name, sel_name)
                continue
            # Scalar field — check for JSON path extraction
            response_key = sel.alias.value if sel.alias else sel_name
            gql_field_name = response_key
            col_path = ctx.column_paths.get((table.table_id, sel_name))
            phys_name = ctx.gql_to_physical.get((table.table_id, sel_name), sel_name)
            if col_path:
                # path is "source_col.key1.key2" → PG JSON extraction
                # Emits PG syntax; SQLGlot transpiles to Trino json_extract_scalar
                path_parts = col_path.split(".")
                source_col = path_parts[0]
                keys = path_parts[1:]
                if use_aliases:
                    assert root_alias is not None
                    expr = f'{_q(root_alias)}.{_q(source_col)}'
                else:
                    expr = _q(source_col)
                # Navigate with -> for intermediate keys, ->> for final (text extract)
                for i, key in enumerate(keys):
                    op = "->>" if i == len(keys) - 1 else "->"
                    expr = f"{expr}{op}'{key}'"
                if sel.alias:
                    expr = f'{expr} AS {_q(response_key)}'
                select_parts.append(expr)
            elif phys_name in _VIRTUAL_COLS:
                vval = (ctx.virtual_columns.get(table.table_id) or {}).get(phys_name, "")
                expr = _sql_str_literal(vval)
                select_parts.append(f'{expr} AS {_q(response_key)}')
            elif use_aliases:
                assert root_alias is not None
                col_expr = f'{_q(root_alias)}.{_q(phys_name)}'
                if sel.alias:
                    select_parts.append(f'{col_expr} AS {_q(response_key)}')
                else:
                    select_parts.append(col_expr)
            else:
                if sel.alias:
                    select_parts.append(f'{_q(phys_name)} AS {_q(response_key)}')
                else:
                    select_parts.append(_q(phys_name))
            columns.append(ColumnRef(
                alias=root_alias if use_aliases else None,
                column=phys_name,
                field_name=gql_field_name,
                nested_in=None,
            ))

    # FROM clause
    ref = _table_ref(table, use_catalog)
    if use_aliases:
        assert root_alias is not None
        from_clause = f'{ref} {_q(root_alias)}'
    else:
        from_clause = ref

    # Process arguments before building SQL so as_of can modify the FROM clause
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    # Time-travel: append FOR TIMESTAMP/VERSION AS OF to table ref in FROM clause (REQ-372)
    if "as_of" in args:
        if table.source_type not in TIME_TRAVEL_SOURCES:
            raise ValueError(
                f"as_of is not supported for source type {table.source_type!r}; "
                f"only iceberg and delta_lake sources support time-travel"
            )
        as_of_val = args["as_of"]
        # Numeric → version; string → timestamp
        try:
            version = int(as_of_val)
            time_travel_clause = f" FOR VERSION AS OF {version}"
        except (TypeError, ValueError):
            time_travel_clause = f" FOR TIMESTAMP AS OF TIMESTAMP '{as_of_val}'"
        if use_aliases:
            assert root_alias is not None
            from_clause = f'{ref}{time_travel_clause} {_q(root_alias)}'
        else:
            from_clause = f'{ref}{time_travel_clause}'

    # When ops LATERAL joins are present, wrap the base table in a subquery so that
    # the base row count is capped before the lateral Cartesian expansion.
    # Without this cap, Trino runs one full Iceberg scan per base row (no secondary index).
    result_limit: int | None = None
    if has_lateral_ops_joins:
        base_limit = int(args["limit"]) if "limit" in args else _get_default_row_limit()
        result_limit = base_limit if "limit" in args else None
        if use_aliases:
            assert root_alias is not None
            from_clause = f'(SELECT * FROM {ref} LIMIT {base_limit}) {_q(root_alias)}'
        else:
            from_clause = f'(SELECT * FROM {ref} LIMIT {base_limit})'

    sql = f'SELECT {", ".join(select_parts)} FROM {from_clause}'

    # JOIN clauses
    for join_clause in join_clauses:
        sql += f" {join_clause}"

    # DISTINCT ON — inject after SELECT keyword
    if "distinct_on" in args:
        distinct_cols = args["distinct_on"]
        if isinstance(distinct_cols, str):
            distinct_cols = [distinct_cols]
        parts_d = [
            _q(c) if root_alias is None else f"{_q(root_alias)}.{_q(c)}"
            for c in distinct_cols
        ]
        distinct_prefix = f"DISTINCT ON ({', '.join(parts_d)}) "
        sql = f"SELECT {distinct_prefix}{sql[len('SELECT '):]}"

    # WHERE
    if "where" in args:
        _vvals = ctx.virtual_columns.get(table.table_id)
        where_sql = _compile_where(args["where"], collector, root_alias, _vvals)
        sql += f" WHERE {where_sql}"

    # ORDER BY
    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        order_sql = _compile_order_by(order_by_val, root_alias)
        sql += f" ORDER BY {order_sql}"

    # LIMIT / OFFSET
    # When ops LATERAL joins are present, SQL LIMIT would cut the expanded join rows,
    # not the base rows. result_limit is set above (base-table subquery) for the SQL path;
    # it's also used for Python-level truncation after grouping in the GraphQL path.
    if has_lateral_ops_joins:
        if result_limit is None and "limit" in args:
            result_limit = int(args["limit"])
        # Outer LIMIT is meaningless after lateral expansion; use a fixed cap
        sql += " LIMIT 25"
        if "offset" in args:
            sql += f" OFFSET {collector.add(int(args['offset']))}"
    else:
        # Always parameterized, never interpolated.
        # Apply _DEFAULT_ROW_LIMIT when caller supplies no explicit limit so that
        # unbounded scans over large tables (e.g. OTel Iceberg) cannot OOM Trino.
        if "limit" in args:
            sql += f" LIMIT {collector.add(int(args['limit']))}"
        elif "offset" not in args:
            sql += f" LIMIT {_get_default_row_limit()}"
        if "offset" in args:
            sql += f" OFFSET {collector.add(int(args['offset']))}"

    # Collect native filter args (any arg not handled by SQL compilation above)
    _STANDARD_ARGS = {"where", "order_by", "limit", "offset", "distinct_on", "as_of"}
    api_args = {k: v for k, v in args.items() if k not in _STANDARD_ARGS}

    # Inject _nf_-prefixed WHERE conditions so the SQL/CQL preview shows the filter.
    # nf_extractor strips these before Trino execution; endpoint.py uses api_args for the REST call.
    if api_args:
        nf_conditions = []
        for k, v in api_args.items():
            col = f"_nf_{k}"
            quoted_col = _q(col)
            if isinstance(v, bool):
                lit = "TRUE" if v else "FALSE"
            elif isinstance(v, (int, float)):
                lit = str(v)
            else:
                lit = "'" + str(v).replace("'", "''") + "'"
            nf_conditions.append(f"{quoted_col} = {lit}")
        nf_where = " AND ".join(nf_conditions)
        if " WHERE " in sql:
            sql += f" AND {nf_where}"
        else:
            sql += f" WHERE {nf_where}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        api_args=api_args,
        result_limit=result_limit,
    )


def _collect_requested_agg_funcs(
    field_node: FieldNode,
) -> tuple[bool, list[str], list[str], list[str], list[str], bool]:
    """Parse the aggregate selection set to find which functions are requested.

    Returns: (has_count, sum_cols, avg_cols, min_cols, max_cols, has_nodes)
    """
    has_count = False
    sum_cols: list[str] = []
    avg_cols: list[str] = []
    min_cols: list[str] = []
    max_cols: list[str] = []
    has_nodes = False

    if not field_node.selection_set:
        return has_count, sum_cols, avg_cols, min_cols, max_cols, has_nodes

    for sel in field_node.selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue
        name = sel.name.value
        if name == "nodes":
            has_nodes = True
        elif name == "aggregate" and sel.selection_set:
            for agg_sel in sel.selection_set.selections:
                if not isinstance(agg_sel, FieldNode):
                    continue
                agg_name = agg_sel.name.value
                if agg_name == "count":
                    has_count = True
                elif agg_name in ("sum", "avg", "min", "max") and agg_sel.selection_set:
                    cols = [
                        s.name.value
                        for s in agg_sel.selection_set.selections
                        if isinstance(s, FieldNode)
                    ]
                    if agg_name == "sum":
                        sum_cols = cols
                    elif agg_name == "avg":
                        avg_cols = cols
                    elif agg_name == "min":
                        min_cols = cols
                    elif agg_name == "max":
                        max_cols = cols

    return has_count, sum_cols, avg_cols, min_cols, max_cols, has_nodes


def _compile_aggregate_field(
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
) -> CompiledQuery:
    """Compile an _aggregate root query field to SQL."""
    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    has_count, sum_cols, avg_cols, min_cols, max_cols, has_nodes = (
        _collect_requested_agg_funcs(field_node)
    )

    # Collect aliases for aggregate sub-fields
    agg_key = "aggregate"
    func_aliases: dict[str, str] = {}
    col_aliases: dict[str, dict[str, str]] = {}
    if field_node.selection_set:
        for sel in field_node.selection_set.selections:
            if not isinstance(sel, FieldNode) or sel.name.value != "aggregate":
                continue
            if sel.alias:
                agg_key = sel.alias.value
            if not sel.selection_set:
                continue
            for agg_sel in sel.selection_set.selections:
                if not isinstance(agg_sel, FieldNode):
                    continue
                func = agg_sel.name.value
                if agg_sel.alias:
                    func_aliases[func] = agg_sel.alias.value
                if agg_sel.selection_set:
                    col_aliases[func] = {}
                    for col_sel in agg_sel.selection_set.selections:
                        if isinstance(col_sel, FieldNode) and col_sel.alias:
                            col_aliases[func][col_sel.name.value] = col_sel.alias.value

    # Build SELECT parts for aggregate functions
    select_parts: list[str] = []
    columns: list[ColumnRef] = []

    if has_count:
        select_parts.append("COUNT(*)")
        columns.append(ColumnRef(alias=None, column="count", field_name="count", nested_in=agg_key))

    for col_name in sum_cols:
        fn_key = func_aliases.get("sum", "sum")
        field_name = col_aliases.get("sum", {}).get(col_name, col_name)
        expr = f'SUM({_q(col_name)})'
        if field_name != col_name:
            expr += f' AS {_q(field_name)}'
        select_parts.append(expr)
        columns.append(ColumnRef(alias=None, column=col_name, field_name=field_name, nested_in=f"{agg_key}.{fn_key}"))

    for col_name in avg_cols:
        fn_key = func_aliases.get("avg", "avg")
        field_name = col_aliases.get("avg", {}).get(col_name, col_name)
        expr = f'AVG({_q(col_name)})'
        if field_name != col_name:
            expr += f' AS {_q(field_name)}'
        select_parts.append(expr)
        columns.append(ColumnRef(alias=None, column=col_name, field_name=field_name, nested_in=f"{agg_key}.{fn_key}"))

    for col_name in min_cols:
        fn_key = func_aliases.get("min", "min")
        field_name = col_aliases.get("min", {}).get(col_name, col_name)
        expr = f'MIN({_q(col_name)})'
        if field_name != col_name:
            expr += f' AS {_q(field_name)}'
        select_parts.append(expr)
        columns.append(ColumnRef(alias=None, column=col_name, field_name=field_name, nested_in=f"{agg_key}.{fn_key}"))

    for col_name in max_cols:
        fn_key = func_aliases.get("max", "max")
        field_name = col_aliases.get("max", {}).get(col_name, col_name)
        expr = f'MAX({_q(col_name)})'
        if field_name != col_name:
            expr += f' AS {_q(field_name)}'
        select_parts.append(expr)
        columns.append(ColumnRef(alias=None, column=col_name, field_name=field_name, nested_in=f"{agg_key}.{fn_key}"))

    if not select_parts:
        select_parts.append("1")

    ref = _table_ref(table, use_catalog)
    sql = f'SELECT {", ".join(select_parts)} FROM {ref}'

    # Process arguments (where)
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    _agg_vvals = ctx.virtual_columns.get(table.table_id)
    if "where" in args:
        where_sql = _compile_where(args["where"], collector, None, _agg_vvals)
        sql += f" WHERE {where_sql}"

    # Build nodes SQL: plain SELECT with same WHERE, no aggregate functions
    nodes_sql: str | None = None
    nodes_columns: list[ColumnRef] | None = None
    nodes_params: list = []
    if has_nodes:
        nodes_select_parts: list[str] = []
        nodes_cols: list[ColumnRef] = []
        assert field_node.selection_set is not None
        for sel in field_node.selection_set.selections:
            if not isinstance(sel, FieldNode) or sel.name.value != "nodes":
                continue
            if sel.selection_set:
                for node_sel in sel.selection_set.selections:
                    if not isinstance(node_sel, FieldNode):
                        continue
                    col_name = node_sel.name.value
                    nodes_select_parts.append(_q(col_name))
                    nodes_cols.append(ColumnRef(
                        alias=None, column=col_name,
                        field_name=col_name, nested_in=None,
                    ))
        if nodes_select_parts:
            nodes_sql = f'SELECT {", ".join(nodes_select_parts)} FROM {ref}'
            if "where" in args:
                nodes_collector = ParamCollector()
                nodes_where_sql = _compile_where(args["where"], nodes_collector, None, _agg_vvals)
                nodes_sql += f" WHERE {nodes_where_sql}"
                nodes_params = nodes_collector.params
            nodes_columns = nodes_cols

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        nodes_sql=nodes_sql,
        nodes_columns=nodes_columns,
        nodes_params=nodes_params,
        agg_alias=agg_key,
    )


def _sql_literal(val: object) -> str:
    """Convert a Python value to a SQL literal for VALUES injection."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(val)
    if isinstance(val, str):
        escaped = val.replace("'", "''")
        return f"'{escaped}'"
    return f"'{val!s}'"


def rewrite_hot_joins(compiled: CompiledQuery, hot_manager: object) -> CompiledQuery:
    """Rewrite JOINs targeting hot tables to use VALUES-based CTEs.

    When a LEFT JOIN target is a hot-cached table, replace the table reference
    with a CTE containing the cached rows as VALUES. This works cross-source
    since the data travels as constants in the query.
    """
    from provisa.cache.hot_tables import HotTableManager
    assert isinstance(hot_manager, HotTableManager)

    sql = compiled.sql
    ctes: list[str] = []

    # Match LEFT JOIN patterns: LEFT JOIN "schema"."table" "alias" ON ...
    # or LEFT JOIN "catalog"."schema"."table" "alias" ON ...
    join_pattern = _re.compile(
        r'LEFT JOIN\s+'
        r'(?:"[^"]+"\.)?' r'"[^"]+"\.' r'"([^"]+)"'  # table name in last segment
        r'\s+"([^"]+)"'  # alias
        r'\s+ON\s+(.+?)(?=\s+(?:LEFT JOIN|WHERE|ORDER BY|LIMIT|OFFSET)\b|\Z)',
        _re.IGNORECASE,
    )

    for match in reversed(list(join_pattern.finditer(sql))):
        table_name = match.group(1)
        alias = match.group(2)
        on_clause = match.group(3)

        if not hot_manager.is_hot(table_name):
            continue

        entry = hot_manager.get_entry(table_name)
        if entry is None or not entry.rows:
            continue

        # Build VALUES rows
        cte_name = f"_hot_{table_name}"
        col_names = entry.column_names

        value_rows = []
        for row in entry.rows:
            vals = [_sql_literal(row.get(c)) for c in col_names]
            value_rows.append(f"({', '.join(vals)})")

        col_defs = ", ".join(f'"{c}"' for c in col_names)
        cte_sql = (
            f"{cte_name}({col_defs}) AS "
            f"(VALUES {', '.join(value_rows)})"
        )
        ctes.append(cte_sql)

        # Replace the JOIN target with the CTE name
        new_join = (
            f'LEFT JOIN "{cte_name}" "{alias}" ON {on_clause}'
        )
        sql = sql[:match.start()] + new_join + sql[match.end():]

    if ctes:
        new_ctes_sql = ", ".join(ctes)
        _with_re = _re.compile(r"^\s*WITH\s+", _re.IGNORECASE)
        if _with_re.match(sql):
            sql = _with_re.sub(f"WITH {new_ctes_sql}, ", sql)
        else:
            sql = f"WITH {new_ctes_sql} " + sql

    if sql != compiled.sql:
        return CompiledQuery(
            sql=sql,
            params=compiled.params,
            root_field=compiled.root_field,
            canonical_field=compiled.canonical_field,
            columns=compiled.columns,
            sources=compiled.sources,
        )
    return compiled


def _extract_node_selections(field_node: FieldNode) -> list:
    """Extract selections from edges.node in a connection field."""
    if not field_node.selection_set:
        return []
    for sel in field_node.selection_set.selections:
        if isinstance(sel, FieldNode) and sel.name.value == "edges":
            if sel.selection_set:
                for edge_sel in sel.selection_set.selections:
                    if isinstance(edge_sel, FieldNode) and edge_sel.name.value == "node":
                        if edge_sel.selection_set:
                            return list(edge_sel.selection_set.selections)
    return []


def _compile_connection_field(
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
) -> CompiledQuery:
    """Compile a _connection root query field to SQL with cursor pagination."""
    from provisa.compiler.cursor import apply_cursor_pagination, extract_sort_columns, reverse_order

    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    for sel in _extract_node_selections(field_node):
        if not isinstance(sel, FieldNode):
            continue
        sel_name = sel.name.value
        phys_name = ctx.gql_to_physical.get((table.table_id, sel_name), sel_name)
        select_parts.append(_q(phys_name))
        columns.append(ColumnRef(None, phys_name, sel_name, None))

    if not select_parts:
        select_parts.append("1")

    ref = _table_ref(table, use_catalog)
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    sort_columns = extract_sort_columns(args)
    for sc in sort_columns:
        if sc not in [c.field_name for c in columns]:
            select_parts.append(_q(sc))
            columns.append(ColumnRef(None, sc, sc, None))

    sql = f'SELECT {", ".join(select_parts)} FROM {ref}'

    where_parts: list[str] = []
    if "where" in args:
        _conn_vvals = ctx.virtual_columns.get(table.table_id)
        where_parts.append(_compile_where(args["where"], collector, None, _conn_vvals))

    cursor_where, effective_limit, is_backward = apply_cursor_pagination(
        args, sort_columns, collector, None,
    )
    if cursor_where:
        where_parts.append(cursor_where)
    if where_parts:
        sql += f" WHERE {' AND '.join(where_parts)}"

    if "order_by" in args:
        order_by_val = args["order_by"]
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        order_sql = _compile_order_by(order_by_val, None)
        if is_backward:
            order_sql = reverse_order(order_sql)
        sql += f" ORDER BY {order_sql}"
    else:
        direction = "DESC" if is_backward else "ASC"
        sql += f' ORDER BY "id" {direction}'

    page_size = args.get("first") or args.get("last")
    if effective_limit is not None:
        sql += f" LIMIT {collector.add(int(effective_limit))}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        is_connection=True,
        is_backward=is_backward,
        sort_columns=sort_columns,
        page_size=int(page_size) if page_size is not None else None,
        has_cursor=("after" in args or "before" in args),
    )


def compile_query(
    document: DocumentNode,
    ctx: CompilationContext,
    variables: dict | None = None,
    use_catalog: bool = False,
    flat: bool = False,
) -> list[CompiledQuery]:
    """Compile a validated GraphQL document to SQL queries.

    Args:
        document: Validated GraphQL DocumentNode.
        ctx: Compilation context mapping GraphQL names to physical metadata.
        variables: Optional GraphQL variable values.
        use_catalog: If True, emit catalog-qualified table names (for Trino).

    Returns one CompiledQuery per root query field in the document.
    """
    results: list[CompiledQuery] = []

    for definition in document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        for sel in definition.selection_set.selections:
            if isinstance(sel, FieldNode):
                field_name = sel.name.value
                if field_name not in ctx.tables:
                    raise ValueError(
                        f"Unknown root query field: {field_name!r}"
                    )
                with _tracer.start_as_current_span("compiler.compile_query") as span:
                    span.set_attribute("graphql.field", field_name)
                    if field_name.endswith("_aggregate"):
                        compiled = _compile_aggregate_field(
                            sel, ctx, variables, use_catalog,
                        )
                    elif field_name.endswith("_connection"):
                        compiled = _compile_connection_field(
                            sel, ctx, variables, use_catalog,
                        )
                    else:
                        compiled = _compile_root_field(
                            sel, ctx, variables, use_catalog, flat=flat,
                        )
                    span.set_attribute("db.statement", compiled.sql[:1000])
                # Track source tables for warm-table promotion (REQ-AD5)
                table_meta = ctx.tables.get(field_name)
                if table_meta:
                    fqn = (
                        f'"{table_meta.catalog_name}"'
                        f'."{table_meta.schema_name}"'
                        f'."{table_meta.table_name}"'
                    )
                    query_counter.increment(fqn)
                results.append(compiled)

    return results
