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

import json as _json
import re as _re

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
    StringValueNode,
    VariableNode,
)

from provisa.compiler.aggregate_gen import _is_comparable, _is_numeric
from provisa.compiler.params import ParamCollector
from provisa.cache.warm_tables import QueryCounter

# Module-level query counter for warm-table tracking (REQ-AD5)
query_counter = QueryCounter()


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
    table_name: str
    domain_id: str = ""  # semantic domain name (as JDBC clients see it)
    column_presets: list = field(default_factory=list)


@dataclass(frozen=True)
class JoinMeta:
    """Join metadata for a relationship field on a GraphQL type."""

    source_column: str
    target_column: str
    source_column_type: str  # Trino data type (e.g. "integer", "varchar")
    target_column_type: str  # Trino data type on target side
    target: TableMeta
    cardinality: str  # "many-to-one" or "one-to-many"


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


# --- Compiled query result ---


@dataclass(frozen=True)
class ColumnRef:
    """A column in the SELECT list with serialization metadata."""

    alias: str | None  # table alias ("t0") or None if no joins
    column: str  # physical column name
    field_name: str  # GraphQL field name
    nested_in: str | None  # relationship field name, or None for root
    cardinality: str | None = None  # "many-to-one", "one-to-many", or None for root


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


# --- Build CompilationContext from SchemaInput ---


def _lookup_column_type(si: object, table_id: int, column_name: str) -> str:
    """Look up a column's Trino data type from the SchemaInput."""
    col_metas = si.column_types.get(table_id, [])
    for meta in col_metas:
        if meta.column_name == column_name:
            return meta.data_type
    return "varchar"  # safe fallback


def build_context(si: object) -> CompilationContext:
    """Build CompilationContext from a SchemaInput.

    This mirrors the logic in schema_gen._build_visible_tables and _assign_names
    to produce the same field_name/type_name mapping.
    """
    from provisa.compiler.naming import generate_name, to_type_name
    from provisa.compiler.schema_gen import SchemaInput, _build_visible_tables, _assign_names

    assert isinstance(si, SchemaInput)
    ctx = CompilationContext()

    tables = _build_visible_tables(si)
    if not tables:
        return ctx

    _assign_names(tables, si.naming_rules, domain_prefix=si.domain_prefix)

    table_lookup = {t.table_id: t for t in tables}

    physical_map = getattr(si, "physical_table_map", None) or {}
    table_preset_map = {td["id"]: list(td.get("column_presets") or []) for td in si.tables}

    for t in tables:
        physical_name = physical_map.get(t.table_name, t.table_name)
        meta = TableMeta(
            table_id=t.table_id,
            field_name=t.field_name,
            type_name=t.type_name,
            source_id=t.source_id,
            catalog_name=t.source_id.replace("-", "_"),
            schema_name=t.schema_name,
            table_name=physical_name,
            domain_id=t.domain_id,
            column_presets=table_preset_map.get(t.table_id, []),
        )
        ctx.tables[t.field_name] = meta
        # Register aggregate variant pointing to same TableMeta
        ctx.tables[f"{t.field_name}_aggregate"] = meta
        # Register connection variant for cursor pagination
        ctx.tables[f"{t.field_name}_connection"] = meta

        # Store column metadata for aggregate compilation
        col_info = []
        for col in t.visible_columns:
            col_name = col["column_name"]
            col_meta = t.column_metadata.get(col_name)
            if col_meta:
                col_info.append((col_name, col_meta.data_type))
        ctx.aggregate_columns[t.table_id] = col_info

        # Populate column paths for JSON extraction
        for col in t.visible_columns:
            col_path = col.get("path")
            if col_path:
                gql_name = col.get("alias") or col["column_name"]
                ctx.column_paths[(t.table_id, gql_name)] = col_path

    # Build join metadata from visible relationships
    for rel in si.relationships:
        src_id = rel["source_table_id"]
        tgt_id = rel["target_table_id"]
        if src_id not in table_lookup or tgt_id not in table_lookup:
            continue

        src_info = table_lookup[src_id]
        tgt_info = table_lookup[tgt_id]

        tgt_meta = TableMeta(
            table_id=tgt_info.table_id,
            field_name=tgt_info.field_name,
            type_name=tgt_info.type_name,
            source_id=tgt_info.source_id,
            catalog_name=tgt_info.source_id.replace("-", "_"),
            schema_name=tgt_info.schema_name,
            table_name=tgt_info.table_name,
            domain_id=tgt_info.domain_id,
        )

        # Look up column types for the join columns
        src_col_type = _lookup_column_type(si, src_id, rel["source_column"])
        tgt_col_type = _lookup_column_type(si, tgt_id, rel["target_column"])

        # The relationship field on the source type uses target's field_name
        ctx.joins[(src_info.type_name, tgt_info.field_name)] = JoinMeta(
            source_column=rel["source_column"],
            target_column=rel["target_column"],
            source_column_type=src_col_type,
            target_column_type=tgt_col_type,
            target=tgt_meta,
            cardinality=rel["cardinality"],
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
) -> str:
    """Compile a where input object to a SQL WHERE clause fragment."""
    parts: list[str] = []

    for key, value in where_obj.items():
        if key == "_and":
            sub_parts = [_compile_where(sub, collector, alias) for sub in value]
            parts.append(f"({' AND '.join(sub_parts)})")
            continue
        if key == "_or":
            sub_parts = [_compile_where(sub, collector, alias) for sub in value]
            parts.append(f"({' OR '.join(sub_parts)})")
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


def _join_column_expr(alias: str, column: str, my_type: str, other_type: str) -> str:
    """Build a column expression, adding CAST only when types are incompatible."""
    col = f'{_q(alias)}.{_q(column)}'
    if _types_compatible(my_type, other_type):
        return col
    cast_type = _common_cast_type(my_type, other_type)
    return f'CAST({col} AS {cast_type})'


# --- Table reference helpers ---


def _table_ref(meta: TableMeta, use_catalog: bool) -> str:
    """Build a fully qualified table reference."""
    if use_catalog:
        return f'{_q(meta.catalog_name)}.{_q(meta.schema_name)}.{_q(meta.table_name)}'
    return f'{_q(meta.schema_name)}.{_q(meta.table_name)}'


def _semantic_table_ref(meta: TableMeta) -> str:
    """Semantic table reference: domain_id.field_name (as JDBC clients see it)."""
    from provisa.compiler.naming import domain_to_sql_name
    return f'{_q(domain_to_sql_name(meta.domain_id))}.{_q(meta.field_name)}'


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
    for phys in sorted(replacements, key=len, reverse=True):
        sql = sql.replace(phys, replacements[phys])
    return sql


def rewrite_semantic_to_physical(sql: str, ctx: CompilationContext) -> str:
    """Replace semantic (domain.field_name) refs with physical (schema.table) refs."""
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        semantic = _semantic_table_ref(meta)
        replacements[semantic] = _table_ref(meta, use_catalog=False)
    for sem in sorted(replacements, key=len, reverse=True):
        sql = sql.replace(sem, replacements[sem])
    return sql


def rewrite_semantic_to_trino_physical(sql: str, ctx: CompilationContext) -> str:
    """Replace semantic (domain.field_name) refs with Trino catalog-qualified refs."""
    replacements: dict[str, str] = {}
    seen: set[tuple[str, str, str]] = set()
    for meta in ctx.tables.values():
        key = (meta.catalog_name, meta.schema_name, meta.table_name)
        if key in seen:
            continue
        seen.add(key)
        semantic = _semantic_table_ref(meta)
        replacements[semantic] = _table_ref(meta, use_catalog=True)
    for sem in sorted(replacements, key=len, reverse=True):
        sql = sql.replace(sem, replacements[sem])
    return sql


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
    cardinality: str | None = None,
) -> int:
    """Recursively collect columns and JOINs from nested selections."""
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

            src_expr = _join_column_expr(
                parent_alias, nested_join_meta.source_column,
                nested_join_meta.source_column_type, nested_join_meta.target_column_type,
            )
            tgt_expr = _join_column_expr(
                nested_alias, nested_join_meta.target_column,
                nested_join_meta.target_column_type, nested_join_meta.source_column_type,
            )
            join_clauses.append(
                f'LEFT JOIN {_table_ref(nested_join_meta.target, use_catalog)}'
                f' {_q(nested_alias)}'
                f' ON {src_expr} = {tgt_expr}'
            )

            sub_path = f"{nesting_path}.{nested_name}"
            if nested_sel.selection_set:
                alias_counter = _collect_nested_columns(
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
                    cardinality=nested_join_meta.cardinality,
                )
        else:
            # Scalar column from the parent join
            nested_response_key = nested_sel.alias.value if nested_sel.alias else nested_name
            col_expr = f'{_q(parent_alias)}.{_q(nested_name)}'
            if nested_sel.alias:
                select_parts.append(f'{col_expr} AS {_q(nested_response_key)}')
            else:
                select_parts.append(col_expr)
            columns.append(ColumnRef(
                alias=parent_alias,
                column=nested_name,
                field_name=nested_response_key,
                nested_in=nesting_path,
                cardinality=cardinality,
            ))
    return alias_counter


def _compile_root_field(
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
) -> CompiledQuery:
    """Compile a single root query field to SQL."""
    root_name = field_node.alias.value if field_node.alias else field_node.name.value
    table = ctx.tables[field_node.name.value]
    collector = ParamCollector()
    sources: set[str] = {table.source_id}

    use_aliases = _has_joins(field_node, ctx, table.type_name)
    root_alias = "t0" if use_aliases else None
    alias_counter = 1

    # Collect SELECT columns and JOINs
    select_parts: list[str] = []
    columns: list[ColumnRef] = []
    join_clauses: list[str] = []

    for sel in field_node.selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue

        sel_name = sel.name.value
        join_key = (table.type_name, sel_name)

        if join_key in ctx.joins:
            # Relationship field → JOIN
            join_meta = ctx.joins[join_key]
            join_alias = f"t{alias_counter}"
            alias_counter += 1
            sources.add(join_meta.target.source_id)

            src_expr = _join_column_expr(
                root_alias, join_meta.source_column,
                join_meta.source_column_type, join_meta.target_column_type,
            )
            tgt_expr = _join_column_expr(
                join_alias, join_meta.target_column,
                join_meta.target_column_type, join_meta.source_column_type,
            )
            join_clauses.append(
                f'LEFT JOIN {_table_ref(join_meta.target, use_catalog)}'
                f' {_q(join_alias)}'
                f' ON {src_expr} = {tgt_expr}'
            )

            # Add nested columns (recursively handle sub-relationships)
            if sel.selection_set:
                alias_counter = _collect_nested_columns(
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
                    cardinality=join_meta.cardinality,
                )
        else:
            # Scalar field — check for JSON path extraction
            response_key = sel.alias.value if sel.alias else sel_name
            gql_field_name = response_key
            col_path = ctx.column_paths.get((table.table_id, sel_name))
            if col_path:
                # path is "source_col.key1.key2" → PG JSON extraction
                # Emits PG syntax; SQLGlot transpiles to Trino json_extract_scalar
                path_parts = col_path.split(".")
                source_col = path_parts[0]
                keys = path_parts[1:]
                if use_aliases:
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
            elif use_aliases:
                col_expr = f'{_q(root_alias)}.{_q(sel_name)}'
                if sel.alias:
                    select_parts.append(f'{col_expr} AS {_q(response_key)}')
                else:
                    select_parts.append(col_expr)
            else:
                if sel.alias:
                    select_parts.append(f'{_q(sel_name)} AS {_q(response_key)}')
                else:
                    select_parts.append(_q(sel_name))
            columns.append(ColumnRef(
                alias=root_alias,
                column=sel_name,
                field_name=gql_field_name,
                nested_in=None,
            ))

    # FROM clause
    ref = _table_ref(table, use_catalog)
    if use_aliases:
        from_clause = f'{ref} {_q(root_alias)}'
    else:
        from_clause = ref

    sql = f'SELECT {", ".join(select_parts)} FROM {from_clause}'

    # JOIN clauses
    for join_clause in join_clauses:
        sql += f" {join_clause}"

    # Process arguments
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

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
        where_sql = _compile_where(args["where"], collector, root_alias)
        sql += f" WHERE {where_sql}"

    # ORDER BY
    if "order_by" in args:
        order_by_val = args["order_by"]
        # order_by can be a single object or a list
        if isinstance(order_by_val, dict):
            order_by_val = [order_by_val]
        order_sql = _compile_order_by(order_by_val, root_alias)
        sql += f" ORDER BY {order_sql}"

    # LIMIT / OFFSET — always parameterized, never interpolated
    if "limit" in args:
        sql += f" LIMIT {collector.add(int(args['limit']))}"
    if "offset" in args:
        sql += f" OFFSET {collector.add(int(args['offset']))}"

    # Collect native filter args (any arg not handled by SQL compilation above)
    _STANDARD_ARGS = {"where", "order_by", "limit", "offset", "distinct_on"}
    api_args = {k: v for k, v in args.items() if k not in _STANDARD_ARGS}

    # Prepend a self-describing hint comment for external drivers (JDBC/ODBC/Arrow Flight).
    # Any driver that receives the compiled SQL can parse this to discover the REST call.
    # Format: -- @native_filter {"source_id": "...", "operation_id": "...", "args": {...}}
    if api_args:
        hint = _json.dumps(
            {"source_id": table.source_id, "operation_id": table.table_name, "args": api_args},
            separators=(",", ":"),
        )
        sql = f"-- @native_filter {hint}\n{sql}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        canonical_field=field_node.name.value,
        columns=columns,
        sources=sources,
        api_args=api_args,
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

    if "where" in args:
        where_sql = _compile_where(args["where"], collector, None)
        sql += f" WHERE {where_sql}"

    # Build nodes SQL: plain SELECT with same WHERE, no aggregate functions
    nodes_sql: str | None = None
    nodes_columns: list[ColumnRef] | None = None
    nodes_params: list = []
    if has_nodes:
        nodes_select_parts: list[str] = []
        nodes_cols: list[ColumnRef] = []
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
                nodes_where_sql = _compile_where(args["where"], nodes_collector, None)
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
        with_clause = "WITH " + ", ".join(ctes) + " "
        sql = with_clause + sql

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
        select_parts.append(_q(sel_name))
        columns.append(ColumnRef(None, sel_name, sel_name, None))

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
        where_parts.append(_compile_where(args["where"], collector, None))

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
        if not hasattr(definition, "selection_set"):
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
                            sel, ctx, variables, use_catalog,
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


_NATIVE_FILTER_RE = _re.compile(
    r"^--\s*@native_filter\s+(\{.+\})\s*$", _re.MULTILINE
)


def parse_native_filter_hint(sql: str) -> dict | None:
    """Extract the @native_filter hint from a compiled SQL string.

    Returns {"source_id": ..., "operation_id": ..., "args": {...}} or None.
    Intended for external drivers (JDBC/ODBC/Arrow Flight) that receive compiled
    SQL and need to know which REST call to make before executing the query.
    """
    m = _NATIVE_FILTER_RE.search(sql)
    if not m:
        return None
    try:
        return _json.loads(m.group(1))
    except Exception:
        return None
