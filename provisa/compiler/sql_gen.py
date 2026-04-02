# Copyright (c) 2025 Kenneth Stott
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

import re as _re

from dataclasses import dataclass, field

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

from provisa.compiler.params import ParamCollector


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


# --- Compiled query result ---


@dataclass(frozen=True)
class ColumnRef:
    """A column in the SELECT list with serialization metadata."""

    alias: str | None  # table alias ("t0") or None if no joins
    column: str  # physical column name
    field_name: str  # GraphQL field name
    nested_in: str | None  # relationship field name, or None for root


@dataclass
class CompiledQuery:
    """Result of compiling a single GraphQL root query field."""

    sql: str
    params: list
    root_field: str  # GraphQL root field name (e.g., "orders")
    columns: list[ColumnRef]
    sources: set[str]  # source_ids involved (for routing)


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
        )
        ctx.tables[t.field_name] = meta

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


def _compile_order_by(
    order_by_list: list[dict],
    alias: str | None,
) -> str:
    """Compile order_by input list to SQL ORDER BY clause."""
    parts: list[str] = []
    for item in order_by_list:
        field_name = item["field"]
        direction = item.get("direction", "ASC")
        col = _q(field_name) if alias is None else f"{_q(alias)}.{_q(field_name)}"
        parts.append(f"{col} {direction}")
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
                )
        else:
            # Scalar column from the parent join
            select_parts.append(
                f'{_q(parent_alias)}.{_q(nested_name)}'
            )
            columns.append(ColumnRef(
                alias=parent_alias,
                column=nested_name,
                field_name=nested_name,
                nested_in=nesting_path,
            ))
    return alias_counter


def _compile_root_field(
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
    use_catalog: bool = False,
) -> CompiledQuery:
    """Compile a single root query field to SQL."""
    root_name = field_node.name.value
    table = ctx.tables[root_name]
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
                )
        else:
            # Scalar field — check for JSON path extraction
            gql_field_name = sel_name
            col_path = ctx.column_paths.get((table.table_id, gql_field_name))
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
                select_parts.append(expr)
            elif use_aliases:
                select_parts.append(f'{_q(root_alias)}.{_q(sel_name)}')
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

    # LIMIT / OFFSET
    if "limit" in args:
        sql += f" LIMIT {int(args['limit'])}"
    if "offset" in args:
        sql += f" OFFSET {int(args['offset'])}"

    return CompiledQuery(
        sql=sql,
        params=collector.params,
        root_field=root_name,
        columns=columns,
        sources=sources,
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
                if sel.name.value not in ctx.tables:
                    raise ValueError(
                        f"Unknown root query field: {sel.name.value!r}"
                    )
                results.append(
                    _compile_root_field(sel, ctx, variables, use_catalog)
                )

    return results
