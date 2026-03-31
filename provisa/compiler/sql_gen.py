# Copyright (c) 2025 Kenneth Stott
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
    schema_name: str
    table_name: str


@dataclass(frozen=True)
class JoinMeta:
    """Join metadata for a relationship field on a GraphQL type."""

    source_column: str
    target_column: str
    target: TableMeta
    cardinality: str  # "many-to-one" or "one-to-many"


@dataclass
class CompilationContext:
    """Maps GraphQL names to physical table/join metadata."""

    # Root query field_name → TableMeta
    tables: dict[str, TableMeta] = field(default_factory=dict)
    # (source_type_name, relationship_field_name) → JoinMeta
    joins: dict[tuple[str, str], JoinMeta] = field(default_factory=dict)


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

    _assign_names(tables, si.naming_rules)

    table_lookup = {t.table_id: t for t in tables}

    for t in tables:
        meta = TableMeta(
            table_id=t.table_id,
            field_name=t.field_name,
            type_name=t.type_name,
            source_id=t.source_id,
            schema_name=t.schema_name,
            table_name=t.table_name,
        )
        ctx.tables[t.field_name] = meta

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
            schema_name=tgt_info.schema_name,
            table_name=tgt_info.table_name,
        )

        # The relationship field on the source type uses target's field_name
        ctx.joins[(src_info.type_name, tgt_info.field_name)] = JoinMeta(
            source_column=rel["source_column"],
            target_column=rel["target_column"],
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
                placeholder = collector.add(val)
                parts.append(f"{col} = {placeholder}")
            elif op == "neq":
                placeholder = collector.add(val)
                parts.append(f"{col} != {placeholder}")
            elif op == "gt":
                placeholder = collector.add(val)
                parts.append(f"{col} > {placeholder}")
            elif op == "gte":
                placeholder = collector.add(val)
                parts.append(f"{col} >= {placeholder}")
            elif op == "lt":
                placeholder = collector.add(val)
                parts.append(f"{col} < {placeholder}")
            elif op == "lte":
                placeholder = collector.add(val)
                parts.append(f"{col} <= {placeholder}")
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


def _compile_root_field(
    field_node: FieldNode,
    ctx: CompilationContext,
    variables: dict | None,
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

            join_clauses.append(
                f'LEFT JOIN {_q(join_meta.target.schema_name)}.{_q(join_meta.target.table_name)}'
                f' {_q(join_alias)}'
                f' ON {_q(root_alias)}.{_q(join_meta.source_column)}'
                f' = {_q(join_alias)}.{_q(join_meta.target_column)}'
            )

            # Add nested columns
            if sel.selection_set:
                for nested_sel in sel.selection_set.selections:
                    if isinstance(nested_sel, FieldNode):
                        nested_name = nested_sel.name.value
                        select_parts.append(
                            f'{_q(join_alias)}.{_q(nested_name)}'
                        )
                        columns.append(ColumnRef(
                            alias=join_alias,
                            column=nested_name,
                            field_name=nested_name,
                            nested_in=sel_name,
                        ))
        else:
            # Scalar field
            if use_aliases:
                select_parts.append(f'{_q(root_alias)}.{_q(sel_name)}')
            else:
                select_parts.append(_q(sel_name))
            columns.append(ColumnRef(
                alias=root_alias,
                column=sel_name,
                field_name=sel_name,
                nested_in=None,
            ))

    # FROM clause
    if use_aliases:
        from_clause = f'{_q(table.schema_name)}.{_q(table.table_name)} {_q(root_alias)}'
    else:
        from_clause = f'{_q(table.schema_name)}.{_q(table.table_name)}'

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
) -> list[CompiledQuery]:
    """Compile a validated GraphQL document to SQL queries.

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
                results.append(_compile_root_field(sel, ctx, variables))

    return results
