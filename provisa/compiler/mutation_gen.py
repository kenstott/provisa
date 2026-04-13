# Copyright (c) 2026 Kenneth Stott
# Canary: d4fe42b5-d17d-40d8-84e4-ad686c67af0d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Compile GraphQL mutations → INSERT/UPDATE/DELETE SQL (REQ-031–REQ-037).

Mutations always route direct to RDBMS (never Trino).
No mutations for NoSQL sources.
"""

from __future__ import annotations

from dataclasses import dataclass

from graphql import DocumentNode, FieldNode

from datetime import datetime, timezone

from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_gen import CompilationContext, TableMeta, _q, _extract_value


# NoSQL source types — mutations not supported
NOSQL_TYPES: set[str] = {"mongodb", "cassandra"}


_INT_TYPES = frozenset({"integer", "int", "int4", "int2", "int8", "bigint", "smallint", "tinyint"})
_FLOAT_TYPES = frozenset({"float", "double", "real", "decimal", "numeric", "double precision"})
_BOOL_TYPES = frozenset({"boolean", "bool"})


def _coerce_preset_value(raw: str, data_type: str | None) -> object:
    """Coerce a string preset value to the appropriate Python type."""
    if data_type is None:
        return raw
    base = data_type.lower().split("(")[0].strip()
    if base in _INT_TYPES:
        return int(raw)
    if base in _FLOAT_TYPES:
        return float(raw)
    if base in _BOOL_TYPES:
        return raw.lower() in ("true", "1", "yes")
    return raw


def apply_column_presets(
    input_data: dict,
    presets: list[dict],
    headers: dict[str, str] | None = None,
) -> dict:
    """Inject column preset values into mutation input data.

    Preset columns override any user-supplied values (security enforcement).
    """
    result = dict(input_data)
    for preset in presets:
        col = preset["column"]
        source = preset["source"]
        data_type = preset.get("data_type")
        if source == "now":
            result[col] = datetime.now(timezone.utc).isoformat()
        elif source == "header":
            header_name = preset.get("name", "")
            if headers and header_name in headers:
                result[col] = _coerce_preset_value(headers[header_name], data_type)
        elif source == "literal":
            raw = preset.get("value", "")
            result[col] = _coerce_preset_value(raw, data_type)
    return result


@dataclass
class MutationResult:
    """Result of compiling a mutation."""

    sql: str
    params: list
    mutation_type: str  # "insert", "update", "delete"
    table_name: str
    source_id: str
    returning_columns: list[str]


def _get_mutation_meta(
    field_name: str, ctx: CompilationContext,
) -> tuple[str, str, TableMeta]:
    """Parse mutation field name and return (operation, table_field_name, TableMeta).

    Mutation fields are named: insert_<table>, update_<table>, delete_<table>.
    """
    for prefix in ("upsert_", "insert_", "update_", "delete_"):
        if field_name.startswith(prefix):
            op = prefix.rstrip("_")
            table_field = field_name[len(prefix):]
            if table_field in ctx.tables:
                return op, table_field, ctx.tables[table_field]
    raise ValueError(f"Unknown mutation field: {field_name!r}")


def compile_upsert(
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    headers: dict[str, str] | None = None,
) -> MutationResult:
    """Compile an upsert mutation to INSERT ... ON CONFLICT ... DO UPDATE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    input_data = args.get("input", {})
    if table.column_presets:
        input_data = apply_column_presets(input_data, table.column_presets, headers)
    if not input_data:
        raise ValueError("upsert mutation requires 'input' argument")

    on_conflict_cols = args.get("on_conflict", [])
    if not on_conflict_cols:
        raise ValueError("upsert mutation requires 'on_conflict' argument (list of conflict columns)")
    if isinstance(on_conflict_cols, str):
        on_conflict_cols = [on_conflict_cols]

    columns = list(input_data.keys())
    placeholders = [collector.add(input_data[col]) for col in columns]
    cols_sql = ", ".join(_q(c) for c in columns)
    vals_sql = ", ".join(placeholders)
    conflict_sql = ", ".join(_q(c) for c in on_conflict_cols)

    # UPDATE all non-conflict columns on conflict
    update_cols = [c for c in columns if c not in on_conflict_cols]
    if update_cols:
        set_parts = [f'{_q(c)} = EXCLUDED.{_q(c)}' for c in update_cols]
        do_clause = f'DO UPDATE SET {", ".join(set_parts)}'
    else:
        do_clause = "DO NOTHING"

    returning = ", ".join(_q(c) for c in columns)

    sql = (
        f'INSERT INTO {_q(table.schema_name)}.{_q(table.table_name)}'
        f' ({cols_sql}) VALUES ({vals_sql})'
        f' ON CONFLICT ({conflict_sql}) {do_clause}'
        f' RETURNING {returning}'
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="upsert",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=columns,
    )


def compile_insert(
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    headers: dict[str, str] | None = None,
) -> MutationResult:
    """Compile an insert mutation to INSERT SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    input_data = args.get("input", {})
    if table.column_presets:
        input_data = apply_column_presets(input_data, table.column_presets, headers)
    if not input_data:
        raise ValueError("insert mutation requires 'input' argument")

    columns = list(input_data.keys())
    placeholders = [collector.add(input_data[col]) for col in columns]
    cols_sql = ", ".join(_q(c) for c in columns)
    vals_sql = ", ".join(placeholders)

    # RETURNING all inserted columns
    returning = ", ".join(_q(c) for c in columns)

    sql = (
        f'INSERT INTO {_q(table.schema_name)}.{_q(table.table_name)}'
        f' ({cols_sql}) VALUES ({vals_sql})'
        f' RETURNING {returning}'
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="insert",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=columns,
    )


def compile_update(
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
) -> MutationResult:
    """Compile an update mutation to UPDATE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    set_data = args.get("set", {})
    where_data = args.get("where", {})
    if not set_data:
        raise ValueError("update mutation requires 'set' argument")
    if not where_data:
        raise ValueError("update mutation requires 'where' argument")

    # SET clause
    set_parts = []
    for col, val in set_data.items():
        placeholder = collector.add(val)
        set_parts.append(f'{_q(col)} = {placeholder}')
    set_sql = ", ".join(set_parts)

    # WHERE clause (simple equality filters)
    where_parts = []
    for col, filter_obj in where_data.items():
        for op, val in filter_obj.items():
            placeholder = collector.add(val)
            if op == "eq":
                where_parts.append(f'{_q(col)} = {placeholder}')
    where_sql = " AND ".join(where_parts)

    updated_cols = list(set_data.keys())
    returning = ", ".join(_q(c) for c in updated_cols)

    sql = (
        f'UPDATE {_q(table.schema_name)}.{_q(table.table_name)}'
        f' SET {set_sql}'
        f' WHERE {where_sql}'
        f' RETURNING {returning}'
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="update",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=updated_cols,
    )


def compile_delete(
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
) -> MutationResult:
    """Compile a delete mutation to DELETE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    where_data = args.get("where", {})
    if not where_data:
        raise ValueError("delete mutation requires 'where' argument")

    where_parts = []
    for col, filter_obj in where_data.items():
        for op, val in filter_obj.items():
            placeholder = collector.add(val)
            if op == "eq":
                where_parts.append(f'{_q(col)} = {placeholder}')
    where_sql = " AND ".join(where_parts)

    sql = (
        f'DELETE FROM {_q(table.schema_name)}.{_q(table.table_name)}'
        f' WHERE {where_sql}'
        f' RETURNING *'
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="delete",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=[],
    )


def compile_mutation(
    document: DocumentNode,
    ctx: CompilationContext,
    source_types: dict[str, str],
    variables: dict | None = None,
    headers: dict[str, str] | None = None,
) -> list[MutationResult]:
    """Compile a GraphQL mutation document to SQL mutations.

    Raises ValueError for NoSQL sources or cross-source mutations.
    """
    results: list[MutationResult] = []

    for definition in document.definitions:
        if not hasattr(definition, "selection_set"):
            continue
        if hasattr(definition, "operation") and definition.operation.value != "mutation":
            continue

        for sel in definition.selection_set.selections:
            if not isinstance(sel, FieldNode):
                continue

            op, table_field, table = _get_mutation_meta(sel.name.value, ctx)

            # Reject NoSQL mutations
            stype = source_types.get(table.source_id, "")
            if stype in NOSQL_TYPES:
                raise ValueError(
                    f"Mutations not supported for NoSQL source "
                    f"{table.source_id!r} (type: {stype})"
                )

            if op == "upsert":
                results.append(compile_upsert(sel, table, variables, headers))
            elif op == "insert":
                results.append(compile_insert(sel, table, variables, headers))
            elif op == "update":
                results.append(compile_update(sel, table, variables))
            elif op == "delete":
                results.append(compile_delete(sel, table, variables))

    # Reject cross-source mutations
    source_ids = {r.source_id for r in results}
    if len(source_ids) > 1:
        raise ValueError(
            f"Cross-source mutations not supported. "
            f"Sources involved: {source_ids}"
        )

    return results


def inject_rls_into_mutation(
    mutation: MutationResult,
    table_id: int,
    rls_rules: dict[int, str],
) -> MutationResult:
    """Inject RLS WHERE clause into UPDATE/DELETE mutations."""
    if mutation.mutation_type == "insert":
        return mutation  # INSERT doesn't have WHERE

    if table_id not in rls_rules:
        return mutation

    rls_filter = rls_rules[table_id]
    # AND the RLS filter into the existing WHERE
    sql = mutation.sql
    import re
    where_match = re.search(r'\bWHERE\b', sql, re.IGNORECASE)
    if where_match:
        pos = where_match.end()
        sql = f"{sql[:pos]} ({rls_filter}) AND{sql[pos:]}"
    else:
        sql = f"{sql} WHERE ({rls_filter})"

    return MutationResult(
        sql=sql,
        params=mutation.params,
        mutation_type=mutation.mutation_type,
        table_name=mutation.table_name,
        source_id=mutation.source_id,
        returning_columns=mutation.returning_columns,
    )
