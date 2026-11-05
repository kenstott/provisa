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

Mutations always route direct to RDBMS (never the engine).
No mutations for NoSQL sources.
"""

# Requirements: REQ-031, REQ-032, REQ-033, REQ-034, REQ-035, REQ-036, REQ-037, REQ-212, REQ-214

from __future__ import annotations

from collections.abc import Callable

from dataclasses import dataclass, replace

from graphql import DocumentNode, FieldNode, OperationDefinitionNode


from provisa.compiler.params import ParamCollector
from provisa.compiler.sql_gen import CompilationContext, TableMeta, _q
from provisa.compiler.sql_rewrite import translate_epoch_temporal_columns
from provisa.compiler.sql_where import _compile_where, _extract_value


# NoSQL source types — mutations not supported


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
    field_name: str,
    ctx: CompilationContext,
) -> tuple[str, str, TableMeta]:
    """Parse mutation field name and return (operation, table_field_name, TableMeta).

    Handles both camelCase (insertOrders, sa__insertOrders) and
    snake_case (insert_orders, sa__insert_orders) mutation naming styles.
    """
    domain_prefix = ""
    name = field_name
    if "__" in field_name:
        domain_prefix, name = field_name.split("__", 1)
        domain_prefix += "__"

    for op in ("upsert", "insert", "update", "delete"):
        # camelCase style: insertOrders → op=insert, rest=Orders → orders
        if name.startswith(op) and len(name) > len(op):
            rest = name[len(op) :]
            if rest[0].isupper():
                table_field = domain_prefix + rest[0].lower() + rest[1:]
                if table_field in ctx.tables:
                    return op, table_field, ctx.tables[table_field]
        # snake_case style: insert_orders → op=insert, rest=orders
        snake_prefix = op + "_"
        if name.startswith(snake_prefix) and len(name) > len(snake_prefix):
            table_field = domain_prefix + name[len(snake_prefix) :]
            if table_field in ctx.tables:
                return op, table_field, ctx.tables[table_field]
    raise ValueError(f"Unknown mutation field: {field_name!r}")


def _physical(table: TableMeta, ctx: CompilationContext) -> Callable[[str], str]:
    """A column's physical name from the published name a mutation's input, set, where or
    conflict columns name it by -- the names reads use (CompilationContext.exposed_to_physical,
    which holds an entry only where the two differ)."""

    def phys(name: str) -> str:
        return ctx.exposed_to_physical.get((table.table_id, name)) or name

    return phys


def _physical_keys(data: dict, phys: Callable[[str], str]) -> dict:
    return {phys(k): v for k, v in data.items()}


def _mutation_where(
    where: dict, table: TableMeta, ctx: CompilationContext, collector: ParamCollector
) -> str:
    """An update's or delete's ``where``, compiled as a read's is: every operator its input type
    offers, ``_and``/``_or``, and columns named by their published names."""
    return _compile_where(
        where,
        collector,
        None,
        ctx.virtual_columns.get(table.table_id),
        table.table_id,
        ctx.exposed_to_physical,
    )


def compile_upsert(  # REQ-212, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile an upsert mutation to INSERT ... ON CONFLICT ... DO UPDATE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    phys = _physical(table, ctx)
    input_data = _physical_keys(args.get("input", {}), phys)
    if not input_data:
        raise ValueError("upsert mutation requires 'input' argument")

    on_conflict_cols = args.get("on_conflict", [])
    if not on_conflict_cols:
        raise ValueError(
            "upsert mutation requires 'on_conflict' argument (list of conflict columns)"
        )
    if isinstance(on_conflict_cols, str):
        on_conflict_cols = [on_conflict_cols]
    on_conflict_cols = [phys(c) for c in on_conflict_cols]

    columns = list(input_data.keys())
    placeholders = [collector.add(input_data[col]) for col in columns]
    cols_sql = ", ".join(_q(c) for c in columns)
    vals_sql = ", ".join(placeholders)
    conflict_sql = ", ".join(_q(c) for c in on_conflict_cols)

    # UPDATE all non-conflict columns on conflict
    update_cols = [c for c in columns if c not in on_conflict_cols]
    if update_cols:
        set_parts = [f"{_q(c)} = EXCLUDED.{_q(c)}" for c in update_cols]
        do_clause = f"DO UPDATE SET {', '.join(set_parts)}"
    else:
        do_clause = "DO NOTHING"

    returning = ", ".join(_q(c) for c in columns)

    sql = (
        f"INSERT INTO {_q(table.schema_name)}.{_q(table.table_name)}"
        f" ({cols_sql}) VALUES ({vals_sql})"
        f" ON CONFLICT ({conflict_sql}) {do_clause}"
        f" RETURNING {returning}"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="upsert",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=columns,
    )


def compile_insert(  # REQ-032, REQ-033, REQ-034, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile an insert mutation to INSERT SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    input_data = _physical_keys(args.get("input", {}), _physical(table, ctx))
    if not input_data:
        raise ValueError("insert mutation requires 'input' argument")

    columns = list(input_data.keys())
    placeholders = [collector.add(input_data[col]) for col in columns]
    cols_sql = ", ".join(_q(c) for c in columns)
    vals_sql = ", ".join(placeholders)

    # RETURNING all inserted columns
    returning = ", ".join(_q(c) for c in columns)

    sql = (
        f"INSERT INTO {_q(table.schema_name)}.{_q(table.table_name)}"
        f" ({cols_sql}) VALUES ({vals_sql})"
        f" RETURNING {returning}"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="insert",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=columns,
    )


def compile_update(  # REQ-032, REQ-033, REQ-034, REQ-035, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile an update mutation to UPDATE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    set_data = _physical_keys(args.get("set", {}), _physical(table, ctx))
    if not set_data:
        raise ValueError("update mutation requires 'set' argument")
    if not args.get("where"):
        raise ValueError("update mutation requires 'where' argument")

    # SET clause
    set_parts = []
    for col, val in set_data.items():
        placeholder = collector.add(val)
        set_parts.append(f"{_q(col)} = {placeholder}")
    set_sql = ", ".join(set_parts)

    where_sql = _mutation_where(args["where"], table, ctx, collector)

    updated_cols = list(set_data.keys())
    returning = ", ".join(_q(c) for c in updated_cols)

    sql = (
        f"UPDATE {_q(table.schema_name)}.{_q(table.table_name)}"
        f" SET {set_sql}"
        f" WHERE {where_sql}"
        f" RETURNING {returning}"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="update",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=updated_cols,
    )


def compile_delete(  # REQ-032, REQ-033, REQ-035, REQ-036
    field_node: FieldNode,
    table: TableMeta,
    variables: dict | None,
    ctx: CompilationContext,
) -> MutationResult:
    """Compile a delete mutation to DELETE SQL."""
    collector = ParamCollector()
    args = {}
    if field_node.arguments:
        for arg in field_node.arguments:
            args[arg.name.value] = _extract_value(arg.value, variables)

    if not args.get("where"):
        raise ValueError("delete mutation requires 'where' argument")
    where_sql = _mutation_where(args["where"], table, ctx, collector)

    sql = (
        f"DELETE FROM {_q(table.schema_name)}.{_q(table.table_name)} WHERE {where_sql} RETURNING *"
    )

    return MutationResult(
        sql=sql,
        params=collector.params,
        mutation_type="delete",
        table_name=table.table_name,
        source_id=table.source_id,
        returning_columns=[],
    )


def compile_mutation(  # REQ-031, REQ-032, REQ-033, REQ-036, REQ-037
    document: DocumentNode,
    ctx: CompilationContext,
    source_types: dict[str, str],
    variables: dict | None = None,
) -> list[MutationResult]:
    """Compile a GraphQL mutation document to SQL mutations.

    Raises ValueError for NoSQL sources or cross-source mutations.
    """
    results: list[MutationResult] = []

    for definition in document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        if definition.operation.value != "mutation":
            continue

        for sel in definition.selection_set.selections:
            if not isinstance(sel, FieldNode):
                continue

            op, _, table = _get_mutation_meta(sel.name.value, ctx)

            if op == "upsert":
                results.append(compile_upsert(sel, table, variables, ctx))
            elif op == "insert":
                results.append(compile_insert(sel, table, variables, ctx))
            elif op == "update":
                results.append(compile_update(sel, table, variables, ctx))
            elif op == "delete":
                results.append(compile_delete(sel, table, variables, ctx))

    # Reject cross-source mutations
    source_ids = {r.source_id for r in results}
    if len(source_ids) > 1:
        raise ValueError(f"Cross-source mutations not supported. Sources involved: {source_ids}")

    # REQ-1908: a mutation's SQL is physical as built (it never passes the semantic→physical
    # rewrite), so an epoch-stored temporal column is translated here: an ISO 8601 value is written
    # as its epoch number, and RETURNING reads it back as the temporal.
    results = [
        replace(r, sql=translate_epoch_temporal_columns(r.sql, ctx.epoch_columns)) for r in results
    ]

    return results
