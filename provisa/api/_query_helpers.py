# Copyright (c) 2026 Kenneth Stott
# Canary: 9f58cd0e-b512-4600-a8b5-484b7ed78880
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException
from graphql import (
    GraphQLEnumType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLSchema,
)

log = logging.getLogger(__name__)


def get_scalar_fields(schema: GraphQLSchema, table: str) -> list[str]:
    """Get only scalar (non-object) field names for a root query type."""
    query_type = schema.query_type
    if query_type is None:
        return []
    field_map = query_type.fields
    if table not in field_map:
        return []
    gql_field = field_map[table]
    return_type = gql_field.type
    while hasattr(return_type, "of_type"):
        return_type = return_type.of_type
    if not isinstance(return_type, GraphQLObjectType):
        return []
    scalars = []
    for name, f in return_type.fields.items():
        inner = f.type
        while isinstance(inner, (GraphQLNonNull, GraphQLList)):
            inner = inner.of_type
        if isinstance(inner, (GraphQLScalarType, GraphQLEnumType)):
            scalars.append(name)
    return scalars


def build_graphql_query(
    table: str,
    fields: list[str],
    where: dict[str, dict[str, Any]],
    order_by: list[dict[str, str]],
    limit: int | None,
    offset: int | None,
) -> str:
    """Build a GraphQL query string from table/field/filter parameters."""
    args_parts = []

    if limit is not None:
        args_parts.append(f"limit: {limit}")
    if offset is not None:
        args_parts.append(f"offset: {offset}")

    if where:
        where_parts = []
        for col, ops in where.items():
            for op, val in ops.items():
                if isinstance(val, list):
                    formatted = "[" + ", ".join(f'"{v}"' for v in val) + "]"
                    where_parts.append(f'{col}: {{{op}: {formatted}}}')
                elif isinstance(val, str):
                    try:
                        numeric = int(val)
                        where_parts.append(f"{col}: {{{op}: {numeric}}}")
                    except ValueError:
                        try:
                            numeric = float(val)
                            where_parts.append(f"{col}: {{{op}: {numeric}}}")
                        except ValueError:
                            where_parts.append(f'{col}: {{{op}: "{val}"}}')
                else:
                    where_parts.append(f"{col}: {{{op}: {val}}}")
        if where_parts:
            args_parts.append("where: {" + ", ".join(where_parts) + "}")

    if order_by:
        ob_parts = [f'{o["field"]}: {o["dir"]}' for o in order_by]
        args_parts.append("order_by: {" + ", ".join(ob_parts) + "}")

    args_str = f"({', '.join(args_parts)})" if args_parts else ""
    fields_str = " ".join(fields)
    return f"{{ {table}{args_str} {{ {fields_str} }} }}"


async def route_and_execute(compiled, state) -> Any:
    """Route a compiled query to the correct executor and return the result.

    Args:
        compiled: A compiled query object (sql, sources, params, columns).
        state: AppState with source_types, source_dialects, source_pools, trino_conn.

    Returns:
        Execution result with .rows and .columns.

    Raises:
        HTTPException: On execution failure or missing Trino connection.
    """
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile

    has_json_extract = "->>" in compiled.sql
    decision = decide_route(
        sources=compiled.sources,
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        has_json_extract=has_json_extract,
    )

    if decision.route == Route.DIRECT and decision.source_id:
        from provisa.executor.direct import execute_direct
        target_sql = transpile(compiled.sql, decision.dialect or "postgres")
        return await execute_direct(
            state.source_pools, decision.source_id,
            target_sql, compiled.params,
        )

    from provisa.executor.trino import execute_trino
    from provisa.transpiler.transpile import transpile_to_trino
    if state.trino_conn is None:
        raise HTTPException(status_code=503, detail="Trino not connected")
    trino_sql = transpile_to_trino(compiled.sql)
    return execute_trino(state.trino_conn, trino_sql, compiled.params)
