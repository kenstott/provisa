# Copyright (c) 2026 Kenneth Stott
# Canary: 8c9db1ac-e073-437d-aed5-2110b8cea897
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REST endpoint auto-generation from compiled GraphQL schema (REQ-222).

For each root query field, generates GET /data/rest/{table}.
Query params map to GraphQL arguments:
  ?limit=10&offset=20           -> pagination
  ?where.amount.gt=100          -> WHERE clause
  ?order_by.created_at=desc     -> ORDER BY
  ?fields=id,amount             -> field selection
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from graphql import GraphQLObjectType, GraphQLSchema, parse, validate

from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, compile_query

log = logging.getLogger(__name__)

# Supported WHERE operators
_WHERE_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "in"}

# Map REST operator names to GraphQL filter argument names
_OP_MAP = {
    "eq": "eq",
    "neq": "neq",
    "gt": "gt",
    "gte": "gte",
    "lt": "lt",
    "lte": "lte",
    "like": "like",
    "in": "in",
}


def _parse_where_params(params: dict[str, str]) -> dict[str, dict[str, Any]]:
    """Parse where.column.op=value query params into structured filters.

    Returns {column_name: {op: value, ...}, ...}.
    """
    filters: dict[str, dict[str, Any]] = {}
    for key, value in params.items():
        if not key.startswith("where."):
            continue
        parts = key.split(".")
        if len(parts) != 3:
            continue
        _, col, op = parts
        if op not in _WHERE_OPS:
            continue
        if op == "in":
            value = value.split(",")
        filters.setdefault(col, {})[op] = value
    return filters


def _parse_order_by_params(params: dict[str, str]) -> list[dict[str, str]]:
    """Parse order_by.column=asc|desc query params.

    Returns [{"field": col, "dir": "asc"|"desc"}, ...].
    """
    ordering = []
    for key, value in params.items():
        if not key.startswith("order_by."):
            continue
        col = key[len("order_by."):]
        direction = value.lower()
        if direction not in ("asc", "desc"):
            direction = "asc"
        ordering.append({"field": col, "dir": direction})
    return ordering


def _build_graphql_query(
    table: str,
    fields: list[str],
    where: dict[str, dict[str, Any]],
    order_by: list[dict[str, str]],
    limit: int | None,
    offset: int | None,
) -> str:
    """Build a GraphQL query string from parsed REST params."""
    args_parts = []

    if limit is not None:
        args_parts.append(f"limit: {limit}")
    if offset is not None:
        args_parts.append(f"offset: {offset}")

    if where:
        where_parts = []
        for col, ops in where.items():
            for op, val in ops.items():
                gql_op = _OP_MAP[op]
                if isinstance(val, list):
                    formatted = "[" + ", ".join(f'"{v}"' for v in val) + "]"
                    where_parts.append(f'{col}: {{{gql_op}: {formatted}}}')
                elif isinstance(val, str):
                    # Try numeric parse
                    try:
                        numeric = int(val)
                        where_parts.append(f"{col}: {{{gql_op}: {numeric}}}")
                    except ValueError:
                        try:
                            numeric = float(val)
                            where_parts.append(f"{col}: {{{gql_op}: {numeric}}}")
                        except ValueError:
                            where_parts.append(f'{col}: {{{gql_op}: "{val}"}}')
                else:
                    where_parts.append(f"{col}: {{{gql_op}: {val}}}")
        if where_parts:
            args_parts.append("where: {" + ", ".join(where_parts) + "}")

    if order_by:
        ob_parts = []
        for o in order_by:
            ob_parts.append(f'{o["field"]}: {o["dir"]}')
        args_parts.append("order_by: {" + ", ".join(ob_parts) + "}")

    args_str = f"({', '.join(args_parts)})" if args_parts else ""
    fields_str = " ".join(fields)
    return f"{{ {table}{args_str} {{ {fields_str} }} }}"


def _get_table_fields(schema: GraphQLSchema, table: str) -> list[str]:
    """Get all scalar field names for a root query field from the schema."""
    query_type = schema.query_type
    if query_type is None:
        return []
    field_map = query_type.fields
    if table not in field_map:
        return []
    gql_field = field_map[table]
    return_type = gql_field.type
    # Unwrap NonNull and List wrappers
    while hasattr(return_type, "of_type"):
        return_type = return_type.of_type
    if not isinstance(return_type, GraphQLObjectType):
        return []
    return [
        name for name, f in return_type.fields.items()
        if not hasattr(f.type, "fields") or isinstance(f.type, GraphQLObjectType)
    ]


def _get_scalar_fields(schema: GraphQLSchema, table: str) -> list[str]:
    """Get only scalar (non-object) field names for a table."""
    from graphql import (
        GraphQLList,
        GraphQLNonNull,
        GraphQLScalarType,
        GraphQLEnumType,
    )
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


def create_rest_router(state: Any) -> APIRouter:
    """Create a REST router with auto-generated endpoints for each table.

    Args:
        state: AppState with schemas, contexts, rls_contexts.

    Returns:
        APIRouter mounted at /data/rest.
    """
    rest_router = APIRouter(prefix="/data/rest", tags=["rest"])

    @rest_router.get("/{table}")
    async def rest_table_endpoint(
        request: Request,
        table: str,
        limit: int | None = Query(None, ge=1),
        offset: int | None = Query(None, ge=0),
        fields: str | None = Query(None),
    ):
        auth_role = getattr(request.state, "role", None)
        role_id = auth_role or "admin"

        if role_id not in state.schemas:
            raise HTTPException(
                status_code=400,
                detail=f"No schema available for role {role_id!r}",
            )

        schema = state.schemas[role_id]
        ctx = state.contexts[role_id]
        rls = state.rls_contexts.get(role_id, RLSContext.empty())

        # Validate table exists
        query_type = schema.query_type
        if query_type is None or table not in query_type.fields:
            raise HTTPException(status_code=404, detail=f"Table {table!r} not found")

        # Parse all query params
        raw_params = dict(request.query_params)

        # Determine fields
        if fields:
            selected_fields = [f.strip() for f in fields.split(",")]
        else:
            selected_fields = _get_scalar_fields(schema, table)

        if not selected_fields:
            raise HTTPException(
                status_code=400,
                detail=f"No selectable fields for table {table!r}",
            )

        where = _parse_where_params(raw_params)
        order_by = _parse_order_by_params(raw_params)

        # Build and execute GraphQL query
        gql_query = _build_graphql_query(
            table, selected_fields, where, order_by, limit, offset,
        )
        log.debug("REST → GraphQL: %s", gql_query)

        try:
            document = parse_query(schema, gql_query)
        except (GraphQLValidationError, Exception) as e:
            raise HTTPException(status_code=400, detail=str(e))

        compiled_queries = compile_query(document, ctx)
        if not compiled_queries:
            raise HTTPException(status_code=400, detail="Compilation failed")

        compiled = compiled_queries[0]

        # Apply RLS
        from provisa.compiler.rls import inject_rls
        compiled = inject_rls(compiled, ctx, rls)

        # Apply masking
        from provisa.compiler.mask_inject import inject_masking
        compiled = inject_masking(
            compiled, ctx, state.masking_rules, role_id,
        )

        # Route and execute
        from provisa.transpiler.router import Route, decide_route
        from provisa.transpiler.transpile import transpile

        has_json_extract = "->>" in compiled.sql
        decision = decide_route(
            sources=compiled.sources,
            source_types=state.source_types,
            source_dialects=state.source_dialects,
            has_json_extract=has_json_extract,
        )

        try:
            if decision.route == Route.DIRECT and decision.source_id:
                from provisa.executor.direct import execute_direct
                target_sql = transpile(compiled.sql, decision.dialect or "postgres")
                result = await execute_direct(
                    state.source_pools, decision.source_id,
                    target_sql, compiled.params,
                )
            else:
                from provisa.executor.trino import execute_trino
                from provisa.transpiler.transpile import transpile_to_trino
                if state.trino_conn is None:
                    raise HTTPException(status_code=503, detail="Trino not connected")
                trino_sql = transpile_to_trino(compiled.sql)
                result = execute_trino(
                    state.trino_conn, trino_sql, compiled.params,
                )
        except HTTPException:
            raise
        except Exception as e:
            log.exception("REST query execution failed for %s", table)
            raise HTTPException(status_code=500, detail=str(e))

        # Serialize
        from provisa.executor.serialize import serialize_rows
        response_data = serialize_rows(result.rows, compiled.columns, table)
        rows = response_data.get("data", {}).get(table, [])

        return JSONResponse(content={"data": rows})

    return rest_router
