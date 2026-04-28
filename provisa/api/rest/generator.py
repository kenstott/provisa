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
from graphql import GraphQLObjectType, GraphQLSchema

from provisa.api._query_helpers import (
    build_graphql_query as _build_graphql_query_shared,
    get_scalar_fields as _get_scalar_fields_shared,
    route_and_execute,
)
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query

log = logging.getLogger(__name__)

# Supported WHERE operators
_WHERE_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "in"}


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
    return _build_graphql_query_shared(table, fields, where, order_by, limit, offset)


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
    return _get_scalar_fields_shared(schema, table)


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

        try:
            result = await route_and_execute(compiled, state)
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
