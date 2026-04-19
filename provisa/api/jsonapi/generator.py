# Copyright (c) 2026 Kenneth Stott
# Canary: d4dc9aad-e7fa-4e6a-8bbf-ab502a25a06b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""JSON:API endpoint auto-generation from compiled GraphQL schema (Phase AB6).

For each root query field, generates GET /data/jsonapi/{table}.
JSON:API features: sparse fieldsets, filtering, sorting, pagination,
inclusion, content negotiation, error objects.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from graphql import (
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
)

from provisa.api.jsonapi.errors import error_response, jsonapi_error
from provisa.api.jsonapi.pagination import (
    build_pagination_links,
    page_to_limit_offset,
    parse_page_params,
)
from provisa.api.jsonapi.serializer import rows_to_jsonapi
from provisa.api._query_helpers import (
    build_graphql_query as _build_graphql_query_shared,
    get_scalar_fields as _get_scalar_fields_shared,
    route_and_execute,
)
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import compile_query

log = logging.getLogger(__name__)

JSONAPI_CONTENT_TYPE = "application/vnd.api+json"

# Supported filter operators
_FILTER_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "in"}


def _parse_filters(params: dict[str, str]) -> dict[str, dict[str, Any]]:
    """Parse JSON:API filter params.

    Supports:
      filter[region]=US           -> {region: {eq: "US"}}
      filter[amount][gt]=100      -> {amount: {gt: "100"}}
    """
    filters: dict[str, dict[str, Any]] = {}
    for key, value in params.items():
        if not key.startswith("filter["):
            continue
        # Strip "filter[" prefix
        inner = key[7:]
        if "]" not in inner:
            continue

        # Check for nested operator: filter[col][op]
        parts = inner.split("][")
        if len(parts) == 2:
            col = parts[0]
            op = parts[1].rstrip("]")
            if op in _FILTER_OPS:
                if op == "in":
                    value = value.split(",")
                filters.setdefault(col, {})[op] = value
        elif len(parts) == 1:
            col = parts[0].rstrip("]")
            filters.setdefault(col, {})["eq"] = value
    return filters


def _parse_sort(sort_param: str | None) -> list[dict[str, str]]:
    """Parse JSON:API sort param: ?sort=-created_at,amount

    Prefix '-' means descending.
    Returns [{"field": "created_at", "dir": "desc"}, {"field": "amount", "dir": "asc"}].
    """
    if not sort_param:
        return []
    ordering = []
    for part in sort_param.split(","):
        part = part.strip()
        if not part:
            continue
        if part.startswith("-"):
            ordering.append({"field": part[1:], "dir": "desc"})
        else:
            ordering.append({"field": part, "dir": "asc"})
    return ordering


def _parse_sparse_fieldsets(
    params: dict[str, str], table: str,
) -> list[str] | None:
    """Parse JSON:API sparse fieldsets: ?fields[orders]=amount,created_at

    Returns list of field names or None (all fields).
    """
    key = f"fields[{table}]"
    raw = params.get(key)
    if raw is None:
        return None
    return [f.strip() for f in raw.split(",") if f.strip()]


def _get_scalar_fields(schema: GraphQLSchema, table: str) -> list[str]:
    """Get scalar field names for a root query type."""
    return _get_scalar_fields_shared(schema, table)


def _get_relationship_fields(
    schema: GraphQLSchema, table: str,
) -> dict[str, str]:
    """Get relationship field names: {fk_column: related_type_name}.

    Inspects the GraphQL type for object-typed fields.
    """
    query_type = schema.query_type
    if query_type is None:
        return {}
    field_map = query_type.fields
    if table not in field_map:
        return {}
    gql_field = field_map[table]
    return_type = gql_field.type
    while hasattr(return_type, "of_type"):
        return_type = return_type.of_type
    if not isinstance(return_type, GraphQLObjectType):
        return {}

    rels: dict[str, str] = {}
    for name, f in return_type.fields.items():
        inner = f.type
        while isinstance(inner, (GraphQLNonNull, GraphQLList)):
            inner = inner.of_type
        if isinstance(inner, GraphQLObjectType):
            # The FK column is typically name_id, the relationship field is name
            rels[f"{name}_id"] = name
    return rels


def _build_graphql_query(
    table: str,
    fields: list[str],
    filters: dict[str, dict[str, Any]],
    sort: list[dict[str, str]],
    limit: int | None,
    offset: int | None,
) -> str:
    """Build GraphQL query from JSON:API params."""
    return _build_graphql_query_shared(table, fields, filters, sort, limit, offset)


def _jsonapi_error_response(status: int, title: str, detail: str | None = None, **kwargs):
    """Return a JSONResponse with JSON:API error format."""
    body = error_response([jsonapi_error(status, title, detail, **kwargs)])
    return JSONResponse(
        content=body,
        status_code=status,
        media_type=JSONAPI_CONTENT_TYPE,
    )


def create_jsonapi_router(state: Any) -> APIRouter:
    """Create a JSON:API router with auto-generated endpoints for each table.

    Args:
        state: AppState with schemas, contexts, rls_contexts.

    Returns:
        APIRouter mounted at /data/jsonapi.
    """
    jsonapi_router = APIRouter(prefix="/data/jsonapi", tags=["jsonapi"])

    @jsonapi_router.get("/{table}")
    async def jsonapi_table_endpoint(request: Request, table: str):
        # Content negotiation
        accept = request.headers.get("accept", "")
        if accept and JSONAPI_CONTENT_TYPE not in accept and "*/*" not in accept:
            return _jsonapi_error_response(
                406, "Not Acceptable",
                f"This endpoint requires Accept: {JSONAPI_CONTENT_TYPE}",
            )

        auth_role = getattr(request.state, "role", None)
        role_id = auth_role or "admin"

        if role_id not in state.schemas:
            return _jsonapi_error_response(
                400, "Bad Request",
                f"No schema available for role {role_id!r}",
            )

        schema = state.schemas[role_id]
        ctx = state.contexts[role_id]
        rls = state.rls_contexts.get(role_id, RLSContext.empty())

        query_type = schema.query_type
        if query_type is None or table not in query_type.fields:
            return _jsonapi_error_response(
                404, "Not Found", f"Resource type {table!r} not found",
            )

        raw_params = dict(request.query_params)

        # Parse JSON:API params
        sparse = _parse_sparse_fieldsets(raw_params, table)
        all_scalars = _get_scalar_fields(schema, table)
        selected_fields = sparse if sparse else all_scalars

        if not selected_fields:
            return _jsonapi_error_response(
                400, "Bad Request",
                f"No selectable fields for resource type {table!r}",
            )

        # Ensure id is always selected for resource identity
        if "id" in all_scalars and "id" not in selected_fields:
            selected_fields = ["id"] + selected_fields

        filters = _parse_filters(raw_params)
        sort = _parse_sort(raw_params.get("sort"))
        page_number, page_size = parse_page_params(raw_params)
        limit, pg_offset = page_to_limit_offset(page_number, page_size)

        # Validate filter columns
        for col in filters:
            if col not in all_scalars:
                return _jsonapi_error_response(
                    400, "Invalid Filter",
                    f"Unknown filter field {col!r}",
                    source_parameter=f"filter[{col}]",
                )

        # Validate sort columns
        for s in sort:
            if s["field"] not in all_scalars:
                return _jsonapi_error_response(
                    400, "Invalid Sort",
                    f"Unknown sort field {s['field']!r}",
                    source_parameter="sort",
                )

        gql_query = _build_graphql_query(
            table, selected_fields, filters, sort, limit, pg_offset,
        )
        log.debug("JSON:API -> GraphQL: %s", gql_query)

        try:
            document = parse_query(schema, gql_query)
        except (GraphQLValidationError, Exception) as e:
            return _jsonapi_error_response(400, "Bad Request", str(e))

        compiled_queries = compile_query(document, ctx)
        if not compiled_queries:
            return _jsonapi_error_response(400, "Bad Request", "Compilation failed")

        compiled = compiled_queries[0]

        from provisa.compiler.rls import inject_rls
        from provisa.compiler.mask_inject import inject_masking

        compiled = inject_rls(compiled, ctx, rls)
        compiled = inject_masking(compiled, ctx, state.masking_rules, role_id)

        try:
            result = await route_and_execute(compiled, state)
        except HTTPException as e:
            if e.status_code == 503:
                return _jsonapi_error_response(503, "Service Unavailable", e.detail)
            raise
        except Exception as e:
            log.exception("JSON:API query execution failed for %s", table)
            return _jsonapi_error_response(500, "Internal Server Error", str(e))

        # Serialize to flat rows first
        from provisa.executor.serialize import serialize_rows
        response_data = serialize_rows(result.rows, compiled.columns, table)
        rows = response_data.get("data", {}).get(table, [])

        # Detect relationship fields from schema
        rel_fields = _get_relationship_fields(schema, table)

        # Build JSON:API document
        doc = rows_to_jsonapi(
            rows, table, id_field="id", relationship_fields=rel_fields,
        )

        # Pagination links
        base_path = f"/data/jsonapi/{table}"
        extra = {}
        if sparse:
            extra[f"fields[{table}]"] = ",".join(sparse)
        if raw_params.get("sort"):
            extra["sort"] = raw_params["sort"]
        for k, v in raw_params.items():
            if k.startswith("filter["):
                extra[k] = v

        doc["links"] = build_pagination_links(
            base_path, page_number, page_size, len(rows), extra or None,
        )

        return JSONResponse(content=doc, media_type=JSONAPI_CONTENT_TYPE)

    return jsonapi_router
