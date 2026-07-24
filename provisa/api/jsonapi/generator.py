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

# Requirements: REQ-256, REQ-257, REQ-266, REQ-001, REQ-002

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
)
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.sql_gen import compile_query

log = logging.getLogger(__name__)

JSONAPI_CONTENT_TYPE = "application/vnd.api+json"

# Supported filter operators
_FILTER_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "in"}


def _parse_filters(params: dict[str, str]) -> dict[str, dict[str, Any]]:  # REQ-257
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


def _parse_sort(sort_param: str | None) -> list[dict[str, str]]:  # REQ-257
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


def _parse_sparse_fieldsets(  # REQ-257
    params: dict[str, str],
) -> dict[str, list[str]]:
    """Parse JSON:API sparse fieldsets from all fields[*] query params.

    Returns a dict mapping table name to list of requested field names.
    An empty dict means no sparse fieldset requested (return all fields).

    Example: ?fields[orders]=amount,created_at&fields[customers]=name
    → {"orders": ["amount", "created_at"], "customers": ["name"]}
    """
    result: dict[str, list[str]] = {}
    for key, value in params.items():
        if key.startswith("fields[") and key.endswith("]"):
            table_name = key[len("fields[") : -1]
            result[table_name] = [f.strip() for f in value.split(",") if f.strip()]
    return result


def _get_scalar_fields(schema: GraphQLSchema, table: str) -> list[str]:
    """Get scalar field names for a root query type, excluding virtual sentinel fields."""
    return [
        f
        for f in _get_scalar_fields_shared(schema, table)
        if not (f.startswith("_") and f.endswith("_"))
    ]


def _get_relationship_fields(
    schema: GraphQLSchema,
    table: str,
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
            # FK column in serialized rows uses camelCase (e.g. petId), not snake_case (pet_id)
            rels[f"{name}Id"] = name
    return rels


def _extract_included(
    rows: list[dict[str, Any]], include_names: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """REQ-257: move nested included entities out of the primary rows into a deduplicated set.

    The nested object/array under each included relationship is popped from the primary
    resource (its attributes must not carry it — the FK column links it) and collected,
    deduplicated by id, keyed by relationship name (the JSON:API included type).
    """
    included_rows: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        for inc in include_names:
            nested = row.pop(inc, None)
            if not nested:
                continue
            items = nested if isinstance(nested, list) else [nested]
            bucket = included_rows.setdefault(inc, [])
            seen_ids = {r.get("id") for r in bucket}
            for item in items:
                if isinstance(item, dict) and item.get("id") not in seen_ids:
                    bucket.append(item)
                    seen_ids.add(item.get("id"))
    return included_rows


def _relationship_scalars(schema: GraphQLSchema, table: str, rel_field: str) -> list[str]:
    """Scalar field names of the related type for a relationship field on ``table`` (REQ-257)."""
    query_type = schema.query_type
    if query_type is None or table not in query_type.fields:
        return []
    rt = query_type.fields[table].type
    while hasattr(rt, "of_type"):
        rt = rt.of_type
    if not isinstance(rt, GraphQLObjectType) or rel_field not in rt.fields:
        return []
    inner = rt.fields[rel_field].type
    while isinstance(inner, (GraphQLNonNull, GraphQLList)):
        inner = inner.of_type
    if not isinstance(inner, GraphQLObjectType):
        return []
    scalars: list[str] = []
    for name, f in inner.fields.items():
        if name.startswith("_") and name.endswith("_"):
            continue
        ft = f.type
        while isinstance(ft, (GraphQLNonNull, GraphQLList)):
            ft = ft.of_type
        if not isinstance(ft, GraphQLObjectType):
            scalars.append(name)
    return scalars


def _build_graphql_query(
    table: str,
    fields: list[str],
    filters: dict[str, dict[str, Any]] | None = None,
    sort: list[dict[str, str]] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    *,
    where: dict[str, dict[str, Any]] | None = None,
    order_by: list[dict[str, str]] | None = None,
) -> str:
    """Build GraphQL query from JSON:API params.

    ``where`` is accepted as an alias for ``filters``;
    ``order_by`` is accepted as an alias for ``sort``.
    """
    effective_filters = where if filters is None else filters
    effective_sort = order_by if sort is None else sort
    return _build_graphql_query_shared(
        table, fields, effective_filters or {}, effective_sort or [], limit, offset
    )


def _jsonapi_error_response(status: int, title: str, detail: str | None = None, **kwargs):
    """Return a JSONResponse with JSON:API error format."""
    body = error_response([jsonapi_error(status, title, detail, **kwargs)])
    return JSONResponse(
        content=body,
        status_code=status,
        media_type=JSONAPI_CONTENT_TYPE,
    )


def create_jsonapi_router(state: Any) -> APIRouter:  # REQ-256, REQ-257, REQ-266, REQ-001, REQ-002
    """Create a JSON:API router with auto-generated endpoints for each table.

    Args:
        state: AppState with schemas, contexts, rls_contexts.

    Returns:
        APIRouter mounted at /data/jsonapi.
    """
    jsonapi_router = APIRouter(prefix="/data/jsonapi", tags=["jsonapi"])

    @jsonapi_router.get("/openapi.json", include_in_schema=False)
    async def _jsonapi_openapi_json(  # pyright: ignore[reportUnusedFunction]
        request: Request,
        role: str | None = None,
        domains: str | None = None,
    ):
        from provisa.api.jsonapi.spec import generate_jsonapi_openapi_spec

        auth_role = getattr(request.state, "role", None)
        role_id = auth_role or role
        if not role_id:
            return _jsonapi_error_response(401, "Unauthorized", "role required")
        domain_list = [d for d in domains.split(",") if d] if domains else None
        spec = generate_jsonapi_openapi_spec(state, role_id, domains=domain_list)
        download = request.query_params.get("download")
        headers = (
            {"Content-Disposition": "attachment; filename=jsonapi-openapi.json"} if download else {}
        )
        return JSONResponse(content=spec, headers=headers)

    @jsonapi_router.get("/{domain_id}/{table_name}")
    async def _jsonapi_table_endpoint(  # pyright: ignore[reportUnusedFunction]
        request: Request, domain_id: str, table_name: str
    ):
        # Content negotiation
        accept = request.headers.get("accept", "")
        if accept and JSONAPI_CONTENT_TYPE not in accept and "*/*" not in accept:
            return _jsonapi_error_response(
                406,
                "Not Acceptable",
                f"This endpoint requires Accept: {JSONAPI_CONTENT_TYPE}",
            )

        auth_role = getattr(request.state, "role", None)
        if not auth_role:
            return _jsonapi_error_response(401, "Unauthorized", "authenticated role required")
        role_id = auth_role

        if role_id not in state.schemas:
            return _jsonapi_error_response(
                400,
                "Bad Request",
                f"No schema available for role {role_id!r}",
            )

        schema = state.schemas[role_id]
        ctx = state.contexts[role_id]

        # Resolve domain_id + table_name → GQL field name (e.g. "ps__pets")
        path_map = getattr(state, "table_path_maps", {}).get(role_id, {})
        gql_table = next(
            (
                gql
                for gql, meta in path_map.items()
                if meta["domain_id"] == domain_id and meta["table_name"] == table_name
            ),
            None,
        )

        query_type = schema.query_type
        if gql_table is None or query_type is None or gql_table not in query_type.fields:
            return _jsonapi_error_response(
                404,
                "Not Found",
                f"Resource type {domain_id!r}/{table_name!r} not found",
            )

        raw_params = dict(request.query_params)

        # Parse JSON:API params
        sparse = _parse_sparse_fieldsets(raw_params).get(table_name)
        all_scalars = _get_scalar_fields(schema, gql_table)
        selected_fields = sparse if sparse else all_scalars

        if not selected_fields:
            return _jsonapi_error_response(
                400,
                "Bad Request",
                f"No selectable fields for resource type {domain_id!r}/{table_name!r}",
            )

        # Ensure id is always selected for resource identity
        if "id" in all_scalars and "id" not in selected_fields:
            selected_fields = ["id"] + selected_fields

        filters = _parse_filters(raw_params)
        sort = _parse_sort(raw_params.get("sort"))
        page = parse_page_params(raw_params)
        page_number, page_size = page["number"], page["size"]
        limit, pg_offset = page_to_limit_offset(page)

        # Validate filter columns
        for col in filters:
            if col not in all_scalars:
                return _jsonapi_error_response(
                    400,
                    "Invalid Filter",
                    f"Unknown filter field {col!r}",
                    source_parameter=f"filter[{col}]",
                )

        # Validate sort columns
        for s in sort:
            if s["field"] not in all_scalars:
                return _jsonapi_error_response(
                    400,
                    "Invalid Sort",
                    f"Unknown sort field {s['field']!r}",
                    source_parameter="sort",
                )

        # REQ-257: ?include=rel1,rel2 — sideload related resources as a compound document.
        rel_fields = _get_relationship_fields(schema, gql_table)
        valid_rel_names = set(rel_fields.values())
        fk_by_rel = {rel_name: fk for fk, rel_name in rel_fields.items()}
        include_param = raw_params.get("include")
        include_names = (
            [n.strip() for n in include_param.split(",") if n.strip()] if include_param else []
        )
        for inc in include_names:
            if inc not in valid_rel_names:
                return _jsonapi_error_response(
                    400,
                    "Invalid Include",
                    f"Unknown relationship {inc!r}",
                    source_parameter="include",
                )
        query_fields = list(selected_fields)
        for inc in include_names:
            inc_scalars = _relationship_scalars(schema, gql_table, inc)
            if "id" in inc_scalars:
                inc_scalars = ["id"] + [s for s in inc_scalars if s != "id"]
            query_fields.append(f"{inc} {{ {' '.join(inc_scalars)} }}")
            # the FK column must be selected so the resource's relationship linkage resolves
            fk = fk_by_rel.get(inc)
            if fk and fk in all_scalars and fk not in query_fields:
                query_fields.append(fk)

        gql_query = _build_graphql_query(
            gql_table,
            query_fields,
            filters,
            sort,
            limit,
            pg_offset,
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

        # Governance + routing via Stage 2 (REQ-266): RLS, masking, visibility, and the
        # row cap are applied by apply_governance — the same path as GraphQL and REST,
        # so no transport bypasses governance.
        from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

        # REQ-1194/REQ-1195: a caller may request the result be materialized to a sink instead of
        # inlined. The request rides the same X-Provisa-Redirect* headers GraphQL uses; the handle is
        # surfaced in the document's top-level `meta` — JSON:API's side-channel alongside `data`.
        from provisa.api.data.endpoint_helpers import _parse_accept
        from provisa.executor.redirect import delivery_from_request

        _redir_fmt = request.headers.get("x-provisa-redirect-format")
        _redir_thr = request.headers.get("x-provisa-redirect-threshold")
        delivery = delivery_from_request(
            force_redirect=request.headers.get("x-provisa-redirect", "").lower() == "true",
            redirect_format=_parse_accept(_redir_fmt) if _redir_fmt else None,
            threshold=int(_redir_thr) if _redir_thr else None,
            role=role_id,
        )

        try:
            plan = await _govern_and_route_compiled(
                compiled.sql,
                role_id,
                exec_params=compiled.params or None,
                state=state,
                deliver=delivery,
            )
            result = await _execute_plan(plan, state)
        except PermissionError as e:
            return _jsonapi_error_response(403, "Forbidden", str(e))
        except HTTPException as e:
            if e.status_code == 503:
                return _jsonapi_error_response(503, "Service Unavailable", e.detail)
            raise
        except Exception as e:
            log.exception("JSON:API query execution failed for %s", gql_table)
            return _jsonapi_error_response(500, "Internal Server Error", str(e))

        if result.redirect is not None:
            # Materialized: no resource rows crossed the wire. `data: null` + the handle in `meta`.
            return JSONResponse(
                content={"data": None, "meta": {"redirect": result.redirect}},
                media_type=JSONAPI_CONTENT_TYPE,
            )

        # Count query — same filters, no pagination — for accurate total. The compiled inner
        # SELECT is wrapped in COUNT(*) so the engine computes the cardinality and only a single
        # scalar row crosses the wire; the full matching set is never materialized in this process
        # to be counted (REQ-028: no transport buffers a whole result set to page it). RLS/masking
        # still bind to the inner base tables — apply_governance rewrites nested table refs.
        count_field = "id" if "id" in all_scalars else all_scalars[0]
        count_gql = _build_graphql_query(gql_table, [count_field], filters, [], None, None)
        try:
            count_doc = parse_query(schema, count_gql)
            count_compiled = compile_query(count_doc, ctx)
        except (GraphQLValidationError, Exception) as e:
            return _jsonapi_error_response(400, "Bad Request", str(e))
        if not count_compiled:
            return _jsonapi_error_response(400, "Bad Request", "Count compilation failed")
        count_sql = f"SELECT COUNT(*) AS total FROM ({count_compiled[0].sql}) AS _provisa_count"
        try:
            count_plan = await _govern_and_route_compiled(
                count_sql,
                role_id,
                exec_params=count_compiled[0].params or None,
                state=state,
            )
            count_result = await _execute_plan(count_plan, state)
        except PermissionError as e:
            return _jsonapi_error_response(403, "Forbidden", str(e))
        except HTTPException as e:
            if e.status_code == 503:
                return _jsonapi_error_response(503, "Service Unavailable", e.detail)
            raise
        except Exception as e:
            log.exception("JSON:API count query failed for %s", gql_table)
            return _jsonapi_error_response(500, "Internal Server Error", str(e))
        # COUNT(*) yields exactly one scalar row by SQL semantics — no empty-result fallback.
        total_count = int(next(iter(count_result.rows))[0])

        # Serialize to flat rows first
        from provisa.executor.serialize import serialize_rows

        response_data = serialize_rows(result.rows, compiled.columns, gql_table)
        rows = response_data.get("data", {}).get(gql_table, [])

        # REQ-257: pull nested included entities out of the rows into a deduplicated set.
        included_rows = _extract_included(rows, include_names)

        # Build JSON:API document (compound when includes were requested)
        doc = rows_to_jsonapi(
            rows,
            table_name,
            id_field="id",
            relationship_fields=rel_fields,
            included_rows=included_rows or None,
        )
        doc.setdefault("meta", {})["total"] = total_count

        # Pagination links — preserve role, sort, sparse fieldset, filters, and include
        base_path = f"/data/jsonapi/{domain_id}/{table_name}"
        extra = {}
        for k in ("role", "sort", "include"):
            if raw_params.get(k):
                extra[k] = raw_params[k]
        if sparse:
            extra[f"fields[{table_name}]"] = ",".join(sparse)
        for k, v in raw_params.items():
            if k.startswith("filter["):
                extra[k] = v

        doc["links"] = build_pagination_links(
            base_url=base_path,
            page_number=page_number,
            page_size=page_size,
            total=total_count,
            query_params=extra or None,
        )

        return JSONResponse(content=doc, media_type=JSONAPI_CONTENT_TYPE)

    return jsonapi_router
