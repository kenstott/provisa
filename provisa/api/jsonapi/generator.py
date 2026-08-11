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
from provisa.api.jsonapi.naming import (
    relationship_name_maps,
    relationship_scalar_maps,
    rename_row_keys,
    scalar_name_maps,
)
from provisa.api.jsonapi.serializer import rows_to_jsonapi
from provisa.api._query_helpers import (
    build_graphql_query as _build_graphql_query_shared,
    get_scalar_fields as _get_scalar_fields_shared,
)
from provisa.compiler.naming import apply_gql_name
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


def _parse_aggregate_param(value: str | None) -> list[str] | None:  # REQ-1359
    """Parse the JSON:API ``aggregate`` query param.

    Supports:
      (absent)               -> None (aggregate not requested)
      aggregate=true|1|""    -> [] (requested, all available functions)
      aggregate=count,sum    -> ["count", "sum"] (requested, specific functions)
    """
    if value is None:
        return None
    stripped = value.strip()
    if stripped.lower() in ("", "true", "1"):
        return []
    return [f.strip() for f in stripped.split(",") if f.strip()]


def _parse_group_by_param(value: str | None) -> list[str]:  # REQ-1359
    """Parse the JSON:API ``groupBy`` query param: comma-separated column names."""
    if not value:
        return []
    return [c.strip() for c in value.split(",") if c.strip()]


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


def _build_group_by_node_selection(
    schema: GraphQLSchema,
    gql_table: str,
    base_scalars: list[str],
    include_param: str | None,
    rel_physical_to_gql: dict[str, str],
    rel_scalar_physical_to_gql: dict[str, dict[str, str]],
) -> tuple[str, str | None]:  # REQ-1408
    """The ``nodes { ... }`` selection for ``?includeNodes=true`` (REQ-1401).

    Every base-table scalar, plus whatever ``?include=`` names. An entry is either a relationship
    name (``user`` — every scalar of the related table) or a ``rel.col`` dot-path selecting one
    column, the same projection gRPC's ``include`` (query_ir::_include_node_fields) and REST's
    ``?includeNodes=`` dot-path list accept, so one plan drives all three surfaces.

    Both segments are physical names — the spelling ``?groupBy=`` already takes — and are
    translated to the schema's GQL-convention spelling here, at the emit boundary (REQ-1417).

    Returns ``(selection, error_detail)``; ``error_detail`` is non-None when an entry names an
    unknown relationship or column, which the caller turns into a 400.
    """
    node_fields = list(base_scalars)
    include_names = (
        [n.strip() for n in include_param.split(",") if n.strip()] if include_param else []
    )
    rel_columns: dict[str, list[str]] = {}
    for inc in include_names:
        rel, _, column = inc.partition(".")
        gql_rel = rel_physical_to_gql.get(rel)
        if gql_rel is None:
            return "", f"Unknown relationship {rel!r}"
        column_map = rel_scalar_physical_to_gql[gql_rel]
        if column and column not in column_map:
            return "", f"Unknown field {column!r} on relationship {rel!r}"
        rel_scalars = _relationship_scalars(schema, gql_table, gql_rel)
        requested = rel_columns.setdefault(gql_rel, [])
        for col in [column_map[column]] if column else rel_scalars:
            if col not in requested:
                requested.append(col)
    for rel, columns in rel_columns.items():
        if columns:
            node_fields.append(f"{rel} {{ {' '.join(columns)} }}")
    return " ".join(node_fields), None


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


def _unwrap_type(t: Any) -> Any:
    """Strip GraphQLNonNull/GraphQLList wrappers down to the underlying named type."""
    while hasattr(t, "of_type"):
        t = t.of_type
    return t


def _resolve_query_field(
    query_type: GraphQLObjectType, gql_table: str, snake_suffix: str, camel_suffix: str
) -> str | None:  # REQ-1359
    """Resolve a table's aggregate/group-by root field name under either naming convention."""
    for candidate in (f"{gql_table}{snake_suffix}", f"{gql_table}{camel_suffix}"):
        if candidate in query_type.fields:
            return candidate
    return None


def _get_agg_fields_type(root_field: Any) -> GraphQLObjectType | None:  # REQ-1359
    """Extract the ``{Type}AggregateFields`` object type nested under a root field's ``aggregate``.

    Works for both the ``{field}_aggregate`` root field (returns ``{Type}Aggregate``) and the
    ``{field}_group_by`` root field (returns a list of ``{Type}GroupByRow``) since both nest an
    ``aggregate`` field of the same shared type (schema_gen.py builds it once for both).
    """
    rt = _unwrap_type(root_field.type)
    if not isinstance(rt, GraphQLObjectType) or "aggregate" not in rt.fields:
        return None
    return _unwrap_type(rt.fields["aggregate"].type)


def _build_agg_selection(
    agg_fields_type: GraphQLObjectType, funcs_filter: list[str] | None
) -> str:  # REQ-1359
    """Build the GraphQL sub-selection for an ``aggregate { ... }`` block.

    ``funcs_filter`` of None selects every function the schema exposes for this table;
    otherwise only the named functions (already validated against the schema by the caller).
    """
    parts = []
    for name, f in agg_fields_type.fields.items():
        if funcs_filter is not None and name not in funcs_filter:
            continue
        ftype = _unwrap_type(f.type)
        if isinstance(ftype, GraphQLObjectType):
            parts.append(f"{name} {{ {' '.join(ftype.fields.keys())} }}")
        else:
            parts.append(name)
    return " ".join(parts) if parts else "count"


def _build_group_by_graphql_query(  # REQ-1359
    field_name: str,
    by_columns: list[str],
    agg_selection: str,
    filters: dict[str, dict[str, Any]],
    sort: list[dict[str, str]],
    limit: int | None,
    offset: int | None,
    node_selection: str | None = None,  # REQ-1401
) -> str:
    """Build GraphQL query text for a ``{field}_group_by(by: [...])`` root field.

    Mirrors the where/order_by/limit/offset argument formatting ``_build_graphql_query_shared``
    already does for the base table field; adds the ``by`` argument that shared builder has no
    concept of since it's specific to group-by root fields.

    ``by_columns`` are API-native (physical) column names, same as JSON:API's ``?groupBy=``
    accepts; translated to the schema's GQL-convention spelling here, mirroring
    ``grpc_table_to_group_by_graphql_text`` (REQ-1361).

    ``node_selection``, when given, is a pre-built field-selection string appended as a
    ``nodes { ... }`` sub-selection (REQ-1401) — the per-group row-level detail GraphQL's
    ``{table}_group_by`` already exposes.
    """
    args_parts = [f"by: [{', '.join(apply_gql_name(c) for c in by_columns)}]"]
    if limit is not None:
        args_parts.append(f"limit: {limit}")
    if offset:
        args_parts.append(f"offset: {offset}")
    if filters:
        where_parts = []
        for col, ops in filters.items():
            for op, val in ops.items():
                if isinstance(val, list):
                    formatted = "[" + ", ".join(f'"{v}"' for v in val) + "]"
                    where_parts.append(f"{col}: {{{op}: {formatted}}}")
                elif isinstance(val, str):
                    try:
                        numeric: float | int = int(val)
                    except ValueError:
                        try:
                            numeric = float(val)
                        except ValueError:
                            where_parts.append(f'{col}: {{{op}: "{val}"}}')
                            continue
                    where_parts.append(f"{col}: {{{op}: {numeric}}}")
                else:
                    where_parts.append(f"{col}: {{{op}: {val}}}")
        if where_parts:
            args_parts.append("where: {" + ", ".join(where_parts) + "}")
    if sort:
        ob_parts = [f"{o['field']}: {o['dir']}" for o in sort]
        args_parts.append("order_by: {" + ", ".join(ob_parts) + "}")
    args_str = f"({', '.join(args_parts)})"
    nodes_part = f" nodes {{ {node_selection} }}" if node_selection else ""
    return f"{{ {field_name}{args_str} {{ groupKey aggregate {{ {agg_selection} }}{nodes_part} }} }}"


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

        all_scalars = _get_scalar_fields(schema, gql_table)

        # REQ-1417: JSON:API is an API over the SQL plane, so every name it accepts in a param and
        # every key it emits is the *physical* column or relationship name. The GQL naming
        # convention is the GraphQL surface's alone (REQ-471); this handler borrows it only for the
        # GraphQL text it synthesizes below, so names are translated physical → GQL here at that
        # emit boundary and back on the way out — the rule ?groupBy= (REQ-1361) and gRPC's include
        # (REQ-1408) already follow.
        table_meta = ctx.tables.get(gql_table)
        if table_meta is None:
            return _jsonapi_error_response(
                404,
                "Not Found",
                f"Resource type {domain_id!r}/{table_name!r} not found",
            )
        gql_to_physical, physical_to_gql = scalar_name_maps(ctx, table_meta.table_id, all_scalars)
        rel_fields = _get_relationship_fields(schema, gql_table)
        gql_rel_names = list(rel_fields.values())
        rel_gql_to_physical, rel_physical_to_gql = relationship_name_maps(gql_rel_names)
        rel_scalars_by_rel = {
            rel: _relationship_scalars(schema, gql_table, rel) for rel in gql_rel_names
        }
        rel_scalar_gql_to_physical, rel_scalar_physical_to_gql = relationship_scalar_maps(
            ctx, table_meta.type_name, rel_scalars_by_rel
        )

        # Parse JSON:API params
        sparse = _parse_sparse_fieldsets(raw_params).get(table_name)
        if sparse is not None:
            unknown = [f for f in sparse if f not in physical_to_gql]
            if unknown:
                return _jsonapi_error_response(
                    400,
                    "Invalid Fieldset",
                    f"Unknown field {unknown[0]!r}",
                    source_parameter=f"fields[{table_name}]",
                )
        selected_fields = [physical_to_gql[f] for f in sparse] if sparse else list(all_scalars)

        if not selected_fields:
            return _jsonapi_error_response(
                400,
                "Bad Request",
                f"No selectable fields for resource type {domain_id!r}/{table_name!r}",
            )

        # Ensure id is always selected for resource identity
        if "id" in all_scalars and "id" not in selected_fields:
            selected_fields = ["id"] + selected_fields

        physical_filters = _parse_filters(raw_params)
        physical_sort = _parse_sort(raw_params.get("sort"))
        page = parse_page_params(raw_params)
        page_number, page_size = page["number"], page["size"]
        limit, pg_offset = page_to_limit_offset(page)

        # Validate + translate filter columns
        for col in physical_filters:
            if col not in physical_to_gql:
                return _jsonapi_error_response(
                    400,
                    "Invalid Filter",
                    f"Unknown filter field {col!r}",
                    source_parameter=f"filter[{col}]",
                )
        filters = {physical_to_gql[col]: ops for col, ops in physical_filters.items()}

        # Validate + translate sort columns
        for s in physical_sort:
            if s["field"] not in physical_to_gql:
                return _jsonapi_error_response(
                    400,
                    "Invalid Sort",
                    f"Unknown sort field {s['field']!r}",
                    source_parameter="sort",
                )
        sort = [{**s, "field": physical_to_gql[s["field"]]} for s in physical_sort]

        # REQ-1359: ?aggregate/?groupBy synthesize {field}_aggregate / {field}_group_by GraphQL
        # query text instead of the base table field — same synthesize-then-compile pipeline
        # GraphQL/REST already run, so governance/RLS/masking apply identically. A table without
        # the corresponding enable_aggregates/enable_group_by flag has no such root field, so
        # resolution below fails and maps to a 400 rather than the parser's 500 on unknown field.
        agg_funcs = _parse_aggregate_param(raw_params.get("aggregate"))
        group_cols = _parse_group_by_param(raw_params.get("groupBy"))
        is_group_by = bool(group_cols)
        is_aggregate_only = agg_funcs is not None and not is_group_by

        if is_aggregate_only or is_group_by:
            # REQ-1361: ?groupBy= takes API-native (physical) column names, same as the
            # jsonapi-group-by-columns picker offers — not the schema's GQL-convention field
            # names, which all_scalars holds. Validate against ctx.aggregate_columns instead.
            valid_group_by_cols = {
                c for c, _t in ctx.aggregate_columns.get(table_meta.table_id, [])
            }
            for col in group_cols:
                if col not in valid_group_by_cols:
                    return _jsonapi_error_response(
                        400,
                        "Invalid Group By",
                        f"Unknown group-by field {col!r}",
                        source_parameter="groupBy",
                    )

            if is_group_by:
                target_field = _resolve_query_field(query_type, gql_table, "_group_by", "GroupBy")
                kind = "group-by"
            else:
                target_field = _resolve_query_field(query_type, gql_table, "_aggregate", "Aggregate")
                kind = "aggregate"
            if target_field is None:
                return _jsonapi_error_response(
                    400,
                    "Bad Request",
                    f"Resource type {domain_id!r}/{table_name!r} does not support {kind}",
                )

            agg_fields_type = _get_agg_fields_type(query_type.fields[target_field])
            if agg_fields_type is None:
                return _jsonapi_error_response(
                    400, "Bad Request", f"Malformed aggregate result type for {gql_table!r}"
                )

            funcs_filter = agg_funcs if agg_funcs else None
            if funcs_filter is not None:
                valid_funcs = set(agg_fields_type.fields.keys())
                for fn in funcs_filter:
                    if fn not in valid_funcs:
                        return _jsonapi_error_response(
                            400,
                            "Invalid Aggregate Function",
                            f"Unknown aggregate function {fn!r}",
                            source_parameter="aggregate",
                        )
            agg_selection = _build_agg_selection(agg_fields_type, funcs_filter)

            if is_group_by:
                # REQ-1401: ?includeNodes=true adds a nodes { ... } sub-selection — per-group
                # row-level detail, mirroring GraphQL's {table}_group_by nodes field. Reuses
                # the same ?include= relationship sideloading the base listing endpoint offers.
                node_selection = None
                if raw_params.get("includeNodes") in ("true", "1"):
                    node_selection, bad_include = _build_group_by_node_selection(
                        schema,
                        gql_table,
                        list(all_scalars),
                        raw_params.get("include"),
                        rel_physical_to_gql,
                        rel_scalar_physical_to_gql,
                    )
                    if bad_include is not None:
                        return _jsonapi_error_response(
                            400, "Invalid Include", bad_include, source_parameter="include"
                        )

                gql_query = _build_group_by_graphql_query(
                    target_field,
                    group_cols,
                    agg_selection,
                    filters,
                    sort,
                    limit,
                    pg_offset,
                    node_selection,
                )
            else:
                gql_query = _build_graphql_query_shared(
                    target_field, [f"aggregate {{ {agg_selection} }}"], filters, [], None, None
                )
            log.debug("JSON:API aggregate -> GraphQL: %s", gql_query)

            try:
                agg_document = parse_query(schema, gql_query)
            except (GraphQLValidationError, Exception) as e:
                return _jsonapi_error_response(400, "Bad Request", str(e))

            agg_compiled_queries = compile_query(agg_document, ctx)
            if not agg_compiled_queries:
                return _jsonapi_error_response(400, "Bad Request", "Compilation failed")
            agg_compiled = agg_compiled_queries[0]

            from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

            try:
                agg_plan = await _govern_and_route_compiled(
                    agg_compiled.sql,
                    role_id,
                    exec_params=agg_compiled.params or None,
                    state=state,
                )
                agg_result = await _execute_plan(agg_plan, state)

                nodes_result = None
                if agg_compiled.nodes_sql is not None:
                    nodes_plan = await _govern_and_route_compiled(
                        agg_compiled.nodes_sql,
                        role_id,
                        exec_params=agg_compiled.nodes_params or None,
                        state=state,
                    )
                    nodes_result = await _execute_plan(nodes_plan, state)
            except PermissionError as e:
                return _jsonapi_error_response(403, "Forbidden", str(e))
            except HTTPException as e:
                if e.status_code == 503:
                    return _jsonapi_error_response(503, "Service Unavailable", e.detail)
                raise
            except Exception as e:
                log.exception("JSON:API aggregate query execution failed for %s", gql_table)
                return _jsonapi_error_response(500, "Internal Server Error", str(e))

            from provisa.executor.serialize import serialize_aggregate, serialize_group_by

            if is_group_by:
                shaped = serialize_group_by(
                    agg_result.rows,
                    agg_compiled.columns,
                    nodes_result.rows if nodes_result is not None else None,
                    agg_compiled.nodes_columns if nodes_result is not None else None,
                    agg_compiled.root_field,
                )
                group_rows = shaped.get("data", {}).get(agg_compiled.root_field, [])
                # REQ-1417: groupKey is already keyed physically — its field names come straight
                # from the ?groupBy= list (compiler/aggregates.py ColumnRef field_name=col) — but
                # the nodes projection is keyed by the GraphQL selection set, so it is renamed back.
                for row in group_rows:
                    if "nodes" in row:
                        row["nodes"] = [
                            rename_row_keys(
                                node,
                                gql_to_physical,
                                rel_gql_to_physical,
                                rel_scalar_gql_to_physical,
                            )
                            for node in row["nodes"]
                        ]
                data = [
                    {"type": f"{table_name}GroupBy", "id": str(idx), "attributes": row}
                    for idx, row in enumerate(group_rows)
                ]
                return JSONResponse(content={"data": data}, media_type=JSONAPI_CONTENT_TYPE)

            shaped = serialize_aggregate(
                agg_result.rows,
                agg_compiled.columns,
                None,
                None,
                agg_compiled.root_field,
                agg_alias=agg_compiled.agg_alias,
            )
            agg_payload = (
                shaped.get("data", {}).get(agg_compiled.root_field, {}).get(agg_compiled.agg_alias, {})
            )
            return JSONResponse(
                content={"data": None, "meta": {"aggregate": agg_payload}},
                media_type=JSONAPI_CONTENT_TYPE,
            )

        # REQ-257: ?include=rel1,rel2 — sideload related resources as a compound document.
        # REQ-1417: the names are physical; they are translated to the schema's field names here.
        include_param = raw_params.get("include")
        physical_include_names = (
            [n.strip() for n in include_param.split(",") if n.strip()] if include_param else []
        )
        for inc in physical_include_names:
            if inc not in rel_physical_to_gql:
                return _jsonapi_error_response(
                    400,
                    "Invalid Include",
                    f"Unknown relationship {inc!r}",
                    source_parameter="include",
                )
        include_names = [rel_physical_to_gql[inc] for inc in physical_include_names]
        query_fields = list(selected_fields)
        for inc in include_names:
            inc_scalars = list(rel_scalars_by_rel[inc])
            if "id" in inc_scalars:
                inc_scalars = ["id"] + [s for s in inc_scalars if s != "id"]
            query_fields.append(f"{inc} {{ {' '.join(inc_scalars)} }}")
            # the FK column must be selected so the resource's relationship linkage resolves.
            # It is named from the *physical* convention (``pet`` → ``pet_id``) and translated to
            # the schema's spelling, rather than assuming one — under apollo the exposed column is
            # ``petId``, under snake it stays ``pet_id``.
            fk = physical_to_gql.get(f"{rel_gql_to_physical[inc]}_id")
            if fk and fk not in query_fields:
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
                buffered=True,  # REQ-1224: buffered transport — terminal auto-thresholds inline vs CTAS
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

        # REQ-1417: back to physical names before anything downstream keys off them, so the
        # relationship map, the included buckets, and the emitted attributes all speak one
        # convention.
        rows = [
            rename_row_keys(
                row, gql_to_physical, rel_gql_to_physical, rel_scalar_gql_to_physical
            )
            for row in rows
        ]
        physical_rel_fields = {f"{p}_id": p for p in rel_physical_to_gql}

        # REQ-257: pull nested included entities out of the rows into a deduplicated set.
        included_rows = _extract_included(rows, physical_include_names)

        # Build JSON:API document (compound when includes were requested)
        doc = rows_to_jsonapi(
            rows,
            table_name,
            id_field="id",
            relationship_fields=physical_rel_fields,
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
