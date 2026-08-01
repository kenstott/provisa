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

# Requirements: REQ-222, REQ-256, REQ-266, REQ-267

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from graphql import GraphQLObjectType, GraphQLSchema

from provisa.api._query_helpers import (
    build_graphql_query as _build_graphql_query_shared,
    get_scalar_fields as _get_scalar_fields_shared,
)
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.sql_gen import compile_query
from provisa.executor.serialize import serialize_aggregate, serialize_group_by

log = logging.getLogger(__name__)

# Supported WHERE operators
_WHERE_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "in"}


def _parse_where_params(params: dict[str, str]) -> dict[str, dict[str, Any]]:
    """Parse filter=JSON query param into structured filters.

    Accepts JSON string: filter=[{"field":"col","comparator":"eq","value":"x"}]
    Returns {column_name: {op: value, ...}, ...}.
    """
    import json

    raw = params.get("filter", "").strip()
    if not raw:
        return {}
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    filters: dict[str, dict[str, Any]] = {}
    for entry in entries if isinstance(entries, list) else []:
        field = entry.get("field")
        comparator = entry.get("comparator")
        value = entry.get("value")
        if not field or not comparator or value is None:
            continue
        if comparator not in _WHERE_OPS:
            continue
        filters.setdefault(field, {})[comparator] = value
    return filters


def _parse_order_by_params(params: dict[str, str]) -> list[dict[str, str]]:
    """Parse orderBy=JSON query param.

    Accepts JSON string: orderBy=[{"field":"col","direction":"asc"}]
    Returns [{"field": col, "dir": "asc"|"desc"}, ...].
    """
    import json

    raw = params.get("orderBy", "").strip()
    if not raw:
        return []
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError:
        return []
    ordering = []
    for entry in entries if isinstance(entries, list) else []:
        field = entry.get("field")
        direction = (entry.get("direction") or "asc").lower()
        if not field:
            continue
        if direction not in ("asc", "desc"):
            direction = "asc"
        ordering.append({"field": field, "dir": direction})
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


def _get_scalar_fields(schema: GraphQLSchema, table: str) -> list[str]:
    """Get only scalar (non-object) field names for a table."""
    return _get_scalar_fields_shared(schema, table)


# --- Aggregate / group-by param parsing and GraphQL-text synthesis (REQ-1359) ---
#
# Mirrors JSON:API's aggregate/groupBy handling: schema exposure of {field}_aggregate /
# {field}_group_by is already flag-gated at generation time (schema_gen.py), so REST needs
# zero new gating code here — an unknown field (table without the flag) fails GraphQL
# validation in parse_query() below and is already mapped to a 400.

_AGG_FUNCS = ("count", "sum", "avg", "stddev", "variance", "min", "max")


def _parse_aggregate_param(params: dict[str, str]) -> list[str] | None:
    """Parse ?aggregate=count,sum,avg (or bare ?aggregate / ?aggregate=true).

    Returns None when absent (not an aggregate request); [] means "all standard
    functions"; otherwise the explicit function list, filtered to known functions.
    """
    if "aggregate" not in params:
        return None
    raw = (params.get("aggregate") or "").strip()
    if not raw or raw.lower() == "true":
        return []
    return [f.strip() for f in raw.split(",") if f.strip() in _AGG_FUNCS]


def _parse_group_by_param(params: dict[str, str]) -> list[str]:
    """Parse ?groupBy=col1,col2 into a list of column names."""
    raw = params.get("groupBy", "").strip()
    if not raw:
        return []
    return [c.strip() for c in raw.split(",") if c.strip()]


def _unwrap_type(gql_type: Any) -> Any:
    while hasattr(gql_type, "of_type"):
        gql_type = gql_type.of_type
    return gql_type


def _resolve_agg_field_name(
    schema: GraphQLSchema, table: str, group_by: bool
) -> str | None:
    """Resolve {table}_aggregate / {table}_group_by against the live schema under either
    naming convention (snake or apollo camelCase — process-local, see naming.py), mirroring
    JSON:API's ``_resolve_query_field`` so REST doesn't hardcode one convention (REQ-1359)."""
    query_type = schema.query_type
    if query_type is None:
        return None
    snake_suffix, camel_suffix = ("_group_by", "GroupBy") if group_by else ("_aggregate", "Aggregate")
    for candidate in (f"{table}{snake_suffix}", f"{table}{camel_suffix}"):
        if candidate in query_type.fields:
            return candidate
    return None


def _agg_fields_map(
    schema: GraphQLSchema, table: str, group_by: bool
) -> dict[str, list[str]] | None:
    """Introspect {table}_aggregate / {table}_group_by's AggregateFields sub-type.

    Returns {func_name: [eligible column names]} ("count" always present, mapped to []),
    or None when the table doesn't expose the root field (flag disabled).
    """
    query_type = schema.query_type
    if query_type is None:
        return None
    field_name = _resolve_agg_field_name(schema, table, group_by)
    if field_name is None:
        return None
    field = query_type.fields.get(field_name)
    if field is None:
        return None

    result_type = _unwrap_type(field.type)
    if not isinstance(result_type, GraphQLObjectType):
        return None

    agg_field = result_type.fields.get("aggregate")
    if agg_field is None:
        return None

    agg_fields_type = _unwrap_type(agg_field.type)
    if not isinstance(agg_fields_type, GraphQLObjectType):
        return None

    result: dict[str, list[str]] = {"count": []}
    for func_name in _AGG_FUNCS[1:]:
        sub_field = agg_fields_type.fields.get(func_name)
        if sub_field is None:
            continue
        sub_type = _unwrap_type(sub_field.type)
        if isinstance(sub_type, GraphQLObjectType):
            result[func_name] = list(sub_type.fields.keys())
    return result


def _build_aggregate_selection(
    agg_map: dict[str, list[str]] | None,
    funcs: list[str],
    requested_cols: list[str] | None,
) -> str:
    """Build the sub-selection text for an `aggregate { ... }` GraphQL block."""
    if agg_map is None:
        # Unknown root field — parse_query() 400s on the outer field regardless of body.
        return "count"
    want_all = not funcs
    parts: list[str] = []
    for func_name in _AGG_FUNCS:
        if func_name == "count":
            if want_all or "count" in funcs:
                parts.append("count")
            continue
        if func_name not in agg_map or not (want_all or func_name in funcs):
            continue
        cols = agg_map[func_name]
        if requested_cols:
            cols = [c for c in cols if c in requested_cols]
        if not cols:
            continue
        parts.append(f"{func_name} {{ {' '.join(cols)} }}")
    return " ".join(parts) or "count"


def _format_where_arg(where: dict[str, dict[str, Any]]) -> str | None:
    """Format a parsed where dict into a `where: {...}` GraphQL argument fragment."""
    if not where:
        return None
    where_parts = []
    for col, ops in where.items():
        for op, val in ops.items():
            if isinstance(val, list):
                formatted = "[" + ", ".join(f'"{v}"' for v in val) + "]"
                where_parts.append(f"{col}: {{{op}: {formatted}}}")
            elif isinstance(val, str):
                try:
                    numeric: int | float = int(val)
                except ValueError:
                    try:
                        numeric = float(val)
                    except ValueError:
                        where_parts.append(f'{col}: {{{op}: "{val}"}}')
                        continue
                where_parts.append(f"{col}: {{{op}: {numeric}}}")
            else:
                where_parts.append(f"{col}: {{{op}: {val}}}")
    return "where: {" + ", ".join(where_parts) + "}"


def _build_aggregate_graphql_query(
    schema: GraphQLSchema,
    table: str,
    funcs: list[str],
    where: dict[str, dict[str, Any]],
    fields: list[str] | None,
) -> str:
    """Build `{ {table}_aggregate(where: ...) { aggregate { <selection> } } }` GraphQL text."""
    agg_map = _agg_fields_map(schema, table, group_by=False)
    selection = _build_aggregate_selection(agg_map, funcs, fields)
    where_arg = _format_where_arg(where)
    args_str = f"({where_arg})" if where_arg else ""
    field_name = _resolve_agg_field_name(schema, table, group_by=False) or f"{table}_aggregate"
    return f"{{ {field_name}{args_str} {{ aggregate {{ {selection} }} }} }}"


def _build_group_by_graphql_query(
    schema: GraphQLSchema,
    table: str,
    by_cols: list[str],
    funcs: list[str],
    where: dict[str, dict[str, Any]],
    order_by: list[dict[str, str]],
    limit: int | None,
    offset: int | None,
    fields: list[str] | None,
) -> str:
    """Build `{ {table}_group_by(by: [...]) { groupKey aggregate { <selection> } } }`."""
    agg_map = _agg_fields_map(schema, table, group_by=True)
    selection = _build_aggregate_selection(agg_map, funcs, fields)
    args_parts = [f"by: [{', '.join(by_cols)}]"]
    where_arg = _format_where_arg(where)
    if where_arg:
        args_parts.append(where_arg)
    if order_by:
        ob_parts = [f"{o['field']}: {o['dir']}" for o in order_by]
        args_parts.append("order_by: {" + ", ".join(ob_parts) + "}")
    if limit is not None:
        args_parts.append(f"limit: {limit}")
    if offset:
        args_parts.append(f"offset: {offset}")
    args_str = f"({', '.join(args_parts)})"
    field_name = _resolve_agg_field_name(schema, table, group_by=True) or f"{table}_group_by"
    return f"{{ {field_name}{args_str} {{ groupKey aggregate {{ {selection} }} }} }}"


def create_rest_router(state: Any) -> APIRouter:  # REQ-222, REQ-256, REQ-266, REQ-267
    """Create a REST router with auto-generated endpoints for each table.

    Args:
        state: AppState with schemas, contexts, rls_contexts.

    Returns:
        APIRouter mounted at /data/rest.
    """
    rest_router = APIRouter(prefix="/data/rest", tags=["rest"])

    @rest_router.get("/openapi.json", include_in_schema=False)
    async def rest_openapi_json(  # pyright: ignore[reportUnusedFunction]
        request: Request,
        role: str | None = Query(None),
        domains: str | None = Query(None),
    ):
        from provisa.api.rest.openapi_spec import generate_rest_openapi_spec

        auth_role = getattr(request.state, "role", None)
        role_id = auth_role or role
        if not role_id:
            raise HTTPException(status_code=401, detail="role required")
        domain_list = [d for d in domains.split(",") if d] if domains else None
        spec = generate_rest_openapi_spec(state, role_id, domains=domain_list)
        download = request.query_params.get("download")
        headers = {"Content-Disposition": "attachment; filename=openapi.json"} if download else {}
        return JSONResponse(content=spec, headers=headers)

    @rest_router.get("/docs", include_in_schema=False)
    async def rest_docs(  # pyright: ignore[reportUnusedFunction]
    ):
        from provisa.api.rest.openapi_spec import SWAGGER_UI_HTML

        return HTMLResponse(content=SWAGGER_UI_HTML)

    @rest_router.get("/{domain_id}/{table_name}")
    async def rest_table_endpoint(  # pyright: ignore[reportUnusedFunction]
        request: Request,
        domain_id: str,
        table_name: str,
        limit: int | None = Query(None, ge=1),
        offset: int | None = Query(None, ge=0),
        fields: str | None = Query(None),
    ):
        auth_role = getattr(request.state, "role", None)
        if not auth_role:
            raise HTTPException(status_code=401, detail="authenticated role required")
        role_id = auth_role

        if role_id not in state.schemas:
            raise HTTPException(
                status_code=400,
                detail=f"No schema available for role {role_id!r}",
            )

        schema = state.schemas[role_id]
        ctx = state.contexts[role_id]

        # Resolve {domain_id}/{table_name} → GQL field name via path map
        path_map = getattr(state, "table_path_maps", {}).get(role_id, {})
        table = next(
            (
                gql_field
                for gql_field, meta in path_map.items()
                if meta["domain_id"] == domain_id and meta["table_name"] == table_name
            ),
            None,
        )
        query_type = schema.query_type
        if table is None or query_type is None or table not in query_type.fields:
            raise HTTPException(
                status_code=404, detail=f"Table {domain_id!r}/{table_name!r} not found"
            )

        # Parse all query params
        raw_params = dict(request.query_params)

        where = _parse_where_params(raw_params)
        order_by = _parse_order_by_params(raw_params)

        # REQ-1359: aggregate/groupBy target {table}_aggregate / {table}_group_by instead of
        # the base table field. Schema exposure is already flag-gated (schema_gen.py); a
        # disabled table simply fails GraphQL validation below (400), no extra checks needed.
        agg_funcs = _parse_aggregate_param(raw_params)
        group_by_cols = _parse_group_by_param(raw_params)
        requested_fields = [f.strip() for f in fields.split(",")] if fields else None
        is_group_by = bool(group_by_cols)
        is_aggregate = agg_funcs is not None and not is_group_by

        if is_group_by:
            gql_query = _build_group_by_graphql_query(
                schema,
                table,
                group_by_cols,
                agg_funcs or [],
                where,
                order_by,
                limit,
                offset,
                requested_fields,
            )
        elif is_aggregate:
            gql_query = _build_aggregate_graphql_query(
                schema, table, agg_funcs or [], where, requested_fields
            )
        else:
            # Determine fields
            if fields:
                selected_fields = requested_fields or []
            else:
                selected_fields = _get_scalar_fields(schema, table)

            if not selected_fields:
                raise HTTPException(
                    status_code=400,
                    detail=f"No selectable fields for table {table!r}",
                )

            gql_query = _build_graphql_query(
                table,
                selected_fields,
                where,
                order_by,
                limit,
                offset,
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

        from provisa.pgwire._pipeline import _govern_and_route_compiled, _execute_plan

        try:
            plan = await _govern_and_route_compiled(
                compiled.sql,
                role_id,
                exec_params=compiled.params or None,
                state=state,
                # REQ-1224: buffered transport (terminal auto-thresholds inline vs CTAS) only applies
                # to raw row queries, which can be large; aggregate/group-by results are always small,
                # so route them the same unbuffered way JSON:API's aggregate branch does (REQ-1359) —
                # buffering was forcing them onto the CTAS/engine path unnecessarily.
                buffered=not (is_group_by or is_aggregate),
            )
            result = await _execute_plan(plan, state)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc))
        except HTTPException:
            raise
        except Exception as e:
            log.exception("REST query execution failed for %s", table)
            raise HTTPException(status_code=500, detail=str(e))

        if result.redirect is not None:
            # REQ-1224: the result exceeded the row threshold and was landed as an engine-native CTAS
            # off Provisa's heap — surface the delivery handle instead of buffering the body here.
            return JSONResponse(content={"data": None, "meta": {"redirect": result.redirect}})

        # REQ-1359: aggregate/group-by results aren't resource rows — reuse the same
        # serializers the GraphQL/data pipeline uses for _aggregate/_group_by fields, but
        # flatten to REST's existing flat {"data": ...} shape (no nodes requested).
        if is_group_by:
            gb_response = serialize_group_by(
                result.rows, compiled.columns, None, None, compiled.root_field
            )
            return JSONResponse(content={"data": gb_response["data"][compiled.root_field]})
        if is_aggregate:
            agg_response = serialize_aggregate(
                result.rows,
                compiled.columns,
                None,
                None,
                compiled.root_field,
                agg_alias=compiled.agg_alias,
            )
            agg_obj = agg_response["data"][compiled.root_field][compiled.agg_alias]
            return JSONResponse(content={"data": agg_obj})

        # Serialize
        from provisa.executor.serialize import serialize_rows

        response_data = serialize_rows(result.rows, compiled.columns, table)
        rows = response_data.get("data", {}).get(table, [])
        col_names = list(compiled.columns or [])
        accept = request.headers.get("accept", "application/json").lower()

        if "text/csv" in accept:
            import csv
            import io

            buf = io.StringIO()
            writer = csv.writer(buf)
            if rows:
                writer.writerow(rows[0].keys())
                for row in rows:
                    writer.writerow(row.values())
            from fastapi.responses import Response

            return Response(
                content=buf.getvalue(),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={table_name}.csv"},
            )

        if "application/vnd.apache.parquet" in accept:
            try:
                import io

                import pyarrow as pa
                import pyarrow.parquet as pq

                keys = list(rows[0].keys()) if rows else col_names
                arrays = {k: [row.get(k) for row in rows] for k in keys}
                table_pa = pa.table(arrays)
                buf = io.BytesIO()
                pq.write_table(table_pa, buf)
                from fastapi.responses import Response

                return Response(
                    content=buf.getvalue(),
                    media_type="application/vnd.apache.parquet",
                    headers={"Content-Disposition": f"attachment; filename={table_name}.parquet"},
                )
            except ImportError:
                raise HTTPException(status_code=400, detail="parquet format requires pyarrow")

        if "application/vnd.apache.arrow.stream" in accept:
            try:
                import io

                import pyarrow as pa

                keys = list(rows[0].keys()) if rows else col_names
                arrays = {k: [row.get(k) for row in rows] for k in keys}
                table_pa = pa.table(arrays)
                buf = io.BytesIO()
                with pa.ipc.new_stream(buf, table_pa.schema) as writer:
                    writer.write_table(table_pa)
                from fastapi.responses import Response

                return Response(
                    content=buf.getvalue(),
                    media_type="application/vnd.apache.arrow.stream",
                    headers={"Content-Disposition": f"attachment; filename={table_name}.arrow"},
                )
            except ImportError:
                raise HTTPException(status_code=400, detail="arrow format requires pyarrow")

        return JSONResponse(content={"data": rows})

    @rest_router.post("/{domain_id}/commands/{command_name}")
    async def rest_command_endpoint(  # pyright: ignore[reportUnusedFunction]  # REQ-1155
        request: Request,
        domain_id: str,
        command_name: str,
    ):
        """Invoke a registered command over REST — the OpenAPI mirror of the shared executor.

        Body is a JSON object of the command's declared arguments. Routes through the one
        governed executor (invoke_tracked_function), which enforces writable_by.
        """
        auth_role = getattr(request.state, "role", None)
        if not auth_role:
            raise HTTPException(status_code=401, detail="authenticated role required")
        role_id = auth_role

        # Functions AND webhooks are governed commands (REQ-872) — both callable over REST, both
        # routed through the one invoke_tracked_function executor below.
        fns = getattr(state, "tracked_functions", {}) or {}
        whs = getattr(state, "tracked_webhooks", {}) or {}
        fn = fns.get(command_name) or whs.get(command_name)
        if fn is None or fn.get("domain_id") != domain_id:
            raise HTTPException(
                status_code=404, detail=f"Command {domain_id!r}/{command_name!r} not found"
            )
        visible_to = fn.get("visible_to") or []
        if visible_to and role_id not in visible_to:
            raise HTTPException(
                status_code=404, detail=f"Command {domain_id!r}/{command_name!r} not found"
            )

        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="request body must be a JSON object")

        from provisa.api.data.action_exec import invoke_tracked_function

        try:
            rows = await invoke_tracked_function(command_name, body, state, role_id)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc))

        return JSONResponse(content={"data": rows})

    return rest_router
