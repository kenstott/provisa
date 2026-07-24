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
from graphql import GraphQLSchema

from provisa.api._query_helpers import (
    build_graphql_query as _build_graphql_query_shared,
    get_scalar_fields as _get_scalar_fields_shared,
)
from provisa.compiler.parser import GraphQLValidationError, parse_query
from provisa.compiler.sql_gen import compile_query

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
                buffered=True,  # REQ-1224: buffered transport — terminal auto-thresholds inline vs CTAS
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
