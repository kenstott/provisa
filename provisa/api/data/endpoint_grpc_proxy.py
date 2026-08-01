# Copyright (c) 2026 Kenneth Stott
# Canary: bf1b51eb-bbd4-4b84-97e1-cce9284990d3
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""HTTP→gRPC proxy endpoint for the gRPC Explorer UI (Phase AB7).

Translates POST /data/grpc/{TypeName} into the same pipeline used by the
real gRPC servicer: parse GraphQL query, compile, govern + route, execute.
This endpoint lets the browser-based gRPC Explorer call gRPC methods without
a native gRPC client.
"""

# Requirements: REQ-045, REQ-143, REQ-266

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from provisa.api.errors import ApiError
from provisa.grpc.query_ir import (
    AGG_FUNCS,
    grpc_table_to_aggregate_graphql_text,
    grpc_table_to_group_by_graphql_text,
    grpc_table_to_semantic_sql,
    split_agg_columns,
    split_group_by_columns,
)
from provisa.grpc.proto_gen import _to_proto_field_name
from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


def _parse_read_mask(body: dict) -> dict[str, set[str] | None]:
    """Parse read_mask (proto field names, dot-notation) into a projection map.

    "status" → include status fully; "_meta" → include _meta with all sub-fields
    (None); "_meta.source_id" → include _meta restricted to source_id.
    """
    read_mask = body.get("read_mask") or {}
    mask_paths = read_mask.get("paths") or [] if isinstance(read_mask, dict) else []
    mask_map: dict[str, set[str] | None] = {}
    for p in mask_paths:
        parts = p.split(".", 1)
        top = parts[0]
        sub = parts[1] if len(parts) > 1 else None
        if top not in mask_map:
            mask_map[top] = set() if sub else None
        if sub and mask_map[top] is not None:
            mask_map[top].add(sub)  # type: ignore[union-attr]
        elif not sub:
            mask_map[top] = None
    return mask_map


def _apply_read_mask(proto_rows, mask_map: dict[str, set[str] | None]):
    """Project proto-keyed rows to the read_mask (mask_map keys are proto names)."""
    if not (mask_map and isinstance(proto_rows, list)):
        return proto_rows

    def _restrict(v, subs: set[str]):
        if isinstance(v, dict):
            return {sk: sv for sk, sv in v.items() if sk in subs}
        if isinstance(v, list):
            return [_restrict(item, subs) for item in v]
        return v

    projected: list[object] = []
    for row in proto_rows:
        if not isinstance(row, dict):
            projected.append(row)
            continue
        kept: dict[str, object] = {}
        for k, v in row.items():
            if k not in mask_map:
                continue
            subs = mask_map[k]
            kept[k] = v if subs is None else _restrict(v, subs)
        projected.append(kept)
    return projected


@router.get("/grpc-commands/{role_id}")
async def grpc_commands(role_id: str):  # REQ-1156
    """List role-visible registered commands (tracked functions) for the gRPC Explorer.

    The gRPC surface exposes every command through the single generic ``CallCommand`` RPC, so
    the browser Explorer can't discover them from the proto. Mirror the GraphQL action-field
    visibility gate (visible_to + domain access) to populate the command picker.
    """
    from provisa.api.app import state

    fns = getattr(state, "tracked_functions", {}) or {}
    role = state.roles.get(role_id) or {}
    accessible = set(role.get("domain_access") or [])
    all_access = "*" in accessible

    seen: set[str] = set()
    out: list[dict] = []
    for fn in fns.values():
        name = fn.get("name")
        if not name or name in seen:
            continue
        visible_to = fn.get("visible_to") or []
        if visible_to and role_id not in visible_to:
            continue
        domain_id = fn.get("domain_id", "")
        if not all_access and domain_id and domain_id not in accessible:
            continue
        seen.add(name)
        out.append(
            {
                "name": name,
                "description": fn.get("description"),
                "arguments": [
                    {"name": a.get("name"), "type": a.get("type")}
                    for a in (fn.get("arguments") or [])
                    if a.get("name")
                ],
            }
        )
    return out


@router.post("/grpc-command/{role_id}")
async def grpc_command(role_id: str, request: Request):  # REQ-1156
    """Invoke a registered command via the shared executor — the HTTP mirror of CallCommand.

    Body: ``{name, args_json}`` (args_json is a JSON object string, matching the CommandRequest
    proto). writable_by/governance is enforced inside invoke_tracked_function.
    """
    import json

    from provisa.api.app import state
    from provisa.api.data.action_exec import invoke_tracked_function

    body = await request.json()
    name = body.get("name")
    if not name:
        raise ApiError(400, "data.missing_command_name", "Missing command name")
    if name not in (getattr(state, "tracked_functions", {}) or {}):
        raise ApiError(404, "data.unknown_command", f"Unknown command {name!r}", name=name)

    raw = body.get("args_json")
    parsed_args: Any
    if raw in (None, ""):
        parsed_args = {}
    elif isinstance(raw, str):
        try:
            parsed_args = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ApiError(
                400, "data.args_json_invalid", f"args_json not valid JSON: {exc}", error=str(exc)
            )
    else:
        parsed_args = raw
    if not isinstance(parsed_args, dict):
        raise ApiError(400, "data.args_json_not_object", "args_json must be a JSON object")
    args = parsed_args

    try:
        rows = await invoke_tracked_function(name, args, state, role_id)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    from fastapi.encoders import jsonable_encoder

    return JSONResponse(jsonable_encoder(rows))


@router.get("/grpc-group-by-columns/{role_id}/{type_name}")
async def grpc_group_by_columns(role_id: str, type_name: str):  # REQ-1361
    """List the columns valid in a Query{Type}GroupBy request's ``by`` argument.

    The gRPC Explorer's group-by picker must offer only columns the server's
    ``{Type}DistinctOnColumn`` enum (the schema's actual source of truth for valid ``by``
    columns, per ``_build_distinct_on_enum``) accepts — the ``{Type}Filter`` message's field
    set the picker used before is a different, broader column set and yields runtime 400s.
    """
    from graphql import GraphQLEnumType

    from provisa.api.app import state
    from provisa.grpc.query_ir import _find_table_meta

    if role_id not in state.schemas:
        raise ApiError(
            404, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    ctx = state.contexts[role_id]
    base_type_name = type_name[: -len("GroupBy")] if type_name.endswith("GroupBy") else type_name
    meta = _find_table_meta(ctx, base_type_name)
    if meta is None:
        return []

    schema = state.schemas[role_id]
    enum_type = schema.get_type(f"{meta.type_name}DistinctOnColumn")
    if not isinstance(enum_type, GraphQLEnumType):
        return []
    return sorted(enum_type.values)


@router.get("/jsonapi-group-by-columns/{role_id}/{domain_id}/{table_name}")
async def jsonapi_group_by_columns(role_id: str, domain_id: str, table_name: str):  # REQ-1361
    """List the columns valid in a JSON:API ``?groupBy=`` request for this table.

    Same rationale as ``grpc_group_by_columns``: the ``{Type}DistinctOnColumn`` enum is the
    schema's actual source of truth for valid ``by`` columns, not the table's full column set.
    """
    from graphql import GraphQLEnumType

    from provisa.api.app import state

    if role_id not in state.schemas:
        raise ApiError(
            404, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    ctx = state.contexts[role_id]
    meta = next(
        (
            m
            for m in ctx.tables.values()
            if m.domain_id == domain_id and m.table_name == table_name
        ),
        None,
    )
    if meta is None:
        return []

    schema = state.schemas[role_id]
    enum_type = schema.get_type(f"{meta.type_name}DistinctOnColumn")
    if not isinstance(enum_type, GraphQLEnumType):
        return []
    return sorted(enum_type.values)


@router.post("/grpc/{type_name}")
async def grpc_proxy(type_name: str, request: Request):  # REQ-045, REQ-266
    """Translate an HTTP+JSON request into the gRPC query pipeline and return JSON rows."""
    from provisa.api.app import state

    body = await request.json()
    role_id = request.headers.get("x-provisa-role") or body.get("role_id") or body.get("role")
    limit = int(body.get("limit", 100))

    if not role_id:
        raise ApiError(400, "data.missing_role_id", "Missing role_id")
    if role_id not in state.schemas:
        raise ApiError(
            404, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )

    ctx = state.contexts[role_id]
    mask_map = _parse_read_mask(body)

    # REQ-1359: Aggregate/GroupBy synthetic proto type names have no semantic-SQL table match —
    # route them through the same GraphQL-text synthesis + compile path the native gRPC servicer
    # (provisa/grpc/server.py::_handle_query_aggregate_bound / _handle_query_group_by_bound) uses,
    # instead of falling through to grpc_table_to_semantic_sql (which only knows plain tables).
    if type_name.endswith("Aggregate") or type_name.endswith("GroupBy"):
        from provisa.compiler.parser import GraphQLValidationError, parse_query
        from provisa.compiler.sql_gen import compile_query
        from fastapi.encoders import jsonable_encoder

        schema = state.schemas[role_id]
        is_group_by = type_name.endswith("GroupBy")
        # grpc_table_to_*_graphql_text match against the bare table type name (mirrors
        # server.py's __getattr__, which strips both the "Query" prefix and the
        # "Aggregate"/"GroupBy" suffix before dispatching) — this endpoint's {type_name} path
        # param only ever carries the suffix, never the "Query" prefix.
        base_type_name = type_name[: -len("GroupBy")] if is_group_by else type_name[: -len("Aggregate")]

        # REQ-1361: optional "funcs" body param restricts which aggregate functions are
        # computed (parity with JSON:API/REST's ?aggregate=count,sum), instead of always
        # returning every function the schema exposes for the table.
        funcs = body.get("funcs") or None
        if funcs is not None:
            if not isinstance(funcs, list) or any(f not in AGG_FUNCS for f in funcs):
                raise ApiError(
                    400,
                    "data.invalid_aggregate_functions",
                    f"funcs must be a subset of {list(AGG_FUNCS)!r}",
                    funcs=funcs,
                )

        if is_group_by:
            by_columns = list(body.get("by") or [])
            gql_text = grpc_table_to_group_by_graphql_text(ctx, base_type_name, by_columns, funcs)
        else:
            gql_text = grpc_table_to_aggregate_graphql_text(ctx, base_type_name, funcs)

        if gql_text is None:
            raise ApiError(
                404,
                "data.no_query_field_for_proto_type",
                f"No query field for proto type {type_name!r} under role {role_id!r}",
                type_name=type_name,
                role_id=role_id,
            )

        try:
            document = parse_query(schema, gql_text)
            compiled_queries = compile_query(document, ctx)
        except GraphQLValidationError as exc:
            raise ApiError(400, "data.aggregate_query_validation_failed", str(exc)) from exc
        if not compiled_queries:
            raise HTTPException(status_code=500, detail="Aggregate/GroupBy compilation failed")
        compiled = compiled_queries[0]

        try:
            plan = await _govern_and_route_compiled(
                compiled.sql, role_id, exec_params=compiled.params or None, state=state
            )
            result = await _execute_plan(plan, state)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        if is_group_by:
            group_key_cols, group_key_idx, agg_cols, agg_idx = split_group_by_columns(compiled.columns)
            out_rows = []
            for row in result.rows:
                group_key = {c.field_name: row[i] for c, i in zip(group_key_cols, group_key_idx)}
                agg_row = tuple(row[i] for i in agg_idx)
                top, nested = split_agg_columns(agg_cols, agg_row)
                out_rows.append({"group_key": group_key, "aggregate": {**top, **nested}})
            return JSONResponse(jsonable_encoder(out_rows))

        row = result.rows[0] if result.rows else ()
        top, nested = split_agg_columns(compiled.columns, row)
        return JSONResponse(jsonable_encoder({**top, **nested}))

    # Same IR path as the native gRPC servicer (query language → IR → governed IR → plan → physical).
    # Lower the request straight to a semantic SELECT — never round-trip through GraphQL.
    semantic_sql = grpc_table_to_semantic_sql(ctx, type_name, limit)
    if semantic_sql is None:
        raise ApiError(
            404,
            "data.no_query_field_for_proto_type",
            f"No query field for proto type {type_name!r} under role {role_id!r}",
            type_name=type_name,
            role_id=role_id,
        )

    try:
        plan = await _govern_and_route_compiled(semantic_sql, role_id, state=state)
        result = await _execute_plan(plan, state)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    # Key each row by the proto field name (the physical column → proto name authority), then apply
    # the read-mask field restriction.
    proto_cols = [_to_proto_field_name(c) for c in result.column_names]
    proto_rows = [
        {
            proto_cols[i]: row[i]
            for i in range(len(proto_cols))
            if i < len(row) and row[i] is not None
        }
        for row in result.rows
    ]
    proto_rows = _apply_read_mask(proto_rows, mask_map)
    # Coerce driver-native scalars (PG Decimal, date/datetime) the JSON encoder can't emit directly.
    from fastapi.encoders import jsonable_encoder

    return JSONResponse(jsonable_encoder(proto_rows))
