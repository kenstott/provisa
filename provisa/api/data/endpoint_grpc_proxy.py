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

from provisa.api.json_response import OrjsonResponse
from provisa.api.acting_role import acting_role, named_role
from provisa.api.errors import ApiError
from provisa.grpc.query_ir import (
    AGG_FUNCS,
    FilterError,
    ReadMaskError,
    grpc_table_to_aggregate_graphql_text,
    grpc_table_to_group_by_graphql_text,
    grpc_table_to_semantic_sql,
    resolve_read_mask,
    restrict_json,
    split_agg_columns,
    split_group_by_columns,
)
from provisa.grpc.proto_gen import _to_proto_field_name
from provisa.compiler.directives import cache_hint_from_grpc_metadata
from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled
from provisa.compiler.complexity import ComplexityLimitExceeded

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


def _read_mask_paths(body: dict) -> list[str]:
    """The request body's ``read_mask.paths`` (proto field names, dot-notation into JSON-valued
    fields). An absent ``read_mask`` or ``paths`` is the empty mask — every field."""
    read_mask = body.get("read_mask")
    if read_mask is None:
        return []
    paths = read_mask.get("paths", []) if isinstance(read_mask, dict) else None
    if not isinstance(paths, list) or not all(isinstance(p, str) for p in paths):
        raise ApiError(
            400,
            "data.invalid_read_mask",
            'read_mask must be an object {"paths": [<field path>, ...]}',
        )
    return paths


def _filter_object(body: dict) -> dict | None:
    """The request body's ``filter`` — field → value equality, the JSON form of the native
    request's ``{Type}Filter`` message. Absent means no filter."""
    filter_ = body.get("filter")
    if filter_ is not None and not isinstance(filter_, dict):
        raise ApiError(
            400, "data.invalid_filter", "filter must be an object {<field>: <value>, ...}"
        )
    return filter_


@router.get("/grpc-commands/{role_id}")
async def grpc_commands(role_id: str, request: Request):  # REQ-1156
    """List role-visible registered commands (tracked functions) for the gRPC Explorer.

    The gRPC surface exposes every command through the single generic ``CallCommand`` RPC, so
    the browser Explorer can't discover them from the proto. Mirror the GraphQL action-field
    visibility gate (visible_to + domain access) to populate the command picker.
    """
    from provisa.api.app import state

    from provisa.api.data.action_exec import list_visible_commands
    from provisa.security.rights import require_role

    role_id = named_role(request, role_id)
    require_role(state.roles, role_id)
    # The one discovery list (functions and webhooks the role may call), as the picker shows it.
    return [
        {"name": c["name"], "description": c["description"], "arguments": c["arguments"]}
        for c in list_visible_commands(state, role_id)
    ]


@router.post("/grpc-command/{role_id}")
async def grpc_command(role_id: str, request: Request):  # REQ-1156
    """Invoke a registered command via the shared executor — the HTTP mirror of CallCommand.

    Body: ``{name, args_json}`` (args_json is a JSON object string, matching the CommandRequest
    proto). The command admission and governance are inside invoke_tracked_function.
    """
    import json

    from provisa.api.app import state
    from provisa.api.data.action_exec import bind_named_args, invoke_tracked_function

    role_id = named_role(request, role_id)  # the command runs AS this role: held by the caller
    body = await request.json()
    name = body.get("name")
    if not name:
        raise ApiError(400, "data.missing_command_name", "Missing command name")
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
    try:
        args = bind_named_args(name, parsed_args, state, role_id)
        rows = await invoke_tracked_function(name, args, state, role_id)
    except ComplexityLimitExceeded:
        raise  # REQ-1174: answered as 413 by the app's handler
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))

    return OrjsonResponse(rows)


@router.get("/grpc-group-by-columns/{role_id}/{type_name}")
async def grpc_group_by_columns(role_id: str, type_name: str, request: Request):  # REQ-1361
    """List the columns valid in a Query{Type}GroupBy request's ``by`` argument, and (REQ-1882)
    the same table's aggregate-eligible columns for a GroupBy/Aggregate request's ``columns``
    picker — both draw from ``ctx.aggregate_columns``, the identical universe
    ``_agg_fields_selection`` (provisa/grpc/query_ir.py) restricts against server-side.

    The gRPC Explorer's group-by picker must offer only columns the server's
    ``{Type}DistinctOnColumn`` enum (the schema's actual source of truth for valid ``by``
    columns, per ``_build_distinct_on_enum``) accepts — the ``{Type}Filter`` message's field
    set the picker used before is a different, broader column set and yields runtime 400s.
    """
    from graphql import GraphQLEnumType

    from provisa.api.app import state
    from provisa.grpc.query_ir import _find_table_meta

    role_id = named_role(request, role_id)
    if role_id not in state.schemas:
        raise ApiError(
            404, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    ctx = state.contexts[role_id]
    # REQ-1882: an Aggregate-suffixed typeName (the columns picker's caller for Query{Type}
    # Aggregate) needs the same stripping GroupBy already got, or _find_table_meta never matches.
    if type_name.endswith("GroupBy"):
        base_type_name = type_name[: -len("GroupBy")]
    elif type_name.endswith("Aggregate"):
        base_type_name = type_name[: -len("Aggregate")]
    else:
        base_type_name = type_name
    meta = _find_table_meta(ctx, base_type_name)
    if meta is None:
        return []

    schema = state.schemas[role_id]
    enum_type = schema.get_type(f"{meta.type_name}DistinctOnColumn")
    if not isinstance(enum_type, GraphQLEnumType):
        return []
    # REQ-1361/naming: gRPC's ``by`` argument takes proto-native column names (translated to the
    # GraphQL enum's convention internally, see grpc_table_to_group_by_graphql_text) — the picker
    # must offer the same names the request body actually accepts, not the enum's GQL-convention
    # spelling.
    return sorted(c for c, _t in ctx.aggregate_columns.get(meta.table_id, []))


@router.get("/jsonapi-group-by-columns/{role_id}/{domain_id}/{table_name}")
async def jsonapi_group_by_columns(
    role_id: str, domain_id: str, table_name: str, request: Request
):  # REQ-1361
    """List the columns valid in a JSON:API ``?groupBy=`` request for this table.

    Same rationale as ``grpc_group_by_columns``: the ``{Type}DistinctOnColumn`` enum is the
    schema's actual source of truth for valid ``by`` columns, not the table's full column set.
    """
    from graphql import GraphQLEnumType

    from provisa.api.app import state

    role_id = named_role(request, role_id)
    if role_id not in state.schemas:
        raise ApiError(
            404, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    ctx = state.contexts[role_id]
    meta = next(
        (m for m in ctx.tables.values() if m.domain_id == domain_id and m.table_name == table_name),
        None,
    )
    if meta is None:
        return []

    schema = state.schemas[role_id]
    enum_type = schema.get_type(f"{meta.type_name}DistinctOnColumn")
    if not isinstance(enum_type, GraphQLEnumType):
        return []
    # Same rationale as grpc_group_by_columns: offer the API-native column names the
    # ``?groupBy=`` request body actually accepts, not the enum's GQL-convention spelling.
    return sorted(c for c, _t in ctx.aggregate_columns.get(meta.table_id, []))


@router.post("/grpc/{type_name}")
async def grpc_proxy(type_name: str, request: Request):  # REQ-045, REQ-266
    """Translate an HTTP+JSON request into the gRPC query pipeline and return JSON rows."""
    from provisa.api.app import state

    body = await request.json()
    # REQ-273: the request runs as the role the auth layer established (one held role, or the
    # meta-role of the set X-Provisa-Role names); a body role that differs from it is refused.
    role_id = acting_role(
        request, request.headers.get("x-provisa-role"), body.get("role_id") or body.get("role"), ""
    )
    # REQ-544: the native servicer's `x-provisa-cache` / `x-provisa-cache-ttl` opt-in, as headers.
    cache_hint = cache_hint_from_grpc_metadata(request.headers.items())
    limit = int(body.get("limit", 100))

    if not role_id:
        raise ApiError(400, "data.missing_role_id", "Missing role_id")
    if role_id not in state.schemas:
        raise ApiError(
            404, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )

    ctx = state.contexts[role_id]

    # REQ-1359: Aggregate/GroupBy synthetic proto type names have no semantic-SQL table match —
    # route them through the same GraphQL-text synthesis + compile path the native gRPC servicer
    # (provisa/grpc/server.py::_handle_query_aggregate_bound / _handle_query_group_by_bound) uses,
    # instead of falling through to grpc_table_to_semantic_sql (which only knows plain tables).
    if type_name.endswith("Aggregate") or type_name.endswith("GroupBy"):
        from provisa.compiler.parser import GraphQLValidationError, parse_query
        from provisa.compiler.sql_gen import compile_query

        schema = state.schemas[role_id]
        is_group_by = type_name.endswith("GroupBy")
        # grpc_table_to_*_graphql_text match against the bare table type name (mirrors
        # server.py's __getattr__, which strips both the "Query" prefix and the
        # "Aggregate"/"GroupBy" suffix before dispatching) — this endpoint's {type_name} path
        # param only ever carries the suffix, never the "Query" prefix.
        base_type_name = (
            type_name[: -len("GroupBy")] if is_group_by else type_name[: -len("Aggregate")]
        )

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

        # REQ-1401/REQ-1408: include_nodes/include widen the group-by result with a nodes
        # sub-selection. The Explorer sends the same body the native RPC takes, so the proxy has
        # to honour both here or the two surfaces answer the same request differently.
        include_nodes = bool(body.get("include_nodes"))
        include = list(body.get("include") or [])
        if is_group_by:
            by_columns = list(body.get("by") or [])
            # REQ-803: the body's filter is the native request's {Type}GroupByRequest.filter — the
            # same lowering (a GraphQL where argument), so both surfaces group the same rows.
            try:
                gql_text = grpc_table_to_group_by_graphql_text(
                    ctx,
                    base_type_name,
                    by_columns,
                    funcs,
                    include_nodes=include_nodes,
                    include=include,
                    filter_msg=_filter_object(body),
                )
            except FilterError as exc:
                raise ApiError(
                    400,
                    "data.invalid_filter_field",
                    str(exc),
                    field=exc.field,
                    type_name=base_type_name,
                ) from exc
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
            document = parse_query(schema, gql_text, ctx=ctx)
            compiled_queries = compile_query(document, ctx)
        except GraphQLValidationError as exc:
            raise ApiError(400, "data.aggregate_query_validation_failed", str(exc)) from exc
        if not compiled_queries:
            raise HTTPException(status_code=500, detail="Aggregate/GroupBy compilation failed")
        compiled = compiled_queries[0]

        try:
            plan = await _govern_and_route_compiled(
                compiled.sql,
                role_id,
                exec_params=compiled.params or None,
                state=state,
                cache_hint=cache_hint,
                serve_cached=True,  # REQ-1897: _execute_plan serves the pre-route HIT
                sdl_joins=True,
            )
            result = await _execute_plan(plan, state)
        except ComplexityLimitExceeded:
            raise  # REQ-1174: answered as 413 by the app's handler
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc))
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

        if is_group_by:
            group_key_cols, group_key_idx, agg_cols, agg_idx = split_group_by_columns(
                compiled.columns
            )

            # The nodes selection compiles to a second SQL string keyed by the group-by columns;
            # run it through the identical govern → route → execute pipeline and join on that key,
            # exactly as the native servicer does (provisa/grpc/server.py::
            # _handle_query_group_by_bound).
            from provisa.executor.serialize import _convert_value

            nodes_by_group_key: dict[tuple, list] = {}
            if include_nodes and compiled.nodes_sql is not None and compiled.nodes_columns:
                try:
                    nodes_plan = await _govern_and_route_compiled(
                        compiled.nodes_sql,
                        role_id,
                        exec_params=compiled.nodes_params or None,
                        state=state,
                        cache_hint=cache_hint,
                        serve_cached=True,  # REQ-1897
                        sdl_joins=True,
                    )
                    nodes_result = await _execute_plan(nodes_plan, state)
                except ComplexityLimitExceeded:
                    raise  # REQ-1174: answered as 413 by the app's handler
                except PermissionError as exc:
                    raise HTTPException(status_code=403, detail=str(exc))
                except Exception as exc:
                    raise HTTPException(status_code=503, detail=str(exc)) from exc
                join_key_idx = [
                    i for i, c in enumerate(compiled.nodes_columns) if c.nested_in == "__join_key__"
                ]
                output_cols = [
                    (i, c) for i, c in enumerate(compiled.nodes_columns) if c.nested_in is None
                ]
                for node_row in nodes_result.rows:
                    join_key = tuple(_convert_value(node_row[i]) for i in join_key_idx)
                    nodes_by_group_key.setdefault(join_key, []).append(
                        {c.field_name: _convert_value(node_row[i]) for i, c in output_cols}
                    )

            out_rows = []
            for row in result.rows:
                group_key = {c.column: row[i] for c, i in zip(group_key_cols, group_key_idx)}
                agg_row = tuple(row[i] for i in agg_idx)
                top, nested = split_agg_columns(agg_cols, agg_row)
                out_row: dict[str, Any] = {"group_key": group_key, "aggregate": {**top, **nested}}
                if include_nodes:
                    join_key = tuple(_convert_value(row[i]) for i in group_key_idx)
                    out_row["nodes"] = nodes_by_group_key.get(join_key, [])
                out_rows.append(out_row)
            return OrjsonResponse(out_rows)

        row = result.rows[0] if result.rows else ()
        top, nested = split_agg_columns(compiled.columns, row)
        return OrjsonResponse({**top, **nested})

    # Same IR path as the native gRPC servicer (query language → IR → governed IR → plan → physical).
    # Lower the request straight to a semantic SELECT — never round-trip through GraphQL.
    # REQ-803: the read_mask and the filter are part of the QUERY — the native servicer's
    # semantics, from the same functions: only the masked columns are selected, the filter is the
    # statement's WHERE, and a mask path or filter field that is not a field this role can read is
    # rejected by name rather than dropped.
    try:
        read_mask = resolve_read_mask(ctx, type_name, _read_mask_paths(body))
        semantic = grpc_table_to_semantic_sql(
            ctx, type_name, limit, _filter_object(body), read_mask
        )
    except ReadMaskError as exc:
        raise ApiError(
            400, "data.invalid_read_mask_path", str(exc), path=exc.path, type_name=type_name
        ) from exc
    except FilterError as exc:
        raise ApiError(
            400, "data.invalid_filter_field", str(exc), field=exc.field, type_name=type_name
        ) from exc
    if semantic is None:
        raise ApiError(
            404,
            "data.no_query_field_for_proto_type",
            f"No query field for proto type {type_name!r} under role {role_id!r}",
            type_name=type_name,
            role_id=role_id,
        )
    # The statement is the request's shape; the filter values and the limit travel bound
    # (REQ-1877).
    semantic_sql, bound_params = semantic

    try:
        plan = await _govern_and_route_compiled(
            semantic_sql,
            role_id,
            exec_params=bound_params or None,
            state=state,
            cache_hint=cache_hint,
            # REQ-1897: an opted-in request whose entry exists is answered before routing; the
            # chokepoint below (_execute_plan) serves that Route.CACHE plan.
            serve_cached=True,
            sdl_joins=True,
        )
        result = await _execute_plan(plan, state)
    except ComplexityLimitExceeded:
        raise  # REQ-1174: answered as 413 by the app's handler
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    # Key each row by the proto field name (the physical column → proto name authority). The query
    # already selected only the masked columns; a JSON sub-path selection is applied to its value.
    proto_cols = [_to_proto_field_name(c) for c in result.column_names]
    selections = (
        read_mask.restrictions(result.column_names)
        if read_mask is not None
        else [None] * len(proto_cols)
    )
    proto_rows = [
        {
            proto_cols[i]: restrict_json(row[i], selections[i])
            for i in range(len(proto_cols))
            if i < len(row) and row[i] is not None
        }
        for row in result.rows
    ]
    # REQ-1867: a driver-native scalar orjson has no encoding for (a PG Decimal) goes through
    # jsonable_encoder on its own; datetimes it encodes itself.
    return OrjsonResponse(proto_rows)
