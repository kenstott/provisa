# Copyright (c) 2026 Kenneth Stott
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
import re

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


def _pascal_to_snake(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def _proto_type_name(gql_name: str) -> str:
    """Mirror of proto_gen._to_proto_type_name: PS__Pets → PsPets."""
    if "__" in gql_name:
        prefix, rest = gql_name.split("__", 1)
        return prefix.capitalize() + rest
    return gql_name


def _is_scalar_field(gql_type) -> bool:
    from graphql import GraphQLScalarType, GraphQLEnumType, GraphQLNonNull

    if isinstance(gql_type, GraphQLNonNull):
        return _is_scalar_field(gql_type.of_type)
    return isinstance(gql_type, (GraphQLScalarType, GraphQLEnumType))


@router.post("/grpc/{type_name}")
async def grpc_proxy(type_name: str, request: Request):  # REQ-045, REQ-266
    """Translate an HTTP+JSON request into the gRPC query pipeline and return JSON rows."""
    from provisa.api.app import state
    from provisa.compiler.parser import parse_query
    from provisa.compiler.sql_gen import compile_query
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled
    from graphql import GraphQLList, GraphQLNonNull

    body = await request.json()
    role_id = request.headers.get("x-provisa-role") or body.get("role_id") or body.get("role")
    limit = int(body.get("limit", 100))

    if not role_id:
        raise HTTPException(status_code=400, detail="Missing role_id")
    if role_id not in state.schemas:
        raise HTTPException(status_code=404, detail=f"No schema for role {role_id!r}")

    schema = state.schemas[role_id]
    ctx = state.contexts[role_id]

    query_type = schema.query_type
    if query_type is None:
        raise HTTPException(status_code=404, detail=f"No query type for role {role_id!r}")

    # Try direct pascal→snake first (e.g. Pets → pets), then fall back to searching
    # by proto type name so domain-prefixed types resolve correctly (PsPets → ps__pets).
    field_name = _pascal_to_snake(type_name)
    if field_name not in query_type.fields:
        for fname, fld in query_type.fields.items():
            inner_t = fld.type
            while isinstance(inner_t, (GraphQLNonNull, GraphQLList)):
                inner_t = inner_t.of_type
            if hasattr(inner_t, "name") and _proto_type_name(inner_t.name) == type_name:
                field_name = fname
                break
        else:
            raise HTTPException(
                status_code=404,
                detail=f"No query field for proto type {type_name!r} under role {role_id!r}",
            )

    # Unwrap the field type to get its object type
    inner = query_type.fields[field_name].type
    while isinstance(inner, (GraphQLNonNull, GraphQLList)):
        inner = inner.of_type
    if not hasattr(inner, "fields"):
        raise HTTPException(status_code=400, detail=f"{type_name} is not an object type")

    # Build GQL field selections: scalars directly, object types as nested sub-selections.
    field_selections: list[str] = []
    for fname, f in inner.fields.items():
        if _is_scalar_field(f.type):
            field_selections.append(fname)
        else:
            ftype = f.type
            while isinstance(ftype, (GraphQLNonNull, GraphQLList)):
                ftype = ftype.of_type
            if hasattr(ftype, "fields"):
                sub_scalars = [
                    sn
                    for sn, sf in ftype.fields.items()
                    if _is_scalar_field(sf.type) and not (sn.startswith("_") and sn.endswith("_"))
                ]
                if sub_scalars:
                    field_selections.append(f"{fname} {{ {' '.join(sub_scalars)} }}")
    if not field_selections:
        raise HTTPException(status_code=400, detail=f"No fields found for {type_name}")

    # Apply read_mask with dot-notation support.
    # "status" → include status scalar fully
    # "_meta" → include _meta with all sub-fields
    # "_meta.source_id" → include _meta but restrict to source_id only
    read_mask = body.get("read_mask") or {}
    mask_paths: list[str] = read_mask.get("paths") or [] if isinstance(read_mask, dict) else []
    if mask_paths:
        # Build top_level_map: field → None (all sub-fields) or set of specific sub-fields
        top_level_map: dict[str, set[str] | None] = {}
        for p in mask_paths:
            parts = p.split(".", 1)
            top = parts[0]
            sub = parts[1] if len(parts) > 1 else None
            if top not in top_level_map:
                top_level_map[top] = set() if sub else None
            if sub and top_level_map[top] is not None:
                top_level_map[top].add(sub)  # type: ignore[union-attr]
            elif not sub:
                top_level_map[top] = None  # None means include all sub-fields

        filtered: list[str] = []
        for sel in field_selections:
            sel_name = sel.split()[0]
            snake_name = _pascal_to_snake(sel_name)
            key = (
                snake_name
                if snake_name in top_level_map
                else (sel_name if sel_name in top_level_map else None)
            )
            if key is None:
                continue
            sub_filter = top_level_map[key]
            if sub_filter is None or "{" not in sel:
                filtered.append(sel)
            else:
                # Restrict nested selection to specified sub-fields
                current_subs = re.findall(r"\b(\w+)\b", sel.split("{", 1)[1].rstrip("}").strip())
                restricted = [
                    s for s in current_subs if s in sub_filter or _pascal_to_snake(s) in sub_filter
                ]
                if restricted:
                    filtered.append(f"{sel_name} {{ {' '.join(restricted)} }}")
        if filtered:
            field_selections = filtered

    # Include native filter args (query_param / path_param) from body["filter"] in the GQL query.
    # schema_gen prefixes the GQL arg name with "_" when the bare name collides with a response
    # field (e.g. bare "id" → GQL arg "_id").  We check both the prefixed and bare name when
    # looking up the value in filter_dict, and store the bare name in nf_api_args so it matches
    # required_args[].name in _materialize_api_to_trino_cache.
    filter_dict = body.get("filter") or {}
    arg_parts: list[str] = []
    nf_api_args: dict = {}
    if isinstance(filter_dict, dict):
        for arg_name, arg_def in query_type.fields[field_name].args.items():
            if arg_def.description and "Native API filter" in arg_def.description:
                bare = arg_name.lstrip("_")
                val = (
                    filter_dict.get(arg_name) if arg_name in filter_dict else filter_dict.get(bare)
                )
                if val is not None:
                    lit = (
                        f'"{val}"'
                        if isinstance(val, str)
                        else str(val).lower()
                        if isinstance(val, bool)
                        else str(val)
                    )
                    arg_parts.append(f"{arg_name}: {lit}")
                    nf_api_args[bare] = val
    if limit > 0:
        arg_parts.append(f"limit: {limit}")
    arg_clause = f"({', '.join(arg_parts)})" if arg_parts else ""
    gql_query = f"{{ {field_name}{arg_clause} {{ {' '.join(field_selections)} }} }}"

    document = parse_query(schema, gql_query)
    compiled_queries = compile_query(document, ctx)
    if not compiled_queries:
        raise HTTPException(status_code=500, detail="No compiled queries")
    compiled = compiled_queries[0]

    try:
        plan = await _govern_and_route_compiled(
            compiled.sql,
            role_id,
            exec_params=compiled.params or None,
            state=state,
            api_args=nf_api_args or None,
        )
        result = await _execute_plan(plan, state)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    from provisa.executor.serialize import serialize_rows

    serialized = serialize_rows(result.rows, compiled.columns, field_name)
    gql_rows = (serialized.get("data") or {}).get(field_name) or []

    def _snake_keys(obj: object) -> object:  # object-ok: arbitrary serialized GQL value
        if isinstance(obj, dict):
            return {_pascal_to_snake(k): _snake_keys(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_snake_keys(item) for item in obj]
        return obj

    return JSONResponse(_snake_keys(gql_rows))
