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

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from provisa.compiler.parser import parse_query
from provisa.compiler.sql_gen import compile_query
from provisa.executor.serialize import serialize_rows
from provisa.grpc.proto_gen import _to_proto_field_name, _to_proto_type_name
from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])


def _is_scalar_field(gql_type) -> bool:
    from graphql import GraphQLEnumType, GraphQLNonNull, GraphQLScalarType

    if isinstance(gql_type, GraphQLNonNull):
        return _is_scalar_field(gql_type.of_type)
    return isinstance(gql_type, (GraphQLScalarType, GraphQLEnumType))


def _unwrap(gql_type):
    """Strip NonNull/List wrappers to reach the underlying named type."""
    from graphql import GraphQLList, GraphQLNonNull

    while isinstance(gql_type, (GraphQLNonNull, GraphQLList)):
        gql_type = gql_type.of_type
    return gql_type


def _resolve_field_name(query_type, type_name: str) -> str | None:
    """Return the query field whose object type maps to *type_name*, using the naming
    authority (proto_gen._to_proto_type_name) rather than a local case transform."""
    for fname, fld in query_type.fields.items():
        inner_t = _unwrap(fld.type)
        if hasattr(inner_t, "name") and _to_proto_type_name(inner_t.name) == type_name:
            return fname
    return None


def _build_field_selections(inner) -> list[str]:
    """GQL selections: scalars directly, object types as nested scalar sub-selections."""
    selections: list[str] = []
    for fname, f in inner.fields.items():
        if _is_scalar_field(f.type):
            selections.append(fname)
            continue
        ftype = _unwrap(f.type)
        if not hasattr(ftype, "fields"):
            continue
        sub_scalars = [
            sn
            for sn, sf in ftype.fields.items()
            if _is_scalar_field(sf.type) and not (sn.startswith("_") and sn.endswith("_"))
        ]
        if sub_scalars:
            selections.append(f"{fname} {{ {' '.join(sub_scalars)} }}")
    return selections


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


def _gql_literal(val) -> str:
    if isinstance(val, str):
        return f'"{val}"'
    if isinstance(val, bool):
        return str(val).lower()
    return str(val)


def _build_arg_clause(field, filter_dict, limit: int) -> tuple[str, dict]:
    """Build the GQL arg clause + native-filter api_args from body["filter"].

    schema_gen prefixes a GQL arg with "_" when the bare name collides with a
    response field (bare "id" → GQL arg "_id"); we look up both the prefixed and
    bare name, and store the bare name in api_args so it matches required_args.
    """
    arg_parts: list[str] = []
    api_args: dict = {}
    if isinstance(filter_dict, dict):
        for arg_name, arg_def in field.args.items():
            if not (arg_def.description and "Native API filter" in arg_def.description):
                continue
            bare = arg_name.lstrip("_")
            val = filter_dict.get(arg_name) if arg_name in filter_dict else filter_dict.get(bare)
            if val is not None:
                arg_parts.append(f"{arg_name}: {_gql_literal(val)}")
                api_args[bare] = val
    if limit > 0:
        arg_parts.append(f"limit: {limit}")
    arg_clause = f"({', '.join(arg_parts)})" if arg_parts else ""
    return arg_clause, api_args


def _rekey_to_proto(gql_rows, columns):
    """Re-key serialized GQL output to the exact proto field names a native gRPC
    client sees, reusing proto_gen's own authority (physical column → proto name)."""

    def _col_pair(c):
        # ColumnRef carries (field_name → GQL, column → physical); a bare str is both.
        if isinstance(c, str):
            return c, c
        return c.field_name, c.column

    gql_to_proto = {gql: _to_proto_field_name(phys) for gql, phys in map(_col_pair, columns)}

    def _proto_key(k: str) -> str:
        return gql_to_proto.get(k) or _to_proto_field_name(k)

    def _walk(obj):
        if isinstance(obj, dict):
            return {_proto_key(k): _walk(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_walk(item) for item in obj]
        return obj

    return _walk(gql_rows)


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


@router.post("/grpc/{type_name}")
async def grpc_proxy(type_name: str, request: Request):  # REQ-045, REQ-266
    """Translate an HTTP+JSON request into the gRPC query pipeline and return JSON rows."""
    from provisa.api.app import state

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

    field_name = _resolve_field_name(query_type, type_name)
    if field_name is None:
        raise HTTPException(
            status_code=404,
            detail=f"No query field for proto type {type_name!r} under role {role_id!r}",
        )

    inner = _unwrap(query_type.fields[field_name].type)
    if not hasattr(inner, "fields"):
        raise HTTPException(status_code=400, detail=f"{type_name} is not an object type")

    field_selections = _build_field_selections(inner)
    if not field_selections:
        raise HTTPException(status_code=400, detail=f"No fields found for {type_name}")

    mask_map = _parse_read_mask(body)
    arg_clause, nf_api_args = _build_arg_clause(
        query_type.fields[field_name], body.get("filter") or {}, limit
    )
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

    serialized = serialize_rows(result.rows, compiled.columns, field_name)
    gql_rows = (serialized.get("data") or {}).get(field_name) or []
    proto_rows = _rekey_to_proto(gql_rows, compiled.columns)
    proto_rows = _apply_read_mask(proto_rows, mask_map)
    return JSONResponse(proto_rows)
