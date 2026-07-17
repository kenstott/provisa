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

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from provisa.grpc.query_ir import grpc_table_to_semantic_sql
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

    ctx = state.contexts[role_id]
    mask_map = _parse_read_mask(body)

    # Same IR path as the native gRPC servicer (query language → IR → governed IR → plan → physical).
    # Lower the request straight to a semantic SELECT — never round-trip through GraphQL.
    semantic_sql = grpc_table_to_semantic_sql(ctx, type_name, limit)
    if semantic_sql is None:
        raise HTTPException(
            status_code=404,
            detail=f"No query field for proto type {type_name!r} under role {role_id!r}",
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
