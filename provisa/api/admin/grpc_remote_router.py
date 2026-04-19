# Copyright (c) 2026 Kenneth Stott
# Canary: bab62a3e-25cb-4c13-b423-a21a13bf2c52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for gRPC Remote Schema Connector (Phase AR).

Endpoints:
  POST /admin/grpc-remote/register         — compile stubs + auto-register tables/functions
  POST /admin/grpc-remote/refresh/{id}     — re-compile + re-register
  GET  /admin/grpc-remote/list             — list registered gRPC sources
  GET  /admin/grpc-remote/{id}/proto       — return stored proto text
  PUT  /admin/grpc-remote/{id}/proto       — store new proto text + re-register
"""
from __future__ import annotations

import logging

import grpc.aio

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from provisa.compiler.naming import source_to_catalog

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/grpc-remote", tags=["admin", "grpc-remote"])


class GrpcRemoteRegisterRequest(BaseModel):
    source_id: str
    proto_path: str                  # local path or http/https URL to .proto file
    server_address: str              # host:port
    namespace: str = ""
    domain_id: str = ""
    import_paths: list[str] = []
    tls: bool = False
    auth_config: dict | None = None
    cache_ttl: int = 300


async def _load_and_register(
    source_id: str,
    proto_path: str,
    server_address: str,
    namespace: str,
    domain_id: str,
    import_paths: list[str],
    tls: bool,
    auth_config: dict | None,
    cache_ttl: int,
    state,
) -> tuple[str, int, int]:
    """Load proto, compile stubs, open channel, register tables/functions.

    Returns (proto_text, n_tables, n_mutations).
    """
    from provisa.grpc_remote.loader import load_proto, compile_proto_stubs
    from provisa.grpc_remote.mapper import map_proto
    from provisa.grpc_remote.executor import load_stubs
    from provisa.api.admin.actions_router import _ensure_tables

    proto_dict = await load_proto(proto_path, import_paths=import_paths or None)

    # Re-read raw text for storage
    if proto_path.startswith("http://") or proto_path.startswith("https://"):
        import httpx
        r = httpx.get(proto_path, timeout=30, follow_redirects=True)
        r.raise_for_status()
        proto_text = r.text
    else:
        from pathlib import Path
        proto_text = Path(proto_path).read_text()

    pb2_path, pb2_grpc_path = compile_proto_stubs(
        proto_text,
        proto_name=source_to_catalog(source_id),
        import_paths=import_paths or None,
    )
    pb2, _ = load_stubs(pb2_path, pb2_grpc_path)

    queries, mutations = map_proto(proto_dict, namespace, source_id, domain_id)

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        n_tables, n_mutations = await _register_schema(
            source_id, queries, mutations, conn, namespace, domain_id
        )

    # Open gRPC channel
    if tls:
        credentials = grpc.ssl_channel_credentials()
        channel = grpc.aio.secure_channel(server_address, credentials)
    else:
        channel = grpc.aio.insecure_channel(server_address)

    if not hasattr(state, "grpc_remote_sources"):
        state.grpc_remote_sources = {}

    # Close previous channel if exists
    existing = state.grpc_remote_sources.get(source_id, {})
    old_channel = existing.get("channel")
    if old_channel is not None:
        try:
            await old_channel.close()
        except Exception:
            pass

    state.grpc_remote_sources[source_id] = {
        "proto_path": proto_path,
        "proto_text": proto_text,
        "server_address": server_address,
        "namespace": namespace,
        "domain_id": domain_id,
        "import_paths": import_paths,
        "tls": tls,
        "auth_config": auth_config,
        "cache_ttl": cache_ttl,
        "pb2_path": pb2_path,
        "pb2_grpc_path": pb2_grpc_path,
        "pb2": pb2,
        "channel": channel,
        "queries": queries,
        "mutations": mutations,
    }

    return proto_text, n_tables, n_mutations


async def _register_schema(
    source_id: str,
    queries,
    mutations,
    conn,
    namespace: str,
    domain_id: str,
) -> tuple[int, int]:
    """Upsert virtual tables and tracked functions for discovered gRPC methods."""
    from provisa.grpc_remote.mapper import GrpcQuery, GrpcMutation

    prefix = f"{namespace}__" if namespace else ""

    # Virtual tables from query methods
    for q in queries:
        table_name = f"{prefix}{q.service}__{q.method}"
        col_defs = ", ".join(f"{c.name} {c.type}" for c in q.columns) or "result jsonb"
        await conn.execute(
            """
            INSERT INTO provisa_sources (source_id, source_type, table_name, column_defs,
                                         namespace, domain_id, extra)
            VALUES ($1, 'grpc_remote', $2, $3, $4, $5, $6)
            ON CONFLICT (source_id, table_name) DO UPDATE
              SET column_defs = EXCLUDED.column_defs,
                  namespace   = EXCLUDED.namespace,
                  domain_id   = EXCLUDED.domain_id,
                  extra       = EXCLUDED.extra
            """,
            source_id,
            table_name,
            col_defs,
            namespace,
            domain_id,
            f"grpc_query:{q.full_method_path}",
        )

    # Tracked functions from mutation methods
    for m in mutations:
        fn_name = f"{prefix}{m.service}__{m.method}"
        arg_defs = ", ".join(f"{c.name} {c.type}" for c in m.input_fields)
        return_cols = ", ".join(f"{c.name} {c.type}" for c in m.return_columns) or "result jsonb"
        await conn.execute(
            """
            INSERT INTO provisa_functions (source_id, fn_name, arg_defs, return_schema,
                                           namespace, domain_id, extra)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (source_id, fn_name) DO UPDATE
              SET arg_defs      = EXCLUDED.arg_defs,
                  return_schema = EXCLUDED.return_schema,
                  namespace     = EXCLUDED.namespace,
                  domain_id     = EXCLUDED.domain_id,
                  extra         = EXCLUDED.extra
            """,
            source_id,
            fn_name,
            arg_defs,
            return_cols,
            namespace,
            domain_id,
            f"grpc_mutation:{m.full_method_path}",
        )

    return len(queries), len(mutations)


@router.post("/register")
async def register_grpc_remote_source(body: GrpcRemoteRegisterRequest, request: Request):
    """Compile proto stubs and auto-register virtual tables + tracked functions."""
    state = request.app.state
    try:
        proto_text, n_tables, n_mutations = await _load_and_register(
            body.source_id, body.proto_path, body.server_address,
            body.namespace, body.domain_id, body.import_paths,
            body.tls, body.auth_config, body.cache_ttl, state,
        )
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Registration failed: {exc}") from exc

    log.info(
        "Registered gRPC remote source %s (%d tables, %d mutations)",
        body.source_id, n_tables, n_mutations,
    )
    return {"source_id": body.source_id, "tables": n_tables, "mutations": n_mutations}


@router.post("/refresh/{source_id}")
async def refresh_grpc_remote_source(source_id: str, request: Request):
    """Re-compile proto stubs from stored path and re-run registration."""
    state = request.app.state
    sources = getattr(state, "grpc_remote_sources", {})
    if source_id not in sources:
        raise HTTPException(status_code=404, detail=f"gRPC source {source_id!r} not registered")

    reg = sources[source_id]
    try:
        _, n_tables, n_mutations = await _load_and_register(
            source_id,
            reg["proto_path"],
            reg["server_address"],
            reg.get("namespace", ""),
            reg.get("domain_id", ""),
            reg.get("import_paths", []),
            reg.get("tls", False),
            reg.get("auth_config"),
            reg.get("cache_ttl", 300),
            state,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Refresh failed: {exc}") from exc

    log.info("Refreshed gRPC remote source %s (%d tables, %d mutations)", source_id, n_tables, n_mutations)
    return {"source_id": source_id, "tables": n_tables, "mutations": n_mutations}


@router.get("/list")
async def list_grpc_remote_sources(request: Request):
    """Return all registered gRPC remote sources (without channel/pb2 objects)."""
    sources = getattr(request.app.state, "grpc_remote_sources", {})
    result = []
    for sid, reg in sources.items():
        result.append({
            "source_id": sid,
            "server_address": reg.get("server_address"),
            "proto_path": reg.get("proto_path"),
            "namespace": reg.get("namespace", ""),
            "domain_id": reg.get("domain_id", ""),
            "tls": reg.get("tls", False),
            "tables": len(reg.get("queries", [])),
            "mutations": len(reg.get("mutations", [])),
        })
    return result


@router.get("/{source_id}/proto")
async def get_grpc_proto(source_id: str, request: Request):
    """Return stored proto text for a registered gRPC source."""
    sources = getattr(request.app.state, "grpc_remote_sources", {})
    if source_id not in sources:
        raise HTTPException(status_code=404, detail=f"gRPC source {source_id!r} not registered")
    return {"source_id": source_id, "proto_text": sources[source_id].get("proto_text", "")}


@router.put("/{source_id}/proto")
async def put_grpc_proto(source_id: str, request: Request):
    """Store new proto text and re-run registration."""
    state = request.app.state
    sources = getattr(state, "grpc_remote_sources", {})
    if source_id not in sources:
        raise HTTPException(status_code=404, detail=f"gRPC source {source_id!r} not registered")

    try:
        body = await request.json()
        proto_text = body["proto_text"]
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid request: {exc}") from exc

    reg = sources[source_id]
    from provisa.grpc_remote.loader import compile_proto_stubs, parse_proto_text
    from provisa.grpc_remote.mapper import map_proto
    from provisa.grpc_remote.executor import load_stubs
    from provisa.api.admin.actions_router import _ensure_tables

    try:
        pb2_path, pb2_grpc_path = compile_proto_stubs(
            proto_text,
            proto_name=source_to_catalog(source_id),
            import_paths=reg.get("import_paths") or None,
        )
        pb2, _ = load_stubs(pb2_path, pb2_grpc_path)
        proto_dict = parse_proto_text(proto_text)
        queries, mutations = map_proto(
            proto_dict,
            reg.get("namespace", ""),
            source_id,
            reg.get("domain_id", ""),
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Proto compilation failed: {exc}") from exc

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        n_tables, n_mutations = await _register_schema(
            source_id, queries, mutations, conn,
            reg.get("namespace", ""), reg.get("domain_id", ""),
        )

    sources[source_id].update({
        "proto_text": proto_text,
        "pb2_path": pb2_path,
        "pb2_grpc_path": pb2_grpc_path,
        "pb2": pb2,
        "queries": queries,
        "mutations": mutations,
    })

    log.info("Updated proto for gRPC source %s (%d tables, %d mutations)", source_id, n_tables, n_mutations)
    return {"source_id": source_id, "tables": n_tables, "mutations": n_mutations}
