# Copyright (c) 2026 Kenneth Stott
# Canary: 4cacee03-22dd-4ecc-81ac-358e79b93838
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for OpenAPI Auto-Registration Connector (Phase AQ).

Endpoints:
  POST /admin/openapi/register         — load spec + auto-register tables/functions
  POST /admin/openapi/refresh/{id}     — re-load spec and re-run registration
  POST /admin/openapi/preview          — parse spec and return discovered ops (no persist)
  GET  /admin/openapi/spec/{id}        — return stored spec JSON
  PUT  /admin/openapi/spec/{id}        — store spec JSON + run auto-register
"""
from __future__ import annotations
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/openapi", tags=["admin", "openapi"])


class OpenAPIRegisterRequest(BaseModel):
    source_id: str
    spec_path: str = ""
    spec_content: str = ""   # inline YAML or JSON; takes precedence over spec_path
    domain_id: str = ""
    base_url: str = ""
    auth_config: dict | None = None
    cache_ttl: int = 300


class OpenAPIPreviewRequest(BaseModel):
    spec_path: str = ""
    spec_content: str = ""   # inline YAML or JSON; takes precedence over spec_path


async def _load_and_register(
    source_id: str,
    spec_path: str,
    domain_id: str,
    auth_config: dict | None,
    cache_ttl: int,
    base_url: str = "",
    spec_content: str = "",
) -> tuple[dict, int, int]:
    """Load spec, upsert source record, store in state. Returns (spec, n_queries, n_mutations).

    Tables and functions are NOT auto-registered here. Users register them
    individually via the Register Table / Register Action UI.
    """
    from provisa.openapi.loader import load_spec, parse_text
    from provisa.openapi.mapper import parse_spec
    from provisa.api.admin.actions_router import _ensure_tables
    from provisa.api.app import state

    if spec_content:
        spec = parse_text(spec_content)
    else:
        spec = load_spec(spec_path)

    # Resolve base_url: explicit override > spec servers[0].url
    resolved_base_url = base_url.strip()
    if not resolved_base_url:
        servers = spec.get("servers", [])
        if servers:
            resolved_base_url = servers[0].get("url", "")

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    async with state.pg_pool.acquire() as conn:
        from provisa.core.models import Source, SourceType
        from provisa.core.repositories import source as source_repo
        await source_repo.upsert(conn, Source(
            id=source_id,
            type=SourceType.openapi,
            host="",
            port=0,
            database="",
            username="",
            path=spec_path if spec_path else ":inline:",
        ))

    queries, mutations = parse_spec(spec)

    if not hasattr(state, "openapi_specs"):
        state.openapi_specs = {}
    state.openapi_specs[source_id] = {
        "spec_path": spec_path,
        "spec_content": spec_content,
        "spec": spec,
        "base_url": resolved_base_url,
        "domain_id": domain_id,
        "auth_config": auth_config,
        "cache_ttl": cache_ttl,
    }

    return spec, len(queries), len(mutations)


@router.post("/register")
async def register_openapi_source(body: OpenAPIRegisterRequest):
    """Load an OpenAPI spec and auto-register tables and tracked functions."""
    try:
        spec, n_tables, n_mutations = await _load_and_register(
            body.source_id, body.spec_path, body.domain_id,
            body.auth_config, body.cache_ttl, base_url=body.base_url,
            spec_content=body.spec_content,
        )
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Registration failed: {exc}") from exc

    log.info(
        "Registered OpenAPI source %s (%d tables, %d mutations)",
        body.source_id, n_tables, n_mutations,
    )
    return {
        "source_id": body.source_id,
        "tables": n_tables,
        "mutations": n_mutations,
    }


@router.post("/refresh/{source_id}")
async def refresh_openapi_source(source_id: str):
    """Re-load spec from stored path and re-run auto-registration."""
    from provisa.api.app import state
    specs = getattr(state, "openapi_specs", {})
    if source_id not in specs:
        raise HTTPException(status_code=404, detail=f"OpenAPI source {source_id!r} not registered")

    reg = specs[source_id]
    try:
        spec, n_tables, n_mutations = await _load_and_register(
            source_id,
            reg.get("spec_path", ""),
            reg.get("domain_id", ""),
            reg.get("auth_config"),
            reg.get("cache_ttl", 300),
            base_url=reg.get("base_url", ""),
            spec_content=reg.get("spec_content", ""),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Refresh failed: {exc}") from exc

    log.info("Refreshed OpenAPI source %s (%d tables, %d mutations)", source_id, n_tables, n_mutations)
    return {
        "source_id": source_id,
        "tables": n_tables,
        "mutations": n_mutations,
    }


@router.post("/preview")
async def preview_openapi_spec(body: OpenAPIPreviewRequest):
    """Parse spec and return discovered queries/mutations without persisting."""
    from provisa.openapi.loader import load_spec, parse_text
    from provisa.openapi.mapper import parse_spec
    try:
        if body.spec_content:
            spec = parse_text(body.spec_content)
        else:
            spec = load_spec(body.spec_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Spec load failed: {exc}") from exc

    queries, mutations = parse_spec(spec)
    return {
        "queries": [
            {
                "operation_id": q.operation_id,
                "path": q.path,
                "method": q.method,
                "summary": q.summary,
                "path_params": q.path_params,
                "query_params": q.query_params,
            }
            for q in queries
        ],
        "mutations": [
            {
                "operation_id": m.operation_id,
                "path": m.path,
                "method": m.method,
                "summary": m.summary,
            }
            for m in mutations
        ],
    }


@router.get("/spec/{source_id}")
async def get_openapi_spec(source_id: str):
    """Return stored spec JSON for a registered OpenAPI source."""
    from provisa.api.app import state
    specs = getattr(state, "openapi_specs", {})
    if source_id not in specs:
        raise HTTPException(status_code=404, detail=f"OpenAPI source {source_id!r} not registered")
    return specs[source_id]["spec"]


@router.put("/spec/{source_id}")
async def put_openapi_spec(source_id: str, request: Request):
    """Store raw spec JSON and run auto-registration."""
    from provisa.api.app import state
    try:
        spec = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {exc}") from exc

    from provisa.openapi.register import auto_register_openapi_source
    from provisa.api.admin.actions_router import _ensure_tables

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database not connected")

    await _ensure_tables(state.pg_pool)

    specs = getattr(state, "openapi_specs", {})
    existing = specs.get(source_id, {})
    domain_id = existing.get("domain_id", "")
    base_url = existing.get("base_url", "") or ""
    if not base_url:
        servers = spec.get("servers", [])
        if servers:
            base_url = servers[0].get("url", "")
    auth_config = existing.get("auth_config")
    cache_ttl = existing.get("cache_ttl", 300)

    async with state.pg_pool.acquire() as conn:
        n_tables, n_mutations = await auto_register_openapi_source(
            source_id, spec, conn, domain_id,
            base_url=base_url, auth_config=auth_config, cache_ttl=cache_ttl,
        )

    if not hasattr(state, "openapi_specs"):
        state.openapi_specs = {}
    state.openapi_specs[source_id] = {
        **existing,
        "spec": spec,
        "spec_path": existing.get("spec_path", ""),
    }

    log.info("Stored spec for OpenAPI source %s (%d tables, %d mutations)", source_id, n_tables, n_mutations)
    return {
        "source_id": source_id,
        "tables": n_tables,
        "mutations": n_mutations,
    }
