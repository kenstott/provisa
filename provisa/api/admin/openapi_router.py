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
  GET  /admin/openapi/list             — return registration metadata for all sources
  GET  /admin/openapi/spec/{id}        — return stored spec JSON
  PUT  /admin/openapi/spec/{id}        — store spec JSON + run auto-register
"""

# Requirements: REQ-314, REQ-315, REQ-316, REQ-317, REQ-320, REQ-321, REQ-406, REQ-407, REQ-408

from __future__ import annotations
import logging
from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, model_validator

from provisa.api.errors import ApiError
from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.schema_common import remote_source_counts

if TYPE_CHECKING:
    from provisa.core.database import Connection

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/openapi", tags=["admin", "openapi"])


class OpenAPIRegisterRequest(BaseModel):
    source_id: str
    spec_path: str = ""
    spec_content: str = ""  # inline YAML or JSON; takes precedence over spec_path
    domain_id: str = ""
    base_url: str = ""
    auth_config: dict | None = None
    cache_ttl: int = 300
    operation_overrides: dict[str, str] = {}  # {operationId: "query" | "mutation"}
    relationships: list[dict] = []

    @model_validator(mode="after")
    def _set_inline_sentinel(self) -> "OpenAPIRegisterRequest":  # REQ-407
        if self.spec_content and not self.spec_path:
            self.spec_path = ":inline:"
        return self


class OpenAPIPreviewRequest(BaseModel):
    spec_path: str = ""
    spec_content: str = ""  # inline YAML or JSON; takes precedence over spec_path


async def _load_and_register(  # REQ-314, REQ-315, REQ-316, REQ-317, REQ-320, REQ-407
    source_id: str,
    spec_path: str,
    domain_id: str,
    auth_config: dict | None,
    cache_ttl: int,
    base_url: str = "",
    spec_content: str = "",
    operation_overrides: dict[str, str] | None = None,
    relationships: list[dict] | None = None,
    store_auth: bool = False,
) -> tuple[dict, int, int]:
    """Load spec, upsert source record, store in state. Returns (spec, n_queries, n_mutations).

    ``store_auth``: this call is the registration that supplies the source's auth, which is
    stored for the caller; a refresh re-reads the spec and leaves the stored auth as it is.

    Tables and functions are NOT auto-registered here. Users register them
    individually via the Register Table / Register Action UI.
    """
    from provisa.openapi.loader import load_spec, parse_text
    from provisa.openapi.mapper import parse_spec
    from provisa.api.app import state

    if spec_content:
        spec = parse_text(spec_content)
    else:
        from provisa.core.secrets import resolve_secrets as _resolve_secrets

        spec = load_spec(_resolve_secrets(spec_path))

    # Resolve base_url: explicit override > spec servers[0].url
    resolved_base_url = base_url.strip()
    if not resolved_base_url:
        servers = spec.get("servers", [])
        if servers:
            resolved_base_url = servers[0].get("url", "")

    if state.tenant_db is None:
        raise ApiError(503, "openapi.database_not_connected", "Database not connected")

    pool = state.tenant_db
    assert pool is not None
    async with pool.acquire() as _conn:
        from provisa.core.models import Source, SourceType
        from provisa.core.repositories import source as source_repo

        _existing = await source_repo.get(cast("Connection", _conn), source_id)
        _spec_source = Source(
            id=source_id,
            type=SourceType.openapi,
            host="",
            port=0,
            database="",
            username="",
            path=spec_path if spec_path else ":inline:",
        )
        if _existing is not None:
            # REQ-1909: re-importing a spec replaces the spec, not the operator's Load Management
            # and Timeliness settings the upsert now persists (cache, live cap, gates).
            _spec_source = source_repo.source_from_row(_existing).model_copy(
                update={"type": SourceType.openapi, "path": _spec_source.path}
            )
        await source_repo.upsert(cast("Connection", _conn), _spec_source, origin="admin")
        # REQ-316/REQ-318: what the source's tables are called through (api_source.caller):
        # its base URL, and its auth when this call is the registration that supplies it.
        from provisa.api_source.openapi_endpoint import (
            api_auth,
            register_openapi_source,
            store_openapi_auth,
        )

        await register_openapi_source(cast("Connection", _conn), source_id, resolved_base_url)
        if store_auth:
            await store_openapi_auth(cast("Connection", _conn), source_id, api_auth(auth_config))

    queries, mutations = parse_spec(spec, operation_overrides=operation_overrides)

    if relationships:
        from provisa.api.admin.graphql_remote_router import _upsert_relationships_to_semantic_layer

        await _upsert_relationships_to_semantic_layer(relationships, state.tenant_db, state)

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
        "operation_overrides": operation_overrides or {},
        "relationships": relationships or [],
    }

    # REQ-1729: the upsert above wrote straight to the sources table — state.source_types and
    # state.source_catalogs (needed by available_schemas/catalog_for) only backfill from that
    # table inside _rebuild_schemas, mirroring graphql_remote_router's own registration.
    try:
        from provisa.api.app import _rebuild_schemas

        await _rebuild_schemas()
    except Exception:
        log.warning("Schema rebuild failed after openapi registration", exc_info=True)

    # REQ-1729: createSource's mutation path also provisions the source ON THE ENGINE (the
    # ATTACH/catalog-create a later query needs); this REST endpoint never did.
    from types import SimpleNamespace

    from provisa.api.admin.schema_common import SourceInput, _register_source_on_engine

    _register_source_on_engine(
        state,
        Source(id=source_id, type=SourceType.openapi, path=spec_path),
        cast(SourceInput, SimpleNamespace(id=source_id)),
    )

    return spec, len(queries), len(mutations)


@router.post("/register")
async def register_openapi_source(
    request: Request,
    body: OpenAPIRegisterRequest,
):  # REQ-314, REQ-315, REQ-316, REQ-317, REQ-320, REQ-406, REQ-407, REQ-408
    """Load an OpenAPI spec and add the source. Its GET operations are tables on offer and its
    other operations commands on offer; none is registered here (REQ-316)."""
    require_capability_request(request, "source_registration")
    try:
        _, n_offered, n_commands = await _load_and_register(
            body.source_id,
            body.spec_path,
            body.domain_id,
            body.auth_config,
            body.cache_ttl,
            base_url=body.base_url,
            spec_content=body.spec_content,
            operation_overrides=body.operation_overrides or None,
            relationships=body.relationships or None,
            store_auth=True,
        )
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise ApiError(
            422, "openapi.registration_failed", f"Registration failed: {exc}", error=str(exc)
        ) from exc

    log.info(
        "Added OpenAPI source %s (%d tables and %d commands on offer)",
        body.source_id,
        n_offered,
        n_commands,
    )
    return {"source_id": body.source_id, **remote_source_counts(0, n_offered, n_commands)}


@router.post("/refresh/{source_id}")
async def refresh_openapi_source(request: Request, source_id: str):  # REQ-321
    """Re-load the spec from its stored path. Nothing is registered (REQ-316)."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state

    specs = getattr(state, "openapi_specs", {})
    if source_id not in specs:
        raise ApiError(
            404,
            "openapi.source_not_registered",
            f"OpenAPI source {source_id!r} not registered",
            source_id=source_id,
        )

    reg = specs[source_id]
    try:
        _, n_offered, n_commands = await _load_and_register(
            source_id,
            reg.get("spec_path", ""),
            reg.get("domain_id", ""),
            reg.get("auth_config"),
            reg.get("cache_ttl", 300),
            base_url=reg.get("base_url", ""),
            spec_content=reg.get("spec_content", ""),
            operation_overrides=reg.get("operation_overrides") or None,
            relationships=reg.get("relationships") or None,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise ApiError(
            422, "openapi.refresh_failed", f"Refresh failed: {exc}", error=str(exc)
        ) from exc

    log.info(
        "Refreshed OpenAPI source %s (%d tables and %d commands on offer)",
        source_id,
        n_offered,
        n_commands,
    )
    return {"source_id": source_id, **remote_source_counts(0, n_offered, n_commands)}


@router.post("/preview")
async def preview_openapi_spec(request: Request, body: OpenAPIPreviewRequest):  # REQ-315, REQ-407
    """Parse spec and return discovered queries/mutations without persisting."""
    require_capability_request(request, "source_registration")
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
        raise ApiError(
            422, "openapi.spec_load_failed", f"Spec load failed: {exc}", error=str(exc)
        ) from exc

    queries, mutations = parse_spec(spec)
    info = spec.get("info", {})
    spec_description = info.get("description") or info.get("title", "")
    return {
        "spec_description": spec_description,
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


@router.get("/list")
async def list_openapi_sources(request: Request):
    """Return registration metadata for all OpenAPI sources (without the parsed spec)."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state

    specs = getattr(state, "openapi_specs", {})
    result = []
    for sid, reg in specs.items():
        result.append(
            {
                "source_id": sid,
                "spec_path": reg.get("spec_path", ""),
                "has_inline_spec": bool(reg.get("spec_content")),
                "base_url": reg.get("base_url", ""),
                "domain_id": reg.get("domain_id", ""),
                "cache_ttl": reg.get("cache_ttl", 300),
                "auth_config": reg.get("auth_config"),
            }
        )
    return result


@router.get("/spec/{source_id}")
async def get_openapi_spec(request: Request, source_id: str):
    """Return stored spec JSON for a registered OpenAPI source."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state

    specs = getattr(state, "openapi_specs", {})
    if source_id not in specs:
        raise ApiError(
            404,
            "openapi.source_not_registered",
            f"OpenAPI source {source_id!r} not registered",
            source_id=source_id,
        )
    return specs[source_id]["spec"]


@router.put("/spec/{source_id}")
async def put_openapi_spec(source_id: str, request: Request):  # REQ-315, REQ-316
    """Store a spec written or edited by hand. It is treated as a fetched one is (REQ-315): its
    GET operations are tables on offer and its other operations are commands on offer, and
    nothing is registered by storing it -- the steward registers what is wanted (REQ-316)."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state
    from provisa.openapi.mapper import parse_spec

    try:
        spec = await request.json()
        queries, mutations = parse_spec(spec)
    except Exception as exc:
        raise ApiError(422, "openapi.invalid_json", f"Invalid JSON: {exc}", error=str(exc)) from exc

    specs = getattr(state, "openapi_specs", {})
    existing = specs.get(source_id, {})
    base_url = existing.get("base_url", "") or ""
    if not base_url:
        servers = spec.get("servers", [])
        if servers:
            base_url = servers[0].get("url", "")

    if not hasattr(state, "openapi_specs"):
        state.openapi_specs = {}
    state.openapi_specs[source_id] = {
        **existing,
        "spec": spec,
        "spec_path": existing.get("spec_path", ""),
        "base_url": base_url,
    }

    log.info(
        "Stored spec for OpenAPI source %s (%d tables and %d commands on offer)",
        source_id,
        len(queries),
        len(mutations),
    )
    return {"source_id": source_id, **remote_source_counts(0, len(queries), len(mutations))}
