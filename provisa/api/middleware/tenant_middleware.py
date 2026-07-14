# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Starlette middleware that resolves per-tenant context from JWT claims."""

# Requirements: REQ-456, REQ-458, REQ-462

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from provisa.core.tenant_context import TenantContext, TenantContextCache

_SKIP_PATHS = {
    "/billing/signup",
    "/billing/webhook",
    "/health",
    "/data/openapi/docs",
    "/data/openapi/redoc",
    "/data/openapi/openapi.json",
}

_cache = TenantContextCache()


class TenantMiddleware(BaseHTTPMiddleware):  # REQ-456, REQ-462
    async def dispatch(self, request: Request, call_next):
        if request.url.path in _SKIP_PATHS:
            return await call_next(request)

        identity = getattr(request.state, "identity", None)
        if identity is None:
            return JSONResponse(status_code=401, content={"detail": "Unauthenticated"})

        tenant_id: str | None = identity.raw_claims.get("tenant_id")
        if not tenant_id:
            return JSONResponse(status_code=401, content={"detail": "tenant_id claim missing"})

        ctx = _cache.get(tenant_id)
        if ctx is None:
            ctx = await _build_tenant_context(request, tenant_id)
            if ctx is None:
                return JSONResponse(status_code=401, content={"detail": "Tenant not found"})
            _cache.set(tenant_id, ctx)

        request.state.tenant_id = tenant_id
        request.state.tenant_context = ctx
        # REQ-828: bind the tenant to the app-layer meta-RLS guard for this request, so every
        # control-plane read/write is confined to it store-independently. Reset on the way out.
        from provisa.core.meta_rls import reset_meta_tenant, set_meta_tenant

        _meta_token = set_meta_tenant(tenant_id)
        try:
            return await call_next(request)
        finally:
            reset_meta_tenant(_meta_token)


async def _build_tenant_context(
    request: Request, tenant_id: str
) -> TenantContext | None:  # REQ-456, REQ-458
    from provisa.api.billing.tenant_db import fetch_config_entities
    from provisa.api.billing.kms import decrypt_data_key, aes_decrypt
    from provisa.core.config_loader import parse_config_dict
    import json

    pool = request.app.state.tenant_db
    entity_rows = await fetch_config_entities(pool, tenant_id, "config")
    if not entity_rows:
        return None

    decrypted: dict = {}
    for row in entity_rows:
        dek = await decrypt_data_key(row["entity_id"], row["encrypted_dek"])
        plaintext = aes_decrypt(row["iv"], row["ciphertext"], dek)
        decrypted[row["entity_id"]] = json.loads(plaintext)

    config = parse_config_dict(decrypted)

    return TenantContext(
        tenant_id=tenant_id,
        config=config,
        compilation_contexts={},
        rls_contexts={},
        schemas={},
    )
