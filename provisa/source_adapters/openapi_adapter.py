# Copyright (c) 2026 Kenneth Stott
# Canary: af919853-693c-4b27-893e-b146bcc8d07f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source adapter for OpenAPI sources — cache-aside HTTP execution."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import cast

import httpx

from provisa.cache.store import CacheStore
from provisa.openapi.mapper import OpenAPIMutation, OpenAPIQuery

# Requirements: REQ-314, REQ-316, REQ-317, REQ-318, REQ-319, REQ-320

log = logging.getLogger(__name__)


def _build_auth_headers(auth_config: dict[str, str] | None) -> dict[str, str]:  # REQ-320
    if not auth_config:
        return {}
    auth_type = auth_config.get("type", "none")
    if auth_type == "bearer":
        return {"Authorization": f"Bearer {auth_config.get('token', '')}"}
    if auth_type == "basic":
        import base64

        creds = base64.b64encode(
            f"{auth_config.get('username', '')}:{auth_config.get('password', '')}".encode()
        ).decode()
        return {"Authorization": f"Basic {creds}"}
    if auth_type == "api_key":
        header_name = auth_config.get("header_name", "X-API-Key")
        return {header_name: auth_config.get("api_key", "")}
    return {}


async def fetch(  # REQ-316, REQ-318, REQ-319
    base_url: str,
    query: OpenAPIQuery,
    args: dict[str, object],
    auth_config: dict[str, str] | None,
    response_cache_store: CacheStore,
    source_id: str,
    role: str = "",
    ttl: int = 300,
) -> list[dict]:
    """Execute a GET operation with cache-aside."""
    args_hash = hashlib.sha256(json.dumps(sorted(args.items())).encode()).hexdigest()[:12]
    cache_key = f"openapi:{source_id}:{query.operation_id}:{args_hash}:{role}"

    cached = await response_cache_store.get(cache_key)
    if cached is not None:
        log.debug("Cache hit for %s", cache_key)
        return json.loads(cached.data)

    url = base_url.rstrip("/") + query.path
    path_params = {p["name"] for p in query.path_params}

    # Args may have leading '_' on keys when name collides with a visible column (schema_gen renames)
    def _get_arg(name: str) -> object:
        return args.get(name, args.get(f"_{name}"))

    query_params: dict[str, object] = {
        k.lstrip("_") if k.lstrip("_") in path_params else k: v
        for k, v in args.items()
        if k.lstrip("_") not in path_params
    }
    for p_name in path_params:
        val = _get_arg(p_name)
        if val is not None:
            url = url.replace(f"{{{p_name}}}", str(val))

    headers = _build_auth_headers(auth_config)
    async with httpx.AsyncClient(timeout=30.0) as client:
        # httpx accepts primitive query values at runtime; args carry GraphQL
        # primitives whose static type is opaque (object).
        resp = await client.get(
            url,
            params=cast("httpx.QueryParams", query_params),
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()

    rows = data if isinstance(data, list) else [data]
    await response_cache_store.set(cache_key, json.dumps(rows).encode(), ttl=ttl)
    return rows


async def execute(  # REQ-317
    base_url: str,
    mutation: OpenAPIMutation,
    input_data: dict[str, object],
    auth_config: dict[str, str] | None,
) -> dict:
    """Execute a non-GET operation (not cached)."""
    url = base_url.rstrip("/") + mutation.path
    headers = {"Content-Type": "application/json", **_build_auth_headers(auth_config)}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.request(
            mutation.method.upper(),
            url,
            json=input_data,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()
