# Copyright (c) 2026 Kenneth Stott
# Canary: 9785aa13-375b-4f60-b321-78bc63c59dc5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""HTTP/gRPC client for API data sources (Phase U)."""

from __future__ import annotations

import asyncio
import re

import httpx

from provisa.api_source.models import (
    ApiEndpoint,
    ApiSourceType,
    PaginationType,
)


class ApiCallError(Exception):
    """Raised when an API call fails after retries."""


_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 1.0
_DEFAULT_TIMEOUT = 30.0


def _build_request_parts(
    endpoint: ApiEndpoint,
    resolved_params: dict,
) -> tuple[str, dict, dict, dict | None]:
    """Build URL, query params, headers, and body from endpoint config and resolved params.

    Returns (url, query_params, headers, body).
    """
    url = endpoint.path
    query_params: dict = {}
    headers: dict = {}
    body_parts: dict = {}

    for col in endpoint.columns:
        if col.param_type is None or col.param_name is None:
            continue
        param_key = col.param_name
        value = resolved_params.get(col.param_name) or resolved_params.get(col.name)
        if value is None:
            continue

        if col.param_type.value == "query":
            query_params[param_key] = value
        elif col.param_type.value == "path":
            url = url.replace(f"{{{param_key}}}", str(value))
        elif col.param_type.value == "body":
            body_parts[param_key] = value
        elif col.param_type.value == "header":
            headers[param_key] = str(value)
        elif col.param_type.value == "variable":
            # GraphQL variables go in body
            body_parts[param_key] = value

    body = body_parts if body_parts else None
    return url, query_params, headers, body


async def _request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    json_body: dict | None = None,
    form_body: dict | None = None,
    timeout: float = _DEFAULT_TIMEOUT,
) -> httpx.Response:
    """Make an HTTP request with retry on 429/5xx."""
    for attempt in range(_MAX_RETRIES):
        resp = await client.request(
            method, url, params=params, headers=headers,
            json=json_body, data=form_body, timeout=timeout,
        )
        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt < _MAX_RETRIES - 1:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                await asyncio.sleep(wait)
                continue
            raise ApiCallError(
                f"API call failed after {_MAX_RETRIES} retries: "
                f"{resp.status_code} {resp.text[:200]}"
            )
        resp.raise_for_status()
        return resp
    raise ApiCallError("Unreachable: retry loop exhausted")


async def _paginate(
    client: httpx.AsyncClient,
    endpoint: ApiEndpoint,
    url: str,
    params: dict,
    headers: dict,
    body: dict | None,
    timeout: float,
    form_body: dict | None = None,
) -> list[dict]:
    """Follow pagination, collecting all pages."""
    pagination = endpoint.pagination
    if pagination is None:
        resp = await _request_with_retry(
            client, endpoint.method, url, params, headers,
            json_body=body, form_body=form_body, timeout=timeout,
        )
        return [resp.json()]

    pages: list[dict] = []
    max_pages = pagination.max_pages

    if pagination.type == PaginationType.link_header:
        next_url: str | None = url
        for _ in range(max_pages):
            if next_url is None:
                break
            resp = await _request_with_retry(
                client, endpoint.method, next_url, params, headers,
                json_body=body, form_body=form_body, timeout=timeout,
            )
            pages.append(resp.json())
            link = resp.headers.get("link", "")
            match = re.search(r'<([^>]+)>;\s*rel="next"', link)
            next_url = match.group(1) if match else None
            params = None  # subsequent pages use full URL from link

    elif pagination.type == PaginationType.cursor:
        cursor_param = pagination.cursor_param or "cursor"
        cursor_field = pagination.cursor_field or "next_cursor"
        for _ in range(max_pages):
            resp = await _request_with_retry(
                client, endpoint.method, url, params, headers,
                json_body=body, form_body=form_body, timeout=timeout,
            )
            data = resp.json()
            pages.append(data)
            cursor = data.get(cursor_field) if isinstance(data, dict) else None
            if not cursor:
                break
            params = dict(params or {})
            params[cursor_param] = cursor

    elif pagination.type == PaginationType.offset:
        page_size = pagination.page_size
        page_size_param = pagination.page_size_param or "limit"
        offset_param = pagination.page_param or "offset"
        offset = 0
        for _ in range(max_pages):
            p = dict(params or {})
            p[page_size_param] = page_size
            p[offset_param] = offset
            resp = await _request_with_retry(
                client, endpoint.method, url, p, headers,
                json_body=body, form_body=form_body, timeout=timeout,
            )
            data = resp.json()
            pages.append(data)
            # Heuristic: if response is a list shorter than page_size, we're done
            if isinstance(data, list) and len(data) < page_size:
                break
            offset += page_size

    elif pagination.type == PaginationType.page_number:
        page_param = pagination.page_param or "page"
        page_size_param = pagination.page_size_param or "per_page"
        page_size = pagination.page_size
        for page_num in range(1, max_pages + 1):
            p = dict(params or {})
            p[page_param] = page_num
            p[page_size_param] = page_size
            resp = await _request_with_retry(
                client, endpoint.method, url, p, headers,
                json_body=body, form_body=form_body, timeout=timeout,
            )
            data = resp.json()
            pages.append(data)
            if isinstance(data, list) and len(data) < page_size:
                break

    return pages


def _apply_auth(auth, headers: dict, query_params: dict) -> None:
    """Apply typed auth config to request headers/params, resolving secrets."""
    if auth is None:
        return

    from provisa.core.auth_models import (
        ApiAuthBearer, ApiAuthBasic, ApiAuthApiKey,
        ApiAuthOAuth2ClientCredentials, ApiAuthCustomHeaders,
        ApiKeyLocation,
    )
    from provisa.core.secrets import resolve_secrets

    # Legacy dict support for backward compatibility
    if isinstance(auth, dict):
        if "bearer" in auth:
            headers["Authorization"] = f"Bearer {resolve_secrets(auth['bearer'])}"
        if "api_key_header" in auth and "api_key" in auth:
            headers[auth["api_key_header"]] = resolve_secrets(auth["api_key"])
        if "headers" in auth:
            for k, v in auth["headers"].items():
                headers[k] = resolve_secrets(v)
        return

    match auth:
        case ApiAuthBearer(token=token):
            headers["Authorization"] = f"Bearer {resolve_secrets(token)}"
        case ApiAuthBasic(username=u, password=p):
            import base64
            cred = base64.b64encode(
                f"{resolve_secrets(u)}:{resolve_secrets(p)}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {cred}"
        case ApiAuthApiKey(key=key, name=name, location=loc):
            resolved_key = resolve_secrets(key)
            if loc == ApiKeyLocation.header:
                headers[name] = resolved_key
            else:
                query_params[name] = resolved_key
        case ApiAuthOAuth2ClientCredentials() as oauth:
            token = _fetch_oauth2_token(oauth)
            headers["Authorization"] = f"Bearer {token}"
        case ApiAuthCustomHeaders(headers=h):
            for k, v in h.items():
                headers[k] = resolve_secrets(v)


# Simple token cache for OAuth2 client credentials
_oauth2_cache: dict[str, tuple[str, float]] = {}


def _fetch_oauth2_token(oauth) -> str:
    """Fetch and cache an OAuth2 client credentials token."""
    import time
    import httpx as _httpx
    from provisa.core.secrets import resolve_secrets

    cache_key = f"{oauth.client_id}:{oauth.token_url}"
    cached = _oauth2_cache.get(cache_key)
    if cached:
        token, expires_at = cached
        if time.time() < expires_at - 30:  # 30s buffer
            return token

    data = {
        "grant_type": "client_credentials",
        "client_id": resolve_secrets(oauth.client_id),
        "client_secret": resolve_secrets(oauth.client_secret),
    }
    if oauth.scope:
        data["scope"] = oauth.scope

    resp = _httpx.post(oauth.token_url, data=data, timeout=10)
    resp.raise_for_status()
    body = resp.json()
    token = body["access_token"]
    expires_in = body.get("expires_in", 3600)
    _oauth2_cache[cache_key] = (token, time.time() + expires_in)
    return token


async def call_api(
    endpoint: ApiEndpoint,
    resolved_params: dict,
    base_url: str = "",
    auth=None,
    timeout: float = _DEFAULT_TIMEOUT,
) -> list[dict]:
    """Make the API call and return raw response data (list of page responses)."""
    url, query_params, headers, body = _build_request_parts(endpoint, resolved_params)

    # Prepend base_url if path is relative
    if not url.startswith("http"):
        url = base_url.rstrip("/") + "/" + url.lstrip("/")

    _apply_auth(auth, headers, query_params)

    # GraphQL: wrap query in proper body
    json_body: dict | None = None
    form_body: dict | None = None
    if endpoint.method == "QUERY":
        body = body or {}
        json_body = {"query": endpoint.path, "variables": body}
        method = "POST"
    elif endpoint.method == "RPC":
        return await _call_grpc(endpoint, resolved_params, base_url)
    elif endpoint.body_encoding == "json":
        # Neo4j HTTP API: POST with JSON body containing the Cypher statement
        json_body = {"statement": endpoint.query_template} if endpoint.query_template else body
        method = endpoint.method
    elif endpoint.body_encoding == "form":
        # SPARQL 1.1: POST with form-encoded query parameter
        form_body = {"query": endpoint.query_template} if endpoint.query_template else {}
        if body:
            form_body.update(body)
        method = endpoint.method
    else:
        json_body = body
        method = endpoint.method

    async with httpx.AsyncClient() as client:
        pages = await _paginate(
            client, endpoint, url, query_params, headers,
            body=json_body, timeout=timeout, form_body=form_body,
        )

    return pages


async def _call_grpc(
    endpoint: ApiEndpoint,
    resolved_params: dict,
    host_port: str,
) -> list[dict]:
    """Call a gRPC endpoint. Returns response as list of dicts."""
    import grpc
    from google.protobuf import json_format, descriptor_pool, descriptor_pb2

    channel = grpc.insecure_channel(host_port)
    # gRPC dynamic invocation requires proto descriptors at runtime.
    # This is a simplified implementation; production would use grpc_reflection.
    path_parts = endpoint.path.strip("/").split("/")
    if len(path_parts) < 2:
        raise ApiCallError(f"Invalid gRPC path: {endpoint.path}")

    service_name = path_parts[0]
    method_name = path_parts[1]

    # Use reflection to get method descriptor and make call
    from grpc_reflection.v1alpha import reflection_pb2 as refl_pb2, reflection_pb2_grpc
    stub = reflection_pb2_grpc.ServerReflectionStub(channel)

    req = refl_pb2.ServerReflectionRequest(file_containing_symbol=service_name)
    responses = stub.ServerReflectionInfo(iter([req]))

    for resp in responses:
        for proto_bytes in resp.file_descriptor_response.file_descriptor_proto:
            fd = descriptor_pb2.FileDescriptorProto()
            fd.ParseFromString(proto_bytes)
            # Build request message from resolved_params
            from google.protobuf import descriptor as proto_descriptor
            from google.protobuf.message_factory import MessageFactory

            pool = descriptor_pool.DescriptorPool()
            pool.Add(fd)

            svc_desc = pool.FindServiceByName(f"{fd.package}.{service_name}" if fd.package else service_name)
            method_desc = svc_desc.FindMethodByName(method_name)

            factory = MessageFactory(pool)
            request_class = factory.GetPrototype(method_desc.input_type)
            request_msg = request_class(**resolved_params)

            # Unary call
            full_method = f"/{service_name}/{method_name}"
            response_bytes = channel.unary_unary(full_method)(request_msg.SerializeToString())

            response_class = factory.GetPrototype(method_desc.output_type)
            response_msg = response_class()
            response_msg.ParseFromString(response_bytes)

            result = json_format.MessageToDict(response_msg)
            channel.close()
            return [result]

    channel.close()
    raise ApiCallError(f"Could not resolve gRPC service {service_name}")
