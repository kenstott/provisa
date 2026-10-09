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
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from provisa.api_source.models import ApiEndpoint
from provisa.core.paging import PaginationConfig, PaginationType

# Requirements: REQ-295, REQ-297, REQ-298, REQ-316, REQ-320, REQ-322, REQ-325


class ApiCallError(Exception):
    """Raised when an API call fails after retries."""


class ApiNotFoundError(Exception):
    """Raised when the API returns 404 — caller should yield empty rows."""


_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 1.0
_DEFAULT_TIMEOUT = 30.0
# httpx's `timeout` float bounds each individual connect/read/write op, not the call's total
# wall-clock time -- a response that keeps streaming without ever idling past _DEFAULT_TIMEOUT
# between chunks never trips it, however long the transfer runs. Every caller of call_api sits
# under a hard external deadline (e.g. pgwire's `.result(timeout=120)`); without a total-duration
# cap here, a call that overruns that deadline keeps running to completion in the background while
# the outer wait already gave up with an empty/masked error (confirmed live: neo4j_materialize_cold's
# unfiltered whole-table land pulls 2M rows / ~485MB and takes 60s+ just to transfer).
_DEFAULT_TOTAL_TIMEOUT = 90.0


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
        if col.param_type is None:
            continue
        # A parameter is sent under its declared name, else under the column's own.
        param_key = col.param_name or col.name
        value = resolved_params.get(param_key) or resolved_params.get(col.name)
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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _retry_after(value: str) -> float | None:
    """How many seconds a ``Retry-After`` header asks the caller to wait: it carries either a
    count of seconds or the date to wait until. None when it carries neither."""
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        until = parsedate_to_datetime(value)
    except ValueError:
        return None
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    return max(0.0, (until - _utcnow()).total_seconds())


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
    """Make an HTTP request with retry on 429/5xx. A refusal that names its own wait
    (``Retry-After``) is waited out as asked; one asking for longer than a call may take in
    total is not retried."""
    for attempt in range(_MAX_RETRIES):
        resp = await client.request(
            method,
            url,
            params=params,
            headers=headers,
            json=json_body,
            data=form_body,
            timeout=timeout,
        )
        if resp.status_code == 404:
            raise ApiNotFoundError(f"404 Not Found: {resp.url}")
        if resp.status_code == 429 or resp.status_code >= 500:
            asked = resp.headers.get("Retry-After")
            named = "" if asked is None else f" (Retry-After: {asked})"
            wait = _RETRY_BACKOFF_BASE * (2**attempt) if asked is None else _retry_after(asked)
            if wait is None:
                raise ApiCallError(
                    f"API call refused with an unreadable wait: {resp.status_code}{named}"
                )
            if wait > _DEFAULT_TOTAL_TIMEOUT:
                raise ApiCallError(
                    f"API call refused for longer than a call may take "
                    f"({_DEFAULT_TOTAL_TIMEOUT:g}s): {resp.status_code}{named}"
                )
            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(wait)
                continue
            raise ApiCallError(
                f"API call failed after {_MAX_RETRIES} retries: "
                f"{resp.status_code}{named} {resp.text[:200]}"
            )
        resp.raise_for_status()
        return resp
    raise ApiCallError("Unreachable: retry loop exhausted")


@dataclass
class Paging:
    """What the transport knows, after each page, about whether the endpoint has more: True or
    False where the answer itself says (a next link, a next cursor), None where only the size
    of the page can (offset and page-number paging, judged by whoever counts its rows)."""

    more: bool | None = None


def _short_page(endpoint: ApiEndpoint, page: Any, page_size: int) -> bool:
    """Whether ``page`` holds fewer rows than a full page, so it is the last one. The rows are
    where the endpoint says they are (its ``response_root``)."""
    from provisa.api_source.flattener import _navigate_path

    rows = _navigate_path(page, endpoint.response_root)
    return isinstance(rows, list) and len(rows) < page_size


def _last_row_value(endpoint: ApiEndpoint, page: Any, row_field: str) -> Any:
    """``row_field`` of the last row of a full ``page``: what the next page starts after."""
    from provisa.api_source.flattener import _navigate_path

    rows = _navigate_path(page, endpoint.response_root)
    last = rows[-1] if isinstance(rows, list) and rows else None
    if not isinstance(last, dict) or last.get(row_field) is None:
        raise ApiCallError(
            f"{endpoint.table_name}: its paging starts each page after the last row's "
            f"{row_field!r}, and the last row of a page has none"
        )
    return last[row_field]


async def _pages(
    client: httpx.AsyncClient,
    endpoint: ApiEndpoint,
    url: str,
    params: dict,
    headers: dict,
    body: dict | None,
    timeout: float,
    form_body: dict | None = None,
    paging: Paging | None = None,
    source_headers: dict[str, str] | None = None,
) -> AsyncGenerator[Any, None]:
    """Follow pagination, yielding each page as it arrives: a reader that takes them one at a
    time holds one page, not the collection."""
    # REQ-1882: `resp.json()` runs a synchronous json.loads over the full buffered body -- for a
    # large source response (confirmed live: neo4j_materialize_cold's 2M-row/485MB unfiltered
    # land) that's tens of milliseconds to double-digit seconds of pure CPU. call_api is reached
    # from the governance/residency-prep path every governed query is dispatched onto ONE shared
    # asyncio loop (see provisa/api/flight/server.py's `_run_on_loop`), so a decode running inline
    # here blocks every OTHER concurrent query's governance for its duration -- this is the
    # confirmed root cause of the "whole server hangs, empty error" symptoms seen live on
    # federated_join/neo4j_materialize_cold/cypher_cross_engine. run_in_executor moves it off the
    # loop, same pattern as _off_loop in provisa/pgwire/_pipeline.py.
    loop = asyncio.get_running_loop()
    pagination = endpoint.pagination
    if pagination is None or pagination.type is None:  # one answer, wrapped or not
        resp = await _request_with_retry(
            client,
            endpoint.method,
            url,
            params,
            headers,
            json_body=body,
            form_body=form_body,
            timeout=timeout,
        )
        yield await loop.run_in_executor(None, resp.json)
        return

    max_pages = pagination.max_pages

    if pagination.type == PaginationType.link_header:
        next_url: str | None = url
        for _ in range(max_pages):
            if next_url is None:
                break
            resp = await _request_with_retry(
                client,
                endpoint.method,
                next_url,
                params,
                headers,
                json_body=body,
                form_body=form_body,
                timeout=timeout,
            )
            page = await loop.run_in_executor(None, resp.json)
            link = resp.headers.get("link", "")
            match = re.search(r'<([^>]+)>;\s*rel="next"', link)
            next_url = match.group(1) if match else None
            params = {}  # subsequent pages use full URL from link
            if paging is not None:
                paging.more = next_url is not None
            yield page

    elif pagination.type == PaginationType.cursor:
        cursor_param = pagination.cursor_param or "cursor"
        cursor_field = pagination.cursor_field or "next_cursor"
        if pagination.page_size_param:
            # A cursor names where a page starts and not how long it is: the page size is sent
            # only under a parameter the table declares, and the remote's own applies without one.
            params = {**(params or {}), pagination.page_size_param: pagination.page_size}
        for _ in range(max_pages):
            resp = await _request_with_retry(
                client,
                endpoint.method,
                url,
                params,
                headers,
                json_body=body,
                form_body=form_body,
                timeout=timeout,
            )
            data = await loop.run_in_executor(None, resp.json)
            cursor = data.get(cursor_field) if isinstance(data, dict) else None
            if paging is not None:
                paging.more = bool(cursor)
            yield data
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
                client,
                endpoint.method,
                url,
                p,
                headers,
                json_body=body,
                form_body=form_body,
                timeout=timeout,
            )
            data = await loop.run_in_executor(None, resp.json)
            yield data
            if _short_page(endpoint, data, page_size):
                break
            offset += page_size

    elif pagination.type == PaginationType.last_row:
        page_size = pagination.page_size
        page_size_param = pagination.page_size_param or "limit"
        after_param = pagination.cursor_param or "starting_after"
        row_field = pagination.cursor_field or "id"
        p = dict(params or {})
        p[page_size_param] = page_size
        for _ in range(max_pages):
            resp = await _request_with_retry(
                client,
                endpoint.method,
                url,
                p,
                headers,
                json_body=body,
                form_body=form_body,
                timeout=timeout,
            )
            data = await loop.run_in_executor(None, resp.json)
            yield data
            if _short_page(endpoint, data, page_size):
                break
            p = {**p, after_param: _last_row_value(endpoint, data, row_field)}

    elif pagination.type == PaginationType.page_number:
        page_param = pagination.page_param or "page"
        page_size_param = pagination.page_size_param or "per_page"
        page_size = pagination.page_size
        for page_num in range(1, max_pages + 1):
            p = dict(params or {})
            p[page_param] = page_num
            p[page_size_param] = page_size
            resp = await _request_with_retry(
                client,
                endpoint.method,
                url,
                p,
                headers,
                json_body=body,
                form_body=form_body,
                timeout=timeout,
            )
            data = await loop.run_in_executor(None, resp.json)
            yield data
            if _short_page(endpoint, data, page_size):
                break


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
    return [
        page
        async for page in _pages(
            client, endpoint, url, params, headers, body, timeout, form_body=form_body
        )
    ]


def _apply_auth(auth, headers: dict, query_params: dict) -> None:  # REQ-320
    """Apply typed auth config to request headers/params, resolving secrets."""
    if auth is None:
        return

    from provisa.core.auth_models import (
        ApiAuthBearer,
        ApiAuthBasic,
        ApiAuthApiKey,
        ApiAuthGoogleServiceAccount,
        ApiAuthOAuth2ClientCredentials,
        ApiAuthOAuth2RefreshToken,
        ApiAuthCustomHeaders,
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

            cred = base64.b64encode(f"{resolve_secrets(u)}:{resolve_secrets(p)}".encode()).decode()
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
        case ApiAuthOAuth2RefreshToken() | ApiAuthGoogleServiceAccount():
            from provisa.api_source.oauth_grants import access_token

            headers["Authorization"] = f"Bearer {access_token(auth)}"
        case ApiAuthCustomHeaders(headers=h):
            for k, v in h.items():
                headers[k] = resolve_secrets(v)


# Simple token cache for OAuth2 client credentials
_oauth2_cache: dict[str, tuple[str, float]] = {}


def _fetch_oauth2_token(oauth) -> str:  # REQ-320
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


@dataclass(frozen=True)
class PreparedCall:
    """One API call as it is sent: where, with what, and how its body is encoded."""

    method: str
    url: str
    params: dict
    headers: dict
    json_body: dict | None
    form_body: dict | None


def prepare_call(
    endpoint: ApiEndpoint,
    resolved_params: dict,
    base_url: str = "",
    auth: Any = None,
    source_headers: dict[str, str] | None = None,
) -> PreparedCall:
    """The HTTP call ``endpoint`` makes with ``resolved_params``: its URL under ``base_url``,
    its source's own headers (``source_headers``) and auth applied, its body encoded as the
    endpoint declares. Not for a gRPC endpoint
    (``method == "RPC"``), which is not an HTTP call."""
    from provisa.core.secrets import resolve_secrets

    url, query_params, headers, body = _build_request_parts(endpoint, resolved_params)

    # A source's address is stored as it was written; a credential reference in it is resolved
    # here, at the call, as the auth's are below.
    base_url = resolve_secrets(base_url)
    if not url:
        url = base_url  # the endpoint IS the source's address (it declares no path of its own)
    elif not url.startswith("http"):
        # Prepend base_url if path is relative
        url = base_url.rstrip("/") + "/" + url.lstrip("/")

    headers.update(source_headers or {})
    _apply_auth(auth, headers, query_params)

    # GraphQL: wrap query in proper body
    json_body: dict | None = None
    form_body: dict | None = None
    if endpoint.method == "QUERY":
        body = body or {}
        json_body = {"query": endpoint.path, "variables": body}
    elif endpoint.body_encoding == "neo4j_tx":
        # REQ-1668: Neo4j HTTP transaction API (/db/{db}/tx/commit) — the endpoint every 5.x
        # server exposes; the Query API v2 (/query/v2) is absent (404) on the community images.
        # REQ-1865: "parameters" was missing entirely, so no neo4j_tx call ever bound a value into
        # its query_template — latent until the row-level materializer's keyed fetch needed a real
        # $keys binding. resolved_params passes straight through: Cypher parameter names match dict
        # keys directly, no column/param_type indirection needed the way query/path/body params do.
        json_body = {
            "statements": [{"statement": endpoint.query_template, "parameters": resolved_params}]
        }
    elif endpoint.body_encoding == "json":
        # Generic query-API POST with the query as a JSON body
        json_body = {"statement": endpoint.query_template} if endpoint.query_template else body
    elif endpoint.body_encoding == "form":
        # SPARQL 1.1: POST with form-encoded query parameter
        form_body = {"query": endpoint.query_template} if endpoint.query_template else {}
        if body:
            form_body.update(body)
    else:
        json_body = body
    return PreparedCall(endpoint.method, url, query_params, headers, json_body, form_body)


@dataclass(frozen=True)
class AnswerCut:
    """An answer that stopped at the endpoint's ``max_pages`` while the endpoint had more."""

    max_pages: int
    rows: int


@dataclass(frozen=True)
class ApiAnswer:
    """What one API call returned: its pages, and what the paging knows about whether the
    endpoint had more when the call stopped."""

    pages: list[Any]
    pagination: PaginationConfig | None = None
    #: The answer's own word on whether there is more (a next link, a next cursor); None where
    #: only the size of the last page can say (offset and page-number paging).
    more: bool | None = None

    def cut(self, last_page_rows: int) -> bool:
        """Whether the call stopped at ``max_pages`` with more to read. ``last_page_rows``: the
        rows the last page held, for paging whose answer does not say."""
        paging = self.pagination
        if paging is None or len(self.pages) < paging.max_pages:
            return False
        if self.more is not None:
            return self.more
        return last_page_rows >= paging.page_size


def answer_rows(endpoint: ApiEndpoint, answer: ApiAnswer) -> tuple[list[dict], AnswerCut | None]:
    """The rows of ``answer`` flattened as ``endpoint`` declares, and how it was cut (None when
    it is the whole answer)."""
    from provisa.api_source.flattener import flatten_response

    rows: list[dict] = []
    last = 0
    for page in answer.pages:
        page_rows = flatten_response(
            page, endpoint.response_root, endpoint.columns, endpoint.response_normalizer
        )
        last = len(page_rows)
        rows.extend(page_rows)
    if not answer.cut(last):
        return rows, None
    assert answer.pagination is not None  # cut() is True only for a paged call
    return rows, AnswerCut(answer.pagination.max_pages, len(rows))


def answer_cut_warning(table: str, cut: AnswerCut) -> Any:
    """The warning a statement answered from a cut API answer carries (REQ-1350)."""
    from provisa.core.statement_warnings import ServerWarning

    return ServerWarning(
        code="api.answer_cut",
        params={"table": table, "max_pages": cut.max_pages, "rows": cut.rows},
        message=(
            f"the answer for {table} was cut at max_pages={cut.max_pages} ({cut.rows} rows): "
            "the API has more. Raise the endpoint's max_pages or its page size to read it all."
        ),
    )


async def call_api(  # REQ-295, REQ-297, REQ-298, REQ-316
    endpoint: ApiEndpoint,
    resolved_params: dict,
    base_url: str = "",
    auth=None,
    timeout: float = _DEFAULT_TIMEOUT,
    total_timeout: float = _DEFAULT_TOTAL_TIMEOUT,
    source_headers: dict[str, str] | None = None,
) -> ApiAnswer:
    """Make the API call and return its pages, with whether the endpoint had more when the
    call stopped at its ``max_pages`` (:class:`ApiAnswer`; :func:`answer_rows` flattens it).

    ``timeout`` bounds each individual connect/read/write op (httpx semantics); ``total_timeout``
    bounds the whole call's wall-clock time, including every paginated page, so a response that
    streams continuously without ever idling still fails explicitly instead of outrunning a
    caller's own external deadline (see module docstring on ``_DEFAULT_TOTAL_TIMEOUT``)."""
    if endpoint.method == "RPC":
        from provisa.core.secrets import resolve_secrets

        return ApiAnswer(await _call_grpc(endpoint, resolved_params, resolve_secrets(base_url)))
    call = prepare_call(endpoint, resolved_params, base_url, auth, source_headers)

    async def _run() -> ApiAnswer:
        paging = Paging()
        async with httpx.AsyncClient() as client:
            pages = [
                page
                async for page in _pages(
                    client,
                    endpoint,
                    call.url,
                    call.params,
                    call.headers,
                    call.json_body,
                    timeout,
                    form_body=call.form_body,
                    paging=paging,
                )
            ]
        return ApiAnswer(pages, endpoint.pagination, paging.more)

    try:
        return await asyncio.wait_for(_run(), timeout=total_timeout)
    except asyncio.TimeoutError as exc:
        raise ApiCallError(
            f"API call to {call.url!r} exceeded total_timeout={total_timeout}s "
            f"(source={endpoint.source_id!r}, table={endpoint.table_name!r}) -- "
            f"response kept streaming without idling past a single connect/read op, so httpx's "
            f"per-op timeout={timeout}s never tripped"
        ) from exc


async def iter_api_pages(  # REQ-1915
    endpoint: ApiEndpoint,
    resolved_params: dict,
    base_url: str = "",
    auth: Any = None,
    timeout: float = _DEFAULT_TIMEOUT,
    paging: Paging | None = None,
    source_headers: dict[str, str] | None = None,
) -> AsyncGenerator[Any, None]:
    """The pages of a paginated HTTP call, one at a time as each arrives, for a reader that
    copies a whole collection (a replica build). It holds one page at a time, and has no total
    time limit: a build is not under a request's deadline, and each page is under ``timeout``.
    It stops at the endpoint's ``max_pages`` like every call; ``paging`` tells the reader
    whether the endpoint had more."""
    call = prepare_call(endpoint, resolved_params, base_url, auth, source_headers)
    async with httpx.AsyncClient() as client:
        async for page in _pages(
            client,
            endpoint,
            call.url,
            call.params,
            call.headers,
            call.json_body,
            timeout,
            form_body=call.form_body,
            paging=paging,
        ):
            yield page


async def _call_grpc(  # REQ-322, REQ-325
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
    responses = stub.ServerReflectionInfo(iter([req]))  # type: ignore[operator]

    for resp in responses:
        for proto_bytes in resp.file_descriptor_response.file_descriptor_proto:
            fd = descriptor_pb2.FileDescriptorProto()
            fd.ParseFromString(proto_bytes)
            # Build request message from resolved_params
            from google.protobuf.message_factory import MessageFactory

            pool = descriptor_pool.DescriptorPool()
            pool.Add(fd)

            svc_desc = pool.FindServiceByName(
                f"{fd.package}.{service_name}" if fd.package else service_name
            )
            method_desc = svc_desc.FindMethodByName(method_name)

            factory = MessageFactory(pool)
            request_class = factory.GetPrototype(method_desc.input_type)  # type: ignore[attr-defined]
            request_msg = request_class(**resolved_params)  # type: ignore[operator]

            # Unary call
            full_method = f"/{service_name}/{method_name}"
            response_bytes = channel.unary_unary(full_method)(request_msg.SerializeToString())

            response_class = factory.GetPrototype(method_desc.output_type)  # type: ignore[attr-defined]
            response_msg = response_class()
            response_msg.ParseFromString(response_bytes)

            result = json_format.MessageToDict(response_msg)
            channel.close()
            return [result]

    channel.close()
    raise ApiCallError(f"Could not resolve gRPC service {service_name}")
