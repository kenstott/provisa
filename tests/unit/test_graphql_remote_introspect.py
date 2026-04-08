# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4f2595-a7b2-4f3a-8f2f-2cbcbeb37cd1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for graphql_remote introspection (REQ-307)."""
import base64

import httpx
import pytest
import respx

from provisa.graphql_remote.introspect import introspect_schema, _build_headers

SAMPLE_SCHEMA = {
    "queryType": {"name": "Query"},
    "mutationType": None,
    "types": [
        {"kind": "OBJECT", "name": "Query", "fields": [
            {"name": "users", "type": {"kind": "LIST", "name": None, "ofType": {"kind": "OBJECT", "name": "User", "ofType": None}}, "args": []}
        ]},
        {"kind": "OBJECT", "name": "User", "fields": [
            {"name": "id", "type": {"kind": "SCALAR", "name": "ID", "ofType": None}, "args": []},
        ]},
    ],
}

INTROSPECTION_URL = "https://example.com/graphql"


# --- _build_headers tests ---

def test_build_headers_no_auth():
    assert _build_headers(None) == {}


def test_build_headers_none_type():
    assert _build_headers({"type": "none"}) == {}


def test_build_headers_bearer():
    headers = _build_headers({"type": "bearer", "token": "mytoken"})
    assert headers == {"Authorization": "Bearer mytoken"}


def test_build_headers_basic():
    headers = _build_headers({"type": "basic", "username": "user", "password": "pass"})
    expected = base64.b64encode(b"user:pass").decode()
    assert headers == {"Authorization": f"Basic {expected}"}


def test_build_headers_basic_empty_creds():
    headers = _build_headers({"type": "basic", "username": "", "password": ""})
    expected = base64.b64encode(b":").decode()
    assert headers == {"Authorization": f"Basic {expected}"}


# --- introspect_schema tests ---

@pytest.mark.anyio
@respx.mock
async def test_successful_introspection():
    respx.post(INTROSPECTION_URL).mock(
        return_value=httpx.Response(200, json={"data": {"__schema": SAMPLE_SCHEMA}})
    )
    result = await introspect_schema(INTROSPECTION_URL)
    assert result == SAMPLE_SCHEMA


@pytest.mark.anyio
@respx.mock
async def test_bearer_auth_sends_header():
    route = respx.post(INTROSPECTION_URL).mock(
        return_value=httpx.Response(200, json={"data": {"__schema": SAMPLE_SCHEMA}})
    )
    await introspect_schema(INTROSPECTION_URL, auth={"type": "bearer", "token": "tok123"})
    assert route.called
    req = route.calls[0].request
    assert req.headers["Authorization"] == "Bearer tok123"


@pytest.mark.anyio
@respx.mock
async def test_basic_auth_sends_header():
    route = respx.post(INTROSPECTION_URL).mock(
        return_value=httpx.Response(200, json={"data": {"__schema": SAMPLE_SCHEMA}})
    )
    await introspect_schema(INTROSPECTION_URL, auth={"type": "basic", "username": "u", "password": "p"})
    req = route.calls[0].request
    expected = "Basic " + base64.b64encode(b"u:p").decode()
    assert req.headers["Authorization"] == expected


@pytest.mark.anyio
@respx.mock
async def test_http_error_raises():
    respx.post(INTROSPECTION_URL).mock(return_value=httpx.Response(500, text="Internal Server Error"))
    with pytest.raises(httpx.HTTPStatusError):
        await introspect_schema(INTROSPECTION_URL)


@pytest.mark.anyio
@respx.mock
async def test_graphql_errors_raise_value_error():
    respx.post(INTROSPECTION_URL).mock(
        return_value=httpx.Response(200, json={"errors": [{"message": "Not allowed"}]})
    )
    with pytest.raises(ValueError, match="Introspection errors"):
        await introspect_schema(INTROSPECTION_URL)


@pytest.mark.anyio
@respx.mock
async def test_content_type_header_set():
    route = respx.post(INTROSPECTION_URL).mock(
        return_value=httpx.Response(200, json={"data": {"__schema": SAMPLE_SCHEMA}})
    )
    await introspect_schema(INTROSPECTION_URL)
    req = route.calls[0].request
    assert req.headers["Content-Type"] == "application/json"
