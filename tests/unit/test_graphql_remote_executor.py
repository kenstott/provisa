# Copyright (c) 2026 Kenneth Stott
# Canary: a3f7c821-4d9e-4b1a-9e3f-d82c15e7b904
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/graphql_remote/executor.py — query building with pagination."""

import json
import pytest
import httpx
import respx

from provisa.graphql_remote.executor import execute_remote


def _ok_response(field: str, rows: list) -> httpx.Response:
    return httpx.Response(200, json={"data": {field: rows}})


@respx.mock
@pytest.mark.asyncio
async def test_no_pagination_sends_plain_query():
    route = respx.post("http://gql/").mock(return_value=_ok_response("users", [{"id": "1"}]))
    await execute_remote("http://gql/", None, "users", ["id"])
    body = json.loads(route.calls[0].request.content)
    assert "first" not in body["query"]
    assert "limit" not in body["query"]


@respx.mock
@pytest.mark.asyncio
async def test_first_pagination_arg_injected():
    route = respx.post("http://gql/").mock(return_value=_ok_response("users", [{"id": "1"}]))
    await execute_remote(
        "http://gql/",
        None,
        "users",
        ["id"],
        limit=50,
        pagination={"limit_arg": "first", "offset_arg": None, "cursor_arg": None},
    )
    body = json.loads(route.calls[0].request.content)
    assert "first: 50" in body["query"]


@respx.mock
@pytest.mark.asyncio
async def test_limit_pagination_arg_injected():
    route = respx.post("http://gql/").mock(return_value=_ok_response("items", [{"id": "2"}]))
    await execute_remote(
        "http://gql/",
        None,
        "items",
        ["id"],
        limit=100,
        pagination={"limit_arg": "limit", "offset_arg": None, "cursor_arg": None},
    )
    body = json.loads(route.calls[0].request.content)
    assert "limit: 100" in body["query"]


@respx.mock
@pytest.mark.asyncio
async def test_offset_arg_injected_when_provided():
    route = respx.post("http://gql/").mock(return_value=_ok_response("users", []))
    await execute_remote(
        "http://gql/",
        None,
        "users",
        ["id"],
        limit=10,
        offset=20,
        pagination={"limit_arg": "first", "offset_arg": "offset", "cursor_arg": None},
    )
    body = json.loads(route.calls[0].request.content)
    assert "first: 10" in body["query"]
    assert "offset: 20" in body["query"]


@respx.mock
@pytest.mark.asyncio
async def test_offset_not_injected_when_none():
    route = respx.post("http://gql/").mock(return_value=_ok_response("users", []))
    await execute_remote(
        "http://gql/",
        None,
        "users",
        ["id"],
        limit=10,
        offset=None,
        pagination={"limit_arg": "first", "offset_arg": "offset", "cursor_arg": None},
    )
    body = json.loads(route.calls[0].request.content)
    assert "offset" not in body["query"]


@respx.mock
@pytest.mark.asyncio
async def test_pagination_with_required_args():
    """Pagination args are appended alongside required variable args."""
    route = respx.post("http://gql/").mock(return_value=_ok_response("orders", [{"id": "3"}]))
    required_args = [{"name": "tenant", "gql_type": "String!", "provisa_type": "text"}]
    await execute_remote(
        "http://gql/",
        None,
        "orders",
        ["id"],
        variables={"tenant": "acme"},
        required_args=required_args,
        limit=25,
        pagination={"limit_arg": "first", "offset_arg": None, "cursor_arg": None},
    )
    body = json.loads(route.calls[0].request.content)
    assert "tenant: $tenant" in body["query"]
    assert "first: 25" in body["query"]


@respx.mock
@pytest.mark.asyncio
async def test_no_pagination_arg_when_pagination_dict_has_no_limit_arg():
    route = respx.post("http://gql/").mock(return_value=_ok_response("users", []))
    await execute_remote(
        "http://gql/",
        None,
        "users",
        ["id"],
        limit=50,
        pagination={"limit_arg": None, "offset_arg": None, "cursor_arg": None},
    )
    body = json.loads(route.calls[0].request.content)
    assert "(" not in body["query"]


@respx.mock
@pytest.mark.asyncio
async def test_rows_returned():
    respx.post("http://gql/").mock(
        return_value=_ok_response("products", [{"sku": "A"}, {"sku": "B"}])
    )
    rows = await execute_remote("http://gql/", None, "products", ["sku"])
    assert rows == [{"sku": "A"}, {"sku": "B"}]
