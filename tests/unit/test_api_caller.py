# Copyright (c) 2026 Kenneth Stott
# Canary: a71379b5-31dd-4c37-bfda-91136438157f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for API source caller (Phase U)."""

from unittest.mock import AsyncMock, patch

import pytest

from provisa.api_source.caller import _build_request_parts, call_api
from provisa.api_source.models import (
    ApiColumn,
    ApiColumnType,
    ApiEndpoint,
    PaginationConfig,
    PaginationType,
    ParamType,
)


def _make_endpoint(**kwargs) -> ApiEndpoint:
    defaults = {
        "id": 1,
        "source_id": "test-api",
        "path": "/users",
        "method": "GET",
        "table_name": "users",
        "columns": [],
        "ttl": 300,
    }
    defaults.update(kwargs)
    return ApiEndpoint(**defaults)


# --- Param assembly ---

def test_query_param_assembly():
    """Query params are placed in query string."""
    endpoint = _make_endpoint(columns=[
        ApiColumn(name="status", type=ApiColumnType.string, param_type=ParamType.query, param_name="status"),
    ])
    url, params, headers, body = _build_request_parts(endpoint, {"status": "active"})
    assert params == {"status": "active"}


def test_path_param_interpolation():
    """Path params are interpolated into the URL."""
    endpoint = _make_endpoint(
        path="/users/{user_id}/posts",
        columns=[
            ApiColumn(name="user_id", type=ApiColumnType.integer, param_type=ParamType.path, param_name="user_id"),
        ],
    )
    url, params, headers, body = _build_request_parts(endpoint, {"user_id": 42})
    assert url == "/users/42/posts"
    assert params == {}


def test_body_param_assembly():
    """Body params go into the request body."""
    endpoint = _make_endpoint(columns=[
        ApiColumn(name="filter", type=ApiColumnType.string, param_type=ParamType.body, param_name="filter"),
    ])
    url, params, headers, body = _build_request_parts(endpoint, {"filter": "active"})
    assert body == {"filter": "active"}


def test_header_param_assembly():
    """Header params go into request headers."""
    endpoint = _make_endpoint(columns=[
        ApiColumn(name="tenant", type=ApiColumnType.string, param_type=ParamType.header, param_name="X-Tenant"),
    ])
    url, params, headers, body = _build_request_parts(endpoint, {"X-Tenant": "acme"})
    assert headers == {"X-Tenant": "acme"}


def test_mixed_param_assembly():
    """Multiple param types assembled correctly."""
    endpoint = _make_endpoint(
        path="/orgs/{org_id}/members",
        columns=[
            ApiColumn(name="org_id", type=ApiColumnType.integer, param_type=ParamType.path, param_name="org_id"),
            ApiColumn(name="role", type=ApiColumnType.string, param_type=ParamType.query, param_name="role"),
            ApiColumn(name="api_key", type=ApiColumnType.string, param_type=ParamType.header, param_name="X-Api-Key"),
        ],
    )
    resolved = {"org_id": 5, "role": "admin", "X-Api-Key": "secret"}
    url, params, headers, body = _build_request_parts(endpoint, resolved)
    assert url == "/orgs/5/members"
    assert params == {"role": "admin"}
    assert headers == {"X-Api-Key": "secret"}
    assert body is None


# --- Pagination ---

class FakeResponse:
    def __init__(self, data, status_code=200, headers=None):
        self._data = data
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


@pytest.mark.asyncio
async def test_pagination_link_header():
    """Link header pagination follows next links."""
    page1 = FakeResponse(
        [{"id": 1}],
        headers={"link": '<http://api.example.com/users?page=2>; rel="next"'},
    )
    page2 = FakeResponse([{"id": 2}], headers={})

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.request = AsyncMock(side_effect=[page1, page2])

    endpoint = _make_endpoint(
        pagination=PaginationConfig(type=PaginationType.link_header),
    )

    with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
        pages = await call_api(endpoint, {}, base_url="http://api.example.com")

    assert len(pages) == 2
    assert pages[0] == [{"id": 1}]
    assert pages[1] == [{"id": 2}]


@pytest.mark.asyncio
async def test_pagination_cursor():
    """Cursor pagination follows cursor field."""
    page1 = FakeResponse({"items": [{"id": 1}], "next_cursor": "abc"})
    page2 = FakeResponse({"items": [{"id": 2}], "next_cursor": None})

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.request = AsyncMock(side_effect=[page1, page2])

    endpoint = _make_endpoint(
        pagination=PaginationConfig(
            type=PaginationType.cursor,
            cursor_field="next_cursor",
            cursor_param="cursor",
        ),
    )

    with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
        pages = await call_api(endpoint, {}, base_url="http://api.example.com")

    assert len(pages) == 2


@pytest.mark.asyncio
async def test_no_pagination_single_request():
    """Without pagination config, a single request is made."""
    resp = FakeResponse([{"id": 1}, {"id": 2}])

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.request = AsyncMock(return_value=resp)

    endpoint = _make_endpoint()

    with patch("provisa.api_source.caller.httpx.AsyncClient", return_value=mock_client):
        pages = await call_api(endpoint, {}, base_url="http://api.example.com")

    assert len(pages) == 1
    assert mock_client.request.call_count == 1
