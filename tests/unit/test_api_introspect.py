# Copyright (c) 2026 Kenneth Stott
# Canary: 52c29451-2254-4970-9190-3fb23859e110
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for API source introspection (Phase U)."""

from unittest.mock import AsyncMock, patch

import pytest

from provisa.api_source.introspect import introspect_openapi, introspect_graphql
from provisa.api_source.models import ApiColumnType


# --- OpenAPI introspection ---

OPENAPI_SPEC = {
    "openapi": "3.0.0",
    "paths": {
        "/users": {
            "get": {
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/User"},
                                }
                            }
                        }
                    }
                }
            }
        },
        "/orders": {
            "get": {
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Order"},
                                }
                            }
                        }
                    }
                }
            }
        },
        "/users/{id}": {
            "post": {
                "responses": {"200": {"content": {"application/json": {"schema": {}}}}}
            }
        },
    },
    "components": {
        "schemas": {
            "User": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "active": {"type": "boolean"},
                    "score": {"type": "number"},
                    "address": {"$ref": "#/components/schemas/Address"},
                },
            },
            "Address": {
                "type": "object",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"},
                },
            },
            "Order": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "integer"},
                    "items": {"type": "array", "items": {"type": "string"}},
                },
            },
        }
    },
}


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


@pytest.mark.asyncio
async def test_introspect_openapi_candidates():
    """OpenAPI spec parsing produces candidate tables for GET endpoints."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=FakeResponse(OPENAPI_SPEC))

    with patch("provisa.api_source.introspect.httpx.AsyncClient", return_value=mock_client):
        candidates = await introspect_openapi("http://example.com/openapi.json")

    # Only GET endpoints: /users and /orders (not POST /users/{id})
    assert len(candidates) == 2
    table_names = {c.table_name for c in candidates}
    assert "users" in table_names
    assert "orders" in table_names


@pytest.mark.asyncio
async def test_introspect_openapi_type_mapping():
    """Primitive types map to native columns, complex types to JSONB."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=FakeResponse(OPENAPI_SPEC))

    with patch("provisa.api_source.introspect.httpx.AsyncClient", return_value=mock_client):
        candidates = await introspect_openapi("http://example.com/openapi.json")

    users = next(c for c in candidates if c.table_name == "users")
    col_map = {c.name: c for c in users.columns}

    assert col_map["id"].type == ApiColumnType.integer
    assert col_map["name"].type == ApiColumnType.string
    assert col_map["email"].type == ApiColumnType.string
    assert col_map["active"].type == ApiColumnType.boolean
    assert col_map["score"].type == ApiColumnType.number
    # Address is a $ref to an object -> JSONB
    assert col_map["address"].type == ApiColumnType.jsonb
    assert col_map["address"].filterable is False


@pytest.mark.asyncio
async def test_introspect_openapi_array_column():
    """Array properties map to JSONB."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get = AsyncMock(return_value=FakeResponse(OPENAPI_SPEC))

    with patch("provisa.api_source.introspect.httpx.AsyncClient", return_value=mock_client):
        candidates = await introspect_openapi("http://example.com/openapi.json")

    orders = next(c for c in candidates if c.table_name == "orders")
    col_map = {c.name: c for c in orders.columns}
    # "items" is type: array -> JSONB
    assert col_map["items"].type == ApiColumnType.jsonb


# --- GraphQL introspection ---

GRAPHQL_INTROSPECTION_RESPONSE = {
    "data": {
        "__schema": {
            "queryType": {"name": "Query"},
            "types": [
                {
                    "name": "Query",
                    "kind": "OBJECT",
                    "fields": [
                        {
                            "name": "users",
                            "type": {
                                "name": None,
                                "kind": "LIST",
                                "ofType": {"name": "User", "kind": "OBJECT", "ofType": None},
                            },
                        },
                        {
                            "name": "__schema",
                            "type": {"name": "__Schema", "kind": "OBJECT", "ofType": None},
                        },
                    ],
                },
                {
                    "name": "User",
                    "kind": "OBJECT",
                    "fields": [
                        {
                            "name": "id",
                            "type": {"name": "ID", "kind": "SCALAR", "ofType": None},
                        },
                        {
                            "name": "name",
                            "type": {"name": "String", "kind": "SCALAR", "ofType": None},
                        },
                        {
                            "name": "age",
                            "type": {"name": "Int", "kind": "SCALAR", "ofType": None},
                        },
                        {
                            "name": "profile",
                            "type": {"name": "Profile", "kind": "OBJECT", "ofType": None},
                        },
                    ],
                },
                {
                    "name": "Profile",
                    "kind": "OBJECT",
                    "fields": [],
                },
            ],
        }
    }
}


@pytest.mark.asyncio
async def test_introspect_graphql_candidates():
    """GraphQL introspection produces candidate tables, skips internal types."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=FakeResponse(GRAPHQL_INTROSPECTION_RESPONSE))

    with patch("provisa.api_source.introspect.httpx.AsyncClient", return_value=mock_client):
        candidates = await introspect_graphql("http://example.com/graphql")

    # Should have "users" but not "__schema"
    assert len(candidates) == 1
    assert candidates[0].table_name == "users"
    assert candidates[0].method == "QUERY"


@pytest.mark.asyncio
async def test_introspect_graphql_type_mapping():
    """GraphQL scalar types map to native, objects to JSONB."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=FakeResponse(GRAPHQL_INTROSPECTION_RESPONSE))

    with patch("provisa.api_source.introspect.httpx.AsyncClient", return_value=mock_client):
        candidates = await introspect_graphql("http://example.com/graphql")

    users = candidates[0]
    col_map = {c.name: c for c in users.columns}

    assert col_map["id"].type == ApiColumnType.string  # GraphQL ID -> string
    assert col_map["name"].type == ApiColumnType.string
    assert col_map["age"].type == ApiColumnType.integer
    assert col_map["profile"].type == ApiColumnType.jsonb
    assert col_map["profile"].filterable is False
