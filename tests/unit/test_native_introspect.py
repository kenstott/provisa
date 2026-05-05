# Copyright (c) 2026 Kenneth Stott
# Canary: 2a4b6c8d-0e1f-2a3b-4c5d-6e7f8a9b0c1d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.api.admin.introspect native introspection helpers."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from provisa.api.admin.introspect import (
    native_schemas,
    native_tables,
    _openapi_is_table,
    _gql_field_returns_list,
    _unwrap_gql_type,
)
from provisa.api.admin.types import AvailableTableType
from provisa.executor.trino import QueryResult


# ── helpers ──────────────────────────────────────────────────────────────────

def _pool(source_id: str, rows: list[list]) -> MagicMock:
    """Mock SourcePool that returns given rows for execute()."""
    pool = MagicMock()
    pool.has.return_value = True
    pool.execute = AsyncMock(return_value=QueryResult(rows=rows, column_names=[]))
    return pool


def _empty_pool(source_id: str) -> MagicMock:
    pool = MagicMock()
    pool.has.return_value = False
    return pool


# ── _openapi_is_table ─────────────────────────────────────────────────────────

def test_openapi_is_table_direct_array():
    q = MagicMock()
    q.response_schema = {"type": "array", "items": {"type": "object"}}
    assert _openapi_is_table(q) is True


def test_openapi_is_table_pagination_wrapper():
    q = MagicMock()
    q.response_schema = {
        "type": "object",
        "properties": {
            "items": {"type": "array"},
            "total": {"type": "integer"},
        },
    }
    assert _openapi_is_table(q) is True


def test_openapi_is_table_single_object():
    q = MagicMock()
    q.response_schema = {"type": "object", "properties": {"id": {"type": "string"}}}
    assert _openapi_is_table(q) is False


def test_openapi_is_table_no_schema():
    q = MagicMock()
    q.response_schema = None
    assert _openapi_is_table(q) is False


def test_openapi_is_table_object_multiple_array_props():
    # Two array props → not a clean pagination wrapper
    q = MagicMock()
    q.response_schema = {
        "type": "object",
        "properties": {
            "items": {"type": "array"},
            "related": {"type": "array"},
        },
    }
    assert _openapi_is_table(q) is False


# ── _gql_field_returns_list ───────────────────────────────────────────────────

def test_gql_field_returns_list_direct():
    field = {"name": "pets", "type": {"kind": "LIST", "ofType": {"kind": "OBJECT", "name": "Pet"}}}
    assert _gql_field_returns_list(field) is True


def test_gql_field_returns_list_nonnull_wrapped():
    field = {
        "name": "pets",
        "type": {"kind": "NON_NULL", "ofType": {"kind": "LIST", "ofType": {"kind": "OBJECT", "name": "Pet"}}},
    }
    assert _gql_field_returns_list(field) is True


def test_gql_field_returns_single_object():
    field = {"name": "pet", "type": {"kind": "OBJECT", "name": "Pet"}}
    assert _gql_field_returns_list(field) is False


def test_gql_field_returns_scalar():
    field = {"name": "count", "type": {"kind": "SCALAR", "name": "Int"}}
    assert _gql_field_returns_list(field) is False


# ── native_schemas ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_native_schemas_graphql():
    result = await native_schemas("src", "graphql", _empty_pool("src"), None)
    assert result == ["default"]


@pytest.mark.asyncio
async def test_native_schemas_graphql_remote():
    result = await native_schemas("src", "graphql_remote", _empty_pool("src"), None)
    assert result == ["default"]


@pytest.mark.asyncio
async def test_native_schemas_grpc():
    result = await native_schemas("src", "grpc", _empty_pool("src"), None)
    assert result == ["default"]


@pytest.mark.asyncio
async def test_native_schemas_grpc_remote():
    result = await native_schemas("src", "grpc_remote", _empty_pool("src"), None)
    assert result == ["default"]


@pytest.mark.asyncio
async def test_native_schemas_kafka():
    result = await native_schemas("src", "kafka", _empty_pool("src"), None)
    assert result == ["default"]


@pytest.mark.asyncio
async def test_native_schemas_neo4j():
    result = await native_schemas("src", "neo4j", _empty_pool("src"), None)
    assert result == []


@pytest.mark.asyncio
async def test_native_schemas_sparql():
    result = await native_schemas("src", "sparql", _empty_pool("src"), None)
    assert result == []


@pytest.mark.asyncio
async def test_native_schemas_openapi():
    result = await native_schemas("src", "openapi", _empty_pool("src"), None)
    assert result == ["openapi"]


@pytest.mark.asyncio
async def test_native_schemas_postgresql():
    pool = _pool("src", [["public"], ["pet_store"]])
    result = await native_schemas("src", "postgresql", pool, None)
    assert result == ["public", "pet_store"]


@pytest.mark.asyncio
async def test_native_schemas_no_driver_returns_none():
    result = await native_schemas("src", "postgresql", _empty_pool("src"), None)
    assert result is None


@pytest.mark.asyncio
async def test_native_schemas_mysql():
    pool = _pool("src", [["mydb"], ["information_schema"], ["mysql"], ["sys"]])
    result = await native_schemas("src", "mysql", pool, None)
    assert result == ["mydb"]


@pytest.mark.asyncio
async def test_native_schemas_unknown_type_returns_none():
    result = await native_schemas("src", "bigquery", _empty_pool("src"), None)
    assert result is None


# ── native_tables ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_native_tables_neo4j_returns_empty():
    result = await native_tables("src", "neo4j", "default", _empty_pool("src"), None, MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_sparql_returns_empty():
    result = await native_tables("src", "sparql", "default", _empty_pool("src"), None, MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_no_driver_returns_none():
    result = await native_tables("src", "postgresql", "public", _empty_pool("src"), None, MagicMock())
    assert result is None


@pytest.mark.asyncio
async def test_native_tables_postgresql():
    pool = _pool("src", [["orders", "Customer orders"], ["pets", None]])
    result = await native_tables("src", "postgresql", "public", pool, None, MagicMock())
    assert len(result) == 2
    assert result[0].name == "orders"
    assert result[0].comment == "Customer orders"
    assert result[1].name == "pets"
    assert result[1].comment is None


@pytest.mark.asyncio
async def test_native_tables_kafka():
    config_conn = AsyncMock()
    config_conn.fetch = AsyncMock(return_value=[{"topic": "orders"}, {"topic": "events"}])
    result = await native_tables("src", "kafka", "default", _empty_pool("src"), config_conn, MagicMock())
    assert [t.name for t in result] == ["orders", "events"]


@pytest.mark.asyncio
async def test_native_tables_kafka_wrong_schema():
    result = await native_tables("src", "kafka", "other", _empty_pool("src"), None, MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_openapi_filters_non_array():
    state = MagicMock()
    q_array = MagicMock()
    q_array.operation_id = "listPets"
    q_array.summary = "List all pets"
    q_array.response_schema = {"type": "array"}

    q_single = MagicMock()
    q_single.operation_id = "getPetById"
    q_single.summary = "Get one pet"
    q_single.response_schema = {"type": "object", "properties": {"id": {"type": "string"}}}

    with patch("provisa.openapi.mapper.parse_spec", return_value=([q_array, q_single], [])):
        state.openapi_specs = {"src": {"spec": {}}}
        result = await native_tables("src", "openapi", "openapi", _empty_pool("src"), None, state)

    assert len(result) == 1
    assert result[0].name == "listPets"


@pytest.mark.asyncio
async def test_native_tables_openapi_pagination_wrapper_included():
    state = MagicMock()
    q = MagicMock()
    q.operation_id = "searchPets"
    q.summary = "Search pets"
    q.response_schema = {
        "type": "object",
        "properties": {"items": {"type": "array"}, "total": {"type": "integer"}},
    }

    with patch("provisa.openapi.mapper.parse_spec", return_value=([q], [])):
        state.openapi_specs = {"src": {"spec": {}}}
        result = await native_tables("src", "openapi", "openapi", _empty_pool("src"), None, state)

    assert len(result) == 1
    assert result[0].name == "searchPets"


@pytest.mark.asyncio
async def test_native_tables_graphql_filters_non_list():
    state = MagicMock()
    state.graphql_remote_sources = {
        "src": {"url": "http://example.com/graphql", "auth": None}
    }
    schema = {
        "queryType": {"name": "Query"},
        "types": [
            {
                "name": "Query",
                "fields": [
                    {"name": "pets", "description": "All pets", "type": {"kind": "LIST", "ofType": {"kind": "OBJECT", "name": "Pet"}}},
                    {"name": "pet", "description": "One pet", "type": {"kind": "OBJECT", "name": "Pet"}},
                ],
            }
        ],
    }
    with patch("provisa.graphql_remote.introspect.introspect_schema", new=AsyncMock(return_value=schema)):
        result = await native_tables("src", "graphql", "default", _empty_pool("src"), None, state)

    assert len(result) == 1
    assert result[0].name == "pets"


@pytest.mark.asyncio
async def test_native_tables_grpc_streaming_only():
    state = MagicMock()
    state.grpc_remote_sources = {
        "src": {
            "proto_text": "syntax = 'proto3';"
        }
    }
    proto_dict = {
        "messages": {
            "PetResponse": [{"name": "id", "repeated": False}],
            "PetListResponse": [{"name": "pets", "repeated": True}],
        },
        "services": [
            {
                "methods": [
                    {"name": "StreamPets", "server_streaming": True, "output_type": "PetResponse"},
                    {"name": "GetPet", "server_streaming": False, "output_type": "PetResponse"},
                    {"name": "ListPets", "server_streaming": False, "output_type": "PetListResponse"},
                ]
            }
        ],
    }
    with patch("provisa.grpc_remote.loader.parse_proto_text", return_value=proto_dict):
        result = await native_tables("src", "grpc", "default", _empty_pool("src"), None, state)

    names = [t.name for t in result]
    assert "StreamPets" in names   # server-streaming
    assert "ListPets" in names     # repeated field in response
    assert "GetPet" not in names   # single object, no repeated
