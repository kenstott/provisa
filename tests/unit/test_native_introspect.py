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
)
from provisa.executor.result import QueryResult
from provisa.openapi.mapper import parse_spec


# ── helpers ──────────────────────────────────────────────────────────────────


def _pool(rows: list[tuple]) -> MagicMock:
    """Mock SourcePool that returns given rows for execute()."""
    pool = MagicMock()
    pool.has.return_value = True
    pool.execute = AsyncMock(return_value=QueryResult(rows=rows, column_names=[]))
    return pool


def _empty_pool() -> MagicMock:
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
    assert _openapi_is_table(q) is True


def test_openapi_is_table_no_schema():
    q = MagicMock()
    q.response_schema = None
    assert _openapi_is_table(q) is False


def test_openapi_is_table_object_multiple_array_props():
    q = MagicMock()
    q.response_schema = {
        "type": "object",
        "properties": {
            "items": {"type": "array"},
            "related": {"type": "array"},
        },
    }
    assert _openapi_is_table(q) is True


# ── _gql_field_returns_list ───────────────────────────────────────────────────


def test_gql_field_returns_list_direct():
    field = {"name": "pets", "type": {"kind": "LIST", "ofType": {"kind": "OBJECT", "name": "Pet"}}}
    assert _gql_field_returns_list(field) is True


def test_gql_field_returns_list_nonnull_wrapped():
    field = {
        "name": "pets",
        "type": {
            "kind": "NON_NULL",
            "ofType": {"kind": "LIST", "ofType": {"kind": "OBJECT", "name": "Pet"}},
        },
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
    result = await native_schemas("src", "graphql", _empty_pool(), None)
    assert result == ["graphql"]


@pytest.mark.asyncio
async def test_native_schemas_graphql_remote():
    result = await native_schemas("src", "graphql_remote", _empty_pool(), None)
    assert result == ["graphql"]


@pytest.mark.asyncio
async def test_native_schemas_grpc():
    result = await native_schemas("src", "grpc", _empty_pool(), None)
    assert result == ["grpc"]


@pytest.mark.asyncio
async def test_native_schemas_grpc_remote():
    result = await native_schemas("src", "grpc_remote", _empty_pool(), None)
    assert result == ["grpc"]


@pytest.mark.asyncio
async def test_native_schemas_kafka():
    result = await native_schemas("src", "kafka", _empty_pool(), None)
    assert result == ["kafka"]


@pytest.mark.asyncio
async def test_native_schemas_neo4j():
    result = await native_schemas("src", "neo4j", _empty_pool(), None)
    assert result == ["neo4j"]


@pytest.mark.asyncio
async def test_native_schemas_sparql():
    result = await native_schemas("src", "sparql", _empty_pool(), None)
    assert result == ["sparql"]


@pytest.mark.asyncio
async def test_native_schemas_openapi():
    result = await native_schemas("src", "openapi", _empty_pool(), None)
    assert result == ["openapi"]


@pytest.mark.asyncio
async def test_native_schemas_postgresql():
    pool = _pool([("pet_store",)])
    result = await native_schemas("src", "postgresql", pool, None)
    assert result == ["pet_store"]


@pytest.mark.asyncio
async def test_native_schemas_no_driver_returns_none():
    result = await native_schemas("src", "postgresql", _empty_pool(), None)
    assert result is None


@pytest.mark.asyncio
async def test_native_schemas_mysql():
    pool = _pool([("mydb",), ("information_schema",), ("mysql",), ("sys",)])
    result = await native_schemas("src", "mysql", pool, None)
    assert result == ["mydb"]


@pytest.mark.asyncio
async def test_native_schemas_unknown_type_returns_none():
    result = await native_schemas("src", "bigquery", _empty_pool(), None)
    assert result is None


@pytest.mark.asyncio
async def test_native_schemas_sqlite_returns_main():
    # Regression: SQLite was falling through to Trino which returned internal PG
    # schemas (e.g. "pet_store", "analytics") — implementation-layer details unknown
    # to the user. SQLite's physical schema is "main"; return that directly.
    result = await native_schemas("src", "sqlite", _empty_pool(), None)
    assert result == ["main"]


# ── native_tables ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_native_tables_sqlite_reads_file(tmp_path):
    # Regression: SQLite fell through to Trino fallback (schema "main" not in Trino)
    # returning empty list. Must read physical .db file directly.
    import sqlite3 as _sqlite3

    db = tmp_path / "test.db"
    sq = _sqlite3.connect(str(db))
    sq.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
    sq.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
    sq.commit()
    sq.close()

    config_conn = AsyncMock()
    config_conn.fetchrow = AsyncMock(return_value={"path": str(db)})

    result = await native_tables("src", "sqlite", "main", _empty_pool(), config_conn, MagicMock())
    assert result is not None
    assert {t.name for t in result} == {"orders", "customers"}


@pytest.mark.asyncio
async def test_native_tables_sqlite_wrong_schema_returns_empty():
    result = await native_tables("src", "sqlite", "other", _empty_pool(), None, MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_neo4j_returns_empty():
    result = await native_tables("src", "neo4j", "neo4j", _empty_pool(), None, MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_sparql_returns_empty():
    result = await native_tables("src", "sparql", "sparql", _empty_pool(), None, MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_no_driver_returns_none():
    result = await native_tables("src", "postgresql", "public", _empty_pool(), None, MagicMock())
    assert result is None


@pytest.mark.asyncio
async def test_native_tables_postgresql():
    pool = _pool([("orders", "Customer orders"), ("pets", None)])
    result = await native_tables("src", "postgresql", "public", pool, None, MagicMock())
    assert result is not None
    assert len(result) == 2
    assert result[0].name == "orders"
    assert result[0].comment == "Customer orders"
    assert result[1].name == "pets"
    assert result[1].comment is None


@pytest.mark.asyncio
async def test_native_tables_kafka():
    config_conn = AsyncMock()
    config_conn.fetch = AsyncMock(return_value=[{"topic": "orders"}, {"topic": "events"}])
    result = await native_tables("src", "kafka", "kafka", _empty_pool(), config_conn, MagicMock())
    assert result is not None
    assert [t.name for t in result] == ["orders", "events"]


@pytest.mark.asyncio
async def test_native_tables_kafka_wrong_schema():
    result = await native_tables("src", "kafka", "other", _empty_pool(), None, MagicMock())
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
        result = await native_tables("src", "openapi", "openapi", _empty_pool(), None, state)

    assert result is not None
    assert len(result) == 2
    assert {r.name for r in result} == {"listPets", "getPetById"}


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
        result = await native_tables("src", "openapi", "openapi", _empty_pool(), None, state)

    assert result is not None
    assert len(result) == 1
    assert result[0].name == "searchPets"


@pytest.mark.asyncio
async def test_native_tables_graphql_filters_non_list():
    state = MagicMock()
    state.graphql_remote_sources = {"src": {"url": "http://example.com/graphql", "auth": None}}
    config_conn = AsyncMock()
    schema = {
        "queryType": {"name": "Query"},
        "types": [
            {
                "name": "Query",
                "fields": [
                    {
                        "name": "pets",
                        "description": "All pets",
                        "type": {"kind": "LIST", "ofType": {"kind": "OBJECT", "name": "Pet"}},
                    },
                    {
                        "name": "pet",
                        "description": "One pet",
                        "type": {"kind": "OBJECT", "name": "Pet"},
                    },
                ],
            }
        ],
    }
    with patch(
        "provisa.graphql_remote.introspect.introspect_schema", new=AsyncMock(return_value=schema)
    ):
        result = await native_tables("src", "graphql", "graphql", _empty_pool(), config_conn, state)

    assert result is not None
    assert len(result) == 1
    assert result[0].name == "pets"


@pytest.mark.asyncio
async def test_native_tables_grpc_streaming_only():
    state = MagicMock()
    state.grpc_remote_sources = {"src": {"proto_text": "syntax = 'proto3';"}}
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
                    {
                        "name": "ListPets",
                        "server_streaming": False,
                        "output_type": "PetListResponse",
                    },
                ]
            }
        ],
    }
    with patch("provisa.grpc_remote.loader.parse_proto_text", return_value=proto_dict):
        result = await native_tables("src", "grpc", "grpc", _empty_pool(), None, state)

    assert result is not None
    names = [t.name for t in result]
    assert "StreamPets" in names
    assert "ListPets" in names
    assert "GetPet" not in names


# ── _openapi_is_table via parse_spec (petstore regression) ───────────────────

_PETSTORE_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Petstore", "version": "1.0.0"},
    "components": {
        "schemas": {
            "Pet": {
                "type": "object",
                "required": ["name", "photoUrls"],
                "properties": {
                    "id": {"type": "integer", "format": "int64"},
                    "name": {"type": "string"},
                    "status": {"type": "string"},
                },
            }
        }
    },
    "paths": {
        "/pet/findByStatus": {
            "get": {
                "operationId": "findPetsByStatus",
                "summary": "Finds Pets by status",
                "parameters": [
                    {
                        "name": "status",
                        "in": "query",
                        "schema": {"type": "string"},
                    }
                ],
                "responses": {
                    "200": {
                        "description": "successful operation",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Pet"},
                                }
                            }
                        },
                    }
                },
            }
        },
        "/pet/{petId}": {
            "get": {
                "operationId": "getPetById",
                "summary": "Find pet by ID",
                "parameters": [
                    {
                        "name": "petId",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer", "format": "int64"},
                    }
                ],
                "responses": {
                    "200": {
                        "description": "successful operation",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Pet"}}
                        },
                    }
                },
            }
        },
        "/user/logout": {
            "get": {
                "operationId": "logoutUser",
                "summary": "Logs out current logged in user session",
                "parameters": [],
                "responses": {"default": {"description": "successful operation"}},
            }
        },
    },
}


def _query_by_id(queries, operation_id):
    return next(q for q in queries if q.operation_id == operation_id)


def test_openapi_is_table_petstore_array_response():
    queries, _ = parse_spec(_PETSTORE_SPEC)
    q = _query_by_id(queries, "findPetsByStatus")
    assert _openapi_is_table(q) is True


def test_openapi_is_table_petstore_single_object_response():
    queries, _ = parse_spec(_PETSTORE_SPEC)
    q = _query_by_id(queries, "getPetById")
    assert _openapi_is_table(q) is True


def test_openapi_is_table_petstore_null_response():
    queries, _ = parse_spec(_PETSTORE_SPEC)
    q = _query_by_id(queries, "logoutUser")
    assert _openapi_is_table(q) is False
