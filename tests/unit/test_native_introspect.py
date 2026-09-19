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
    native_columns,
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


def _no_conn() -> MagicMock:
    """A stand-in ``Connection`` for a branch that never touches it (unused by the source type
    under test) — keeps the mock's static type matching the real (required) parameter.

    Plain ``AsyncMock()`` makes every attribute access an ``AsyncMock`` too, so an unconfigured
    ``config_conn.execute_core(...).fetchone()`` (kafka's branch: sync ``fetchone`` on the awaited
    result) returns an un-awaited coroutine instead of a value — pin ``execute_core`` to the real
    shape (async call, sync ``Result``) instead."""
    conn = MagicMock()
    conn.execute_core = AsyncMock(return_value=MagicMock())
    return conn


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
    result = await native_schemas("src", "graphql", _empty_pool(), _no_conn())
    assert result == ["graphql"]


@pytest.mark.asyncio
async def test_native_schemas_graphql_remote():
    result = await native_schemas("src", "graphql_remote", _empty_pool(), _no_conn())
    assert result == ["graphql"]


@pytest.mark.asyncio
async def test_native_schemas_grpc():
    result = await native_schemas("src", "grpc", _empty_pool(), _no_conn())
    assert result == ["grpc"]


@pytest.mark.asyncio
async def test_native_schemas_grpc_remote():
    result = await native_schemas("src", "grpc_remote", _empty_pool(), _no_conn())
    assert result == ["grpc"]


@pytest.mark.asyncio
async def test_native_schemas_kafka():
    result = await native_schemas("src", "kafka", _empty_pool(), _no_conn())
    assert result == ["kafka"]


@pytest.mark.asyncio
async def test_native_schemas_neo4j():
    result = await native_schemas("src", "neo4j", _empty_pool(), _no_conn())
    assert result == ["neo4j"]


@pytest.mark.asyncio
async def test_native_schemas_sparql():
    result = await native_schemas("src", "sparql", _empty_pool(), _no_conn())
    assert result == ["sparql"]


@pytest.mark.asyncio
async def test_native_schemas_openapi():
    result = await native_schemas("src", "openapi", _empty_pool(), _no_conn())
    assert result == ["openapi"]


@pytest.mark.asyncio
async def test_native_schemas_postgresql():
    pool = _pool([("pet_store",)])
    result = await native_schemas("src", "postgresql", pool, _no_conn())
    assert result == ["pet_store"]


@pytest.mark.asyncio
async def test_native_schemas_no_driver_returns_none():
    result = await native_schemas("src", "postgresql", _empty_pool(), _no_conn())
    assert result is None


@pytest.mark.asyncio
async def test_native_schemas_mysql():
    pool = _pool([("mydb",), ("information_schema",), ("mysql",), ("sys",)])
    result = await native_schemas("src", "mysql", pool, _no_conn())
    assert result == ["mydb"]


@pytest.mark.asyncio
async def test_native_schemas_unknown_type_returns_none():
    result = await native_schemas("src", "bigquery", _empty_pool(), _no_conn())
    assert result is None


# ── trino-as-a-source (REQ-1732) ───────────────────────────────────────────────
# trino-as-a-source has a real DIRECT driver (SourcePool via executor/drivers/registry.py's
# _make_trino) but no engine attaches it live except another Trino, so these are the ONLY
# discovery/column-resolution path when it's registered under a different active engine (e.g.
# DuckDB) — see native_columns's own docstring for the full story.


@pytest.mark.asyncio
async def test_native_schemas_trino():
    pool = _pool([("tiny",), ("information_schema",)])
    result = await native_schemas("src", "trino", pool, _no_conn())
    assert result == ["tiny"]


@pytest.mark.asyncio
async def test_native_schemas_trino_no_driver_returns_none():
    result = await native_schemas("src", "trino", _empty_pool(), _no_conn())
    assert result is None


@pytest.mark.asyncio
async def test_native_tables_trino():
    pool = _pool([("nation",), ("orders",)])
    result = await native_tables("src", "trino", "tiny", pool, _no_conn(), None)
    assert result is not None
    assert [t.name for t in result] == ["nation", "orders"]


@pytest.mark.asyncio
async def test_native_columns_trino():
    pool = _pool([("nationkey", "bigint"), ("name", "varchar(25)")])
    result = await native_columns("src", "trino", "tiny", "nation", pool)
    assert result == [("nationkey", "bigint"), ("name", "varchar(25)")]


@pytest.mark.asyncio
async def test_native_columns_non_trino_returns_none():
    """Every other RDBMS type is ATTACH-mechanism on whatever engine it's normally registered
    under, so resolve_available_columns_metadata's existing engine-catalog fallback already
    covers it — native_columns must not intercept those."""
    result = await native_columns("src", "postgresql", "public", "widgets", _pool([]))
    assert result is None


@pytest.mark.asyncio
async def test_native_columns_trino_no_driver_returns_none():
    result = await native_columns("src", "trino", "tiny", "nation", _empty_pool())
    assert result is None


# ── mysql/mariadb-as-a-source under an engine with no ATTACH connector (REQ-1732) ─────────────
# mysql/mariadb are normally registered under Trino (a real JDBC connector attaches them live),
# but neither has a DuckDB connector at all (connector_duckdb.py has none) — the same "no engine
# attaches it live" gap trino has when registered under DuckDB.


@pytest.mark.asyncio
async def test_native_tables_mysql_uses_percent_s_placeholder():
    """Regression: this branch used to pass a literal `?` placeholder, which aiomysql does not
    support (it expects `%s`) — raised inside pymysql's own escaping, silently caught by this
    function's `except Exception: return None` and never surfaced as an error, just an empty
    picker. Asserting the query text catches a regression without a live mysql server."""
    pool = _pool([("widgets", None)])
    result = await native_tables("src", "mysql", "verify_db", pool, _no_conn(), None)
    assert result is not None
    assert [t.name for t in result] == ["widgets"]
    _, query, params = pool.execute.call_args[0]
    assert "%s" in query and "?" not in query
    assert params == ["verify_db"]


@pytest.mark.asyncio
async def test_native_columns_mysql():
    pool = _pool([("id", "int"), ("name", "varchar")])
    result = await native_columns("src", "mysql", "verify_db", "widgets", pool)
    assert result == [("id", "int"), ("name", "varchar")]
    _, query, params = pool.execute.call_args[0]
    assert "%s" in query and "?" not in query
    assert params == ["verify_db", "widgets"]


@pytest.mark.asyncio
async def test_native_columns_mariadb():
    pool = _pool([("id", "int")])
    result = await native_columns("src", "mariadb", "verify_db", "widgets", pool)
    assert result == [("id", "int")]


@pytest.mark.asyncio
async def test_native_columns_mysql_no_driver_returns_none():
    result = await native_columns("src", "mysql", "verify_db", "widgets", _empty_pool())
    assert result is None


# ── tidb-as-a-source (REQ-1671 e2e fanout regression) ──────────────────────────────────────────
# tidb speaks the identical MySQL wire protocol as mysql/mariadb (REQ-950) but was missing from
# all three mysql/mariadb dispatch tuples in this module — native_schemas/native_tables/
# native_columns all silently `return None` for it, and available_schemas' engine-catalog
# fallback then queries a catalog that does not exist pre-registration, swallowed by
# discovery_fallback into an empty list: the Register Table schema/table/column pickers stayed
# permanently empty for a tidb source with no error ever surfacing.


@pytest.mark.asyncio
async def test_native_schemas_tidb():
    pool = _pool([("mydb",), ("information_schema",), ("mysql",), ("sys",)])
    result = await native_schemas("src", "tidb", pool, _no_conn())
    assert result == ["mydb"]


@pytest.mark.asyncio
async def test_native_tables_tidb_uses_percent_s_placeholder():
    pool = _pool([("widgets", None)])
    result = await native_tables("src", "tidb", "verify_db", pool, _no_conn(), None)
    assert result is not None
    assert [t.name for t in result] == ["widgets"]
    _, query, params = pool.execute.call_args[0]
    assert "%s" in query and "?" not in query
    assert params == ["verify_db"]


@pytest.mark.asyncio
async def test_native_columns_tidb():
    pool = _pool([("id", "int"), ("name", "varchar")])
    result = await native_columns("src", "tidb", "verify_db", "widgets", pool)
    assert result == [("id", "int"), ("name", "varchar")]
    _, query, params = pool.execute.call_args[0]
    assert "%s" in query and "?" not in query
    assert params == ["verify_db", "widgets"]


# ── saphana-as-a-source (REQ-1753) ──────────────────────────────────────────────────────────────
# saphana was reachable only as the whole active engine (_RDB_KINDS) before REQ-1753 added it to
# registry.py's _SQLALCHEMY_FALLBACK; these three dispatch branches were the other half — without
# them a registered saphana source's schema/table/column pickers stayed empty forever, same class
# of bug as tidb/duckdb above (REQ-1749/1750).
@pytest.mark.asyncio
async def test_native_schemas_saphana_excludes_system_schemas_in_sql():
    # The exclusion runs in the SQL text (a NOT IN clause), not a Python-side filter — this pool
    # mock doesn't execute SQL, so it's given only what a real HANA's SYS.SCHEMAS would already
    # have filtered, and the assertion is on the generated query text, not a re-filtered result.
    pool = _pool([("SYSTEM",), ("MYAPP",)])
    result = await native_schemas("src", "saphana", pool, _no_conn())
    assert result == ["SYSTEM", "MYAPP"]
    _, query = pool.execute.call_args[0]
    assert "SYS.SCHEMAS" in query
    assert "'SYS'" in query and "'PUBLIC'" in query


@pytest.mark.asyncio
async def test_native_tables_saphana_uses_dollar_placeholder():
    pool = _pool([("WIDGETS", None)])
    result = await native_tables("src", "saphana", "SYSTEM", pool, _no_conn(), None)
    assert result is not None
    assert [t.name for t in result] == ["WIDGETS"]
    _, query, params = pool.execute.call_args[0]
    assert "$1" in query and "?" not in query and "%s" not in query
    assert params == ["SYSTEM"]


@pytest.mark.asyncio
async def test_native_columns_saphana():
    pool = _pool([("ID", "INTEGER"), ("NAME", "NVARCHAR")])
    result = await native_columns("src", "saphana", "SYSTEM", "WIDGETS", pool)
    assert result == [("ID", "INTEGER"), ("NAME", "NVARCHAR")]
    _, query, params = pool.execute.call_args[0]
    assert "$1" in query and "$2" in query
    assert params == ["SYSTEM", "WIDGETS"]


# ── duckdb-as-a-source's column picker (REQ-1749) ──────────────────────────────────────────────
# native_schemas/_native_tables_rdbms already had "duckdb" branches (REQ-1746) using the DIRECT
# pool connection, scoped to current_database() so a fresh attached file's system/temp catalogs
# don't leak in. native_columns had NO such branch — it fell through to the engine's own ATTACH
# seam, which is empty pre-registration (REQ-1673: the engine attaches a source only once a table
# on it is registered) — the Register Table form's column checkboxes never appeared.


@pytest.mark.asyncio
async def test_native_columns_duckdb_scoped_to_current_database():
    pool = _pool([("id", "int"), ("name", "varchar")])
    result = await native_columns("src", "duckdb", "main", "widgets", pool)
    assert result == [("id", "int"), ("name", "varchar")]
    _, query, params = pool.execute.call_args[0]
    assert "current_database()" in query
    assert params == ["main", "widgets"]


@pytest.mark.asyncio
async def test_native_schemas_sqlite_returns_main():
    # Regression: SQLite was falling through to Trino which returned internal PG
    # schemas (e.g. "pet_store", "analytics") — implementation-layer details unknown
    # to the user. SQLite's physical schema is "main"; return that directly.
    result = await native_schemas("src", "sqlite", _empty_pool(), _no_conn())
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
    _res = MagicMock()
    _res.fetchone.return_value = (str(db),)
    config_conn.execute_core = AsyncMock(return_value=_res)

    result = await native_tables("src", "sqlite", "main", _empty_pool(), config_conn, MagicMock())
    assert result is not None
    assert {t.name for t in result} == {"orders", "customers"}


@pytest.mark.asyncio
async def test_native_tables_sqlite_wrong_schema_returns_empty():
    result = await native_tables("src", "sqlite", "other", _empty_pool(), _no_conn(), MagicMock())
    assert result == []


# ── files (directory of CSVs) ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_native_schemas_files_returns_normalised_source_id():
    result = await native_schemas("e2e-northwind", "files", _empty_pool(), _no_conn())
    assert result == ["e2e_northwind"]


@pytest.mark.asyncio
async def test_native_tables_files_falls_through_to_engine_seam(tmp_path):  # REQ-1690
    """``native_tables`` must return ``None`` for ``files`` (no dispatch branch at all) so
    ``available_tables`` (schema_query.py) falls through to the generic engine seam — the
    Calcite pgwire ATTACH REQ-1690 describes. A hand-rolled ``directory.rglob("*.csv")`` branch
    used to live here (pre-REQ-1690), always returning a list and never ``None``: Register Table
    for a `files` source never once reached the ATTACH, regardless of how correct
    DuckDBFilesConnector/_files_operand were, so a directory of xlsx files always showed empty
    (or, if any .csv happened to share the directory, ONLY those) — confirmed live by the total
    absence of a `.aperio/debug-model-<schema>.json` for a freshly created `files` source
    (FileSchemaFactory.create() was never invoked)."""
    config_conn = AsyncMock()
    _res = MagicMock()
    _res.fetchone.return_value = (str(tmp_path / "**"),)
    config_conn.execute_core = AsyncMock(return_value=_res)

    result = await native_tables("src", "files", "src", _empty_pool(), config_conn, MagicMock())
    assert result is None


@pytest.mark.asyncio
async def test_native_tables_neo4j_returns_empty():
    result = await native_tables("src", "neo4j", "neo4j", _empty_pool(), _no_conn(), MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_sparql_returns_empty():
    result = await native_tables("src", "sparql", "sparql", _empty_pool(), _no_conn(), MagicMock())
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_no_driver_returns_none():
    result = await native_tables(
        "src", "postgresql", "public", _empty_pool(), _no_conn(), MagicMock()
    )
    assert result is None


@pytest.mark.asyncio
async def test_native_tables_postgresql():
    pool = _pool([("orders", "Customer orders"), ("pets", None)])
    result = await native_tables("src", "postgresql", "public", pool, _no_conn(), MagicMock())
    assert result is not None
    assert len(result) == 2
    assert result[0].name == "orders"
    assert result[0].comment == "Customer orders"
    assert result[1].name == "pets"
    assert result[1].comment is None


@pytest.mark.asyncio
async def test_native_tables_kafka():
    config_conn = AsyncMock()
    _res = MagicMock()
    _res.fetchall.return_value = [("orders",), ("events",)]
    config_conn.execute_core = AsyncMock(return_value=_res)
    result = await native_tables("src", "kafka", "kafka", _empty_pool(), config_conn, MagicMock())
    assert result is not None
    assert [t.name for t in result] == ["orders", "events"]


@pytest.mark.asyncio
async def test_native_tables_kafka_wrong_schema():
    result = await native_tables("src", "kafka", "other", _empty_pool(), _no_conn(), MagicMock())
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
        result = await native_tables("src", "openapi", "openapi", _empty_pool(), _no_conn(), state)

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
        result = await native_tables("src", "openapi", "openapi", _empty_pool(), _no_conn(), state)

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
        result = await native_tables("src", "grpc", "grpc", _empty_pool(), _no_conn(), state)

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
