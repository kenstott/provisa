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
    q.columns_declared = False
    assert _openapi_is_table(q) is False


def test_openapi_is_table_columns_declared():
    """REQ-1924 (amended): no response schema, its columns declared at registration."""
    q = MagicMock()
    q.response_schema = None
    q.columns_declared = True
    assert _openapi_is_table(q) is True


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
    # The schema a gRPC source's tables register under (REQ-322), so a picked table is
    # registered where its readers look for it.
    assert result == ["grpc_remote"]


@pytest.mark.asyncio
async def test_native_schemas_grpc_remote():
    result = await native_schemas("src", "grpc_remote", _empty_pool(), _no_conn())
    assert result == ["grpc_remote"]


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
async def test_native_columns_lists_postgresql_through_its_own_driver():
    """REQ-1912: a source's catalog is listed through the source's own driver. A floored
    postgresql source has no live attach on any engine (no Trino catalog), so the engine-catalog
    path cannot be what lists it; the driver's information_schema does, in the IR vocabulary."""
    # A catalog row: name, information_schema's data_type and udt_name, then pg_catalog's
    # format_type and typtype (REQ-1965).
    pool = _pool(
        [
            ("id", "integer", "int4", "integer", "b"),
            ("label", "character varying", "varchar", "character varying(40)", "b"),
        ]
    )
    result = await native_columns("src", "postgresql", "public", "widgets", pool)
    assert result == [("id", "integer"), ("label", "text")]


@pytest.mark.asyncio
async def test_native_columns_of_a_type_with_no_driver_listing_returns_none():
    """A type this dispatch has no driver listing for answers None; the caller then asks the
    engine's catalog, when the engine holds a live attach of the source."""
    result = await native_columns("src", "mongodb", "db", "widgets", _pool([]))
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
async def test_native_tables_mysql_uses_the_drivers_dollar_placeholder():
    """Regression: MySQLDriver.execute binds `$N` and escapes every literal `%`, so a `?` or a
    `%s` written here is a literal and PyMySQL's formatting raises. Asserting the query text
    catches a regression without a live mysql server; test_mysql_discovery_binds_through_its_driver
    runs the real driver's binding."""
    pool = _pool([("widgets", None)])
    result = await native_tables("src", "mysql", "verify_db", pool, _no_conn(), None)
    assert result is not None
    assert [t.name for t in result] == ["widgets"]
    _, query, params = pool.execute.call_args[0]
    assert "$1" in query and "%s" not in query and "?" not in query
    assert params == ["verify_db"]


@pytest.mark.asyncio
async def test_native_columns_mysql():
    pool = _pool([("id", "int"), ("name", "varchar")])
    result = await native_columns("src", "mysql", "verify_db", "widgets", pool)
    assert result == [("id", "int"), ("name", "varchar")]
    _, query, params = pool.execute.call_args[0]
    assert "$1" in query and "$2" in query and "%s" not in query and "?" not in query
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
async def test_native_tables_tidb_uses_the_drivers_dollar_placeholder():
    pool = _pool([("widgets", None)])
    result = await native_tables("src", "tidb", "verify_db", pool, _no_conn(), None)
    assert result is not None
    assert [t.name for t in result] == ["widgets"]
    _, query, params = pool.execute.call_args[0]
    assert "$1" in query and "%s" not in query and "?" not in query
    assert params == ["verify_db"]


@pytest.mark.asyncio
async def test_native_columns_tidb():
    pool = _pool([("id", "int"), ("name", "varchar")])
    result = await native_columns("src", "tidb", "verify_db", "widgets", pool)
    assert result == [("id", "int"), ("name", "varchar")]
    _, query, params = pool.execute.call_args[0]
    assert "$1" in query and "$2" in query and "%s" not in query and "?" not in query
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
async def test_native_tables_files_is_listed_by_its_connector_server(
    tmp_path, monkeypatch
):  # REQ-1690
    """A ``files`` source's tables are what its bundled Calcite server serves -- never a hand-rolled
    ``directory.rglob("*.csv")``. One used to live here (pre-REQ-1690), always returning a list:
    Register Table for a `files` source never once reached the server, so a directory of xlsx
    files always showed empty (or, if any .csv happened to share the directory, ONLY those) --
    confirmed live by the total absence of a `.aperio/debug-model-<schema>.json` for a freshly
    created `files` source (FileSchemaFactory.create() was never invoked). The server is asked
    through its information_schema as soon as it listens, not through the engine's attach of it,
    which waits for the server's first catalog query (tests/unit/test_files_tables_listed_by_server.py)."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from provisa.api.admin import introspect
    from provisa.federation import pgwire_replica
    from provisa.federation.engine import build_duckdb_engine

    (tmp_path / "loose.csv").write_text("a\n1\n")  # a file a filesystem scan would have listed
    asked: list[str] = []

    async def _source(source_id):
        return SimpleNamespace(id=source_id, type="files")

    @asynccontextmanager
    async def _bound():
        yield

    async def _tables(source, schema):
        asked.append(schema)
        return ["Orders"]  # what FileSchemaFactory serves, xlsx sheets included

    monkeypatch.setattr(introspect, "_adapter_source", _source)
    monkeypatch.setattr(introspect, "_adapter_bound", _bound)
    monkeypatch.setattr(pgwire_replica, "adapter_tables", _tables)
    config_conn = AsyncMock()
    state = SimpleNamespace(federation_engine=SimpleNamespace(engine=build_duckdb_engine()))

    result = await native_tables("src", "files", "src", _empty_pool(), config_conn, state)
    assert [t.name for t in result] == ["Orders"]
    assert asked == ["src"]
    config_conn.execute_core.assert_not_called()  # no glob read off the source row to scan


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
async def test_native_tables_graphql_offers_every_table_the_schema_maps():
    """REQ-308 (amended 2026-10-02): the picker lists what can be registered -- every table the
    schema maps to, a single-object field included (it registers as a table whose required
    argument is its filter), under the name it registers with."""
    state = MagicMock()
    state.graphql_remote_sources = {
        "src": {
            "source_id": "src",
            "url": "http://example.com/graphql",
            "auth": None,
            "namespace": "shop",
        }
    }
    config_conn = AsyncMock()
    schema = {
        "queryType": {"name": "Query"},
        "types": [
            {
                "kind": "OBJECT",
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
                    {"name": "petCount", "type": {"kind": "SCALAR", "name": "Int"}},
                ],
            }
        ],
    }
    with patch(
        "provisa.graphql_remote.introspect.introspect_schema", new=AsyncMock(return_value=schema)
    ):
        result = await native_tables("src", "graphql", "graphql", _empty_pool(), config_conn, state)

    assert result is not None
    assert {t.name: t.comment for t in result} == {
        "shop__pets": "All pets",
        "shop__pet": "One pet",
    }  # petCount returns a scalar: a function, not a table


@pytest.mark.asyncio
async def test_native_tables_graphql_source_this_process_does_not_hold_offers_nothing():
    state = MagicMock()
    state.graphql_remote_sources = {}
    result = await native_tables("src", "graphql", "graphql", _empty_pool(), AsyncMock(), state)
    assert result == []


@pytest.mark.asyncio
async def test_native_tables_grpc_offers_every_query_method_by_its_registered_name():
    """REQ-322 (amended 2026-10-02): the picker lists what can be registered -- one table per
    query method of the proto, under the name it registers with and the schema it registers
    in."""
    from provisa.grpc_remote.mapper import GrpcQuery

    def _query(method: str) -> GrpcQuery:
        return GrpcQuery(
            service="PetService",
            method=method,
            full_method_path=f"/pets.PetService/{method}",
            input_message="Req",
            output_message="Res",
        )

    state = MagicMock()
    state.grpc_remote_sources = {
        "src": {"namespace": "pets", "queries": [_query("StreamPets"), _query("ListPets")]}
    }
    result = await native_tables("src", "grpc", "grpc_remote", _empty_pool(), _no_conn(), state)
    assert result is not None
    assert [t.name for t in result] == [
        "pets__PetService__StreamPets",
        "pets__PetService__ListPets",
    ]
    # The schema the tables do not register in offers nothing.
    assert await native_tables("src", "grpc", "grpc", _empty_pool(), _no_conn(), state) == []


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


def test_a_petstore_get_with_no_response_schema_is_a_command_and_a_declared_table():
    """REQ-1924 (amended): a command that reads, and a table whose columns are declared."""
    queries, commands = parse_spec(_PETSTORE_SPEC)
    assert _query_by_id(queries, "logoutUser").columns_declared is True
    assert _query_by_id(commands, "logoutUser").reads is True


@pytest.mark.parametrize("source_type", ["cockroachdb", "yugabytedb", "greenplum", "redshift"])
async def test_native_columns_pg_wire_family_reports_ir_types(source_type):
    """information_schema's ANSI spellings resolve through the IR table (provisa.core.ir_types) —
    every downstream type map (GraphQL, proto) keys on canonical names, not "character varying"."""
    # A catalog row: name, data_type, udt_name, then format_type and typtype where the kind is a
    # PostgreSQL fork whose pg_catalog is read (REQ-1965).
    pool = _pool(
        [
            ("id", "integer", "int4", "integer", "b"),
            ("name", "character varying", "varchar", "character varying", "b"),
            ("price", "double precision", "float8", "double precision", "b"),
            ("seen_at", "timestamp without time zone", "timestamp", None, None),
            ("opens", "time without time zone", "time", None, None),
        ]
    )
    result = await native_columns("src", source_type, "public", "widgets", pool)
    assert result == [
        ("id", "integer"),
        ("name", "text"),
        ("price", "double"),
        ("seen_at", "timestamp"),
        ("opens", "time"),
    ]


async def test_redshift_lists_its_tables_without_casting_text_to_regclass():
    """Redshift has no text-to-regclass cast ("cannot cast type text to regclass", u7spec-kaggle
    40184207167), so a listing that resolved each table's comment by casting its name left the
    Register Table picker empty. Its tables and their comments are read from pg_catalog, which
    Redshift keeps (pg_class, pg_namespace, pg_description)."""
    from types import SimpleNamespace

    from provisa.api.admin import introspect

    sent: list[tuple[str, list]] = []

    class _Redshift:
        def has(self, _sid):
            return True

        async def execute(self, _sid, sql, params=None):
            if "regclass" in sql:
                raise RuntimeError("cannot cast type text to regclass")
            sent.append((sql, params))
            return SimpleNamespace(rows=[("widgets", "the e2e's table")])

    tables = await introspect._native_tables_rdbms("rs", "redshift", "provisa_ui_e2e", _Redshift())
    assert [(t.name, t.comment) for t in tables] == [("widgets", "the e2e's table")]
    ((sql, params),) = sent
    assert "pg_description" in sql and params == ["provisa_ui_e2e"]


class _MssqlCatalog:
    """INFORMATION_SCHEMA.TABLES of a warehouse holding one view, widgets, as a Synapse
    serverless pool holds its relations (it has no tables of its own, only views and external
    tables): answers each listing with the relations of the types it names."""

    def __init__(self):
        self.relations = [("widgets", "VIEW")]

    def has(self, _sid):
        return True

    async def execute(self, _sid, sql, params=None):
        from types import SimpleNamespace

        rows = [(name, None) for name, kind in self.relations if f"'{kind}'" in sql]
        return SimpleNamespace(rows=rows)


async def test_a_synapse_view_is_offered_as_a_table():
    """A Synapse serverless pool holds only views and external tables, so a listing of base tables
    alone offered nothing to register (u7spec-kaggle 40184207167: the e2e's widgets view)."""
    from provisa.api.admin import introspect

    tables = await introspect.native_tables(
        "syn", "synapse", "provisa_ui_e2e", _MssqlCatalog(), None, None
    )
    assert [t.name for t in tables] == ["widgets"]


async def test_fabric_still_offers_only_its_base_tables():
    from provisa.api.admin import introspect

    tables = await introspect.native_tables(
        "fab", "fabric", "provisa_ui_e2e", _MssqlCatalog(), None, None
    )
    assert tables == []
