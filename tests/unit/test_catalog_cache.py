# Copyright (c) 2026 Kenneth Stott
# Canary: 8c1d0f2a-6b4e-4a71-9f3c-2d5e7a9b1c04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-464: source catalog cache read/write, migrated to SQLAlchemy Core."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.core.schema_org import source_catalog_cache
from provisa.discovery.catalog_cache import (
    CachedTable,
    invalidate_source,
    read_cache,
    write_cache,
)


def _pool_with_conn(conn) -> MagicMock:
    """A pool whose acquire() yields the given (async) conn, and conn.transaction() is an async CM."""
    conn.transaction = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=False)
        )
    )
    pool = MagicMock()
    pool.acquire = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False)
        )
    )
    return pool


def _result(rows) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = [MagicMock(_mapping=r) for r in rows]
    return result


# ---------------------------------------------------------------------------
# schema (metadata-authoritative — no raw DDL string)
# ---------------------------------------------------------------------------


class TestSchema:
    def test_table_name(self):
        assert source_catalog_cache.name == "source_catalog_cache"

    def test_primary_key(self):
        pk = {c.name for c in source_catalog_cache.primary_key.columns}
        assert pk == {"source_id", "schema_name", "table_name"}


# ---------------------------------------------------------------------------
# read_cache
# ---------------------------------------------------------------------------


class TestReadCache:
    @pytest.mark.asyncio
    async def test_returns_none_on_empty(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock(return_value=_result([]))
        result = await read_cache(_pool_with_conn(conn), "src1", "public")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_cached_tables(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock(
            return_value=_result(
                [
                    {
                        "source_id": "src1",
                        "schema_name": "public",
                        "table_name": "orders",
                        "column_names": ["id", "amount"],
                        "comment": "All orders",
                        "indexed_at": None,
                    },
                    {
                        "source_id": "src1",
                        "schema_name": "public",
                        "table_name": "products",
                        "column_names": ["id", "sku"],
                        "comment": None,
                        "indexed_at": None,
                    },
                ]
            )
        )
        result = await read_cache(_pool_with_conn(conn), "src1", "public")
        assert result is not None
        assert len(result) == 2
        assert result[0].table_name == "orders"
        assert result[0].column_names == ["id", "amount"]
        assert result[0].comment == "All orders"
        assert result[1].comment is None

    @pytest.mark.asyncio
    async def test_filters_by_source_and_schema(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock(return_value=_result([]))
        await read_cache(_pool_with_conn(conn), "my-source", "sales")
        stmt = conn.execute_core.await_args.args[0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "my-source" in compiled
        assert "sales" in compiled


# ---------------------------------------------------------------------------
# write_cache
# ---------------------------------------------------------------------------


class TestWriteCache:
    @pytest.mark.asyncio
    async def test_no_op_on_empty(self):
        pool = MagicMock()
        await write_cache(pool, "src1", "public", [])
        assert pool.acquire.call_count == 0

    @pytest.mark.asyncio
    async def test_upserts_each_table(self):
        conn = AsyncMock()
        conn.upsert = AsyncMock()
        tables = [
            CachedTable(
                schema_name="public", table_name="orders", column_names=["id"], comment=None
            ),
            CachedTable(
                schema_name="public",
                table_name="products",
                column_names=["id", "sku"],
                comment="Products",
            ),
        ]
        await write_cache(_pool_with_conn(conn), "src1", "public", tables)
        assert conn.upsert.await_count == 2
        upserted = [c.args[1]["table_name"] for c in conn.upsert.await_args_list]
        assert upserted == ["orders", "products"]


# ---------------------------------------------------------------------------
# invalidate_source
# ---------------------------------------------------------------------------


class TestInvalidateSource:
    @pytest.mark.asyncio
    async def test_executes_delete(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock()
        await invalidate_source(_pool_with_conn(conn), "src-to-delete")
        conn.execute_core.assert_awaited_once()
        stmt = conn.execute_core.await_args.args[0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "DELETE" in compiled.upper()
        assert "src-to-delete" in compiled


def test_ensure_table_on_a_control_plane_without_schemas(tmp_path):
    """A SQLite control plane has one namespace. Its ``Database`` still carries the org's schema
    name; naming that schema to SQLite asks for a database that is not attached
    (``no such table: org_x.sqlite_master``) and the server does not start."""
    import asyncio

    import sqlalchemy as sa

    from provisa.core.database import Database
    from provisa.discovery.catalog_cache import ensure_table

    engine = sa.create_engine(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    db = Database(engine, name="org", search_path="org_acme")
    asyncio.run(ensure_table(db))
    asyncio.run(ensure_table(db))  # a second start finds it
    assert "source_catalog_cache" in sa.inspect(engine).get_table_names()
    engine.dispose()


# ---------------------------------------------------------------------------
# The cache is a state table: written and read through the org's state store
# ---------------------------------------------------------------------------


@pytest.fixture
def stores(tmp_path):
    """The org's model store and state store as the product hands them out: one database here,
    two handles, each refusing the other's tables (``core.store_sides``, REQ-1922)."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import metadata

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'org.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw)
    return Database(engine, "org-model", holds="model"), Database(engine, "state", holds="state")


@pytest.mark.asyncio
async def test_indexing_a_source_writes_its_catalog_where_the_search_reads_it(
    stores, monkeypatch, caplog
):
    """Indexing lists a source through the model store and keeps the result in the state store.
    Handed the model store for both, every write was refused -- `source_catalog_cache is a
    state table, use the org's tenant_db` -- logged, and the cache stayed empty for every
    source of every type."""
    import logging
    from types import SimpleNamespace

    from provisa.api.admin import introspect
    from provisa.api.admin.table_search_router import _candidates_from_cache
    from provisa.discovery.catalog_cache import index_source

    model_db, tenant_db = stores

    async def _schemas(source_id, source_type, pools, conn):
        return ["sec"]

    async def _tables(source_id, source_type, schema, pools, conn, state):
        return [SimpleNamespace(name="financial_facts", comment="facts")]

    async def _columns(source_id, source_type, schema, table, pools, conn):
        return [("cik", "varchar"), ("value", "double")]

    async def _attached(state, source_id):
        return None

    monkeypatch.setattr(introspect, "native_schemas", _schemas)
    monkeypatch.setattr(introspect, "native_tables", _tables)
    monkeypatch.setattr(introspect, "native_columns", _columns)
    monkeypatch.setattr(introspect, "unattached_source", _attached)
    state = SimpleNamespace(tenant_db=tenant_db, model_db=model_db)

    with caplog.at_level(logging.WARNING):
        await index_source("src", model_db, None, None, {"src": "govdata"}, state)

    assert "write failed" not in caplog.text
    found = await _candidates_from_cache("src", "sec", state)
    assert found is not None
    assert [(c.name, c.columns) for c in found] == [("financial_facts", ["cik", "value"])]


@pytest.mark.asyncio
async def test_invalidating_a_source_empties_its_catalog_in_the_state_store(stores):
    from provisa.discovery.catalog_cache import invalidate_source, read_cache, write_cache

    _, tenant_db = stores
    await write_cache(
        tenant_db,
        "src",
        "sec",
        [CachedTable(schema_name="sec", table_name="t", column_names=["a"], comment=None)],
    )
    assert await read_cache(tenant_db, "src", "sec")
    await invalidate_source(tenant_db, "src")
    assert await read_cache(tenant_db, "src", "sec") is None
