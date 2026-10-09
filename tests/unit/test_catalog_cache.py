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


# ---------------------------------------------------------------------------
# A source the engine attaches is listed through the attach seam, as the form lists it
# ---------------------------------------------------------------------------


class _AttachingEngine:
    """An engine that lists an attached source through its attach seam (the Register Table
    form's path) and holds NO catalog named after the source: asked for one in SQL, it answers
    as DuckDB did -- `Binder Error: Catalog "test" does not exist!`."""

    def __init__(self, tables_of, *, starting_for: int = 0) -> None:
        self._tables_of = tables_of
        self._starting_for = starting_for
        self.seam_calls: list[str] = []
        self.statements: list[str] = []

    def introspect_tables(self, source, schema_name):
        self.seam_calls.append(schema_name)
        if self._starting_for > 0:
            self._starting_for -= 1
            from provisa.federation.pgwire_replica import SourceStillStartingError

            raise SourceStillStartingError(source.id)
        return self._tables_of(schema_name)

    async def execute_engine(self, sql, **kwargs):
        self.statements.append(sql)
        raise RuntimeError('Binder Error: Catalog "test" does not exist!')


class _no_binding:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _attached_source_arranged(monkeypatch, schemas):
    from types import SimpleNamespace

    from provisa.api.admin import introspect, schema_query
    from provisa.discovery import catalog_cache

    async def _schemas(source_id, source_type, pools, conn):
        return list(schemas)

    async def _no_native(*args):
        return None  # the driver lists nothing: the engine's attach does

    async def _attached(state, source_id):
        return None

    async def _source(source_id):
        return SimpleNamespace(id=source_id)

    monkeypatch.setattr(introspect, "native_schemas", _schemas)
    monkeypatch.setattr(introspect, "native_tables", _no_native)
    monkeypatch.setattr(introspect, "native_columns", _no_native)
    monkeypatch.setattr(introspect, "unattached_source", _attached)
    monkeypatch.setattr(schema_query, "_source_for_introspection", _source)
    # The seam runs inside the org's vault binding (the source's credential is a reference
    # into it); these stand-ins resolve none, so the binding is a no-op here.
    monkeypatch.setattr(catalog_cache, "_seam_bound", _no_binding, raising=False)


@pytest.mark.asyncio
async def test_an_attached_source_is_indexed_through_the_attach_seam(stores, monkeypatch, caplog):
    """The index asked the engine for `"<source>".information_schema.tables`. A native engine
    exposes an attached source as views of its registered tables, never as a catalog named after
    it, so every schema of an AskAmerica source logged `table list failed ... Catalog "test" does
    not exist!` and the source was never indexed."""
    import logging
    from types import SimpleNamespace

    from provisa.discovery.catalog_cache import index_source, read_cache

    model_db, tenant_db = stores
    _attached_source_arranged(monkeypatch, ["sec", "econ"])
    engine = _AttachingEngine(lambda schema: [f"{schema}_facts", f"{schema}_meta"])
    state = SimpleNamespace(tenant_db=tenant_db, catalog_for=lambda sid: sid)

    with caplog.at_level(logging.WARNING):
        await index_source("test", model_db, engine, None, {"test": "govdata"}, state)

    assert "table list failed" not in caplog.text
    assert engine.statements == []  # no catalog SQL for a source listed through its attach
    assert engine.seam_calls == ["sec", "econ"]
    sec = await read_cache(tenant_db, "test", "sec")
    assert sec is not None and [t.table_name for t in sec] == ["sec_facts", "sec_meta"]
    assert await read_cache(tenant_db, "test", "econ")


@pytest.mark.asyncio
async def test_a_source_that_is_still_starting_is_indexed_when_it_has_started(
    stores, monkeypatch, caplog
):
    """STARTING is "index later", not a failure: the index waits for the source's server and
    then lists it."""
    import logging
    from types import SimpleNamespace

    from provisa.discovery import catalog_cache

    model_db, tenant_db = stores
    _attached_source_arranged(monkeypatch, ["sec"])
    waits: list[float] = []

    async def _sleep(seconds):
        waits.append(seconds)

    monkeypatch.setattr(catalog_cache.asyncio, "sleep", _sleep)
    engine = _AttachingEngine(lambda schema: ["financial_facts"], starting_for=3)
    state = SimpleNamespace(tenant_db=tenant_db, catalog_for=lambda sid: sid)

    with caplog.at_level(logging.WARNING):
        await catalog_cache.index_source("test", model_db, engine, None, {"test": "govdata"}, state)

    assert waits == [catalog_cache.INDEX_STARTING_POLL_SECONDS] * 3
    assert caplog.text == ""  # nothing failed: nothing is warned
    found = await catalog_cache.read_cache(tenant_db, "test", "sec")
    assert found is not None and [t.table_name for t in found] == ["financial_facts"]


@pytest.mark.asyncio
async def test_a_source_that_never_starts_is_reported_once_by_name(stores, monkeypatch, caplog):
    import logging
    from types import SimpleNamespace

    from provisa.discovery import catalog_cache

    model_db, tenant_db = stores
    _attached_source_arranged(monkeypatch, ["sec", "econ"])

    async def _sleep(seconds):
        return None

    monkeypatch.setattr(catalog_cache.asyncio, "sleep", _sleep)
    monkeypatch.setattr(catalog_cache, "INDEX_STARTING_WAIT_SECONDS", 30)
    engine = _AttachingEngine(lambda schema: ["t"], starting_for=10_000)
    state = SimpleNamespace(tenant_db=tenant_db, catalog_for=lambda sid: sid)

    with caplog.at_level(logging.WARNING):
        await catalog_cache.index_source("test", model_db, engine, None, {"test": "govdata"}, state)

    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warnings) == 1, warnings
    assert "'test' is not indexed" in warnings[0] and "still starting" in warnings[0]
    assert engine.statements == []
    assert await catalog_cache.read_cache(tenant_db, "test", "sec") is None


@pytest.mark.asyncio
async def test_an_engine_with_no_attach_seam_is_still_asked_for_its_catalog(stores, monkeypatch):
    """A federator (Trino) lists a source through a catalog of its own: unchanged."""
    from types import SimpleNamespace

    from provisa.discovery.catalog_cache import index_source, read_cache

    model_db, tenant_db = stores
    _attached_source_arranged(monkeypatch, ["sec"])

    class _Federator:
        statements: list[str] = []

        def introspect_tables(self, source, schema_name):
            return None  # no seam on this engine

        async def execute_engine(self, sql, **kwargs):
            self.statements.append(sql)
            rows = [("financial_facts",)] if "information_schema.tables" in sql else [("cik",)]
            return SimpleNamespace(rows=rows)

    engine = _Federator()
    state = SimpleNamespace(tenant_db=tenant_db, catalog_for=lambda sid: "test_cat")
    await index_source("test", model_db, engine, None, {"test": "govdata"}, state)

    assert any('"test_cat".information_schema.tables' in s for s in engine.statements)
    found = await read_cache(tenant_db, "test", "sec")
    assert found is not None and [(t.table_name, t.column_names) for t in found] == [
        ("financial_facts", ["cik"])
    ]


# ---------------------------------------------------------------------------
# Column names for searching are loaded lazily (REQ-464)
# ---------------------------------------------------------------------------
#
# The index stays cheap: table names and comments. A schema's column names are fetched the first
# time a search needs them -- one query for the whole schema, through the source's adapter, off
# the request -- and kept in the cache from then on.


@pytest.fixture
def fills(monkeypatch):
    """Column fills run inline here instead of on a background worker, and are recorded."""
    from provisa.discovery import catalog_cache

    monkeypatch.setattr(catalog_cache, "_COLUMN_FILLS", {})
    started: list = []

    def _spawn(coro, *, name=None):
        started.append((name, coro))

    monkeypatch.setattr(catalog_cache, "_spawn_fill", _spawn)
    yield started
    for _, coro in started:
        coro.close()  # a fill a test started and did not run


async def _seeded(tenant_db, tables=("financial_facts", "filings")):
    from provisa.discovery.catalog_cache import write_cache

    await write_cache(
        tenant_db,
        "test",
        "sec",
        [
            CachedTable(schema_name="sec", table_name=t, column_names=[], comment=None)
            for t in tables
        ],
    )


def _columns_through_the_adapter(monkeypatch, answer):
    from types import SimpleNamespace

    from provisa.api.admin import schema_query
    from provisa.federation import pgwire_replica as pr

    asked: list[tuple[str, str]] = []

    async def _source(source_id):
        return SimpleNamespace(id=source_id)

    async def _columns(source, schema):
        asked.append((source.id, schema))
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(schema_query, "_source_for_introspection", _source)
    monkeypatch.setattr(pr, "schema_columns", _columns)
    return asked


@pytest.mark.asyncio
async def test_a_search_answers_with_what_is_known_and_the_columns_are_filled_off_the_request(
    stores, monkeypatch, fills
):
    from types import SimpleNamespace

    from provisa.discovery import catalog_cache

    _, tenant_db = stores
    await _seeded(tenant_db)
    asked = _columns_through_the_adapter(
        monkeypatch, {"financial_facts": ["cik", "value"], "filings": ["cik", "form"]}
    )
    state = SimpleNamespace(tenant_db=tenant_db)
    known = await catalog_cache.read_cache(tenant_db, "test", "sec")

    assert catalog_cache.request_column_fill("test", "govdata", "sec", known, state) is True
    assert asked == []  # nothing fetched on the request: the fill was handed off
    assert len(fills) == 1 and fills[0][0] == "catalog-columns:test/sec"

    await fills[0][1]  # the background work
    assert asked == [("test", "sec")]  # ONE query for the schema, not one per table
    filled = await catalog_cache.read_cache(tenant_db, "test", "sec")
    assert {t.table_name: t.column_names for t in filled} == {
        "financial_facts": ["cik", "value"],
        "filings": ["cik", "form"],
    }


@pytest.mark.asyncio
async def test_a_schema_is_filled_at_most_once_per_index_generation(stores, monkeypatch, fills):
    from types import SimpleNamespace

    from provisa.discovery import catalog_cache

    _, tenant_db = stores
    await _seeded(tenant_db)
    # A table the adapter lists no columns for stays without: that is not "unfilled".
    asked = _columns_through_the_adapter(monkeypatch, {"financial_facts": ["cik"]})
    state = SimpleNamespace(tenant_db=tenant_db)

    async def _search() -> bool:
        known = await catalog_cache.read_cache(tenant_db, "test", "sec")
        return catalog_cache.request_column_fill("test", "govdata", "sec", known, state)

    assert await _search() is True
    assert await _search() is False  # in flight: not started twice
    await fills[0][1]
    assert await _search() is False  # filled this generation, "filings" still has none
    assert asked == [("test", "sec")]

    await catalog_cache.invalidate_source(tenant_db, "test")  # a new generation
    await _seeded(tenant_db)
    assert await _search() is True


@pytest.mark.asyncio
async def test_a_source_that_is_still_starting_is_filled_by_a_later_search(
    stores, monkeypatch, fills, caplog
):
    import logging
    from types import SimpleNamespace

    from provisa.discovery import catalog_cache
    from provisa.federation.pgwire_replica import SourceStillStartingError

    _, tenant_db = stores
    await _seeded(tenant_db)
    _columns_through_the_adapter(monkeypatch, SourceStillStartingError("test"))
    state = SimpleNamespace(tenant_db=tenant_db)
    known = await catalog_cache.read_cache(tenant_db, "test", "sec")

    with caplog.at_level(logging.WARNING):
        assert catalog_cache.request_column_fill("test", "govdata", "sec", known, state) is True
        await fills[0][1]
    assert caplog.text == ""  # "later", not an error
    assert catalog_cache.request_column_fill("test", "govdata", "sec", known, state) is True


@pytest.mark.asyncio
async def test_a_fill_that_fails_is_reported_once_and_not_repeated_this_generation(
    stores, monkeypatch, fills, caplog
):
    import logging
    from types import SimpleNamespace

    from provisa.discovery import catalog_cache
    from provisa.federation.pgwire_replica import ServerExited

    _, tenant_db = stores
    await _seeded(tenant_db)
    _columns_through_the_adapter(monkeypatch, ServerExited("test", 1, "boom"))
    state = SimpleNamespace(tenant_db=tenant_db)
    known = await catalog_cache.read_cache(tenant_db, "test", "sec")

    with caplog.at_level(logging.WARNING):
        catalog_cache.request_column_fill("test", "govdata", "sec", known, state)
        await fills[0][1]
    assert "column names of 'test'/'sec' are not loaded" in caplog.text
    assert catalog_cache.request_column_fill("test", "govdata", "sec", known, state) is False


@pytest.mark.asyncio
async def test_nothing_is_fetched_when_the_columns_are_known_or_the_kind_is_not_lazy(
    stores, monkeypatch, fills
):
    from types import SimpleNamespace

    from provisa.discovery import catalog_cache

    state = SimpleNamespace(tenant_db=stores[1])
    known = [CachedTable(schema_name="sec", table_name="t", column_names=["a"], comment=None)]
    assert catalog_cache.request_column_fill("test", "govdata", "sec", known, state) is False
    bare = [CachedTable(schema_name="public", table_name="t", column_names=[], comment=None)]
    # A source listed by its own driver has its columns from the index, as before.
    assert catalog_cache.request_column_fill("pg", "postgresql", "public", bare, state) is False
    assert fills == []


@pytest.mark.asyncio
async def test_the_adapter_is_asked_for_a_schemas_columns_in_one_statement(monkeypatch):
    from types import SimpleNamespace

    from provisa.federation import pgwire_replica as pr

    statements: list[str] = []

    class _Conn:
        async def fetch(self, sql, *args, timeout=None):
            statements.append(sql)
            return [
                {"table_name": "filings", "column_name": "cik"},
                {"table_name": "filings", "column_name": "form"},
                {"table_name": "financial_facts", "column_name": "cik"},
            ]

        async def close(self):
            statements.append("closed")

    async def _connect(host, port):
        return _Conn()

    monkeypatch.setattr(
        pr, "ensure_endpoint_for_discovery", lambda source: pr.PortPair(5440, "127.0.0.1", 5540)
    )
    monkeypatch.setattr(pr, "_pg_connect", _connect)
    columns = await pr.schema_columns(SimpleNamespace(id="test"), "sec")

    assert columns == {"filings": ["cik", "form"], "financial_facts": ["cik"]}
    assert len(statements) == 2 and statements[1] == "closed"
    assert "information_schema.columns" in statements[0] and "table_schema = 'sec'" in statements[0]
    assert "ORDER BY table_name, ordinal_position" in statements[0]
    with pytest.raises(ValueError, match="not a schema name"):
        await pr.schema_columns(SimpleNamespace(id="test"), "sec'; DROP")


@pytest.mark.asyncio
async def test_the_index_fetches_no_column_of_an_attached_source(stores, monkeypatch):
    """The background index stays cheap: names only, no statement and no describe per table."""
    from types import SimpleNamespace

    from provisa.discovery.catalog_cache import index_source, read_cache
    from provisa.federation import pgwire_replica as pr

    model_db, tenant_db = stores
    _attached_source_arranged(monkeypatch, ["sec"])

    async def _never(source, schema):
        raise AssertionError("the index fetched columns")

    monkeypatch.setattr(pr, "schema_columns", _never)
    engine = _AttachingEngine(lambda schema: ["financial_facts"])
    state = SimpleNamespace(tenant_db=tenant_db, catalog_for=lambda sid: sid)
    await index_source("test", model_db, engine, None, {"test": "govdata"}, state)
    found = await read_cache(tenant_db, "test", "sec")
    assert found is not None and found[0].column_names == []
    assert engine.statements == []


@pytest.mark.asyncio
async def test_a_search_before_the_index_lists_an_attached_source_through_the_seam(monkeypatch):
    """The cache is cold (the index has not run): the request lists the schema's tables through
    the attach seam and fetches no column per table."""
    from types import SimpleNamespace

    from provisa.api.admin import introspect, schema, schema_query, table_search_router
    from provisa.discovery import catalog_cache

    class _Pool:
        def acquire(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    async def _pool():
        return _Pool()

    async def _no_native(*args):
        return None

    async def _boom(*args, **kwargs):
        raise AssertionError("the request asked for a catalog or a table's columns")

    async def _source(source_id):
        return SimpleNamespace(id=source_id)

    monkeypatch.setattr(schema, "_get_pool", _pool)
    monkeypatch.setattr(introspect, "native_tables", _no_native)
    monkeypatch.setattr(introspect, "native_columns", _boom)
    monkeypatch.setattr(introspect, "require_live_attach", _boom)
    monkeypatch.setattr(table_search_router, "run_admin_catalog_sql", _boom)
    monkeypatch.setattr(schema_query, "_source_for_introspection", _source)
    monkeypatch.setattr(catalog_cache, "_seam_bound", _no_binding, raising=False)
    state = SimpleNamespace(
        source_types={"test": "govdata"},
        source_pools=None,
        federation_engine=_AttachingEngine(lambda schema_name: ["financial_facts", "filings"]),
    )

    found = await table_search_router._candidates_live("test", "sec", state)
    assert [(c.name, c.columns) for c in found] == [("financial_facts", []), ("filings", [])]
