# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Whether a table has been landed is read from the catalog of the store it lands in (REQ-318).

Each engine family answers from its own catalog listing: absent is False, landed is True, and a
catalog that cannot be read raises -- it is never read as a table that is not there, which would
fetch and land it again over a store that is failing. Here: the base store path (a broad
federator's, Trino's), the Postgres engine, DuckDB's embedded store, the SQLAlchemy engine, the
dispatch, and the API cache's ``table_exists`` over them. The warehouse engines, Fabric and
ClickHouse are checked on their own lanes, beside their landing tests."""

# Requirements: REQ-318

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import psycopg2
import pytest
import sqlalchemy
from sqlalchemy.exc import OperationalError

from provisa.federation.land_guard import LandGuard
from tests.port_lease import lease_port

pytestmark = pytest.mark.asyncio

_FEDERATION = Path(__file__).resolve().parents[3] / "provisa" / "federation"


def _dead_dsn() -> str:
    """A Postgres address nothing listens on: a store that cannot be reached."""
    return f"postgresql://provisa:provisa@127.0.0.1:{lease_port()}/provisa"


@pytest.fixture
def pg_schema(embedded_postgres):
    """A fresh schema of the session's Postgres, holding one table ``landed``."""
    import uuid

    schema = f"lte_{uuid.uuid4().hex[:8]}"
    con = psycopg2.connect(embedded_postgres.dsn())
    con.autocommit = True
    with con.cursor() as cur:
        cur.execute(f'CREATE SCHEMA "{schema}"')
        cur.execute(f'CREATE TABLE "{schema}"."landed" (id integer)')
    yield schema
    with con.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
    con.close()


# -- the base store path (Trino lands here) ------------------------------------------------------


async def test_the_store_says_whether_it_holds_a_table(embedded_postgres, pg_schema):
    from provisa.federation import store_writer

    dsn = embedded_postgres.dsn()
    assert await store_writer.table_exists(dsn, schema=pg_schema, table="landed") is True
    assert await store_writer.table_exists(dsn, schema=pg_schema, table="never") is False
    assert await store_writer.table_exists(dsn, schema="no_such_schema", table="x") is False


async def test_a_store_that_cannot_be_read_raises():
    from provisa.federation import store_writer

    with pytest.raises(OperationalError):
        await store_writer.table_exists(_dead_dsn(), schema="s", table="t")


async def test_the_base_backend_asks_the_materialization_store(embedded_postgres, pg_schema):
    from provisa.federation.backend import EngineBackend

    backend = EngineBackend.__new__(EngineBackend)
    backend.engine = SimpleNamespace(materialize_store=embedded_postgres.dsn)  # type: ignore[attr-defined]
    assert await backend.landed_table_exists(None, schema=pg_schema, table="landed") is True
    assert await backend.landed_table_exists(None, schema=pg_schema, table="never") is False
    backend.engine = SimpleNamespace(materialize_store=_dead_dsn)  # type: ignore[attr-defined]
    with pytest.raises(OperationalError):
        await backend.landed_table_exists(None, schema=pg_schema, table="landed")


# -- the Postgres engine's own store -------------------------------------------------------------


def _pg_runtime(dsn: str):
    from provisa.federation.pg_runtime import PgFederationRuntime

    rt = PgFederationRuntime.__new__(PgFederationRuntime)
    rt._land_guard = LandGuard("test store connection")
    rt._con = psycopg2.connect(dsn)
    rt._con.autocommit = True
    return rt


async def test_the_postgres_engine_reads_its_own_catalog(embedded_postgres, pg_schema):
    rt = _pg_runtime(embedded_postgres.dsn())
    try:
        assert await rt.landed_table_exists(schema=pg_schema, table="landed") is True
        assert await rt.landed_table_exists(schema=pg_schema, table="never") is False
    finally:
        rt._con.close()
    with pytest.raises(psycopg2.InterfaceError):  # its connection is gone
        await rt.landed_table_exists(schema=pg_schema, table="landed")


# -- DuckDB's embedded store, through its broker -------------------------------------------------


def _duckdb_runtime(broker):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime.__new__(DuckDBFederationRuntime)
    rt._land_guard = LandGuard("test store connection")
    rt.ensure_materialize_attached = lambda: None  # type: ignore[method-assign]
    rt._store_is_duckdb = lambda: True  # type: ignore[method-assign]
    rt._store_broker = broker
    return rt


async def test_duckdb_reads_its_embedded_stores_catalog(tmp_path):
    from provisa.federation.materialize_broker import _MAT_STORE_ALIAS, get_broker

    broker = get_broker(str(tmp_path / "store.duckdb"))
    broker.execute(f"CREATE SCHEMA {_MAT_STORE_ALIAS}.s")
    broker.execute(f'CREATE TABLE {_MAT_STORE_ALIAS}.s."landed" (id INTEGER)')
    rt = _duckdb_runtime(broker)
    assert await rt.landed_table_exists(schema="s", table="landed") is True
    assert await rt.landed_table_exists(schema="s", table="never") is False
    assert await rt.landed_table_exists(schema="no_such_schema", table="x") is False


async def test_duckdb_raises_when_its_store_cannot_be_read(tmp_path):
    from provisa.federation.materialize_broker import get_broker

    # A store file under a directory that does not exist cannot be opened.
    broker = get_broker(str(tmp_path / "missing" / "store.duckdb"))
    rt = _duckdb_runtime(broker)
    with pytest.raises(FileNotFoundError, match="store.duckdb"):
        await rt.landed_table_exists(schema="s", table="landed")


# -- the SQLAlchemy engine's own store -----------------------------------------------------------


def _sqlalchemy_runtime(url: str):
    from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime

    rt = SqlAlchemyFederationRuntime.__new__(SqlAlchemyFederationRuntime)
    rt._sa = sqlalchemy.create_engine(url)
    return rt


async def test_the_sqlalchemy_engine_reads_its_own_catalog(embedded_postgres, pg_schema):
    rt = _sqlalchemy_runtime(embedded_postgres.dsn())
    try:
        assert await rt.landed_table_exists(schema=pg_schema, table="landed") is True
        assert await rt.landed_table_exists(schema=pg_schema, table="never") is False
    finally:
        rt._sa.dispose()
    broken = _sqlalchemy_runtime(_dead_dsn())
    with pytest.raises(OperationalError):
        await broken.landed_table_exists(schema=pg_schema, table="landed")


# -- the dispatch --------------------------------------------------------------------------------


def _runtime_classes_that_land() -> list[str]:
    """Every runtime class under ``provisa/federation`` that defines ``land_table``."""
    landing = []
    for path in sorted(_FEDERATION.glob("*_runtime.py")):
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.ClassDef):
                names = {
                    n.name
                    for n in node.body
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                }
                if "land_table" in names:
                    landing.append(f"{path.stem}.{node.name}:{'landed_table_exists' in names}")
    return landing


def test_every_runtime_that_lands_says_what_it_landed():
    """A native engine's write face is its runtime; whether a table is landed is asked of the
    same runtime, so each one that lands answers from its own catalog."""
    landing = _runtime_classes_that_land()
    assert len(landing) >= 8, landing
    assert [entry for entry in landing if entry.endswith(":False")] == []


async def test_a_native_engine_asks_its_runtime_and_a_federator_asks_the_store(monkeypatch):
    from provisa.federation import native_backend
    from provisa.federation.backend import EngineBackend
    from provisa.federation.materialization import WriteFace

    runtime = SimpleNamespace(land_table=object(), landed_table_exists=AsyncMock(return_value=True))
    backend = native_backend.NativeEngineBackend.__new__(native_backend.NativeEngineBackend)
    backend.engine = SimpleNamespace(materialize_store=lambda: "postgresql://x/y")  # type: ignore[attr-defined]
    backend._runtime_for = lambda _state: runtime  # type: ignore[method-assign]
    store = AsyncMock(return_value=False)
    monkeypatch.setattr(EngineBackend, "landed_table_exists", store)

    monkeypatch.setattr(
        "provisa.federation.materialization.select_write_face",
        lambda *_a, **_k: WriteFace.ENGINE_NATIVE,
    )
    assert await backend.landed_table_exists(None, schema="s", table="t") is True
    runtime.landed_table_exists.assert_awaited_once_with(schema="s", table="t")
    store.assert_not_awaited()

    monkeypatch.setattr(
        "provisa.federation.materialization.select_write_face",
        lambda *_a, **_k: WriteFace.SQLALCHEMY_UPSERT,
    )
    assert await backend.landed_table_exists(None, schema="s", table="t") is False
    store.assert_awaited_once()


# -- the API cache's question --------------------------------------------------------------------


@pytest.fixture
def fresh_cache():
    from provisa.api_source import engine_cache

    engine_cache._TABLE_EXISTS_CACHE.clear()
    yield engine_cache
    engine_cache._TABLE_EXISTS_CACHE.clear()


async def test_the_api_cache_asks_the_engine_and_keeps_only_a_landed_answer(fresh_cache):
    loc = fresh_cache.CacheLocation("mat", "org_cache", "relational")
    engine = SimpleNamespace(landed_table_exists=AsyncMock(return_value=False))
    assert await fresh_cache.table_exists(engine, loc, "r_1", ttl=300) is False
    assert await fresh_cache.table_exists(engine, loc, "r_1", ttl=300) is False
    assert engine.landed_table_exists.await_count == 2  # an absent answer is never kept

    engine.landed_table_exists = AsyncMock(return_value=True)
    assert await fresh_cache.table_exists(engine, loc, "r_1", ttl=300) is True
    assert await fresh_cache.table_exists(engine, loc, "r_1", ttl=300) is True
    engine.landed_table_exists.assert_awaited_once_with(schema="org_cache", table="r_1")


async def test_the_api_cache_raises_when_the_store_cannot_be_read(fresh_cache):
    loc = fresh_cache.CacheLocation("mat", "org_cache", "relational")
    engine = SimpleNamespace(landed_table_exists=AsyncMock(side_effect=OSError("store is down")))
    with pytest.raises(OSError, match="store is down"):
        await fresh_cache.table_exists(engine, loc, "r_1", ttl=300)
    assert ("mat", "org_cache", "r_1") not in fresh_cache._TABLE_EXISTS_CACHE
    assert inspect.iscoroutinefunction(fresh_cache.table_exists)
