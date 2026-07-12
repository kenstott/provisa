# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the PRODUCTION API-cache write face, against a live DuckDB engine + Postgres store
(REQ-318, REQ-848, REQ-855, REQ-932).

The store-lifecycle e2e proves the POLICY modules composed, but drives them with local land/read
helpers because "the production executor wiring is still in-progress". This test closes exactly that
gap: it drives the REAL production functions the /data endpoint calls —

  land_api_cache()  → store_writer.land() lands API rows into the PG store (the ONE write face)
  execute_engine_sync() over the DuckDB store-attach → the engine READS the landed replica back
  rewrite_from_cache() → the semantic FROM is repointed at the cache table (the production rewrite)
  schedule_drop()   → TTL expiry really DROPs the landed table through the engine

— all bound to a live DuckDBFederationRuntime whose materialization store is a real Postgres. Three
residual live-integration claims are asserted end to end: rows actually land, the engine's federated
read equals the store's direct read, and the TTL sweep really expires the entry.
"""

from __future__ import annotations

import os
from collections import namedtuple
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

duckdb = pytest.importorskip("duckdb")
asyncpg = pytest.importorskip("asyncpg")

from provisa.api_source.engine_cache import (  # noqa: E402
    cache_location,
    cache_table_name,
    ensure_cache_schema,
    land_api_cache,
    rewrite_from_cache,
    schedule_drop,
    table_exists,
)
from provisa.federation.engine import build_duckdb_engine  # noqa: E402
from provisa.federation.runtime import EngineRuntime  # noqa: E402

_SOURCE_ID = "orders-api"
_CACHE_SCHEMA = "e2e_write_face"
_Col = namedtuple("_Col", ["name", "type"])  # matches _land_columns' (.name, .type) contract
# The API-shaped rows a REST pull would hand the write face (openapi is non-attachable → LANDED).
_ROWS = [
    {"id": 10, "customer_id": 1, "amount": 19.99},
    {"id": 11, "customer_id": 2, "amount": 49.99},
    {"id": 12, "customer_id": 1, "amount": 5.0},
]
_COLS = [_Col("id", "integer"), _Col("customer_id", "integer"), _Col("amount", "number")]


def _pg_dsn() -> str:
    u = os.environ.get("PG_USER", "provisa")
    pw = os.environ.get("PG_PASSWORD", "provisa")
    h = os.environ.get("PG_HOST", "localhost")
    p = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DATABASE", "provisa")
    return f"postgresql://{u}:{pw}@{h}:{p}/{db}"


@pytest.fixture(scope="session", autouse=True)
def _db_snapshot_restore():
    """Override: this module owns an isolated cache schema it creates and drops itself — the
    session-wide pg_dump/restore cycle is unnecessary here."""
    yield


@pytest.fixture()
def engine_runtime(monkeypatch):
    """A live DuckDB EngineRuntime whose materialization store is the dev-stack Postgres.

    Both the store-attach (DuckDB ATTACH ... TYPE postgres) and the write face
    (materialize_store_dsn → store_writer.land) resolve the SAME DSN via $PROVISA_MATERIALIZE_URL,
    so the engine reads exactly what the write face landed. No app boot — the runtime is built
    directly, the seam the /data endpoint executors use.
    """
    monkeypatch.setenv("PROVISA_MATERIALIZE_URL", _pg_dsn())
    engine = build_duckdb_engine()
    # config=None → the backend's lazy source-attach is a no-op; we only exercise the store terminal.
    rt = EngineRuntime(engine, SimpleNamespace(config=None, engine_conn_kwargs={}))
    yield rt
    engine.backend.close(rt._state)


async def _drop_cache_schema() -> None:
    conn = await asyncpg.connect(dsn=_pg_dsn())
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_CACHE_SCHEMA} CASCADE")
    finally:
        await conn.close()


@pytest.fixture()
async def clean_cache_schema():
    await _drop_cache_schema()
    yield
    await _drop_cache_schema()


async def test_write_face_lands_and_engine_reads_the_replica(engine_runtime, clean_cache_schema):
    """land_api_cache lands API rows into PG; the DuckDB engine reads them back through its attach,
    and the federated read equals the store's own direct read (REQ-848 + direct/federated parity)."""
    rt = engine_runtime
    # loc.catalog = the engine's store-attach alias (mat_store); loc.schema = our isolated cache schema.
    loc = cache_location(_SOURCE_ID, cache_schema=_CACHE_SCHEMA, engine=rt)
    table = cache_table_name(_SOURCE_ID, "listOrders", {})

    # The engine creates the cache schema in the attached store (DDL through the engine, as production).
    with rt.isolated_sync() as conn:
        ensure_cache_schema(conn, loc)

    # LAND: rows really written into PG through the ONE write face (store_writer.land), not the engine.
    await land_api_cache(rt, loc, table, _ROWS, _COLS)

    # DIRECT read: straight from the Postgres store, no engine involved.
    pg = await asyncpg.connect(dsn=_pg_dsn())
    try:
        direct = await pg.fetch(
            f'SELECT id, customer_id, amount FROM {_CACHE_SCHEMA}."{table}" ORDER BY id'
        )
    finally:
        await pg.close()
    direct_rows = [(r["id"], r["customer_id"], float(r["amount"])) for r in direct]
    assert direct_rows == [(10, 1, 19.99), (11, 2, 49.99), (12, 1, 5.0)]  # rows really landed

    # FEDERATED read: the engine reads the SAME landed replica back through its store-attach.
    fed = rt.execute_engine_sync(
        f'SELECT id, customer_id, amount FROM mat_store."{_CACHE_SCHEMA}"."{table}" ORDER BY id'
    )
    fed_rows = [(r[0], r[1], float(r[2])) for r in fed.rows]

    assert fed_rows == direct_rows  # federated == direct: the engine reads exactly what was landed


async def test_rewrite_from_cache_repoints_semantic_from_at_the_landed_table(
    engine_runtime, clean_cache_schema
):
    """The production Phase-2 rewrite: a semantic FROM is repointed at the cache table, then the
    engine executes it over the store-attach — the actual read path an API query takes."""
    rt = engine_runtime
    loc = cache_location(_SOURCE_ID, cache_schema=_CACHE_SCHEMA, engine=rt)
    table = cache_table_name(_SOURCE_ID, "listOrders", {})
    with rt.isolated_sync() as conn:
        ensure_cache_schema(conn, loc)
    await land_api_cache(rt, loc, table, _ROWS, _COLS)

    # The compiler-shaped physical SQL references the source's semantic relation; rewrite_from_cache
    # repoints the root FROM at mat_store."schema"."cache_table" (the real Phase-2 substitution).
    semantic_sql = (
        'SELECT id, amount FROM "orders_api"."main"."orders" WHERE amount > 10 ORDER BY id'
    )
    rewritten = rewrite_from_cache(semantic_sql, loc, table)
    assert f'"{_CACHE_SCHEMA}"."{table}"' in rewritten or f"{_CACHE_SCHEMA}" in rewritten

    result = rt.execute_engine_sync(rewritten)
    rows = [(r[0], float(r[1])) for r in result.rows]
    assert rows == [(10, 19.99), (11, 49.99)]  # WHERE/ORDER applied by the engine over the replica


async def test_schedule_drop_expires_the_landed_entry_through_the_engine(
    engine_runtime, clean_cache_schema
):
    """TTL expiry against the live engine: schedule_drop sleeps the TTL, then DROPs the landed table
    through a fresh engine connection — the store entry is really gone afterward (REQ-855)."""
    rt = engine_runtime
    loc = cache_location(_SOURCE_ID, cache_schema=_CACHE_SCHEMA, engine=rt)
    table = cache_table_name(_SOURCE_ID, "listOrders", {})
    with rt.isolated_sync() as conn:
        ensure_cache_schema(conn, loc)
    await land_api_cache(rt, loc, table, _ROWS, _COLS)

    # Present before the sweep — the engine sees the landed table through its attach.
    with rt.isolated_sync() as conn:
        assert table_exists(conn, loc, table) is True

    await schedule_drop(rt, loc, table, ttl=1)  # sleeps 1s, then DROPs through a fresh engine conn

    # Gone from the store itself — verified directly, not via the engine's (now-invalidated) cache.
    pg = await asyncpg.connect(dsn=_pg_dsn())
    try:
        exists = await pg.fetchval("SELECT to_regclass($1) IS NOT NULL", f"{_CACHE_SCHEMA}.{table}")
    finally:
        await pg.close()
    assert exists is False  # TTL sweep really dropped the landed replica

    with rt.isolated_sync() as conn:
        assert table_exists(conn, loc, table) is False  # and the engine no longer resolves it


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
