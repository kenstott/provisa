# Copyright (c) 2026 Kenneth Stott
# Canary: 005f3b2c-8591-4e61-b5e7-8bb3d49c041f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for API source caching via Trino (Phase U, REQ-309/318/327)."""

import pytest

from provisa.api_source.cache import DEFAULT_TTL, resolve_ttl
from provisa.api_source.engine_cache import (
    CacheLocation,
    cache_location,
    cache_table_name,
    rewrite_from_cache,
)


# --- TTL resolution ---


def test_ttl_endpoint_wins():
    assert resolve_ttl(60, 120, 300) == 60


def test_ttl_source_fallback():
    assert resolve_ttl(None, 120, 300) == 120


def test_ttl_global_fallback():
    assert resolve_ttl(None, None, 300) == 300


def test_ttl_default():
    assert resolve_ttl(None, None, None) == DEFAULT_TTL


# --- cache_location factory ---


def test_cache_location_default_uses_source_catalog():
    loc = cache_location("petstore-api")
    assert loc.catalog == "petstore_api"
    assert loc.schema == "api_cache"
    assert loc.backend == "relational"


def test_cache_location_explicit_catalog():
    loc = cache_location("petstore-api", cache_catalog="analytics_pg")
    assert loc.catalog == "analytics_pg"
    assert loc.backend == "relational"


def test_cache_location_iceberg_catalog():
    loc = cache_location("petstore-api", cache_catalog="results")
    assert loc.catalog == "results"
    assert loc.backend == "iceberg"


def test_cache_location_custom_schema():
    loc = cache_location("my-source", cache_schema="scratch")
    assert loc.schema == "scratch"


# --- Cache table name ---


def test_cache_table_name_deterministic():
    """Same inputs always produce the same name."""
    n1 = cache_table_name("src", "/users", {"page": 1})
    n2 = cache_table_name("src", "/users", {"page": 1})
    assert n1 == n2


def test_cache_table_name_param_order_stable():
    """Param ordering does not change the name."""
    n1 = cache_table_name("src", "/users", {"a": 1, "b": 2})
    n2 = cache_table_name("src", "/users", {"b": 2, "a": 1})
    assert n1 == n2


def test_cache_table_name_different_source():
    n1 = cache_table_name("src-a", "/users", {})
    n2 = cache_table_name("src-b", "/users", {})
    assert n1 != n2


def test_cache_table_name_different_path():
    n1 = cache_table_name("src", "/users", {})
    n2 = cache_table_name("src", "/orders", {})
    assert n1 != n2


def test_cache_table_name_different_params():
    n1 = cache_table_name("src", "/users", {"page": 1})
    n2 = cache_table_name("src", "/users", {"page": 2})
    assert n1 != n2


def test_cache_table_name_format():
    """Name must start with 'r_' and be a valid identifier."""
    name = cache_table_name("src", "/endpoint", {})
    assert name.startswith("r_")
    assert len(name) == 2 + 16  # r_ + 16 hex chars


# --- SQL FROM rewrite ---

_LOC = CacheLocation("petstore_api", "api_cache", "postgresql")


def test_rewrite_from_cache_simple():
    sql = 'SELECT "id" FROM "public"."users"'
    result = rewrite_from_cache(sql, _LOC, "r_abc123")
    assert "petstore_api" in result
    assert "api_cache" in result
    assert "r_abc123" in result
    assert "SELECT" in result


def test_rewrite_from_cache_preserves_where():
    sql = 'SELECT "id" FROM "public"."users" WHERE "active" = TRUE'
    result = rewrite_from_cache(sql, _LOC, "r_abc123")
    assert "WHERE" in result
    assert "active" in result


def test_rewrite_from_cache_preserves_limit():
    sql = 'SELECT "id" FROM "public"."users" LIMIT 10 OFFSET 0'
    result = rewrite_from_cache(sql, _LOC, "r_abc123")
    assert "LIMIT" in result
    assert "10" in result


def test_rewrite_from_cache_preserves_order_by():
    sql = 'SELECT "id", "name" FROM "public"."users" ORDER BY "name" ASC'
    result = rewrite_from_cache(sql, _LOC, "r_tbl9")
    assert "ORDER BY" in result
    assert "r_tbl9" in result


def test_rewrite_from_cache_catalog_schema():
    sql = 'SELECT "x" FROM "db"."tbl"'
    loc = cache_location("my-source", cache_catalog="analytics_pg", cache_schema="staging")
    result = rewrite_from_cache(sql, loc, "r_xyz")
    assert "analytics_pg" in result
    assert "staging" in result


# --- REQ-280/REQ-1688: statistics are collected where the landed table lives ---


class _FakeCursor:
    def __init__(self, executed):
        self._executed = executed

    def execute(self, sql):
        self._executed.append(sql)

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self):
        self.executed: list[str] = []

    def cursor(self):
        return _FakeCursor(self.executed)

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def fetchall(self):
        return []


def _col(name: str, type_name: str):
    return type("C", (), {"name": name, "type": type_name})()


def test_create_and_insert_issues_no_analyze():
    """The engine's ANALYZE is not the store's (DuckDB refuses it on an attached Postgres table);
    statistics are the runtime's analyze_landed_table, dispatched per store (REQ-1688)."""
    from provisa.api_source.engine_cache import CacheLocation, create_and_insert

    conn = _FakeConn()
    loc = CacheLocation(catalog="petstore_api", schema="api_cache", backend="postgres")
    create_and_insert(conn, loc, "api_cache_users", [{"id": 1}], [_col("id", "integer")])
    assert not any(s.startswith("ANALYZE") for s in conn.executed)
    assert any(s.startswith("INSERT") for s in conn.executed)


class _StoreConn:
    def __init__(self, dialect, executed):
        self.capabilities = type("Caps", (), {"dialect": dialect})()
        self._executed = executed

    async def execute(self, sql, *args):
        self._executed.append(sql)
        return ""


def _store_connection_factory(dialect, executed):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _cm(dsn):
        executed.append(f"DSN {dsn}")
        yield _StoreConn(dialect, executed)

    return _cm


@pytest.mark.asyncio
async def test_native_backend_analyzes_a_postgres_store_through_the_store(monkeypatch):
    from provisa.federation import store_writer
    from provisa.federation.native_backend import NativeEngineBackend

    executed: list[str] = []
    monkeypatch.setattr(
        store_writer, "store_connection", _store_connection_factory("postgresql", executed)
    )
    backend = NativeEngineBackend.__new__(NativeEngineBackend)
    backend.engine = type("E", (), {"materialize_store": lambda self: "postgresql://u:p@h/db"})()
    backend._runtime = type("R", (), {"_store_is_duckdb": lambda self: False})()
    backend._attach_registered = lambda state: None
    await backend.analyze_landed_table(None, catalog="mat_store", schema="api_cache", table="t")
    assert executed == ["DSN postgresql://u:p@h/db", 'ANALYZE "api_cache"."t"']


@pytest.mark.asyncio
async def test_native_backend_analyzes_an_embedded_duckdb_store_through_the_engine(monkeypatch):
    from contextlib import contextmanager

    from provisa.federation.native_backend import NativeEngineBackend

    conn = _FakeConn()
    backend = NativeEngineBackend.__new__(NativeEngineBackend)
    backend._runtime = type("R", (), {"_store_is_duckdb": lambda self: True})()
    backend._attach_registered = lambda state: None

    @contextmanager
    def _iso(state):
        yield conn

    backend.isolated_sync = _iso
    await backend.analyze_landed_table(None, catalog="mat_store", schema="api_cache", table="t")
    assert conn.executed == ['ANALYZE mat_store.api_cache."t"']


@pytest.mark.asyncio
async def test_a_store_dialect_without_statistics_is_skipped_by_name(monkeypatch, caplog):
    import logging

    from provisa.federation import store_writer
    from provisa.federation.backend import EngineBackend

    executed: list[str] = []
    monkeypatch.setattr(
        store_writer, "store_connection", _store_connection_factory("sqlite", executed)
    )
    backend = EngineBackend.__new__(EngineBackend)
    backend.engine = type("E", (), {"materialize_store": lambda self: "sqlite:///x.db"})()
    with caplog.at_level(logging.INFO):
        await backend.analyze_landed_table(None, catalog="c", schema="s", table="t")
    assert executed == ["DSN sqlite:///x.db"]
    assert "collects none" in caplog.text


@pytest.mark.asyncio
async def test_analyze_cache_table_is_best_effort(caplog):
    import logging

    from provisa.api_source.engine_cache import CacheLocation, analyze_cache_table

    class _Engine:
        async def analyze_landed_table(self, **_):
            raise RuntimeError("store unreachable")

    loc = CacheLocation(catalog="mat_store", schema="api_cache", backend="postgres")
    with caplog.at_level(logging.WARNING):
        await analyze_cache_table(_Engine(), loc, "t")
    assert "store unreachable" in caplog.text
