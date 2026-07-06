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
    assert loc.backend == "postgresql"


def test_cache_location_explicit_catalog():
    loc = cache_location("petstore-api", cache_catalog="analytics_pg")
    assert loc.catalog == "analytics_pg"
    assert loc.backend == "postgresql"


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


# --- REQ-280: ANALYZE after cache materialization ---


class _FakeCursor:
    def __init__(self, log):
        self._log = log

    def execute(self, sql):
        self._log.append(sql)

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self):
        self.executed: list[str] = []

    def cursor(self):
        return _FakeCursor(self.executed)


class _Col:
    def __init__(self, name, type_="VARCHAR"):
        self.name = name
        self.type = type_


def test_create_and_insert_runs_analyze():
    from provisa.api_source.engine_cache import CacheLocation, create_and_insert

    conn = _FakeConn()
    loc = CacheLocation(catalog="petstore_api", schema="api_cache", backend="postgresql")
    cols = [_Col("id", "BIGINT"), _Col("name", "VARCHAR")]
    create_and_insert(conn, loc, "api_cache_users", [{"id": 1, "name": "x"}], cols)

    analyze = [s for s in conn.executed if s.startswith("ANALYZE")]
    assert len(analyze) == 1
    assert 'petstore_api.api_cache."api_cache_users"' in analyze[0]


def test_create_and_insert_analyze_after_insert():
    from provisa.api_source.engine_cache import CacheLocation, create_and_insert

    conn = _FakeConn()
    loc = CacheLocation(catalog="petstore_api", schema="api_cache", backend="postgresql")
    create_and_insert(conn, loc, "t", [{"id": 1}], [_Col("id", "BIGINT")])

    kinds = [s.split()[0] for s in conn.executed]
    # ANALYZE comes after CREATE and INSERT
    assert kinds.index("ANALYZE") > kinds.index("INSERT")


def test_create_and_insert_no_analyze_when_empty():
    from provisa.api_source.engine_cache import CacheLocation, create_and_insert

    conn = _FakeConn()
    loc = CacheLocation(catalog="petstore_api", schema="api_cache", backend="postgresql")
    create_and_insert(conn, loc, "t", [], [_Col("id", "BIGINT")])
    # empty result returns before insert/analyze
    assert not any(s.startswith("ANALYZE") for s in conn.executed)
