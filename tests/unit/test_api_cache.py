# Copyright (c) 2026 Kenneth Stott
# Canary: 005f3b2c-8591-4e61-b5e7-8bb3d49c041f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for API source caching via Trino Iceberg (Phase U, REQ-309/318/327)."""

import pytest

from provisa.api_source.cache import DEFAULT_TTL, resolve_ttl
from provisa.api_source.trino_cache import (
    CACHE_CATALOG,
    CACHE_SCHEMA,
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

def test_rewrite_from_cache_simple():
    sql = 'SELECT "id" FROM "public"."users"'
    result = rewrite_from_cache(sql, "r_abc123")
    assert f"{CACHE_CATALOG}.{CACHE_SCHEMA}" in result
    assert "r_abc123" in result
    assert "SELECT" in result


def test_rewrite_from_cache_preserves_where():
    sql = 'SELECT "id" FROM "public"."users" WHERE "active" = TRUE'
    result = rewrite_from_cache(sql, "r_abc123")
    assert "WHERE" in result
    assert "active" in result


def test_rewrite_from_cache_preserves_limit():
    sql = 'SELECT "id" FROM "public"."users" LIMIT 10 OFFSET 0'
    result = rewrite_from_cache(sql, "r_abc123")
    assert "LIMIT" in result
    assert "10" in result


def test_rewrite_from_cache_preserves_order_by():
    sql = 'SELECT "id", "name" FROM "public"."users" ORDER BY "name" ASC'
    result = rewrite_from_cache(sql, "r_tbl9")
    assert "ORDER BY" in result
    assert "r_tbl9" in result


def test_rewrite_from_cache_catalog_schema():
    sql = 'SELECT "x" FROM "db"."tbl"'
    result = rewrite_from_cache(sql, "r_xyz")
    assert CACHE_CATALOG in result
    assert CACHE_SCHEMA in result
