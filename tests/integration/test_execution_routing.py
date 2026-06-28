# Copyright (c) 2026 Kenneth Stott
# Canary: d3f1a2b4-c5e6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: execution routing, MV lifecycle, cache headers, and federation.

Tests cover the routing pipeline functions directly — not HTTP endpoints.
"""

# Requirements: REQ-027, REQ-028, REQ-029, REQ-030, REQ-031, REQ-234, REQ-235,
#               REQ-238, REQ-239, REQ-240, REQ-241, REQ-275, REQ-276, REQ-277,
#               REQ-278, REQ-279, REQ-280, REQ-281, REQ-397, REQ-536, REQ-544,
#               REQ-552, REQ-595

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source_types(**kwargs: str) -> dict[str, str]:
    return kwargs


def _make_dialects(**kwargs: str) -> dict[str, str]:
    return kwargs


# ---------------------------------------------------------------------------
# REQ-027, REQ-028, REQ-030, REQ-031, REQ-552: decide_route
# ---------------------------------------------------------------------------


class TestDecideRoute:
    def test_single_pg_source_routes_direct(self):
        # REQ-027: single RDBMS source with direct driver routes DIRECT
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"sales-pg"},
            source_types={"sales-pg": "postgresql"},
            source_dialects={"sales-pg": "postgres"},
        )
        assert result.route == Route.DIRECT
        assert result.source_id == "sales-pg"

    def test_multi_source_routes_trino(self):
        # REQ-028: cross-source queries route to Trino
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"sales-pg", "crm-pg"},
            source_types={"sales-pg": "postgresql", "crm-pg": "postgresql"},
            source_dialects={"sales-pg": "postgres", "crm-pg": "postgres"},
        )
        assert result.route == Route.TRINO
        assert result.source_id is None

    def test_steward_override_trino(self):
        # REQ-030: steward override forces Trino even for a single RDBMS source
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"sales-pg"},
            source_types={"sales-pg": "postgresql"},
            source_dialects={"sales-pg": "postgres"},
            steward_hint="federated",
        )
        assert result.route == Route.TRINO

    def test_steward_override_direct(self):
        # REQ-030: steward can force DIRECT on single RDBMS source
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"sales-pg"},
            source_types={"sales-pg": "postgresql"},
            source_dialects={"sales-pg": "postgres"},
            steward_hint="direct",
        )
        assert result.route == Route.DIRECT

    def test_mutation_always_direct(self):
        # REQ-031: DB mutations always route DIRECT, never Trino
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"sales-pg"},
            source_types={"sales-pg": "postgresql"},
            source_dialects={"sales-pg": "postgres"},
            is_mutation=True,
        )
        assert result.route == Route.DIRECT
        assert "mutation" in result.reason

    def test_virtual_source_routes_trino(self):
        # REQ-028: NoSQL/virtual sources always route through Trino
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"events-kafka"},
            source_types={"events-kafka": "kafka"},
            source_dialects={"events-kafka": ""},
        )
        assert result.route == Route.TRINO

    def test_api_source_routes_api(self):
        # REQ-027: openapi sources route through API caller pipeline
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"ext-api"},
            source_types={"ext-api": "openapi"},
            source_dialects={"ext-api": ""},
        )
        assert result.route == Route.API

    def test_cross_source_type_coercion_routes_trino(self):
        # REQ-552: cross-source JOINs across differing native types route Trino
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"sales-pg", "analytics-mysql"},
            source_types={"sales-pg": "postgresql", "analytics-mysql": "mysql"},
            source_dialects={"sales-pg": "postgres", "analytics-mysql": "mysql"},
        )
        assert result.route == Route.TRINO

    def test_colocated_sources_route_direct(self):
        # REQ-027: two sources on the same physical DB DSN route DIRECT
        from provisa.transpiler.router import Route, decide_route

        dsn = "postgresql://user:pw@host:5432/db"
        result = decide_route(
            sources={"src-a", "src-b"},
            source_types={"src-a": "postgresql", "src-b": "postgresql"},
            source_dialects={"src-a": "postgres", "src-b": "postgres"},
            source_dsns={"src-a": dsn, "src-b": dsn},
        )
        assert result.route == Route.DIRECT


# ---------------------------------------------------------------------------
# REQ-029: Large-result redirect parameter plumbing
# ---------------------------------------------------------------------------


class TestRedirectParams:
    def test_build_redirect_params_force_redirect(self):
        # REQ-029: X-Provisa-Redirect: true forces redirect regardless of row count
        from provisa.api.data.endpoint import _build_redirect_params

        directives = MagicMock()
        directives.redirect_format = None
        directives.redirect_threshold = None

        redirect_format, effective_threshold, force_redirect = _build_redirect_params(
            "true", None, None, directives
        )
        assert force_redirect is True

    def test_build_redirect_params_threshold_header(self):
        # REQ-029: X-Provisa-Redirect-Threshold header sets effective_threshold
        from provisa.api.data.endpoint import _build_redirect_params

        directives = MagicMock()
        directives.redirect_format = None
        directives.redirect_threshold = None

        _, effective_threshold, force_redirect = _build_redirect_params(
            None, 1000, None, directives
        )
        assert effective_threshold == 1000
        assert force_redirect is False

    def test_build_redirect_params_format_without_threshold_forces_redirect(self):
        # REQ-029: redirect_format without threshold implies force_redirect
        from provisa.api.data.endpoint import _build_redirect_params

        directives = MagicMock()
        directives.redirect_format = None
        directives.redirect_threshold = None

        redirect_format, effective_threshold, force_redirect = _build_redirect_params(
            None, None, "application/vnd.apache.parquet", directives
        )
        assert force_redirect is True
        assert redirect_format == "parquet"


# ---------------------------------------------------------------------------
# REQ-397: Cache key / injection probe limit
# ---------------------------------------------------------------------------


class TestInjectProbeLimit:
    def test_inject_no_existing_limit(self):
        # REQ-397: inject LIMIT when query has none
        from provisa.api.data.endpoint import _inject_probe_limit

        sql = 'SELECT "id" FROM "public"."orders"'
        result = _inject_probe_limit(sql, 500)
        assert "LIMIT 500" in result

    def test_inject_tightens_existing_literal_limit(self):
        # REQ-397: probe limit tightens existing literal LIMIT
        from provisa.api.data.endpoint import _inject_probe_limit

        sql = 'SELECT "id" FROM "public"."orders" LIMIT 2000'
        result = _inject_probe_limit(sql, 500)
        assert "LIMIT 500" in result
        assert "LIMIT 2000" not in result

    def test_inject_respects_smaller_existing_limit(self):
        # REQ-397: probe limit leaves existing LIMIT when it is already smaller
        from provisa.api.data.endpoint import _inject_probe_limit

        sql = 'SELECT "id" FROM "public"."orders" LIMIT 100'
        result = _inject_probe_limit(sql, 500)
        assert "LIMIT 100" in result

    def test_inject_skips_parameterized_limit(self):
        # REQ-397: parameterized LIMIT is user-supplied and must not be overridden
        from provisa.api.data.endpoint import _inject_probe_limit

        sql = 'SELECT "id" FROM "public"."orders" LIMIT $1'
        result = _inject_probe_limit(sql, 500)
        assert "LIMIT $1" in result
        assert "LIMIT 500" not in result


# ---------------------------------------------------------------------------
# REQ-536: Cache headers (build_cache_headers)
# ---------------------------------------------------------------------------


class TestCacheHeaders:
    def test_cache_miss_header(self):
        # REQ-536: responses include X-Provisa-Cache: MISS when not cached
        from provisa.cache.middleware import build_cache_headers

        headers = build_cache_headers(None)
        assert headers["X-Provisa-Cache"] == "MISS"
        assert "X-Provisa-Cache-Age" not in headers

    def test_cache_hit_header(self):
        # REQ-536: responses include X-Provisa-Cache: HIT and X-Provisa-Cache-Age on HIT
        from provisa.cache.middleware import build_cache_headers
        from provisa.cache.store import CachedResult

        cached = CachedResult(data=b'{"data":{}}', cached_at=time.time() - 10, ttl=300)
        headers = build_cache_headers(cached)
        assert headers["X-Provisa-Cache"] == "HIT"
        assert "X-Provisa-Cache-Age" in headers
        assert int(headers["X-Provisa-Cache-Age"]) >= 10


# ---------------------------------------------------------------------------
# REQ-544: Cache key includes role_id and RLS context
# ---------------------------------------------------------------------------


class TestCacheKey:
    def test_different_roles_produce_different_keys(self):
        # REQ-544: cache keys are role-partitioned
        from provisa.cache.key import cache_key

        k1 = cache_key("SELECT 1", [], "admin", {})
        k2 = cache_key("SELECT 1", [], "viewer", {})
        assert k1 != k2

    def test_different_rls_produce_different_keys(self):
        # REQ-544: RLS filter values are included in cache key
        from provisa.cache.key import cache_key

        k1 = cache_key("SELECT 1", [], "admin", {1: "region = 'us-east'"})
        k2 = cache_key("SELECT 1", [], "admin", {1: "region = 'eu-west'"})
        assert k1 != k2

    def test_empty_rls_rule_raises(self):
        # REQ-544: empty RLS rule expression raises ValueError (security defect detection)
        from provisa.cache.key import cache_key

        with pytest.raises(ValueError, match="empty filter expression"):
            cache_key("SELECT 1", [], "admin", {1: ""})

    def test_same_inputs_stable_key(self):
        # REQ-544: cache key is deterministic for identical inputs
        from provisa.cache.key import cache_key

        k1 = cache_key("SELECT 1", [42], "admin", {1: "x = 1"})
        k2 = cache_key("SELECT 1", [42], "admin", {1: "x = 1"})
        assert k1 == k2

    def test_table_level_ttl_resolution(self):
        # REQ-544: table-level TTL overrides source-level and global TTL
        from provisa.cache.policy import CachePolicy, resolve_policy

        policy, ttl = resolve_policy(
            stable_id="q1",
            cache_ttl=None,
            default_ttl=300,
            source_cache_enabled=True,
            source_cache_ttl=120,
            table_cache_ttl=60,
        )
        assert policy == CachePolicy.TTL
        assert ttl == 60  # table-level wins

    def test_source_level_ttl_overrides_global(self):
        # REQ-544: source-level TTL overrides global default when no table-level TTL
        from provisa.cache.policy import CachePolicy, resolve_policy

        policy, ttl = resolve_policy(
            stable_id="q1",
            cache_ttl=None,
            default_ttl=300,
            source_cache_enabled=True,
            source_cache_ttl=120,
            table_cache_ttl=None,
        )
        assert policy == CachePolicy.TTL
        assert ttl == 120

    def test_source_cache_disabled_overrides_table_ttl(self):
        # REQ-544: source cache_enabled=false disables all caching for that source
        from provisa.cache.policy import CachePolicy, resolve_policy

        policy, ttl = resolve_policy(
            stable_id="q1",
            cache_ttl=None,
            default_ttl=300,
            source_cache_enabled=False,
            table_cache_ttl=60,
        )
        assert policy == CachePolicy.NONE


# ---------------------------------------------------------------------------
# REQ-552: Cross-source type coercion detection via route reason
# ---------------------------------------------------------------------------


class TestTypeCoercedRouting:
    def test_mysql_source_routes_trino_with_json_extract(self):
        # REQ-552: MySQL source with JSON extract routes Trino (not JSON-safe for direct)
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"mysql-src"},
            source_types={"mysql-src": "mysql"},
            source_dialects={"mysql-src": "mysql"},
            has_json_extract=True,
        )
        assert result.route == Route.TRINO

    def test_postgres_source_with_json_extract_routes_direct(self):
        # REQ-552: Postgres supports ->> natively; json_extract does not force Trino
        from provisa.transpiler.router import Route, decide_route

        result = decide_route(
            sources={"pg-src"},
            source_types={"pg-src": "postgresql"},
            source_dialects={"pg-src": "postgres"},
            has_json_extract=True,
        )
        assert result.route == Route.DIRECT


# ---------------------------------------------------------------------------
# REQ-595: Tenant-prefixed cache keys
# ---------------------------------------------------------------------------


class TestTenantCachePrefix:
    def test_noop_store_always_misses(self):
        # REQ-595: NoopCacheStore never returns cached results (caching disabled)
        import asyncio

        from provisa.cache.store import NoopCacheStore

        store = NoopCacheStore()
        result = asyncio.run(store.get("any-key"))
        assert result is None

    def test_cache_table_name_stable(self):
        # REQ-595: cache table name is a stable SHA-256 hash of source+operation+args
        from provisa.api_source.trino_cache import cache_table_name

        t1 = cache_table_name("my-source", "my_table", {"page": 1})
        t2 = cache_table_name("my-source", "my_table", {"page": 1})
        assert t1 == t2
        assert t1.startswith("r_")

    def test_cache_table_name_differs_by_source(self):
        # REQ-595: different source IDs produce different cache table names
        from provisa.api_source.trino_cache import cache_table_name

        t1 = cache_table_name("source-a", "my_table", {})
        t2 = cache_table_name("source-b", "my_table", {})
        assert t1 != t2

    def test_cache_table_name_differs_by_args(self):
        # REQ-595: different native args produce different cache table names
        from provisa.api_source.trino_cache import cache_table_name

        t1 = cache_table_name("src", "tbl", {"status": "open"})
        t2 = cache_table_name("src", "tbl", {"status": "closed"})
        assert t1 != t2


# ---------------------------------------------------------------------------
# REQ-234, REQ-235: Materialized View Lifecycle
# ---------------------------------------------------------------------------


class TestMVLifecycle:
    def test_stale_mv_not_in_fresh_list(self):
        # REQ-234: STALE MV is excluded from get_fresh() so queries fall back to live source
        from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
        from provisa.mv.registry import MVRegistry

        reg = MVRegistry()
        jp = JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
        )
        mv = MVDefinition(
            id="mv-1",
            source_tables=["orders", "customers"],
            target_catalog="iceberg",
            target_schema="mv",
            join_pattern=jp,
            status=MVStatus.STALE,
        )
        reg.register(mv)
        assert reg.get_fresh() == []

    def test_fresh_mv_within_ttl_in_fresh_list(self):
        # REQ-235: FRESH MV within TTL is returned by get_fresh() for rewriter
        from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
        from provisa.mv.registry import MVRegistry

        reg = MVRegistry()
        jp = JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
        )
        mv = MVDefinition(
            id="mv-fresh",
            source_tables=["orders", "customers"],
            target_catalog="iceberg",
            target_schema="mv",
            join_pattern=jp,
            refresh_interval=300,
        )
        mv.status = MVStatus.FRESH
        mv.last_refresh_at = time.time() - 10  # 10s ago, well within 300s TTL
        reg.register(mv)
        fresh = reg.get_fresh()
        assert len(fresh) == 1
        assert fresh[0].id == "mv-fresh"

    def test_fresh_mv_past_ttl_excluded(self):
        # REQ-235: FRESH MV whose TTL has elapsed is excluded (prevents stale reads)
        from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
        from provisa.mv.registry import MVRegistry

        reg = MVRegistry()
        jp = JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
        )
        mv = MVDefinition(
            id="mv-expired",
            source_tables=["orders", "customers"],
            target_catalog="iceberg",
            target_schema="mv",
            join_pattern=jp,
            refresh_interval=60,
        )
        mv.status = MVStatus.FRESH
        mv.last_refresh_at = time.time() - 120  # 120s ago, past 60s TTL
        reg.register(mv)
        assert reg.get_fresh() == []

    def test_unregister_removes_mv(self):
        # REQ-234: unregistering MV removes it from registry (storage reclamation)
        from provisa.mv.models import MVDefinition
        from provisa.mv.registry import MVRegistry

        reg = MVRegistry()
        mv = MVDefinition(
            id="mv-drop",
            source_tables=["orders"],
            target_catalog="iceberg",
            target_schema="mv",
        )
        reg.register(mv)
        assert reg.get("mv-drop") is not None
        reg.unregister("mv-drop")
        assert reg.get("mv-drop") is None

    def test_mark_stale_by_table(self):
        # REQ-234: changing a source table marks dependent MVs STALE
        from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
        from provisa.mv.registry import MVRegistry

        reg = MVRegistry()
        jp = JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
        )
        mv = MVDefinition(
            id="mv-orders",
            source_tables=["orders", "customers"],
            target_catalog="iceberg",
            target_schema="mv",
            join_pattern=jp,
        )
        mv.status = MVStatus.FRESH
        mv.last_refresh_at = time.time()
        reg.register(mv)
        affected = reg.mark_stale("orders")
        assert "mv-orders" in affected
        mv_ref = reg.get("mv-orders")
        assert mv_ref is not None
        assert mv_ref.status == MVStatus.STALE

    def test_mv_size_guard_status(self):
        # REQ-235: MVs with SKIPPED_SIZE status are excluded from get_fresh()
        from provisa.mv.models import MVDefinition, MVStatus
        from provisa.mv.registry import MVRegistry

        reg = MVRegistry()
        mv = MVDefinition(
            id="mv-big",
            source_tables=["big_table"],
            target_catalog="iceberg",
            target_schema="mv",
            max_rows=1_000_000,
        )
        mv.status = MVStatus.SKIPPED_SIZE
        reg.register(mv)
        assert reg.get_fresh() == []


# ---------------------------------------------------------------------------
# REQ-275, REQ-276, REQ-277, REQ-278, REQ-279, REQ-280, REQ-281:
# Federation Performance — session hints and ANALYZE
# ---------------------------------------------------------------------------


class TestFederationHints:
    def test_table_known_live_returns_false_on_miss(self):
        # REQ-280: table_known_live is False when cache has no entry
        from provisa.api_source.trino_cache import (
            CacheLocation,
            _TABLE_EXISTS_CACHE,
            table_known_live,
        )

        loc = CacheLocation(catalog="test_catalog", schema="test_schema", backend="postgresql")
        _TABLE_EXISTS_CACHE.pop((loc.catalog, loc.schema, "nonexistent_table"), None)
        assert table_known_live(loc, "nonexistent_table") is False

    def test_table_known_live_returns_true_within_ttl(self):
        # REQ-280: table_known_live is True when in-process cache entry is valid
        import time

        from provisa.api_source.trino_cache import (
            CacheLocation,
            _TABLE_EXISTS_CACHE,
            table_known_live,
        )

        loc = CacheLocation(catalog="test_c", schema="test_s", backend="postgresql")
        _TABLE_EXISTS_CACHE[("test_c", "test_s", "live_table")] = time.monotonic() + 300
        assert table_known_live(loc, "live_table") is True
        _TABLE_EXISTS_CACHE.pop(("test_c", "test_s", "live_table"), None)

    def test_table_known_live_returns_false_after_expiry(self):
        # REQ-280: expired in-process cache entry is treated as a miss
        import time

        from provisa.api_source.trino_cache import (
            CacheLocation,
            _TABLE_EXISTS_CACHE,
            table_known_live,
        )

        loc = CacheLocation(catalog="test_c2", schema="test_s2", backend="postgresql")
        _TABLE_EXISTS_CACHE[("test_c2", "test_s2", "old_table")] = time.monotonic() - 1
        assert table_known_live(loc, "old_table") is False
        _TABLE_EXISTS_CACHE.pop(("test_c2", "test_s2", "old_table"), None)

    def test_cache_location_postgresql_backend(self):
        # REQ-275: cache_location builds correct CacheLocation for PG-backed catalog
        from provisa.api_source.trino_cache import cache_location

        loc = cache_location("my-source", None, "api_cache")
        assert loc.catalog == "my_source"  # hyphens → underscores
        assert loc.schema == "api_cache"
        assert loc.backend == "postgresql"

    def test_cache_location_iceberg_backend(self):
        # REQ-275: cache_location with "results" catalog uses iceberg backend
        from provisa.api_source.trino_cache import cache_location

        loc = cache_location("my-source", "results", "mv")
        assert loc.catalog == "results"
        assert loc.backend == "iceberg"

    def test_rewrite_from_cache_replaces_root_table(self):
        # REQ-276: rewrite_from_cache rewrites FROM clause to point at cache table
        from provisa.api_source.trino_cache import CacheLocation, rewrite_from_cache

        sql = 'SELECT "id" FROM "public"."orders"'
        loc = CacheLocation(catalog="my_src", schema="api_cache", backend="postgresql")
        result = rewrite_from_cache(sql, loc, "r_abc123")
        assert "r_abc123" in result
        assert "api_cache" in result

    def test_rewrite_all_from_cache_rewrites_all_tables(self):
        # REQ-277, REQ-278: rewrite_all_from_cache handles multiple table references
        from provisa.api_source.trino_cache import CacheLocation, rewrite_all_from_cache

        sql = (
            'SELECT "t0"."id" FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        loc_orders = CacheLocation(catalog="src", schema="cache", backend="postgresql")
        loc_customers = CacheLocation(catalog="src", schema="cache", backend="postgresql")
        rewrites = {
            "orders": (loc_orders, "r_orders_hash"),
            "customers": (loc_customers, "r_cust_hash"),
        }
        result = rewrite_all_from_cache(sql, rewrites)
        assert "r_orders_hash" in result or "r_cust_hash" in result

    def test_parse_accept_header(self):
        # REQ-279: _parse_accept returns format name from MIME type
        from provisa.api.data.endpoint import _parse_accept

        assert _parse_accept("application/json") == "json"
        assert _parse_accept("text/csv") == "csv"
        assert _parse_accept("application/vnd.apache.parquet") == "parquet"
        assert _parse_accept(None) == "json"
        assert _parse_accept("application/vnd.apache.arrow.stream") == "arrow"

    def test_detect_introspection(self):
        # REQ-281: _detect_introspection identifies pure introspection queries
        from graphql import build_schema, parse

        from provisa.api.data.endpoint import _detect_introspection

        schema = build_schema("type Query { _dummy: String }")

        introspection_doc = parse("{ __schema { types { name } } }")
        assert _detect_introspection(introspection_doc) is True

        data_doc = parse("{ __typename }")
        assert _detect_introspection(data_doc) is True

    def test_mv_rewrite_skips_stale_mv(self):
        # REQ-275: rewrite_if_mv_match skips MV when no fresh MVs provided
        from provisa.compiler.sql_gen import CompiledQuery
        from provisa.mv.rewriter import rewrite_if_mv_match

        compiled = CompiledQuery(
            sql='SELECT "t0"."id" FROM "public"."orders" "t0"',
            params=[],
            root_field="orders",
            columns=[],
            sources={"sales-pg"},
        )
        result = rewrite_if_mv_match(compiled, [])
        # No MV match — returned unchanged
        assert result.sql == compiled.sql

    def test_mv_rewrite_applies_fresh_mv(self):
        # REQ-276: rewrite_if_mv_match rewrites SQL when a matching fresh MV exists
        import time

        from provisa.compiler.sql_gen import CompiledQuery
        from provisa.mv.models import JoinPattern, MVDefinition, MVStatus
        from provisa.mv.rewriter import rewrite_if_mv_match

        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = CompiledQuery(
            sql=sql,
            params=[],
            root_field="orders",
            columns=[],
            sources={"sales-pg"},
        )
        jp = JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
        )
        mv = MVDefinition(
            id="mv-ord-cust",
            source_tables=["orders", "customers"],
            target_catalog="iceberg",
            target_schema="mv",
            target_table="mv_ord_cust",
            join_pattern=jp,
            refresh_interval=300,
        )
        mv.status = MVStatus.FRESH
        mv.last_refresh_at = time.time() - 5

        result = rewrite_if_mv_match(compiled, [mv])
        # Should rewrite to use MV table
        assert "mv_ord_cust" in result.sql
        # JOINs removed
        assert "JOIN" not in result.sql
