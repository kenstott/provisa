# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-544 — Hierarchical Cache TTL and security-partitioned cache keys."""

from __future__ import annotations

import pytest
import pytest_asyncio
from pytest_bdd import given, when, then, parsers, scenario

from provisa.cache.policy import CachePolicy, resolve_policy
from provisa.cache.key import cache_key


@pytest.fixture
def shared_data() -> dict:
    return {}


@given("cache TTL configured at table, source, and global levels")
def cache_ttl_configured(shared_data):
    # Configure a hierarchy: table=60, source=600, global default=300
    shared_data["table_cache_ttl"] = 60
    shared_data["source_cache_ttl"] = 600
    shared_data["default_ttl"] = 300
    shared_data["source_cache_enabled"] = True
    shared_data["cache_ttl"] = None
    shared_data["stable_id"] = "abc-123"

    # Security context for cache key partitioning
    shared_data["sql"] = "SELECT id, name FROM customers WHERE region = ?"
    shared_data["params"] = ["EU"]
    shared_data["rls_rules"] = {42: "tenant_id = 7"}

    assert shared_data["table_cache_ttl"] is not None
    assert shared_data["source_cache_ttl"] is not None
    assert shared_data["default_ttl"] is not None


@when("a cache key is resolved")
def resolve_cache_key_and_policy(shared_data):
    # Resolve the TTL policy across the hierarchy
    policy, ttl = resolve_policy(
        stable_id=shared_data["stable_id"],
        cache_ttl=shared_data["cache_ttl"],
        default_ttl=shared_data["default_ttl"],
        source_cache_enabled=shared_data["source_cache_enabled"],
        source_cache_ttl=shared_data["source_cache_ttl"],
        table_cache_ttl=shared_data["table_cache_ttl"],
    )
    shared_data["resolved_policy"] = policy
    shared_data["resolved_ttl"] = ttl

    # Resolve cache keys for two distinct roles to validate security partitioning
    key_role_a = cache_key(
        sql=shared_data["sql"],
        params=shared_data["params"],
        role_id="role-analyst",
        rls_rules=shared_data["rls_rules"],
    )
    key_role_b = cache_key(
        sql=shared_data["sql"],
        params=shared_data["params"],
        role_id="role-admin",
        rls_rules=shared_data["rls_rules"],
    )
    # Same role, different RLS context
    key_role_a_other_rls = cache_key(
        sql=shared_data["sql"],
        params=shared_data["params"],
        role_id="role-analyst",
        rls_rules={42: "tenant_id = 99"},
    )
    shared_data["key_role_a"] = key_role_a
    shared_data["key_role_b"] = key_role_b
    shared_data["key_role_a_other_rls"] = key_role_a_other_rls


@then(
    "table-level TTL takes precedence, and cache keys include role_id and RLS context for"
    " security partitioning"
)
def assert_table_ttl_and_partitioning(shared_data):
    # Table-level TTL (60) wins over source (600) and global default (300)
    assert shared_data["resolved_policy"] == CachePolicy.TTL
    assert shared_data["resolved_ttl"] == 60, (
        f"Expected table-level TTL 60 to win, got {shared_data['resolved_ttl']}"
    )

    # role_id is part of the key: different roles → different keys
    assert shared_data["key_role_a"] != shared_data["key_role_b"], (
        "Cache keys must differ across roles to prevent cross-role leakage"
    )

    # RLS context is part of the key: same role, different RLS → different keys
    assert shared_data["key_role_a"] != shared_data["key_role_a_other_rls"], (
        "Cache keys must differ when RLS context differs"
    )

    # Keys are deterministic SHA-256 hex digests
    assert len(shared_data["key_role_a"]) == 64
    assert all(c in "0123456789abcdef" for c in shared_data["key_role_a"])


@then("source-level cache disable overrides all table TTLs")
def assert_source_disable_overrides(shared_data):
    policy, ttl = resolve_policy(
        stable_id=shared_data["stable_id"],
        cache_ttl=shared_data["cache_ttl"],
        default_ttl=shared_data["default_ttl"],
        source_cache_enabled=False,
        source_cache_ttl=shared_data["source_cache_ttl"],
        table_cache_ttl=shared_data["table_cache_ttl"],
    )
    assert policy == CachePolicy.NONE
    assert ttl == 0


@then(parsers.parse("an unresolved RLS context raises a security error"))
def assert_unresolved_rls_raises(shared_data):
    with pytest.raises(ValueError, match="unresolved RLS context"):
        cache_key(
            sql=shared_data["sql"],
            params=shared_data["params"],
            role_id="role-analyst",
            rls_rules={42: "   "},
        )


@scenario(
    "REQ-544.feature",
    "REQ-544 default behaviour",
)
def test_req_544_default_behaviour():
    """Hierarchical TTL resolution and security-partitioned cache keys."""
