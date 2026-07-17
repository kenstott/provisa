# Copyright (c) 2026 Kenneth Stott
# Canary: b19e55b6-44f0-44f2-b97a-68708dfd4a65
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for cache requirements: REQ-595"""

from __future__ import annotations


from provisa.cache.store import RedisCacheStore
from provisa.apq.cache import RedisAPQCache


# ---------------------------------------------------------------------------
# REQ-595: Tenant-prefixed cache keys
# ---------------------------------------------------------------------------


class TestREQ595CacheKeyPrefixing:
    """In multi-tenant mode, RedisCacheStore prefixes every cache key with
    the tenant_id (provisa:cache:<tenant_id>:<sha256_key>).
    Table invalidation keys follow provisa:table:<tenant_id>:<table_id>.
    The APQ cache uses provisa:apq:<tenant_id>:<sha256_hex>.
    Default APQ TTL is 86400 seconds, configurable via PROVISA_APQ_TTL.
    One tenant's cache flush never touches another tenant's cached results.
    """

    # REQ-595
    def test_cache_key_includes_tenant_prefix(self):
        """Cache key for a tenant includes provisa:cache:<tenant_id>:<key>."""
        store = RedisCacheStore("redis://localhost:6379/0")
        result = store._prefixed_key("abc123", "tenant_a")
        assert result == "provisa:cache:tenant_a:abc123"

    # REQ-595
    def test_cache_key_without_tenant_has_no_tenant_segment(self):
        """Single-tenant cache key is provisa:cache:<key> (no tenant segment)."""
        store = RedisCacheStore("redis://localhost:6379/0")
        result = store._prefixed_key("abc123", None)
        assert result == "provisa:cache:abc123"

    # REQ-595
    def test_table_invalidation_key_includes_tenant(self):
        """Table invalidation key format is provisa:table:<tenant_id>:<table_id>."""
        store = RedisCacheStore("redis://localhost:6379/0")
        result = store._prefixed_table_key(42, "tenant_b")
        assert result == "provisa:table:tenant_b:42"

    # REQ-595
    def test_table_invalidation_key_without_tenant(self):
        """Single-tenant table invalidation key is provisa:table:<table_id>."""
        store = RedisCacheStore("redis://localhost:6379/0")
        result = store._prefixed_table_key(42, None)
        assert result == "provisa:table:42"

    # REQ-595
    def test_apq_cache_key_includes_tenant_prefix(self):
        """APQ cache key for a tenant is provisa:apq:<tenant_id>:<sha256_hex>."""
        apq = RedisAPQCache("redis://localhost:6379/0")
        result = apq._build_key("deadbeef", "tenant_c")
        assert result == "provisa:apq:tenant_c:deadbeef"

    # REQ-595
    def test_apq_cache_key_without_tenant(self):
        """Single-tenant APQ cache key is provisa:apq:<sha256_hex>."""
        apq = RedisAPQCache("redis://localhost:6379/0")
        result = apq._build_key("deadbeef", None)
        assert result == "provisa:apq:deadbeef"

    # REQ-595
    def test_apq_default_ttl_is_86400_seconds(self):
        """Default APQ TTL is 86400 seconds (24 hours)."""
        apq = RedisAPQCache("redis://localhost:6379/0")
        assert apq._ttl == 86400

    # REQ-595
    def test_apq_ttl_configurable_via_env(self, monkeypatch):
        """APQ TTL is configurable via PROVISA_APQ_TTL environment variable."""
        monkeypatch.setenv("PROVISA_APQ_TTL", "3600")
        import importlib
        import provisa.apq.cache as apq_module

        importlib.reload(apq_module)
        assert apq_module._DEFAULT_TTL == 3600
        # Restore
        importlib.reload(apq_module)

    # REQ-595
    def test_different_tenants_produce_different_cache_keys(self):
        """Two tenants with the same raw key produce different prefixed cache keys."""
        store = RedisCacheStore("redis://localhost:6379/0")
        key_a = store._prefixed_key("same_key", "tenant_a")
        key_b = store._prefixed_key("same_key", "tenant_b")
        assert key_a != key_b

    # REQ-595
    def test_different_tenants_produce_different_table_keys(self):
        """Two tenants with the same table_id produce different table invalidation keys."""
        store = RedisCacheStore("redis://localhost:6379/0")
        tkey_a = store._prefixed_table_key(7, "tenant_a")
        tkey_b = store._prefixed_table_key(7, "tenant_b")
        assert tkey_a != tkey_b

    # REQ-595
    def test_different_tenants_produce_different_apq_keys(self):
        """Two tenants with the same hash produce different APQ cache keys."""
        apq = RedisAPQCache("redis://localhost:6379/0")
        key_a = apq._build_key("cafebabe", "tenant_a")
        key_b = apq._build_key("cafebabe", "tenant_b")
        assert key_a != key_b

    # REQ-595
    def test_cache_key_tenant_not_a_prefix_of_another_tenant(self):
        """A tenant whose id is a prefix of another tenant's id still produces distinct keys."""
        store = RedisCacheStore("redis://localhost:6379/0")
        key_short = store._prefixed_key("k", "abc")
        key_long = store._prefixed_key("k", "abcd")
        assert key_short != key_long

    # REQ-595
    def test_apq_key_tenant_not_a_prefix_of_another_tenant(self):
        """APQ: tenant whose id is a prefix of another tenant's id still produces distinct keys."""
        apq = RedisAPQCache("redis://localhost:6379/0")
        key_short = apq._build_key("hash", "abc")
        key_long = apq._build_key("hash", "abcd")
        assert key_short != key_long
