# Copyright (c) 2026 Kenneth Stott
# Canary: 882d87c0-053c-4924-a69e-4e1fd6224f0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for cache key generation."""

from provisa.cache.key import cache_key, is_cacheable


class TestCacheKey:
    def test_same_inputs_same_key(self):
        k1 = cache_key("SELECT 1", [1, "us"], "analyst", {1: "region = 'us'"})
        k2 = cache_key("SELECT 1", [1, "us"], "analyst", {1: "region = 'us'"})
        assert k1 == k2

    def test_different_role_different_key(self):
        k1 = cache_key("SELECT 1", [], "admin", {})
        k2 = cache_key("SELECT 1", [], "analyst", {})
        assert k1 != k2

    def test_different_rls_different_key(self):
        k1 = cache_key("SELECT 1", [], "analyst", {1: "region = 'us'"})
        k2 = cache_key("SELECT 1", [], "analyst", {1: "region = 'eu'"})
        assert k1 != k2

    def test_different_params_different_key(self):
        k1 = cache_key("SELECT 1", [1], "admin", {})
        k2 = cache_key("SELECT 1", [2], "admin", {})
        assert k1 != k2

    def test_different_sql_different_key(self):
        k1 = cache_key("SELECT 1", [], "admin", {})
        k2 = cache_key("SELECT 2", [], "admin", {})
        assert k1 != k2

    def test_empty_rls_allowed(self):
        k = cache_key("SELECT 1", [], "admin", {})
        assert isinstance(k, str) and len(k) == 64  # SHA-256 hex digest

    def test_key_is_sha256_hex(self):
        k = cache_key("SELECT 1", [], "admin", {})
        assert len(k) == 64
        int(k, 16)  # should not raise

    def test_rls_order_independent(self):
        k1 = cache_key("SELECT 1", [], "a", {1: "x", 2: "y"})
        k2 = cache_key("SELECT 1", [], "a", {2: "y", 1: "x"})
        assert k1 == k2


class TestCacheKeyNormalization:  # REQ-864
    def test_whitespace_and_case_share_a_key(self):
        k1 = cache_key("SELECT a, b FROM t WHERE x = 1", [], "r", {})
        k2 = cache_key("select   A,\n  B\nfrom T\nwhere X = 1", [], "r", {})
        assert k1 == k2

    def test_commutable_predicate_order_shares_a_key(self):
        k1 = cache_key('SELECT "a" FROM "t" WHERE "x" = 1 AND "y" = 2', [], "r", {})
        k2 = cache_key('SELECT "a" FROM "t" WHERE "y" = 2 AND "x" = 1', [], "r", {})
        assert k1 == k2

    def test_distinct_literal_values_do_not_collapse(self):
        # Isolation-preserving: different predicate VALUES must stay distinct (REQ-866).
        k1 = cache_key("SELECT a FROM t WHERE tenant_id = 'acme'", [], "r", {})
        k2 = cache_key("SELECT a FROM t WHERE tenant_id = 'beta'", [], "r", {})
        assert k1 != k2

    def test_unparseable_sql_still_deterministic(self):
        # Falls back to raw text; distinct raw text → distinct key (miss, never wrong hit).
        weird = ">>> not sql <<<"
        assert cache_key(weird, [], "r", {}) == cache_key(weird, [], "r", {})
        assert cache_key(weird, [], "r", {}) != cache_key(weird + "!", [], "r", {})


class TestIsCacheable:  # REQ-866 fail-closed
    def test_plain_query_is_cacheable(self):
        ok, _ = is_cacheable("SELECT 1", {})
        assert ok is True

    def test_resolved_rls_is_cacheable(self):
        ok, _ = is_cacheable("SELECT a FROM t WHERE region = 'us'", {1: "region = 'us'"})
        assert ok is True

    def test_empty_rls_filter_not_cacheable(self):
        ok, reason = is_cacheable("SELECT 1", {1: ""})
        assert ok is False and "empty" in reason

    def test_whitespace_rls_filter_not_cacheable(self):
        ok, _ = is_cacheable("SELECT 1", {1: "   "})
        assert ok is False

    def test_current_setting_in_rls_not_cacheable(self):
        ok, reason = is_cacheable("SELECT 1", {1: "tenant_id = current_setting('provisa.tenant')"})
        assert ok is False and "session state" in reason

    def test_current_setting_in_sql_not_cacheable(self):
        ok, _ = is_cacheable("SELECT a FROM t WHERE u = current_setting('provisa.user_id')", {})
        assert ok is False
