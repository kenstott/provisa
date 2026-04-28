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

import pytest

from provisa.cache.key import cache_key


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

    def test_empty_rls_filter_raises(self):
        with pytest.raises(ValueError, match="empty filter expression"):
            cache_key("SELECT 1", [], "analyst", {1: ""})

    def test_whitespace_only_rls_filter_raises(self):
        with pytest.raises(ValueError, match="empty filter expression"):
            cache_key("SELECT 1", [], "analyst", {1: "   "})

    def test_key_is_sha256_hex(self):
        k = cache_key("SELECT 1", [], "admin", {})
        assert len(k) == 64
        int(k, 16)  # should not raise

    def test_rls_order_independent(self):
        k1 = cache_key("SELECT 1", [], "a", {1: "x", 2: "y"})
        k2 = cache_key("SELECT 1", [], "a", {2: "y", 1: "x"})
        assert k1 == k2
