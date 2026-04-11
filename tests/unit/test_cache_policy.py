# Copyright (c) 2026 Kenneth Stott
# Canary: 4ef13187-873b-46ac-bcf2-e5f036173aa4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for hierarchical cache policy resolution."""

from provisa.cache.policy import CachePolicy, resolve_policy


class TestResolvePolicy:
    def test_non_approved_query_returns_none(self):
        policy, ttl = resolve_policy(stable_id=None, cache_ttl=None)
        assert policy == CachePolicy.NONE
        assert ttl == 0

    def test_approved_with_explicit_ttl(self):
        policy, ttl = resolve_policy(stable_id="abc-123", cache_ttl=600)
        assert policy == CachePolicy.TTL
        assert ttl == 600

    def test_approved_with_zero_ttl_disables_caching(self):
        policy, ttl = resolve_policy(stable_id="abc-123", cache_ttl=0)
        assert policy == CachePolicy.NONE
        assert ttl == 0

    def test_approved_with_negative_ttl_disables_caching(self):
        policy, ttl = resolve_policy(stable_id="abc-123", cache_ttl=-1)
        assert policy == CachePolicy.NONE
        assert ttl == 0

    def test_approved_without_ttl_uses_default(self):
        policy, ttl = resolve_policy(stable_id="abc-123", cache_ttl=None)
        assert policy == CachePolicy.TTL
        assert ttl == 300  # default

    def test_custom_default_ttl(self):
        policy, ttl = resolve_policy(stable_id="abc-123", cache_ttl=None, default_ttl=120)
        assert policy == CachePolicy.TTL
        assert ttl == 120

    def test_test_mode_ad_hoc_not_cached(self):
        policy, ttl = resolve_policy(stable_id=None, cache_ttl=300)
        assert policy == CachePolicy.NONE


class TestHierarchicalTTL:
    def test_table_ttl_overrides_source_ttl(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=None,
            source_cache_ttl=600, table_cache_ttl=60,
        )
        assert policy == CachePolicy.TTL
        assert ttl == 60

    def test_table_ttl_overrides_query_ttl(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=300,
            table_cache_ttl=120,
        )
        assert policy == CachePolicy.TTL
        assert ttl == 120

    def test_source_ttl_overrides_global_default(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=None,
            source_cache_ttl=600, default_ttl=300,
        )
        assert policy == CachePolicy.TTL
        assert ttl == 600

    def test_query_ttl_overrides_source_ttl(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=120,
            source_cache_ttl=600,
        )
        assert policy == CachePolicy.TTL
        assert ttl == 120

    def test_no_overrides_uses_global_default(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=None,
            source_cache_ttl=None, table_cache_ttl=None,
            default_ttl=300,
        )
        assert policy == CachePolicy.TTL
        assert ttl == 300

    def test_source_cache_disabled_returns_none(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=300,
            source_cache_enabled=False,
            source_cache_ttl=600, table_cache_ttl=60,
        )
        assert policy == CachePolicy.NONE
        assert ttl == 0

    def test_source_cache_disabled_ignores_all_ttls(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=None,
            source_cache_enabled=False,
            table_cache_ttl=120,
        )
        assert policy == CachePolicy.NONE

    def test_table_ttl_zero_disables_caching(self):
        policy, ttl = resolve_policy(
            stable_id="abc", cache_ttl=300,
            source_cache_ttl=600, table_cache_ttl=0,
        )
        assert policy == CachePolicy.NONE
        assert ttl == 0
