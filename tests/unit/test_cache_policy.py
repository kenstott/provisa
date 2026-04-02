# Copyright (c) 2025 Kenneth Stott
# Canary: 49ff3585-be6b-47b3-b71c-85d2d4c73215
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for cache policy resolution."""

from provisa.cache.policy import CachePolicy, resolve_policy


class TestResolvePolicy:
    def test_unapproved_query_returns_none(self):
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
