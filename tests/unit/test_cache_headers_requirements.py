# Copyright (c) 2026 Kenneth Stott
# Canary: 59338a86-8ddf-4ec8-b0c3-f5cafc00ff90
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for cache header requirements: REQ-536"""

from __future__ import annotations

import time


# ---------------------------------------------------------------------------
# REQ-536: All data responses include cache status headers:
# `X-Provisa-Cache: HIT|MISS` on every response, and
# `X-Provisa-Cache-Age: <seconds>` on cache HITs only.
# ---------------------------------------------------------------------------


def test_build_cache_headers_miss_when_no_cached_result():
    # REQ-536: When no cached result, X-Provisa-Cache must be MISS.
    from provisa.cache.middleware import build_cache_headers

    headers = build_cache_headers(None)
    assert headers["X-Provisa-Cache"] == "MISS"


def test_build_cache_headers_miss_has_no_age():
    # REQ-536: X-Provisa-Cache-Age must NOT be present on a MISS.
    from provisa.cache.middleware import build_cache_headers

    headers = build_cache_headers(None)
    assert "X-Provisa-Cache-Age" not in headers


def test_build_cache_headers_hit_when_cached_result_present():
    # REQ-536: When a cached result is available, X-Provisa-Cache must be HIT.
    from unittest.mock import MagicMock
    from provisa.cache.middleware import build_cache_headers

    cached = MagicMock()
    cached.age_seconds = 10
    headers = build_cache_headers(cached)
    assert headers["X-Provisa-Cache"] == "HIT"


def test_build_cache_headers_hit_includes_age():
    # REQ-536: Cache HIT response must include X-Provisa-Cache-Age.
    from unittest.mock import MagicMock
    from provisa.cache.middleware import build_cache_headers

    cached = MagicMock()
    cached.age_seconds = 42
    headers = build_cache_headers(cached)
    assert "X-Provisa-Cache-Age" in headers
    assert headers["X-Provisa-Cache-Age"] == "42"


def test_build_cache_headers_age_is_string():
    # REQ-536: X-Provisa-Cache-Age must be a string (HTTP header value).
    from unittest.mock import MagicMock
    from provisa.cache.middleware import build_cache_headers

    cached = MagicMock()
    cached.age_seconds = 5
    headers = build_cache_headers(cached)
    assert isinstance(headers["X-Provisa-Cache-Age"], str)


def test_build_cache_headers_age_reflects_cache_age_seconds():
    # REQ-536: X-Provisa-Cache-Age value matches CachedResult.age_seconds.
    from unittest.mock import MagicMock
    from provisa.cache.middleware import build_cache_headers

    for age in (0, 1, 120, 3600):
        cached = MagicMock()
        cached.age_seconds = age
        headers = build_cache_headers(cached)
        assert headers["X-Provisa-Cache-Age"] == str(age)


def test_cached_result_age_seconds_computed_from_cached_at():
    # REQ-536: CachedResult.age_seconds computes seconds since cached_at.
    from provisa.cache.store import CachedResult

    cached_at = time.time() - 30
    result = CachedResult(data=b"[]", cached_at=cached_at, ttl=60)
    age = result.age_seconds
    assert 29 <= age <= 32


def test_cache_headers_always_present_on_miss():
    # REQ-536: X-Provisa-Cache header always present, even on MISS.
    from provisa.cache.middleware import build_cache_headers

    headers = build_cache_headers(None)
    assert "X-Provisa-Cache" in headers


def test_cache_headers_always_present_on_hit():
    # REQ-536: X-Provisa-Cache header always present on HIT too.
    from unittest.mock import MagicMock
    from provisa.cache.middleware import build_cache_headers

    cached = MagicMock()
    cached.age_seconds = 0
    headers = build_cache_headers(cached)
    assert "X-Provisa-Cache" in headers
