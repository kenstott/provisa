# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Fallback-cache invalidation (REQ-425).

Decides when the pgvector fallback cache (REQ-424) is stale and must be refreshed:
TTL expiry, a mutation on the source table, a manual refresh request, or row-count
drift between source and cache.
"""

# Requirements: REQ-425

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class InvalidationReason(str, Enum):  # REQ-425
    TTL = "ttl"
    MUTATION = "mutation"
    MANUAL = "manual"
    DRIFT = "drift"


@dataclass
class CacheState:  # REQ-425
    """What the invalidation check needs to know about a cache entry."""

    last_refresh_ts: float | None  # epoch seconds of the last successful refresh
    ttl_seconds: int | None  # None = never expires on TTL
    source_row_count: int
    cache_row_count: int
    mutated_since_refresh: bool = False
    manual_refresh_requested: bool = False


def is_ttl_expired(state: CacheState, now_ts: float) -> bool:  # REQ-425
    if state.ttl_seconds is None:
        return False
    if state.last_refresh_ts is None:
        return True  # never refreshed
    return (now_ts - state.last_refresh_ts) >= state.ttl_seconds


def has_drift(state: CacheState) -> bool:  # REQ-425
    """Row-count drift between source and cache signals a stale cache."""
    return state.source_row_count != state.cache_row_count


def invalidation_reason(state: CacheState, now_ts: float) -> InvalidationReason | None:  # REQ-425
    """Return why the cache should be refreshed, or None if it is still valid.

    Precedence: manual > mutation > TTL > drift.
    """
    if state.manual_refresh_requested:
        return InvalidationReason.MANUAL
    if state.mutated_since_refresh:
        return InvalidationReason.MUTATION
    if is_ttl_expired(state, now_ts):
        return InvalidationReason.TTL
    if has_drift(state):
        return InvalidationReason.DRIFT
    return None


def needs_refresh(state: CacheState, now_ts: float) -> bool:  # REQ-425
    return invalidation_reason(state, now_ts) is not None
