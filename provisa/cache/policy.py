# Copyright (c) 2026 Kenneth Stott
# Canary: 8d6d5c51-8e1f-4e21-9052-057ee11cd102
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Caching policy resolution (REQ-079, REQ-080).

Determines whether a query result should be cached and with what TTL.
Supports hierarchical TTL: table → source → global default (most specific wins).
"""

from __future__ import annotations

from enum import Enum


class CachePolicy(str, Enum):
    NONE = "none"  # no caching
    TTL = "ttl"  # cache with TTL


def resolve_policy(
    stable_id: str | None,
    cache_ttl: int | None,
    default_ttl: int = 300,
    *,
    source_cache_enabled: bool = True,
    source_cache_ttl: int | None = None,
    table_cache_ttl: int | None = None,
) -> tuple[CachePolicy, int]:
    """Resolve caching policy for a query.

    TTL resolution order (most specific wins):
      1. table_cache_ttl (per-table override)
      2. cache_ttl (steward-specified per-query TTL)
      3. source_cache_ttl (per-source override)
      4. default_ttl (global default)

    Args:
        stable_id: Approved query stable_id (None for ad-hoc/test queries).
        cache_ttl: Steward-specified TTL for this query (from approved metadata).
        default_ttl: Global default TTL when no explicit TTL is set.
        source_cache_enabled: Whether caching is enabled for the source.
        source_cache_ttl: Source-level TTL override (None = inherit global).
        table_cache_ttl: Table-level TTL override (None = inherit source/global).

    Returns:
        (policy, ttl_seconds) tuple.
    """
    # Non-approved / test-mode queries are never cached
    if stable_id is None:
        return CachePolicy.NONE, 0

    # Source-level cache disabled → no caching for any table in this source
    if not source_cache_enabled:
        return CachePolicy.NONE, 0

    # Resolve TTL: table → query → source → global (first non-null wins)
    resolved_ttl = (
        table_cache_ttl
        if table_cache_ttl is not None else cache_ttl
        if cache_ttl is not None else source_cache_ttl
        if source_cache_ttl is not None else default_ttl
    )

    # TTL ≤ 0 means caching disabled
    if resolved_ttl <= 0:
        return CachePolicy.NONE, 0

    return CachePolicy.TTL, resolved_ttl
