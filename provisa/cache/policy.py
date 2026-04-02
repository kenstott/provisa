# Copyright (c) 2025 Kenneth Stott
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
) -> tuple[CachePolicy, int]:
    """Resolve caching policy for a query.

    Args:
        stable_id: Approved query stable_id (None for ad-hoc/test queries).
        cache_ttl: Steward-specified TTL for this query (from approved metadata).
        default_ttl: Global default TTL when query has no explicit TTL.

    Returns:
        (policy, ttl_seconds) tuple.
    """
    # Unapproved / test-mode queries are never cached
    if stable_id is None:
        return CachePolicy.NONE, 0

    # Approved query with explicit TTL=0 means caching disabled for this query
    if cache_ttl is not None and cache_ttl <= 0:
        return CachePolicy.NONE, 0

    # Approved query with explicit TTL
    if cache_ttl is not None:
        return CachePolicy.TTL, cache_ttl

    # Approved query with no explicit TTL → use default
    return CachePolicy.TTL, default_ttl
