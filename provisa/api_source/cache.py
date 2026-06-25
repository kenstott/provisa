# Copyright (c) 2026 Kenneth Stott
# Canary: aaba33c7-dfa7-4166-acfc-b183d0bd1f2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""TTL resolution for API source caches (Phase U)."""

# Requirements: REQ-309, REQ-318, REQ-327

from __future__ import annotations

# Global default TTL (seconds)
DEFAULT_TTL = 300


def resolve_ttl(
    endpoint_ttl: int | None = None,
    source_ttl: int | None = None,
    global_ttl: int | None = None,
) -> int:  # REQ-309, REQ-318, REQ-327
    """Resolve TTL: endpoint > source > global default (300s)."""
    if endpoint_ttl is not None:
        return endpoint_ttl
    if source_ttl is not None:
        return source_ttl
    if global_ttl is not None:
        return global_ttl
    return DEFAULT_TTL
