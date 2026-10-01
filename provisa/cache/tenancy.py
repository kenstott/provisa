# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f6a21-4e9b-4c70-b1d5-2a7e9f0c3b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org a response-cache entry belongs to, and the one way to invalidate by table (REQ-595).

Every response-cache read, write and invalidation — GraphQL, every raw-SQL/compiled plan, every
write path — uses the same tenant: the org the request is acting in (``current_org``), else the
deployment's own org. Invalidating under any other tenant (e.g. none) targets a different index
and leaves the acting org's entries stale.
"""

# Requirements: REQ-595, REQ-544

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def cache_tenant(state: Any) -> str:
    """The acting org — the prefix every response-cache key and table index is written under."""
    from provisa.core.request_context import current_org

    return current_org.get() or state.org_id


async def invalidate_tables(state: Any, table_ids: Iterable[int]) -> int:
    """Drop every response-cache entry indexed under ``table_ids`` for the acting org; returns how
    many were dropped. A store failure raises (``RedisCacheStore.invalidate_by_table``): a write
    must not leave stale entries."""
    tenant = cache_tenant(state)
    dropped = 0
    for table_id in set(table_ids):
        dropped += await state.response_cache_store.invalidate_by_table(table_id, tenant_id=tenant)
    return dropped
