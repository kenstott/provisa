# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f6a21-4e9b-4c70-b1d5-2a7e9f0c3b18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The scope a response-cache entry belongs to, and the one way to invalidate by table (REQ-595).

Every response-cache read, write and invalidation — GraphQL, every raw-SQL/compiled plan, every
write path — is made under the same scope: the org the request is acting in (``current_org``,
else the deployment's own org), the environment it is acting in, and the model that runtime
loaded. Invalidating under any other scope (e.g. none) targets a different index and leaves the
acting org's entries stale.

Why the model is part of it. An entry's key is the governed statement, its values and the role.
That text carries the role's row filters, masks and visible columns, so a change to any of those
is a different key. It does not carry what is resolved AFTER governance: a view is named in the
statement and expanded later, a table is named and bound to its physical relation later. A view
redefined to leave rows out would go on being answered from the entry computed under its old
definition. The control plane's model stamp (REQ-1914) is advanced by every change to the model,
and each runtime records the stamp its copy was loaded at, so scoping an entry to that stamp makes
it readable only by a runtime holding the same model: workers that loaded the same model share
entries, a worker that reloads moves onto new ones, and the old ones expire by their TTL.

Why the environment is part of it. An environment is its own copy of the model reaching its
sources through its own bindings (REQ-1529), and its stamp counts separately from its base's, so
org and stamp alone do not tell the two apart.
"""

# Requirements: REQ-595, REQ-544, REQ-1914, REQ-1529

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def cache_place(state: Any) -> str:
    """The acting org and environment: whose cached data this is, whatever model it holds."""
    from provisa.api.org_runtime import runtime_key
    from provisa.core.request_context import current_env, require_current_org

    return runtime_key(require_current_org(), current_env.get())


def cache_tenant(state: Any) -> str:
    """The acting org, environment and loaded model — the prefix every response-cache key and
    table index is written under."""
    return f"{cache_place(state)}:m{state.model_stamp}"


async def purge_acting_place(state: Any) -> int:
    """Remove every response-cache entry of the org and environment the request is acting in,
    under every model that runtime has held; returns how many. Another org's entries, and
    another environment's, are not touched: there is no deployment-wide purge."""
    return await state.response_cache_store.purge_place(cache_place(state))


def acting_scope() -> str:
    """:func:`cache_tenant` for the process's own state: for a caller that keeps a cache and
    was not handed the state (a source adapter, the API cache's table naming)."""
    from provisa.api.app import state

    return cache_tenant(state)


async def invalidate_tables(state: Any, table_ids: Iterable[int]) -> int:
    """Drop every response-cache entry indexed under ``table_ids`` for the acting org; returns how
    many were dropped. A store failure raises (``RedisCacheStore.invalidate_by_table``): a write
    must not leave stale entries."""
    tenant = cache_tenant(state)
    dropped = 0
    for table_id in set(table_ids):
        dropped += await state.response_cache_store.invalidate_by_table(table_id, tenant_id=tenant)
    return dropped
