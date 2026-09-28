# Copyright (c) 2026 Kenneth Stott
# Canary: 11079bd4-af4e-4024-bcd4-15475e054ca8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""In-memory, per-org, generation-keyed cache of `registered_tables` (REQ-1882).

`registered_tables`/`row_materialized_tables_by_name` (`provisa/federation/registry_view.py`,
`provisa/federation/query_residency.py`) are read from `fetch_tables` on every single governed
query, with no cache — confirmed by reading every call site (`_pipeline.py`, `query_residency.py`,
`row_materialize_lifecycle.py`, `app_wiring.py`, `cypher_router.py`, `backend.py`) before adding
this. Same invalidation posture as REQ-1877's `compiled_query_cache.py`: keyed on
`(schema_boot_id, schema_version)`, the same generation pair `register_table`/`update_table`
already bump via `_rebuild_schemas` on every table registration/mutation (confirmed by reading
`provisa/api/admin/schema_mutation.py` and `provisa/api/app.py`'s `state.schema_version += 1`) — a
schema/table change invalidates every cached entry for every role at once, so this can never serve
a stale table list past the next rebuild.

Scope: ONLY the pool-acquire path (`conn is None`) in `registered_tables` — a caller passing its
own `conn` is already inside an explicit transaction/connection reuse and is left uncached, exactly
as before.

Keyed additionally by `current_org` (REQ-1266): `schema_boot_id`/`schema_version` live on the
process-global `AppState`, not per-org, so the generation alone cannot separate two orgs'
registered-table lists — the org id is folded into the key so one org's cached rows can never be
served for another's request.

The cache instance itself lives ON the `state` object passed to `registered_tables`
(`get_cache_for`, lazily attached), the same per-instance scoping `compiled_query_cache.py` already
uses for `OrgRuntime.compiled_query_cache` — never a module-global singleton, so two unrelated
`state`/`AppState` instances (e.g. two unit tests, or two independently-booted processes) never
share cached rows through this module.
"""

# Requirements: REQ-1882

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

_DEFAULT_TTL_SECONDS = 5.0
_MAX_ENTRIES = 8


@dataclass
class _Entry:
    tables: list[Any]
    expires_at: float


@dataclass
class RegisteredTablesCache:
    """A per-org, in-memory, TTL-evicted cache of `registered_tables`' return value.

    TTL is a short backstop (5s), not the primary invalidation mechanism — the generation key
    (`schema_boot_id`, `schema_version`) is what actually invalidates on a real schema/table
    change; the TTL only bounds how long a cache entry can be reused when the generation hasn't
    moved, matching the read-mostly nature of the registry between rebuilds.
    """

    ttl_seconds: float = _DEFAULT_TTL_SECONDS
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _entries: dict[tuple[str | None, str, int], _Entry] = field(default_factory=dict)

    def get(self, generation: tuple[str | None, str, int]) -> list[Any] | None:
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(generation)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._entries[generation]
                return None
            return entry.tables

    def put(self, generation: tuple[str | None, str, int], tables: list[Any]) -> None:
        now = time.monotonic()
        with self._lock:
            if len(self._entries) >= _MAX_ENTRIES and generation not in self._entries:
                # Small, bounded map (one entry per live org x generation) -- drop everything
                # else rather than track per-key ordering for a cache this small.
                self._entries.clear()
            self._entries[generation] = _Entry(tables=tables, expires_at=now + self.ttl_seconds)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_ATTR = "_req_1882_registered_tables_cache"


def get_cache_for(state: Any) -> RegisteredTablesCache:
    """The `RegisteredTablesCache` bound to this `state` instance, creating and attaching one on
    first use. See module docstring for why this is per-instance, not a module-global singleton."""
    cache = getattr(state, _ATTR, None)
    if cache is None:
        cache = RegisteredTablesCache()
        setattr(state, _ATTR, cache)
    return cache
