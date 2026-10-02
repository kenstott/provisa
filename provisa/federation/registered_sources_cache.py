# Copyright (c) 2026 Kenneth Stott
# Canary: 11079bd4-af4e-4024-bcd4-15475e054ca8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""In-memory, per-org, generation-keyed cache of `registered_sources` (REQ-1892).

`registered_sources` (`provisa/federation/registry_view.py`) is read from `source_repo.list_all`
on every single call with no cache -- confirmed by grepping every call site (`query_residency.py`,
`backend.py`, `app_wiring.py`, `row_materialize_lifecycle.py`, `_pipeline.py`,
`schema_mutation_ops.py`, `app.py`, `push_wiring.py`): `_execute_plan_in_org` alone calls it 2-3
times per governed invocation (`ensure_rows_resident`, `ensure_resident`, and again inside
the replica build), warm or cold, every time. This is the exact same shape of finding REQ-1882
fixed for `registered_tables` in the same file, just not applied here too -- see
`registered_tables_cache.py`'s module docstring for the full reasoning this cache reuses unchanged:
same generation key `(current_org, schema_boot_id, schema_version)`, same TTL backstop, same
per-instance (never module-global) cache scoping, same explicit-``conn``-bypasses-cache behavior.

Reuses `RegisteredTablesCache` directly rather than duplicating the class: its shape (TTL-evicted,
generation-keyed, `list[Any]`-valued) is already source/table-agnostic -- nothing in it references
tables specifically. Only the per-``state``-instance attachment point differs, via its own
attribute name below, so a `registered_sources` cache and a `registered_tables` cache attached to
the same `state` never collide or share entries.
"""

# Requirements: REQ-1892 (extends REQ-1882)

from __future__ import annotations

from typing import Any

from provisa.federation.registered_tables_cache import RegisteredTablesCache

_ATTR = "_req_1892_registered_sources_cache"


def get_cache_for(state: Any) -> RegisteredTablesCache:
    """The `RegisteredTablesCache` bound to this `state` instance for `registered_sources`,
    creating and attaching one on first use. Same per-instance scoping as
    `registered_tables_cache.get_cache_for` -- see that module's docstring for why."""
    cache = getattr(state, _ATTR, None)
    if cache is None:
        cache = RegisteredTablesCache()
        setattr(state, _ATTR, cache)
    return cache
