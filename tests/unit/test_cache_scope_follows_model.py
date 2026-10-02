# Copyright (c) 2026 Kenneth Stott
# Canary: 0144888c-644c-4535-bb3e-daf938163065
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A response-cache entry belongs to the model it was computed under (REQ-544, REQ-595, REQ-1914).

The key of an entry is the governed statement, its values and the role. That text carries the
role's row filters, masks and visible columns, but not what is resolved after governance: a view
is named in it and expanded later, a table is named and bound to its physical relation later. So
the statement alone cannot tell an entry computed under one definition of a view from a request
made under the next.

The scope every read, write and invalidation is made under therefore carries the model the
runtime loaded — the control plane's model stamp — and the environment, whose model and source
bindings are its own. Workers that loaded the same model share entries; a change to the model
moves a worker onto new entries when it reloads.
"""

# Requirements: REQ-544, REQ-595, REQ-1914, REQ-1529

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from provisa.cache.key import cache_key, raw_sql_cache_key
from provisa.cache.middleware import check_cache, store_result
from provisa.cache.tenancy import cache_tenant, invalidate_tables
from provisa.core.request_context import current_env, current_org
from tests.unit.test_response_cache_shared import FakeCacheStore

VIEW_READ = 'SELECT "id", "amount" FROM "sales"."open_orders"'
ROLE = "analyst"


class _IndexedStore(FakeCacheStore):
    """The fake store, with the per-table index invalidation needs."""

    def __init__(self) -> None:
        super().__init__()
        self._by_table: dict[tuple[str | None, int], set[str]] = {}

    async def set(self, key, data, ttl, tenant_id=None, table_ids=None) -> None:
        await super().set(key, data, ttl, tenant_id=tenant_id, table_ids=table_ids)
        for table_id in table_ids or ():
            self._by_table.setdefault((tenant_id, table_id), set()).add(self._k(key, tenant_id))

    async def invalidate_by_table(self, table_id: int, tenant_id: str | None = None) -> int:
        keys = self._by_table.pop((tenant_id, table_id), set())
        for key in keys:
            self._data.pop(key, None)
        return len(keys)


@dataclass
class _Worker:
    """What a worker process holds: the shared store and the model it loaded."""

    response_cache_store: Any
    model_stamp: int | None
    org_id: str = "acme"


async def _write(worker: _Worker, key: str, rows: list[dict]) -> None:
    await store_result(
        worker.response_cache_store,
        key,
        {"rows": rows},
        ttl=60,
        table_ids={7},
        org_id=cache_tenant(worker),
    )


async def _read(worker: _Worker, key: str):
    return await check_cache(worker.response_cache_store, key, cache_tenant(worker))


@pytest.fixture(params=["graphql", "raw_sql"])
def key(request) -> str:
    """The key of a read of a view, on each of the two key functions. Neither changes when the
    view's definition does: the statement names the view."""
    if request.param == "graphql":
        return cache_key(VIEW_READ, [], ROLE, {})
    return raw_sql_cache_key(VIEW_READ, [], ROLE, wire_formats=None)


async def test_an_entry_written_under_one_model_is_not_read_under_the_next(key):
    store = _IndexedStore()
    before = _Worker(store, model_stamp=41)
    await _write(before, key, [{"id": 1, "amount": 10}, {"id": 2, "amount": 99}])
    assert await _read(before, key) is not None

    # The view is redefined to leave order 2 out; the worker reloads at the new stamp.
    after = _Worker(store, model_stamp=42)
    assert await _read(after, key) is None


async def test_workers_that_loaded_the_same_model_share_an_entry(key):
    store = _IndexedStore()
    await _write(_Worker(store, model_stamp=42), key, [{"id": 1}])
    assert await _read(_Worker(store, model_stamp=42), key) is not None


async def test_a_worker_that_has_not_reloaded_does_not_feed_one_that_has(key):
    store = _IndexedStore()
    stale, reloaded = _Worker(store, model_stamp=41), _Worker(store, model_stamp=42)
    await _write(stale, key, [{"id": 2, "amount": 99}])
    assert await _read(reloaded, key) is None


async def test_an_environment_does_not_read_its_bases_entries(key):
    """A branch holds its own copy of the model and its own source bindings (REQ-1529); its model
    stamp counts separately from prod's, so the two can be equal."""
    store = _IndexedStore()
    worker = _Worker(store, model_stamp=5)
    await _write(worker, key, [{"id": 1}])
    token = current_env.set("staging")
    try:
        assert await _read(worker, key) is None
    finally:
        current_env.reset(token)
    assert await _read(worker, key) is not None


async def test_another_org_does_not_read_the_entry(key):
    store = _IndexedStore()
    worker = _Worker(store, model_stamp=5)
    await _write(worker, key, [{"id": 1}])
    token = current_org.set("globex")
    try:
        assert await _read(worker, key) is None
    finally:
        current_org.reset(token)


async def test_a_write_invalidates_the_entries_of_the_model_it_ran_under(key):
    store = _IndexedStore()
    worker = _Worker(store, model_stamp=42)
    await _write(worker, key, [{"id": 1}])
    assert await invalidate_tables(worker, [7]) == 1
    assert await _read(worker, key) is None


def test_the_runtime_publishes_the_stamp_its_model_was_loaded_at():
    import provisa.api.app as appmod

    runtime = appmod.state._active_runtime()
    held = runtime.model_stamp
    runtime.model_stamp = 1234
    try:
        assert appmod.state.model_stamp == 1234
        assert cache_tenant(appmod.state).endswith(":m1234")
    finally:
        runtime.model_stamp = held
