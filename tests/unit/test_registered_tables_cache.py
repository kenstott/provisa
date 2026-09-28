# Copyright (c) 2026 Kenneth Stott
# Canary: b230227c-fc3b-46dc-b62f-0aa7cc94e0a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882: `registered_tables`' per-instance, generation-keyed cache.

Confirms `registered_tables` (provisa.federation.registry_view) no longer re-runs `fetch_tables`
on every call for the same schema generation, does invalidate on a schema_version bump, and never
lets one `state` instance's cache leak into another's (the fix for the concurrency-blocking
py-spy finding also had to avoid introducing cross-org/cross-test cache pollution).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation import registered_tables_cache, registry_view


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _state(schema_boot_id="boot-1", schema_version=1):
    conn = object()
    db = SimpleNamespace(acquire=lambda: _Acquire(conn))
    return SimpleNamespace(
        config=SimpleNamespace(sources=[], tables=[]),
        tenant_db=db,
        schema_boot_id=schema_boot_id,
        schema_version=schema_version,
    )


_ROW = {
    "id": 1,
    "source_id": "src",
    "schema_name": "public",
    "table_name": "orders",
    "dq_contract": None,
    "columns": [],
}


@pytest.mark.asyncio
async def test_second_call_same_generation_is_a_cache_hit_and_skips_fetch(monkeypatch):
    state = _state()
    calls = {"n": 0}

    async def _fetch_tables(conn):
        calls["n"] += 1
        return [_ROW]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)

    first = await registry_view.registered_tables(state)
    second = await registry_view.registered_tables(state)

    assert calls["n"] == 1  # fetch_tables ran only once — the second call was a cache hit
    assert first == second


@pytest.mark.asyncio
async def test_schema_version_bump_invalidates(monkeypatch):
    state = _state(schema_version=1)
    calls = {"n": 0}

    async def _fetch_tables(conn):
        calls["n"] += 1
        return [_ROW]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)

    await registry_view.registered_tables(state)
    state.schema_version = 2
    await registry_view.registered_tables(state)

    assert calls["n"] == 2  # the generation moved, so the second call was a real re-fetch


@pytest.mark.asyncio
async def test_explicit_conn_bypasses_cache(monkeypatch):
    state = _state()
    calls = {"n": 0}

    async def _fetch_tables(conn):
        calls["n"] += 1
        return [_ROW]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)

    explicit_conn = object()
    await registry_view.registered_tables(state, conn=explicit_conn)
    await registry_view.registered_tables(state, conn=explicit_conn)

    assert calls["n"] == 2  # never cached: a caller-supplied conn always re-fetches


@pytest.mark.asyncio
async def test_two_state_instances_never_share_a_cache(monkeypatch):
    """Guards the cross-instance-pollution risk a naive module-global cache would have had: two
    unrelated `state` objects (e.g. two orgs, or two tests) must never see each other's rows."""
    state_a = _state()
    state_b = _state()

    async def _fetch_tables(conn):
        return [_ROW]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)

    await registry_view.registered_tables(state_a)
    cache_a = registered_tables_cache.get_cache_for(state_a)
    cache_b = registered_tables_cache.get_cache_for(state_b)
    generation = (None, state_a.schema_boot_id, state_a.schema_version)

    assert cache_a is not cache_b
    assert cache_a.get(generation) is not None  # state_a's own call populated its own cache
    assert cache_b.get(generation) is None  # state_b's cache was never touched


def test_cache_get_put_and_ttl_expiry():
    cache = registered_tables_cache.RegisteredTablesCache(ttl_seconds=0.0)
    generation = (None, "boot-1", 1)
    assert cache.get(generation) is None
    cache.put(generation, [SimpleNamespace(table_name="orders")])
    # ttl_seconds=0.0 -> immediately expired on the next monotonic read
    assert cache.get(generation) is None


def test_cache_hit_within_ttl():
    cache = registered_tables_cache.RegisteredTablesCache(ttl_seconds=60.0)
    generation = (None, "boot-1", 1)
    tables = [SimpleNamespace(table_name="orders")]
    cache.put(generation, tables)
    assert cache.get(generation) == tables


def test_cache_clear():
    cache = registered_tables_cache.RegisteredTablesCache()
    generation = (None, "boot-1", 1)
    cache.put(generation, [SimpleNamespace(table_name="orders")])
    cache.clear()
    assert cache.get(generation) is None
