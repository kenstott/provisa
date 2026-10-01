# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2f19-6c3a-4d71-a5e0-9f2d7c1b8e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-595 / REQ-544: a write invalidates the ACTING org's response-cache entries — and only them.

Every entry is stored under the acting org's prefix (``cache.tenancy.cache_tenant``: the bound
``current_org``, else the deployment's own org). The GraphQL mutation, Cypher write, raw-SQL write
and admin purge paths used to call ``invalidate_by_table(table_id)`` with no tenant, which targets
the UNPREFIXED index — so an org's entry for the written table survived the write and was served
stale until its TTL ran out. One helper now invalidates, under the same tenant the entries were
written with, and nothing else calls the store directly.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import fakeredis.aioredis
import pytest

from provisa.cache.store import RedisCacheStore

_PROVISA = Path(__file__).resolve().parents[2] / "provisa"


def _store() -> RedisCacheStore:
    store = RedisCacheStore("redis://unused")
    store._redis = fakeredis.aioredis.FakeRedis()  # a real Redis protocol, in memory
    return store


@pytest.mark.asyncio
async def test_a_write_in_org_a_drops_org_as_entry_and_leaves_org_bs():
    from provisa.cache.tenancy import invalidate_tables
    from provisa.core.request_context import reset_current_org, set_current_org

    store = _store()
    await store.set("q1", b"a-rows", 60, tenant_id="org-a", table_ids={7})
    await store.set("q1", b"b-rows", 60, tenant_id="org-b", table_ids={7})
    state = SimpleNamespace(response_cache_store=store, org_id="default")
    token = set_current_org("org-a")
    try:
        await invalidate_tables(state, [7])
    finally:
        reset_current_org(token)
    assert await store.get("q1", tenant_id="org-a") is None
    b = await store.get("q1", tenant_id="org-b")
    assert b is not None and b.data == b"b-rows"


@pytest.mark.asyncio
async def test_an_unbound_request_invalidates_the_deployments_own_org():
    from provisa.cache.tenancy import cache_tenant, invalidate_tables

    store = _store()
    state = SimpleNamespace(response_cache_store=store, org_id="default")
    assert cache_tenant(state) == "default"
    await store.set("q1", b"rows", 60, tenant_id="default", table_ids={7})
    await invalidate_tables(state, [7])
    assert await store.get("q1", tenant_id="default") is None


def test_every_invalidation_goes_through_the_tenant_helper():
    """No module calls ``invalidate_by_table`` itself — a direct call is how the tenant got
    dropped (#128-class regression guard). Only the helper and the store define/call it."""
    allowed = {_PROVISA / "cache" / "tenancy.py", _PROVISA / "cache" / "store.py"}
    offenders = [
        f"{p.relative_to(_PROVISA.parent)}:{n}"
        for p in _PROVISA.rglob("*.py")
        if p not in allowed
        for n, line in enumerate(p.read_text().splitlines(), 1)
        if re.search(r"\.invalidate_by_table\(", line)
    ]
    assert offenders == []
