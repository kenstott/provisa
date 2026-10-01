# Copyright (c) 2026 Kenneth Stott
# Canary: ffe6f50b-5a0f-4b0c-8b50-ca0d612689aa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-829: the admin cache statistics name the embedded response cache as a live store.

With no Redis URL the response cache runs on the embedded in-process Redis. The admin page reads
its statistics through ``cache_stats``; it must report that store as ``memory`` (enabled), never as
``noop`` (disabled).
"""

# Requirements: REQ-829

import pytest

from provisa.cache.store import NoopCacheStore, RedisCacheStore


async def _stats(monkeypatch, store):
    from provisa.api import app as app_module
    from provisa.api.admin.schema_query import Query

    monkeypatch.setattr(app_module.state, "response_cache_store", store, raising=False)
    return await Query().cache_stats()


@pytest.mark.asyncio
async def test_the_embedded_response_cache_is_reported_as_an_enabled_store(monkeypatch):
    store = RedisCacheStore(None)
    await store.set("k", b"{}", 60)

    stats = await _stats(monkeypatch, store)

    assert stats.store_type == "memory"
    assert stats.total_keys >= 1


@pytest.mark.asyncio
async def test_a_disabled_response_cache_is_reported_as_noop(monkeypatch):
    stats = await _stats(monkeypatch, NoopCacheStore())

    assert stats.store_type == "noop"
