# Copyright (c) 2026 Kenneth Stott
# Canary: f829ad78-b075-48f2-9b09-c40ed55ddc1b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Purging the cache removes the acting org's entries in the acting environment, and no others (REQ-595).

``purgeCache`` is an org administrator's act (``org_settings``). It asked the store to delete
the pattern ``provisa:cache:*`` — every org's entries by intent (and, the store prefixing the
pattern a second time, nothing at all in practice). It now removes what belongs to the org and
environment the caller is acting in, under every model that runtime has held, and that org's
per-table index with it.
"""

# Requirements: REQ-595, REQ-544, REQ-1529

from __future__ import annotations

import types
from typing import Any

import pytest

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.cache import tenancy
from provisa.cache.store import RedisCacheStore

SCOPES = ["acme:m7", "acme:m8", "acme_env_staging:m7", "globex:m7"]


@pytest.fixture
async def store():
    """The real Redis store on the embedded server, holding one entry per scope, each indexed
    under table 7."""
    s = RedisCacheStore(None)
    await s._connect()
    await s._redis.flushall()
    for scope in SCOPES:
        await s.set("q1", b"rows of " + scope.encode(), 60, tenant_id=scope, table_ids={7})
    yield s
    await s._redis.flushall()
    await s.close()


async def _held(store: RedisCacheStore) -> set[str]:
    return {scope for scope in SCOPES if await store.get("q1", tenant_id=scope) is not None}


async def _indexed(store: RedisCacheStore) -> set[str]:
    keys = [k.decode() async for k in store._redis.scan_iter(match=store.TABLE_PREFIX + "*")]
    return {k[len(store.TABLE_PREFIX) :].rsplit(":", 1)[0] for k in keys}


async def test_a_place_is_purged_under_every_model_and_nothing_else_is(store):
    assert await _held(store) == set(SCOPES)
    removed = await store.purge_place("acme")
    assert removed == 2
    assert await _held(store) == {"acme_env_staging:m7", "globex:m7"}
    assert await _indexed(store) == {"acme_env_staging:m7", "globex:m7"}


async def test_purging_a_branch_leaves_prod(store):
    await store.purge_place("acme_env_staging")
    assert await _held(store) == {"acme:m7", "acme:m8", "globex:m7"}


async def test_the_old_request_removed_nothing(store):
    """What ``purgeCache`` used to ask of the store, kept as the record of why it changed."""
    assert await store.invalidate_by_pattern("provisa:cache:*") == 0
    assert await _held(store) == set(SCOPES)


async def test_the_mutation_purges_the_callers_org_and_environment_only(store, monkeypatch):
    from provisa.core.request_context import reset_current_org, set_current_org

    # The request's org is bound, as the routing middleware binds it (REQ-1266): the deployment's
    # own org, whose runtime holds the store; the place it purges is stubbed to acme's.
    token = set_current_org(appmod.state.org_id)
    try:
        monkeypatch.setattr(appmod.state, "response_cache_store", store, raising=False)
        monkeypatch.setattr(tenancy, "cache_place", lambda _state: "acme")
        request = types.SimpleNamespace(
            state=types.SimpleNamespace(identity=None, active_org_id="acme")
        )
        info: Any = types.SimpleNamespace(context={"request": request})

        result = await schema_mutation.Mutation().purge_cache(info)
    finally:
        monkeypatch.undo()  # the routed store is put back while the org is still bound
        reset_current_org(token)

    assert (result.success, result.code, result.params) == (
        True,
        "schema.cache_purged",
        {"count": 2},
    )
    assert await _held(store) == {"acme_env_staging:m7", "globex:m7"}


async def test_the_per_table_counts_are_the_acting_places_alone(store):
    """The admin cache view counts entries per table. It reads the acting org's and
    environment's index — under every model that place has held — and no other org's."""
    assert await store.table_entry_counts("acme") == {7: 2}
    assert await store.table_entry_counts("acme_env_staging") == {7: 1}
    assert await store.table_entry_counts("globex") == {7: 1}
    assert await store.table_entry_counts("nobody") == {}
