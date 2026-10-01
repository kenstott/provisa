# Copyright (c) 2026 Kenneth Stott
# Canary: 6f2a9c1e-4b8d-4e37-a05c-9d3b7e1f2c84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a GraphQL response-cache HIT serves the rows the MISS stored (REQ-544, REQ-1896).

The real /data/graphql endpoint and governed pipeline against a real Redis response cache
(RedisCacheStore on the self-provisioned stack). Regression: the HIT read the decoded payload one
level too shallow and answered ``{"data": {"orders": []}}`` for every identical repeat request
(perf bench cache pass: 1000 rows on the miss, 0 rows on every hit).
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _with_provisa_directives(state):
    """The shared test state's hand-built schema, rebuilt with Provisa's directives declared (a
    generated schema always has them — schema_gen), so ``@cached`` validates."""
    from graphql import GraphQLSchema, specified_directives

    from provisa.compiler.schema_directives import PROVISA_DIRECTIVES

    state.org_id = "graphql-cache-itest"  # AppState's own org: the response-cache tenant
    base = state.schemas["admin"]
    state.schemas = {
        "admin": GraphQLSchema(
            query=base.query_type, directives=[*specified_directives, *PROVISA_DIRECTIVES]
        )
    }
    return state


async def test_identical_graphql_request_twice_returns_the_same_rows():
    httpx = pytest.importorskip("httpx")
    from fastapi import FastAPI

    from provisa.api.data.endpoint import router as data_router
    from provisa.cache.store import RedisCacheStore
    from tests.integration.test_client_access_integration import _make_app_state_with_orders

    state = _with_provisa_directives(_make_app_state_with_orders())
    store = RedisCacheStore(os.environ["REDIS_URL"])
    state.response_cache_store = store
    # REQ-544 (amended 2026-09-30): the response cache is per-request opt-in.
    query = {"query": "query @cached { orders { id region amount } }", "role": "admin"}
    try:
        with patch("provisa.api.app.state", state):
            app = FastAPI()
            app.include_router(data_router)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test", headers={"X-Provisa-Role": "admin"}
            ) as client:
                await store.invalidate_by_pattern("*")
                first = await client.post("/data/graphql", json=query)
                second = await client.post("/data/graphql", json=query)
        assert first.status_code == 200 and second.status_code == 200
        assert first.headers["X-Provisa-Cache"] == "MISS"
        assert second.headers["X-Provisa-Cache"] == "HIT"
        assert first.json() == {
            "data": {"orders": [{"id": 1, "region": "us-east", "amount": 9.99}]}
        }
        assert second.json() == first.json()
        assert state.source_pools.execute.await_count == 1  # the HIT never re-ran the source
    finally:
        await store.invalidate_by_pattern("*")
        await store.close()


async def test_without_the_hint_every_request_misses():
    """REQ-544 (amended 2026-09-30): no @cached, no cache read and no cache write."""
    httpx = pytest.importorskip("httpx")
    from fastapi import FastAPI

    from provisa.api.data.endpoint import router as data_router
    from provisa.cache.store import RedisCacheStore
    from tests.integration.test_client_access_integration import _make_app_state_with_orders

    state = _with_provisa_directives(_make_app_state_with_orders())
    store = RedisCacheStore(os.environ["REDIS_URL"])
    state.response_cache_store = store
    query = {"query": "{ orders { id region amount } }", "role": "admin"}
    try:
        with patch("provisa.api.app.state", state):
            app = FastAPI()
            app.include_router(data_router)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test", headers={"X-Provisa-Role": "admin"}
            ) as client:
                await store.invalidate_by_pattern("*")
                first = await client.post("/data/graphql", json=query)
                second = await client.post("/data/graphql", json=query)
        assert first.headers["X-Provisa-Cache"] == "MISS"
        assert second.headers["X-Provisa-Cache"] == "MISS"
        assert state.source_pools.execute.await_count == 2  # both went to the source
    finally:
        await store.invalidate_by_pattern("*")
        await store.close()


async def test_a_disabled_source_blocks_the_hint():
    """The operator's source cache_enabled=false is the permission: @cached cannot override it."""
    httpx = pytest.importorskip("httpx")
    from fastapi import FastAPI

    from provisa.api.data.endpoint import router as data_router
    from provisa.cache.store import RedisCacheStore
    from tests.integration.test_client_access_integration import _make_app_state_with_orders

    state = _with_provisa_directives(_make_app_state_with_orders())
    state.source_cache = {"test-pg": {"cache_enabled": False}}
    store = RedisCacheStore(os.environ["REDIS_URL"])
    state.response_cache_store = store
    query = {"query": "query @cached(ttl: 600) { orders { id region amount } }", "role": "admin"}
    try:
        with patch("provisa.api.app.state", state):
            app = FastAPI()
            app.include_router(data_router)
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test", headers={"X-Provisa-Role": "admin"}
            ) as client:
                await store.invalidate_by_pattern("*")
                await client.post("/data/graphql", json=query)
                second = await client.post("/data/graphql", json=query)
        assert second.headers["X-Provisa-Cache"] == "MISS"
        assert state.source_pools.execute.await_count == 2
    finally:
        await store.invalidate_by_pattern("*")
        await store.close()
