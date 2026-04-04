# Copyright (c) 2025 Kenneth Stott
# Canary: 6ec7c311-2017-4b9d-97cc-76aabe988ee6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for API source candidate discovery, registration, and rejection."""

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.api_source.candidates import (
    accept_candidate,
    list_candidates,
    reject_candidate,
    store_candidates,
)
from provisa.api_source.models import ApiColumn, ApiEndpointCandidate
from provisa.core.db import init_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(pg_pool):
    await init_schema(pg_pool, SCHEMA_SQL)


@pytest_asyncio.fixture(autouse=True)
async def _clean(pg_pool, _init_schema):
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            DELETE FROM api_endpoint_candidates;
            DELETE FROM api_endpoints;
            INSERT INTO api_sources (id, type, base_url)
            VALUES ('test-api', 'openapi', 'https://api.example.com')
            ON CONFLICT (id) DO NOTHING;
        """)


def _make_candidate(**kwargs) -> ApiEndpointCandidate:
    defaults = dict(
        source_id="test-api",
        path="/users",
        method="GET",
        table_name="users",
        columns=[
            ApiColumn(name="id", type="integer", filterable=True, nullable=False),
            ApiColumn(name="name", type="string", filterable=True, nullable=True),
            ApiColumn(name="metadata", type="jsonb", filterable=False, nullable=True),
        ],
    )
    defaults.update(kwargs)
    return ApiEndpointCandidate(**defaults)


class TestStoreCandidates:
    async def test_store_and_list(self, pg_pool):
        async with pg_pool.acquire() as conn:
            candidates = [_make_candidate()]
            ids = await store_candidates(conn, "test-api", candidates)
            assert len(ids) == 1

            listed = await list_candidates(conn, "test-api")
            assert len(listed) == 1
            assert listed[0].path == "/users"
            assert listed[0].table_name == "users"
            assert len(listed[0].columns) == 3
            assert listed[0].status == "discovered"

    async def test_upsert_on_conflict(self, pg_pool):
        async with pg_pool.acquire() as conn:
            c1 = _make_candidate(table_name="users_v1")
            await store_candidates(conn, "test-api", [c1])

            c2 = _make_candidate(table_name="users_v2")
            await store_candidates(conn, "test-api", [c2])

            listed = await list_candidates(conn, "test-api")
            assert len(listed) == 1
            assert listed[0].table_name == "users_v2"

    async def test_multiple_candidates(self, pg_pool):
        async with pg_pool.acquire() as conn:
            candidates = [
                _make_candidate(path="/users"),
                _make_candidate(path="/orders", table_name="orders"),
            ]
            ids = await store_candidates(conn, "test-api", candidates)
            assert len(ids) == 2

            listed = await list_candidates(conn, "test-api")
            assert len(listed) == 2


class TestAcceptCandidate:
    async def test_accept_creates_endpoint(self, pg_pool):
        async with pg_pool.acquire() as conn:
            ids = await store_candidates(conn, "test-api", [_make_candidate()])
            endpoint = await accept_candidate(conn, ids[0])

            assert endpoint.table_name == "users"
            assert endpoint.path == "/users"
            assert len(endpoint.columns) == 3

            # Candidate status updated
            listed = await list_candidates(conn, "test-api")
            assert len(listed) == 0  # no longer 'discovered'

    async def test_accept_with_overrides(self, pg_pool):
        async with pg_pool.acquire() as conn:
            ids = await store_candidates(conn, "test-api", [_make_candidate()])
            endpoint = await accept_candidate(conn, ids[0], {
                "table_name": "api_users",
                "ttl": 600,
            })
            assert endpoint.table_name == "api_users"
            assert endpoint.ttl == 600

    async def test_accept_already_registered_fails(self, pg_pool):
        async with pg_pool.acquire() as conn:
            ids = await store_candidates(conn, "test-api", [_make_candidate()])
            await accept_candidate(conn, ids[0])
            with pytest.raises(ValueError, match="not 'discovered'"):
                await accept_candidate(conn, ids[0])


class TestRejectCandidate:
    async def test_reject_removes_from_discovered(self, pg_pool):
        async with pg_pool.acquire() as conn:
            ids = await store_candidates(conn, "test-api", [_make_candidate()])
            await reject_candidate(conn, ids[0])

            listed = await list_candidates(conn, "test-api")
            assert len(listed) == 0

    async def test_reject_already_rejected_fails(self, pg_pool):
        async with pg_pool.acquire() as conn:
            ids = await store_candidates(conn, "test-api", [_make_candidate()])
            await reject_candidate(conn, ids[0])
            with pytest.raises(ValueError, match="not found or not in"):
                await reject_candidate(conn, ids[0])

    async def test_reject_nonexistent_fails(self, pg_pool):
        async with pg_pool.acquire() as conn:
            with pytest.raises(ValueError):
                await reject_candidate(conn, 99999)
