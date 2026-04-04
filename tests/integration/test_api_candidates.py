"""Integration tests for API source candidate discovery, registration, and rejection."""

import asyncio
import json
from pathlib import Path

import pytest

from provisa.api_source.candidates import (
    accept_candidate,
    list_candidates,
    reject_candidate,
    store_candidates,
)
from provisa.api_source.models import ApiColumn, ApiEndpointCandidate
from provisa.core.db import init_schema

pytestmark = pytest.mark.integration

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()


@pytest.fixture(scope="module")
def _init_schema(pg_pool, event_loop):
    event_loop.run_until_complete(init_schema(pg_pool, SCHEMA_SQL))


@pytest.fixture(autouse=True)
def _clean(pg_pool, _init_schema, event_loop):
    async def _truncate():
        async with pg_pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM api_endpoint_candidates;
                DELETE FROM api_endpoints;
                INSERT INTO api_sources (id, type, base_url)
                VALUES ('test-api', 'openapi', 'https://api.example.com')
                ON CONFLICT (id) DO NOTHING;
            """)
    event_loop.run_until_complete(_truncate())


def _make_candidate(**kwargs) -> ApiEndpointCandidate:
    defaults = dict(
        source_id="test-api",
        path="/users",
        method="GET",
        table_name="users",
        columns=[
            ApiColumn(name="id", type="integer", filterable=True, nullable=False),
            ApiColumn(name="name", type="varchar", filterable=True, nullable=True),
            ApiColumn(name="metadata", type="jsonb", filterable=False, nullable=True),
        ],
    )
    defaults.update(kwargs)
    return ApiEndpointCandidate(**defaults)


class TestStoreCandidates:
    def test_store_and_list(self, pg_pool, event_loop):
        async def _run():
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
        event_loop.run_until_complete(_run())

    def test_upsert_on_conflict(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                c1 = _make_candidate(table_name="users_v1")
                await store_candidates(conn, "test-api", [c1])

                c2 = _make_candidate(table_name="users_v2")
                await store_candidates(conn, "test-api", [c2])

                listed = await list_candidates(conn, "test-api")
                assert len(listed) == 1
                assert listed[0].table_name == "users_v2"
        event_loop.run_until_complete(_run())

    def test_multiple_candidates(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                candidates = [
                    _make_candidate(path="/users"),
                    _make_candidate(path="/orders", table_name="orders"),
                ]
                ids = await store_candidates(conn, "test-api", candidates)
                assert len(ids) == 2

                listed = await list_candidates(conn, "test-api")
                assert len(listed) == 2
        event_loop.run_until_complete(_run())


class TestAcceptCandidate:
    def test_accept_creates_endpoint(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                ids = await store_candidates(conn, "test-api", [_make_candidate()])
                endpoint = await accept_candidate(conn, ids[0])

                assert endpoint.table_name == "users"
                assert endpoint.path == "/users"
                assert len(endpoint.columns) == 3

                # Candidate status updated
                listed = await list_candidates(conn, "test-api")
                assert len(listed) == 0  # no longer 'discovered'
        event_loop.run_until_complete(_run())

    def test_accept_with_overrides(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                ids = await store_candidates(conn, "test-api", [_make_candidate()])
                endpoint = await accept_candidate(conn, ids[0], {
                    "table_name": "api_users",
                    "ttl": 600,
                })
                assert endpoint.table_name == "api_users"
                assert endpoint.ttl == 600
        event_loop.run_until_complete(_run())

    def test_accept_already_registered_fails(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                ids = await store_candidates(conn, "test-api", [_make_candidate()])
                await accept_candidate(conn, ids[0])
                with pytest.raises(ValueError, match="not 'discovered'"):
                    await accept_candidate(conn, ids[0])
        event_loop.run_until_complete(_run())


class TestRejectCandidate:
    def test_reject_removes_from_discovered(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                ids = await store_candidates(conn, "test-api", [_make_candidate()])
                await reject_candidate(conn, ids[0])

                listed = await list_candidates(conn, "test-api")
                assert len(listed) == 0
        event_loop.run_until_complete(_run())

    def test_reject_already_rejected_fails(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                ids = await store_candidates(conn, "test-api", [_make_candidate()])
                await reject_candidate(conn, ids[0])
                with pytest.raises(ValueError, match="not found or not in"):
                    await reject_candidate(conn, ids[0])
        event_loop.run_until_complete(_run())

    def test_reject_nonexistent_fails(self, pg_pool, event_loop):
        async def _run():
            async with pg_pool.acquire() as conn:
                with pytest.raises(ValueError):
                    await reject_candidate(conn, 99999)
        event_loop.run_until_complete(_run())
