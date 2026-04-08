# Copyright (c) 2026 Kenneth Stott
# Canary: a24a7afb-976e-4c82-84f7-d6ace38e48de
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for GraphQL Remote Schema Connector (Phase AP).

Uses the live FastAPI test client with respx mocking the remote GraphQL server.
No external services required for these tests.
"""
import os

import httpx
import pytest
import pytest_asyncio
import respx
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

REMOTE_URL = "https://remote-graphql.example.com/graphql"

SAMPLE_INTROSPECTION_RESPONSE = {
    "data": {
        "__schema": {
            "queryType": {"name": "Query"},
            "mutationType": {"name": "Mutation"},
            "types": [
                {
                    "kind": "OBJECT",
                    "name": "Query",
                    "fields": [
                        {
                            "name": "users",
                            "type": {"kind": "LIST", "name": None, "ofType": {"kind": "OBJECT", "name": "User", "ofType": None}},
                            "args": [],
                        }
                    ],
                },
                {
                    "kind": "OBJECT",
                    "name": "Mutation",
                    "fields": [
                        {
                            "name": "createUser",
                            "type": {"kind": "OBJECT", "name": "CreateUserResult", "ofType": None},
                            "args": [
                                {"name": "name", "type": {"kind": "SCALAR", "name": "String", "ofType": None}},
                            ],
                        }
                    ],
                },
                {
                    "kind": "OBJECT",
                    "name": "User",
                    "fields": [
                        {"name": "id", "type": {"kind": "SCALAR", "name": "ID", "ofType": None}, "args": []},
                        {"name": "name", "type": {"kind": "SCALAR", "name": "String", "ofType": None}, "args": []},
                    ],
                },
                {
                    "kind": "OBJECT",
                    "name": "CreateUserResult",
                    "fields": [
                        {"name": "id", "type": {"kind": "SCALAR", "name": "ID", "ofType": None}, "args": []},
                        {"name": "ok", "type": {"kind": "SCALAR", "name": "Boolean", "ofType": None}, "args": []},
                    ],
                },
            ],
        }
    }
}


@pytest_asyncio.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestGraphQLRemoteSourceRegistration:
    @respx.mock
    async def test_register_source(self, client):
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "test-remote",
                "url": REMOTE_URL,
                "namespace": "testns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["source_id"] == "test-remote"
        assert body["tables"] == 1
        assert body["functions"] == 1
        assert "testns__users" in body["table_names"]
        assert "testns__createUser" in body["function_names"]

    @respx.mock
    async def test_list_sources(self, client):
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        # Register first
        await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "list-test-remote",
                "url": REMOTE_URL,
                "namespace": "listns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        resp = await client.get("/admin/sources/graphql-remote")
        assert resp.status_code == 200
        sources = resp.json()
        source_ids = [s["source_id"] for s in sources]
        assert "list-test-remote" in source_ids

    @respx.mock
    async def test_refresh_source(self, client):
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        # Register first
        await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "refresh-remote",
                "url": REMOTE_URL,
                "namespace": "refreshns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        # Now refresh
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        resp = await client.post("/admin/sources/graphql-remote/refresh-remote/refresh")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source_id"] == "refresh-remote"
        assert body["tables"] == 1
        assert body["functions"] == 1

    async def test_refresh_unknown_source_returns_404(self, client):
        resp = await client.post("/admin/sources/graphql-remote/nonexistent-source/refresh")
        assert resp.status_code == 404

    @respx.mock
    async def test_registration_failure_on_bad_url(self, client):
        respx.post(REMOTE_URL).mock(return_value=httpx.Response(500, text="Server Error"))
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "bad-remote",
                "url": REMOTE_URL,
                "namespace": "badns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        assert resp.status_code == 422
        assert "Introspection failed" in resp.json()["detail"]
