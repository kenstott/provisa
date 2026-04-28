# Copyright (c) 2026 Kenneth Stott
# Canary: d9d13daf-9138-4538-accb-6392dc665697
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for OpenAPI Auto-Registration Connector (Phase AQ).

Requires a live PostgreSQL connection (docker-compose provides it).
"""
from __future__ import annotations
import json
import os
import pathlib
import tempfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

SAMPLE_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Test API", "version": "1.0.0"},
    "components": {
        "schemas": {
            "User": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                },
            }
        }
    },
    "paths": {
        "/users": {
            "get": {
                "operationId": "listUsers",
                "summary": "List all users",
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/User"},
                                }
                            }
                        },
                    }
                },
            },
            "post": {
                "operationId": "createUser",
                "summary": "Create user",
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/User"}
                        }
                    }
                },
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/User"}
                            }
                        },
                        "description": "created",
                    }
                },
            },
        },
        "/users/{id}": {
            "get": {
                "operationId": "getUser",
                "parameters": [
                    {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/User"},
                            }
                        },
                        "description": "ok",
                    }
                },
            }
        },
    },
}


@pytest_asyncio.fixture(scope="module")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    from provisa.api.app import create_app
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest_asyncio.fixture(scope="module")
def spec_file():
    """Write sample spec to a temp file, yield path, clean up."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(SAMPLE_SPEC, f)
        path = f.name
    yield path
    pathlib.Path(path).unlink(missing_ok=True)


async def test_preview_returns_queries_and_mutations(client, spec_file):
    resp = await client.post(
        "/admin/openapi/preview",
        json={"spec_path": spec_file},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "queries" in body
    assert "mutations" in body
    query_ids = [q["operation_id"] for q in body["queries"]]
    assert "listUsers" in query_ids
    assert "getUser" in query_ids
    mutation_ids = [m["operation_id"] for m in body["mutations"]]
    assert "createUser" in mutation_ids


async def test_register_creates_tables_and_functions(client, spec_file):
    resp = await client.post(
        "/admin/openapi/register",
        json={
            "spec_path": spec_file,
            "source_id": "test-openapi",
            "namespace": "testns",
            "domain_id": "",
            "cache_ttl": 60,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["source_id"] == "test-openapi"
    assert body["tables"] == 2   # listUsers, getUser
    assert body["mutations"] == 1  # createUser


async def test_refresh_reruns_registration(client, spec_file):
    # First register so it's in state
    await client.post(
        "/admin/openapi/register",
        json={
            "spec_path": spec_file,
            "source_id": "test-openapi-refresh",
            "namespace": "",
            "domain_id": "",
        },
    )
    resp = await client.post("/admin/openapi/refresh/test-openapi-refresh")
    assert resp.status_code == 200
    body = resp.json()
    assert body["source_id"] == "test-openapi-refresh"
    assert body["tables"] >= 1


async def test_get_spec_returns_stored_spec(client, spec_file):
    # Ensure registered
    await client.post(
        "/admin/openapi/register",
        json={
            "spec_path": spec_file,
            "source_id": "test-openapi-get",
            "namespace": "",
            "domain_id": "",
        },
    )
    resp = await client.get("/admin/openapi/spec/test-openapi-get")
    assert resp.status_code == 200
    body = resp.json()
    assert body["openapi"] == "3.0.0"


async def test_refresh_unknown_source_returns_404(client):
    resp = await client.post("/admin/openapi/refresh/no-such-source-xyz")
    assert resp.status_code == 404


async def test_get_spec_unknown_source_returns_404(client):
    resp = await client.get("/admin/openapi/spec/no-such-source-xyz")
    assert resp.status_code == 404
