# Copyright (c) 2026 Kenneth Stott
# Canary: b1c2d3e4-f5a6-4b7c-8d9e-0f1a2b3c4d5e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for the health check endpoint.

Endpoint surveyed in provisa/api/app.py:

    @app.get("/health")
    async def health():
        return {"status": "ok"}

Only /health is implemented. There is no /ping, /healthz, /ready, or /live
endpoint, and no dependency-level health reporting in the current codebase.

These tests verify:
1. GET /health returns HTTP 200.
2. Response body contains {"status": "ok"}.
3. Response Content-Type is application/json.
4. The endpoint is available without authentication (no X-Role header required).
"""

from __future__ import annotations

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
async def client():
    """Create an AsyncClient against the live FastAPI app with full startup."""
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHealthEndpoint:

    async def test_health_returns_200(self, client):
        """GET /health must return HTTP 200."""
        resp = await client.get("/health")
        assert resp.status_code == 200

    async def test_health_response_status_field_is_ok(self, client):
        """Response body must contain {"status": "ok"}."""
        resp = await client.get("/health")
        body = resp.json()
        assert "status" in body, "Response JSON must include a 'status' field"
        assert body["status"] == "ok"

    async def test_health_response_is_json(self, client):
        """Content-Type must indicate JSON."""
        resp = await client.get("/health")
        assert "application/json" in resp.headers.get("content-type", "")

    async def test_health_without_auth_headers_still_200(self, client):
        """Health endpoint must not require authentication headers."""
        resp = await client.get("/health", headers={})
        assert resp.status_code == 200

    async def test_health_response_schema_no_extra_error_fields(self, client):
        """A healthy response must not contain an 'error' field."""
        resp = await client.get("/health")
        body = resp.json()
        assert "error" not in body

    async def test_health_responds_to_head_request(self, client):
        """HEAD /health should also succeed (standard FastAPI behaviour)."""
        resp = await client.head("/health")
        assert resp.status_code == 200

    @pytest.mark.skip(reason="liveness endpoint not yet implemented (/live, /healthz, /ready are absent)")
    async def test_liveness_endpoint_returns_200(self, client):
        """GET /live must return HTTP 200 — endpoint not yet implemented."""
        resp = await client.get("/live")
        assert resp.status_code == 200

    @pytest.mark.skip(reason="readiness endpoint not yet implemented")
    async def test_readiness_endpoint_returns_200(self, client):
        """GET /ready must return HTTP 200 — endpoint not yet implemented."""
        resp = await client.get("/ready")
        assert resp.status_code == 200

    @pytest.mark.skip(reason="dependency health reporting not yet implemented")
    async def test_health_includes_database_status(self, client):
        """Health response should include dependency status fields.

        Expected future schema:
          {
              "status": "ok",
              "dependencies": {
                  "postgres": "ok",
                  "trino": "ok",
                  "redis": "ok"
              }
          }
        """
        resp = await client.get("/health")
        body = resp.json()
        assert "dependencies" in body
        assert "postgres" in body["dependencies"]
