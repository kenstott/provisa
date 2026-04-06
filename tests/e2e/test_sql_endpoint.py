# Copyright (c) 2026 Kenneth Stott
# Canary: de41178a-9623-4ad8-bff6-2c8f07622e85
# (run scripts/canary_stamp.py on this file after creating it)

"""E2E tests: POST /data/sql → governed SQL execution (REQ-264)."""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture
async def client():
    """Create test client against the FastAPI app with full startup."""
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestSQLEndpoint:
    async def test_sql_returns_200(self, client):
        resp = await client.post(
            "/data/sql",
            json={"sql": "SELECT id, amount FROM orders", "role": "admin"},
        )
        assert resp.status_code == 200

    async def test_sql_returns_data(self, client):
        resp = await client.post(
            "/data/sql",
            json={"sql": "SELECT id, amount FROM orders", "role": "admin"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data

    async def test_sql_invalid_role_rejected(self, client):
        resp = await client.post(
            "/data/sql",
            json={"sql": "SELECT 1", "role": "nonexistent_role"},
        )
        assert resp.status_code == 400
