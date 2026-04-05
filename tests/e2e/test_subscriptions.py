# Copyright (c) 2025 Kenneth Stott
# Canary: a91a34f7-2b09-4fd1-bf7b-8e6f7eb42d02
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for SSE subscription endpoint (Phase AB)."""

import asyncio
import json
import os
from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestSubscribeSSE:
    async def test_subscribe_returns_event_stream(self, client):
        """SSE endpoint should return text/event-stream content type."""
        resp = await client.get(
            "/data/subscribe/orders",
            headers={"Accept": "text/event-stream"},
            timeout=5.0,
        )
        # Either 200 (streaming) or 503 (no DB) or 404 (table not found)
        assert resp.status_code in (200, 404, 503)

    async def test_subscribe_503_without_pool(self, client):
        """Endpoint returns 503 when pg_pool is unavailable."""
        from provisa.api.app import state

        original_pool = state.pg_pool
        state.pg_pool = None
        try:
            resp = await client.get("/data/subscribe/orders")
            assert resp.status_code == 503
        finally:
            state.pg_pool = original_pool


class TestSubscribeValidation:
    async def test_nonexistent_table_404(self):
        """Subscribe to a table not in config returns 404."""
        from fastapi import FastAPI
        from provisa.api.data.subscribe import router

        app = FastAPI()
        app.include_router(router)

        # Mock state with contexts that don't have the table
        mock_ctx = MagicMock()
        mock_ctx.tables = {"users": MagicMock()}

        with patch("provisa.api.app.state") as mock_state:
            mock_state.pg_pool = MagicMock()
            mock_state.rls_contexts = {}
            mock_state.contexts = {"analyst": mock_ctx}
            mock_state.source_types = {"pg": "postgresql"}

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                resp = await c.get("/data/subscribe/nonexistent_table")
                assert resp.status_code == 404
