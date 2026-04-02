# Copyright (c) 2025 Kenneth Stott
# Canary: 9674ae1c-033f-4e6b-8aa4-7b0f69483719
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for large result redirect behavior.

Tests verify that:
- Small results return inline JSON (no redirect)
- Redirect is disabled by default (PROVISA_REDIRECT_ENABLED=false)
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    # Redirect disabled by default in test
    os.environ["PROVISA_REDIRECT_ENABLED"] = "false"

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestInlineResults:
    async def test_small_result_inline(self, client):
        """Small results always return inline JSON."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders(limit: 3) { id amount } }", "role": "admin"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert "redirect" not in data
        assert len(data["data"]["sales_analytics__orders"]) <= 3

    async def test_all_results_inline_when_disabled(self, client):
        """With redirect disabled, all results are inline regardless of size."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id } }", "role": "admin"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert "redirect" not in data
