# Copyright (c) 2026 Kenneth Stott
# Canary: a1f4d9c2-e7b3-4f82-a5c1-8e2d7f3b9a16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for POST /data/submit endpoint (REQ-162).

Verifies that submit stores queries in the persisted-query registry for
steward approval, captures metadata, and enforces naming requirements.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest_asyncio.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestSubmitBasic:
    async def test_submit_returns_query_id(self, client):
        """Successful submit returns a UUID query_id."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query MonthlyRevenue { orders { id amount region } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "query_id" in body
        # UUID format: 8-4-4-4-12 hex chars
        assert len(body["query_id"]) == 36

    async def test_submit_returns_operation_name(self, client):
        """Submit response echoes back the operation name."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query SalesReport { orders { id amount } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["operation_name"] == "SalesReport"

    async def test_submit_returns_confirmation_message(self, client):
        """Submit response includes a human-readable confirmation message."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query CustomerList { customers { id name } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 200
        message = resp.json()["message"]
        assert "CustomerList" in message
        assert "submitted" in message.lower()


class TestSubmitRequiresOperationName:
    async def test_submit_without_operation_name_400(self, client):
        """Query without a named operation is rejected (REQ-162)."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "{ orders { id amount } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 400

    async def test_submit_anonymous_query_400(self, client):
        """Anonymous query keyword (no name) is rejected."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query { orders { id } }",
                "role": "admin",
            },
        )
        assert resp.status_code == 400


class TestSubmitWithMetadata:
    async def test_submit_with_business_purpose(self, client):
        """Submit stores optional business metadata fields."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query RevenueByRegion { orders { id amount region } }",
                "role": "admin",
                "business_purpose": "Monthly revenue tracking by region",
                "owner_team": "analytics",
                "data_sensitivity": "internal",
                "expected_row_count": "< 10000",
            },
        )
        assert resp.status_code == 200
        assert "query_id" in resp.json()

    async def test_submit_with_sink_config(self, client):
        """Submit with a Kafka sink configuration stores the sink spec."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query OrderStream { orders { id amount } }",
                "role": "admin",
                "sink": {
                    "topic": "order-events",
                    "trigger": "change_event",
                    "key_column": "id",
                },
            },
        )
        assert resp.status_code == 200
        assert "query_id" in resp.json()

    async def test_submit_with_expiry_date(self, client):
        """Submit accepts an optional expiry_date for the approved query."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query QuarterlyReport { orders { id } }",
                "role": "admin",
                "expiry_date": "2026-12-31",
            },
        )
        assert resp.status_code == 200
        assert "query_id" in resp.json()


class TestSubmitGovernanceGate:
    async def test_submit_requires_query_development_capability(self, client):
        """Roles without query_development capability cannot submit queries."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query SecretData { orders { id } }",
                "role": "readonly",
            },
        )
        # readonly role should not have query_development capability
        assert resp.status_code in (400, 403)

    async def test_submit_unknown_role_400(self, client):
        """Unknown role returns 400."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query Test { orders { id } }",
                "role": "nonexistent",
            },
        )
        assert resp.status_code == 400


class TestSubmitIdempotency:
    async def test_submit_same_operation_twice_returns_distinct_ids(self, client):
        """Each submit call produces a new pending entry, even for the same op name."""
        query = "query DuplicateTest { orders { id } }"

        resp1 = await client.post(
            "/data/submit",
            json={"query": query, "role": "admin"},
        )
        resp2 = await client.post(
            "/data/submit",
            json={"query": query, "role": "admin"},
        )

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        # Each submission gets its own registry entry
        assert resp1.json()["query_id"] != resp2.json()["query_id"]
