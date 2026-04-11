# Copyright (c) 2026 Kenneth Stott
# Canary: a1f4d9c2-e7b3-4f82-a5c1-8e2d7f3b9a16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for submitQuery GQL mutation (REQ-162).

Verifies that submit stores queries in the persisted-query registry for
steward approval, captures metadata, and enforces naming requirements.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_SUBMIT_MUTATION = """
mutation SubmitQuery($input: SubmitQueryInput!) {
  submitQuery(input: $input) { queryId operationName message }
}
"""


async def _submit(client: AsyncClient, query: str, role: str = "admin", **kwargs):
    inp = {"query": query, "role": role, **kwargs}
    resp = await client.post(
        "/admin/graphql",
        json={"query": _SUBMIT_MUTATION, "variables": {"input": inp}},
    )
    return resp


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
        """Successful submit returns a positive integer queryId."""
        resp = await _submit(client, "query MonthlyRevenue { sales_analytics__orders { id amount region } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        result = body["data"]["submitQuery"]
        assert isinstance(result["queryId"], int) and result["queryId"] > 0

    async def test_submit_returns_operation_name(self, client):
        """Submit response echoes back the operation name."""
        resp = await _submit(client, "query SalesReport { sales_analytics__orders { id amount } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        assert body["data"]["submitQuery"]["operationName"] == "SalesReport"

    async def test_submit_returns_confirmation_message(self, client):
        """Submit response includes a human-readable confirmation message."""
        resp = await _submit(client, "query CustomerList { sales_analytics__customers { id name } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        message = body["data"]["submitQuery"]["message"]
        assert "CustomerList" in message
        assert "submitted" in message.lower()


class TestSubmitRequiresOperationName:
    async def test_submit_without_operation_name_error(self, client):
        """Query without a named operation is rejected (REQ-162)."""
        resp = await _submit(client, "{ sales_analytics__orders { id amount } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body

    async def test_submit_anonymous_query_error(self, client):
        """Anonymous query keyword (no name) is rejected."""
        resp = await _submit(client, "query { sales_analytics__orders { id } }")
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body


class TestSubmitWithMetadata:
    async def test_submit_with_business_purpose(self, client):
        """Submit stores optional business metadata fields."""
        resp = await _submit(
            client,
            "query RevenueByRegion { sales_analytics__orders { id amount region } }",
            businessPurpose="Monthly revenue tracking by region",
            ownerTeam="analytics",
            dataSensitivity="internal",
            expectedRowCount="< 10000",
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        assert body["data"]["submitQuery"]["queryId"] > 0

    async def test_submit_with_sink_config(self, client):
        """Submit with a Kafka sink configuration stores the sink spec."""
        resp = await _submit(
            client,
            "query OrderStream { sales_analytics__orders { id amount } }",
            sink={"topic": "order-events", "trigger": "change_event", "keyColumn": "id"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        assert body["data"]["submitQuery"]["queryId"] > 0

    async def test_submit_with_expiry_date(self, client):
        """Submit accepts an optional expiryDate for the approved query."""
        resp = await _submit(
            client,
            "query QuarterlyReport { sales_analytics__orders { id } }",
            expiryDate="2026-12-31",
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" not in body, body.get("errors")
        assert body["data"]["submitQuery"]["queryId"] > 0


class TestSubmitGovernanceGate:
    async def test_submit_unknown_role_error(self, client):
        """Unknown role returns GQL error."""
        resp = await _submit(
            client,
            "query Test { sales_analytics__orders { id } }",
            role="nonexistent",
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "errors" in body


class TestSubmitIdempotency:
    async def test_submit_same_operation_twice_returns_distinct_ids(self, client):
        """Each submit call produces a new pending entry, even for the same op name."""
        query = "query DuplicateTest { sales_analytics__orders { id } }"

        resp1 = await _submit(client, query)
        resp2 = await _submit(client, query)

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        assert "errors" not in resp1.json(), resp1.json().get("errors")
        assert "errors" not in resp2.json(), resp2.json().get("errors")
        id1 = resp1.json()["data"]["submitQuery"]["queryId"]
        id2 = resp2.json()["data"]["submitQuery"]["queryId"]
        assert id1 != id2
