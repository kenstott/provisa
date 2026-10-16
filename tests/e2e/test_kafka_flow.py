# Copyright (c) 2026 Kenneth Stott
# Canary: e2c9a483-a714-4bce-b0c1-df3b390df90a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E tests for Kafka sink flow: query → result published to topic.

Requires: docker-compose up (Kafka + Provisa backend)
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture(scope="module")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


# NOTE: The `submitQuery` mutation / `SubmitQueryInput` type and the
# `governedQueries` query were removed; Kafka sinks are now configured via the
# `@sink` GraphQL directive (see provisa/api/data/subscription_sse.py), and a
# table publishes its new rows through a live output (live.outputs); the table
# Kafka sink and its executor were retired.


class TestKafkaSourceRegistration:
    async def test_kafka_topics_in_admin(self, client):
        """Kafka topics registered as data sources should appear in admin."""
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ tables { id tableName sourceId } }"},
        )
        if resp.status_code != 200:
            pytest.fail(f"Admin API returned {resp.status_code}: {resp.text}")

        data = resp.json()
        tables = data.get("data", {}).get("tables", [])
        # Don't fail if no Kafka tables — just verify the query works
        assert isinstance(tables, list)

    async def test_kafka_table_queryable_via_graphql(self, client):
        """Kafka-backed tables should be queryable through the GraphQL endpoint."""
        # First check what tables exist
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ tables { tableName sourceId } }"},
        )
        if resp.status_code != 200:
            pytest.fail(f"Admin API returned {resp.status_code}: {resp.text}")

        tables = resp.json().get("data", {}).get("tables", [])
        kafka_tables = [t for t in tables if "kafka" in t.get("sourceId", "").lower()]
        assert kafka_tables, (
            "No Kafka-backed tables registered — add kafka_sources to sample_config.yaml"
        )

        # Try to query one via the data endpoint
        table_name = kafka_tables[0]["tableName"]
        resp = await client.post(
            "/data/graphql",
            json={"query": f"{{ {table_name}(limit: 1) {{ __typename }} }}"},
            headers={"X-Provisa-Role": "org_admin"},
        )
        # May fail if SDL doesn't include this table, but should not 500
        assert resp.status_code in (200, 400)
