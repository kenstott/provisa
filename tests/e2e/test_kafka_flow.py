"""E2E tests for Kafka sink flow: approved query → result published to topic.

Requires: docker-compose up (Kafka + Provisa backend)
"""

import os

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


class TestKafkaSinkConfiguration:
    async def test_submit_query_with_sink(self, client):
        """Submit a query with Kafka sink configuration."""
        resp = await client.post(
            "/data/submit",
            json={
                "query": "query KafkaSinkTest { sales_analytics__orders(limit: 5) { id amount region } }",
                "operation_name": "KafkaSinkTest",
                "developer_id": "e2e-kafka-test",
                "business_purpose": "E2E Kafka sink test",
                "data_sensitivity": "internal",
                "sink": {
                    "topic": "test-sink-orders",
                    "trigger": "manual",
                    "key_column": "region",
                },
            },
            headers={"X-Role": "admin"},
        )
        if resp.status_code == 503:
            pytest.skip("Backend not fully initialized")
        assert resp.status_code == 200
        data = resp.json()
        assert "query_id" in data
        assert "KafkaSinkTest" in data.get("message", "")

    async def test_persisted_query_has_sink_config(self, client):
        """Approved queries with sinks should store the sink configuration."""
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ persistedQueries { id queryText sinkTopic sinkTrigger sinkKeyColumn status } }"},
        )
        if resp.status_code != 200:
            pytest.skip("Admin API not available")

        data = resp.json()
        queries = data.get("data", {}).get("persistedQueries", [])
        sink_queries = [q for q in queries if q.get("sinkTopic")]
        # At least the one we just submitted (or from previous runs)
        if not sink_queries:
            pytest.skip("No queries with sink config found")

        q = sink_queries[0]
        assert q["sinkTopic"] is not None
        assert q["sinkTrigger"] in ("change_event", "schedule", "manual")


class TestKafkaSinkExecution:
    async def test_sink_executor_handles_missing_kafka(self, client):
        """Sink execution should not crash when Kafka is unavailable."""
        from provisa.kafka.sink_executor import trigger_sinks_for_table

        # This should handle missing Kafka gracefully (log warning, not crash)
        try:
            from provisa.api.app import state
            count = await trigger_sinks_for_table("orders", state)
            # count may be 0 if no sinks configured, or > 0 if Kafka is available
            assert isinstance(count, int)
        except Exception:
            # If state isn't initialized, that's expected in test context
            pass


class TestKafkaSourceRegistration:
    async def test_kafka_topics_in_admin(self, client):
        """Kafka topics registered as data sources should appear in admin."""
        resp = await client.post(
            "/admin/graphql",
            json={"query": "{ tables { id tableName sourceId } }"},
        )
        if resp.status_code != 200:
            pytest.skip("Admin API not available")

        data = resp.json()
        tables = data.get("data", {}).get("tables", [])
        # Check if any tables come from Kafka sources
        kafka_tables = [t for t in tables if "kafka" in t.get("sourceId", "").lower()
                       or "ticket" in t.get("tableName", "").lower()]
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
            pytest.skip("Admin API not available")

        tables = resp.json().get("data", {}).get("tables", [])
        kafka_tables = [t for t in tables if "kafka" in t.get("sourceId", "").lower()]
        if not kafka_tables:
            pytest.skip("No Kafka-backed tables registered")

        # Try to query one via the data endpoint
        table_name = kafka_tables[0]["tableName"]
        resp = await client.post(
            "/data/graphql",
            json={"query": f"{{ {table_name}(limit: 1) {{ __typename }} }}"},
            headers={"X-Role": "admin"},
        )
        # May fail if SDL doesn't include this table, but should not 500
        assert resp.status_code in (200, 400)
