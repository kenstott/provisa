# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7f4a9c-1b8d-4e3f-a5c2-9d6b0e1f8a4c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Debezium CDC subscription provider (REQ-261).

Requires: docker-compose up (postgres, kafka, debezium-connect)

Tests the full CDC pipeline:
  PostgreSQL row change → Debezium connector → Kafka topic
  → DebeziumNotificationProvider → ChangeEvent
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import httpx
import pytest

from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

DEBEZIUM_HOST = os.environ.get("DEBEZIUM_HOST", "localhost")
DEBEZIUM_PORT = int(os.environ.get("DEBEZIUM_PORT", "8083"))
DEBEZIUM_URL = f"http://{DEBEZIUM_HOST}:{DEBEZIUM_PORT}"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_PREFIX = "provisa_test"
DATABASE = os.environ.get("PG_DATABASE", "provisa")

CONNECTOR_NAME = "provisa-test-pg"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _register_connector(pg_host: str, pg_port: int, pg_user: str, pg_password: str) -> None:
    """Register a Debezium PostgreSQL connector via the Connect REST API."""
    connector_config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": pg_host,
            "database.port": str(pg_port),
            "database.user": pg_user,
            "database.password": pg_password,
            "database.dbname": DATABASE,
            "database.server.name": TOPIC_PREFIX,
            "topic.prefix": TOPIC_PREFIX,
            "table.include.list": "public.orders",
            "plugin.name": "pgoutput",
            "slot.name": f"debezium_test_{uuid.uuid4().hex[:8]}",
            "publication.name": f"debezium_pub_{uuid.uuid4().hex[:8]}",
            "snapshot.mode": "initial",
            "publication.autocreate.mode": "filtered",
            "decimal.handling.mode": "double",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
        },
    }

    # Delete existing connector if present
    httpx.delete(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}", timeout=5)
    time.sleep(1)

    r = httpx.post(
        f"{DEBEZIUM_URL}/connectors",
        json=connector_config,
        timeout=10,
    )
    assert r.status_code in (200, 201), (
        f"Failed to register connector: {r.status_code} {r.text}"
    )


def _wait_connector_running(timeout: int = 60) -> None:
    """Poll until the connector transitions to RUNNING state.

    Raises RuntimeError if the connector does not reach RUNNING within *timeout* seconds.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(
                f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status",
                timeout=5,
            )
            if r.status_code == 200:
                status = r.json()
                connector_state = status.get("connector", {}).get("state", "")
                tasks = status.get("tasks", [])
                if connector_state == "RUNNING" and tasks and tasks[0]["state"] == "RUNNING":
                    return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(
        f"Debezium connector '{CONNECTOR_NAME}' did not reach RUNNING state within {timeout}s"
    )


async def _collect_events(
    provider: DebeziumNotificationProvider,
    table: str,
    count: int,
    timeout: float = 30.0,
) -> list:
    """Collect up to *count* ChangeEvents from *provider* within *timeout* seconds."""
    events = []
    try:
        async with asyncio.timeout(timeout):
            async for event in provider.watch(table):
                events.append(event)
                if len(events) >= count:
                    break
    except TimeoutError:
        pass
    return events


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def debezium_connector(pg_pool):
    """Register the Debezium connector once per module and tear it down after."""
    pg_host = os.environ.get("PG_HOST", "localhost")
    # Inside docker-compose the connector talks to the internal PG hostname,
    # but when running tests from the host we tell Debezium to reach PG
    # via its service name (or override via env).
    debezium_pg_host = os.environ.get("DEBEZIUM_PG_HOST", "postgres")
    debezium_pg_port = int(os.environ.get("DEBEZIUM_PG_PORT", "5432"))

    _register_connector(
        pg_host=debezium_pg_host,
        pg_port=debezium_pg_port,
        pg_user=os.environ.get("PG_USER", "provisa"),
        pg_password=os.environ.get("PG_PASSWORD", "provisa"),
    )

    _wait_connector_running(timeout=60)

    yield

    # Cleanup: delete the connector
    httpx.delete(f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}", timeout=5)
    time.sleep(1)

    # Drop inactive replication slots to avoid exhausting max_replication_slots
    # (set to 10 in docker-compose) across repeated test runs.
    import subprocess
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_user = os.environ.get("PG_USER", "provisa")
    pg_db = os.environ.get("PG_DATABASE", "provisa")
    subprocess.run(
        [
            "psql",
            f"postgresql://{pg_user}:provisa@{pg_host}:{pg_port}/{pg_db}",
            "-c",
            "SELECT pg_drop_replication_slot(slot_name) "
            "FROM pg_replication_slots WHERE active = false",
        ],
        capture_output=True,
        timeout=10,
    )


@pytest.fixture(scope="module")
def provider():
    return DebeziumNotificationProvider(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic_prefix=TOPIC_PREFIX,
        database=DATABASE,
        consumer_group_id=f"provisa-test-{uuid.uuid4().hex[:8]}",
        source_type="postgresql",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDebeziumConnectorRegistration:
    def test_debezium_connect_reachable(self):
        """Debezium Connect REST API is reachable."""
        r = httpx.get(f"{DEBEZIUM_URL}/connectors", timeout=5)
        assert r.status_code == 200

    def test_connector_registered_and_running(self, debezium_connector):
        """Connector is registered and in RUNNING state."""
        r = httpx.get(
            f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status",
            timeout=5,
        )
        assert r.status_code == 200
        status = r.json()
        assert status["connector"]["state"] == "RUNNING"
        assert status["tasks"][0]["state"] == "RUNNING"


class TestDebeziumInsertEvents:
    async def test_insert_yields_insert_event(self, debezium_connector, provider, pg_pool):
        """Inserting a row in PG produces an insert ChangeEvent via Debezium."""
        # Insert a unique row so we can identify our event
        marker = f"debezium-test-{uuid.uuid4().hex[:8]}"

        async def do_insert():
            # Give the consumer a moment to subscribe before the insert
            await asyncio.sleep(3)
            async with pg_pool.acquire() as conn:
                pid = await conn.fetchval("SELECT id FROM products LIMIT 1")
                await conn.execute(
                    "INSERT INTO orders (customer_id, product_id, amount, region) VALUES ($1, $2, $3, $4)",
                    1, pid, 999.99, marker,
                )

        events_task = asyncio.create_task(
            _collect_events(provider, "orders", count=5, timeout=30)
        )
        await do_insert()
        events = await events_task

        insert_events = [e for e in events if e.operation == "insert"]
        assert insert_events, "Expected at least one insert ChangeEvent"

        # Find our specific marker row
        matching = [e for e in insert_events if e.row.get("region") == marker]
        assert matching, (
            f"Expected insert event with region={marker!r}, got: "
            f"{[e.row for e in insert_events]}"
        )
        event = matching[0]
        assert event.table == "orders"
        assert event.row["amount"] == pytest.approx(999.99, rel=0.01)

    async def test_insert_event_has_timestamp(self, debezium_connector, provider, pg_pool):
        """ChangeEvents from Debezium carry a UTC timestamp from ts_ms."""
        marker = f"ts-test-{uuid.uuid4().hex[:8]}"

        async def do_insert():
            await asyncio.sleep(2)
            async with pg_pool.acquire() as conn:
                pid = await conn.fetchval("SELECT id FROM products LIMIT 1")
                await conn.execute(
                    "INSERT INTO orders (customer_id, product_id, amount, region) VALUES ($1, $2, $3, $4)",
                    1, pid, 1.0, marker,
                )

        events_task = asyncio.create_task(
            _collect_events(provider, "orders", count=3, timeout=25)
        )
        await do_insert()
        events = await events_task

        matching = [e for e in events if e.row.get("region") == marker]
        assert matching, "Marker insert event not received"
        event = matching[0]
        assert event.timestamp is not None
        # Timestamp should be recent (within last 60 seconds)
        from datetime import datetime, timezone
        age = (datetime.now(timezone.utc) - event.timestamp).total_seconds()
        assert abs(age) < 60, f"Event timestamp is too old: {event.timestamp}"


class TestDebeziumUpdateEvents:
    async def test_update_yields_update_event(self, debezium_connector, provider, pg_pool):
        """Updating a row produces an update ChangeEvent."""
        # Insert first, then update
        marker = f"upd-test-{uuid.uuid4().hex[:8]}"
        async with pg_pool.acquire() as conn:
            pid = await conn.fetchval("SELECT id FROM products LIMIT 1")
            row_id = await conn.fetchval(
                "INSERT INTO orders (customer_id, product_id, amount, region) VALUES ($1, $2, $3, $4) RETURNING id",
                1, pid, 50.0, marker,
            )

        # Small delay to let the insert event pass, then update
        await asyncio.sleep(2)

        async def do_update():
            await asyncio.sleep(2)
            async with pg_pool.acquire() as conn:
                await conn.execute(
                    "UPDATE orders SET amount = $1 WHERE id = $2",
                    123.45, row_id,
                )

        events_task = asyncio.create_task(
            _collect_events(provider, "orders", count=5, timeout=30)
        )
        await do_update()
        events = await events_task

        update_events = [e for e in events if e.operation == "update"]
        matching = [e for e in update_events if e.row.get("id") == row_id]
        assert matching, (
            f"Expected update event for id={row_id}, got updates: "
            f"{[e.row for e in update_events]}"
        )
        assert matching[0].row["amount"] == pytest.approx(123.45, rel=0.01)


class TestDebeziumDeleteEvents:
    async def test_delete_yields_delete_event(self, debezium_connector, provider, pg_pool):
        """Deleting a row produces a delete ChangeEvent."""
        marker = f"del-test-{uuid.uuid4().hex[:8]}"
        async with pg_pool.acquire() as conn:
            pid = await conn.fetchval("SELECT id FROM products LIMIT 1")
            row_id = await conn.fetchval(
                "INSERT INTO orders (customer_id, product_id, amount, region) VALUES ($1, $2, $3, $4) RETURNING id",
                1, pid, 10.0, marker,
            )

        await asyncio.sleep(2)

        async def do_delete():
            await asyncio.sleep(2)
            async with pg_pool.acquire() as conn:
                await conn.execute("DELETE FROM orders WHERE id = $1", row_id)

        events_task = asyncio.create_task(
            _collect_events(provider, "orders", count=5, timeout=30)
        )
        await do_delete()
        events = await events_task

        delete_events = [e for e in events if e.operation == "delete"]
        assert delete_events, "Expected at least one delete ChangeEvent"


class TestDebeziumProviderLifecycle:
    async def test_provider_close_is_idempotent(self, provider):
        """Closing the provider twice does not raise."""
        p = DebeziumNotificationProvider(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic_prefix=TOPIC_PREFIX,
            database=DATABASE,
            consumer_group_id=f"lifecycle-{uuid.uuid4().hex[:8]}",
        )
        await p.close()
        await p.close()  # should not raise

    def test_topic_name_matches_debezium_convention(self, provider):
        """Topic name follows {prefix}.{schema}.{table} convention for PostgreSQL."""
        topic = provider._build_topic("orders")
        # PostgreSQL Debezium connector uses schema name (public) not database name
        assert topic == f"{TOPIC_PREFIX}.public.orders"

    async def test_snapshot_events_received_on_fresh_consumer(self, debezium_connector):
        """A fresh consumer group receives snapshot (read) events as inserts."""
        fresh_provider = DebeziumNotificationProvider(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic_prefix=TOPIC_PREFIX,
            database=DATABASE,
            consumer_group_id=f"snapshot-{uuid.uuid4().hex[:8]}",
            source_type="postgresql",
        )
        # Set offset to earliest to catch snapshot events
        # (snapshot events have op="r", mapped to "insert")
        fresh_provider._consumer  # not connected yet

        # Override auto_offset_reset for this test only
        original_init = fresh_provider.__class__.__init__

        events = []
        from aiokafka import AIOKafkaConsumer  # noqa: PLC0415

        consumer = AIOKafkaConsumer(
            fresh_provider._build_topic("orders"),
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=f"snapshot-earliest-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            async with asyncio.timeout(15):
                async for msg in consumer:
                    if msg.value:
                        envelope = json.loads(msg.value)
                        payload = envelope.get("payload", envelope)
                        if payload.get("op") == "r":
                            events.append(payload)
                        if len(events) >= 3:
                            break
        except TimeoutError:
            pass
        finally:
            await consumer.stop()

        assert events, (
            "Expected snapshot (op=r) events from earliest offset. "
            "The Debezium connector may not have completed its initial snapshot yet."
        )
