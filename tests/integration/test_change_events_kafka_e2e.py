# Copyright (c) 2026 Kenneth Stott
# Canary: 03d5d7f2-999c-4881-b48e-499fd94a1d09
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A write through Provisa announces its table on the change-event topic (REQ-172..175), on a
real server and a real broker.

The publisher once imported a Kafka client the product does not declare, so no install could emit
an event: every write logged a failed import and nothing reached the topic, while the unit tests
passed against a mocked import. Here a server started with a broker named inserts, updates and
deletes a row, and a consumer reads what arrived."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from datetime import datetime

import pytest

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_kafka,
    pytest.mark.asyncio(loop_scope="session"),
]

_WRITABLE = {"visible_to": ["org_admin", "analyst"], "writable_by": ["org_admin"]}


@pytest.fixture(scope="module")
def topic() -> str:
    return f"provisa-change-events-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def boot(topic):
    b = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        env={
            "PROVISA_REDIRECT_ENABLED": "false",
            # The broker this session's kafka lane runs (tests/conftest.py names it).
            "PROVISA_CHANGE_EVENT_BOOTSTRAP": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "PROVISA_CHANGE_EVENT_TOPIC": topic,
        },
    )
    b._extra_config = {
        "tables": [
            {
                "source_id": "sales-pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [
                    {"name": "id", "data_type": "integer", "is_primary_key": True, **_WRITABLE},
                    {"name": "region", "data_type": "varchar", **_WRITABLE},
                ],
            }
        ]
    }
    b.create_database()
    try:
        b.start()
        b.wait_all_ready(timeout=300)
        yield b
    finally:
        b.cleanup()


def _sql(b, sql: str) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{b.ports['http']}/data/sql",
        data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": "org_admin"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


async def _events(topic: str, count: int) -> list[tuple[bytes | None, dict]]:
    """The first ``count`` messages on ``topic``, from its beginning."""
    import asyncio

    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    got: list[tuple[bytes | None, dict]] = []
    try:
        async with asyncio.timeout(60):
            async for message in consumer:
                got.append((message.key, json.loads(message.value.decode())))
                if len(got) == count:
                    break
    finally:
        await consumer.stop()
    return got


async def test_an_insert_an_update_and_a_delete_each_announce_the_table(boot, topic):
    for statement in (
        "INSERT INTO sales.orders VALUES (9, 'north')",
        "UPDATE sales.orders SET region = 'south' WHERE id = 9",
        "DELETE FROM sales.orders WHERE id = 9",
    ):
        status, body = _sql(boot, statement)
        assert status == 200, (statement, body, boot.log_text()[-3000:])

    events = await _events(topic, 3)
    assert len(events) == 3, (events, boot.log_text()[-3000:])
    for key, event in events:
        assert key == b"sales-pg.orders"  # REQ-175: keyed by the dataset
        assert (event["table"], event["source"]) == ("orders", "sales-pg")
        assert event["type"] == "mutation"
        assert datetime.fromisoformat(event["timestamp"]).tzinfo is not None
        assert set(event) == {"table", "source", "type", "timestamp"}  # no row-level detail
