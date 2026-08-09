# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4d1e77-58ab-4f0e-b2a1-7e63c05d8f94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka: config-declared topic through to a served semantic query.

Kafka does not register from a ``Source`` row — ``TrinoKafkaConnector.details`` returns ``{}``, so
``createSource`` has nothing to build catalog properties from. A Kafka topic becomes a queryable
table through ``kafka_sources[]`` at config load: ``_process_kafka_sources``
(``provisa/api/app_loaders.py:114``) calls ``register_kafka_catalog`` — which writes the connector
``.properties`` plus a per-topic table-description JSON under ``trino_etc_dir()/kafka`` and issues
``CREATE CATALOG`` — and appends one ``tables[]`` entry per topic. So this module drives
``PROVISA_CONFIG`` where every other module in this bucket drives the admin mutations, and the thing
under test is the same: does the topic reach ``/data/sql`` as ``"domain"."table"``.

The messages are produced over the HOST listener while Trino consumes over the internal one
(``kafka:29092``); both reach the same broker.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.integration.connector_source_harness import query_semantic, rebuild_schemas

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_kafka,
    pytest.mark.asyncio(loop_scope="session"),
]

_CONFIG = Path(__file__).resolve().parents[1] / "fixtures" / "kafka_pipeline_config.yaml"
_DOMAIN = "kafka_e2e"
_TABLE = "provisa_pipeline_orders"
_TOPIC = "provisa.pipeline.orders"
_ORDERS = [
    {"order_id": "O-100", "sku": "W-001"},
    {"order_id": "O-101", "sku": "W-002"},
    {"order_id": "O-102", "sku": "W-003"},
]


@pytest_asyncio.fixture(loop_scope="session")
async def kafka_client(trino_conn):
    """An in-process app bound to the isolated stack and to the Kafka fixture config.

    This is ``connector_source_harness.connector_client`` plus the ``PROVISA_CONFIG`` pin: the Kafka
    catalog and its semantic table are created by config load inside the lifespan, so the pin has to
    be in place before ``create_app``'s lifespan runs and removed after, leaving the rest of the
    session on the default config. The engine is rebuilt for the same reason it is there —
    ``AppState.__init__`` already called ``build_engine`` at import of ``provisa.api.app``, long
    before any fixture, so setting ``PROVISA_ENGINE`` alone would have no effect.
    """
    import provisa.api.app as app_mod
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    prior = {k: os.environ.get(k) for k in ("PROVISA_ENGINE", "PROVISA_CONFIG")}
    os.environ["PROVISA_ENGINE"] = "trino"
    os.environ["PROVISA_CONFIG"] = str(_CONFIG)
    os.environ.setdefault("PG_PASSWORD", "provisa")
    prior_engine = app_mod.state.federation_engine
    app_mod.state.federation_engine = EngineRuntime(build_engine(), app_mod.state)
    app_mod.state.federation_engine.bind_terminal()
    try:
        app = app_mod.create_app()
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test", timeout=180) as c:
                yield c
    finally:
        app_mod.state.federation_engine = prior_engine
        for key, value in prior.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        # Two Kafka-connector catalogs (this one and kafka_support from test_kafka_source.py)
        # coexisting in the same Trino coordinator cause CREATE CATALOG to fail for the second
        # catalog — drop this catalog as soon as the test is done so at most one Kafka-connector
        # catalog that shares kafka.table-description-dir is ever live at a time.
        cur = trino_conn.cursor()
        cur.execute("DROP CATALOG IF EXISTS kafka_pipeline")
        cur.fetchall()


async def _create_topic() -> None:
    """Create the topic explicitly rather than leaning on broker-side auto-creation.

    A producer's ``allow_auto_topic_creation`` makes topic existence a race against the
    controller: the first send returns UNKNOWN_TOPIC_OR_PARTITION, the broker creates the topic
    out of band, and the client's metadata refresh may or may not see partitions before
    ``request_timeout_ms`` expires (it did not, on a cold ``kafka_test_data`` volume). The topic
    is a precondition of this test, so it is established, not hoped for.
    """
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"])
    await admin.start()
    try:
        await admin.create_topics([NewTopic(_TOPIC, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass  # a prior run in the same stack left it behind; the messages below still land
    finally:
        await admin.close()


async def _produce_orders() -> None:
    """Publish the orders as JSON, matching the topic's declared columns."""
    from aiokafka import AIOKafkaProducer

    await _create_topic()
    producer = AIOKafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"])
    await producer.start()
    try:
        for order in _ORDERS:
            await producer.send_and_wait(_TOPIC, json.dumps(order).encode())
    finally:
        await producer.stop()


async def test_kafka_topic_serves_semantic_query(kafka_client):
    await _produce_orders()
    await rebuild_schemas(kafka_client)
    sql = f'SELECT "order_id", "sku" FROM "{_DOMAIN}"."{_TABLE}" ORDER BY "order_id"'
    rows = await query_semantic(kafka_client, sql)
    assert rows == _ORDERS, f"served rows {rows!r} != produced {_ORDERS!r}"
