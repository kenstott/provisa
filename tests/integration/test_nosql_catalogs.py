# Copyright (c) 2026 Kenneth Stott
# Canary: 4f6ea13b-8cc5-486b-b764-a442fda9df80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-251: NoSQL/non-relational catalog generation verified against live Trino.

Drives the real create_catalog path (catalog_properties_for -> dynamic CREATE
CATALOG) for a Prometheus source and queries a metric end-to-end. Prometheus is
the connector that needs no on-disk table-description files, so it is fully
verifiable against the running stack.
"""

from __future__ import annotations

import os
import time

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture(scope="module", autouse=True)
def _wait_for_trino():
    """Wait for Trino to finish initializing before running Trino tests."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(
                host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Trino did not become ready within 120s")


_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
# Reachable from the Trino container's network (compose service name).
_PROM_URL = os.environ.get("PROM_INTERNAL_URL", "http://prometheus:9090")


def _trino_cursor():
    import trino

    conn = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()
    return conn, cur


def _drop(cur, name):
    try:
        cur.execute(f"DROP CATALOG {name}")
        cur.fetchall()
    except Exception:
        pass


@pytest.mark.requires_prometheus
async def test_prometheus_catalog_created_and_queryable():
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    conn, cur = _trino_cursor()

    catalog = "prom_itest"
    _drop(cur, catalog)
    src = Source(id="prom-itest", type=SourceType.prometheus, mapping={"url": _PROM_URL})
    try:
        # Real path: builds props via catalog_properties_for, issues CREATE CATALOG.
        create_catalog(conn, src, "")

        # The catalog now exists and exposes the prometheus 'default' schema.
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert "default" in schemas

        # The 'up' metric appears once Prometheus has completed its first self-scrape; querying it
        # proves data flows through. Retry to absorb the initial scrape interval on a fresh stack.
        rows: list = []
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SELECT value FROM {catalog}.default.up LIMIT 1")
                rows = cur.fetchall()
            except trino.exceptions.TrinoExternalError:
                rows = []  # PROMETHEUS_UNKNOWN_ERROR before the first scrape lands
            if rows:
                break
            time.sleep(3)
        assert len(rows) >= 1
    finally:
        _drop(cur, catalog)


_KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


@pytest.mark.requires_kafka
async def test_kafka_sample_infers_record_layout():
    """REQ-250 SAMPLE: produce JSON records, sample the topic, infer column types.

    The no-Confluent path — sampling a topic and proposing the record layout.
    """
    from aiokafka import AIOKafkaProducer

    from provisa.kafka.source import infer_columns_from_records, sample_topic_records

    import json as _json

    topic = "itest.orders"
    producer = AIOKafkaProducer(bootstrap_servers=_KAFKA_BOOTSTRAP, request_timeout_ms=3000)
    await producer.start()
    try:
        for i in range(8):
            rec = {"order_id": i, "amount": round(i * 1.5, 2), "paid": i % 2 == 0, "region": "us"}
            await producer.send_and_wait(topic, _json.dumps(rec).encode())
    finally:
        await producer.stop()

    records = await sample_topic_records(_KAFKA_BOOTSTRAP, topic, max_records=20, timeout_ms=3000)
    assert len(records) >= 8
    cols = {c.name: (c.data_type, c.is_complex) for c in infer_columns_from_records(records)}
    assert cols["order_id"] == ("BIGINT", False)
    assert cols["amount"] == ("DOUBLE", False)
    assert cols["paid"] == ("BOOLEAN", False)
    assert cols["region"] == ("VARCHAR", False)
