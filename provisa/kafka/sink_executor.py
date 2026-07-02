# Copyright (c) 2026 Kenneth Stott
# Canary: f2c393dd-e15a-4a02-acba-52a447da7207
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka sink executor (REQ-176 through REQ-181).

When a dataset change event fires, finds approved queries with change_event
sinks targeting the changed table, re-executes them, and publishes results
to the configured Kafka topic.
"""

from __future__ import annotations

import json
import logging
import os
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.app import AppState

# Requirements: REQ-176, REQ-177, REQ-181, REQ-282

log = logging.getLogger(__name__)


class _Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if hasattr(o, "isoformat"):
            return o.isoformat()
        return str(o)


async def trigger_sinks_for_table(table_name: str, state: AppState) -> int:  # REQ-176, REQ-177
    """Find and execute change_event-triggered sinks for the given table."""

    triggered = 0
    for table in state.config.tables:
        if table.table_name != table_name:
            continue
        if table.kafka_sink is None:
            continue
        if "change_event" not in table.kafka_sink.triggers:
            continue
        await _execute_and_publish_table_sink(table, state)
        triggered += 1
    return triggered


async def _execute_and_publish_table_sink(table, state: AppState) -> None:  # REQ-176, REQ-181
    """Execute a SELECT on the table and publish rows to its Kafka sink."""
    if state.tenant_db is None:
        log.warning("No tenant_db for sink execution on %s", table.table_name)
        return
    sink = table.kafka_sink
    assert sink is not None

    bootstrap = os.environ.get(
        "PROVISA_CHANGE_EVENT_BOOTSTRAP",
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
    )
    if not bootstrap:
        log.warning("No Kafka bootstrap for sink on %s", table.table_name)
        return

    async with state.tenant_db.acquire() as conn:
        rows_raw = await conn.fetch(
            f'SELECT * FROM "{table.schema_name}"."{table.table_name}" LIMIT 1000'
        )
    rows = [dict(r) for r in rows_raw]

    from confluent_kafka import Producer  # pyright: ignore[reportMissingImports]

    producer = Producer({"bootstrap.servers": bootstrap})
    for row in rows:
        key = None
        if sink.key_column and sink.key_column in row:
            key = str(row[sink.key_column]).encode()
        producer.produce(
            sink.topic,
            key=key,
            value=json.dumps(row, cls=_Encoder).encode(),
        )
    producer.flush(timeout=10)
    log.info("Sink published %d rows to %s for table %s", len(rows), sink.topic, table.table_name)
