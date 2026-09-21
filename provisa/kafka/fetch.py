# Copyright (c) 2026 Kenneth Stott
# Canary: e7f1a4c9-3b6d-4e82-9f7a-1c5d8e4b2a90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka over aiokafka, engine-independently (REQ-1730).

Trino's own connector (``TrinoKafkaConnector``) was the only reader of a Kafka source, and even
there only when a Confluent Schema Registry is configured (``source.database`` — its own ``details``
returns no catalog properties otherwise, so an unregistered topic is unreachable on Trino too).
Every other engine has no live Kafka reach at all — same gap REQ-1672/1675/1676/(REQ-1730's own
mongodb fix) already closed for Elasticsearch/Redis/Cassandra/MongoDB.

A topic is not a table: it is an unbounded append log. "Current rows" here means the same bounded
read ``kafka.source.sample_topic_records`` (schema discovery, REQ-150) already performs and Trino's
own kafka connector performs when queried without streaming semantics — drain messages from the
earliest offset until the broker answers an empty batch, never a live/infinite consume. A later call
re-reads the same growing prefix, exactly like the SQL-federatable engines' own
``SELECT * FROM ref`` re-reads a mutable table. Unlike ``sample_topic_records`` (capped at a small
sample for schema typing), this has no record cap — every current message lands.

Message decoding mirrors ``subscriptions.kafka_provider.KafkaNotificationProvider.watch``'s own
convention: JSON body, optionally wrapped as ``{"op": ..., "row": {...}}`` (only ``row`` is kept
here — a fetch has no operation semantics, unlike the CDC subscription path), else the decoded body
itself is the row.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

_DRAIN_TIMEOUT_MS = 5000


@dataclass(frozen=True)
class KafkaConnection:  # REQ-1730
    bootstrap_servers: str

    @classmethod
    def build(cls, host: str, port: int | None) -> "KafkaConnection":
        # The source's ``host`` may list several brokers, comma-separated (same convention as
        # push_wiring.py's CDC listener and TrinoKafkaConnector.details()'s kafka.nodes).
        servers = f"{host}:{port}" if port else (host or "localhost:9092")
        return cls(bootstrap_servers=servers)


def _decode_row(raw: bytes | str | None) -> dict | None:
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(value, dict):
        row = value.get("row", value)
        return row if isinstance(row, dict) else {"value": row}
    return {"value": value}


async def fetch_rows(
    conn: KafkaConnection, topic: str, columns: list[str]
) -> list[dict]:  # REQ-1730
    """Every message currently on ``topic``, from the earliest offset until the broker answers an
    empty batch, ``columns`` projected. ``[]`` when the topic does not exist or is empty."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=conn.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    rows: list[dict] = []
    try:
        while True:
            batch = await consumer.getmany(timeout_ms=_DRAIN_TIMEOUT_MS)
            if not batch:
                break
            for messages in batch.values():
                for msg in messages:
                    row = _decode_row(msg.value)
                    if row is not None:
                        rows.append({c: row.get(c) for c in columns})
    finally:
        await consumer.stop()
    return rows
