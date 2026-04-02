# Copyright (c) 2025 Kenneth Stott
# Canary: a4185b63-252b-4df8-8278-32b6ffb029d0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka sink — publish query results to Kafka topics (REQ-115).

After query execution, serialize result rows as JSON and produce to a topic.
Async fire-and-forget with delivery callback.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class KafkaSinkConfig:
    """Configuration for publishing query results to a Kafka topic."""

    query_stable_id: str  # approved query stable_id
    topic: str
    key_column: str | None = None  # column to use as message key
    value_format: str = "json"


class KafkaProducer:
    """Async Kafka producer wrapper.

    Uses confluent-kafka producer under the hood.
    """

    def __init__(self, bootstrap_servers: str, **kwargs):
        self._bootstrap_servers = bootstrap_servers
        self._producer = None
        self._extra_config = kwargs

    def _ensure_producer(self):
        if self._producer is None:
            try:
                from confluent_kafka import Producer
                config = {
                    "bootstrap.servers": self._bootstrap_servers,
                    "client.id": "provisa-sink",
                    **self._extra_config,
                }
                self._producer = Producer(config)
            except ImportError:
                raise ImportError(
                    "confluent-kafka is required for Kafka sink. "
                    "Install with: pip install confluent-kafka"
                )

    def _delivery_callback(self, err, msg):
        if err:
            log.error("Kafka delivery failed: %s", err)
        else:
            log.debug(
                "Kafka message delivered to %s [%d] @ %d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    async def publish_rows(
        self,
        topic: str,
        rows: list[dict],
        columns: list[str],
        key_column: str | None = None,
    ) -> int:
        """Publish query result rows to a Kafka topic.

        Each row becomes a JSON message. Fire-and-forget with delivery callback.

        Args:
            topic: Kafka topic name.
            rows: List of row dicts from query result.
            columns: Column names for serialization.
            key_column: Column to use as message key (optional).

        Returns:
            Number of messages produced.
        """
        self._ensure_producer()
        count = 0

        for row in rows:
            # Build message value
            if isinstance(row, dict):
                value = json.dumps(row, default=str).encode("utf-8")
            else:
                # Row is a tuple — zip with column names
                row_dict = dict(zip(columns, row))
                value = json.dumps(row_dict, default=str).encode("utf-8")

            # Build message key
            key = None
            if key_column:
                key_val = row.get(key_column) if isinstance(row, dict) else None
                if key_val is not None:
                    key = str(key_val).encode("utf-8")

            self._producer.produce(
                topic,
                value=value,
                key=key,
                callback=self._delivery_callback,
            )
            count += 1

        # Trigger delivery of buffered messages (non-blocking flush)
        self._producer.poll(0)
        return count

    def flush(self, timeout: float = 5.0) -> None:
        """Flush all buffered messages. Call on shutdown."""
        if self._producer:
            self._producer.flush(timeout)

    def close(self) -> None:
        """Close the producer."""
        if self._producer:
            self._producer.flush(5.0)
            self._producer = None
