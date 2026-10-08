# Copyright (c) 2026 Kenneth Stott
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

from provisa.kafka.producer import Producer, shared

# Requirements: REQ-176, REQ-177, REQ-178, REQ-180, REQ-181

log = logging.getLogger(__name__)


@dataclass
class KafkaSinkConfig:  # REQ-176, REQ-177, REQ-178, REQ-180
    """Configuration for publishing query results to a Kafka topic."""

    query_stable_id: str  # sink query stable_id
    topic: str
    key_column: str | None = None  # column to use as message key
    value_format: str = "json"


class KafkaProducer:  # REQ-176, REQ-181
    """Publishes query result rows to topics of one Kafka cluster, through the process's producer
    for that cluster (``provisa.kafka.producer``)."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: Producer | None = None

    def _ensure_producer(self) -> None:
        if self._producer is None:
            self._producer = shared(self._bootstrap_servers)

    async def publish_rows(  # REQ-181
        self,
        topic: str,
        rows: list[dict],
        columns: list[str],
        key_column: str | None = None,
    ) -> int:
        """Publish query result rows to a Kafka topic.

        Each row becomes a JSON message, handed to the producer's own thread: this returns
        without waiting on the broker, and a row that cannot be delivered is dropped and logged
        there.

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

        from decimal import Decimal as _Decimal

        def _serial(o):
            if isinstance(o, _Decimal):
                return float(o)
            return str(o)

        for row in rows:
            # Build message value
            if isinstance(row, dict):
                value = json.dumps(row, default=_serial).encode("utf-8")
            else:
                # Row is a tuple — zip with column names
                row_dict = dict(zip(columns, row))
                value = json.dumps(row_dict, default=_serial).encode("utf-8")

            # Build message key
            key = None
            if key_column:
                key_val = row.get(key_column) if isinstance(row, dict) else None
                if key_val is not None:
                    key = str(key_val).encode("utf-8")

            assert self._producer is not None
            self._producer.send(topic, value=value, key=key)
            count += 1
        return count

    def close(self) -> None:
        """Publish no more through this sink. The producer is the process's and is stopped by
        the lifespan, which sends what it was handed first."""
        self._producer = None
