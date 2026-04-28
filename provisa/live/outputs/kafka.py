# Copyright (c) 2026 Kenneth Stott
# Canary: f6a7b8c9-d0e1-2345-f012-456789012345
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka sink output for live queries (Phase AM).

Publishes new rows to a Kafka topic using the confluent-kafka producer.
Each row is serialized as JSON.  If a *key_column* is configured its value
is used as the Kafka message key, enabling per-entity partitioning.
"""

from __future__ import annotations

import json
import logging

from provisa.live.outputs.base import LiveOutput

log = logging.getLogger(__name__)


class KafkaSinkOutput(LiveOutput):
    """Produce live query rows to a Kafka topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        key_column: str | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._key_column = key_column
        self._producer = None

    def _ensure_producer(self):
        if self._producer is not None:
            return
        try:
            from confluent_kafka import Producer  # type: ignore[import-untyped]
            self._producer = Producer({"bootstrap.servers": self._bootstrap_servers})
        except ImportError:
            raise RuntimeError("confluent-kafka is required for Kafka live output")

    async def send(self, rows: list[dict]) -> None:
        if not rows:
            return
        self._ensure_producer()
        for row in rows:
            value = json.dumps(row).encode()
            key = None
            if self._key_column and self._key_column in row:
                key = str(row[self._key_column]).encode()
            self._producer.produce(self._topic, value=value, key=key)
        self._producer.poll(0)
        log.debug("[KAFKA LIVE] produced %d rows to %s", len(rows), self._topic)

    async def close(self) -> None:
        if self._producer:
            self._producer.flush()
            self._producer = None
