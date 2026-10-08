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

Publishes new rows to a Kafka topic through the process's producer (provisa/kafka/producer.py).
Each row is serialized as JSON.  If a *key_column* is configured its value
is used as the Kafka message key, enabling per-entity partitioning.
"""

from __future__ import annotations

import json
import logging

from provisa.kafka.producer import Producer, shared
from provisa.live.outputs.base import LiveOutput

log = logging.getLogger(__name__)

# Requirements: REQ-176, REQ-181, REQ-286


class KafkaSinkOutput(LiveOutput):  # REQ-176, REQ-181, REQ-286
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
        self._producer: Producer | None = None

    def _ensure_producer(self) -> None:
        if self._producer is None:
            self._producer = shared(self._bootstrap_servers)

    async def send(self, rows: list[dict]) -> None:  # REQ-565
        if not rows:
            return
        self._ensure_producer()
        assert self._producer is not None
        for row in rows:
            value = json.dumps(row).encode()
            key = None
            if self._key_column and self._key_column in row:
                key = str(row[self._key_column]).encode()
            self._producer.send(self._topic, value=value, key=key)
        log.debug("[KAFKA LIVE] handed %d rows to the producer for %s", len(rows), self._topic)

    async def close(self) -> None:  # REQ-565
        # The producer is the process's: the lifespan stops it, sending what it was handed first.
        self._producer = None
