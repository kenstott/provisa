# Copyright (c) 2026 Kenneth Stott
# Canary: 8af97227-d0b9-42b7-ab9d-d7ac5a8b6017
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dataset change event publisher (REQ-172 through REQ-175).

Emits lightweight change events to Kafka when mutations modify data.
Events contain no row-level detail — just which dataset changed and when.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_producer = None
_topic = None


def _get_topic() -> str:
    return os.environ.get("PROVISA_CHANGE_EVENT_TOPIC", "provisa.change-events")


def _get_producer():
    """Lazy-init Kafka producer. Returns None if Kafka unavailable."""
    global _producer
    if _producer is not None:
        return _producer

    bootstrap = os.environ.get("PROVISA_CHANGE_EVENT_BOOTSTRAP")
    if not bootstrap:
        # Try the first kafka_source bootstrap from config
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        return None

    try:
        from confluent_kafka import Producer
        _producer = Producer({"bootstrap.servers": bootstrap})
        log.info("Change event producer connected to %s", bootstrap)
        return _producer
    except Exception:
        log.warning("Failed to create change event producer", exc_info=True)
        return None


def emit_change_event(
    table_name: str,
    source_id: str,
    mutation_type: str = "mutation",
) -> None:
    """Emit a dataset change event to Kafka.

    Args:
        table_name: The table that was modified.
        source_id: The source containing the table.
        mutation_type: Type of change (e.g., "insert", "update", "delete", "mutation").
    """
    producer = _get_producer()
    if producer is None:
        return

    topic = _get_topic()
    event = {
        "table": table_name,
        "source": source_id,
        "type": mutation_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        producer.produce(
            topic,
            key=f"{source_id}.{table_name}".encode(),
            value=json.dumps(event).encode(),
        )
        producer.poll(0)  # Trigger delivery callbacks without blocking
        log.debug("Change event emitted: %s.%s", source_id, table_name)
    except Exception:
        log.warning("Failed to emit change event for %s.%s", source_id, table_name, exc_info=True)


def flush() -> None:
    """Flush any buffered change events. Call on shutdown."""
    if _producer is not None:
        _producer.flush(timeout=5)
