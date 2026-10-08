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

Change events are on when the deployment names a broker for them
(``PROVISA_CHANGE_EVENT_BOOTSTRAP``, else ``KAFKA_BOOTSTRAP_SERVERS``): the lifespan then starts
the producer (:func:`start`) and stops it (:func:`stop`). With no broker named there is no
producer and nothing is emitted; that is configuration, and nothing is logged about it.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from provisa.kafka.producer import Producer, shared

# Requirements: REQ-172, REQ-173, REQ-174, REQ-175

log = logging.getLogger(__name__)

_producer: Producer | None = None


def _get_topic() -> str:  # REQ-175
    return os.environ.get("PROVISA_CHANGE_EVENT_TOPIC", "provisa.change-events")


def bootstrap_servers() -> str | None:
    """The broker change events go to, or None when the deployment names none."""
    return (
        os.environ.get("PROVISA_CHANGE_EVENT_BOOTSTRAP")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or None
    )


def start() -> None:
    """Start the change-event producer when a broker is named for it. Called once per process,
    by the lifespan."""
    global _producer
    bootstrap = bootstrap_servers()
    if bootstrap is None or _producer is not None:
        return
    _producer = shared(bootstrap)
    log.info("change events are sent to %s on %s", _get_topic(), bootstrap)


def stop() -> None:
    """Emit no more change events. Called by the lifespan, which then stops the process's
    producers (``provisa.kafka.producer.stop_all``): those already emitted are sent first."""
    global _producer
    _producer = None


def emit_change_event(  # REQ-172, REQ-173, REQ-174
    table_name: str,
    source_id: str,
    mutation_type: str = "mutation",
) -> None:
    """Emit a dataset change event to Kafka.

    Returns at once: the event is handed to the producer's own thread, so the write that caused
    it neither waits on the broker nor fails with it (an event that cannot be delivered is
    dropped, and the producer logs why).

    Args:
        table_name: The table that was modified.
        source_id: The source containing the table.
        mutation_type: Type of change (e.g., "insert", "update", "delete", "mutation").
    """
    if _producer is None:
        return
    event = {
        "table": table_name,
        "source": source_id,
        "type": mutation_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _producer.send(
        _get_topic(),
        json.dumps(event).encode(),
        key=f"{source_id}.{table_name}".encode(),
    )
