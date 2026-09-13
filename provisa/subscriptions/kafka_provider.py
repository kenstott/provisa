# Copyright (c) 2026 Kenneth Stott
# Canary: daf5e228-2ef2-4813-a0d0-154babce6989
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka subscription provider using aiokafka."""

# Requirements: REQ-258, REQ-261, REQ-1734

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from datetime import datetime, timezone
from typing import Any, Protocol

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)


class _KafkaMessage(Protocol):
    """Structural type for an aiokafka consumer record."""

    value: object
    timestamp: int
    topic: str
    partition: int
    offset: int


class _KafkaConsumer(Protocol):
    """Structural type for the subset of AIOKafkaConsumer used here."""

    def __aiter__(self) -> AsyncIterator[_KafkaMessage]: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def commit(self, offsets: dict[Any, Any] | None = None) -> None: ...


class KafkaNotificationProvider(NotificationProvider):  # REQ-258, REQ-261
    """Consumes from a Kafka topic and maps messages to ChangeEvent.

    REQ-1734: auto-commit is OFF by default (unlike aiokafka's own default) — the offset advances
    only when the caller calls ``ack()`` on the specific events it has durably applied (landed),
    never on a timer gated merely on "was this message yielded to the app". Debounce/backpressure
    buffering (subscriptions.cdc_landing) can hold a message for a while before landing it; a
    time-based auto-commit would advance the offset past it regardless, and a crash in that window
    would lose the message on restart.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str = "provisa-subscriptions",
        **consumer_kwargs: str | int | bool | None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._consumer_kwargs = consumer_kwargs
        self._consumer: _KafkaConsumer | None = None

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]

        topic = table
        consumer: _KafkaConsumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset="latest",
            enable_auto_commit=False,  # REQ-1734: ack() commits, not a timer
            # reason: arbitrary pass-through Kafka config; each strict aiokafka param
            # cannot be matched against the heterogeneous kwargs union.
            **self._consumer_kwargs,  # type: ignore[reportArgumentType]
        )
        self._consumer = consumer
        await consumer.start()
        log.info("KafkaProvider: consuming topic %s (auto-commit off; ack() commits)", topic)

        try:
            async for msg in consumer:
                try:
                    value = (
                        json.loads(msg.value) if isinstance(msg.value, (bytes, str)) else msg.value
                    )
                except (json.JSONDecodeError, TypeError):
                    log.warning("KafkaProvider: invalid message on %s", topic)
                    continue

                if isinstance(value, dict):
                    op = value.pop("op", "insert").lower()
                    row = value.pop("row", value)
                else:
                    op = "insert"
                    row = {"value": value}

                yield ChangeEvent(
                    operation=op,
                    table=table,
                    row=row if isinstance(row, dict) else {"value": row},
                    timestamp=datetime.fromtimestamp(msg.timestamp / 1000, tz=timezone.utc)
                    if msg.timestamp
                    else datetime.now(timezone.utc),
                    ack_token=(msg.topic, msg.partition, msg.offset),  # REQ-1734
                )
        finally:
            await consumer.stop()
            self._consumer = None

    async def ack(self, events: list[ChangeEvent]) -> None:  # REQ-1734
        """Commit each (topic, partition)'s HIGHEST offset among *events* — never "wherever the
        consumer currently is", which could be past other events the caller hasn't landed yet
        (still sitting in a debounce buffer). Kafka commit convention: commit offset+1 (the next
        offset to read on resume). Events with no ack_token (e.g. a test double) are skipped."""
        if self._consumer is None:
            return
        from aiokafka import TopicPartition  # type: ignore[import-untyped]
        from aiokafka.structs import OffsetAndMetadata  # type: ignore[import-untyped]

        highest: dict[tuple[str, int], int] = {}
        for ev in events:
            token = ev.ack_token
            if not (isinstance(token, tuple) and len(token) == 3):
                continue
            _topic, partition, offset = token
            key = (_topic, partition)
            if offset > highest.get(key, -1):
                highest[key] = offset
        if not highest:
            return
        offsets = {
            TopicPartition(topic, partition): OffsetAndMetadata(offset + 1, "")
            for (topic, partition), offset in highest.items()
        }
        await self._consumer.commit(offsets=offsets)

    async def close(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
