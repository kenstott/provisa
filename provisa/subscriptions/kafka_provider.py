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

from provisa.kafka.avro_registry import (
    RegistrySettings,
    SchemaRegistry,
    is_registry_framed,
    refuse_avro_without_registry,
)
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

    A message is a JSON object, or Avro in a Confluent-compatible schema registry's wire format;
    each message says which, as for the Debezium provider (REQ-1951). An Avro message on a source
    that names no registry is refused by name. A message that cannot be read -- not JSON, or a datum
    that does not match its schema -- is reported at ERROR with where it is
    (topic, partition, offset) and passed over: one bad message never ends the stream. Its offset
    is committed with the next landed one. A value that is not an object is the row
    ``{"value": ...}`` (a subscription's scalar payload). ``field_mapping`` renames a message's fields to the
    table's columns (``LiveKafkaParams.field_mapping``).
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str = "provisa-subscriptions",
        *,
        registry: RegistrySettings | None = None,
        field_mapping: dict[str, str] | None = None,
        landed: str | None = None,
        **consumer_kwargs: str | int | bool | None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        # REQ-1733: the group the table's landing records where it stopped under
        # (kafka.landed_offsets.landed_group); None for a reader of a topic nothing lands.
        self._landed = landed
        self._group_id = group_id
        self._registry_settings = registry
        self._registry: SchemaRegistry | None = None
        self._field_mapping = dict(field_mapping or {})
        self._consumer_kwargs = consumer_kwargs
        self._consumer: _KafkaConsumer | None = None

    async def _decode(self, raw: bytes | str, topic: str) -> Any:
        """The message's value: Avro through the registry, else JSON."""
        if isinstance(raw, bytes) and is_registry_framed(raw):
            if self._registry is None:
                raise refuse_avro_without_registry(topic)
            return await self._registry.decode(raw)
        return json.loads(raw)

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]

        topic = table
        # REQ-1951: a source that names a registry is asked now, so a registry that is down or
        # refuses the source is refused by name before any message is read.
        if self._registry_settings is not None:
            self._registry = SchemaRegistry(self._registry_settings)
            try:
                await self._registry.reach()
            except BaseException:
                await self._close_registry()
                raise
        if self._landed is not None:
            # REQ-1733: a partition this group has committed nothing on starts where the table's
            # landing stopped: nothing published since is missed, nothing landed is read again.
            from provisa.kafka.landed_offsets import start_where_landed

            await start_where_landed(self._bootstrap_servers, self._group_id, topic, self._landed)
        consumer: _KafkaConsumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            # A partition the group has committed on resumes there, and one a landing recorded
            # was committed above. One with neither -- no landing has recorded where it stopped
            # -- is read from the earliest offset: from the newest, a message published before
            # this start would never be landed.
            auto_offset_reset="earliest",
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
                where = f"{msg.topic}[{msg.partition}]@{msg.offset}"
                raw = msg.value
                try:
                    value = await self._decode(raw, topic) if isinstance(raw, (bytes, str)) else raw
                except (ValueError, TypeError, EOFError) as exc:
                    # Not JSON or not text at all (ValueError covers a UnicodeDecodeError), or an
                    # Avro datum that does not match the schema its id names.
                    log.error("KafkaProvider: unreadable message %s passed over: %s", where, exc)
                    continue
                if isinstance(value, dict):
                    op = str(value.pop("op", "insert")).lower()
                    row = value.pop("row", value)
                else:
                    op = "insert"
                    row = {"value": value}
                if not isinstance(row, dict):
                    row = {"value": row}
                if self._field_mapping:
                    row = {self._field_mapping.get(k, k): v for k, v in row.items()}
                yield ChangeEvent(
                    operation=op,
                    table=table,
                    row=row,
                    timestamp=datetime.fromtimestamp(msg.timestamp / 1000, tz=timezone.utc)
                    if msg.timestamp
                    else datetime.now(timezone.utc),
                    ack_token=(msg.topic, msg.partition, msg.offset),  # REQ-1734
                )
        finally:
            await consumer.stop()
            self._consumer = None
            await self._close_registry()

    async def _close_registry(self) -> None:
        if self._registry is not None:
            await self._registry.close()
            self._registry = None

    async def ack(self, events: list[ChangeEvent]) -> None:  # REQ-1734
        """Commit what *events* landed (:func:`commit_landed`)."""
        if self._consumer is None:
            return
        await commit_landed(self._consumer, events)

    async def close(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None


async def commit_landed(consumer: Any, events: list[ChangeEvent]) -> None:  # REQ-1734
    """Commit each (topic, partition)'s HIGHEST offset among *events* — never "wherever the
    consumer currently is", which could be past other events the caller hasn't landed yet
    (still sitting in a debounce buffer). Kafka commit convention: commit offset+1 (the next
    offset to read on resume). Events with no ack_token (e.g. a test double) are skipped."""
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
    await consumer.commit(offsets=offsets)
