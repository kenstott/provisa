# Copyright (c) 2026 Kenneth Stott
# Canary: 4d8f1a2e-9b3c-4e7f-a1d6-2c5e8b0f4a9d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Debezium CDC subscription provider (REQ-261).

Consumes Debezium change data capture events from Kafka topics and maps them
to ChangeEvent notifications. Supports JSON and Avro (via Schema Registry)
deserializers. Compatible with MySQL, SQL Server, Oracle, and PostgreSQL sources
running Debezium connectors.

Debezium topic naming convention: {prefix}.{database}.{table}
Debezium op codes: c=create, u=update, d=delete, r=read/snapshot
"""

# Requirements: REQ-261, REQ-285

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, AsyncIterator, Protocol, runtime_checkable

from provisa.kafka.avro_registry import (
    RegistrySettings,
    SchemaRegistry,
    is_registry_framed,
    refuse_avro_without_registry,
)
from provisa.subscriptions.base import ChangeEvent, NotificationProvider

# REQ-922: missing/unparseable ts_ms sorts oldest via a stable sentinel (mirrors
# the RSS provider's REQ-343), never now() — which would advance the watermark.
_UNPARSEABLE_TS = datetime.min.replace(tzinfo=timezone.utc)


@runtime_checkable
class _KafkaConsumer(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[object]: ...


log = logging.getLogger(__name__)

# Map Debezium op codes to internal operation names
_OP_MAP = {
    "c": "insert",
    "u": "update",
    "d": "delete",
    "r": "insert",  # snapshot read — treat as insert
}


class DebeziumNotificationProvider(NotificationProvider):  # REQ-261, REQ-285
    """Consumes Debezium CDC events from Kafka and emits ChangeEvents.

    A message is JSON, or Avro in a Confluent-compatible schema registry's wire format; each
    message says which. Avro needs the source's registry (REQ-1951).

    Args:
        bootstrap_servers: Kafka bootstrap servers string.
        topic_prefix: Debezium connector topic prefix (e.g. "dbserver1").
        database: Source database name, used to build topic name.
        consumer_group_id: Kafka consumer group ID.
        registry: How the source's schema registry is reached, for Avro topics (REQ-1951).
        source_type: Source DB type: "mysql", "sqlserver", "oracle", "postgresql".
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic_prefix: str,
        database: str,
        consumer_group_id: str = "provisa-debezium",
        registry: RegistrySettings | None = None,
        source_type: str = "postgresql",
        pg_schema: str = "public",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic_prefix = topic_prefix
        self._database = database
        self._consumer_group_id = consumer_group_id
        self._registry_settings = registry
        self._registry: SchemaRegistry | None = None
        self._source_type = source_type
        self._pg_schema = pg_schema
        self._consumer: _KafkaConsumer | None = None

    def _build_topic(self, table: str) -> str:
        """Build Debezium topic name.

        PostgreSQL: {prefix}.{schema}.{table}  (Debezium uses schema, not dbname)
        Other DBs:  {prefix}.{database}.{table}
        """
        if self._source_type == "postgresql":
            return f"{self._topic_prefix}.{self._pg_schema}.{table}"
        return f"{self._topic_prefix}.{self._database}.{table}"

    def _parse_json_message(self, raw: bytes) -> dict | None:
        """Parse a JSON-encoded Debezium envelope."""
        try:
            return json.loads(raw)
        except (ValueError, TypeError) as exc:  # not JSON, or not text at all
            log.warning("DebeziumProvider: invalid JSON message: %s", exc)
            return None

    async def _parse_avro_message(self, raw: bytes, topic: str) -> dict | None:
        """The envelope of a message in the schema registry's wire format (REQ-1951). It is
        never read as JSON: with no registry named, the topic is refused by name."""
        if self._registry is None:
            raise refuse_avro_without_registry(topic)
        try:
            return await self._registry.decode(raw)
        except (EOFError, ValueError) as exc:  # the datum does not match the schema its id names
            log.warning("DebeziumProvider: undecodable Avro message on %s: %s", topic, exc)
            return None

    def _extract_event(self, envelope: dict, table: str) -> ChangeEvent | None:
        """Extract a ChangeEvent from a Debezium envelope dict.

        Handles both the Debezium JSON converter envelope (with "payload" key)
        and the bare envelope format.
        """
        payload = envelope.get("payload", envelope)
        if not isinstance(payload, dict):
            return None

        op_code = payload.get("op")
        if op_code not in _OP_MAP:
            # Heartbeat, schema change, or unknown message — skip silently
            return None

        operation = _OP_MAP[op_code]

        # "after" contains the new row state; "before" is the old state
        # For deletes, "after" is null — use "before" as the row
        if operation == "delete":
            row = payload.get("before") or {}
        else:
            row = payload.get("after") or {}

        if not isinstance(row, dict):
            row = {}

        # Watermark: Debezium stores event time in ts_ms (epoch milliseconds).
        # REQ-922: ts_ms is optional (snapshot/tombstone envelopes may omit it and
        # values can be malformed). A missing/unparseable ts_ms sorts oldest via a
        # stable sentinel rather than now(), which would advance the watermark and
        # drop later real events.
        ts_ms = payload.get("ts_ms")
        if ts_ms is None:
            timestamp = _UNPARSEABLE_TS
        else:
            try:
                timestamp = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            except (ValueError, OSError):
                timestamp = _UNPARSEABLE_TS

        return ChangeEvent(
            operation=operation,
            table=table,
            row=row,
            timestamp=timestamp,
        )

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        """Yield ChangeEvents for *table* from the Debezium CDC stream.

        filter_expr is accepted for interface compatibility but not applied
        server-side — Debezium streams all changes. Callers may filter
        post-yield if needed.
        """
        from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]

        topic = self._build_topic(table)

        # REQ-1951: a source that names a registry is asked now, so a registry that is down,
        # hangs or refuses the source's credentials is refused by name before any message is
        # read. A message in the registry's wire format is decoded with the schema its id names.
        if self._registry_settings is not None:
            self._registry = SchemaRegistry(self._registry_settings)
            try:
                await self._registry.reach()
            except BaseException:
                await self._close_registry()
                raise

        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._consumer_group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        log.info(
            "DebeziumProvider: consuming CDC topic %s (source_type=%s)",
            topic,
            self._source_type,
        )

        try:
            async for msg in self._consumer:
                raw = msg.value
                if raw is None:
                    # Tombstone record (Kafka compaction marker) — treat as delete
                    yield ChangeEvent(
                        operation="delete",
                        table=table,
                        row={},
                        timestamp=datetime.now(timezone.utc),
                    )
                    continue

                # A JSON document never begins with a zero byte; the registry's wire format
                # always does. The message says which it is.
                if is_registry_framed(raw):
                    envelope = await self._parse_avro_message(raw, topic)
                else:
                    envelope = self._parse_json_message(raw)

                if envelope is None:
                    continue

                # Handle schema change events gracefully — log and skip
                if "ddlType" in envelope or envelope.get("type") == "schema_change":
                    log.info(
                        "DebeziumProvider: schema change event on %s — skipping",
                        topic,
                    )
                    continue

                event = self._extract_event(envelope, table)
                if event is not None:
                    yield event

        except Exception as exc:
            log.exception("DebeziumProvider: error consuming topic %s: %s", topic, exc)
            raise
        finally:
            await self._consumer.stop()
            self._consumer = None
            await self._close_registry()

    async def _close_registry(self) -> None:
        registry, self._registry = self._registry, None
        if registry is not None:
            await registry.close()

    async def close(self) -> None:
        """Stop the Kafka consumer."""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        await self._close_registry()
