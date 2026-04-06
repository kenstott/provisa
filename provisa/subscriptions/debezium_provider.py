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

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)

# Map Debezium op codes to internal operation names
_OP_MAP = {
    "c": "insert",
    "u": "update",
    "d": "delete",
    "r": "insert",  # snapshot read — treat as insert
}


class DebeziumNotificationProvider(NotificationProvider):
    """Consumes Debezium CDC events from Kafka and emits ChangeEvents.

    Supports JSON deserialization by default. When schema_registry_url is
    provided, uses confluent-kafka Avro deserializer instead.

    Args:
        bootstrap_servers: Kafka bootstrap servers string.
        topic_prefix: Debezium connector topic prefix (e.g. "dbserver1").
        database: Source database name, used to build topic name.
        consumer_group_id: Kafka consumer group ID.
        schema_registry_url: Optional Confluent Schema Registry URL for Avro.
        source_type: Source DB type: "mysql", "sqlserver", "oracle", "postgresql".
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic_prefix: str,
        database: str,
        consumer_group_id: str = "provisa-debezium",
        schema_registry_url: str | None = None,
        source_type: str = "postgresql",
        pg_schema: str = "public",
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic_prefix = topic_prefix
        self._database = database
        self._consumer_group_id = consumer_group_id
        self._schema_registry_url = schema_registry_url
        self._source_type = source_type
        self._pg_schema = pg_schema
        self._consumer: Any | None = None

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
        except (json.JSONDecodeError, TypeError) as exc:
            log.warning("DebeziumProvider: invalid JSON message: %s", exc)
            return None

    def _parse_avro_message(self, raw: bytes, deserializer: Any) -> dict | None:
        """Deserialize an Avro-encoded Debezium message using Schema Registry."""
        try:
            return deserializer(raw, None)
        except Exception as exc:
            log.warning("DebeziumProvider: Avro deserialization error: %s", exc)
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

        # Watermark: Debezium stores event time in ts_ms (epoch milliseconds)
        ts_ms = payload.get("ts_ms")
        if ts_ms is not None:
            try:
                timestamp = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            except (ValueError, OSError):
                timestamp = datetime.now(timezone.utc)
        else:
            timestamp = datetime.now(timezone.utc)

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

        # Set up Avro deserializer if schema registry is configured
        avro_deserializer = None
        if self._schema_registry_url:
            try:
                from confluent_kafka.schema_registry import SchemaRegistryClient  # type: ignore[import-untyped]
                from confluent_kafka.schema_registry.avro import AvroDeserializer  # type: ignore[import-untyped]

                registry_client = SchemaRegistryClient({"url": self._schema_registry_url})
                avro_deserializer = AvroDeserializer(registry_client)
                log.info(
                    "DebeziumProvider: using Avro deserializer (schema registry: %s)",
                    self._schema_registry_url,
                )
            except ImportError:
                log.warning(
                    "DebeziumProvider: confluent-kafka not installed; "
                    "falling back to JSON deserialization"
                )

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

                # Deserialize
                if avro_deserializer is not None:
                    envelope = self._parse_avro_message(raw, avro_deserializer)
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
            log.exception(
                "DebeziumProvider: error consuming topic %s: %s", topic, exc
            )
            raise
        finally:
            await self._consumer.stop()
            self._consumer = None

    async def close(self) -> None:
        """Stop the Kafka consumer."""
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
