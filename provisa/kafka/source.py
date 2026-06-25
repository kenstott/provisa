# Copyright (c) 2026 Kenneth Stott
# Canary: 1b66a618-a590-4049-a7ec-a3a78a4ad918
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka topic registration as data sources (REQ-114).

Each topic + schema → registered table. Trino Kafka connector handles reads.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum

# Requirements: REQ-147, REQ-148, REQ-149, REQ-150, REQ-250

log = logging.getLogger(__name__)


class ValueFormat(str, Enum):
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


class SchemaSource(str, Enum):
    REGISTRY = "registry"
    MANUAL = "manual"
    SAMPLE = "sample"


@dataclass
class KafkaColumn:
    """Column definition for a Kafka topic table."""

    name: str
    data_type: str  # Trino type: varchar, integer, bigint, double, boolean, etc.
    is_complex: bool = False  # True for JSONB columns (objects/arrays)


@dataclass
class KafkaDiscriminator:  # REQ-149
    """Filter a shared topic to a single message type."""

    field: str  # column/field name containing the type discriminator
    value: str  # value that identifies this message type


@dataclass
class KafkaTopicConfig:  # REQ-147, REQ-148, REQ-149, REQ-150
    """Configuration for a Kafka topic registered as a table."""

    id: str
    topic: str
    source_id: str
    schema_source: SchemaSource = SchemaSource.REGISTRY
    value_format: ValueFormat = ValueFormat.JSON
    columns: list[KafkaColumn] = field(default_factory=list)
    table_name: str | None = None  # defaults to topic name sanitized
    default_window: str | None = "1h"  # auto-injected time bound  # REQ-148
    discriminator: KafkaDiscriminator | None = None  # filter by message type  # REQ-149

    def __post_init__(self):
        if self.table_name is None:
            self.table_name = self.topic.replace(".", "_").replace("-", "_")


@dataclass
class KafkaSourceConfig:  # REQ-147
    """Configuration for a Kafka cluster source."""

    id: str
    bootstrap_servers: str
    schema_registry_url: str | None = None
    auth: object = None  # object-ok: KafkaAuth union from core.auth_models; avoids import cycle
    topics: list[KafkaTopicConfig] = field(default_factory=list)


def generate_trino_kafka_properties(source: KafkaSourceConfig) -> str:  # REQ-147, REQ-250
    """Generate Trino Kafka connector properties file content.

    Returns the content for a kafka.properties file to be placed
    in Trino's catalog directory.
    """
    from provisa.core.auth_models import (
        KafkaAuthSaslPlain,
        KafkaAuthSaslScram256,
        KafkaAuthSaslScram512,
    )

    lines = [
        "connector.name=kafka",
        f"kafka.nodes={source.bootstrap_servers}",
        "kafka.hide-internal-columns=false",
    ]

    # REQ-250: Confluent is optional. Use the schema registry only when one is
    # configured; otherwise use FILE table descriptions generated from the topic's
    # manual columns (or a sampled layout) — no Confluent dependency.
    if source.schema_registry_url:
        lines.append("kafka.table-description-supplier=CONFLUENT")
        lines.append(f"kafka.confluent-schema-registry-url={source.schema_registry_url}")
    else:
        lines.append("kafka.table-description-supplier=FILE")
        lines.append("kafka.table-description-dir=/etc/trino/kafka")
        # Table names (sanitized) match the table-description tableName; the
        # description maps each back to its raw topicName.
        table_names = [t.table_name or t.topic for t in source.topics]
        if table_names:
            lines.append("kafka.table-names=" + ",".join(table_names))

    if isinstance(source.auth, (KafkaAuthSaslPlain, KafkaAuthSaslScram256, KafkaAuthSaslScram512)):
        lines.append("kafka.config.resources=/etc/trino/kafka-client.properties")

    return "\n".join(lines)


def generate_kafka_table_definitions(
    source: "KafkaSourceConfig",
) -> list[dict]:  # REQ-147, REQ-150, REQ-250
    """Generate Trino Kafka FILE table-description dicts from each topic's columns.

    Used when no Confluent schema registry is configured: the record layout comes
    from the topic's manually-entered (or sampled) columns. One dict per topic in
    Trino's kafka table-description JSON format.
    """
    definitions: list[dict] = []
    for topic in source.topics:
        fields = [
            {"name": c.name, "type": "VARCHAR" if c.is_complex else c.data_type, "mapping": c.name}
            for c in topic.columns
        ]
        definitions.append(
            {
                "tableName": topic.table_name or topic.topic,
                "topicName": topic.topic,
                "schemaName": "default",
                "message": {"dataFormat": topic.value_format.value, "fields": fields},
            }
        )
    return definitions


def generate_kafka_client_properties(source: KafkaSourceConfig) -> str | None:
    """Generate JAAS config for Kafka SASL auth. Returns None if no auth."""
    from provisa.core.auth_models import (
        KafkaAuthSaslPlain,
        KafkaAuthSaslScram256,
        KafkaAuthSaslScram512,
    )
    from provisa.core.secrets import resolve_secrets

    auth = source.auth
    if auth is None:
        return None

    if isinstance(auth, KafkaAuthSaslPlain):
        mechanism = "PLAIN"
        module = "org.apache.kafka.common.security.plain.PlainLoginModule"
    elif isinstance(auth, KafkaAuthSaslScram256):
        mechanism = "SCRAM-SHA-256"
        module = "org.apache.kafka.common.security.scram.ScramLoginModule"
    elif isinstance(auth, KafkaAuthSaslScram512):
        mechanism = "SCRAM-SHA-512"
        module = "org.apache.kafka.common.security.scram.ScramLoginModule"
    else:
        return None

    username = resolve_secrets(auth.username)
    password = resolve_secrets(auth.password)

    return "\n".join(
        [
            "security.protocol=SASL_PLAINTEXT",
            f"sasl.mechanism={mechanism}",
            f'sasl.jaas.config={module} required username="{username}" password="{password}";',
        ]
    )


def generate_topic_table_names(source: KafkaSourceConfig) -> list[str]:
    """Generate the kafka.table-names property value from topic configs."""
    return [t.topic for t in source.topics]


def map_avro_to_trino(avro_type: str | dict) -> tuple[str, bool]:
    """Map an Avro type to a Trino column type.

    Returns (trino_type, is_complex).
    """
    if isinstance(avro_type, str):
        mapping = {
            "null": ("varchar", False),
            "boolean": ("boolean", False),
            "int": ("integer", False),
            "long": ("bigint", False),
            "float": ("real", False),
            "double": ("double", False),
            "bytes": ("varbinary", False),
            "string": ("varchar", False),
        }
        return mapping.get(avro_type, ("varchar", False))

    if isinstance(avro_type, dict):
        avro_kind = avro_type.get("type", "")
        if avro_kind in ("record", "array", "map"):
            return ("varchar", True)  # JSONB in practice
        if avro_kind == "enum":
            return ("varchar", False)
        if avro_kind == "fixed":
            return ("varbinary", False)

    # Union types: pick the first non-null type
    if isinstance(avro_type, list):
        for t in avro_type:
            if t != "null":
                return map_avro_to_trino(t)

    return ("varchar", True)


def map_json_schema_to_trino(json_type: str) -> tuple[str, bool]:
    """Map a JSON Schema type to a Trino column type."""
    mapping = {
        "string": ("varchar", False),
        "integer": ("bigint", False),
        "number": ("double", False),
        "boolean": ("boolean", False),
        "object": ("varchar", True),  # JSONB
        "array": ("varchar", True),  # JSONB
    }
    return mapping.get(json_type, ("varchar", True))


def _json_type_of(value) -> str | None:
    """JSON Schema type name for a decoded value (None = skip, no type signal)."""
    if value is None:
        return None
    if isinstance(value, bool):  # before int — bool is a subclass of int
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"


def infer_columns_from_records(records: list[dict]) -> list[KafkaColumn]:  # REQ-150
    """Propose KafkaColumn types from sampled JSON records (SchemaSource.SAMPLE).

    Field order follows first appearance. When a field shows more than one type
    across the sample, the widest wins: object/array (complex) > number > integer >
    boolean > string. null values contribute no type signal.
    """
    order: list[str] = []
    seen: dict[str, set[str]] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        for key, value in rec.items():
            jt = _json_type_of(value)
            if jt is None:
                continue
            if key not in seen:
                seen[key] = set()
                order.append(key)
            seen[key].add(jt)

    columns: list[KafkaColumn] = []
    for key in order:
        types = seen[key]
        if "object" in types or "array" in types:
            jt = "object"
        elif "number" in types:
            jt = "number"
        elif "integer" in types:
            jt = "integer"
        elif "boolean" in types:
            jt = "boolean"
        else:
            jt = "string"
        trino_type, is_complex = map_json_schema_to_trino(jt)
        columns.append(KafkaColumn(name=key, data_type=trino_type.upper(), is_complex=is_complex))
    return columns


async def sample_topic_records(  # REQ-150
    bootstrap_servers: str, topic: str, max_records: int = 50, timeout_ms: int = 4000
) -> list[dict]:
    """Consume up to ``max_records`` JSON messages from ``topic`` (SchemaSource.SAMPLE).

    Reads from the earliest offset and JSON-decodes each value; non-JSON messages are
    skipped. Requires a reachable broker. Returns the decoded records.
    """
    import json

    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    records: list[dict] = []
    try:
        while len(records) < max_records:
            batch = await consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
            if not batch:
                break
            for _tp, messages in batch.items():
                for msg in messages:
                    if msg.value is None:
                        continue
                    try:
                        decoded = json.loads(msg.value)
                    except Exception:
                        continue
                    if isinstance(decoded, dict):
                        records.append(decoded)
    finally:
        await consumer.stop()
    return records[:max_records]
