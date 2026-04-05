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
class KafkaDiscriminator:
    """Filter a shared topic to a single message type."""

    field: str  # column/field name containing the type discriminator
    value: str  # value that identifies this message type


@dataclass
class KafkaTopicConfig:
    """Configuration for a Kafka topic registered as a table."""

    id: str
    topic: str
    source_id: str
    schema_source: SchemaSource = SchemaSource.REGISTRY
    value_format: ValueFormat = ValueFormat.JSON
    columns: list[KafkaColumn] = field(default_factory=list)
    table_name: str | None = None  # defaults to topic name sanitized
    default_window: str | None = "1h"  # auto-injected time bound
    discriminator: KafkaDiscriminator | None = None  # filter by message type

    def __post_init__(self):
        if self.table_name is None:
            self.table_name = self.topic.replace(".", "_").replace("-", "_")


@dataclass
class KafkaSourceConfig:
    """Configuration for a Kafka cluster source."""

    id: str
    bootstrap_servers: str
    schema_registry_url: str | None = None
    auth: object = None  # KafkaAuth from core.auth_models (or None)
    topics: list[KafkaTopicConfig] = field(default_factory=list)


def generate_trino_kafka_properties(source: KafkaSourceConfig) -> str:
    """Generate Trino Kafka connector properties file content.

    Returns the content for a kafka.properties file to be placed
    in Trino's catalog directory.
    """
    from provisa.core.auth_models import (
        KafkaAuthSaslPlain, KafkaAuthSaslScram256, KafkaAuthSaslScram512,
    )

    lines = [
        "connector.name=kafka",
        f"kafka.nodes={source.bootstrap_servers}",
        "kafka.table-description-supplier=CONFLUENT",
        "kafka.hide-internal-columns=false",
    ]

    if source.schema_registry_url:
        lines.append(
            f"kafka.confluent-schema-registry-url={source.schema_registry_url}"
        )

    if isinstance(source.auth, (KafkaAuthSaslPlain, KafkaAuthSaslScram256, KafkaAuthSaslScram512)):
        lines.append("kafka.config.resources=/etc/trino/kafka-client.properties")

    return "\n".join(lines)


def generate_kafka_client_properties(source: KafkaSourceConfig) -> str | None:
    """Generate JAAS config for Kafka SASL auth. Returns None if no auth."""
    from provisa.core.auth_models import (
        KafkaAuthSaslPlain, KafkaAuthSaslScram256, KafkaAuthSaslScram512,
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

    return "\n".join([
        "security.protocol=SASL_PLAINTEXT",
        f"sasl.mechanism={mechanism}",
        f'sasl.jaas.config={module} required username="{username}" password="{password}";',
    ])


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
