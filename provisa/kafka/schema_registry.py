# Copyright (c) 2026 Kenneth Stott
# Canary: 979b6ab6-31e7-4012-967e-11e2a5c50521
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Confluent Schema Registry client (REQ-116).

Fetch Avro/Protobuf/JSON Schema from Schema Registry.
Map schema fields to Kafka column definitions.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import httpx

from provisa.kafka.source import (
    KafkaColumn,
    ValueFormat,
    map_avro_to_trino,
    map_json_schema_to_trino,
)

log = logging.getLogger(__name__)


@dataclass
class SchemaInfo:
    """Schema information from the registry."""

    subject: str
    version: int
    schema_id: int
    schema_type: str  # AVRO, PROTOBUF, JSON
    schema_str: str


class SchemaRegistryClient:
    """Client for Confluent Schema Registry REST API."""

    def __init__(self, registry_url: str):
        self._url = registry_url.rstrip("/")

    async def get_latest_schema(self, subject: str) -> SchemaInfo:
        """Fetch the latest schema for a subject."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self._url}/subjects/{subject}/versions/latest",
            )
            resp.raise_for_status()
            data = resp.json()
            return SchemaInfo(
                subject=subject,
                version=data["version"],
                schema_id=data["id"],
                schema_type=data.get("schemaType", "AVRO"),
                schema_str=data["schema"],
            )

    async def get_subjects(self) -> list[str]:
        """List all registered subjects."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{self._url}/subjects")
            resp.raise_for_status()
            return resp.json()

    async def check_compatibility(self, subject: str, schema_str: str) -> bool:
        """Check if a schema is compatible with the latest version."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self._url}/compatibility/subjects/{subject}/versions/latest",
                json={"schema": schema_str},
            )
            if resp.status_code == 200:
                return resp.json().get("is_compatible", False)
            return False


def columns_from_avro_schema(schema_str: str) -> list[KafkaColumn]:
    """Extract column definitions from an Avro schema.

    Avro record fields → KafkaColumn list.
    Primitives → native types, records/arrays/maps → JSONB.
    """
    schema = json.loads(schema_str)
    if schema.get("type") != "record":
        raise ValueError(f"Expected Avro record schema, got: {schema.get('type')}")

    columns = []
    for field in schema.get("fields", []):
        trino_type, is_complex = map_avro_to_trino(field["type"])
        columns.append(KafkaColumn(
            name=field["name"],
            data_type=trino_type,
            is_complex=is_complex,
        ))
    return columns


def columns_from_json_schema(schema_str: str) -> list[KafkaColumn]:
    """Extract column definitions from a JSON Schema.

    Top-level properties → KafkaColumn list.
    """
    schema = json.loads(schema_str)
    if schema.get("type") != "object":
        raise ValueError(f"Expected JSON Schema object, got: {schema.get('type')}")

    columns = []
    for name, prop in schema.get("properties", {}).items():
        json_type = prop.get("type", "string")
        trino_type, is_complex = map_json_schema_to_trino(json_type)
        columns.append(KafkaColumn(
            name=name,
            data_type=trino_type,
            is_complex=is_complex,
        ))
    return columns


async def discover_topic_columns(
    registry_url: str,
    topic: str,
    value_format: ValueFormat,
) -> list[KafkaColumn]:
    """Discover columns for a Kafka topic from Schema Registry.

    Args:
        registry_url: Schema Registry URL.
        topic: Kafka topic name.
        value_format: Expected value format (avro, json, protobuf).

    Returns:
        List of column definitions.
    """
    client = SchemaRegistryClient(registry_url)

    # Schema Registry subject naming convention: {topic}-value
    subject = f"{topic}-value"

    try:
        schema_info = await client.get_latest_schema(subject)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise ValueError(
                f"No schema found for subject {subject!r} in Schema Registry"
            )
        raise

    if schema_info.schema_type == "AVRO" or value_format == ValueFormat.AVRO:
        return columns_from_avro_schema(schema_info.schema_str)

    if schema_info.schema_type == "JSON" or value_format == ValueFormat.JSON:
        return columns_from_json_schema(schema_info.schema_str)

    if schema_info.schema_type == "PROTOBUF":
        log.warning(
            "Protobuf schema parsing not yet implemented for topic %s. "
            "Use manual column definition.",
            topic,
        )
        return []

    raise ValueError(f"Unsupported schema type: {schema_info.schema_type}")
