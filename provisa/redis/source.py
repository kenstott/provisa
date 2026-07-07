# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-1234-567890abcdef
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Redis source mapping — key_pattern, key_column, value_type (REQ-250).

Generates the engine Redis connector catalog properties and JSON table
definition files from YAML config.
"""
# Requirements: REQ-250, REQ-251

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


class ValueType:
    HASH = "hash"
    STRING = "string"
    ZSET = "zset"
    LIST = "list"


@dataclass
class RedisColumn:  # REQ-251
    """Column mapped from a Redis hash field or value."""

    name: str
    data_type: str  # the engine type: VARCHAR, INTEGER, etc.
    field: str | None = None  # Redis hash field name (None for string values)


@dataclass
class RedisTableConfig:  # REQ-251
    """Table definition derived from a Redis key pattern."""

    name: str
    key_pattern: str
    key_column: str
    value_type: str = ValueType.HASH
    columns: list[RedisColumn] = field(default_factory=list)


@dataclass
class RedisSourceConfig:  # REQ-250, REQ-251
    """Redis source connection + table mappings."""

    id: str
    host: str = "localhost"
    port: int = 6379
    password: str | None = None
    tables: list[RedisTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: RedisSourceConfig) -> dict[str, str]:  # REQ-250
    """Generate the engine Redis connector catalog properties."""
    props = {
        "connector.name": "redis",
        "redis.nodes": f"{config.host}:{config.port}",
        "redis.table-names": ",".join(t.name for t in config.tables),
        "redis.key-delimiter": ":",
        "redis.table-description-dir": "/etc/engine/redis",
    }
    if config.password:
        props["redis.password"] = config.password
    return props


def generate_table_definitions(config: RedisSourceConfig) -> list[dict]:  # REQ-251
    """Generate JSON table definition files for the Redis connector.

    Each table config produces one JSON dict matching the engine's
    redis table-description format.
    """
    definitions = []
    for table in config.tables:
        key_col = {
            "name": table.key_column,
            "type": "VARCHAR",
            "mapping": "key",
        }
        value_columns = []
        for col in table.columns:
            vc = {
                "name": col.name,
                "type": col.data_type,
                "mapping": col.field or col.name,
            }
            value_columns.append(vc)

        table_def = {
            "tableName": table.name,
            "schemaName": "default",
            "key": {
                "dataFormat": "raw",
                "fields": [key_col],
            },
            "value": {
                "dataFormat": _data_format_for(table.value_type),
                "fields": value_columns,
            },
        }
        definitions.append(table_def)
    return definitions


def generate_table_json(config: RedisSourceConfig) -> dict[str, str]:  # REQ-497
    """Return mapping of filename -> JSON content for each table."""
    result = {}
    for table_def in generate_table_definitions(config):
        filename = f"{table_def['tableName']}.json"
        result[filename] = json.dumps(table_def, indent=2)
    return result


def _data_format_for(value_type: str) -> str:
    """Map Redis value type to the engine data format."""
    mapping = {
        ValueType.HASH: "hash",
        ValueType.STRING: "raw",
        ValueType.ZSET: "json",
        ValueType.LIST: "json",
    }
    if value_type not in mapping:
        raise ValueError(f"unknown Redis ValueType: {value_type}")
    return mapping[value_type]
