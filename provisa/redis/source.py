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


DEFAULT_SCHEMA = "default"
KEY_DELIMITER = ":"

# Where the engine container reads the table-description files this module's definitions are
# written to. The writer puts them under ``trino_etc_dir()/redis`` on the host and the compose
# mount maps that directory here — the same arrangement Kafka uses for ``/etc/trino/kafka``
# (trino_catalog_files.py:338). A path outside the mounted tree resolves to an empty directory in
# the container, and every table then reports no columns.
TABLE_DESCRIPTION_DIR = "/etc/trino/redis"


def required_key_pattern(table_name: str, schema: str = DEFAULT_SCHEMA) -> str:  # REQ-251
    """The only key pattern the Redis connector can scan for ``table_name``.

    With ``redis.key-prefix-schema-table=true`` the connector derives its scan from the table
    identity and there is no per-table pattern setting. The alternative is leaving that property
    false, which makes every table scan the whole keyspace (``KEYS *``) and read unrelated keys,
    including Provisa's own cache entries, as rows.

    The schema segment is present only for a NON-default schema. ``RedisRecordCursor``
    (trino-redis 481) builds the ``SCAN ... MATCH`` prefix as ``schemaName.equals("default") ? "" :
    schemaName + delimiter``, then appends ``tableName + delimiter + "*"`` — verified against the
    live connector, whose MONITOR output for a ``default``-schema table is
    ``SCAN 0 MATCH pipeline_widgets:* COUNT 100``. Emitting ``default:<table>:*`` here made every
    query return zero rows: the keys existed, the description file was applied (SHOW COLUMNS was
    correct), and the connector simply scanned a prefix nothing was written under.
    """
    prefix = "" if schema == DEFAULT_SCHEMA else f"{schema}{KEY_DELIMITER}"
    return f"{prefix}{table_name}{KEY_DELIMITER}*"


def generate_catalog_properties(config: RedisSourceConfig) -> dict[str, str]:  # REQ-250
    """Generate the engine Redis connector catalog properties."""
    for table in config.tables:
        expected = required_key_pattern(table.name)
        if table.key_pattern != expected:
            raise ValueError(
                f"redis table {table.name!r}: key_pattern {table.key_pattern!r} cannot be mapped — "
                f"the connector scans {expected!r} for this table. Either name the keys that way "
                f"or name the table after the key prefix."
            )
    props = {
        "connector.name": "redis",
        "redis.nodes": f"{config.host}:{config.port}",
        "redis.table-names": ",".join(f"{DEFAULT_SCHEMA}.{t.name}" for t in config.tables),
        "redis.key-delimiter": KEY_DELIMITER,
        # Scope each table's scan to its own key prefix (see required_key_pattern).
        "redis.key-prefix-schema-table": "true",
        "redis.table-description-dir": TABLE_DESCRIPTION_DIR,
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
        # No "mapping": the key is decoded with dataFormat ``raw``, whose mapping grammar is
        # ``<start>[:<end>]`` over the raw bytes. Any other string — "key" included — fails
        # decoder construction with ``invalid mapping format 'key' for column '<name>'`` the
        # moment the key column is selected. Omitting it decodes the whole key.
        key_col = {
            "name": table.key_column,
            "type": "VARCHAR",
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
            "schemaName": DEFAULT_SCHEMA,
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
