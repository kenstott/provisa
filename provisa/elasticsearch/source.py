# Copyright (c) 2025 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-5678-901234ef0123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Elasticsearch source mapping — index discovery + nested flattening (REQ-250).

Generates Trino Elasticsearch connector catalog properties. Supports
schema inference from ES index mappings with nested field flattening.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


ES_TYPE_TO_TRINO = {
    "text": "VARCHAR",
    "keyword": "VARCHAR",
    "long": "BIGINT",
    "integer": "INTEGER",
    "short": "SMALLINT",
    "byte": "TINYINT",
    "double": "DOUBLE",
    "float": "REAL",
    "half_float": "REAL",
    "scaled_float": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "TIMESTAMP",
    "ip": "VARCHAR",
    "binary": "VARBINARY",
    "nested": "VARCHAR",
    "object": "VARCHAR",
    "geo_point": "VARCHAR",
}


@dataclass
class ESColumn:
    """Column definition with optional nested path mapping."""

    name: str
    data_type: str  # Trino type
    path: str | None = None  # ES field path (e.g., "request.method")


@dataclass
class ESTableConfig:
    """Table mapped from an Elasticsearch index or index pattern."""

    name: str
    index: str  # Index name or pattern (e.g., "nginx-access-*")
    discover: bool = False
    columns: list[ESColumn] = field(default_factory=list)


@dataclass
class ESSourceConfig:
    """Elasticsearch source connection + table mappings."""

    id: str
    host: str = "localhost"
    port: int = 9200
    tls: bool = False
    auth_user: str | None = None
    auth_password: str | None = None
    tables: list[ESTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: ESSourceConfig) -> dict[str, str]:
    """Generate Trino Elasticsearch connector catalog properties."""
    scheme = "https" if config.tls else "http"
    props = {
        "connector.name": "elasticsearch",
        "elasticsearch.host": config.host,
        "elasticsearch.port": str(config.port),
        "elasticsearch.default-schema-name": "default",
    }
    if config.tls:
        props["elasticsearch.tls.enabled"] = "true"
    if config.auth_user:
        props["elasticsearch.auth.user"] = config.auth_user
    if config.auth_password:
        props["elasticsearch.auth.password"] = config.auth_password
    return props


def generate_table_definitions(config: ESSourceConfig) -> list[dict]:
    """Generate table definition entries for each configured index."""
    definitions = []
    for table in config.tables:
        entry = {
            "tableName": table.name,
            "index": table.index,
            "discover": table.discover,
            "columns": [],
        }
        for col in table.columns:
            col_def = {
                "name": col.name,
                "type": col.data_type,
                "sourcePath": col.path or col.name,
            }
            entry["columns"].append(col_def)
        definitions.append(entry)
    return definitions


def discover_schema(index_mapping: dict) -> list[dict]:
    """Infer columns from an Elasticsearch index mapping.

    Args:
        index_mapping: The ``mappings.properties`` dict from an ES
            GET /<index>/_mapping response.

    Returns:
        List of column definition dicts with flattened nested paths.
    """
    columns: list[dict] = []
    _flatten_mapping(index_mapping, "", columns)
    return columns


def _flatten_mapping(
    properties: dict, prefix: str, columns: list[dict]
) -> None:
    """Recursively flatten nested ES mapping properties."""
    for field_name, field_def in sorted(properties.items()):
        full_path = f"{prefix}{field_name}" if not prefix else f"{prefix}{field_name}"

        # Nested objects have their own 'properties'
        nested_props = field_def.get("properties")
        if nested_props:
            _flatten_mapping(nested_props, f"{full_path}.", columns)
            continue

        es_type = field_def.get("type", "object")
        trino_type = ES_TYPE_TO_TRINO.get(es_type, "VARCHAR")
        columns.append({
            "name": full_path.replace(".", "_"),
            "type": trino_type,
            "sourcePath": full_path,
        })
