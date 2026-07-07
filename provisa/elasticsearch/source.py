# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-5678-901234ef0123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Elasticsearch source mapping — index discovery + nested flattening (REQ-250).

Generates the engine Elasticsearch connector catalog properties. Supports
schema inference from ES index mappings with nested field flattening.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

# Requirements: REQ-017, REQ-250, REQ-251, REQ-252

ES_TYPE_TO_IR = {
    "text": "VARCHAR",
    "keyword": "VARCHAR",
    "long": "BIGINT",
    "integer": "INTEGER",
    "short": "SMALLINT",
    "byte": "smallint",
    "double": "DOUBLE",
    "float": "REAL",
    "half_float": "REAL",
    "scaled_float": "DOUBLE",
    "boolean": "BOOLEAN",
    "date": "TIMESTAMP",
    "ip": "VARCHAR",
    "binary": "bytea",
    "nested": "VARCHAR",
    "object": "VARCHAR",
    "geo_point": "VARCHAR",
}


@dataclass
class ESColumn:  # REQ-251
    """Column definition with optional nested path mapping."""

    name: str
    data_type: str  # the engine type
    path: str | None = None  # ES field path (e.g., "request.method")


@dataclass
class ESTableConfig:  # REQ-251
    """Table mapped from an Elasticsearch index or index pattern."""

    name: str
    index: str  # Index name or pattern (e.g., "nginx-access-*")
    discover: bool = False
    columns: list[ESColumn] = field(default_factory=list)


@dataclass
class ESSourceConfig:  # REQ-250, REQ-251
    """Elasticsearch source connection + table mappings."""

    id: str
    host: str = "localhost"
    port: int = 9200
    tls: bool = False
    auth_user: str | None = None
    auth_password: str | None = None
    tables: list[ESTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: ESSourceConfig) -> dict[str, str]:  # REQ-250
    """Generate the engine Elasticsearch connector catalog properties."""
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


def generate_table_definitions(config: ESSourceConfig) -> list[dict]:  # REQ-250, REQ-251
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


def discover_schema(index_mapping: dict) -> list[dict]:  # REQ-252
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


def extract_mapping_properties(mapping_response: dict, index: str) -> dict:  # REQ-252
    """Extract ``mappings.properties`` from a GET /<index>/_mapping response (REQ-252).

    ES returns ``{<index>: {"mappings": {"properties": {...}}}}``. When the request used an
    alias or the index name differs, fall back to the single key in the response.
    """
    entry = mapping_response.get(index)
    if entry is None and len(mapping_response) == 1:
        entry = next(iter(mapping_response.values()))
    if not entry:
        raise ValueError(f"no mapping returned for Elasticsearch index {index!r}")
    properties = (entry.get("mappings") or {}).get("properties")
    if properties is None:
        raise ValueError(f"Elasticsearch index {index!r} has no mappings.properties")
    return properties


def fetch_index_mapping(host: str, port: int, index: str, use_ssl: bool = False) -> dict:  # REQ-252
    """Fetch the live ``mappings.properties`` for an index via GET /<index>/_mapping (REQ-252).

    Raises on any transport/HTTP error — discovery must not silently produce empty columns.
    """
    import httpx

    scheme = "https" if use_ssl else "http"
    url = f"{scheme}://{host}:{port}/{index}/_mapping"
    resp = httpx.get(url, timeout=10.0)
    resp.raise_for_status()
    return extract_mapping_properties(resp.json(), index)


def _flatten_mapping(properties: dict, prefix: str, columns: list[dict]) -> None:
    """Recursively flatten nested ES mapping properties."""
    for field_name, field_def in sorted(properties.items()):
        full_path = f"{prefix}{field_name}" if not prefix else f"{prefix}{field_name}"

        # Nested objects have their own 'properties'
        nested_props = field_def.get("properties")
        if nested_props:
            _flatten_mapping(nested_props, f"{full_path}.", columns)
            continue

        if "type" not in field_def:
            raise ValueError(f"ES field {full_path!r} has neither 'type' nor 'properties'")
        es_type = field_def["type"]
        if es_type not in ES_TYPE_TO_IR:
            raise ValueError(f"unmapped ES type: {es_type}")
        column_type = ES_TYPE_TO_IR[es_type]
        columns.append(
            {
                "name": full_path.replace(".", "_"),
                "type": column_type,
                "sourcePath": full_path,
            }
        )
