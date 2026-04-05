# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-3456-789012cdef01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MongoDB source mapping — collection discovery + override (REQ-250).

Trino MongoDB connector auto-discovers collections. This module generates
catalog properties and supports schema inference from sample documents.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


BSON_TO_TRINO = {
    "string": "VARCHAR",
    "int": "INTEGER",
    "int32": "INTEGER",
    "long": "BIGINT",
    "int64": "BIGINT",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "date": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
    "objectId": "VARCHAR",
    "object": "VARCHAR",
    "array": "VARCHAR",
    "binData": "VARBINARY",
}


@dataclass
class MongoColumn:
    """Column override or supplement for a MongoDB collection."""

    name: str
    data_type: str  # Trino type
    alias: str | None = None  # Flatten nested path to alias
    path: str | None = None  # Dot-notation path in document


@dataclass
class MongoTableConfig:
    """Table mapped from a MongoDB collection."""

    name: str
    collection: str
    discover: bool = False
    columns: list[MongoColumn] = field(default_factory=list)


@dataclass
class MongoSourceConfig:
    """MongoDB source connection + table mappings."""

    id: str
    connection_url: str = "mongodb://localhost:27017/"
    database: str | None = None
    tables: list[MongoTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: MongoSourceConfig) -> dict[str, str]:
    """Generate Trino MongoDB connector catalog properties."""
    props = {
        "connector.name": "mongodb",
        "mongodb.connection-url": config.connection_url,
    }
    if config.database:
        props["mongodb.schema-collection"] = "_schema"
    return props


def generate_table_definitions(config: MongoSourceConfig) -> list[dict]:
    """Generate table definition entries for each configured collection."""
    definitions = []
    for table in config.tables:
        entry = {
            "tableName": table.name,
            "collection": table.collection,
            "discover": table.discover,
            "columns": [],
        }
        for col in table.columns:
            col_def = {
                "name": col.alias or col.name,
                "type": col.data_type,
                "sourcePath": col.path or col.name,
            }
            entry["columns"].append(col_def)
        definitions.append(entry)
    return definitions


def discover_schema(
    sample_docs: list[dict], collection_name: str
) -> list[dict]:
    """Infer column definitions from a list of sample MongoDB documents.

    Scans all documents to build a union of fields. Nested paths are
    flattened using dot notation.
    """
    field_types: dict[str, str] = {}
    for doc in sample_docs:
        _extract_fields(doc, "", field_types)

    columns = []
    for path, bson_type in sorted(field_types.items()):
        trino_type = BSON_TO_TRINO.get(bson_type, "VARCHAR")
        columns.append({
            "name": path.replace(".", "_"),
            "type": trino_type,
            "sourcePath": path,
        })
    return columns


def _extract_fields(
    doc: dict, prefix: str, field_types: dict[str, str]
) -> None:
    """Recursively extract field names and BSON-like types from a document."""
    for key, value in doc.items():
        if key == "_id":
            continue
        full_path = f"{prefix}{key}" if not prefix else f"{prefix}{key}"
        if isinstance(value, dict):
            _extract_fields(value, f"{full_path}.", field_types)
        elif isinstance(value, list):
            field_types.setdefault(full_path, "array")
        elif isinstance(value, bool):
            field_types.setdefault(full_path, "boolean")
        elif isinstance(value, int):
            field_types.setdefault(full_path, "int")
        elif isinstance(value, float):
            field_types.setdefault(full_path, "double")
        elif isinstance(value, str):
            field_types.setdefault(full_path, "string")
        else:
            field_types.setdefault(full_path, "string")
