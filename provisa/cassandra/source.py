# Copyright (c) 2025 Kenneth Stott
# Canary: a7b8c9d0-e1f2-3456-7890-123456a01235
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cassandra source mapping — keyspace discovery + partition key awareness (REQ-250).

Generates Trino Cassandra connector catalog properties. Supports schema
inference from keyspace metadata with partition/clustering key annotations.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


CQL_TYPE_TO_TRINO = {
    "ascii": "VARCHAR",
    "bigint": "BIGINT",
    "blob": "VARBINARY",
    "boolean": "BOOLEAN",
    "counter": "BIGINT",
    "date": "DATE",
    "decimal": "DECIMAL",
    "double": "DOUBLE",
    "float": "REAL",
    "inet": "VARCHAR",
    "int": "INTEGER",
    "smallint": "SMALLINT",
    "text": "VARCHAR",
    "time": "TIME",
    "timestamp": "TIMESTAMP",
    "timeuuid": "UUID",
    "tinyint": "TINYINT",
    "uuid": "UUID",
    "varchar": "VARCHAR",
    "varint": "BIGINT",
    "list": "VARCHAR",
    "map": "VARCHAR",
    "set": "VARCHAR",
}


@dataclass
class CassandraTableConfig:
    """Table mapped from a Cassandra keyspace table."""

    name: str
    keyspace: str
    table: str
    discover: bool = False


@dataclass
class CassandraSourceConfig:
    """Cassandra source connection + table mappings."""

    id: str
    contact_points: str = "localhost"
    port: int = 9042
    username: str | None = None
    password: str | None = None
    tables: list[CassandraTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: CassandraSourceConfig) -> dict[str, str]:
    """Generate Trino Cassandra connector catalog properties."""
    props = {
        "connector.name": "cassandra",
        "cassandra.contact-points": config.contact_points,
        "cassandra.native-protocol-port": str(config.port),
    }
    if config.username:
        props["cassandra.username"] = config.username
    if config.password:
        props["cassandra.password"] = config.password
    return props


def generate_table_definitions(config: CassandraSourceConfig) -> list[dict]:
    """Generate table definition entries for each configured Cassandra table."""
    definitions = []
    for table in config.tables:
        entry = {
            "tableName": table.name,
            "keyspace": table.keyspace,
            "table": table.table,
            "discover": table.discover,
        }
        definitions.append(entry)
    return definitions


def discover_schema(keyspace_metadata: dict) -> list[dict]:
    """Infer columns from Cassandra keyspace table metadata.

    Args:
        keyspace_metadata: Dict with keys:
            - ``columns``: list of {name, type} dicts
            - ``partition_keys``: list of column names
            - ``clustering_keys``: list of column names

    Returns:
        List of column definition dicts with key annotations.
    """
    columns_meta = keyspace_metadata.get("columns", [])
    partition_keys = set(keyspace_metadata.get("partition_keys", []))
    clustering_keys = set(keyspace_metadata.get("clustering_keys", []))

    columns = []
    for col in columns_meta:
        cql_type = col.get("type", "text")
        # Strip collection wrappers like list<text> -> list
        base_type = cql_type.split("<")[0].strip()
        trino_type = CQL_TYPE_TO_TRINO.get(base_type, "VARCHAR")

        col_def = {
            "name": col["name"],
            "type": trino_type,
            "cqlType": cql_type,
        }
        if col["name"] in partition_keys:
            col_def["partitionKey"] = True
        if col["name"] in clustering_keys:
            col_def["clusteringKey"] = True
        columns.append(col_def)
    return columns
