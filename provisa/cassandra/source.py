# Copyright (c) 2026 Kenneth Stott
# Canary: a7b8c9d0-e1f2-3456-7890-123456a01235
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cassandra source mapping — keyspace discovery + partition key awareness (REQ-250).

Generates the engine Cassandra connector catalog properties. Supports schema
inference from keyspace metadata with partition/clustering key annotations.
"""

# Requirements: REQ-250, REQ-251, REQ-252

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


CQL_TYPE_TO_IR = {
    "ascii": "VARCHAR",
    "bigint": "BIGINT",
    "blob": "bytea",
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
    "tinyint": "smallint",
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


def generate_catalog_properties(
    config: CassandraSourceConfig,
) -> dict[str, str]:  # REQ-250, REQ-251
    """Generate the engine Cassandra connector catalog properties."""
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


def generate_table_definitions(config: CassandraSourceConfig) -> list[dict]:  # REQ-494
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


def discover_schema(keyspace_metadata: dict) -> list[dict]:  # REQ-252
    """Infer columns from Cassandra keyspace table metadata.

    Args:
        keyspace_metadata: Dict with keys:
            - ``columns``: list of {name, type} dicts
            - ``partition_keys``: list of column names
            - ``clustering_keys``: list of column names

    Returns:
        List of column definition dicts with key annotations.
    """
    if "columns" not in keyspace_metadata:
        raise ValueError("keyspace metadata missing 'columns'")
    if "partition_keys" not in keyspace_metadata:
        raise ValueError("keyspace metadata missing 'partition_keys'")
    if "clustering_keys" not in keyspace_metadata:
        raise ValueError("keyspace metadata missing 'clustering_keys'")
    columns_meta = keyspace_metadata["columns"]
    partition_keys = set(keyspace_metadata["partition_keys"])
    clustering_keys = set(keyspace_metadata["clustering_keys"])

    columns = []
    for col in columns_meta:
        if "type" not in col:
            raise ValueError(f"column missing 'type': {col.get('name')}")
        cql_type = col["type"]
        # Strip collection wrappers like list<text> -> list
        base_type = cql_type.split("<")[0].strip()
        if base_type not in CQL_TYPE_TO_IR:
            raise ValueError(f"unmapped CQL type: {cql_type}")
        column_type = CQL_TYPE_TO_IR[base_type]

        col_def = {
            "name": col["name"],
            "type": column_type,
            "cqlType": cql_type,
        }
        if col["name"] in partition_keys:
            col_def["partitionKey"] = True
        if col["name"] in clustering_keys:
            col_def["clusteringKey"] = True
        columns.append(col_def)
    return columns
