# Copyright (c) 2026 Kenneth Stott
# Canary: e1f2a3b4-c5d6-7890-1234-567890e01239
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Accumulo source mapping — column family/qualifier mapping (REQ-250).

Like Redis, Accumulo requires explicit column definitions. Each column
maps to an Accumulo column family and qualifier.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class AccumuloColumn:
    """Column mapped from an Accumulo column family/qualifier."""

    name: str
    data_type: str  # Trino type
    family: str  # Accumulo column family
    qualifier: str  # Accumulo column qualifier


@dataclass
class AccumuloTableConfig:
    """Table mapped from an Accumulo table."""

    name: str
    accumulo_table: str
    columns: list[AccumuloColumn] = field(default_factory=list)


@dataclass
class AccumuloSourceConfig:
    """Accumulo source connection + table mappings."""

    id: str
    instance: str = "accumulo"
    zookeepers: str = "localhost:2181"
    username: str | None = None
    password: str | None = None
    tables: list[AccumuloTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: AccumuloSourceConfig) -> dict[str, str]:
    """Generate Trino Accumulo connector catalog properties."""
    props = {
        "connector.name": "accumulo",
        "accumulo.instance": config.instance,
        "accumulo.zookeepers": config.zookeepers,
    }
    if config.username:
        props["accumulo.username"] = config.username
    if config.password:
        props["accumulo.password"] = config.password
    return props


def generate_table_definitions(config: AccumuloSourceConfig) -> list[dict]:
    """Generate table definition entries for each configured Accumulo table."""
    definitions = []
    for table in config.tables:
        columns = []
        for col in table.columns:
            columns.append({
                "name": col.name,
                "type": col.data_type,
                "family": col.family,
                "qualifier": col.qualifier,
            })
        entry = {
            "tableName": table.name,
            "accumuloTable": table.accumulo_table,
            "columns": columns,
        }
        definitions.append(entry)
    return definitions
