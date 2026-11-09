# Copyright (c) 2026 Kenneth Stott
# Canary: c4d80f37-61ab-49e2-8f04-1e6a9b27d3f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The warehouse a dbt model's rows are read from, identified from the manifest (REQ-2228).

A manifest names the adapter and each model's database, and holds no credential; dbt's profiles
file is not read. A registered source that matches the adapter and the database is the warehouse.
Otherwise the source's setup form offers to create one, filled in with what the manifest knows.
"""

# Requirements: REQ-2228

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import PurePath
from typing import Protocol

#: The source type a dbt adapter's warehouse is registered as.
ADAPTER_SOURCE_TYPE = {
    "athena": "athena",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "fabric": "fabric",
    "mysql": "mysql",
    "oracle": "oracle",
    "postgres": "postgresql",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "sqlserver": "sqlserver",
    "trino": "trino",
}

#: The types whose database is the file they are read from, named by dbt for the file's stem.
_FILE_DATABASES = frozenset({"duckdb"})


class SourceRow(Protocol):
    id: str
    type: str
    database: str
    path: str | None


@dataclass(frozen=True)
class WarehouseOffer:
    """One database a manifest's models are built in, and what reads it."""

    database: str
    source_type: str | None
    #: The registered source that reads it; None when the form has one to offer.
    source_id: str | None


def _database_of(source: SourceRow) -> str:
    if source.type in _FILE_DATABASES and source.path:
        return PurePath(source.path).stem
    return source.database


def warehouse_for(adapter: str, database: str, sources: Iterable[SourceRow]) -> str | None:
    """The id of the registered source for ``adapter``'s ``database``, in id order when several
    match; None when none does."""
    wanted = ADAPTER_SOURCE_TYPE.get(adapter)
    if wanted is None:
        return None
    matches = sorted(
        s.id for s in sources if s.type == wanted and _database_of(s).lower() == database.lower()
    )
    return matches[0] if matches else None


def warehouse_offers(
    adapter: str, databases: Iterable[str], sources: Iterable[SourceRow]
) -> list[WarehouseOffer]:
    """What the setup form shows of each database: its warehouse, or what creating one starts from."""
    held = list(sources)
    return [
        WarehouseOffer(
            database, ADAPTER_SOURCE_TYPE.get(adapter), warehouse_for(adapter, database, held)
        )
        for database in databases
    ]
