# Copyright (c) 2026 Kenneth Stott
# Canary: 0f166822-96d8-4ead-871b-a59f24fc0552
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Collect table metadata from Trino and PG for LLM relationship discovery."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import asyncpg
import trino

log = logging.getLogger(__name__)

_SAFE_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass
class TableMeta:
    table_id: int
    source_id: str
    domain_id: str
    schema_name: str
    table_name: str
    columns: list[dict]  # [{name, type}]
    sample_values: list[dict]  # [{col: val, ...}]


@dataclass
class DiscoveryInput:
    tables: list[TableMeta]
    existing_relationships: list[dict]
    rejected_pairs: list[dict]


def _validate_ident(value: str) -> str:
    if not _SAFE_IDENT.match(value):
        raise ValueError(f"Unsafe identifier: {value!r}")
    return value


def _fetch_column_types(
    trino_conn: trino.dbapi.Connection,
    catalog: str,
    schema: str,
    table: str,
) -> list[dict]:
    cat = _validate_ident(catalog)
    _validate_ident(schema)
    _validate_ident(table)
    cur = trino_conn.cursor()
    cur.execute(
        f"SELECT column_name, data_type "
        f"FROM {cat}.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        f"ORDER BY ordinal_position"
    )
    return [{"name": row[0], "type": row[1].lower()} for row in cur.fetchall()]


def _fetch_samples(
    trino_conn: trino.dbapi.Connection,
    catalog: str,
    schema: str,
    table: str,
    columns: list[dict],
    sample_size: int,
) -> list[dict]:
    cat = _validate_ident(catalog)
    sch = _validate_ident(schema)
    tbl = _validate_ident(table)
    col_names = [_validate_ident(c["name"]) for c in columns[:10]]  # cap columns
    if not col_names:
        return []
    cols_sql = ", ".join(col_names)
    cur = trino_conn.cursor()
    try:
        cur.execute(f"SELECT {cols_sql} FROM {cat}.{sch}.{tbl} LIMIT {int(sample_size)}")
        rows = cur.fetchall()
    except Exception as e:
        log.warning("Failed to sample %s.%s.%s: %s", cat, sch, tbl, e)
        return []
    return [
        {col_names[i]: str(val) if val is not None else None for i, val in enumerate(row)}
        for row in rows
    ]


async def collect_metadata(
    trino_conn: trino.dbapi.Connection,
    pg_conn: asyncpg.Connection,
    scope: str,
    scope_id: str | int | None = None,
    sample_size: int = 20,
) -> DiscoveryInput:
    """Collect metadata for LLM discovery.

    scope: "table", "domain", "cross-domain"
    scope_id: table_id (int) for "table", domain_id (str) for "domain"
    """
    # Fetch all registered tables
    all_tables = await pg_conn.fetch(
        "SELECT id, source_id, domain_id, schema_name, table_name FROM registered_tables ORDER BY id"
    )
    all_tables = [dict(r) for r in all_tables]

    # Fetch sources for catalog name mapping
    sources = {
        r["id"]: dict(r)
        for r in await pg_conn.fetch("SELECT id FROM sources")
    }

    # Filter tables by scope
    if scope == "table":
        target_table = next((t for t in all_tables if t["id"] == scope_id), None)
        if target_table is None:
            raise ValueError(f"Table {scope_id} not found")
        # Include target table plus all others in same domain
        domain_id = target_table["domain_id"]
        tables = [t for t in all_tables if t["domain_id"] == domain_id]
    elif scope == "domain":
        tables = [t for t in all_tables if t["domain_id"] == scope_id]
        if not tables:
            raise ValueError(f"No tables found in domain {scope_id}")
    elif scope == "cross-domain":
        tables = all_tables
    else:
        raise ValueError(f"Invalid scope: {scope!r}")

    # Collect metadata per table
    table_metas: list[TableMeta] = []
    for t in tables:
        catalog = t["source_id"].replace("-", "_")
        columns = _fetch_column_types(trino_conn, catalog, t["schema_name"], t["table_name"])
        samples = _fetch_samples(
            trino_conn, catalog, t["schema_name"], t["table_name"], columns, sample_size
        )
        table_metas.append(TableMeta(
            table_id=t["id"],
            source_id=t["source_id"],
            domain_id=t["domain_id"],
            schema_name=t["schema_name"],
            table_name=t["table_name"],
            columns=columns,
            sample_values=samples,
        ))

    # Fetch existing relationships
    existing = await pg_conn.fetch(
        "SELECT source_table_id, target_table_id, source_column, target_column, cardinality "
        "FROM relationships"
    )
    existing_rels = [dict(r) for r in existing]

    # Fetch previously rejected candidates
    rejected = await pg_conn.fetch(
        "SELECT source_table_id, source_column, target_table_id, target_column "
        "FROM relationship_candidates WHERE status = 'rejected'"
    )
    rejected_pairs = [dict(r) for r in rejected]

    return DiscoveryInput(
        tables=table_metas,
        existing_relationships=existing_rels,
        rejected_pairs=rejected_pairs,
    )
