# Copyright (c) 2026 Kenneth Stott
# Canary: e6f7a8b9-c0d1-2345-ef01-678901234567
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source catalog cache: pre-index table+column metadata for fast NL search (REQ-464).

The cache is populated in the background after source registration.
The search endpoint reads from cache; falls back to live the engine if cache is cold.
"""

# Requirements: REQ-464

from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS source_catalog_cache (
    source_id   TEXT        NOT NULL,
    schema_name TEXT        NOT NULL,
    table_name  TEXT        NOT NULL,
    column_names TEXT[]     NOT NULL DEFAULT '{}',
    comment     TEXT,
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_id, schema_name, table_name)
)
"""


async def ensure_table(pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(_DDL)


@dataclass
class CachedTable:
    schema_name: str
    table_name: str
    column_names: list[str]
    comment: str | None


async def read_cache(pool, source_id: str, schema_name: str) -> list[CachedTable] | None:  # REQ-464
    """Return cached tables for source+schema, or None if cache is cold."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT schema_name, table_name, column_names, comment "
            "FROM source_catalog_cache "
            "WHERE source_id = $1 AND schema_name = $2",
            source_id,
            schema_name,
        )
    if not rows:
        return None
    return [
        CachedTable(
            schema_name=r["schema_name"],
            table_name=r["table_name"],
            column_names=list(r["column_names"] or []),
            comment=r["comment"],
        )
        for r in rows
    ]


async def write_cache(
    pool,
    source_id: str,
    schema_name: str,
    tables: list[CachedTable],
) -> None:  # REQ-464
    if not tables:
        return
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO source_catalog_cache "
            "(source_id, schema_name, table_name, column_names, comment, indexed_at) "
            "VALUES ($1, $2, $3, $4, $5, NOW()) "
            "ON CONFLICT (source_id, schema_name, table_name) DO UPDATE SET "
            "column_names = EXCLUDED.column_names, "
            "comment = EXCLUDED.comment, "
            "indexed_at = EXCLUDED.indexed_at",
            [(source_id, schema_name, t.table_name, t.column_names, t.comment) for t in tables],
        )


async def invalidate_source(pool, source_id: str) -> None:  # REQ-464
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM source_catalog_cache WHERE source_id = $1", source_id)


async def index_source(
    source_id: str, pool, engine, source_pools, source_types, state
) -> None:  # REQ-464
    """Background task: walk all schemas+tables for a source and populate cache.

    Errors are logged and swallowed — cache miss is always safe (live fallback).
    """
    from provisa.api.admin.introspect import native_schemas, native_tables

    source_type = source_types.get(source_id, "")
    try:
        async with pool.acquire() as config_conn:
            schemas = await native_schemas(source_id, source_type, source_pools, config_conn)
    except Exception as exc:
        log.warning("catalog_cache: schema list failed for %r: %s", source_id, exc)
        schemas = None

    if schemas is None:
        # the engine fallback for schema list
        from provisa.api.admin.schema import source_to_catalog

        catalog = source_to_catalog(source_id)
        try:
            res = await engine.execute_engine(
                f'SELECT schema_name FROM "{catalog}".information_schema.schemata '
                f"ORDER BY schema_name"
            )
            schemas = [row[0] for row in res.rows]
        except Exception as exc:
            log.warning("catalog_cache: the engine schema list failed for %r: %s", source_id, exc)
            return

    for schema in schemas:
        try:
            async with pool.acquire() as config_conn:
                tables = await native_tables(
                    source_id, source_type, schema, source_pools, config_conn, state
                )
        except Exception:
            tables = None

        if tables is None:
            from provisa.api.admin.schema import source_to_catalog

            catalog = source_to_catalog(source_id)
            try:
                res = await engine.execute_engine(
                    f'SELECT table_name FROM "{catalog}".information_schema.tables '
                    f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' "
                    f"ORDER BY table_name"
                )
                table_names = [row[0] for row in res.rows]
                tables_with_cols = [
                    CachedTable(schema_name=schema, table_name=t, column_names=[], comment=None)
                    for t in table_names
                ]
            except Exception as exc:
                log.warning(
                    "catalog_cache: table list failed for %r/%r: %s", source_id, schema, exc
                )
                continue
        else:
            tables_with_cols = [
                CachedTable(
                    schema_name=schema,
                    table_name=t.name,
                    column_names=[],
                    comment=t.comment,
                )
                for t in tables
            ]

        # Enrich with column names from the engine
        from provisa.api.admin.schema import source_to_catalog

        catalog = source_to_catalog(source_id)
        for cached in tables_with_cols:
            try:
                res = await engine.execute_engine(
                    f'SELECT column_name FROM "{catalog}".information_schema.columns '
                    f"WHERE table_schema = '{schema}' AND table_name = '{cached.table_name}' "
                    f"ORDER BY ordinal_position"
                )
                cached.column_names = [row[0] for row in res.rows]
            except Exception:
                pass

        try:
            await write_cache(pool, source_id, schema, tables_with_cols)
            log.debug(
                "catalog_cache: indexed %d tables for %r/%r",
                len(tables_with_cols),
                source_id,
                schema,
            )
        except Exception as exc:
            log.warning("catalog_cache: write failed for %r/%r: %s", source_id, schema, exc)
