# Copyright (c) 2026 Kenneth Stott
# Canary: f3a1b2c4-d5e6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Migrate registered file-source tables (SQLite) into PostgreSQL for Trino federation.

Only the tables explicitly registered in Provisa are migrated — not the entire file.
PG schema name matches the table's registered schema_name so Trino paths are consistent.
"""

import logging
import os
import sqlite3
import time

import asyncpg

from provisa.file_source.source import _sqlite_type_to_sql

log = logging.getLogger(__name__)

_PG_TYPE_MAP = {
    "VARCHAR": "TEXT",
    "BIGINT": "BIGINT",
    "INTEGER": "INTEGER",
    "SMALLINT": "SMALLINT",
    "TINYINT": "SMALLINT",
    "DOUBLE": "DOUBLE PRECISION",
    "REAL": "REAL",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "VARBINARY": "BYTEA",
}


def _to_pg_type(sqlite_declared: str) -> str:
    sql_type = _sqlite_type_to_sql(sqlite_declared)
    return _PG_TYPE_MAP.get(sql_type, "TEXT")


# SQL type (from _sqlite_type_to_sql) → Trino type name, matching what Trino's
# information_schema.columns reports for the migrated PG table. Used to persist
# column types from the authoritative SQLite schema instead of introspecting
# Trino — the migration runs inside the config-load transaction, so Trino (a
# separate JDBC connection) cannot see the uncommitted PG table.
_TRINO_TYPE_MAP = {
    "VARCHAR": "varchar",
    "BIGINT": "bigint",
    "INTEGER": "integer",
    "SMALLINT": "smallint",
    "DOUBLE": "double",
    "REAL": "real",
    "BOOLEAN": "boolean",
    "TIMESTAMP": "timestamp(6)",
    "DATE": "date",
    "VARBINARY": "varbinary",
}


def sqlite_column_trino_types(source_path: str, sqlite_table: str) -> dict[str, str]:
    """Return {column_name: trino_type} for a SQLite table from its declared schema."""
    sq = sqlite3.connect(source_path)
    try:
        info = sq.execute(f'PRAGMA table_info("{sqlite_table}")').fetchall()
    finally:
        sq.close()
    return {
        row[1]: _TRINO_TYPE_MAP.get(_sqlite_type_to_sql(row[2]), "varchar") for row in info
    }


async def migrate_sqlite_table(
    source_path: str,
    sqlite_table: str,
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
) -> int:
    """Read one table from a SQLite file and write it into PostgreSQL.

    Creates the PG schema and table if they don't exist, then truncates and reloads.
    Returns row count inserted.
    """
    sq = sqlite3.connect(source_path)
    sq.row_factory = sqlite3.Row
    try:
        info = sq.execute(f"PRAGMA table_info(\"{sqlite_table}\")").fetchall()
        if not info:
            log.warning("SQLite table %r not found in %s", sqlite_table, source_path)
            return 0

        col_names = [row[1] for row in info]
        col_defs = ", ".join(
            f'"{row[1]}" {_to_pg_type(row[2])}' for row in info
        )
        # PRAGMA table_info columns: (cid, name, type, notnull, dflt_value, pk).
        # pk > 0 marks PRIMARY KEY membership (value = 1-based position for composites).
        # Preserve it so the migrated PG table carries the constraint — the PK
        # resolution pass reads it from there (SQLite has no Trino/native driver path).
        pk_cols = [r[1] for r in sorted((r for r in info if r[5]), key=lambda r: r[5])]
        if pk_cols:
            col_defs += ", PRIMARY KEY (" + ", ".join(f'"{c}"' for c in pk_cols) + ")"

        await pg_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"')
        await pg_conn.execute(f'DROP TABLE IF EXISTS "{pg_schema}"."{pg_table}"')
        await pg_conn.execute(
            f'CREATE TABLE "{pg_schema}"."{pg_table}" ({col_defs})'
        )

        rows = sq.execute(f'SELECT * FROM "{sqlite_table}"').fetchall()
        if rows:
            placeholders = ", ".join(f"${i + 1}" for i in range(len(col_names)))
            col_list = ", ".join(f'"{c}"' for c in col_names)
            await pg_conn.executemany(
                f'INSERT INTO "{pg_schema}"."{pg_table}" ({col_list}) VALUES ({placeholders})',
                [tuple(row) for row in rows],
            )

        log.info(
            "Migrated SQLite %s.%s → PG %s.%s (%d rows)",
            sqlite_table, source_path, pg_schema, pg_table, len(rows),
        )
        return len(rows)
    finally:
        sq.close()


async def record_mtime(table_id: int, source_path: str, pg_conn: asyncpg.Connection) -> None:
    """Record the current file mtime for table_id in file_source_mtimes."""
    try:
        mtime = os.path.getmtime(source_path)
    except OSError:
        return
    await pg_conn.execute(
        """INSERT INTO file_source_mtimes (table_id, source_mtime, synced_at)
           VALUES ($1, $2, $3)
           ON CONFLICT (table_id) DO UPDATE SET source_mtime = $2, synced_at = $3""",
        table_id, mtime, time.time(),
    )


async def migrate_if_stale(
    table_id: int,
    source_path: str,
    sqlite_table: str,
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
) -> bool:
    """Re-migrate if source file mtime is newer than last recorded. Returns True if migrated."""
    try:
        current_mtime = os.path.getmtime(source_path)
    except OSError:
        return False
    row = await pg_conn.fetchrow(
        "SELECT source_mtime FROM file_source_mtimes WHERE table_id = $1", table_id
    )
    if row and row["source_mtime"] >= current_mtime:
        return False
    await migrate_sqlite_table(source_path, sqlite_table, pg_conn, pg_schema, pg_table)
    await record_mtime(table_id, source_path, pg_conn)
    return True
