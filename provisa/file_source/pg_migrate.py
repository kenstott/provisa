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
import sqlite3

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

        await pg_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"')
        await pg_conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{pg_schema}"."{pg_table}" ({col_defs})'
        )
        await pg_conn.execute(f'TRUNCATE "{pg_schema}"."{pg_table}"')

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
