# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a2c60-7b19-4d54-9e02-1c7a0d6f8b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLite connector: plain sync query helpers for the file_source contract.

Mirrors connector_duckdb.py's shape. Sync, stateless, one-shot query operations
(schema discovery, arbitrary SQL, bulk row reads for migration) — doesn't fit
federation's session-lived async contract, so this owns ``import sqlite3``
directly rather than routing through a Connector/runtime.
"""

from __future__ import annotations

import sqlite3


def table_names(path: str) -> list[str]:
    """List all table names in a SQLite file, ordered by name."""
    conn = sqlite3.connect(path)
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def table_info(path: str, table: str) -> list[tuple]:
    """Return PRAGMA table_info(table) rows: (cid, name, type, notnull, dflt_value, pk)."""
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f'PRAGMA table_info("{table}")').fetchall()  # noqa: S608
    finally:
        conn.close()


def execute_sync(path: str, sql: str) -> list[dict]:  # REQ-229
    """Execute SQL against a SQLite file, returning rows as dicts."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(sql)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def fetch_all_rows(path: str, table: str) -> list[tuple]:  # REQ-012, REQ-017, REQ-250
    """SELECT * FROM table, returning raw row tuples (for bulk migration reads)."""
    conn = sqlite3.connect(path)
    try:
        return conn.execute(f'SELECT * FROM "{table}"').fetchall()  # noqa: S608
    finally:
        conn.close()
