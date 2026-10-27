# Copyright (c) 2026 Kenneth Stott
# Canary: b6a36616-cdc6-45f6-8733-2201717af8a3
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
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import quote


def _open(path: str, *, write: bool = False, **options: object) -> sqlite3.Connection:
    """The source's file, opened as it is: never created. ``sqlite3.connect`` makes an empty
    database at a path with nothing there, and a source whose file had gone (a demo's source
    after its seed was built, REQ-2006) then read as an empty table rather than failing by name.
    Readers open it read-only; ``write`` (a statement that may change it) read-write."""
    mode = "rw" if write else "ro"
    uri = f"file:{quote(str(Path(path).resolve()))}?mode={mode}"
    return sqlite3.connect(uri, uri=True, **options)  # type: ignore[arg-type]


def table_names(path: str) -> list[str]:
    """List all table names in a SQLite file, ordered by name."""
    conn = _open(path)
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def table_info(path: str, table: str) -> list[tuple]:
    """Return PRAGMA table_info(table) rows: (cid, name, type, notnull, dflt_value, pk)."""
    conn = _open(path)
    try:
        return conn.execute(f'PRAGMA table_info("{table}")').fetchall()  # noqa: S608
    finally:
        conn.close()


def execute_sync(path: str, sql: str) -> list[dict]:  # REQ-229
    """Execute SQL against a SQLite file, returning rows as dicts."""
    conn = _open(path, write=True)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(sql)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def iter_row_batches(path: str, sql: str, batch_rows: int) -> Iterator[list[dict]]:  # REQ-1915
    """The rows of ``sql`` against a SQLite file in batches of at most ``batch_rows``, fetched
    from the statement's cursor a batch at a time: only one batch is held. The connection may
    be stepped from more than one thread, one at a time."""
    conn = _open(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(sql)
        while True:
            chunk = cursor.fetchmany(batch_rows)
            if not chunk:
                return
            yield [dict(row) for row in chunk]
    finally:
        conn.close()


def fetch_all_rows(path: str, table: str) -> list[tuple]:  # REQ-012, REQ-017, REQ-250
    """SELECT * FROM table, returning raw row tuples (for bulk migration reads)."""
    conn = _open(path)
    try:
        return conn.execute(f'SELECT * FROM "{table}"').fetchall()  # noqa: S608
    finally:
        conn.close()
