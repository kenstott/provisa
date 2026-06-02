# Copyright (c) 2026 Kenneth Stott
# Canary: ca27dd2a-7cc7-45b3-bbbf-f8c095daff20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""Per-process cache of Trino column data types, queried on demand.

Used as a fallback when compile-time column_types dict does not contain
a type (e.g. for dynamically registered tables or API-backed sources).

Call `init(conn)` once at startup.  Then use `get_column_type(...)` anywhere.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

log = logging.getLogger(__name__)

_TTL = 300  # seconds


@dataclass
class _TableEntry:
    columns: dict[str, str]  # column_name → trino data_type (lower)
    expiry: float


_cache: dict[tuple[str, str, str], _TableEntry] = {}
_conn = None  # trino.dbapi.Connection


def init(conn) -> None:
    """Store the Trino connection for on-demand schema queries."""
    global _conn
    _conn = conn


def get_column_type(catalog: str, schema: str, table: str, column: str) -> str:
    """Return the Trino data type for catalog.schema.table.column.

    Returns 'varchar' if the column cannot be resolved.
    """
    key = (catalog, schema, table)
    entry = _cache.get(key)
    now = time.monotonic()
    if entry is None or entry.expiry < now:
        _fetch(catalog, schema, table)
        entry = _cache.get(key)
    if entry is None:
        return "varchar"
    return entry.columns.get(column, entry.columns.get(column.lower(), "varchar"))


def preload(catalog: str, schema: str, table: str) -> None:
    """Eagerly populate the cache for a table (no-op if already fresh)."""
    key = (catalog, schema, table)
    entry = _cache.get(key)
    if entry is None or entry.expiry < time.monotonic():
        _fetch(catalog, schema, table)


def invalidate(catalog: str, schema: str, table: str) -> None:
    """Remove a table from the cache (e.g. after schema change)."""
    _cache.pop((catalog, schema, table), None)


def _fetch(catalog: str, schema: str, table: str) -> None:
    if _conn is None:
        return
    from provisa.compiler.introspect import _validate_ident, _escape_literal

    try:
        cat = _validate_ident(catalog)
        _validate_ident(schema)
        _validate_ident(table)
        cur = _conn.cursor()
        cur.execute(
            f"SELECT column_name, data_type "
            f"FROM {cat}.information_schema.columns "
            f"WHERE table_schema = '{_escape_literal(schema)}' "
            f"AND table_name = '{_escape_literal(table)}'"
        )
        rows = cur.fetchall()
        columns = {row[0].lower(): row[1].lower() for row in rows}
        _cache[(catalog, schema, table)] = _TableEntry(
            columns=columns,
            expiry=time.monotonic() + _TTL,
        )
    except Exception as exc:
        log.debug(
            "[schema_service] fetch failed for %s.%s.%s: %s",
            catalog,
            schema,
            table,
            exc,
        )
