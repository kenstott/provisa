# Copyright (c) 2026 Kenneth Stott
# Canary: 2b3c4d5e-6f70-8192-b3c4-d5e6f7081920
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GovData query executor via askamerica Python package."""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

from provisa.core.models import GovDataSource

log = logging.getLogger(__name__)

# Requirements: REQ-492, REQ-540

_jvm_lock = threading.Lock()

# (source_id, schema) -> [table_name, ...]
_tables_cache: dict[tuple[str, str], list[str]] = {}
# (source_id, schema, table) -> [(col_name, type_name, remarks), ...]
_columns_cache: dict[tuple[str, str, str], list[tuple[str, str, str | None]]] = {}
# (source_id, schema, table) -> {pk_col_name, ...}
_pk_cache: dict[tuple[str, str, str], set[str]] = {}
# (source_id, schema, table) -> [{"fk_col", "ref_schema", "ref_table", "ref_col"}, ...]
_fk_cache: dict[tuple[str, str, str], list[dict[str, str]]] = {}


def connect(source: GovDataSource):  # REQ-492, REQ-540
    from askamerica.engine import DEFAULT_SCHEMAS, get_connection  # pyright: ignore[reportMissingImports]

    with _jvm_lock:
        if "ASKAMERICA_SCHEMAS" not in os.environ:
            os.environ["ASKAMERICA_SCHEMAS"] = DEFAULT_SCHEMAS
        if "ASKAMERICA_DATA_DIR" not in os.environ:
            os.environ["ASKAMERICA_DATA_DIR"] = os.path.expanduser("~/.provisa_askamerica/data")
        return get_connection(source.api_key)


def fetch_tables(source: GovDataSource, schema: str) -> list[str]:  # REQ-492, REQ-540
    key = (source.id, schema)
    if key in _tables_cache:
        return _tables_cache[key]
    conn = connect(source)
    meta = conn.getMetaData()
    rs = meta.getTables(None, schema, "%", None)
    names = []
    while rs.next():
        names.append(str(rs.getString("TABLE_NAME")).lower())
    rs.close()
    _tables_cache[key] = names
    return names


def fetch_columns(  # REQ-492, REQ-540
    source: GovDataSource, schema: str, table: str
) -> list[tuple[str, str, str | None]]:
    key = (source.id, schema, table)
    if key in _columns_cache:
        return _columns_cache[key]
    conn = connect(source)
    meta = conn.getMetaData()
    rs = meta.getColumns(None, schema, table, "%")
    cols: list[tuple[str, str, str | None]] = []
    while rs.next():
        cols.append(
            (
                str(rs.getString("COLUMN_NAME")).lower(),
                str(rs.getString("TYPE_NAME")).lower(),
                rs.getString("REMARKS"),
            )
        )
    rs.close()
    _columns_cache[key] = cols
    return cols


def fetch_primary_keys(source: GovDataSource, schema: str, table: str) -> set[str]:  # REQ-492
    key = (source.id, schema, table)
    if key in _pk_cache:
        return _pk_cache[key]
    conn = connect(source)
    meta = conn.getMetaData()
    rs = meta.getPrimaryKeys(None, schema, table)
    pks: set[str] = set()
    while rs.next():
        pks.add(str(rs.getString("COLUMN_NAME")).lower())
    rs.close()
    _pk_cache[key] = pks
    return pks


def fetch_foreign_keys(
    source: GovDataSource, schema: str, table: str
) -> list[dict[str, str]]:  # REQ-492, REQ-018
    """Return imported FK references for *table* via JDBC getImportedKeys().

    Each entry: {"fk_col", "ref_schema", "ref_table", "ref_col"}.
    """
    key = (source.id, schema, table)
    if key in _fk_cache:
        return _fk_cache[key]
    conn = connect(source)
    meta = conn.getMetaData()
    rs = meta.getImportedKeys(None, schema, table)
    fks: list[dict[str, str]] = []
    while rs.next():
        fks.append(
            {
                "fk_col": str(rs.getString("FKCOLUMN_NAME")).lower(),
                "ref_schema": (rs.getString("PKTABLE_SCHEM") or schema).lower(),
                "ref_table": str(rs.getString("PKTABLE_NAME")).lower(),
                "ref_col": str(rs.getString("PKCOLUMN_NAME")).lower(),
            }
        )
    rs.close()
    _fk_cache[key] = fks
    return fks


def prime_source(source: GovDataSource, schemas: list[str]) -> None:  # REQ-492, REQ-540
    """Fetch and cache tables (and columns) for all schemas. Called after source creation."""
    for schema in schemas:
        try:
            tables = fetch_tables(source, schema)
            for table in tables:
                try:
                    fetch_columns(source, schema, table)
                    fetch_primary_keys(source, schema, table)
                    fetch_foreign_keys(source, schema, table)
                except Exception:
                    log.warning("govdata prime_source: columns failed for %s.%s", schema, table)
        except Exception:
            log.warning("govdata prime_source: tables failed for schema %s", schema)


def execute_query(source: GovDataSource, sql: str) -> list[dict[str, Any]]:  # REQ-492, REQ-540
    from askamerica.engine import execute_query as _execute  # pyright: ignore[reportMissingImports]

    conn = connect(source)
    log.warning("GovData query: %s", sql[:300])
    return _execute(conn, sql)
