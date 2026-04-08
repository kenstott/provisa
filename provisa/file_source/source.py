# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-1234-abcdef012345
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""File-based source adapter: SQLite, CSV, Parquet (Issue #27).

Supports local paths and fsspec transports (s3://, ftp://, sftp://).
Each adapter exposes:
  - FileSourceConfig: dataclass with connection params
  - discover_schema(config) -> list[dict]  — infer columns from file
  - execute_query(config, sql) -> list[dict]  — run SQL/filter, return rows

For CSV and Parquet, a DuckDB in-memory instance executes SQL against the
file, enabling full SQL support without a running database server.
For SQLite, the standard sqlite3 module is used directly.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Python→SQL type string mapping for schema inference
_PYTHON_TO_SQL: dict[str, str] = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "int16": "SMALLINT",
    "int8": "TINYINT",
    "float64": "DOUBLE",
    "float32": "REAL",
    "bool": "BOOLEAN",
    "object": "VARCHAR",
    "string": "VARCHAR",
    "datetime64[ns]": "TIMESTAMP",
    "date32[day]": "DATE",
    "date64": "TIMESTAMP",
}


@dataclass
class FileSourceConfig:
    """File-based source connection configuration."""

    id: str
    source_type: str  # "sqlite" | "csv" | "parquet"
    path: str  # local path or fsspec URL (s3://bucket/key.parquet, etc.)
    options: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Schema discovery
# ---------------------------------------------------------------------------


def discover_schema(config: FileSourceConfig) -> list[dict]:
    """Infer columns from a file-based source.

    Returns list of dicts: {"name": str, "type": str, "nullable": bool}.
    Raises ValueError for unsupported source_type.
    """
    if config.source_type == "sqlite":
        return _discover_sqlite(config)
    if config.source_type == "csv":
        return _discover_csv(config)
    if config.source_type == "parquet":
        return _discover_parquet(config)
    raise ValueError(f"Unsupported file source type: {config.source_type!r}")


def _discover_sqlite(config: FileSourceConfig) -> list[dict]:
    """Discover all tables and their columns from a SQLite file."""
    conn = sqlite3.connect(config.path)
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        columns: list[dict] = []
        for table in tables:
            info = conn.execute(f"PRAGMA table_info({table})").fetchall()  # noqa: S608
            for col in info:
                # col: (cid, name, type, notnull, default_val, pk)
                columns.append({
                    "table": table,
                    "name": col[1],
                    "type": _sqlite_type_to_sql(col[2]),
                    "nullable": col[3] == 0,
                })
        return columns
    finally:
        conn.close()


def _sqlite_type_to_sql(sqlite_type: str) -> str:
    """Map SQLite declared type to standard SQL type string."""
    t = sqlite_type.upper()
    if "INT" in t:
        return "BIGINT"
    if "REAL" in t or "FLOAT" in t or "DOUBLE" in t:
        return "DOUBLE"
    if "BOOL" in t:
        return "BOOLEAN"
    if "DATE" in t or "TIME" in t:
        return "TIMESTAMP"
    if "BLOB" in t:
        return "VARBINARY"
    return "VARCHAR"


def _discover_csv(config: FileSourceConfig) -> list[dict]:
    """Infer columns from a CSV file using pyarrow."""
    import pyarrow.csv as pac

    table = pac.read_csv(config.path)
    return _arrow_schema_to_columns(table.schema)


def _discover_parquet(config: FileSourceConfig) -> list[dict]:
    """Infer columns from a Parquet file using pyarrow."""
    import pyarrow.parquet as pq

    schema = pq.read_schema(config.path)
    return _arrow_schema_to_columns(schema)


def _arrow_schema_to_columns(schema: Any) -> list[dict]:
    """Convert a pyarrow Schema to column definition dicts."""
    import pyarrow as pa

    columns: list[dict] = []
    for i in range(len(schema)):
        field = schema.field(i)
        columns.append({
            "name": field.name,
            "type": _arrow_type_to_sql(field.type),
            "nullable": field.nullable,
        })
    return columns


def _arrow_type_to_sql(arrow_type: Any) -> str:
    """Map a pyarrow DataType to a SQL type string."""
    import pyarrow as pa

    if pa.types.is_int64(arrow_type) or pa.types.is_uint64(arrow_type):
        return "BIGINT"
    if pa.types.is_int32(arrow_type) or pa.types.is_uint32(arrow_type):
        return "INTEGER"
    if pa.types.is_int16(arrow_type) or pa.types.is_uint16(arrow_type):
        return "SMALLINT"
    if pa.types.is_int8(arrow_type) or pa.types.is_uint8(arrow_type):
        return "TINYINT"
    if pa.types.is_float64(arrow_type):
        return "DOUBLE"
    if pa.types.is_float32(arrow_type):
        return "REAL"
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_date(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP"
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "VARBINARY"
    return "VARCHAR"


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


def execute_query(config: FileSourceConfig, sql: str) -> list[dict]:
    """Execute a SQL statement against a file-based source.

    For CSV/Parquet: uses DuckDB in-memory (auto-registered as a view).
    For SQLite: uses the sqlite3 module directly.

    Returns list of row dicts.
    Raises ValueError for unsupported source_type.
    """
    if config.source_type == "sqlite":
        return _execute_sqlite(config, sql)
    if config.source_type in ("csv", "parquet"):
        return _execute_duckdb(config, sql)
    raise ValueError(f"Unsupported file source type: {config.source_type!r}")


def _execute_sqlite(config: FileSourceConfig, sql: str) -> list[dict]:
    """Execute SQL against a SQLite file."""
    conn = sqlite3.connect(config.path)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(sql)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def _execute_duckdb(config: FileSourceConfig, sql: str) -> list[dict]:
    """Execute SQL against a CSV or Parquet file via DuckDB in-memory."""
    import duckdb  # optional dep — raises ImportError if not installed

    ext = config.source_type
    path = config.path
    view_name = Path(path).stem.replace("-", "_").replace(".", "_")

    con = duckdb.connect(":memory:")
    if ext == "csv":
        con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_csv_auto('{path}')")
    else:
        con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{path}')")

    rel = con.execute(sql)
    cols = [desc[0] for desc in rel.description]
    rows = rel.fetchall()
    return [dict(zip(cols, row)) for row in rows]


# ---------------------------------------------------------------------------
# Source adapter interface (matches registry protocol)
# ---------------------------------------------------------------------------


def generate_catalog_properties(config: FileSourceConfig) -> dict[str, str]:
    """Not applicable for file sources — returns empty dict."""
    return {}


def generate_table_definitions(config: FileSourceConfig) -> list[dict]:
    """Return table definitions inferred from the file schema."""
    columns = discover_schema(config)
    # Group by table (sqlite has multiple tables; csv/parquet have one)
    by_table: dict[str, list[dict]] = {}
    for col in columns:
        tbl = col.get("table", Path(config.path).stem)
        by_table.setdefault(tbl, []).append({
            "name": col["name"],
            "type": col["type"],
        })
    return [
        {"tableName": tbl, "columns": cols}
        for tbl, cols in by_table.items()
    ]
