# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB-native materialization-store write face (REQ-989, REQ-990).

An embedded DuckDB file is the fully-embedded zero-config store (REQ-989). DuckDB enforces a single
writer per file: a separate write connection CANNOT open a file the federation engine already ATTACHed
("Unique file handle conflict"). So the DuckDB store is written through the ENGINE'S OWN connection —
the one that already holds the store attached under a catalog alias — never a second connection. This
is the one exception to "the engine never writes the store": engine and store share one file handle,
so they must share one connection.

Landing is columnar/bulk (REQ-990): the table DDL is derived from the canonical IR→SQLAlchemy type
map (portable, no per-store spelling), and rows land through DuckDB's native ``executemany`` — one
prepared statement for the whole batch, never a per-row loop. JSON columns receive the source's
serialized-text value directly (DuckDB's ``JSON`` type parses text on insert).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.schema import CreateSchema, CreateTable

from provisa.core.change_signal import APPEND, select_landing_shape
from provisa.federation.materialize_exec import build_table


def _duckdb_dialect() -> Any:
    import duckdb_engine

    return duckdb_engine.Dialect()


def _qualified(catalog: str, schema: str, table: str) -> str:
    return f'{catalog}."{schema}"."{table}"'


def _existing_columns(con: Any, catalog: str, schema: str, table: str) -> list[str]:
    """The store table's current column names in ordinal order, or ``[]`` if it does not exist."""
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [catalog, schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def _ensure_schema(con: Any, catalog: str, schema: str, dialect: Any) -> None:
    con.execute(
        str(CreateSchema(f"{catalog}.{schema}", if_not_exists=True).compile(dialect=dialect))
    )


def _create_ddl(catalog: str, schema: str, table: str, columns: list[tuple[str, str]]) -> str:
    """CREATE TABLE DDL for the landed table, types from the canonical IR→SQLAlchemy map. The primary
    key is intentionally omitted from the DDL: a BigInteger PK would render as an autoincrementing
    BIGSERIAL, but a landed replica carries the source's own key values — the column stays a plain
    type. (CDC identity, when needed, is enforced by the refresh path, not the store DDL.)"""
    tbl = build_table(f"{catalog}.{schema}", table, columns, ())
    return str(CreateTable(tbl, if_not_exists=True).compile(dialect=_duckdb_dialect()))


def reconcile_duckdb_native(
    con: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
) -> str:
    """Converge the DuckDB store's landing table to ``columns`` through the engine's connection —
    the DDL half of landing (no data), so the catalog is complete at startup and survives restart.

    - absent        -> create.
    - columns match -> KEEP (landed data intact — the restart case).
    - columns drift -> RECREATE (a config/schema change is authoritative; data re-lands on refresh).

    Returns ``created`` | ``kept`` | ``recreated``."""
    dialect = _duckdb_dialect()
    _ensure_schema(con, catalog, schema, dialect)
    have = _existing_columns(con, catalog, schema, table)
    want = [name for name, _ in columns]
    if not have:
        con.execute(_create_ddl(catalog, schema, table, columns))
        return "created"
    if have == want:
        return "kept"
    con.execute(f"DROP TABLE IF EXISTS {_qualified(catalog, schema, table)}")
    con.execute(_create_ddl(catalog, schema, table, columns))
    return "recreated"


def land_duckdb_native(
    con: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    change_signal: str = "ttl",
    watermark_column: str | None = None,
) -> str:
    """Land ``rows`` into the DuckDB store's ``catalog.schema.table`` through the engine connection.

    The shape is chosen from ``change_signal`` (REQ-932): a poll signal with a watermark AMENDS
    (append the delta); every other batch is a full REPLACE (DELETE + bulk insert). Rows land through
    DuckDB's native ``executemany`` — the columnar bulk path (REQ-990). Returns the qualified name."""
    dialect = _duckdb_dialect()
    _ensure_schema(con, catalog, schema, dialect)
    con.execute(_create_ddl(catalog, schema, table, columns))  # create-if-absent (first land)
    qualified = _qualified(catalog, schema, table)
    if select_landing_shape(change_signal, watermark_column) != APPEND:
        con.execute(f"DELETE FROM {qualified}")
    if rows:
        colnames = [name for name, _ in columns]
        collist = ", ".join(f'"{cn}"' for cn in colnames)
        placeholders = ", ".join("?" * len(colnames))
        data = [tuple(r.get(cn) for cn in colnames) for r in rows]
        con.executemany(f"INSERT INTO {qualified} ({collist}) VALUES ({placeholders})", data)
    return qualified
