# Copyright (c) 2026 Kenneth Stott
# Canary: 8928e6fa-ecf0-4419-ae75-b59c00b2f5ec
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
    """The store table's current column names in ordinal order, or ``[]`` if it does not exist.

    Profiled live (REQ-1730 engine-swap investigation): with the store schema holding a realistic
    number of already-landed tables (~150), ``information_schema.columns`` costs ~30x what
    ``duckdb_columns()`` does for the identical (catalog, schema, table) lookup — it appears to
    build the full cross-catalog column list before filtering rather than pushing the predicate
    down. Called once per landed table on every schema rebuild, that difference turned the whole
    landing loop roughly quadratic in the total table count: a rebuild that should cost tens of
    milliseconds per table instead cost multiple SECONDS per table once a few hundred were landed,
    blowing well past any registration-visibility timeout a UI test could reasonably set.
    duckdb_columns() is DuckDB's own catalog table function — same filter shape, same ordinal
    guarantee via column_index — and isn't part of the SQL-standard information_schema surface
    that has to stay engine-agnostic here, so it has none of that cost.
    """
    rows = con.execute(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ? "
        "ORDER BY column_index",
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


def persist_duckdb_native(
    con: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    persist: str,
    pk_columns: list[str] | None = None,
) -> str:
    """Apply an MV's recomputed ``rows`` to its OWN DuckDB store table under the declared
    PERSISTENCE outcome (REQ-965), through the engine's own connection (REQ-989) — the duckdb-native
    mirror of ``store_writer.persist_land``/``apply_persistence`` for a store the engine itself holds
    the file handle for.

    - ``replace`` -> DELETE + bulk INSERT (full current-state refresh).
    - ``append``  -> bulk INSERT, upsert-by-key (DELETE matching PKs then INSERT) when a PK is given.
    - ``upsert``  -> DELETE matching PKs then bulk INSERT (a recompute carries each row's full current
      values, so a delete+reinsert converges identically to a partial-row UPDATE).

    An invalid persistence value, or ``upsert`` without a PK, is an EXPLICIT error (never a silent
    downgrade). Returns the qualified store-table name."""
    from provisa.events.outcomes import (
        PERSIST_APPEND,
        PERSIST_REPLACE,
        PERSIST_UPSERT,
        require_pk,
        validate_persist,
    )

    validate_persist(persist)
    require_pk(persist, set(), pk_columns)
    dialect = _duckdb_dialect()
    _ensure_schema(con, catalog, schema, dialect)
    con.execute(_create_ddl(catalog, schema, table, columns))  # create-if-absent (first land)
    qualified = _qualified(catalog, schema, table)
    colnames = [name for name, _ in columns]

    def _bulk_insert(data_rows: list[dict]) -> None:
        if not data_rows:
            return
        collist = ", ".join(f'"{cn}"' for cn in colnames)
        placeholders = ", ".join("?" * len(colnames))
        data = [tuple(r.get(cn) for cn in colnames) for r in data_rows]
        con.executemany(f"INSERT INTO {qualified} ({collist}) VALUES ({placeholders})", data)

    if persist == PERSIST_REPLACE:
        con.execute(f"DELETE FROM {qualified}")
        _bulk_insert(rows)
        return qualified
    pk = list(pk_columns or ())
    if persist in (PERSIST_APPEND, PERSIST_UPSERT) and pk and rows:
        pk_list = ", ".join(f'"{c}"' for c in pk)
        placeholders = ", ".join("(" + ", ".join("?" * len(pk)) + ")" for _ in rows)
        params = [v for r in rows for v in (r.get(c) for c in pk)]
        con.execute(f"DELETE FROM {qualified} WHERE ({pk_list}) IN ({placeholders})", params)
    _bulk_insert(rows)
    return qualified


def apply_cdc_duckdb_native(
    con: Any,
    *,
    catalog: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    events: list,
) -> dict[str, int]:
    """Apply CDC change events (insert/update -> upsert by PK, delete -> tombstone) to the DuckDB
    store's landing table through the engine's own connection (REQ-989/REQ-1733) — the duckdb-native
    mirror of ``materialize_exec.apply_cdc`` for a store the engine itself holds the file handle for.

    A primary key is REQUIRED — without one there is no identity to upsert or delete by. Each event
    applies as its own DELETE-then-INSERT (upsert) or DELETE (tombstone) in stream order, so a
    delete immediately followed by a re-insert of the same key within one debounced batch still
    converges correctly."""
    if not pk_columns:
        raise ValueError(
            f"CDC land into {_qualified(catalog, schema, table)} requires primary key columns "
            "for upsert/delete"
        )
    dialect = _duckdb_dialect()
    _ensure_schema(con, catalog, schema, dialect)
    con.execute(_create_ddl(catalog, schema, table, columns))  # create-if-absent (first land)
    qualified = _qualified(catalog, schema, table)
    colnames = [name for name, _ in columns]
    pk_where = " AND ".join(f'"{c}" = ?' for c in pk_columns)
    counts = {"upsert": 0, "delete": 0}
    for ev in events:
        pk_vals = [ev.row.get(c) for c in pk_columns]
        con.execute(f"DELETE FROM {qualified} WHERE {pk_where}", pk_vals)
        if ev.operation.lower() == "delete":
            counts["delete"] += 1
            continue
        collist = ", ".join(f'"{cn}"' for cn in colnames)
        placeholders = ", ".join("?" * len(colnames))
        data = [ev.row.get(cn) for cn in colnames]
        con.execute(f"INSERT INTO {qualified} ({collist}) VALUES ({placeholders})", data)
        counts["upsert"] += 1
    return counts


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
    DuckDB's native ``executemany`` — the columnar bulk path (REQ-990). Returns the qualified name.

    Runs on a PRIVATE cursor (``con.cursor()``), never ``con`` directly: the caller dispatches this
    to a background thread (``land_table``) so a large land doesn't block the event loop, and this
    must not be the SAME pending result other threads' cursors on ``con`` are using concurrently —
    same reasoning as ``DuckDBFederationRuntime.run()``'s private-cursor comment. The file handle is
    still shared (this module's own docstring: DuckDB allows only one writer connection per file), a
    cursor does not open a second connection, so this does not violate that constraint."""
    dialect = _duckdb_dialect()
    cur = con.cursor()
    try:
        _ensure_schema(cur, catalog, schema, dialect)
        cur.execute(_create_ddl(catalog, schema, table, columns))  # create-if-absent (first land)
        qualified = _qualified(catalog, schema, table)
        if select_landing_shape(change_signal, watermark_column) != APPEND:
            cur.execute(f"DELETE FROM {qualified}")
        if rows:
            colnames = [name for name, _ in columns]
            collist = ", ".join(f'"{cn}"' for cn in colnames)
            placeholders = ", ".join("?" * len(colnames))
            data = [tuple(r.get(cn) for cn in colnames) for r in rows]
            cur.executemany(f"INSERT INTO {qualified} ({collist}) VALUES ({placeholders})", data)
        return qualified
    finally:
        cur.close()
