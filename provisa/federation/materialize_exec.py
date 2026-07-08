# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d9e42-8c3a-4f27-9a05-2d7e1b4c8f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialization LAND executor for a relational (Postgres) store (REQ-844, REQ-848, REQ-932).

Non-attachable sources (openapi/API, graphql_remote) cannot be referenced in place, so a
single-node engine (DuckDB/Postgres) LANDs them into a relational store it can then attach and
federate. ``select_write_face`` (REQ-848) chooses ``WriteFace.SQLALCHEMY_UPSERT`` for such a store;
this module is that write face's execution.

Three landing shapes, selected by the table's change_signal + watermark_column (REQ-932):
- REPLACE  — ``land_rows_into_pg`` — drop+create+insert; a full refresh (no watermark).
- APPEND   — ``append_rows_into_pg`` — insert a watermark-filtered delta into the existing table.
- CDC      — ``apply_cdc_events_into_pg`` — upsert (insert/update) and tombstone (delete) by PK,
  the only shape that carries hard deletes; fed by a Debezium/Kafka provider.
"""

from __future__ import annotations

from typing import Any


def _col_defs(columns: list[tuple[str, str]]) -> str:
    return ", ".join(f'"{name}" {pg_type}' for name, pg_type in columns)


async def land_rows_into_pg(
    conn: Any,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
) -> str:
    """REPLACE land: drop+create+insert ``rows`` into ``schema.table`` (full refresh, no watermark).

    ``columns`` are (name, pg_type) pairs — the projected shape of the source result."""
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    await conn.execute(f'CREATE TABLE "{schema}"."{table}" ({_col_defs(columns)})')
    if rows:
        names = [name for name, _ in columns]
        cols_sql = ", ".join(f'"{n}"' for n in names)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(names)))
        insert = f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        for r in rows:
            await conn.execute(insert, *[r.get(n) for n in names])
    return f"{schema}.{table}"


async def append_rows_into_pg(
    conn: Any,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
) -> str:
    """APPEND land: insert ``rows`` into an existing table without dropping it (REQ-932).

    ``rows`` are the already-watermark-filtered delta (``WHERE watermark > cursor`` upstream), so
    this only creates-if-absent and inserts — no truncation. The caller advances the cursor."""
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({_col_defs(columns)})')
    if rows:
        names = [name for name, _ in columns]
        cols_sql = ", ".join(f'"{n}"' for n in names)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(names)))
        insert = f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        for r in rows:
            await conn.execute(insert, *[r.get(n) for n in names])
    return f"{schema}.{table}"


async def apply_cdc_events_into_pg(
    conn: Any,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    events: list,
) -> dict[str, int]:
    """CDC land: apply change events to a landed table by primary key (REQ-932).

    insert/update → upsert (``ON CONFLICT (pk) DO UPDATE``); delete → tombstone (``DELETE … WHERE
    pk``). ``events`` are ChangeEvent-like objects with ``.operation`` (insert|update|delete) and
    ``.row``. A primary key is REQUIRED — without it there is no identity to upsert or delete by."""
    if not pk_columns:
        raise ValueError(
            f'CDC land into "{schema}"."{table}" requires primary key columns for upsert/delete'
        )
    names = [name for name, _ in columns]
    pk_sql = ", ".join(f'"{pk}"' for pk in pk_columns)
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" '
        f"({_col_defs(columns)}, PRIMARY KEY ({pk_sql}))"
    )
    cols_sql = ", ".join(f'"{n}"' for n in names)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(names)))
    non_pk = [n for n in names if n not in pk_columns]
    if non_pk:
        set_sql = ", ".join(f'"{n}" = EXCLUDED."{n}"' for n in non_pk)
        conflict = f"ON CONFLICT ({pk_sql}) DO UPDATE SET {set_sql}"
    else:
        conflict = f"ON CONFLICT ({pk_sql}) DO NOTHING"
    upsert = f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders}) {conflict}'
    delete_where = " AND ".join(f'"{pk}" = ${i + 1}' for i, pk in enumerate(pk_columns))
    delete = f'DELETE FROM "{schema}"."{table}" WHERE {delete_where}'

    counts = {"upsert": 0, "delete": 0}
    for ev in events:
        if ev.operation.lower() == "delete":
            await conn.execute(delete, *[ev.row.get(pk) for pk in pk_columns])
            counts["delete"] += 1
        else:  # insert / update → upsert
            await conn.execute(upsert, *[ev.row.get(n) for n in names])
            counts["upsert"] += 1
    return counts
