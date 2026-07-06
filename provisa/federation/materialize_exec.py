# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d9e42-8c3a-4f27-9a05-2d7e1b4c8f36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialization LAND executor for a relational (Postgres) store (REQ-844, REQ-848).

Non-attachable sources (openapi/API, graphql_remote) cannot be referenced in place, so a
single-node engine (DuckDB/Postgres) LANDs them into a relational store it can then attach and
federate. ``select_write_face`` (REQ-848) chooses ``WriteFace.SQLALCHEMY_UPSERT`` for such a store;
this module is that write face's execution — the piece that was missing (only the engine/Iceberg
land existed). Replace semantics here (drop+create) model a full refresh; incremental upsert is a
follow-on keyed by the source's watermark.
"""

from __future__ import annotations

from typing import Any


async def land_rows_into_pg(
    conn: Any,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
) -> str:
    """Land ``rows`` into ``schema.table`` on a Postgres store (asyncpg conn). Returns the qualified
    table name. ``columns`` are (name, pg_type) pairs — the projected shape of the source result."""
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
    col_defs = ", ".join(f'"{name}" {pg_type}' for name, pg_type in columns)
    await conn.execute(f'CREATE TABLE "{schema}"."{table}" ({col_defs})')
    if rows:
        names = [name for name, _ in columns]
        cols_sql = ", ".join(f'"{n}"' for n in names)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(names)))
        insert = f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders})'
        for r in rows:
            await conn.execute(insert, *[r.get(n) for n in names])
    return f"{schema}.{table}"
