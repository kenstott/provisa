# Copyright (c) 2026 Kenneth Stott
# Canary: 2b9e4f1a-7c3d-4a8b-9e0f-5d1c6a2b7e3f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Shared asyncpg query helpers for schema rebuild."""

from __future__ import annotations

import asyncpg


def parse_mask_value(raw: str | None) -> object:
    """Parse a stored mask value string back to a Python value."""
    if raw is None:
        return None
    if raw == "None":
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


async def fetch_tables(conn: asyncpg.Connection) -> list[dict]:
    """Fetch registered tables with columns."""
    rows = await conn.fetch(
        "SELECT id, source_id, domain_id, schema_name, table_name, governance, "
        "alias, description, column_presets "
        "FROM registered_tables ORDER BY id"
    )
    tables = []
    for row in rows:
        table = dict(row)
        col_rows = await conn.fetch(
            "SELECT column_name, visible_to, writable_by, unmasked_to, "
            "mask_type, alias, description, path "
            "FROM table_columns WHERE table_id = $1 ORDER BY id",
            row["id"],
        )
        table["column_presets"] = list(row.get("column_presets") or [])
        table["columns"] = [
            {
                "column_name": r["column_name"],
                "visible_to": list(r["visible_to"]),
                "writable_by": list(r.get("writable_by") or []),
                "unmasked_to": list(r.get("unmasked_to") or []),
                "mask_type": r.get("mask_type"),
                "alias": r["alias"],
                "description": r["description"],
                "path": r["path"],
            }
            for r in col_rows
        ]
        tables.append(table)
    return tables


async def fetch_relationships(conn: asyncpg.Connection) -> list[dict]:
    """Fetch relationships."""
    rows = await conn.fetch(
        "SELECT id, source_table_id, target_table_id, source_column, "
        "target_column, cardinality FROM relationships"
    )
    return [dict(r) for r in rows]
