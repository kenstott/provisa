# Copyright (c) 2026 Kenneth Stott
# Canary: f829b2d8-06bc-4381-80e7-768bf0650a60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Table repository — CRUD for registered tables and columns in PG config DB."""

import json

import asyncpg

from provisa.core import domain_policy
from provisa.core.models import Table


async def upsert(conn: asyncpg.Connection, table: Table) -> int | None:
    """Upsert a registered table and its columns. Returns the table row id."""
    domain_id = domain_policy.resolve_domain_id(table.domain_id)
    table_id = await conn.fetchval(
        """
        INSERT INTO registered_tables
            (source_id, domain_id, schema_name, table_name, alias, description, watermark_column, column_presets, view_sql, data_product, materialize, mv_refresh_interval)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (source_id, schema_name, table_name) DO UPDATE SET
            domain_id = EXCLUDED.domain_id,
            alias = EXCLUDED.alias,
            description = EXCLUDED.description,
            watermark_column = EXCLUDED.watermark_column,
            column_presets = EXCLUDED.column_presets,
            view_sql = EXCLUDED.view_sql,
            data_product = EXCLUDED.data_product,
            materialize = EXCLUDED.materialize,
            mv_refresh_interval = EXCLUDED.mv_refresh_interval
        RETURNING id
        """,
        table.source_id,
        domain_id,
        table.schema_name,
        table.table_name,
        getattr(table, "alias", None),
        getattr(table, "description", None),
        getattr(table, "watermark_column", None),
        json.dumps([p.model_dump() for p in getattr(table, "column_presets", [])]),
        getattr(table, "view_sql", None),
        getattr(table, "data_product", False),
        getattr(table, "materialize", False),
        getattr(table, "mv_refresh_interval", 300),
    )

    # Replace columns: delete existing, insert new
    await conn.execute("DELETE FROM table_columns WHERE table_id = $1", table_id)
    for col in table.columns:
        object_fields_raw = getattr(col, "object_fields", [])
        object_fields_json = json.dumps(
            [f.model_dump() if hasattr(f, "model_dump") else f for f in object_fields_raw]
        )
        await conn.execute(
            """
            INSERT INTO table_columns (table_id, column_name, visible_to, writable_by, unmasked_to,
                mask_type, mask_pattern, mask_replace, mask_value, mask_precision,
                alias, description, data_type, path, native_filter_type, is_primary_key,
                is_foreign_key, is_alternate_key, object_fields, scope)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19::jsonb, $20)
            """,
            table_id,
            col.name,
            col.visible_to,
            getattr(col, "writable_by", []),
            getattr(col, "unmasked_to", []),
            getattr(col, "mask_type", None),
            getattr(col, "mask_pattern", None),
            getattr(col, "mask_replace", None),
            getattr(col, "mask_value", None),
            getattr(col, "mask_precision", None),
            getattr(col, "alias", None),
            getattr(col, "description", None),
            getattr(col, "data_type", None),
            getattr(col, "path", None),
            getattr(col, "native_filter_type", None),
            getattr(col, "is_primary_key", False),
            getattr(col, "is_foreign_key", False),
            getattr(col, "is_alternate_key", False),
            object_fields_json,
            getattr(col, "scope", "domain"),
        )
    return table_id


async def get(conn: asyncpg.Connection, table_id: int) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM registered_tables WHERE id = $1", table_id)
    if not row:
        return None
    result = dict(row)
    cols = await conn.fetch(
        "SELECT column_name, visible_to, writable_by, unmasked_to, mask_type, mask_pattern, mask_replace, mask_value, mask_precision, native_filter_type, is_primary_key, is_foreign_key, is_alternate_key, object_fields, scope FROM table_columns WHERE table_id = $1 ORDER BY id",
        table_id,
    )
    result["columns"] = [dict(c) for c in cols]
    return result


async def get_by_name(
    conn: asyncpg.Connection, source_id: str, schema_name: str, table_name: str
) -> dict | None:
    row = await conn.fetchrow(
        """
        SELECT * FROM registered_tables
        WHERE source_id = $1 AND schema_name = $2 AND table_name = $3
        """,
        source_id,
        schema_name,
        table_name,
    )
    if not row:
        return None
    result = dict(row)
    cols = await conn.fetch(
        "SELECT column_name, visible_to, writable_by, unmasked_to, mask_type, mask_pattern, mask_replace, mask_value, mask_precision, native_filter_type, is_primary_key, is_foreign_key, is_alternate_key, object_fields, scope FROM table_columns WHERE table_id = $1 ORDER BY id",
        result["id"],
    )
    result["columns"] = [dict(c) for c in cols]
    return result


async def find_by_table_name(conn: asyncpg.Connection, table_name: str) -> dict | None:
    """Find a registered table by its virtual name.

    The virtual name is alias when set, otherwise table_name.
    Raises ValueError if multiple tables match.
    """
    rows = await conn.fetch(
        "SELECT * FROM registered_tables WHERE alias = $1 OR (alias IS NULL AND table_name = $1)",
        table_name,
    )
    if not rows:
        return None
    if len(rows) > 1:
        sources = [r["source_id"] for r in rows]
        raise ValueError(
            f"Ambiguous table name {table_name!r}: found in sources {sources}. "
            f"Use source-qualified lookup instead."
        )
    return dict(rows[0])


async def list_all(conn: asyncpg.Connection) -> list[dict]:
    rows = await conn.fetch("SELECT * FROM registered_tables ORDER BY id")
    result = []
    for row in rows:
        r = dict(row)
        cols = await conn.fetch(
            "SELECT column_name, data_type, visible_to, writable_by, unmasked_to, mask_type, mask_pattern, mask_replace, mask_value, mask_precision, native_filter_type, is_primary_key, is_foreign_key, is_alternate_key, object_fields, scope FROM table_columns WHERE table_id = $1 ORDER BY id",
            r["id"],
        )
        r["columns"] = [dict(c) for c in cols]
        result.append(r)
    return result


async def delete(conn: asyncpg.Connection, table_id: int) -> bool:
    result = await conn.execute("DELETE FROM registered_tables WHERE id = $1", table_id)
    return result == "DELETE 1"
