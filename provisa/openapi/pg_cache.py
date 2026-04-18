# Copyright (c) 2026 Kenneth Stott
# Canary: b3c4d5e6-f7a8-9012-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Cache OpenAPI endpoint responses into PostgreSQL for Trino federation."""
from __future__ import annotations

import json
import logging
from typing import Any

import asyncpg
import httpx

log = logging.getLogger(__name__)

_JSON_TO_PG: dict[str, str] = {
    "integer": "BIGINT",
    "number": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "array": "JSONB",
    "object": "JSONB",
    "string": "TEXT",
}


def _schema_to_pg_cols(schema: dict | None) -> list[tuple[str, str]]:
    if not schema:
        return []
    if schema.get("type") == "array" and "items" in schema:
        schema = schema["items"]
    props = schema.get("properties", {})
    return [(name, _JSON_TO_PG.get(prop.get("type", "string"), "TEXT")) for name, prop in props.items()]


def _normalize_rows(data: Any) -> list[dict]:
    if isinstance(data, list):
        return [r if isinstance(r, dict) else {"value": str(r)} for r in data]
    if isinstance(data, dict):
        if all(not isinstance(v, (dict, list)) for v in data.values()):
            return [{"status": k, "count": v} for k, v in data.items()]
        return [data]
    return []


async def cache_openapi_table(
    base_url: str,
    path: str,
    default_params: dict,
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
    response_schema: dict | None,
    fallback_cols: list[tuple[str, str]] | None = None,
) -> int:
    """Fetch an OpenAPI endpoint and store results in PostgreSQL.

    Creates schema/table if missing. Always truncates before reload.
    Skips the HTTP fetch when the path contains path parameters (e.g. {petId}) — the
    table is still created (empty) so Trino can introspect its schema.
    Returns the number of rows inserted.
    """
    cols = _schema_to_pg_cols(response_schema) or (fallback_cols or [])
    if not cols:
        log.warning("No schema for %s.%s — skipping OpenAPI cache", pg_schema, pg_table)
        return 0

    rows: list[dict] = []
    has_path_params = "{" in path
    if not has_path_params:
        url = base_url.rstrip("/") + path
        try:
            r = httpx.get(url, params=default_params, timeout=30, follow_redirects=True)
            r.raise_for_status()
            rows = _normalize_rows(r.json())
        except Exception as exc:
            log.warning("OpenAPI fetch failed for %s: %s — creating empty table", url, exc)

    col_defs = ", ".join(f'"{name}" {pg_type}' for name, pg_type in cols)
    await pg_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"')
    await pg_conn.execute(f'DROP TABLE IF EXISTS "{pg_schema}"."{pg_table}"')
    await pg_conn.execute(f'CREATE TABLE "{pg_schema}"."{pg_table}" ({col_defs})')

    if rows:
        col_names = [c[0] for c in cols]
        placeholders = ", ".join(f"${i + 1}" for i in range(len(col_names)))
        col_list = ", ".join(f'"{c}"' for c in col_names)
        data_rows: list[tuple] = []
        for row in rows:
            vals = []
            for name, _ in cols:
                v = row.get(name)
                if isinstance(v, (list, dict)):
                    v = json.dumps(v)
                vals.append(v)
            data_rows.append(tuple(vals))
        await pg_conn.executemany(
            f'INSERT INTO "{pg_schema}"."{pg_table}" ({col_list}) VALUES ({placeholders})',
            data_rows,
        )

    log.info(
        "Cached OpenAPI %s → PG %s.%s (%d rows)",
        path, pg_schema, pg_table, len(rows),
    )
    return len(rows)
