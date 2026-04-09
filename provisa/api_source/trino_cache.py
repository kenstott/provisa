# Copyright (c) 2026 Kenneth Stott
# Canary: 7e4b2d91-8f3a-4c1e-b5d0-a2f91e83c740
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Materialize API response rows as Parquet in Trino Iceberg (results.api_cache).

Execution model for OpenAPI/REST sources:
  Phase 1 — REST call: native filter args (path/query params) build the URL.
  Phase 2 — Trino SQL: compiled WHERE/ORDER BY/LIMIT applied to the cached rows.

The cache table persists until TTL expires, so repeated queries with the same
native filter args hit Trino directly without re-fetching from the API.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging

import sqlglot
import sqlglot.expressions as exp

log = logging.getLogger(__name__)

CACHE_CATALOG = "results"
CACHE_SCHEMA = "api_cache"
RESULTS_BUCKET = "provisa-results"

_API_TYPE_TO_TRINO: dict[str, str] = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "jsonb": "VARCHAR",
}


def cache_table_name(source_id: str, operation_id: str, native_args: dict) -> str:
    """Stable table name for a given API call signature."""
    key = json.dumps(
        {"s": source_id, "o": operation_id, "a": sorted(native_args.items())},
        sort_keys=True,
    )
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    return f"r_{h}"


def ensure_cache_schema(conn) -> None:
    s3_location = f"s3a://{RESULTS_BUCKET}/api_cache/"
    sql = (
        f"CREATE SCHEMA IF NOT EXISTS {CACHE_CATALOG}.{CACHE_SCHEMA} "
        f"WITH (location = '{s3_location}')"
    )
    try:
        conn.cursor().execute(sql)
    except Exception as exc:
        log.debug("ensure_cache_schema: %s", exc)


def table_exists(conn, table_name: str) -> bool:
    try:
        cur = conn.cursor()
        cur.execute(
            f'SELECT 1 FROM {CACHE_CATALOG}.{CACHE_SCHEMA}."{table_name}" LIMIT 1'
        )
        cur.fetchall()
        return True
    except Exception:
        return False


def create_and_insert(conn, table_name: str, rows: list[dict], columns: list) -> None:
    """Create Iceberg Parquet table and INSERT API response rows.

    Trino workers write Parquet directly to S3 — no serialization in Provisa.
    """
    def _trino_type(col) -> str:
        raw = col.type.value if hasattr(col.type, "value") else str(col.type)
        return _API_TYPE_TO_TRINO.get(raw, "VARCHAR")

    col_defs = ", ".join(f'"{c.name}" {_trino_type(c)}' for c in columns)
    s3_location = f"s3a://{RESULTS_BUCKET}/api_cache/{table_name}/"
    create_sql = (
        f'CREATE TABLE IF NOT EXISTS {CACHE_CATALOG}.{CACHE_SCHEMA}."{table_name}" '
        f"({col_defs}) "
        f"WITH (format = 'PARQUET', location = '{s3_location}')"
    )
    conn.cursor().execute(create_sql)

    if not rows:
        return

    col_names = [c.name for c in columns]

    def _lit(v) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    # Batch INSERT to stay within Trino SQL size limits
    for i in range(0, max(len(rows), 1), 500):
        batch = rows[i : i + 500]
        if not batch:
            break
        vals = ", ".join(
            "(" + ", ".join(_lit(r.get(c)) for c in col_names) + ")"
            for r in batch
        )
        conn.cursor().execute(
            f'INSERT INTO {CACHE_CATALOG}.{CACHE_SCHEMA}."{table_name}" VALUES {vals}'
        )

    log.info(
        "[API CACHE] materialized %d rows → %s.%s.\"%s\"",
        len(rows), CACHE_CATALOG, CACHE_SCHEMA, table_name,
    )


def rewrite_from_cache(sql: str, table_name: str) -> str:
    """Replace the root FROM table in SQL with the Trino Iceberg cache table.

    Uses SQLGlot to safely rewrite the first Table node, preserving all
    WHERE/ORDER BY/LIMIT/JOIN structure.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
        rewritten = False
        for tbl in tree.find_all(exp.Table):
            tbl.set("catalog", exp.to_identifier(CACHE_CATALOG))
            tbl.set("db", exp.to_identifier(CACHE_SCHEMA))
            tbl.set("this", exp.to_identifier(table_name, quoted=True))
            rewritten = True
            break
        if rewritten:
            return tree.sql(dialect="postgres")
    except Exception as exc:
        log.warning("rewrite_from_cache SQLGlot failed: %s", exc)

    # Fallback: regex-based replacement
    import re
    return re.sub(
        r'FROM\s+"[^"]*"\."[^"]*"(?:\."[^"]*")?',
        f'FROM {CACHE_CATALOG}.{CACHE_SCHEMA}."{table_name}"',
        sql,
        count=1,
        flags=re.IGNORECASE,
    )


async def schedule_drop(
    conn,
    table_name: str,
    ttl: int,
    redirect_config=None,
) -> None:
    """Drop cache table and S3 data after TTL seconds."""
    await asyncio.sleep(ttl)
    try:
        conn.cursor().execute(
            f'DROP TABLE IF EXISTS {CACHE_CATALOG}.{CACHE_SCHEMA}."{table_name}"'
        )
        log.info("[API CACHE] dropped %s after TTL=%ds", table_name, ttl)
    except Exception as exc:
        log.warning("[API CACHE] drop failed for %s: %s", table_name, exc)
    if redirect_config is not None:
        from provisa.executor.trino_write import schedule_s3_cleanup
        s3_prefix = f"s3a://{RESULTS_BUCKET}/api_cache/{table_name}/"
        await schedule_s3_cleanup(s3_prefix, redirect_config, delay_seconds=0)
