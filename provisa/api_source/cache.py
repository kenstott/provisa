# Copyright (c) 2026 Kenneth Stott
# Canary: aaba33c7-dfa7-4166-acfc-b183d0bd1f2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PG-based cache for API source responses (Phase U)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import asyncpg

from provisa.api_source.models import ApiColumnType, ApiEndpoint

# Global default TTL (seconds)
DEFAULT_TTL = 300


def _params_hash(endpoint_id: int, params: dict) -> str:
    """Generate a stable hash for cache key from endpoint ID and sorted params."""
    payload = json.dumps({"endpoint_id": endpoint_id, "params": params}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def resolve_ttl(
    endpoint_ttl: int | None = None,
    source_ttl: int | None = None,
    global_ttl: int | None = None,
) -> int:
    """Resolve TTL: endpoint > source > global default (300s)."""
    if endpoint_ttl is not None:
        return endpoint_ttl
    if source_ttl is not None:
        return source_ttl
    if global_ttl is not None:
        return global_ttl
    return DEFAULT_TTL


def _cache_table_name(endpoint: ApiEndpoint) -> str:
    """Generate cache table name for an endpoint."""
    return f"api_cache_{endpoint.table_name}"


_PG_TYPE_MAP: dict[ApiColumnType, str] = {
    ApiColumnType.string: "TEXT",
    ApiColumnType.integer: "BIGINT",
    ApiColumnType.number: "DOUBLE PRECISION",
    ApiColumnType.boolean: "BOOLEAN",
    ApiColumnType.jsonb: "JSONB",
}


def generate_cache_table_ddl(endpoint: ApiEndpoint) -> str:
    """Generate CREATE TABLE DDL for an endpoint's cache table."""
    table_name = _cache_table_name(endpoint)
    col_defs: list[str] = [
        "_cache_id SERIAL PRIMARY KEY",
        "_endpoint_id INTEGER NOT NULL",
        "_params_hash TEXT NOT NULL",
        "_cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    ]
    for col in endpoint.columns:
        pg_type = _PG_TYPE_MAP.get(col.type, "TEXT")
        col_defs.append(f"{col.name} {pg_type}")

    cols = ",\n    ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {cols}\n);"


async def check_cache(
    conn: asyncpg.Connection,
    endpoint: ApiEndpoint,
    params: dict,
    ttl: int,
) -> list[dict] | None:
    """Check if cached data exists and is within TTL. Returns rows or None."""
    table_name = _cache_table_name(endpoint)
    h = _params_hash(endpoint.id, params)

    # Check if cache table exists
    exists = await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
        table_name,
    )
    if not exists:
        return None

    # Find freshest cache entry
    row = await conn.fetchrow(
        f"SELECT _cached_at FROM {table_name} WHERE _endpoint_id = $1 AND _params_hash = $2 "
        f"ORDER BY _cached_at DESC LIMIT 1",
        endpoint.id, h,
    )
    if row is None:
        return None

    cached_at: datetime = row["_cached_at"]
    now = datetime.now(timezone.utc)
    age = (now - cached_at).total_seconds()
    if age > ttl:
        return None

    # Fetch all cached rows for this key
    col_names = [col.name for col in endpoint.columns]
    cols_sql = ", ".join(col_names)
    rows = await conn.fetch(
        f"SELECT {cols_sql} FROM {table_name} WHERE _endpoint_id = $1 AND _params_hash = $2 "
        f"AND _cached_at = $3",
        endpoint.id, h, cached_at,
    )
    return [dict(r) for r in rows]


async def write_cache(
    conn: asyncpg.Connection,
    endpoint: ApiEndpoint,
    params: dict,
    rows: list[dict],
    ttl: int,
) -> None:
    """Write rows to the cache table. Clears stale entries first."""
    table_name = _cache_table_name(endpoint)
    h = _params_hash(endpoint.id, params)

    # Ensure table exists
    ddl = generate_cache_table_ddl(endpoint)
    await conn.execute(ddl)

    # Clear old entries for this key
    await conn.execute(
        f"DELETE FROM {table_name} WHERE _endpoint_id = $1 AND _params_hash = $2",
        endpoint.id, h,
    )

    # Insert new rows
    col_names = [col.name for col in endpoint.columns]
    all_cols = ["_endpoint_id", "_params_hash"] + col_names
    placeholders = ", ".join(f"${i+1}" for i in range(len(all_cols)))
    cols_sql = ", ".join(all_cols)

    for row in rows:
        values = [endpoint.id, h] + [row.get(c) for c in col_names]
        await conn.execute(
            f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})",
            *values,
        )
