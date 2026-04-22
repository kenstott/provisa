# Copyright (c) 2026 Kenneth Stott
# Canary: b3c4d5e6-f7a8-9012-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Cache OpenAPI endpoint responses into PostgreSQL for Trino federation."""
from __future__ import annotations

import hashlib
import json
import logging
import time as _time
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx

# In-memory freshness guard: (schema, table, phash) → monotonic expiry.
# Avoids a PG round-trip on cache hits.
_mem_fresh: dict[tuple[str, str, str], float] = {}

log = logging.getLogger(__name__)

_JSON_TO_PG: dict[str, str] = {
    "integer": "BIGINT",
    "number": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "array": "JSONB",
    "object": "JSONB",
    "string": "TEXT",
}

# Metadata columns appended to every cache table.
# _params_hash groups rows by the fetch params that produced them (supports coexistence
# of mask=true vs mask=false, or any other param variation in the same table).
# _cached_at enables TTL-based expiry per hash group without knowing the entity PK.
_META_COLS = [("_params_hash", "TEXT"), ("_cached_at", "TIMESTAMPTZ")]


def _hash_params(params: dict) -> str:
    return hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:16]


def is_mem_fresh(pg_schema: str, pg_table: str, params: dict) -> bool:
    """Synchronous in-memory-only freshness check — no PG round-trip."""
    return _mem_fresh.get((pg_schema, pg_table, _hash_params(params)), 0) > _time.monotonic()


def _schema_to_pg_cols(schema: dict | None) -> list[tuple[str, str]]:
    if not schema:
        return []
    if schema.get("type") == "array" and "items" in schema:
        schema = schema["items"]
    props = schema.get("properties", {})
    return [(name, _JSON_TO_PG.get(prop.get("type", "string"), "TEXT")) for name, prop in props.items()]


def _check_error_path(data: Any, error_path: str | None) -> str | None:
    """Return error message if error_path resolves to a truthy value in the response."""
    if not error_path or not isinstance(data, dict):
        return None
    val = data
    for key in error_path.split("."):
        if not isinstance(val, dict):
            return None
        val = val.get(key)
    return str(val) if val else None


def _normalize_rows(data: Any, response_root: str | None = None) -> list[dict]:
    if response_root and isinstance(data, dict):
        data = data.get(response_root, data)
    if isinstance(data, list):
        return [r if isinstance(r, dict) else {"value": str(r)} for r in data]
    if isinstance(data, dict):
        if all(not isinstance(v, (dict, list)) for v in data.values()):
            return [{"status": k, "count": v} for k, v in data.items()]
        return [data]
    return []


def _to_row_tuple(row: dict, col_names: list[str], phash: str, text_cols: frozenset[str] = frozenset()) -> tuple:
    vals = []
    for name in col_names:
        v = row.get(name)
        if isinstance(v, (list, dict)):
            v = json.dumps(v)
        elif name in text_cols and isinstance(v, (int, float)) and not isinstance(v, bool):
            v = str(v)
        vals.append(v)
    vals.append(phash)
    vals.append(datetime.now(UTC))
    return tuple(vals)


async def _insert_rows(
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
    col_names: list[str],
    rows: list[dict],
    phash: str,
    text_cols: frozenset[str] = frozenset(),
) -> int:
    if not rows:
        return 0
    all_cols = col_names + ["_params_hash", "_cached_at"]
    placeholders = ", ".join(f"${i + 1}" for i in range(len(all_cols)))
    col_list = ", ".join(f'"{c}"' for c in all_cols)
    data_rows = [_to_row_tuple(row, col_names, phash, text_cols) for row in rows]
    await pg_conn.executemany(
        f'INSERT INTO "{pg_schema}"."{pg_table}" ({col_list}) VALUES ({placeholders})',
        data_rows,
    )
    return len(data_rows)


async def _upsert_rows(
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
    col_names: list[str],
    rows: list[dict],
    phash: str,
    pk_column: str,
    text_cols: frozenset[str] = frozenset(),
) -> int:
    """Upsert rows keyed by (pk_column, _params_hash). Requires a UNIQUE constraint on those columns."""
    if not rows:
        return 0
    all_cols = col_names + ["_params_hash", "_cached_at"]
    placeholders = ", ".join(f"${i + 1}" for i in range(len(all_cols)))
    col_list = ", ".join(f'"{c}"' for c in all_cols)
    update_set = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in all_cols if c not in (pk_column, "_params_hash")
    )
    data_rows = [_to_row_tuple(row, col_names, phash, text_cols) for row in rows]
    await pg_conn.executemany(
        f'INSERT INTO "{pg_schema}"."{pg_table}" ({col_list}) VALUES ({placeholders})'
        f' ON CONFLICT ("{pk_column}", "_params_hash") DO UPDATE SET {update_set}',
        data_rows,
    )
    return len(data_rows)


def _mark_fresh(pg_schema: str, pg_table: str, phash: str, ttl: int) -> None:
    now = _time.monotonic()
    _mem_fresh[(pg_schema, pg_table, phash)] = now + ttl
    # Evict expired entries to prevent unbounded growth
    expired = [k for k, exp in _mem_fresh.items() if exp <= now]
    for k in expired:
        del _mem_fresh[k]


async def _is_fresh(
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
    phash: str,
    ttl: int,
) -> bool:
    if _mem_fresh.get((pg_schema, pg_table, phash), 0) > _time.monotonic():
        return True
    try:
        cached_at = await pg_conn.fetchval(
            f'SELECT _cached_at FROM "{pg_schema}"."{pg_table}" WHERE _params_hash = $1 LIMIT 1',
            phash,
        )
    except Exception:
        return False
    if cached_at is None:
        return False
    fresh = datetime.now(UTC) - cached_at.replace(tzinfo=UTC) < timedelta(seconds=ttl)
    if fresh:
        _mark_fresh(pg_schema, pg_table, phash, ttl)
    return fresh


async def cache_openapi_table(
    base_url: str,
    path: str,
    default_params: dict,
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
    response_schema: dict | None,
    fallback_cols: list[tuple[str, str]] | None = None,
    pk_column: str | None = None,
) -> int:
    """Fetch an OpenAPI endpoint and store results in PostgreSQL.

    Creates schema/table (with _params_hash + _cached_at meta columns) on every call.
    If pk_column is set, adds a UNIQUE constraint on (pk_column, _params_hash) for bulk upsert support.
    Skips HTTP fetch for path-param endpoints — table is created empty for Trino introspection.
    Returns number of rows inserted.
    """
    entity_cols = _schema_to_pg_cols(response_schema) or (fallback_cols or [])
    if not entity_cols:
        log.warning("No schema for %s.%s — skipping OpenAPI cache", pg_schema, pg_table)
        return 0

    rows: list[dict] = []
    has_path_params = "{" in path
    phash = _hash_params(default_params)
    if not has_path_params:
        url = base_url.rstrip("/") + path
        try:
            r = httpx.get(url, params=default_params, timeout=30, follow_redirects=True)
            r.raise_for_status()
            rows = _normalize_rows(r.json())
        except Exception as exc:
            _is_client_err = hasattr(exc, "response") and 400 <= exc.response.status_code < 500
            (_log := log.debug if _is_client_err else log.warning)(
                "OpenAPI fetch failed for %s: %s — creating empty table", url, exc
            )

    all_cols = entity_cols + _META_COLS
    col_defs = ", ".join(f'"{name}" {pg_type}' for name, pg_type in all_cols)
    await pg_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"')
    await pg_conn.execute(f'DROP TABLE IF EXISTS "{pg_schema}"."{pg_table}"')
    await pg_conn.execute(f'CREATE TABLE "{pg_schema}"."{pg_table}" ({col_defs})')
    if pk_column:
        await pg_conn.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{pg_table}__pk_hash_uidx"'
            f' ON "{pg_schema}"."{pg_table}" ("{pk_column}", "_params_hash")'
        )

    col_names = [c[0] for c in entity_cols]
    text_cols = frozenset(name for name, pg_type in entity_cols if pg_type == "TEXT")
    n = await _insert_rows(pg_conn, pg_schema, pg_table, col_names, rows, phash, text_cols)
    log.info("Cached OpenAPI %s → PG %s.%s (%d rows, hash=%s)", path, pg_schema, pg_table, n, phash)
    return n


async def fill_api_table(
    base_url: str,
    path: str,
    params: dict,
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
    ttl: int = 300,
    response_root: str | None = None,
    error_path: str | None = None,
    pk_column: str | None = None,
) -> int:
    """Re-fetch a non-path-param endpoint and reload rows for this params combination.

    Keyed by _params_hash so mask=true and mask=false coexist in the same table.
    If pk_column is set, uses bulk upsert on (pk_column, _params_hash) instead of DELETE+INSERT.
    Skips fetch if rows for this hash are still within TTL.
    Returns number of rows inserted (0 if cache was fresh).
    """
    phash = _hash_params(params)
    if await _is_fresh(pg_conn, pg_schema, pg_table, phash, ttl):
        return 0

    url = base_url.rstrip("/") + path
    try:
        r = httpx.get(url, params=params, timeout=30, follow_redirects=True)
        r.raise_for_status()
        data = r.json()
        err = _check_error_path(data, error_path)
        if err:
            log.warning("fill_api_table API error at %s (error_path=%s): %s", url, error_path, err)
            return 0
        rows = _normalize_rows(data, response_root)
    except Exception as exc:
        _is_client_err = hasattr(exc, "response") and 400 <= exc.response.status_code < 500
        (_log := log.debug if _is_client_err else log.warning)(
            "fill_api_table fetch failed for %s: %s", url, exc
        )
        return 0

    if not rows:
        return 0

    col_names = list(rows[0].keys())
    # Introspect column types to coerce Python scalars to match TEXT columns.
    try:
        type_rows = await pg_conn.fetch(
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = $1 AND table_name = $2",
            pg_schema, pg_table,
        )
        text_cols = frozenset(r["column_name"] for r in type_rows if r["data_type"] in ("text", "character varying"))
    except Exception:
        text_cols = frozenset()
    if pk_column and pk_column in col_names:
        await pg_conn.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS "{pg_table}__pk_hash_uidx"'
            f' ON "{pg_schema}"."{pg_table}" ("{pk_column}", "_params_hash")'
        )
        n = await _upsert_rows(pg_conn, pg_schema, pg_table, col_names, rows, phash, pk_column, text_cols)
    else:
        await pg_conn.execute(
            f'DELETE FROM "{pg_schema}"."{pg_table}" WHERE _params_hash = $1', phash
        )
        n = await _insert_rows(pg_conn, pg_schema, pg_table, col_names, rows, phash, text_cols)
    _mark_fresh(pg_schema, pg_table, phash, ttl)
    log.info("fill_api_table %s → PG %s.%s (%d rows, hash=%s)", path, pg_schema, pg_table, n, phash)
    return n


async def fetch_pk_row(
    base_url: str,
    path_template: str,
    path_param_name: str,
    pk: object,
    pg_conn: asyncpg.Connection,
    pg_schema: str,
    pg_table: str,
    ttl: int = 300,
    response_root: str | None = None,
    error_path: str | None = None,
) -> int:
    """Fetch a single path-param API response and cache it by params hash.

    Each (path_param_name, pk) combination gets its own hash group so
    different PK values coexist in the same table without knowing the entity PK.
    Skips fetch if this hash group is still within TTL.
    Returns number of rows inserted (0 if cache was fresh or fetch failed).
    """
    params = {path_param_name: str(pk)}
    phash = _hash_params(params)
    if await _is_fresh(pg_conn, pg_schema, pg_table, phash, ttl):
        return 0

    path = path_template.replace(f"{{{path_param_name}}}", str(pk))
    url = base_url.rstrip("/") + path
    try:
        r = httpx.get(url, timeout=30, follow_redirects=True)
        if r.status_code == 404:
            return 0
        r.raise_for_status()
        data = r.json()
        err = _check_error_path(data, error_path)
        if err:
            log.warning("fetch_pk_row API error at %s pk=%s (error_path=%s): %s", path_template, pk, error_path, err)
            return 0
        # Path-param endpoints return a single object — wrap directly.
        # _normalize_rows mangles flat dicts into status/count pairs, which is wrong here.
        rows = [data] if isinstance(data, dict) and not response_root else _normalize_rows(data, response_root)
    except Exception as exc:
        log.warning("fetch_pk_row failed for %s: %s", url, exc)
        return 0

    if not rows:
        return 0

    await pg_conn.execute(
        f'DELETE FROM "{pg_schema}"."{pg_table}" WHERE _params_hash = $1', phash
    )
    col_names = list(rows[0].keys())
    n = await _insert_rows(pg_conn, pg_schema, pg_table, col_names, rows, phash)
    _mark_fresh(pg_schema, pg_table, phash, ttl)
    log.info("fetch_pk_row %s pk=%s → PG %s.%s (%d rows, hash=%s)", path_template, pk, pg_schema, pg_table, n, phash)
    return n
