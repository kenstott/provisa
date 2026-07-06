# Copyright (c) 2026 Kenneth Stott
# Canary: 7e4b2d91-8f3a-4c1e-b5d0-a2f91e83c740
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the COPYRIGHT holder.

"""Materialize API response rows into a cache table for Phase 2 the engine execution.

Default backend: source's own the engine catalog (PostgreSQL connector) so same-source
JOINs are pushed down to a single database.

Any registered the engine catalog can be the cache target — specify via cache_catalog
on the Source config.  The only special case is the Iceberg catalog ("results"):
table CREATE adds PARQUET format+S3 location, and DROP triggers S3 cleanup.

Execution model for OpenAPI/REST sources:
  Phase 1 — REST call: native filter args (path/query params) build the URL.
             On cache miss, rows are materialized into the cache table.
  Phase 2 — the engine SQL: compiled WHERE/ORDER BY/LIMIT applied to cached rows.
             Same-source JOINs are pushed down by the engine when cache catalog
             matches the source catalog (both PostgreSQL).
"""

# Requirements: REQ-280, REQ-309, REQ-318, REQ-327

# complexity-gate: allow-ble=6 reason="API-response cache materialization is best-effort augmentation: any cache/store failure falls back to live execution, never failing the query"

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

log = logging.getLogger(__name__)

_ICEBERG_CATALOG = "results"
_ICEBERG_BUCKET = "provisa-results"
_DEFAULT_CACHE_SCHEMA = "api_cache"

# In-process TTL cache for table_exists results.
# Key: (catalog, schema, table_name) → expiry monotonic time.
# Avoids a live the engine probe on every request when the table is known-live.
_TABLE_EXISTS_CACHE: dict[tuple[str, str, str], float] = {}
_TABLE_EXISTS_SAFETY_MARGIN = 30  # expire this many seconds before the engine TTL

# In-process cache for schema existence — evicted only on process restart.
# Schema DROP is not expected in normal operation; safe to cache indefinitely.
_SCHEMA_EXISTS_CACHE: set[tuple[str, str]] = set()

_API_TYPE_TO_IR: dict[str, str] = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
    "jsonb": "VARCHAR",
}


@dataclass(frozen=True)
class CacheLocation:  # REQ-318, REQ-309, REQ-327
    catalog: str
    schema: str
    backend: str  # "iceberg" or "postgresql" (any non-iceberg catalog)


def cache_location(  # REQ-318, REQ-309, REQ-327
    source_id: str,
    cache_catalog: str | None = None,
    cache_schema: str = _DEFAULT_CACHE_SCHEMA,
) -> CacheLocation:
    """Build cache location.

    cache_catalog=None → source's own the engine catalog (source_id with hyphens→underscores).
    Any other catalog name is used as-is; "results" triggers Iceberg S3 behaviour.
    """
    catalog = cache_catalog if cache_catalog is not None else source_id.replace("-", "_")
    backend = "iceberg" if catalog == _ICEBERG_CATALOG else "postgresql"
    return CacheLocation(catalog, cache_schema, backend)


def cache_table_name(  # REQ-318, REQ-309, REQ-327
    source_id: str, operation_id: str, native_args: dict
) -> str:
    """Stable table name for a given API call signature."""
    key = json.dumps(
        {"s": source_id, "o": operation_id, "a": sorted(native_args.items()), "v": 2},
        sort_keys=True,
    )
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    return f"r_{h}"


def ensure_cache_schema(conn, loc: CacheLocation) -> None:  # REQ-318, REQ-309, REQ-327
    key = (loc.catalog, loc.schema)
    if key in _SCHEMA_EXISTS_CACHE:
        return
    if loc.backend == "iceberg":
        s3_location = f"s3a://{_ICEBERG_BUCKET}/{loc.schema}/"
        sql = (
            f"CREATE SCHEMA IF NOT EXISTS {loc.catalog}.{loc.schema} "
            f"WITH (location = '{s3_location}')"
        )
    else:
        sql = f"CREATE SCHEMA IF NOT EXISTS {loc.catalog}.{loc.schema}"
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()
        _SCHEMA_EXISTS_CACHE.add(key)
    except Exception as exc:
        log.debug("ensure_cache_schema: %s", exc)


def table_known_live(loc: CacheLocation, table_name: str) -> bool:  # REQ-318, REQ-309, REQ-327
    """Return True if the in-process cache confirms this table is live — no the engine probe."""
    key = (loc.catalog, loc.schema, table_name)
    expiry = _TABLE_EXISTS_CACHE.get(key)
    return expiry is not None and time.monotonic() < expiry


def table_exists(  # REQ-318, REQ-309, REQ-327
    conn, loc: CacheLocation, table_name: str, ttl: int | None = None
) -> bool:
    key = (loc.catalog, loc.schema, table_name)
    expiry = _TABLE_EXISTS_CACHE.get(key)
    if expiry is not None and time.monotonic() < expiry:
        return True

    sql = f'SELECT 1 FROM {loc.catalog}.{loc.schema}."{table_name}" LIMIT 1'
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()
        # Cache the positive result; expire before the engine drops the table
        if ttl is not None and ttl > _TABLE_EXISTS_SAFETY_MARGIN:
            _TABLE_EXISTS_CACHE[key] = time.monotonic() + ttl - _TABLE_EXISTS_SAFETY_MARGIN
        elif ttl is not None:
            _TABLE_EXISTS_CACHE[key] = time.monotonic() + max(ttl - 5, 1)
        else:
            # No TTL known — cache for 60s as a safe default
            _TABLE_EXISTS_CACHE[key] = time.monotonic() + 60
        return True
    except Exception as exc:
        _TABLE_EXISTS_CACHE.pop(key, None)
        log.debug(
            "[API CACHE] table_exists=False: %s.%s.%r — %s",
            loc.catalog,
            loc.schema,
            table_name,
            exc,
        )
        return False


def create_and_insert(  # REQ-318, REQ-309, REQ-327, REQ-280
    conn, loc: CacheLocation, table_name: str, rows: list[dict], columns: list
) -> None:
    """Create cache table and INSERT API response rows."""

    def _column_type(col) -> str:
        raw = col.type.value if hasattr(col.type, "value") else str(col.type)
        return _API_TYPE_TO_IR.get(raw, "VARCHAR")

    col_defs = ", ".join(f'"{c.name}" {_column_type(c)}' for c in columns)

    if loc.backend == "iceberg":
        s3_location = f"s3a://{_ICEBERG_BUCKET}/{loc.schema}/{table_name}/"
        create_sql = (
            f'CREATE TABLE IF NOT EXISTS {loc.catalog}.{loc.schema}."{table_name}" '
            f"({col_defs}) "
            f"WITH (format = 'PARQUET', location = '{s3_location}')"
        )
    else:
        create_sql = (
            f'CREATE TABLE IF NOT EXISTS {loc.catalog}.{loc.schema}."{table_name}" ({col_defs})'
        )
    cur = conn.cursor()
    cur.execute(create_sql)
    cur.fetchall()

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
        if isinstance(v, (dict, list)):
            return "'" + json.dumps(v).replace("'", "''") + "'"
        return "'" + str(v).replace("'", "''") + "'"

    def _do_inserts() -> None:
        for i in range(0, max(len(rows), 1), 500):
            batch = rows[i : i + 500]
            if not batch:
                break
            vals = ", ".join(
                "(" + ", ".join(_lit(r.get(c)) for c in col_names) + ")" for r in batch
            )
            insert_sql = f'INSERT INTO {loc.catalog}.{loc.schema}."{table_name}" VALUES {vals}'
            cur2 = conn.cursor()
            cur2.execute(insert_sql)
            cur2.fetchall()

    try:
        _do_inserts()
    except Exception as exc:
        if "TYPE_MISMATCH" in str(exc):
            # Stale cache table has wrong schema — drop and recreate
            drop_cur = conn.cursor()
            drop_cur.execute(f'DROP TABLE IF EXISTS {loc.catalog}.{loc.schema}."{table_name}"')
            drop_cur.fetchall()
            create_cur = conn.cursor()
            create_cur.execute(create_sql.replace("IF NOT EXISTS ", ""))
            create_cur.fetchall()
            _do_inserts()
        else:
            raise

    # REQ-280: collect statistics on the freshly-materialized cache table so the engine cost
    # optimizer can plan joins against it. ANALYZE support varies by connector, so a failure is
    # logged (not raised) — matching analyze_source_tables (REQ-275).
    try:
        analyze_cur = conn.cursor()
        analyze_cur.execute(f'ANALYZE {loc.catalog}.{loc.schema}."{table_name}"')
        analyze_cur.fetchall()
    except Exception:
        log.warning(
            "[API CACHE] ANALYZE failed for %s.%s.%s",
            loc.catalog,
            loc.schema,
            table_name,
            exc_info=True,
        )

    log.info(
        '[API CACHE] materialized %d rows → %s.%s."%s"',
        len(rows),
        loc.catalog,
        loc.schema,
        table_name,
    )


def rewrite_from_cache(
    sql: str, loc: CacheLocation, table_name: str
) -> str:  # REQ-318, REQ-309, REQ-327
    """Replace the root FROM table in SQL with the cache table."""
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
        for tbl in tree.find_all(exp.Table):
            tbl.set("catalog", exp.to_identifier(loc.catalog))
            tbl.set("db", exp.to_identifier(loc.schema))
            tbl.set("this", exp.to_identifier(table_name, quoted=True))
            break
        return tree.sql(dialect="postgres")
    except Exception as exc:
        log.warning("rewrite_from_cache SQLGlot failed: %s", exc)

    import re

    return re.sub(
        r'FROM\s+"[^"]*"\."[^"]*"(?:\."[^"]*")?',
        f'FROM {loc.catalog}.{loc.schema}."{table_name}"',
        sql,
        count=1,
        flags=re.IGNORECASE,
    )


def rewrite_all_from_cache(  # REQ-318, REQ-309, REQ-327
    sql: str,
    cache_rewrites: dict[str, tuple["CacheLocation", str]],
) -> str:
    """Replace ALL API-backed table references with their cache table equivalents.

    cache_rewrites maps physical table name (tbl.name) → (CacheLocation, cache_table_name).
    All matching tables in FROM/JOIN clauses are rewritten; unmatched tables are left as-is.
    """
    if not cache_rewrites:
        return sql
    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
        for tbl in tree.find_all(exp.Table):
            if tbl.name in cache_rewrites:
                orig_name = tbl.name
                loc, cache_tbl = cache_rewrites[orig_name]
                # Preserve the original table name as an alias when the ref is
                # unaliased, so column qualifiers (e.g. shelter__animalBreeds.name)
                # still resolve after the relation is renamed to the cache table.
                if not tbl.alias:
                    tbl.set("alias", exp.TableAlias(this=exp.to_identifier(orig_name)))
                tbl.set("catalog", exp.to_identifier(loc.catalog))
                tbl.set("db", exp.to_identifier(loc.schema))
                tbl.set("this", exp.to_identifier(cache_tbl, quoted=True))
        return tree.sql(dialect="postgres")
    except Exception as exc:
        log.warning("rewrite_all_from_cache SQLGlot failed: %s", exc)

    import re

    result = sql
    for orig_tbl, (loc, cache_tbl) in cache_rewrites.items():
        result = re.sub(
            rf'"[^"]*"\."[^"]*"\."{re.escape(orig_tbl)}"',
            f'{loc.catalog}.{loc.schema}."{cache_tbl}"',
            result,
            flags=re.IGNORECASE,
        )
    return result


async def schedule_drop(  # REQ-318, REQ-309, REQ-327
    engine,
    loc: CacheLocation,
    table_name: str,
    ttl: int,
    redirect_config=None,
) -> None:
    """Drop cache table after TTL seconds — acquires a fresh engine connection at
    drop-time (not held across the sleep)."""
    await asyncio.sleep(ttl)
    _TABLE_EXISTS_CACHE.pop((loc.catalog, loc.schema, table_name), None)
    try:
        with engine.isolated_sync() as conn:
            conn.cursor().execute(f'DROP TABLE IF EXISTS {loc.catalog}.{loc.schema}."{table_name}"')
        log.info("[API CACHE] dropped %s after TTL=%ds", table_name, ttl)
    except Exception as exc:
        log.warning("[API CACHE] drop failed for %s: %s", table_name, exc)
    if loc.backend == "iceberg" and redirect_config is not None:
        from provisa.executor.redirect import schedule_s3_cleanup

        s3_prefix = f"s3a://{_ICEBERG_BUCKET}/{loc.schema}/{table_name}/"
        await schedule_s3_cleanup(s3_prefix, redirect_config, delay_seconds=0)
