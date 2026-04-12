# Copyright (c) 2026 Kenneth Stott
# Canary: 7134ab4e-7d57-4ff4-a2f3-591173682e7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Hot tables: small lookup tables cached in Redis for JOIN optimization (Phase AD6)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

log = logging.getLogger(__name__)


class _HotEncoder(json.JSONEncoder):
    """Handle Trino types that aren't natively JSON serializable."""

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, bytes):
            return o.decode("utf-8", errors="replace")
        return super().default(o)


def _dumps(obj):
    return json.dumps(obj, cls=_HotEncoder)

HOT_PREFIX = "provisa:hot:"


@dataclass
class HotTableEntry:
    """Metadata for a single hot-cached table."""

    table_name: str
    catalog: str
    schema: str
    pk_column: str
    rows: list[dict] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)


class HotTableManager:
    """Manages small lookup tables cached in Redis for JOIN optimization."""

    def __init__(self, redis_url: str, auto_threshold: int, max_rows: int):
        self._redis_url = redis_url
        self._auto_threshold = auto_threshold
        self._max_rows = max_rows
        self._redis = None
        self._hot_tables: dict[str, HotTableEntry] = {}

    async def _connect(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(
                self._redis_url, decode_responses=True,
            )

    async def load_table(
        self,
        trino_conn,
        table_name: str,
        schema: str,
        catalog: str,
        pk_column: str,
    ) -> int:
        """Load a table into Redis. Returns row count.

        Stores each row as a Redis hash entry keyed by PK, plus a bulk
        blob with all rows for fast full-table retrieval.
        """
        await self._connect()

        fqn = f'"{catalog}"."{schema}"."{table_name}"'
        cur = trino_conn.cursor()
        cur.execute(f"SELECT * FROM {fqn}")
        rows_raw = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        row_count = len(rows_raw)
        if row_count > self._max_rows:
            log.warning(
                "Hot table %s has %d rows (max %d), skipping",
                table_name, row_count, self._max_rows,
            )
            return row_count

        rows = [dict(zip(columns, row)) for row in rows_raw]

        # Store in Redis
        blob_key = HOT_PREFIX + table_name + ":blob"
        pk_key_prefix = HOT_PREFIX + table_name + ":pk:"

        pipe = self._redis.pipeline()
        # Delete existing keys first
        pipe.delete(blob_key)
        pipe.set(blob_key, _dumps(rows))

        for row in rows:
            pk_val = row.get(pk_column, "")
            pipe.set(pk_key_prefix + str(pk_val), _dumps(row))

        await pipe.execute()

        entry = HotTableEntry(
            table_name=table_name,
            catalog=catalog,
            schema=schema,
            pk_column=pk_column,
            rows=rows,
            column_names=columns,
        )
        self._hot_tables[table_name] = entry

        log.info("Hot table %s loaded: %d rows, %d columns", table_name, row_count, len(columns))
        return row_count

    async def get_rows(self, table_name: str) -> list[dict]:
        """Fetch all rows for a hot table from Redis."""
        await self._connect()

        blob_key = HOT_PREFIX + table_name + ":blob"
        data = await self._redis.get(blob_key)
        if data is None:
            # Check in-memory cache
            entry = self._hot_tables.get(table_name)
            if entry:
                return entry.rows
            raise KeyError(f"Hot table {table_name!r} not found in Redis")
        return json.loads(data)

    async def invalidate(self, table_name: str) -> None:
        """Delete all Redis keys for a hot table."""
        await self._connect()

        blob_key = HOT_PREFIX + table_name + ":blob"
        pk_key_pattern = HOT_PREFIX + table_name + ":pk:*"

        keys_to_delete = [blob_key]
        async for key in self._redis.scan_iter(match=pk_key_pattern):
            keys_to_delete.append(key)

        if keys_to_delete:
            await self._redis.delete(*keys_to_delete)

        self._hot_tables.pop(table_name, None)
        log.info("Hot table %s invalidated", table_name)

    def is_hot(self, table_name: str) -> bool:
        """Check if a table is currently hot-cached."""
        return table_name in self._hot_tables

    def get_entry(self, table_name: str) -> HotTableEntry | None:
        """Get the hot table entry with metadata."""
        return self._hot_tables.get(table_name)

    @property
    def auto_threshold(self) -> int:
        return self._auto_threshold

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
            self._redis = None


def detect_hot_tables(
    tables: list[dict],
    relationships: list[dict],
    hot_overrides: dict[str, bool | None],
) -> list[str]:
    """Determine which tables should be hot-cached.

    Auto-detection: table is target of a many-to-one relationship.
    hot_overrides: table_name → True (force), False (opt out), None (auto).

    Returns list of table names to cache.
    """
    # Find tables that are targets of many-to-one relationships
    many_to_one_targets: set[str] = set()
    for rel in relationships:
        if rel.get("cardinality") == "many-to-one":
            many_to_one_targets.add(rel["target_table_id"])

    result: list[str] = []
    for tbl in tables:
        table_name = tbl.get("table_name", tbl.get("table", ""))
        override = hot_overrides.get(table_name)

        if override is False:
            continue
        if override is True:
            result.append(table_name)
            continue
        # Auto-detect: target of many-to-one
        if table_name in many_to_one_targets:
            result.append(table_name)

    return result


async def count_table_rows(trino_conn, table_name: str, schema: str, catalog: str) -> int:
    """SELECT COUNT(*) for auto-detection sizing."""
    fqn = f'"{catalog}"."{schema}"."{table_name}"'
    cur = trino_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {fqn}")
    row = cur.fetchone()
    return row[0] if row else 0


async def init_hot_tables(
    raw_config: dict,
    trino_conn,
) -> HotTableManager | None:
    """Initialize hot table manager from raw config. Returns manager or None."""
    import os

    hot_config = raw_config.get("hot_tables", {})
    cache_config = raw_config.get("cache", {})
    redis_url = cache_config.get("redis_url", "")
    if redis_url:
        from provisa.core.secrets import resolve_secrets
        redis_url = resolve_secrets(redis_url)
    if not redis_url or not cache_config.get("enabled"):
        return None

    auto_threshold = hot_config.get("auto_threshold", 1_000)
    hot_mgr = HotTableManager(
        redis_url=redis_url,
        auto_threshold=auto_threshold,
        max_rows=auto_threshold,
    )

    # Detect which tables to hot-cache
    hot_overrides: dict[str, bool | None] = {}
    for tbl_cfg in raw_config.get("tables", []):
        tbl_name = tbl_cfg.get("table") or tbl_cfg.get("table_name")
        if tbl_name and "hot" in tbl_cfg:
            hot_overrides[tbl_name] = tbl_cfg["hot"]

    tables_list = raw_config.get("tables", [])
    rels_list = raw_config.get("relationships", [])
    candidates = detect_hot_tables(tables_list, rels_list, hot_overrides)

    for tbl_name in candidates:
        tbl_cfg = next(
            (t for t in tables_list if (t.get("table") or t.get("table_name")) == tbl_name),
            None,
        )
        if tbl_cfg is None:
            continue
        source_id = tbl_cfg.get("source_id", "")
        schema_name = tbl_cfg.get("schema", "public")
        catalog = source_id.replace("-", "_")
        pk_col = tbl_cfg.get("columns", [{}])[0].get("name", "id") if tbl_cfg.get("columns") else "id"

        override = hot_overrides.get(tbl_name)
        if override is not True:
            row_count = await count_table_rows(trino_conn, tbl_name, schema_name, catalog)
            if row_count > auto_threshold:
                log.info(
                    "Skipping hot table %s: %d rows > threshold %d",
                    tbl_name, row_count, auto_threshold,
                )
                continue

        await hot_mgr.load_table(trino_conn, tbl_name, schema_name, catalog, pk_col)

    return hot_mgr
