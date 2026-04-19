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

from provisa.compiler.naming import source_to_catalog

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


@dataclass
class HotTableCandidate:
    """Metadata for a table that should be auto-promoted after its first small query."""

    table_name: str
    pk_column: str
    catalog: str
    schema: str


class HotTableManager:
    """Manages small lookup tables cached in Redis for JOIN optimization."""

    def __init__(self, redis_url: str, auto_threshold: int, max_rows: int):
        self._redis_url = redis_url
        self._auto_threshold = auto_threshold
        self._max_rows = max_rows
        self._redis = None
        self._hot_tables: dict[str, HotTableEntry] = {}
        self._candidates: dict[str, HotTableCandidate] = {}

    async def _connect(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(
                self._redis_url, decode_responses=True,
            )

    async def _store_rows(
        self,
        table_name: str,
        rows: list[dict],
        pk_column: str,
        catalog: str,
        schema: str,
    ) -> int:
        """Write rows into Redis and in-memory cache. Returns row count."""
        await self._connect()

        columns = list(rows[0].keys()) if rows else []
        blob_key = HOT_PREFIX + table_name + ":blob"
        pk_key_prefix = HOT_PREFIX + table_name + ":pk:"

        pipe = self._redis.pipeline()
        pipe.delete(blob_key)
        pipe.set(blob_key, _dumps(rows))
        for row in rows:
            pk_val = row.get(pk_column, "")
            pipe.set(pk_key_prefix + str(pk_val), _dumps(row))
        await pipe.execute()

        self._hot_tables[table_name] = HotTableEntry(
            table_name=table_name,
            catalog=catalog,
            schema=schema,
            pk_column=pk_column,
            rows=rows,
            column_names=columns,
        )
        log.info("Hot table %s loaded: %d rows, %d columns", table_name, len(rows), len(columns))
        return len(rows)

    async def load_table(
        self,
        trino_conn,
        table_name: str,
        schema: str,
        catalog: str,
        pk_column: str,
    ) -> int:
        """Load a Trino-backed table into Redis. Returns row count."""
        fqn = f'"{catalog}"."{schema}"."{table_name}"'
        cur = trino_conn.cursor()
        cur.execute(f"SELECT * FROM {fqn}")
        rows_raw = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        row_count = len(rows_raw)
        if row_count > self._max_rows:
            log.warning("Hot table %s has %d rows (max %d), skipping", table_name, row_count, self._max_rows)
            return row_count

        rows = [dict(zip(columns, row)) for row in rows_raw]
        return await self._store_rows(table_name, rows, pk_column, catalog, schema)

    async def load_table_from_sqlite(
        self,
        source_cfg: dict,
        table_name: str,
        pk_column: str,
    ) -> int:
        """Load a SQLite table into Redis. Returns row count."""
        from provisa.file_source.source import FileSourceConfig, execute_query

        path = source_cfg.get("path", "")
        cfg = FileSourceConfig(id=source_cfg["id"], source_type="sqlite", path=path)
        rows = execute_query(cfg, f"SELECT * FROM \"{table_name}\" LIMIT {self._max_rows + 1}")  # noqa: S608

        if len(rows) > self._max_rows:
            log.info("Skipping hot table %s: %d rows > threshold %d", table_name, len(rows), self._max_rows)
            return len(rows)

        return await self._store_rows(table_name, rows, pk_column, source_cfg["id"], "default")

    async def load_table_from_openapi(
        self,
        source_cfg: dict,
        table_name: str,
        pk_column: str,
    ) -> int:
        """Load an OpenAPI resource into Redis by finding its list operation. Returns row count."""
        import httpx

        spec_url = source_cfg.get("path", "")
        base_url = source_cfg.get("base_url", "").rstrip("/")
        auth_config = source_cfg.get("auth_config")

        async with httpx.AsyncClient(timeout=30.0) as client:
            spec_resp = await client.get(spec_url)
            spec_resp.raise_for_status()
            spec = spec_resp.json()

        rows = await _openapi_list_rows(spec, base_url, table_name, auth_config, self._max_rows)
        if rows is None:
            log.info("No list operation found for %s in OpenAPI spec — skipping hot cache", table_name)
            return 0

        if len(rows) > self._max_rows:
            log.info("Skipping hot table %s: %d rows > threshold %d", table_name, len(rows), self._max_rows)
            return len(rows)

        return await self._store_rows(table_name, rows, pk_column, source_cfg["id"], "default")

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

    def register_candidate(self, candidate: HotTableCandidate) -> None:
        """Register a table as an auto-promotion candidate."""
        self._candidates[candidate.table_name] = candidate

    async def maybe_promote(
        self,
        table_name: str,
        rows: list[tuple],
        column_names: list[str],
    ) -> None:
        """Promote table to hot cache if it's a candidate and result is small enough."""
        if self.is_hot(table_name):
            return
        candidate = self._candidates.get(table_name)
        if candidate is None:
            return
        if len(rows) > self._auto_threshold:
            log.debug("Hot table candidate %s: %d rows > threshold %d, skipping", table_name, len(rows), self._auto_threshold)
            return
        row_dicts = [dict(zip(column_names, row)) for row in rows]
        await self._store_rows(table_name, row_dicts, candidate.pk_column, candidate.catalog, candidate.schema)
        log.info("Auto-promoted %s to hot cache after query (%d rows)", table_name, len(rows))

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


async def _openapi_list_rows(
    spec: dict,
    base_url: str,
    table_name: str,
    auth_config: dict | None,
    max_rows: int,
) -> list[dict] | None:
    """Find a GET list operation for table_name in the spec and execute it.

    Prefers operations with no required params. For required params that have
    an enum, sends all enum values. Returns None if no suitable operation found.
    """
    import httpx

    definitions = spec.get("definitions", {})
    if "components" in spec:
        definitions = spec.get("components", {}).get("schemas", definitions)

    auth_headers: dict = {}
    if auth_config and auth_config.get("type") == "bearer":
        auth_headers["Authorization"] = f"Bearer {auth_config.get('token', '')}"
    elif auth_config and auth_config.get("type") == "api_key":
        auth_headers[auth_config.get("header_name", "X-API-Key")] = auth_config.get("api_key", "")

    # Score candidate paths: prefer exact /{table_name}, then paths containing it
    candidates: list[tuple[int, str, dict]] = []
    for path, methods in spec.get("paths", {}).items():
        if "get" not in methods:
            continue
        # Skip paths with unresolved path parameters — can't auto-call them
        if "{" in path:
            continue
        path_parts = [p for p in path.split("/") if p]
        if table_name not in path_parts:
            continue
        # Only consider operations that return arrays
        get_op = methods["get"]
        responses = get_op.get("responses", {})
        ok_resp = responses.get("200", responses.get("default", {}))
        content = ok_resp.get("content", {})
        schema: dict = {}
        if "application/json" in content:
            schema = content["application/json"].get("schema", {})
        elif "schema" in ok_resp:
            schema = ok_resp.get("schema", {})
        is_array = schema.get("type") == "array"
        if not is_array and "$ref" not in schema:
            ref = schema.get("items", {}).get("$ref", "")
            if not ref:
                continue
        # Score: fewer path parts = closer match, no required params preferred
        params = get_op.get("parameters", [])
        required_params = [p for p in params if p.get("required") and p.get("in") == "query"]
        score = len(path_parts) * 10 + len(required_params)
        candidates.append((score, path, get_op))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    _, best_path, best_op = candidates[0]

    # Build query params — fill required params with enum values or skip
    params = best_op.get("parameters", [])
    query_params: list[tuple[str, str]] = []
    for p in params:
        if p.get("in") != "query":
            continue
        if not p.get("required"):
            continue
        enum_vals = p.get("schema", p).get("enum", [])
        if enum_vals:
            for v in enum_vals:
                query_params.append((p["name"], str(v)))
        else:
            return None  # required param with no enum — can't auto-fill

    url = base_url + best_path
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=query_params, headers=auth_headers)
        resp.raise_for_status()
        data = resp.json()

    rows = data if isinstance(data, list) else [data]
    return rows[:max_rows + 1]


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

    hot_overrides: dict[str, bool | None] = {}
    for tbl_cfg in raw_config.get("tables", []):
        tbl_name = tbl_cfg.get("table") or tbl_cfg.get("table_name")
        if tbl_name and "hot" in tbl_cfg:
            hot_overrides[tbl_name] = tbl_cfg["hot"]

    _TRINO_BACKED = {"postgresql", "mysql", "mongodb", "elasticsearch", "kafka", "delta", "iceberg"}
    source_cfgs = {s["id"]: s for s in raw_config.get("sources", []) if "id" in s}
    tables_list = raw_config.get("tables", [])
    rels_list = raw_config.get("relationships", [])

    def _tbl_meta(tbl_name: str):
        tbl_cfg = next(
            (t for t in tables_list if (t.get("table") or t.get("table_name")) == tbl_name),
            None,
        )
        if tbl_cfg is None:
            return None, None, None, None, None
        source_id = tbl_cfg.get("source_id", "")
        source_cfg = source_cfgs.get(source_id, {})
        source_type = source_cfg.get("type", "")
        pk_col = tbl_cfg.get("columns", [{}])[0].get("name", "id") if tbl_cfg.get("columns") else "id"
        schema_name = tbl_cfg.get("schema", "public")
        return tbl_cfg, source_id, source_cfg, source_type, pk_col, schema_name

    # Startup: only load tables explicitly marked hot: true
    for tbl_name, override in hot_overrides.items():
        if override is not True:
            continue
        result = _tbl_meta(tbl_name)
        if result[0] is None:
            continue
        _, source_id, source_cfg, source_type, pk_col, schema_name = result
        catalog = source_to_catalog(source_id)
        if source_type == "sqlite":
            await hot_mgr.load_table_from_sqlite(source_cfg, tbl_name, pk_col)
        elif source_type == "openapi":
            await hot_mgr.load_table_from_openapi(source_cfg, tbl_name, pk_col)
        elif source_type in _TRINO_BACKED:
            await hot_mgr.load_table(trino_conn, tbl_name, schema_name, catalog, pk_col)
        else:
            log.debug("hot: true table %s: source type %r not supported for caching", tbl_name, source_type)

    # Register auto-detected candidates for lazy promotion after first query
    auto_candidates = detect_hot_tables(tables_list, rels_list, hot_overrides)
    for tbl_name in auto_candidates:
        if hot_overrides.get(tbl_name) is True:
            continue  # already loaded above
        result = _tbl_meta(tbl_name)
        if result[0] is None:
            continue
        _, source_id, source_cfg, source_type, pk_col, schema_name = result
        catalog = source_to_catalog(source_id)
        hot_mgr.register_candidate(HotTableCandidate(
            table_name=tbl_name,
            pk_column=pk_col,
            catalog=catalog,
            schema=schema_name,
        ))
        log.debug("Registered hot table candidate %s (lazy promotion on first query)", tbl_name)

    return hot_mgr
