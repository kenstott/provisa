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

import base64
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from provisa.compiler.naming import source_to_catalog

if TYPE_CHECKING:
    from provisa.encryption import EncryptionService

log = logging.getLogger(__name__)

# Requirements: REQ-230, REQ-231, REQ-232, REQ-233, REQ-236, REQ-237, REQ-241


class _HotEncoder(json.JSONEncoder):
    """Handle the engine types that aren't natively JSON serializable."""

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
_HTTP_NOT_FOUND = 404


def _sql_literal(val) -> str:
    """Render a Python value as a SQL literal (the engine-compatible)."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float, Decimal)):
        return str(val)
    if isinstance(val, datetime):
        return f"TIMESTAMP '{val.isoformat()}'"
    if isinstance(val, date):
        return f"DATE '{val}'"
    if isinstance(val, (dict, list)):
        escaped = json.dumps(val).replace("'", "''")
        return f"'{escaped}'"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def build_values_cte_sql(
    sql: str, table_name: str, entry: "HotTableEntry"
) -> str:  # REQ-232, REQ-233
    """Replace the first table reference matching table_name with a VALUES CTE.

    Works for both FROM and JOIN targets. Merges with any existing WITH clause.

    REQ-233: hot rows are injected verbatim into the CTE on purpose. Column governance
    (RLS / masking / visibility) is applied by Stage 2 (`apply_governance`) to the governed
    SQL that wraps this CTE — the pipeline order is governance → cache/CTE → route — so
    governance filters and masks the CTE rows exactly as it would the live table. Storing
    governed-per-role copies in Redis would be both redundant and a leak risk, so the cache
    holds the raw rows and governance stays at query time.

    REQ-913: structural, AST-only. The table reference is renamed and the CTE is attached
    to the query's ``WITH`` node on the parsed tree — the injection point is never derived
    from SQL text via regex. The CTE definition is built as a self-contained fragment and
    parsed to an AST node (IR construction, not re-deriving the query's structure from text).
    """
    import sqlglot
    import sqlglot.expressions as exp

    if not entry.column_names:
        return sql

    cte_name = f"_hot_{table_name}"
    col_defs = ", ".join(f'"{c}"' for c in entry.column_names)

    if not entry.rows:
        empty_nulls = ", ".join("NULL" for _ in entry.column_names)
        cte_body = f"({col_defs}) AS (SELECT {empty_nulls} WHERE 1=0)"
    else:
        value_rows = [
            "(" + ", ".join(_sql_literal(row.get(c)) for c in entry.column_names) + ")"
            for row in entry.rows
        ]
        cte_body = f"({col_defs}) AS (VALUES {', '.join(value_rows)})"

    cte_sql = f'"{cte_name}"{cte_body}'

    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception as exc:
        # A regex fallback can rewrite the wrong occurrence — fail loud on parse error.
        raise ValueError(
            f"Failed to parse SQL for hot-table CTE rewrite of {table_name!r}"
        ) from exc

    for tbl in tree.find_all(exp.Table):
        if tbl.name == table_name:
            # Preserve the original table name as an alias when the ref is
            # unaliased, so column qualifiers (e.g. shelter__animalBreeds.name)
            # still resolve after the relation is renamed to the CTE.
            if not tbl.alias:
                tbl.set("alias", exp.TableAlias(this=exp.to_identifier(table_name)))
            tbl.set("catalog", None)
            tbl.set("db", None)
            tbl.set("this", exp.to_identifier(cte_name, quoted=True))

    # Parse the CTE definition (a self-contained fragment we constructed) into an AST node
    # and attach it to the query's WITH — the injection point is chosen structurally, never
    # by matching a leading "WITH" in the query text.
    cte_fragment = sqlglot.parse_one(f"WITH {cte_sql} SELECT 1", read="postgres")
    cte_node = cte_fragment.args["with_"].expressions[0]
    existing_with = tree.args.get("with_")
    if existing_with is not None:
        existing_with.set("expressions", [cte_node, *existing_with.expressions])
    else:
        tree.set("with_", exp.With(expressions=[cte_node]))
    return tree.sql(dialect="postgres")


@dataclass
class HotTableEntry:  # REQ-230, REQ-232
    """Metadata for a single hot-cached table."""

    table_name: str
    catalog: str
    schema: str
    pk_column: str
    rows: list[dict] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)
    is_api: bool = False


@dataclass
class HotTableCandidate:  # REQ-236, REQ-237
    """Metadata for a table that should be auto-promoted after its first small query."""

    table_name: str
    pk_column: str
    catalog: str
    schema: str


class HotTableManager:  # REQ-230, REQ-231, REQ-232, REQ-233, REQ-236, REQ-237, REQ-241
    """Manages small lookup tables cached in Redis for JOIN optimization."""

    def __init__(
        self,
        redis_url: str | None,  # REQ-829: None => embedded fakeredis
        auto_threshold: int,
        max_rows: int,
        ttl: int = 300,
        max_bytes: int = 10 * 1024 * 1024,
        encryption: "EncryptionService | None" = None,  # REQ-688
    ):
        from provisa.encryption import NullEncryption  # REQ-688

        self._redis_url = redis_url
        self._auto_threshold = auto_threshold
        self._max_rows = max_rows
        self._max_bytes = max_bytes  # REQ-230: serialized blob ceiling (default 10 MB)
        self._ttl = ttl
        self._redis = None
        self._hot_tables: dict[str, HotTableEntry] = {}
        self._candidates: dict[str, HotTableCandidate] = {}
        # REQ-688: hot-table payloads are encrypted at rest in Redis. Defaults to the
        # platform passthrough (NullEncryption) when no provider is configured; the app
        # injects the configured EncryptionService. Redis ACL isolation (REQ-595) and
        # payload encryption are independent controls.
        self._encryption = encryption or NullEncryption()

    async def _connect(self):
        if self._redis is None:
            from provisa.core.redis_factory import make_redis  # REQ-829

            self._redis = make_redis(self._redis_url, decode_responses=True)

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
        assert self._redis is not None

        columns = list(rows[0].keys()) if rows else []
        blob_key = HOT_PREFIX + table_name + ":blob"

        # REQ-230: measure the serialized blob and skip caching a table that exceeds the byte
        # ceiling, even when its row count is within max_rows (wide rows can still be large).
        blob = _dumps(rows)
        blob_bytes = len(blob.encode("utf-8"))
        if blob_bytes > self._max_bytes:
            log.warning(
                "Hot table %s is %d bytes (max %d), skipping",
                table_name,
                blob_bytes,
                self._max_bytes,
            )
            return len(rows)

        # REQ-688: encrypt the payload at rest. Ciphertext is base64-wrapped so it stays
        # a string under the existing key scheme (the client decodes responses).
        stored = base64.b64encode(self._encryption.encrypt(blob.encode("utf-8"))).decode("ascii")
        pipe = self._redis.pipeline()
        pipe.delete(blob_key)
        pipe.set(blob_key, stored, ex=self._ttl)
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

    async def load_table(  # REQ-544
        self,
        engine,
        table_name: str,
        schema: str,
        catalog: str,
        pk_column: str,
    ) -> int:
        """Load an engine-backed table into Redis vithe engine terminal. Returns row count."""
        fqn = f'"{catalog}"."{schema}"."{table_name}"'
        res = await engine.execute_engine(f"SELECT * FROM {fqn}")
        rows_raw = res.rows
        columns = res.column_names

        row_count = len(rows_raw)
        if row_count > self._max_rows:
            log.warning(
                "Hot table %s has %d rows (max %d), skipping", table_name, row_count, self._max_rows
            )
            return row_count

        rows = [dict(zip(columns, row)) for row in rows_raw]
        return await self._store_rows(table_name, rows, pk_column, catalog, schema)

    async def load_table_from_sqlite(  # REQ-544
        self,
        source_cfg: dict,
        table_name: str,
        pk_column: str,
    ) -> int:
        """Load a SQLite table into Redis. Returns row count."""
        from provisa.file_source.source import FileSourceConfig, execute_query

        path = source_cfg.get("path", "")
        cfg = FileSourceConfig(id=source_cfg["id"], source_type="sqlite", path=path)
        rows = execute_query(cfg, f'SELECT * FROM "{table_name}" LIMIT {self._max_rows + 1}')  # noqa: S608

        if len(rows) > self._max_rows:
            log.info(
                "Skipping hot table %s: %d rows > threshold %d",
                table_name,
                len(rows),
                self._max_rows,
            )
            return len(rows)

        return await self._store_rows(table_name, rows, pk_column, source_cfg["id"], "default")

    async def load_table_from_openapi(  # REQ-544
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

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                spec_resp = await client.get(spec_url)
                spec_resp.raise_for_status()
                spec = spec_resp.json()
        except (httpx.HTTPError, OSError, ValueError) as _e:
            # httpx.HTTPError: request/status failures; OSError: socket; ValueError: bad JSON.
            log.warning("OpenAPI spec fetch failed for %s: %s", table_name, _e)
            return 0

        rows = await _openapi_list_rows(spec, base_url, table_name, auth_config, self._max_rows)
        if rows is None:
            log.info(
                "No list operation found for %s in OpenAPI spec — skipping hot cache", table_name
            )
            return 0

        if len(rows) > self._max_rows:
            log.info(
                "Skipping hot table %s: %d rows > threshold %d",
                table_name,
                len(rows),
                self._max_rows,
            )
            return len(rows)

        return await self._store_rows(table_name, rows, pk_column, source_cfg["id"], "default")

    async def get_rows(self, table_name: str) -> list[dict]:  # REQ-544
        """Fetch all rows for a hot table from Redis."""
        await self._connect()
        assert self._redis is not None

        blob_key = HOT_PREFIX + table_name + ":blob"
        data = await self._redis.get(blob_key)
        if data is None:
            # Check in-memory cache
            entry = self._hot_tables.get(table_name)
            if entry:
                return entry.rows
            # REQ-231: a cache miss returns no rows rather than raising — the caller falls
            # back to the live source. (CTE injection is gated on is_hot()/get_entry(), so an
            # evicted/expired hot table is simply queried live; this is the structural fallback.)
            return []
        # REQ-688: decrypt the at-rest payload (base64-wrapped ciphertext → JSON).
        blob = self._encryption.decrypt(base64.b64decode(data)).decode("utf-8")
        return json.loads(blob)

    async def invalidate(self, table_name: str) -> None:  # REQ-544
        """Delete all Redis keys for a hot table."""
        await self._connect()
        assert self._redis is not None
        blob_key = HOT_PREFIX + table_name + ":blob"
        await self._redis.delete(blob_key)
        self._hot_tables.pop(table_name, None)
        log.info("Hot table %s invalidated", table_name)

    def is_hot(self, table_name: str) -> bool:  # REQ-544
        """Check if a table is currently hot-cached with at least one row."""
        entry = self._hot_tables.get(table_name)
        return entry is not None and len(entry.rows) > 0

    def managed_tables(self) -> set[str]:
        """REQ-241: names of tables owned by the hot tier (loaded or candidate).

        Used for hot-over-warm precedence — a table the hot tier manages must not also be
        promoted to the warm tier.
        """
        return set(self._hot_tables) | set(self._candidates)

    def get_entry(self, table_name: str) -> HotTableEntry | None:  # REQ-544
        """Get the hot table entry with metadata."""
        return self._hot_tables.get(table_name)

    def snapshot(self) -> list[dict]:
        """Admin view of the hot tier: loaded tables and not-yet-loaded candidates.

        Each entry: table_name, catalog, schema, row_count, is_api, loaded.
        """
        out: list[dict] = []
        for name, e in self._hot_tables.items():
            out.append(
                {
                    "table_name": name,
                    "catalog": e.catalog,
                    "schema": e.schema,
                    "row_count": len(e.rows),
                    "is_api": e.is_api,
                    "loaded": True,
                }
            )
        for name, c in self._candidates.items():
            if name in self._hot_tables:
                continue
            out.append(
                {
                    "table_name": name,
                    "catalog": c.catalog,
                    "schema": c.schema,
                    "row_count": 0,
                    "is_api": False,
                    "loaded": False,
                }
            )
        return out

    def register_candidate(self, candidate: HotTableCandidate) -> None:  # REQ-236, REQ-237
        """Register a table as an auto-promotion candidate."""
        self._candidates[candidate.table_name] = candidate

    async def maybe_promote(
        self,
        table_name: str,
        rows: list[tuple],
        column_names: list[str],
    ) -> None:  # REQ-236
        """Promote table to hot cache if it's a candidate and result is small enough."""
        if self.is_hot(table_name):
            return
        candidate = self._candidates.get(table_name)
        if candidate is None:
            return
        if len(rows) > self._auto_threshold:
            log.debug(
                "Hot table candidate %s: %d rows > threshold %d, skipping",
                table_name,
                len(rows),
                self._auto_threshold,
            )
            return
        row_dicts = [dict(zip(column_names, row)) for row in rows]
        await self._store_rows(
            table_name, row_dicts, candidate.pk_column, candidate.catalog, candidate.schema
        )
        log.info("Auto-promoted %s to hot cache after query (%d rows)", table_name, len(rows))

    async def maybe_promote_dicts(self, table_name: str, rows: list[dict]) -> None:  # REQ-236
        """Promote table to hot cache from already-fetched dict rows (API sources)."""
        if self.is_hot(table_name):
            return
        candidate = self._candidates.get(table_name)
        if candidate is None:
            return
        if len(rows) > self._auto_threshold:
            log.debug(
                "Hot table candidate %s: %d rows > threshold %d, skipping",
                table_name,
                len(rows),
                self._auto_threshold,
            )
            return
        await self._store_rows(
            table_name, rows, candidate.pk_column, candidate.catalog, candidate.schema
        )
        log.info("Auto-promoted %s to hot cache after API query (%d rows)", table_name, len(rows))

    @property
    def auto_threshold(self) -> int:
        return self._auto_threshold

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None


def detect_hot_tables(  # REQ-236, REQ-237
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
    query_params: list[tuple[str, str | int | float | bool | None]] = []
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
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, params=query_params, headers=auth_headers)
            if resp.status_code == _HTTP_NOT_FOUND:
                return None
            resp.raise_for_status()
            data = resp.json()
    except (httpx.HTTPError, OSError, ValueError) as _e:
        # httpx.HTTPError: request/status failures; OSError: socket; ValueError: bad JSON body.
        log.warning("OpenAPI list rows failed for %s: %s", url, _e)
        return None

    rows = data if isinstance(data, list) else [data]
    return rows[: max_rows + 1]


async def count_table_rows(engine, table_name: str, schema: str, catalog: str) -> int:  # REQ-544
    """SELECT COUNT(*) for auto-detection sizing, through the engine terminal."""
    fqn = f'"{catalog}"."{schema}"."{table_name}"'
    res = await engine.execute_engine(f"SELECT COUNT(*) FROM {fqn}")
    return res.rows[0][0] if res.rows else 0


async def detect_hot_tables_by_count(  # REQ-236
    engine,
    candidates: list[tuple[str, str, str]],
    auto_threshold: int,
    hot_overrides: dict[str, bool | None],
) -> list[str]:
    """REQ-236 criterion (1): a table whose row count is at/below ``auto_threshold`` is hot.

    candidates: list of (table_name, schema, catalog) for the engine-backed tables to size.
    Opt-outs (hot: false) are skipped; COUNT(*) failures are tolerated (table left non-hot).
    Returns the table names that qualify by row count.
    """
    result: list[str] = []
    for table_name, schema, catalog in candidates:
        if hot_overrides.get(table_name) is False:
            continue
        try:
            count = await count_table_rows(engine, table_name, schema, catalog)
        # complexity-gate: allow-ble=1 reason="Best-effort hot-table auto-detection over a pluggable engine backend (Trino/DuckDB/…) whose COUNT(*) failure taxonomy is unbounded — any failure just leaves this one table non-hot and is logged; it must not abort sizing of the remaining candidates."
        except Exception:
            log.debug("COUNT(*) failed for hot-detect of %s; leaving non-hot", table_name)
            continue
        if 0 < count <= auto_threshold:
            result.append(table_name)
    return result


async def init_hot_tables(  # REQ-230, REQ-231, REQ-236, REQ-237
    raw_config: dict,
    engine,
) -> HotTableManager | None:
    """Initialize hot table manager from raw config. Returns manager or None."""

    hot_config = raw_config.get("hot_tables", {})
    cache_config = raw_config.get("cache", {})
    redis_url = cache_config.get("redis_url", "")
    if redis_url:
        from provisa.core.secrets import resolve_secrets

        redis_url = resolve_secrets(redis_url)
    # REQ-829: with cache enabled but no Redis URL, run hot tables on embedded
    # fakeredis (redis_url=None) so desktop exercises the same hot-cache path.
    if not cache_config.get("enabled", True):  # default on; set enabled: false to opt out
        return None
    redis_url = redis_url or None

    auto_threshold = hot_config.get("auto_threshold", 1_000)
    # REQ-231: hot TTL defaults to the materialized-views default TTL when not set explicitly.
    mv_default_ttl = raw_config.get("materialized_views", {}).get("default_ttl", 300)
    refresh_interval = hot_config.get("refresh_interval", mv_default_ttl)
    # REQ-230: max_rows has its own default (falls back to auto_threshold) and a byte ceiling.
    max_rows = hot_config.get("max_rows", auto_threshold)
    max_bytes = hot_config.get("max_bytes", 10 * 1024 * 1024)
    # REQ-688/684: build the configured EncryptionService (encryption.provider/key_id);
    # unset provider → NullEncryption passthrough (platform default).
    from provisa.encryption import build_encryption_service  # noqa: PLC0415

    _enc_cfg = raw_config.get("encryption", {}) or {}
    encryption = build_encryption_service(_enc_cfg.get("provider"), key_id=_enc_cfg.get("key_id"))
    hot_mgr = HotTableManager(
        redis_url=redis_url,
        auto_threshold=auto_threshold,
        max_rows=max_rows,
        ttl=refresh_interval,
        max_bytes=max_bytes,
        encryption=encryption,
    )

    hot_overrides: dict[str, bool | None] = {}
    for tbl_cfg in raw_config.get("tables", []):
        tbl_name = tbl_cfg.get("table") or tbl_cfg.get("table_name")
        if tbl_name and "hot" in tbl_cfg:
            hot_overrides[tbl_name] = tbl_cfg["hot"]

    _ENGINE_BACKED = {
        "postgresql",
        "mysql",
        "mongodb",
        "elasticsearch",
        "kafka",
        "delta",
        "iceberg",
    }
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
        pk_col = (
            tbl_cfg.get("columns", [{}])[0].get("name", "id") if tbl_cfg.get("columns") else "id"
        )
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
        elif source_type in _ENGINE_BACKED:
            await hot_mgr.load_table(engine, tbl_name, schema_name, catalog, pk_col)
        else:
            log.debug(
                "hot: true table %s: source type %r not supported for caching",
                tbl_name,
                source_type,
            )

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
        hot_mgr.register_candidate(
            HotTableCandidate(
                table_name=tbl_name,
                pk_column=pk_col,
                catalog=catalog,
                schema=schema_name,
            )
        )
        log.debug("Registered hot table candidate %s (lazy promotion on first query)", tbl_name)

    # REQ-236 criterion (1): also size small the engine-backed tables by COUNT(*) and register
    # those at/below auto_threshold as candidates. Skip ones already handled above.
    already = set(auto_candidates) | {n for n, o in hot_overrides.items() if o is True}
    count_candidates: list[tuple[str, str, str]] = []
    count_meta: dict[str, tuple] = {}
    for tbl_cfg in tables_list:
        tbl_name = tbl_cfg.get("table") or tbl_cfg.get("table_name")
        if not tbl_name or tbl_name in already:
            continue
        result = _tbl_meta(tbl_name)
        if result[0] is None or result[3] not in _ENGINE_BACKED:
            continue
        _source_cfg, source_id, _source_type, _, pk_col, schema_name = result  # pyright: ignore[reportUnusedVariable]
        catalog = source_to_catalog(source_id)
        count_candidates.append((tbl_name, schema_name, catalog))
        count_meta[tbl_name] = (pk_col, catalog, schema_name)

    for tbl_name in await detect_hot_tables_by_count(
        engine, count_candidates, auto_threshold, hot_overrides
    ):
        pk_col, catalog, schema_name = count_meta[tbl_name]
        hot_mgr.register_candidate(
            HotTableCandidate(
                table_name=tbl_name, pk_column=pk_col, catalog=catalog, schema=schema_name
            )
        )
        log.debug("Registered hot table candidate %s by row-count (REQ-236)", tbl_name)

    return hot_mgr
