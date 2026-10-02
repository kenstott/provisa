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


def _scope_parts() -> tuple[str, int | None]:
    """Where the request is acting (org and environment) and the model that runtime loaded."""
    from provisa.api.app import state
    from provisa.cache.tenancy import cache_place

    return cache_place(state), state.model_stamp


@dataclass
class _Place:
    """One org's one environment in the hot tier: its candidates, and the tables that are hot
    under the model named by ``stamp``."""

    stamp: int | None
    tables: dict[str, HotTableEntry] = field(default_factory=dict)
    candidates: dict[str, HotTableCandidate] = field(default_factory=dict)
    # Names two relations of this model both claimed: the name cannot say whose rows to
    # substitute, so it is never hot.
    ambiguous: set[str] = field(default_factory=set)


class HotTableManager:  # REQ-230, REQ-231, REQ-232, REQ-233, REQ-236, REQ-237, REQ-241
    """Manages small lookup tables cached in Redis for JOIN optimization.

    One manager serves the process, and its Redis serves every process, so nothing here is kept
    by a table's bare name. The registry is per org and environment (:class:`_Place`), and holds
    only what was loaded under the model that runtime currently has: when the model stamp moves,
    the tables loaded under the previous one stop being hot — a table may now read another
    relation — and are promoted again from their candidates by the next small read. A Redis
    blob's key carries the org, the environment, the model stamp and the table's catalog, schema
    and name.

    The remaining limit: callers (the compiler's VALUES-CTE lookup among them) address the tier
    by a table's bare name, so within one model it cannot tell two relations of the same name
    apart. A name two relations have claimed is therefore never hot. Every write to the registry
    goes through :meth:`_store_rows` or :meth:`hold`, which apply that refusal.
    """

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
        self._places: dict[str, _Place] = {}
        # REQ-688: hot-table payloads are encrypted at rest in Redis. Defaults to the
        # platform passthrough (NullEncryption) when no provider is configured; the app
        # injects the configured EncryptionService. Redis ACL isolation (REQ-595) and
        # payload encryption are independent controls.
        self._encryption = encryption or NullEncryption()

    def _place(self) -> _Place:
        """The acting org and environment's part of the registry, emptied of its tables when
        the model it was loaded under is no longer the one the runtime has."""
        where, stamp = _scope_parts()
        place = self._places.get(where)
        if place is None:
            place = self._places[where] = _Place(stamp)
        elif place.stamp != stamp:
            place.tables.clear()
            place.ambiguous.clear()
            place.stamp = stamp
        return place

    @property
    def _hot_tables(self) -> dict[str, HotTableEntry]:
        return self._place().tables

    @_hot_tables.setter
    def _hot_tables(self, tables: dict[str, HotTableEntry]) -> None:
        self._place().tables = tables

    @property
    def _candidates(self) -> dict[str, HotTableCandidate]:
        return self._place().candidates

    @staticmethod
    def _blob_key(table_name: str, catalog: str, schema: str) -> str:
        where, stamp = _scope_parts()
        return f"{HOT_PREFIX}{where}:m{stamp}:{catalog}.{schema}.{table_name}:blob"

    async def _connect(self):
        if self._redis is None:
            from provisa.core.redis_factory import make_redis  # REQ-829

            self._redis = make_redis(self._redis_url, decode_responses=True)

    def _claim(
        self, place: _Place, table_name: str, catalog: str, schema: str
    ) -> tuple[bool, HotTableEntry | None]:
        """Whether ``table_name`` may be hot for the relation ``catalog.schema``, and the entry
        dropped when it may not. It may not once two relations of this model have claimed the
        name: callers address the hot tier by name alone, so serving either's rows would hand
        them to readers of the other. The name then stays cold."""
        held = place.tables.get(table_name)
        if table_name not in place.ambiguous and (
            held is None or (held.catalog, held.schema) == (catalog, schema)
        ):
            return True, None
        place.ambiguous.add(table_name)
        place.tables.pop(table_name, None)
        log.warning("Hot table name %s is claimed by two relations; not cached", table_name)
        return False, held

    def hold(self, entry: HotTableEntry) -> bool:
        """Make ``entry`` the hot rows of its table in this process — for rows a caller has just
        fetched and wants substituted on the next request, without a Redis blob. Returns False,
        holding nothing, when the name is claimed by two relations."""
        place = self._place()
        allowed, _ = self._claim(place, entry.table_name, entry.catalog, entry.schema)
        if allowed:
            place.tables[entry.table_name] = entry
        return allowed

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

        place = self._place()
        allowed, dropped = self._claim(place, table_name, catalog, schema)
        if not allowed:
            if dropped is not None:
                await self._redis.delete(
                    self._blob_key(table_name, dropped.catalog, dropped.schema)
                )
            return len(rows)

        columns = list(rows[0].keys()) if rows else []
        blob_key = self._blob_key(table_name, catalog, schema)

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

        place.tables[table_name] = HotTableEntry(
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
        # The registered catalog.schema.table name, in the bound engine's own table addressing
        # and dialect (REQ-1730: an engine with no catalog level folds it into the schema).
        res = await engine.execute_engine(engine.engine_physical(f"SELECT * FROM {fqn}"))
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

        # The blob is addressed by the relation the name stands for here: a name that is not
        # hot in the acting org, environment and model has no blob to read.
        entry = self._hot_tables.get(table_name)
        if entry is None:
            return []
        data = await self._redis.get(self._blob_key(table_name, entry.catalog, entry.schema))
        if data is None:
            # Check in-memory cache
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
        entry = self._hot_tables.pop(table_name, None)
        if entry is not None:
            await self._redis.delete(self._blob_key(table_name, entry.catalog, entry.schema))
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

    Auto-detection: table is target of a many-to-one or one-to-one relationship (both
    are looked up by the target's own PK, exactly like the many-to-one case).
    hot_overrides: table_name → True (force), False (opt out), None (auto).

    Returns list of table names to cache.
    """
    # Find tables that are targets of many-to-one/one-to-one relationships
    many_to_one_targets: set[str] = set()
    for rel in relationships:
        if rel.get("cardinality") in ("many-to-one", "one-to-one"):
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
    # In the bound engine's own table addressing and dialect — see HotTableManager.load_table.
    res = await engine.execute_engine(engine.engine_physical(f"SELECT COUNT(*) FROM {fqn}"))
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


def refresh_interval() -> int:
    """How often the hot tier refreshes (REQ-231): its own interval when one is set, else the
    materialized-view default TTL. Both are operator settings (REQ-1913)."""
    from provisa.core import settings_registry

    own = settings_registry.value("hot_tables.refresh_interval")
    return own if own is not None else settings_registry.value("materialized_views.default_ttl")


def max_rows() -> int:
    """The hot tier's row ceiling (REQ-230): its own when one is set, else its auto threshold."""
    from provisa.core import settings_registry

    own = settings_registry.value("hot_tables.max_rows")
    return own if own is not None else settings_registry.value("hot_tables.auto_threshold")


async def init_hot_tables(  # REQ-230, REQ-231, REQ-236, REQ-237
    raw_config: dict,
    engine,
) -> HotTableManager | None:
    """Initialize hot table manager from raw config. Returns manager or None."""

    # REQ-1913: the tier's settings are operator settings, resolved by the settings registry.
    from provisa.core import settings_registry
    from provisa.core.redis_location import redis_url as _redis_url

    # REQ-829: with cache enabled but no Redis URL, run hot tables on embedded
    # fakeredis (redis_url=None) so desktop exercises the same hot-cache path.
    if not settings_registry.value("cache.enabled"):  # default on; set enabled: false to opt out
        return None
    redis_url = _redis_url()

    auto_threshold = settings_registry.value("hot_tables.auto_threshold")
    max_bytes = settings_registry.value("hot_tables.max_bytes")
    # REQ-688/684: build the configured EncryptionService (encryption.provider/key_id);
    # unset provider → NullEncryption passthrough (platform default).
    from provisa.encryption import build_encryption_service  # noqa: PLC0415

    _enc_cfg = raw_config.get("encryption", {}) or {}
    _enc_provider = _enc_cfg.get("provider")
    encryption = build_encryption_service(
        _enc_provider,
        key_id=_enc_cfg.get("key_id"),
        config=_enc_cfg.get(_enc_provider, {}) if _enc_provider else {},
    )
    hot_mgr = HotTableManager(
        redis_url=redis_url,
        auto_threshold=auto_threshold,
        max_rows=max_rows(),
        ttl=refresh_interval(),
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
        # Also a candidate: what is loaded here is hot only under the model loaded now, and a
        # candidate is how the table becomes hot again after the model changes.
        hot_mgr.register_candidate(
            HotTableCandidate(
                table_name=tbl_name, pk_column=pk_col, catalog=catalog, schema=schema_name
            )
        )
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
