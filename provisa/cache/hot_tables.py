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
from collections.abc import Iterable
from typing import TYPE_CHECKING

from provisa.federation.execution_auth import system_auth

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
    """One hot table's rows, under its registered table's id. ``table_name`` is the name the
    table's references in a statement's SQL carry — what substitution replaces."""

    table_id: int
    table_name: str
    catalog: str
    schema: str
    pk_column: str
    rows: list[dict] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)
    is_api: bool = False


@dataclass
class HotTableCandidate:  # REQ-236, REQ-237
    """A registered table that should be auto-promoted after its first small read."""

    table_id: int
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
    under the model named by ``stamp``, each by its registered table's id."""

    stamp: int | None
    tables: dict[int, HotTableEntry] = field(default_factory=dict)
    candidates: dict[int, HotTableCandidate] = field(default_factory=dict)
    # Candidates counted above the auto threshold under this model: not counted again until
    # the model changes.
    too_large: set[int] = field(default_factory=set)


class HotTableManager:  # REQ-230, REQ-231, REQ-232, REQ-233, REQ-236, REQ-237, REQ-241
    """Manages small lookup tables cached in Redis for JOIN optimization.

    One manager serves the process, and its Redis serves every process. The registry is per org
    and environment (:class:`_Place`), keyed by the registered table's id, and holds only what was
    loaded under the model that runtime currently has: when the model stamp moves, the tables
    loaded under the previous one stop being hot — a table may now read another relation — and
    are promoted again from their candidates by the next small read. A Redis blob's key carries
    the org, the environment, the model stamp and the table id.

    A statement is given the hot rows of the tables it reads (:meth:`entries_for`, over the ids
    the pipeline resolved for it), keyed by the name its SQL carries for each; two sources'
    same-named tables are each hot, and each is served to the statements that read it.
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
            place.too_large.clear()
            place.stamp = stamp
        return place

    @property
    def _hot_tables(self) -> dict[int, HotTableEntry]:
        return self._place().tables

    @property
    def _candidates(self) -> dict[int, HotTableCandidate]:
        return self._place().candidates

    @staticmethod
    def _blob_key(table_id: int) -> str:
        where, stamp = _scope_parts()
        return f"{HOT_PREFIX}{where}:m{stamp}:t{table_id}:blob"

    async def _connect(self):
        if self._redis is None:
            from provisa.core.redis_factory import make_redis  # REQ-829

            self._redis = make_redis(self._redis_url, decode_responses=True)

    def hold(self, entry: HotTableEntry) -> None:
        """Make ``entry`` the hot rows of its table in this process — for rows a caller has just
        fetched and wants substituted on the next request, without a Redis blob."""
        self._place().tables[entry.table_id] = entry

    async def _store_rows(
        self,
        table_id: int,
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
        columns = list(rows[0].keys()) if rows else []
        blob_key = self._blob_key(table_id)

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

        place.tables[table_id] = HotTableEntry(
            table_id=table_id,
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
        table_id: int,
        table_name: str,
        schema: str,
        catalog: str,
        pk_column: str,
    ) -> int:
        """Load an engine-backed table into Redis through the engine terminal. Returns row count."""
        fqn = f'"{catalog}"."{schema}"."{table_name}"'
        # The registered catalog.schema.table name, in the bound engine's own table addressing
        # and dialect (REQ-1730: an engine with no catalog level folds it into the schema).
        res = await engine.execute_engine(
            engine.engine_physical(f"SELECT * FROM {fqn}"),
            authorization=system_auth("hot-table load"),
        )
        rows_raw = res.rows
        columns = res.column_names

        row_count = len(rows_raw)
        if row_count > self._max_rows:
            log.warning(
                "Hot table %s has %d rows (max %d), skipping", table_name, row_count, self._max_rows
            )
            return row_count

        rows = [dict(zip(columns, row)) for row in rows_raw]
        return await self._store_rows(table_id, table_name, rows, pk_column, catalog, schema)

    async def load_table_from_sqlite(  # REQ-544
        self,
        source_cfg: dict,
        table_id: int,
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

        return await self._store_rows(
            table_id, table_name, rows, pk_column, source_cfg["id"], "default"
        )

    async def load_table_from_openapi(  # REQ-544, REQ-316
        self,
        endpoint,
        api_source,
        table_id: int,
        table_name: str,
        pk_column: str,
    ) -> int:
        """Load an OpenAPI table into Redis, read as every read of it is: through its endpoint
        and the one caller, with its source's stored address, credential and headers and its
        own paging. Returns row count."""
        from provisa.api_source.caller import answer_rows, call_api

        answer = await call_api(
            endpoint,
            dict(endpoint.default_params),
            base_url=api_source.base_url,
            auth=api_source.auth,
            source_headers=api_source.headers,
        )
        rows, cut = answer_rows(endpoint, answer)
        if cut is not None or len(rows) > self._max_rows:
            # More than the tier holds, or more than one read of the table takes: not hot.
            log.info(
                "Skipping hot table %s: %d rows read%s, threshold %d",
                table_name,
                len(rows),
                "" if cut is None else " and the endpoint had more",
                self._max_rows,
            )
            return len(rows)

        return await self._store_rows(
            table_id, table_name, rows, pk_column, endpoint.source_id, "default"
        )

    async def get_rows(self, table_id: int) -> list[dict]:  # REQ-544
        """Fetch all rows for a hot table from Redis."""
        await self._connect()
        assert self._redis is not None

        # A table that is not hot in the acting org, environment and model has no blob to read.
        entry = self._hot_tables.get(table_id)
        if entry is None:
            return []
        data = await self._redis.get(self._blob_key(table_id))
        if data is None:
            # REQ-231: the blob expired or was never written (rows a caller held): the rows
            # this process holds are the table's hot rows.
            return entry.rows
        # REQ-688: decrypt the at-rest payload (base64-wrapped ciphertext → JSON).
        blob = self._encryption.decrypt(base64.b64decode(data)).decode("utf-8")
        return json.loads(blob)

    async def invalidate(self, table_id: int) -> None:  # REQ-544
        """Delete a hot table's blob and its rows in the acting org and environment."""
        await self._connect()
        assert self._redis is not None
        entry = self._hot_tables.pop(table_id, None)
        await self._redis.delete(self._blob_key(table_id))
        log.info("Hot table %s invalidated", entry.table_name if entry else table_id)

    async def refresh_after_write(self, engine, table_id: int) -> None:  # REQ-544
        """A write changed ``table_id``: its hot rows are dropped and, for a table the engine
        reads, loaded again with the key and address it was hot under. A table not hot here is
        left alone."""
        entry = self._hot_tables.get(table_id)
        if entry is None:
            return
        await self.invalidate(table_id)
        if not entry.is_api:
            await self.load_table(
                engine, table_id, entry.table_name, entry.schema, entry.catalog, entry.pk_column
            )

    def is_hot(self, table_id: int) -> bool:  # REQ-544
        """Check if a table is currently hot-cached with at least one row."""
        entry = self._hot_tables.get(table_id)
        return entry is not None and len(entry.rows) > 0

    def get_entry(self, table_id: int) -> HotTableEntry | None:  # REQ-544
        """Get the hot table entry with metadata."""
        return self._hot_tables.get(table_id)

    def entries_for(self, table_ids: Iterable[int]) -> dict[str, HotTableEntry]:
        """The hot rows a statement that reads ``table_ids`` (the ids the pipeline resolved for
        it) substitutes, keyed by the name its SQL carries for each table. A name two of those
        tables carry is left out: substitution goes by name in the statement, which cannot say
        which reference is which."""
        by_name: dict[str, list[HotTableEntry]] = {}
        hot = self._hot_tables
        for table_id in set(table_ids):
            entry = hot.get(table_id)
            if entry is not None and entry.rows:
                by_name.setdefault(entry.table_name, []).append(entry)
        return {name: found[0] for name, found in by_name.items() if len(found) == 1}

    def managed_tables(self) -> set[int]:
        """REQ-241: ids of tables owned by the hot tier (loaded or candidate)."""
        return set(self._hot_tables) | set(self._candidates)

    def snapshot(self) -> list[dict]:
        """Admin view of the hot tier: loaded tables and not-yet-loaded candidates.

        Each entry: table_id, table_name, catalog, schema, row_count, is_api, loaded.
        """
        out: list[dict] = []
        for table_id, e in self._hot_tables.items():
            out.append(
                {
                    "table_id": table_id,
                    "table_name": e.table_name,
                    "catalog": e.catalog,
                    "schema": e.schema,
                    "row_count": len(e.rows),
                    "is_api": e.is_api,
                    "loaded": True,
                }
            )
        for table_id, c in self._candidates.items():
            if table_id in self._hot_tables:
                continue
            out.append(
                {
                    "table_id": table_id,
                    "table_name": c.table_name,
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
        self._candidates[candidate.table_id] = candidate

    async def promote_on_read(self, engine, table_id: int) -> None:  # REQ-236
        """A statement read candidate ``table_id``: make it hot when the whole table is small
        enough. What the statement read is never the hot rows — it may be filtered, limited or
        projected — so the table is counted and, when at or below the auto threshold, loaded
        whole. A table counted above it is not counted again under this model."""
        if self.is_hot(table_id):
            return
        place = self._place()
        candidate = place.candidates.get(table_id)
        if candidate is None or table_id in place.too_large:
            return
        count = await count_table_rows(
            engine, candidate.table_name, candidate.schema, candidate.catalog
        )
        if count > self._auto_threshold:
            place.too_large.add(table_id)
            return
        await self.load_table(
            engine,
            table_id,
            candidate.table_name,
            candidate.schema,
            candidate.catalog,
            candidate.pk_column,
        )

    async def maybe_promote_dicts(self, table_id: int, rows: list[dict]) -> None:  # REQ-236
        """Promote a table to the hot cache from rows a caller fetched — which must be all of
        the table's rows (an API resource fetched with no arguments)."""
        if self.is_hot(table_id):
            return
        candidate = self._candidates.get(table_id)
        if candidate is None:
            return
        if len(rows) > self._auto_threshold:
            log.debug(
                "Hot table candidate %s: %d rows > threshold %d, skipping",
                candidate.table_name,
                len(rows),
                self._auto_threshold,
            )
            return
        await self._store_rows(
            table_id,
            candidate.table_name,
            rows,
            candidate.pk_column,
            candidate.catalog,
            candidate.schema,
        )
        log.info("Auto-promoted %s to hot cache (%d rows)", candidate.table_name, len(rows))

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


async def count_table_rows(engine, table_name: str, schema: str, catalog: str) -> int:  # REQ-544
    """SELECT COUNT(*) for auto-detection sizing, through the engine terminal."""
    fqn = f'"{catalog}"."{schema}"."{table_name}"'
    # In the bound engine's own table addressing and dialect — see HotTableManager.load_table.
    res = await engine.execute_engine(
        engine.engine_physical(f"SELECT COUNT(*) FROM {fqn}"),
        authorization=system_auth("hot-table load"),
    )
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
    registered: list[dict],
    api_endpoints: dict,
    api_sources: dict,
) -> HotTableManager | None:
    """Initialize hot table manager from raw config. Returns manager or None. ``registered`` are
    the registered tables (``state.tables``); each config table is found among them by its
    identity (source, schema, table) and kept under its id. ``api_endpoints`` and
    ``api_sources`` are the loaded ones (``state``'s), which an OpenAPI table is read through."""

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
    rels_list = raw_config.get("relationships", [])
    # Each config table is one registered table: the hot tier keys it by that table's id.
    ids = {
        (row["source_id"], row["schema_name"], row["table_name"]): int(row["id"])
        for row in registered
    }

    from provisa.compiler.naming import apply_sql_name

    for tbl_cfg in raw_config.get("tables", []):
        declared = tbl_cfg.get("table") or tbl_cfg.get("table_name")
        schema_name = tbl_cfg.get("schema") or tbl_cfg.get("schema_name")
        source_id = tbl_cfg["source_id"]
        # REQ-471: a table on a source the engine cannot attach is registered under its settled
        # SQL name (config_loader._settle_table_names); an attached one under the name declared.
        found = [
            name
            for name in dict.fromkeys((declared, apply_sql_name(declared)))
            if (source_id, schema_name, name) in ids
        ]
        if len(found) != 1:
            raise ValueError(
                f"hot tables: config table {source_id}/{schema_name}.{declared} is "
                + ("not registered" if not found else f"registered twice ({', '.join(found)})")
            )
        tbl_name = found[0]
        table_id = ids[(source_id, schema_name, tbl_name)]
        source_cfg = source_cfgs.get(source_id, {})
        source_type = source_cfg.get("type", "")
        pk_col = (
            tbl_cfg.get("columns", [{}])[0].get("name", "id") if tbl_cfg.get("columns") else "id"
        )
        catalog = source_to_catalog(source_id)
        override = tbl_cfg.get("hot")
        candidate = HotTableCandidate(
            table_id=table_id,
            table_name=tbl_name,
            pk_column=pk_col,
            catalog=catalog,
            schema=schema_name,
        )

        if override is True:
            # Loaded now, and also a candidate: what is loaded here is hot only under the model
            # loaded now, and a candidate is how the table becomes hot again after it changes.
            hot_mgr.register_candidate(candidate)
            if source_type == "sqlite":
                await hot_mgr.load_table_from_sqlite(source_cfg, table_id, tbl_name, pk_col)
            elif source_type == "openapi":
                await hot_mgr.load_table_from_openapi(
                    api_endpoints[(source_id, tbl_name)],
                    api_sources[source_id],
                    table_id,
                    tbl_name,
                    pk_col,
                )
            elif source_type in _ENGINE_BACKED:
                await hot_mgr.load_table(engine, table_id, tbl_name, schema_name, catalog, pk_col)
            else:
                log.debug(
                    "hot: true table %s: source type %r not supported for caching",
                    tbl_name,
                    source_type,
                )
            continue
        if override is False:
            continue
        # Auto-detected: a many-to-one target, promoted on its first small read.
        if detect_hot_tables([tbl_cfg], rels_list, {declared: override}):
            hot_mgr.register_candidate(candidate)
            log.debug("Registered hot table candidate %s (lazy promotion on first query)", tbl_name)
            continue
        # REQ-236 criterion (1): an engine-backed table at/below auto_threshold rows.
        if source_type in _ENGINE_BACKED and await detect_hot_tables_by_count(
            engine, [(tbl_name, schema_name, catalog)], auto_threshold, {}
        ):
            hot_mgr.register_candidate(candidate)
            log.debug("Registered hot table candidate %s by row-count (REQ-236)", tbl_name)

    return hot_mgr
