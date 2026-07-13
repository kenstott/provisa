# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""FastAPI app factory with startup hooks for config load and schema generation."""

# Requirements: REQ-012, REQ-016, REQ-057, REQ-086, REQ-133, REQ-135, REQ-147, REQ-158, REQ-159,
#               REQ-171, REQ-203, REQ-221, REQ-247, REQ-250, REQ-252, REQ-289, REQ-369, REQ-371,
#               REQ-510

from __future__ import annotations


import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import yaml
from fastapi import FastAPI, Request

from provisa.api.data.endpoint import router as data_router
from provisa.api.data.redirect_unwrap import router as redirect_unwrap_router
from provisa.api.data.endpoint_dev import router as dev_router
from provisa.api.data.endpoint_grpc_proxy import router as grpc_proxy_router
from provisa.api.data.sdl import router as sdl_router
from provisa.api.app_loaders import (
    _META_TABLE_ALIAS,
    _apply_server_and_engine_config,
    _build_and_register_schemas,
    _build_source_pools_and_enums,
    _init_ingest_engines,
    _init_meta_rls,
    _load_graphql_remote_sources_from_db,
    _load_masking_rules,
    _load_mv_and_views_config,
    _load_openapi_specs,
    _load_tracked_functions_and_webhooks,
    _process_kafka_sources,
    _setup_approval_hook,
)
from provisa.api.app_rebuild import (
    _bg_hydrate_api_endpoints,
    _finalize_rebuild_state,
    _register_user_views_in_state,
)
from provisa.api.app_schema_build import (
    _assert_domain_table_unique,
    _build_gql_object_columns,
    _filter_tables_by_schema_cfg,
    _inject_gql_required_args,
    _resolve_naming_config,
    _synthesize_column_metadata,
)
from provisa.api.app_startup import (
    _auto_register_graphql_demo,
    _capture_config_boot_snapshot,
    _prewarm_govdata_jvm,
    _start_background_tasks,
    _start_scheduler,
    _start_servers,
)
from provisa.compiler.introspect import ColumnMetadata, introspect_tables
from provisa.compiler.naming import source_to_catalog
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext
from sqlalchemy import select
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.database import Database
from provisa.core.schema_org import (
    domains as _domains_t,
    naming_rules as _naming_rules_t,
    registered_tables as _registered_tables_t,
    roles as _roles_t,
    sources as _sources_t,
)
from provisa.core.secrets import resolve_secrets
from provisa.executor.pool import SourcePool
from provisa.compiler.mask_inject import MaskingRules
from provisa.cache.store import CacheStore, NoopCacheStore, RedisCacheStore
from provisa.api.admin.db_queries import (
    fetch_tables as _fetch_tables,
    fetch_relationships as _fetch_relationships,
)
from provisa.api.otel_setup import setup_otel as _setup_otel, shutdown_otel as _shutdown_otel
from provisa.mv.registry import MVRegistry
from provisa.cache.warm_tables import WarmTableManager
from provisa.apq.cache import APQCache, NoopAPQCache
from provisa.api_source.models import ApiEndpoint as ApiEndpoint, ApiSource as ApiSource
from provisa.core.models import ProvisaConfig  # noqa: F401
from typing import TYPE_CHECKING, Any, cast  # noqa: F401

if TYPE_CHECKING:
    from provisa.cache.hot_tables import HotTableManager
    from provisa.core.tenant_context import TenantContextCache
    from provisa.kafka.window import KafkaTableConfig
    from provisa.core.models import Source
    from sqlalchemy.ext.asyncio import AsyncEngine
    import graphql

log = logging.getLogger(__name__)


class AppState:
    """Shared application state populated at startup."""

    # Control plane handles (SQLAlchemy-backed), two independent engines:
    # ``tenant_db`` is the per-org/tenant control plane (schema-scoped);
    # ``admin_db`` is the global platform control plane (orgs/users/invites/
    # billing), backed by its own SQLAlchemy URI.
    tenant_db: Database | None = None
    admin_db: Database | None = None
    engine_conn: Any | None = None  # engine terminal connection; owned by the engine backend
    engine_conn_kwargs: dict = {}  # kwargs used to create engine_conn (for reconnect)
    # Terminal-route execution binding (REQ-825): owns DIRECT-vs-ENGINE dispatch. Always bound in
    # __init__ (never None); the engine is the reference engine. Typed Any to avoid the runtime import.
    federation_engine: Any = None
    flight_client: Any | None = None  # pyarrow.flight.FlightClient
    schemas: dict[str, graphql.GraphQLSchema] = {}  # role_id → GraphQLSchema
    schema_build_cache: dict = {}  # raw data for on-demand domain-filtered schema building
    schema_version: int = (
        0  # bumped on every _rebuild_schemas; used by clients for cache invalidation
    )
    schema_boot_id: str = (
        ""  # random UUID set at startup; combined with schema_version for cache keys
    )
    contexts: dict[str, CompilationContext] = {}  # role_id → CompilationContext
    rls_contexts: dict[str, RLSContext] = {}  # role_id → RLSContext
    roles: dict[str, dict] = {}  # role_id → role dict
    source_pools: SourcePool = SourcePool()
    source_types: dict[str, str] = {}  # source_id → source_type
    source_catalogs: dict[str, str] = {}  # source_id → the engine catalog name
    source_dialects: dict[str, str] = {}  # source_id → sqlglot dialect
    source_dsns: dict[str, str] = {}  # source_id → "host:port/database" (physical colocation key)
    masking_rules: MaskingRules = {}  # (table_id, role_id) → {col: (rule, dtype)}
    response_cache_store: CacheStore = NoopCacheStore()
    response_cache_default_ttl: int = 300
    mv_registry: MVRegistry = MVRegistry()
    _mv_refresh_task: asyncio.Task | None = None
    proto_files: dict[str, str] = {}  # role_id → .proto content
    table_path_maps: dict[
        str, dict[str, dict]
    ] = {}  # role_id → {gql_field_name → {schema_name, table_name, domain_id}}
    _grpc_server: Any | None = None
    _flight_server: Any | None = None  # ProvisaFlightServer
    kafka_windows: dict[str, str] = {}  # source_id → default_window (e.g. "1h")
    kafka_table_configs: dict[str, KafkaTableConfig] = {}  # table_name → KafkaTableConfig
    view_sql_map: dict[str, str] = {}  # view_table_name → SQL (for inline expansion)
    source_cache: dict[str, dict] = {}  # source_id → {cache_enabled, cache_ttl}
    table_cache: dict[int, int | None] = {}  # table_id → cache_ttl
    auth_config: dict | None = None  # auth section from provisa.yaml
    auth_middleware_active: bool = False  # True only when wire_auth installed AuthMiddleware
    redis_url: str | None = None  # resolved Redis URL (REDIS_URL env or cache.redis_url)
    rate_limiter: Any | None = None  # REQ-369-371: Redis-backed RateLimiter (None until startup)
    approval_hook: Any | None = None  # REQ-247: ApprovalHook instance (None = disabled)
    approval_hook_config: Any | None = None  # REQ-247: ApprovalHookConfig
    table_approval_hooks: dict[int, bool] = {}  # table_id → approval_hook flag
    source_approval_hooks: dict[str, bool] = {}  # source_id → approval_hook flag
    api_endpoints: dict[str, Any] = {}  # table_name → ApiEndpoint
    api_sources: dict[str, Any] = {}  # source_id → ApiSource
    hot_manager: HotTableManager | None = None
    _hot_refresh_task: asyncio.Task | None = None
    warm_manager: WarmTableManager = WarmTableManager()
    _warm_task: asyncio.Task | None = None
    apq_cache: APQCache = NoopAPQCache()  # Phase AN: Automatic Persisted Queries
    apq_ttl: int = 86400  # REQ-289: APQ cache TTL (apq.ttl config / PROVISA_APQ_TTL env)
    live_engine: Any | None = None  # Phase AM: LiveEngine instance
    hostname: str = "localhost"  # publicly reachable hostname (PROVISA_HOSTNAME)
    source_federation_hints: dict[
        str, dict[str, str]
    ] = {}  # source_id → the engine session props (AL3)
    source_allowed_domains: dict[
        str, list[str]
    ] = {}  # source_id → allowed domain ids (empty = unrestricted)
    engine_session_hints: dict[
        str, str
    ] = {}  # FTE session properties injected into every the engine query
    server_cfg: dict = {}  # raw server section from provisa.yaml
    server_limits: dict = {}  # resolved query/request limits (from config + env overrides)
    tracked_functions: dict[str, dict] = {}  # gql field name → fn dict
    tracked_webhooks: dict[str, dict] = {}  # gql field name → wh dict
    pg_enum_types: dict = {}  # pg_name → GraphQLEnumType (REQ-221)
    org_id: str = "default"  # REQ-697: org schema scope (ORG_ID env var)
    graphql_remote_sources: dict[str, dict] = {}  # source_id → GraphQL remote registration
    openapi_specs: dict[str, dict] = {}  # source_id → OpenAPI spec registration
    grpc_remote_sources: dict[str, dict] = {}  # source_id → gRPC remote registration
    # Phase AS — Ingest sources
    ingest_engines: dict[str, AsyncEngine] = {}  # source_id → AsyncEngine
    ingest_tables: dict[str, dict[str, list[dict]]] = {}  # source_id → {table_name → [col defs]}
    # WebSocket sources
    websocket_sources: dict[str, Source] = {}  # source_id → Source
    # RSS/Atom feed sources
    rss_sources: dict[str, Source] = {}  # source_id → Source
    # REQ-824: sources with source-level CDC transport (Debezium/Kafka), entered once per source
    cdc_sources: dict[str, Source] = {}  # source_id → Source (only those with .cdc set)
    pg_notify_tables: set[str] = set()  # table_names with pg_notify triggers installed
    table_watermarks: dict[str, str] = {}  # table_name → watermark_column (for polling fallback)
    _scheduler: Any | None = None  # APScheduler instance for scheduled queries
    global_gql_naming_convention: str = (
        "apollo_graphql"  # runtime override; set via updateNamingConvention
    )
    global_sql_naming_convention: str = "snake"
    otel_compact_cron: str = "* * * * *"  # cron for Parquet→Iceberg compaction
    otel_compact_batch_size: int = 1000  # rows per INSERT batch during compaction
    otel_compact_file_chunk: int = 50  # Parquet files processed per compaction chunk
    otel_s3_endpoint: str = "http://minio:9000"  # MinIO/S3 endpoint for compaction
    domain_write_targets: dict[
        str, tuple[str, str]
    ] = {}  # domain_id → (catalog, domain_id) from Domain.catalog
    multitenancy: bool = False
    tenant_context_cache: TenantContextCache | None = None
    kafka_table_physical: dict[
        str, str
    ] = {}  # virtual gql table → physical the engine table (Kafka sources)
    config: Any = None  # ProvisaConfig set at startup
    # Live config export/diff/patch is opt-in (REQ-164) — coherent only where the generated/normalized
    # config is canonical (the demo), not a hand-authored file. Gates the boot snapshot + endpoints.
    config_live_export: bool = False
    # Normalized config generated ONCE at end of boot — after all runtime auto-derivation (FK tracking,
    # graphql-remote registration). The admin config-diff uses it as the baseline so it shows only
    # changes made SINCE startup, not derived entities that were never in the file (REQ-164).
    config_boot_snapshot: str | None = None
    otel_snapshot_retention_hours: int | None = None  # Iceberg snapshot expiry hours
    _stale_check_task: asyncio.Task | None = None  # schema staleness background loop

    def __init__(self) -> None:
        # Mandatory terminal-execution binding (REQ-825, REQ-840): every AppState is born with its
        # federation engine, so the query path always routes through it — there is no unbound state
        # and no per-call-site fallback. The runtime reads self.engine_conn lazily at execute time,
        # so binding before the connection exists is correct; startup may swap the reference engine.
        from provisa.federation.engine import build_engine  # $PROVISA_ENGINE selects
        from provisa.federation.runtime import EngineRuntime

        self.federation_engine = EngineRuntime(build_engine(), self)


state = AppState()


async def _load_and_build(
    config_path: str | None = None,
) -> None:  # REQ-012, REQ-016, REQ-247, REQ-289, REQ-369, REQ-371
    """Load config, introspect the engine, build schemas for all roles."""
    if config_path is None:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")

    # Use uvicorn's console logger — the root logger's only handler is the OTLP
    # exporter, so provisa.* logs never reach the console / backend.log.
    _startup_log = logging.getLogger("uvicorn.error")
    _startup_marks = [time.perf_counter()]

    def _mark(name: str) -> None:
        now = time.perf_counter()
        _startup_log.warning(
            "startup phase %-20s +%6.2fs (total %6.2fs)",
            name,
            now - _startup_marks[-1],
            now - _startup_marks[0],
        )
        _startup_marks.append(now)

    _startup_log.warning("startup phase %-20s begin", "lifespan")

    # Bring up the control planes + init schema unconditionally — the DB must be
    # available even before a full config exists (admin UI needs it on first
    # start). Connection details come from the config's control_plane section.
    from provisa.api.startup_seed import (
        _init_control_planes,
        _seed_built_in_sources,
        _resolve_pk_from_sources,
    )

    pg_host, pg_port, pg_database, pg_user = await _init_control_planes(config_path)

    _mark("pg-pool")
    _mark("schema-init")

    await _seed_built_in_sources(pg_host, pg_port, pg_database, pg_user)

    _mark("pg+schema+seed")

    path = Path(config_path)
    if not path.exists():
        return

    with open(path) as f:
        raw_config = yaml.safe_load(f)

    _apply_server_and_engine_config(raw_config)

    _mark("engine-connect")

    # Flight (Zaychik), the MinIO buckets, and the results schema are mutually independent
    # engine-terminal network setup, run concurrently to cut startup latency. the engine-terminal
    # infra: a native engine has no Zaychik/MinIO/results-schema, so provision_infra() is a
    # no-op there (it would otherwise block on absent services).
    await state.federation_engine.provision_infra()

    _mark("infra: flight/minio/results")

    # NOTE: Kafka sources must run BEFORE parse_config_dict / load_config so that
    # Kafka-derived tables are present when relationships are validated.
    _process_kafka_sources(raw_config)

    # Store auth config for middleware setup
    _raw_auth = raw_config.get("auth")
    state.auth_config = (
        None if (isinstance(_raw_auth, dict) and _raw_auth.get("provider") == "none") else _raw_auth
    )

    # Load config into PG (and create the engine catalogs)
    config = parse_config_dict(raw_config)
    state.config = config
    state.multitenancy = config.multitenancy
    if config.multitenancy:
        from provisa.core.tenant_context import TenantContextCache

        state.tenant_context_cache = TenantContextCache()
        tenant_db = state.tenant_db
        assert tenant_db is not None
        async with tenant_db.acquire() as _rls_conn:
            await _init_meta_rls(cast(asyncpg.Connection, _rls_conn))

    # Apply observability config to state
    if config.observability:
        state.otel_compact_cron = config.observability.compact_cron
        state.otel_compact_batch_size = config.observability.compact_batch_size
        state.otel_compact_file_chunk = config.observability.compact_file_chunk
        state.otel_snapshot_retention_hours = config.observability.ops_snapshot_retention_hours
        state.otel_s3_endpoint = config.observability.s3_endpoint

    # Initialize cache store — REDIS_URL env var overrides config
    # Live config export/diff/patch (REQ-164) is coherent only when the generated/normalized config is
    # canonical — the demo scenario (config built from installer choices), NOT a hand-authored file
    # with comments/ordering a normalized patch could not stay faithful to. Off unless opted in.
    state.config_live_export = bool(
        raw_config.get("live_config_export", False)
        or os.environ.get("PROVISA_LIVE_CONFIG_EXPORT", "").lower() in ("1", "true", "yes")
    )

    cache_config = raw_config.get("cache", {})
    # Resolve Redis URL regardless of response-cache enablement so rate limiting
    # (REQ-371) can use it even when the response cache is off. PROVISA_REDIS_EMBEDDED
    # forces the in-process fakeredis path (REQ-829) for the native desktop tier — an
    # explicit selection that ignores any configured URL, so no Redis server is needed.
    if os.environ.get("PROVISA_REDIS_EMBEDDED", "").lower() in ("1", "true", "yes"):
        state.redis_url = None
    else:
        state.redis_url = (
            os.environ.get("REDIS_URL")
            or resolve_secrets(cache_config.get("redis_url", ""))
            or None
        )
    # REQ-289: APQ TTL from the apq.ttl config key (PROVISA_APQ_TTL env overrides, like redis_url).
    state.apq_ttl = int(
        os.environ.get("PROVISA_APQ_TTL") or raw_config.get("apq", {}).get("ttl") or 86400
    )
    # Default enabled=True: a store always exists — RedisCacheStore(None) falls back to
    # embedded fakeredis when no Redis URL is set, so there is never a "no cache" state.
    # Set cache.enabled: false explicitly to opt into the NoopCacheStore.
    if cache_config.get("enabled", True):
        # REQ-829: RedisCacheStore(None) transparently uses embedded fakeredis, so
        # desktop exercises the same result-cache code path as production.
        state.response_cache_store = RedisCacheStore(state.redis_url)
        state.response_cache_default_ttl = cache_config.get("default_ttl", 300)

    tenant_db = state.tenant_db
    assert tenant_db is not None
    async with tenant_db.acquire() as conn:
        _replace_mode = os.environ.get("PROVISA_CONFIG_REPLACE", "").lower() in ("1", "true", "yes")
        await load_config(config, conn, state.federation_engine, replace=_replace_mode)

    _mark("load_config")

    state.source_dsns["provisa-admin"] = f"{pg_host}:{pg_port}/{pg_database}"

    await _build_source_pools_and_enums(config)

    await _init_ingest_engines()

    # Second pass — resolve PRIMARY KEYs from each native RDBMS source's own
    # information_schema. the engine normalizes column types and layers Provisa governance
    # on top, but its metadata model omits source constraints (there is no
    # information_schema.table_constraints in the engine catalog), so PKs are read here
    # through the source driver directly, now that the source pools are built. The DB
    # constraint is authoritative — config YAML need not restate is_primary_key.
    await _resolve_pk_from_sources()

    # Schema-currency reconcile (REQ-846/932): converge the materialization store's landing tables
    # to config for every MATERIALIZED source and attach their read views — DDL only, no data landed
    # (that is the refresh's job). Best-effort at boot so a store hiccup never bricks startup (matches
    # the live-engine reconcile pattern); materialized sources become queryable once it succeeds.
    try:
        _landed = await state.federation_engine.reconcile_landed_tables()
        if _landed:
            log.info("reconciled %d landed table(s) into the materialization store", len(_landed))
    except Exception:
        log.exception("landed-table schema reconcile failed")
    _mark("reconcile landed tables")

    # Reload OpenAPI specs from DB into state (survives hot reloads and restarts)
    await _load_openapi_specs()

    # Load materialized view definitions, views, and auto-MV cross-source rels
    _load_mv_and_views_config(raw_config)

    await _load_graphql_remote_sources_from_db()

    # Retry config relationships deferred at load_config time (graphql_remote tables now available)
    if getattr(state, "config", None) is not None and state.tenant_db is not None:
        from provisa.core.repositories import relationship as _rel_repo

        async with state.tenant_db.acquire() as _retry_conn:
            for _rel in state.config.relationships:
                try:
                    await _rel_repo.upsert(_retry_conn, _rel)
                except ValueError:
                    pass

    _mark("source-pools+ingest+remote")

    await _rebuild_schemas(raw_config)

    _mark("rebuild_schemas")

    # Initialize hot tables (Phase AD6)
    from provisa.cache.hot_tables import init_hot_tables

    hot_mgr = await init_hot_tables(raw_config, state.federation_engine)
    if hot_mgr is not None:
        state.hot_manager = hot_mgr

    _mark("hot_tables")


async def _rebuild_schemas(raw_config: dict | None = None) -> None:
    # Rebuild per-role schemas from DB state. Column types come from the authoritative
    # table_columns store (introspect_tables does NOT query the engine), so this runs on any
    # engine; a missing the engine connection only skips the engine-catalog ops seeding below.
    _rebuild_log = logging.getLogger(__name__)
    _rebuild_log.info("_rebuild_schemas called")
    if state.tenant_db is None:
        _rebuild_log.warning("_rebuild_schemas: tenant_db is None, returning")
        return

    kafka_physical = getattr(state, "kafka_table_physical", {})
    domain_prefix, raw_config = _resolve_naming_config(raw_config)

    # REQ-684/686: install the process-wide EncryptionService from config before any
    # encrypt/decrypt (API auth column, hot cache, audit) runs. Unset provider = passthrough.
    from provisa.encryption import configure_encryption

    _enc_cfg = (raw_config or {}).get("encryption", {}) or {}
    configure_encryption(_enc_cfg.get("provider"), key_id=_enc_cfg.get("key_id"))

    # Clear mutable state before rebuild
    state.masking_rules = {}

    async with state.tenant_db.acquire() as conn:
        _pg = cast(asyncpg.Connection, conn)
        tables = await _fetch_tables(_pg)
        _assert_domain_table_unique(tables)
        relationships = await _fetch_relationships(_pg)

        # Apply schema visibility filters (schema.include_ops / schema.include_metrics)
        _schema_cfg = raw_config.get("schema", {}) if raw_config else {}
        tables = _filter_tables_by_schema_cfg(tables, _schema_cfg, state.source_allowed_domains)

        # Install LISTEN/NOTIFY triggers on pre-approved PostgreSQL tables
        from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers

        state.pg_notify_tables = await ensure_pg_notify_triggers(conn, tables, state.source_types)
        state.table_watermarks = {
            tbl["table_name"]: tbl["watermark_column"]
            for tbl in tables
            if tbl.get("watermark_column")
        }
        naming_rules = [
            dict(r._mapping)
            for r in (
                await conn.execute_core(
                    select(_naming_rules_t.c.pattern, _naming_rules_t.c.replacement)
                )
            ).fetchall()
        ]

        # Load per-table cache TTLs
        cache_rows = [
            dict(r._mapping)
            for r in (
                await conn.execute_core(
                    select(_registered_tables_t.c.id, _registered_tables_t.c.cache_ttl).where(
                        _registered_tables_t.c.cache_ttl.is_not(None)
                    )
                )
            ).fetchall()
        ]
        state.table_cache = {r["id"]: r["cache_ttl"] for r in cache_rows}
        domains = [
            dict(r._mapping)
            for r in (
                await conn.execute_core(select(_domains_t.c.id, _domains_t.c.description))
            ).fetchall()
        ]
        sources = {
            r._mapping["id"]: dict(r._mapping)
            for r in (await conn.execute_core(select(_sources_t))).fetchall()
        }
        # Backfill state.source_types; patch postgresql sources to use the engine catalog names.
        for _sid, _src_dict in list(sources.items()):
            if _sid not in state.source_types and _src_dict.get("type"):
                state.source_types[_sid] = _src_dict["type"]
            if _src_dict.get("type") == "postgresql":
                sources[_sid] = {**_src_dict, "database": source_to_catalog(_sid)}
        roles = [
            dict(r._mapping)
            for r in (
                await conn.execute_core(
                    select(_roles_t.c.id, _roles_t.c.capabilities, _roles_t.c.domain_access)
                )
            ).fetchall()
        ]

        # Merge PG-stored allowed_domains into state; inject source naming into table dicts.
        for src_id, src_row in sources.items():
            if pg_domains := list(src_row.get("allowed_domains") or []):
                state.source_allowed_domains[src_id] = pg_domains
        for tbl in tables:
            tbl["source_gql_naming_convention"] = sources.get(tbl["source_id"], {}).get(
                "gql_naming_convention"
            )

        # Ensure ops tables exist before introspection — idempotent, self-healing if boot seeding
        # raced the otel catalog. No-op for a native engine (telemetry lives in the ops store).
        from provisa.api.startup_seed import _OPS_VIEWS

        state.federation_engine.reseed_ops(
            _OPS_VIEWS, getattr(state, "otel_snapshot_retention_hours", None)
        )

        await _register_user_views_in_state(conn, raw_config)

        # Introspect the engine metadata
        col_types_converted: dict[int, list[ColumnMetadata]] = introspect_tables(
            state.engine_conn, tables, sources, {**_META_TABLE_ALIAS, **(kafka_physical or {})}
        )

        _gql_remote_srcs = getattr(state, "graphql_remote_sources", {})

        # Inject required GQL args as native filter columns for graphql_remote tables.
        _inject_gql_required_args(tables, _gql_remote_srcs)

        # Build gql_object_columns: {table_name: {col_name: [sub_field_names]}} for JSON extraction
        _gql_object_cols = _build_gql_object_columns(_gql_remote_srcs)

        # Synthesize ColumnMetadata for ops, provisa-admin, graphql_remote, and govdata tables
        _synthesize_column_metadata(tables, col_types_converted, _gql_remote_srcs)

        # Load API sources and endpoints (Phase U)
        from provisa.api_source.loader import load_api_sources

        state.api_endpoints, state.api_sources = await load_api_sources(
            _pg,
            tables,
            col_types_converted,
            roles,
            state.source_types,
        )

        await _bg_hydrate_api_endpoints()

        # Load RLS rules — domain_id is required so domain-scoped rules (REQ-402)
        # are not silently dropped by build_rls_context. Read through the repo so the
        # encrypted filter_expr (REQ-686) is decrypted back to SQL at this boundary.
        from provisa.core.repositories import rls as _rls_repo

        rls_rules = await _rls_repo.list_all(conn)

        await _load_masking_rules(conn, col_types_converted, roles)

        tracked_functions, tracked_webhooks = await _load_tracked_functions_and_webhooks(
            conn, raw_config
        )

        _build_and_register_schemas(
            roles=roles,
            tables=tables,
            relationships=relationships,
            col_types_converted=col_types_converted,
            naming_rules=naming_rules,
            domains=domains,
            domain_prefix=domain_prefix,
            kafka_physical=kafka_physical,
            tracked_functions=tracked_functions,
            tracked_webhooks=tracked_webhooks,
            gql_object_cols=_gql_object_cols,
            rls_rules=rls_rules,
        )

    # Cache raw build data for on-demand domain-filtered schema generation
    state.schema_build_cache = {
        "tables": tables,
        "relationships": relationships,
        "column_types": col_types_converted,
        "naming_rules": naming_rules,
        "domains": domains,
        "domain_prefix": domain_prefix,
        "sql_naming_convention": state.global_sql_naming_convention,
        "functions": tracked_functions,
        "webhooks": tracked_webhooks,
        "enum_types": state.pg_enum_types,
        "physical_table_map": {**_META_TABLE_ALIAS, **(kafka_physical or {})},
    }
    state.schema_version += 1
    await _finalize_rebuild_state(_rebuild_log)


@asynccontextmanager
async def lifespan(_app: FastAPI):  # pyright: ignore[reportUnusedParameter, reportUnusedVariable]
    """App lifespan: load config and build schemas at startup."""
    _log = logging.getLogger("uvicorn.error")
    state.schema_boot_id = uuid.uuid4().hex
    try:
        await _load_and_build()
    except Exception:
        _log.exception("Startup failed during _load_and_build")
        raise

    _prewarm_govdata_jvm(_log)

    await _start_background_tasks(_log)

    await _start_servers(_log)

    _start_scheduler(_log)

    await _auto_register_graphql_demo(_log)

    # Snapshot the config AFTER all boot-time auto-derivation, so the admin config-diff baseline
    # excludes runtime-derived entities (REQ-164). Opt-in; best-effort (the helper degrades and the
    # diff falls back to the on-disk file).
    await _capture_config_boot_snapshot(_log)

    yield

    # Stop Arrow Flight server
    if state._flight_server:
        state._flight_server.shutdown()

    # Stop gRPC server
    if state._grpc_server:
        await state._grpc_server.stop(grace=5)

    # Cancel schema staleness loop
    if getattr(state, "_stale_check_task", None):
        assert state._stale_check_task is not None
        state._stale_check_task.cancel()
        try:
            await state._stale_check_task
        except asyncio.CancelledError:
            pass

    # Cancel warm-table task
    if state._warm_task:
        state._warm_task.cancel()
        try:
            await state._warm_task
        except asyncio.CancelledError:
            pass

    # Cancel hot-table refresh task (Phase AD6)
    if state._hot_refresh_task:
        state._hot_refresh_task.cancel()
        try:
            await state._hot_refresh_task
        except asyncio.CancelledError:
            pass
    if state.hot_manager is not None:
        from provisa.cache.hot_tables import HotTableManager

        assert isinstance(state.hot_manager, HotTableManager)
        await state.hot_manager.close()

    # Cancel MV refresh task
    if state._mv_refresh_task:
        state._mv_refresh_task.cancel()
        try:
            await state._mv_refresh_task
        except asyncio.CancelledError:
            pass
    from provisa.api.startup_resilience import tolerate_shutdown_failure

    # Stop Live Query Engine (Phase AM)
    if state.live_engine is not None:
        with tolerate_shutdown_failure("live query engine stop"):
            await state.live_engine.stop()

    # Close APQ cache (Phase AN)
    with tolerate_shutdown_failure("APQ cache close"):
        await state.apq_cache.close()

    # Stop scheduler (Phase AX)
    if state._scheduler is not None:
        with tolerate_shutdown_failure("scheduler shutdown"):
            state._scheduler.shutdown(wait=False)

    _shutdown_otel()

    await state.response_cache_store.close()
    await state.source_pools.close_all()
    if state.tenant_db:
        await state.tenant_db.close()
    state.federation_engine.close()


def create_app() -> FastAPI:
    """Create the FastAPI application."""
    from fastapi.middleware.cors import CORSMiddleware
    from strawberry.fastapi import GraphQLRouter

    from provisa.api.admin.schema import admin_schema

    # Swagger/OpenAPI live under /data/openapi/ (not the default /docs) so the UI can
    # own /docs for its in-app documentation reader.
    app = FastAPI(
        title="Provisa",
        lifespan=lifespan,
        docs_url="/data/openapi/docs",
        redoc_url="/data/openapi/redoc",
        openapi_url="/data/openapi/openapi.json",
    )
    state.federation_engine.write_config(os.environ.get("PROVISA_CONFIG", "config/provisa.yaml"))
    _setup_otel(app)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from fastapi import Request as _Request
    from fastapi.responses import JSONResponse as _JSONResponse

    @app.exception_handler(Exception)
    async def _global_exception_handler(_req: _Request, exc: Exception):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        log.exception("Unhandled exception on %s %s", _req.method, _req.url.path)
        return _JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "type": type(exc).__name__},
        )

    @app.exception_handler(asyncio.TimeoutError)
    async def _timeout_handler(_req: _Request, _exc: asyncio.TimeoutError):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        log.error("Request timeout on %s %s", _req.method, _req.url.path)
        return _JSONResponse(status_code=504, content={"detail": "Request timed out"})

    # ABAC approval hook (REQ-247): build from auth.approval_hook config and scope flags.
    _setup_approval_hook(state)

    # Rate limiting (REQ-369-371): Redis-backed limiter + per-role request middleware.
    # Added BEFORE wire_auth so the auth middleware (added later) runs first and
    # populates request.state.role before the rate-limit check sees it.
    from provisa.api.rate_limit import build_rate_limiter

    state.rate_limiter = build_rate_limiter(getattr(state, "redis_url", None))
    from provisa.api.middleware.rate_limit_middleware import RateLimitMiddleware

    app.add_middleware(RateLimitMiddleware)

    # Conditionally add auth middleware and routes
    from provisa.auth.wiring import wire_auth

    wire_auth(app, state.auth_config, db_pool=state.tenant_db, admin_pool=state.admin_db)

    if state.multitenancy:
        from provisa.api.middleware.tenant_middleware import TenantMiddleware

        app.add_middleware(TenantMiddleware)

        from starlette.middleware.base import BaseHTTPMiddleware as _BaseHTTPMiddleware

        class _TenantSpanMiddleware(_BaseHTTPMiddleware):
            async def dispatch(self, request, call_next):
                response = await call_next(request)
                tenant_id = getattr(request.state, "tenant_id", None)
                if tenant_id:
                    try:
                        from opentelemetry import trace as _trace

                        _span = _trace.get_current_span()
                        if _span.is_recording():
                            _span.set_attribute("tenant_id", tenant_id)
                    except (ImportError, AttributeError):
                        # Best-effort span decoration: tolerate an absent OTel install or a
                        # no-op shim span that lacks is_recording/set_attribute. Never break
                        # request handling for a telemetry tag.
                        pass
                return response

        app.add_middleware(_TenantSpanMiddleware)

    app.include_router(data_router)
    app.include_router(redirect_unwrap_router)
    app.include_router(dev_router)
    app.include_router(grpc_proxy_router)
    app.include_router(sdl_router)

    # Ingest push receiver (Phase AS)
    try:
        from provisa.ingest.router import router as ingest_router

        app.include_router(ingest_router)
    except ImportError:
        pass

    # SSE subscription endpoint (Phase AB2)
    try:
        from provisa.api.data.subscribe import router as subscribe_router

        app.include_router(subscribe_router)
    except ImportError:
        pass

    # REST auto-generated endpoints (Phase AB5)
    try:
        from provisa.api.rest.generator import create_rest_router

        app.include_router(create_rest_router(state))
    except ImportError:
        pass

    # JSON:API auto-generated endpoints (Phase AB6)
    try:
        from provisa.api.jsonapi.generator import create_jsonapi_router

        app.include_router(create_jsonapi_router(state))
    except ImportError:
        pass

    # Admin GraphQL API (Strawberry) at /admin/graphql
    async def _admin_graphql_context(request: Request):
        return {"request": request}

    admin_router = GraphQLRouter(admin_schema, context_getter=_admin_graphql_context)
    app.include_router(admin_router, prefix="/admin/graphql")

    @app.middleware("http")
    async def _admin_graphql_schema_version_header(request: Request, call_next):  # pyright: ignore[reportUnusedFunction]
        from starlette.requests import ClientDisconnect
        from starlette.responses import Response as StarletteResponse

        try:
            response = await call_next(request)
        except ClientDisconnect:
            return StarletteResponse(status_code=499)
        if request.url.path.startswith("/admin/graphql"):
            response.headers["X-Schema-Version"] = str(state.schema_version)
        return response

    from provisa.api.admin.discovery import router as discovery_router

    app.include_router(discovery_router)
    from provisa.api.admin.discovery_schema import router as schema_discovery_router

    app.include_router(schema_discovery_router)
    from provisa.api.admin.api_discovery import router as api_discovery_router

    app.include_router(api_discovery_router)
    from provisa.api.admin.neo4j_router import router as neo4j_router

    app.include_router(neo4j_router)
    from provisa.api.admin.sparql_router import router as sparql_router

    app.include_router(sparql_router)
    from provisa.api.admin.graphql_remote_router import router as graphql_remote_router

    app.include_router(graphql_remote_router)
    from provisa.api.admin.openapi_router import router as openapi_router

    app.include_router(openapi_router)
    from provisa.api.admin.grpc_remote_router import router as grpc_remote_router

    app.include_router(grpc_remote_router)
    from provisa.api.admin.actions_router import router as actions_router

    app.include_router(actions_router)
    from provisa.api.admin.crawl_router import router as crawl_router

    app.include_router(crawl_router)
    from provisa.api.admin.settings_router import router as settings_router

    app.include_router(settings_router)
    from provisa.api.admin.source_meta_router import router as source_meta_router

    app.include_router(source_meta_router)
    from provisa.api.admin.table_profile_router import router as table_profile_router

    app.include_router(table_profile_router)
    from provisa.api.admin.table_search_router import router as table_search_router

    app.include_router(table_search_router)
    from provisa.api.admin.local_users_router import router as local_users_router

    app.include_router(local_users_router)
    from provisa.api.admin.orgs_router import router as orgs_router

    app.include_router(orgs_router)
    from provisa.api.admin.invites_router import router as invites_router

    app.include_router(invites_router)
    from provisa.api.admin.roles_router import router as roles_router

    app.include_router(roles_router)
    from provisa.api.admin.creation_requests_router import router as creation_requests_router

    app.include_router(creation_requests_router)
    from provisa.api.auth_router import router as auth_router

    app.include_router(auth_router)
    from provisa.api.setup_router import router as setup_router

    app.include_router(setup_router)

    # Cypher query endpoint (Phase AU)
    try:
        from provisa.api.rest.cypher_router import router as cypher_router

        app.include_router(cypher_router)
    except ImportError:
        pass

    # Neo4j Browser compatibility layer (Query API v2 + discovery)
    try:
        from provisa.api.rest.neo4j_compat_router import router as neo4j_compat_router

        app.include_router(neo4j_compat_router)
    except ImportError:
        pass

    # Natural Language query endpoint (Phase AV)
    try:
        from provisa.api.rest.nl_router import router as nl_router

        app.include_router(nl_router)
    except ImportError:
        pass

    from provisa.api.billing.router import router as billing_router

    app.include_router(billing_router, prefix="/billing", tags=["billing"])

    if state.multitenancy:
        from provisa.control_plane.router import router as control_plane_router

        app.include_router(control_plane_router)

    @app.api_route("/health", methods=["GET", "HEAD"])
    async def health():  # noqa: F841  # pyright: ignore[reportUnusedFunction]
        pg_status = "unavailable"
        if state.tenant_db is not None:
            try:
                async with state.tenant_db.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                pg_status = "ok"
            except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, asyncio.TimeoutError):
                pg_status = "unavailable"
        return {
            "status": "ok",
            "dependencies": {
                "postgres": pg_status,
            },
        }

    @app.api_route("/live", methods=["GET", "HEAD"])
    async def liveness():  # noqa: F841  # pyright: ignore[reportUnusedFunction]
        return {"status": "ok"}

    @app.api_route("/ready", methods=["GET", "HEAD"])
    async def readiness():  # noqa: F841  # pyright: ignore[reportUnusedFunction]
        return {"status": "ok"}

    return app
