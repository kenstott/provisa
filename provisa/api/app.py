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

import yaml
from fastapi import FastAPI, Request, Response
from sqlalchemy.exc import SQLAlchemyError

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
    _populate_source_catalog_names,
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
    _warmup_readiness,
)
from provisa.compiler.introspect import ColumnMetadata, introspect_tables
from provisa.compiler.naming import source_to_catalog
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext
from sqlalchemy import select
from provisa.core.config_loader import config_replace_mode, load_config, parse_config_dict
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
from provisa.api.org_runtime import (
    ActiveOrgPool,
    OrgRegistry,
    OrgRuntime,
    current_org,
    reset_current_org,
    set_current_org,
)
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
    from provisa.core.database import Connection
    from sqlalchemy.ext.asyncio import AsyncEngine
    import graphql

log = logging.getLogger(__name__)


class AppState:
    """Shared application state populated at startup."""

    # Control plane handles (SQLAlchemy-backed), two independent engines:
    # ``tenant_db`` is the per-org/tenant control plane (schema-scoped);
    # ``admin_db`` is the global platform control plane (orgs/users/invites/
    # billing), backed by its own SQLAlchemy URI.
    admin_db: Database | None = None
    # REQ-1316: ONE tenant-plane AsyncEngine shared by every org runtime on a schema-capable
    # backend. Database.acquire() issues the org's search_path on each checkout, so orgs need
    # separate handles, never separate pools. A pool per org multiplies connections by tenant
    # count and exhausts the server's max_connections (Cloud SQL db-f1-micro caps at 25 — two
    # orgs at pool_size=5/overflow=5 already blow past it).
    tenant_engine: Any | None = None  # AsyncEngine; Any avoids the runtime import here
    # engine_conn / engine_conn_kwargs / federation_engine are routed PROPERTIES (REQ-1244):
    # they live on the per-org OrgRuntime and resolve through the current_org ContextVar, falling
    # through to the default-org (shared) runtime for every org without a dedicated engine.
    flight_client: Any | None = None  # pyarrow.flight.FlightClient
    # schema_build_cache is org-routed; see the property below.
    schema_version: int = (
        0  # bumped on every _rebuild_schemas; used by clients for cache invalidation
    )
    schema_boot_id: str = (
        ""  # random UUID set at startup; combined with schema_version for cache keys
    )
    response_cache_store: CacheStore = NoopCacheStore()
    response_cache_default_ttl: int = 300
    # REQ-1008: server-lifetime MCP catalog search index (DuckDB VSS HNSW), built lazily on first
    # search_catalog and invalidated (set None) on catalog reload. Any so the mcp package owns the type.
    mcp_catalog_index: Any = None
    mv_registry: MVRegistry = MVRegistry()
    _mv_refresh_task: asyncio.Task | None = None
    proto_files: dict[str, str] = {}  # role_id → .proto content
    # The one SERVED wire descriptor: union of every role's surface (see
    # app_loaders._build_and_register_schemas). Governance is per-request, not per-descriptor.
    wire_proto: str | None = None
    table_path_maps: dict[
        str, dict[str, dict]
    ] = {}  # role_id → {gql_field_name → {schema_name, table_name, domain_id}}
    _grpc_server: Any | None = None
    _flight_server: Any | None = None  # ProvisaFlightServer
    kafka_windows: dict[str, str] = {}  # source_id → default_window (e.g. "1h")
    kafka_table_configs: dict[str, KafkaTableConfig] = {}  # table_name → KafkaTableConfig
    view_sql_map: dict[str, str] = {}  # view_table_name → SQL (for inline expansion)
    # REQ-1163: bitemporal materialized views → (physical mv target ref, spec), so a request-level
    # as-of (X-Provisa-As-Of) can overlay an as-of reconstruction over each one's append log.
    bitemporal_view_reads: dict = {}  # view_table_name → (mv_ref, BitemporalSpec)
    table_cache: dict[int, int | None] = {}  # table_id → cache_ttl
    auth_config: dict | None = None  # auth section from provisa.yaml
    # Bumped every time _load_and_build resolves auth_config. The lazily-resolving AuthMiddleware
    # (config_resolver path) caches its provider on first request; comparing this generation lets it
    # re-resolve when auth is (re)configured at runtime — e.g. the setup wizard or a PROVISA_IDP boot
    # deferral turns an unsecured server into a firebase one without a process restart (REQ-1267).
    auth_reconfig_generation: int = 0
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
    # Readiness (REQ /ready): False until the boot warmup probe has primed the lazy per-request paths
    # (materialize-store attach + a warm engine terminal). /ready returns 503 while this is False so a
    # launcher/orchestrator holds traffic — and the browser open — until the first interaction is warm.
    is_warm: bool = False
    _warmup_task: asyncio.Task | None = None
    apq_cache: APQCache = NoopAPQCache()  # Phase AN: Automatic Persisted Queries
    apq_ttl: int = 86400  # REQ-289: APQ cache TTL (apq.ttl config / PROVISA_APQ_TTL env)
    live_engine: Any | None = None  # Phase AM: LiveEngine instance
    hostname: str = "localhost"  # publicly reachable hostname (PROVISA_HOSTNAME)
    engine_session_hints: dict[
        str, str
    ] = {}  # FTE session properties injected into every the engine query
    server_cfg: dict = {}  # raw server section from provisa.yaml
    server_limits: dict = {}  # resolved query/request limits (from config + env overrides)
    security_high: bool = (
        False  # REQ-693: high-security mode (pgwire off, data endpoints KMS-gated)
    )
    tracked_functions: dict[str, dict] = {}  # gql field name → fn dict
    tracked_webhooks: dict[str, dict] = {}  # gql field name → wh dict
    # REQ-885: deny-by-default egress allow-list for hosted http/grpc UDFs. host or host:port
    # entries; empty ⇒ all external egress denied (loopback/Provisa pgwire is always allowed).
    udf_egress_allowlist: list[str] = []
    pg_enum_types: dict = {}  # pg_name → GraphQLEnumType (REQ-221)
    _org_id: str = "default"  # REQ-697: org schema scope (ORG_ID env var); see the org_id property
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

        # REQ-1266: per-request multi-org data plane. The routed maps below (source
        # pools, roles, compiled schemas/contexts, catalog names, masking, …) live on
        # a per-org OrgRuntime; the properties resolve the ContextVar-selected runtime,
        # defaulting to the default-org runtime when unset (startup / background boot /
        # single-org tests). The default runtime is registered here so build-time writes
        # (which run before any request sets the ContextVar) always have a target.
        self.org_registry = OrgRegistry()
        self.org_registry.set(self.org_id, OrgRuntime(org_id=self.org_id))

        # The registry must exist first: federation_engine is a routed property (REQ-1244) and
        # this assignment lands on the default-org runtime — the SHARED engine every org without
        # a dedicated binding resolves to.
        self.federation_engine = EngineRuntime(build_engine(), self)

    # --- Per-request org routing (REQ-1266) -----------------------------------
    @property
    def org_id(self) -> str:
        return self._org_id

    @org_id.setter
    def org_id(self, value: str) -> None:
        """Re-point the boot org, moving the default-org runtime with it.

        REQ-1266: ``__init__`` registers the default runtime under the compile-time id, but the
        real id only arrives once ``_init_control_planes`` reads the control-plane config. The
        runtime has to follow, because every build-time write below resolves through it — leaving
        it keyed by the old id strands the writes and the boot fails on the missing runtime."""
        old = self._org_id
        self._org_id = value
        if value == old:
            return
        rt = self.org_registry.get(old)
        if rt is not None:
            self.org_registry.set(value, rt)
            self.org_registry.invalidate(old)

    def _active_runtime(self) -> OrgRuntime:
        """The OrgRuntime for the current request's org, or the default-org runtime
        when no org is bound. Never fabricates a runtime for an unbuilt org — a
        tenant-data path with an unbuilt selected org is a routing defect that the
        entrypoint must have caught (see require_current_org)."""
        org_id = current_org.get() or self.org_id
        rt = self.org_registry.get(org_id)
        if rt is None:
            rt = self.org_registry.get(self.org_id)
            assert rt is not None, "default-org runtime missing — AppState not initialized"
        return rt

    def _default_runtime(self) -> OrgRuntime:
        rt = self.org_registry.get(self.org_id)
        assert rt is not None, "default-org runtime missing — AppState not initialized"
        return rt

    def _engine_runtime(self) -> OrgRuntime:
        """The runtime OWNING the engine terminal for the current context (REQ-1244): the active
        org's runtime when it carries a dedicated federation engine (orgs.isolated_engine), else
        the default-org runtime holding the shared engine — the pooled lane every org starts on
        (REQ-1243)."""
        rt = self._active_runtime()
        if rt.federation_engine is not None:
            return rt
        return self._default_runtime()

    @property
    def federation_engine(self) -> Any:
        return self._engine_runtime().federation_engine

    @federation_engine.setter
    def federation_engine(self, value: Any) -> None:
        rt = self._active_runtime()
        target = rt if rt.isolated_engine else self._default_runtime()
        target.federation_engine = value

    @property
    def engine_conn(self) -> Any:
        return self._engine_runtime().engine_conn

    @engine_conn.setter
    def engine_conn(self, value: Any) -> None:
        self._engine_runtime().engine_conn = value

    @property
    def engine_conn_kwargs(self) -> dict:
        return self._engine_runtime().engine_conn_kwargs

    @engine_conn_kwargs.setter
    def engine_conn_kwargs(self, value: dict) -> None:
        self._engine_runtime().engine_conn_kwargs = value

    @property
    def active_org_id(self) -> str:
        """The org id the current context is bound to, or the default org when none is bound."""
        return current_org.get() or self.org_id

    @property
    def active_isolated_org(self) -> str | None:
        """The active org's id IF that org runs a dedicated federation engine, else ``None`` —
        the seam engine lifecycle code (trino_lifecycle.provision) uses to resolve the dedicated
        coordinator endpoint without knowing about org routing."""
        rt = self._active_runtime()
        return rt.org_id if rt.isolated_engine else None

    @property
    def tenant_db(self) -> Database | None:
        return self._active_runtime().tenant_db

    @tenant_db.setter
    def tenant_db(self, value: Database | None) -> None:
        self._active_runtime().tenant_db = value

    @property
    def source_pools(self) -> SourcePool:
        return self._active_runtime().source_pools

    @source_pools.setter
    def source_pools(self, value: SourcePool) -> None:
        self._active_runtime().source_pools = value

    @property
    def source_types(self) -> dict[str, str]:
        return self._active_runtime().source_types

    @source_types.setter
    def source_types(self, value: dict[str, str]) -> None:
        self._active_runtime().source_types = value

    @property
    def source_dialects(self) -> dict[str, str]:
        return self._active_runtime().source_dialects

    @source_dialects.setter
    def source_dialects(self, value: dict[str, str]) -> None:
        self._active_runtime().source_dialects = value

    @property
    def source_dsns(self) -> dict[str, str]:
        return self._active_runtime().source_dsns

    @source_dsns.setter
    def source_dsns(self, value: dict[str, str]) -> None:
        self._active_runtime().source_dsns = value

    @property
    def source_catalogs(self) -> dict[str, str]:
        return self._active_runtime().source_catalogs

    @source_catalogs.setter
    def source_catalogs(self, value: dict[str, str]) -> None:
        self._active_runtime().source_catalogs = value

    def catalog_for(self, source_id: str) -> str:
        """Physical engine catalog name for ``source_id`` under the current request's org
        (REQ-1266). Consults the ContextVar-selected runtime's ``source_catalogs`` — the
        only correct source of the org-prefixed name. Raises when the source is unknown to
        the active org: a bare ``source_to_catalog`` fallback here would silently resolve to
        the DEFAULT org's physical catalog (cross-org data leak), so there is no fallback."""
        catalogs = self._active_runtime().source_catalogs
        catalog = catalogs.get(source_id)
        if catalog is None:
            raise KeyError(
                f"source {source_id!r} has no catalog in org "
                f"{current_org.get() or self.org_id!r} — source not registered for this org"
            )
        return catalog

    @property
    def source_cache(self) -> dict[str, dict]:
        return self._active_runtime().source_cache

    @source_cache.setter
    def source_cache(self, value: dict[str, dict]) -> None:
        self._active_runtime().source_cache = value

    @property
    def source_allowed_domains(self) -> dict[str, list[str]]:
        return self._active_runtime().source_allowed_domains

    @source_allowed_domains.setter
    def source_allowed_domains(self, value: dict[str, list[str]]) -> None:
        self._active_runtime().source_allowed_domains = value

    @property
    def source_federation_hints(self) -> dict[str, dict[str, str]]:
        return self._active_runtime().source_federation_hints

    @source_federation_hints.setter
    def source_federation_hints(self, value: dict[str, dict[str, str]]) -> None:
        self._active_runtime().source_federation_hints = value

    @property
    def roles(self) -> dict[str, dict]:
        return self._active_runtime().roles

    @roles.setter
    def roles(self, value: dict[str, dict]) -> None:
        self._active_runtime().roles = value

    @property
    def schemas(self) -> dict[str, graphql.GraphQLSchema]:
        return self._active_runtime().schemas

    @schemas.setter
    def schemas(self, value: dict[str, graphql.GraphQLSchema]) -> None:
        self._active_runtime().schemas = value

    @property
    def contexts(self) -> dict[str, CompilationContext]:
        return self._active_runtime().contexts

    @contexts.setter
    def contexts(self, value: dict[str, CompilationContext]) -> None:
        self._active_runtime().contexts = value

    @property
    def rls_contexts(self) -> dict[str, RLSContext]:
        return self._active_runtime().rls_contexts

    @rls_contexts.setter
    def rls_contexts(self, value: dict[str, RLSContext]) -> None:
        self._active_runtime().rls_contexts = value

    @property
    def masking_rules(self) -> MaskingRules:
        return self._active_runtime().masking_rules

    @masking_rules.setter
    def masking_rules(self, value: MaskingRules) -> None:
        self._active_runtime().masking_rules = value

    @property
    def tables(self) -> list[dict]:
        # REQ-263/264/265: full table+column dicts (with visible_to) for every registered
        # table, populated once per org at schema-load time; the raw-SQL governance path
        # (pgwire / Flight SQL / airport) derives visible_columns/all_columns from it.
        return self._active_runtime().tables

    @tables.setter
    def tables(self, value: list[dict]) -> None:
        self._active_runtime().tables = value

    @property
    def relationships(self) -> list[dict]:
        # REQ-1132: resolved user-defined relationships (int source/target table ids),
        # published for the raw-SQL governance path's 1-hop meta row scoping.
        return self._active_runtime().relationships

    @relationships.setter
    def relationships(self, value: list[dict]) -> None:
        self._active_runtime().relationships = value

    @property
    def metrics(self) -> dict[str, Any]:
        # REQ-1317: config-declared metric registry (name → Metric), published for the
        # raw-SQL path's `metrics.<name>` query expansion (before governance).
        return self._active_runtime().metrics

    @metrics.setter
    def metrics(self, value: dict[str, Any]) -> None:
        self._active_runtime().metrics = value

    @property
    def schema_build_cache(self) -> dict:
        # Raw registry rows for on-demand domain-filtered schema building. Per-org: domains,
        # tables and column types differ between orgs, so a process-global cache would serve
        # whichever org rebuilt last to every other one.
        return self._active_runtime().schema_build_cache

    @schema_build_cache.setter
    def schema_build_cache(self, value: dict) -> None:
        self._active_runtime().schema_build_cache = value


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
    # Signal the lazily-resolving AuthMiddleware that auth_config may have changed so it re-resolves
    # its provider on the next request (runtime reconfigure — setup wizard / PROVISA_IDP boot path).
    state.auth_reconfig_generation += 1

    # Load config into PG (and create the engine catalogs)
    config = parse_config_dict(raw_config)
    state.config = config
    state.multitenancy = config.multitenancy
    # REQ-1337: multitenancy demands per-org schema isolation (org_<id> schema + search_path on
    # PostgreSQL). The portable/SQLite bootstrap (_init_schema_portable) writes every org into one
    # flat file with no per-org scoping, so a multitenant deployment on a non-PG tenant DB would
    # silently mix orgs' data. Fail loudly at startup instead of letting that combination run.
    if config.multitenancy and getattr(state.tenant_db, "dialect", "postgresql") != "postgresql":
        raise RuntimeError(
            "multitenancy=true requires a PostgreSQL TENANT_DATABASE_URL "
            f"(got dialect={getattr(state.tenant_db, 'dialect', None)!r}); "
            "the portable/SQLite bootstrap has no per-org schema isolation"
        )
    # REQ-1337: org_admin holds the platform_settings right only in a single-tenant deployment.
    # Asserted here rather than in _init_control_planes because the tenancy mode is only known once
    # the config is parsed, which happens after the root org's schema is created.
    from provisa.core.db import apply_tenancy_role_grants as _apply_tenancy_role_grants

    assert state.tenant_db is not None
    await _apply_tenancy_role_grants(state.tenant_db, state.org_id, multitenancy=config.multitenancy)
    if config.multitenancy:
        from provisa.core.tenant_context import TenantContextCache

        state.tenant_context_cache = TenantContextCache()
        tenant_db = state.tenant_db
        assert tenant_db is not None
        async with tenant_db.acquire() as _rls_conn:
            await _init_meta_rls(cast("Connection", _rls_conn))

    # Apply observability config to state
    if config.observability:
        state.otel_compact_cron = config.observability.compact_cron
        state.otel_compact_batch_size = config.observability.compact_batch_size
        state.otel_compact_file_chunk = config.observability.compact_file_chunk
        state.otel_snapshot_retention_hours = config.observability.ops_snapshot_retention_hours
        state.otel_s3_endpoint = config.observability.s3_endpoint

    # Initialize cache store — REDIS_URL env var overrides config
    # Live config export/diff/patch (REQ-1096) is coherent only when the generated/normalized config is
    # canonical — the demo scenario (config built from installer choices), NOT a hand-authored file
    # with comments/ordering a normalized patch could not stay faithful to. Off unless opted in — EXCEPT
    # demo mode, where the generated config is always canonical so the flag MUST be on (REQ-1096).
    from provisa.core.demo import is_demo

    state.config_live_export = bool(
        raw_config.get("live_config_export", False)
        or os.environ.get("PROVISA_LIVE_CONFIG_EXPORT", "").lower() in ("1", "true", "yes")
        or is_demo()
    )

    # REQ-885: hosted-UDF egress allow-list (deny-by-default). Source: server.udf_egress_allowlist
    # in provisa.yaml, augmented by PROVISA_UDF_EGRESS_ALLOWLIST (comma-separated host[:port]).
    _egress = list(raw_config.get("server", {}).get("udf_egress_allowlist", []) or [])
    _egress_env = os.environ.get("PROVISA_UDF_EGRESS_ALLOWLIST", "")
    _egress += [h.strip() for h in _egress_env.split(",") if h.strip()]
    state.udf_egress_allowlist = _egress

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
        # Single-writer cluster invariant: every node loads the byte-identical baked config, but only
        # the primary may DELETE rows. A secondary's upserts are idempotent no-ops (the advisory lock
        # in load_config serializes them), so it stays consistent with the primary; replace mode would
        # let a secondary wipe primary-registered rows, so it is hard-disabled off the primary. Secrets
        # (source passwords) are file-only by design — schema.sql never stores them — so every node must
        # parse this file for source pools; PG holds only the shared, primary-written schema.
        _replace_mode = config_replace_mode(os.environ)
        # Populate the org-prefixed catalog-name map FIRST so load_config's physical registration
        # AND column introspection resolve each source under the name the compiler later emits
        # (state.catalog_for). build_org_runtime does the same before its load_config; the default
        # path must too, else introspect_columns → catalog_for raises KeyError (REQ-1266). Idempotent.
        _populate_source_catalog_names(config)
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


async def _read_org_flags(org_id: str) -> tuple[bool, bool]:  # REQ-1266, REQ-1244
    """``(seeded_demo, isolated_engine)`` for ``org_id``, read from the admin plane.

    The org registry is in-memory with no TTL, so after a process restart the per-request router
    must rebuild an org's runtime on first access. ``seeded_demo`` tells it whether to reload the
    demo sources; ``isolated_engine`` whether to bind a dedicated federation engine
    (REQ-1043/REQ-1067) — no defaults, the row is authoritative."""
    from sqlalchemy import select as _select

    from provisa.core.schema_admin import orgs as _orgs

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        result = await conn.execute_core(
            _select(_orgs.c.seeded_demo, _orgs.c.isolated_engine).where(_orgs.c.id == org_id)
        )
        row = result.fetchone()
    if row is None:
        raise KeyError(f"org {org_id!r} not found in the admin plane — cannot route a request to it")
    return bool(row[0]), bool(row[1])


async def ensure_org_runtime(org_id: str) -> OrgRuntime:  # REQ-1266
    """Get the org's data-plane runtime, building it (once, under the registry lock) on a miss.

    The single seam every entrypoint (HTTP middleware AND the pgwire/bolt/flight/gRPC/MCP protocol
    servers) uses to lazily materialize an org's runtime before binding ``current_org``. The
    registry is in-memory with no TTL, so first access after a process restart rebuilds; whether to
    reload the demo sources / bind a dedicated engine is read authoritatively from the admin plane
    (no default)."""

    async def _builder(oid: str) -> OrgRuntime:
        include_demo, isolated_engine = await _read_org_flags(oid)
        return await build_org_runtime(
            oid, include_demo=include_demo, isolated_engine=isolated_engine
        )

    return await state.org_registry.get_or_build(org_id, _builder)


async def build_org_runtime(
    org_id: str, *, include_demo: bool = False, isolated_engine: bool = False
) -> OrgRuntime:  # REQ-1266
    """Build (or rebuild) the per-org data-plane runtime for ``org_id`` and register it.

    Registers an empty :class:`OrgRuntime` FIRST so the ``AppState`` property shims route every
    build-time ``state.X`` write into it, binds the ``current_org`` ContextVar, then replays the
    per-org subset of the startup build against that runtime: tenant plane (a ``Database`` scoped
    to ``org_<id>`` + ``schema.sql`` + audit schema), the built-in source rows, optionally the demo
    config (its sources land under org-prefixed engine catalogs — REQ-1266), the direct source
    pools, PK resolution, and the per-role compiled schemas.

    Genuinely-global state (``admin_db``, ``federation_engine``, auth/cache/encryption config,
    ``state.config``, ``state.org_id``) is owned by :func:`_load_and_build` and reused as-is — this
    never re-runs it and never mutates ``state.org_id`` (the default/bootstrap org). For the
    default org itself, startup builds the runtime directly through the shims; calling this again
    for the default org is a safe rebuild.
    """
    from provisa.api.startup_seed import _seed_built_in_sources, _resolve_pk_from_sources
    from provisa.core.config_loader import load_control_plane
    from provisa.core.database import Capabilities, create_engine_from_url
    from provisa.core.db import apply_tenancy_role_grants, init_schema
    from provisa.audit.query_log import init_audit_schema

    rt = OrgRuntime(org_id=org_id)
    state.org_registry.set(org_id, rt)
    token = set_current_org(org_id)
    try:
        if isolated_engine:
            # REQ-1043/REQ-1067/REQ-1244: this org runs on its OWN federation engine. Bind a
            # dedicated EngineRuntime BEFORE any source registration below, so the org's catalogs
            # land on ITS engine, never the shared one. The engine kind is the deployment's
            # (build_engine); a Trino engine targets the org's dedicated coordinator
            # (isolated_engine_endpoint), a native engine is a fresh in-process instance —
            # inherently isolated. bind_terminal stores connection kwargs WITHOUT connecting: the
            # dedicated cluster sleeps between sessions (idle-stop, same as the shared engine's
            # front door) and only real traffic — the first query's lazy connect — wakes it.
            from provisa.federation.engine import build_engine
            from provisa.federation.runtime import EngineRuntime

            rt.isolated_engine = True
            rt.federation_engine = EngineRuntime(build_engine(), state)
            rt.federation_engine.bind_terminal()
        # Tenant plane for THIS org: its own Database scoped to org_<id>. The platform/admin plane
        # (state.admin_db) is global and already up — never rebuilt here.
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        cp = load_control_plane(config_path)
        # REQ-1316: reuse the process-wide tenant engine. Database.acquire() issues this org's
        # search_path on every checkout, so the org boundary is the handle, not the pool. Building
        # an engine per org multiplies open connections by tenant count against ONE server and
        # exhausts max_connections (the "remaining connection slots are reserved" failure).
        # A not-schema-capable backend (SQLite/DuckDB) puts each org in its own FILE, so there the
        # engine genuinely is per-org and a shared one would read the wrong database.
        shared_engine = state.tenant_engine
        assert shared_engine is not None, "tenant engine not built; _init_control_planes must run first"
        if Capabilities.for_dialect(shared_engine.dialect.name).schemas:
            tenant_engine = shared_engine
        else:
            tenant_engine = create_engine_from_url(
                cp.resolved_tenant_url(), pool_size=cp.pool_max, max_overflow=cp.max_overflow
            )
        state.tenant_db = Database(tenant_engine, name="org", search_path=f"org_{org_id}")

        schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
        if not schema_sql_path.exists():
            raise RuntimeError(f"control-plane schema.sql missing from the package: {schema_sql_path}")
        await init_schema(state.tenant_db, schema_sql_path.read_text(), org_id=org_id)
        # REQ-1337: org_admin holds platform_settings only in a single-tenant deployment.
        await apply_tenancy_role_grants(state.tenant_db, org_id, multitenancy=state.multitenancy)
        await init_audit_schema(state.tenant_db, org_id=org_id)

        host, port, database, username, _pw = cp.tenant_parts()
        assert database, "control_plane.tenant_url must specify a database"
        await _seed_built_in_sources(host or "", port, database, username or "", org_id=org_id)
        state.source_dsns["provisa-admin"] = f"{host}:{port}/{database}"

        # Demo orgs load the same config the default org runs; its sources are namespaced under
        # org-prefixed engine catalogs (source_catalogs), so identically-named demo sources across
        # orgs never collide in the shared coordinator's catalog namespace.
        config = state.config if include_demo else None
        if config is not None:
            assert state.tenant_db is not None
            # Populate the org-prefixed catalog-name map FIRST so physical registration inside
            # load_config attaches each source under the org's own catalog name (not the bare,
            # default-org name) — the cross-org collision guard (REQ-1266).
            _populate_source_catalog_names(config)
            async with state.tenant_db.acquire() as conn:
                await load_config(
                    config,
                    conn,
                    state.federation_engine,
                    replace=False,
                    catalog_names=rt.source_catalogs,
                )
            await _build_source_pools_and_enums(config)
            await _resolve_pk_from_sources()

        await _rebuild_schemas()

        # REQ-1266: wire this org's MV event loop onto the shared scheduler so its materialized
        # views refresh on their own cadence. Job ids are org-suffixed and each fire binds
        # current_org (register_runtime reads the bound org), so a second org never clobbers the
        # first's jobs. Best-effort — a missing scheduler (tests, engine not connected) skips it.
        scheduler = getattr(state, "_scheduler", None)
        if scheduler is not None:
            from provisa.events.app_wiring import wire_event_loop

            await wire_event_loop(scheduler, state=state, log=logging.getLogger(__name__))
    finally:
        reset_current_org(token)
    return rt


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
    _enc_provider = _enc_cfg.get("provider")
    configure_encryption(
        _enc_provider,
        key_id=_enc_cfg.get("key_id"),
        config=_enc_cfg.get(_enc_provider, {}) if _enc_provider else {},
    )

    # Clear mutable state before rebuild
    state.masking_rules = {}
    # Invalidate the MCP catalog search index (REQ-1008) — the catalog is changing, so the
    # server-lifetime HNSW index is stale; next search_catalog rebuilds it from the new catalog.
    state.mcp_catalog_index = None

    async with state.tenant_db.acquire() as conn:
        _pg = cast("Connection", conn)
        tables = await _fetch_tables(_pg)
        _assert_domain_table_unique(tables)
        relationships = await _fetch_relationships(_pg)

        # Apply schema visibility filters (schema.include_ops / schema.include_metrics)
        _schema_cfg = raw_config.get("schema", {}) if raw_config else {}
        tables = _filter_tables_by_schema_cfg(tables, _schema_cfg, state.source_allowed_domains)

        # Install LISTEN/NOTIFY triggers on registered PostgreSQL tables
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
        # REQ-1373/1377: the DB is the source of truth for tags; refresh them into state.config
        # so consumers (metadata export builder) see admin-created tags, not just YAML-boot ones.
        from provisa.core.repositories import tag as _tag_repo

        _assignment_rows = await _tag_repo.list_assignments(_pg)
        if state.config is not None:
            from provisa.core.models import Tag as _Tag, TagAssignment as _TagAssignment

            state.config.tags = [
                _Tag(
                    id=r["id"],
                    description=r["description"],
                    applies_to=list(r["applies_to"] or []),
                    is_system=bool(r["is_system"]),
                    reason_policy=r["reason_policy"],
                    expires_policy=r["expires_policy"],
                )
                for r in await _tag_repo.list_all(_pg)
            ]
            state.config.tag_assignments = [
                _TagAssignment(
                    tag_id=r["tag_id"],
                    object_type=r["object_type"],
                    source_id=r["source_id"],
                    table_id=r["table_id"],
                    column_name=r["column_name"],
                    relationship_id=r["relationship_id"],
                    table_ref=r["table_ref"],
                    reason=r["reason"],
                    expires_on=r["expires_on"],
                )
                for r in _assignment_rows
            ]
        # REQ-1375: annotate table/column/relationship rows with the deprecation text the
        # GraphQL schema emits as the standard @deprecated(reason:) directive. "No longer
        # supported" is the GraphQL spec's own directive default, used only for legacy
        # assignments that predate the required-reason rule.
        def _deprecation_text(row: dict) -> str:
            text = row["reason"] or "No longer supported"
            return f"{text} (removal: {row['expires_on']})" if row["expires_on"] else text

        _dep_rows = [r for r in _assignment_rows if r["tag_id"] == "deprecated"]
        _dep_tables = {
            r["table_id"]: _deprecation_text(r) for r in _dep_rows if r["object_type"] == "table"
        }
        _dep_columns = {
            (r["table_id"], r["column_name"]): _deprecation_text(r)
            for r in _dep_rows
            if r["object_type"] == "column"
        }
        _dep_rels = {
            r["relationship_id"]: _deprecation_text(r)
            for r in _dep_rows
            if r["object_type"] == "relationship"
        }
        for _tbl in tables:
            if _tbl["id"] in _dep_tables:
                _tbl["deprecation_reason"] = _dep_tables[_tbl["id"]]
            for _col in _tbl.get("columns") or []:
                _dep = _dep_columns.get((_tbl["id"], _col.get("column_name")))
                if _dep is not None:
                    _col["deprecation_reason"] = _dep
        for _rel in relationships:
            if _rel.get("id") in _dep_rels:
                _rel["deprecation_reason"] = _dep_rels[_rel["id"]]
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
        # Publish the full DB source map so NativeEngineBackend._attach_registered can attach
        # dynamically registered sources that are not in state.config (YAML-loaded only).
        state.runtime_sources = sources
        roles = [
            dict(r._mapping)
            for r in (
                await conn.execute_core(
                    # REQ-1174: include rate_limit so state.roles carries the per-role rate +
                    # query-complexity limits the data endpoint enforces.
                    select(
                        _roles_t.c.id,
                        _roles_t.c.capabilities,
                        _roles_t.c.domain_access,
                        _roles_t.c.rate_limit,
                    )
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

        # REQ-1317/1319: the metric registry feeds schema generation and raw-SQL expansion.
        # Read from the DB — the settled registry: the config loader upserts config-declared
        # metrics into it, and admin mutations (upsertMetric, registerFact) write it directly.
        # Publishing state.config.metrics here instead would hide every runtime-registered
        # metric from `metrics.<name>` queries until the next config reload.
        from provisa.core.models import Metric as _MetricModel
        from provisa.core.repositories import metric as _metric_repo

        _metric_models = [
            _MetricModel(
                name=r["name"],
                expression=r["expression"],
                datatype=r["datatype"],
                description=r["description"],
                ai_context=r["ai_context"],
                visible_to=list(r["visible_to"]),
                from_fact=r["from_fact"],
            )
            for r in await _metric_repo.list_all(conn)
        ]
        _metric_dicts = [m.model_dump() for m in _metric_models]

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
            metrics=_metric_dicts,  # REQ-1319
        )

    # REQ-263, REQ-264, REQ-265: publish filtered table+column dicts for raw-SQL governance
    # (pgwire / Flight SQL / airport). build_governance_context reads state.tables to derive
    # visible_columns and all_columns; without this assignment the list is always empty and
    # column visibility + masking are silently skipped on every raw-SQL transport.
    state.tables = tables
    # REQ-1132: publish the resolved relationship registry alongside tables so the raw-SQL
    # governance path can compute 1-hop meta row scoping.
    state.relationships = relationships
    # REQ-1317: publish the DB-backed metric registry alongside tables so the raw-SQL
    # path can expand `metrics.<name>` queries into governed aggregates (loaded above,
    # same registry the admin surfaces read — runtime-registered metrics included).
    state.metrics = {m.name: m for m in _metric_models}

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
        "metrics": _metric_dicts,  # REQ-1319
    }
    state.schema_version += 1
    await _finalize_rebuild_state(_rebuild_log)
    # REQ-1072: the governed model just changed, so the external catalog is now stale. This is
    # the one chokepoint every model mutation passes through, which is why the event is posted
    # here rather than at each mutation — a new mutation cannot forget to publish.
    from provisa.api.metadata_export.publishing import notify_model_changed

    await notify_model_changed(state.active_org_id, reason="schema rebuild")


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

    # REQ-1267: when PROVISA_IDP names an identity provider (e.g. firebase from the GCP/installer
    # deploy) but the loaded config carries no auth section, configure it now — BEFORE any request
    # reaches the middleware — so the server enforces auth from its first request instead of serving
    # an unsecured admin window until the UI happens to call /setup/status.
    from provisa.api.setup_router import _auto_configure_idp, _idp_override

    _idp = _idp_override()
    if _idp and state.admin_db is not None and state.auth_config is None:
        await _auto_configure_idp(_idp, state.admin_db)

    _prewarm_govdata_jvm(_log)

    await _start_background_tasks(_log)

    await _start_servers(_log)

    # Prime the lazy per-request paths in the background and flip /ready when warm. Background (not
    # awaited) so /health and /live serve immediately; a readiness-gated launcher waits on /ready.
    state._warmup_task = asyncio.create_task(_warmup_readiness(_log))

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

    # Cancel the readiness warmup probe (it may still be priming if shutdown raced boot)
    if state._warmup_task:
        state._warmup_task.cancel()
        try:
            await state._warmup_task
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
    # REQ-1244: every org with a dedicated federation engine owns a live terminal — close each,
    # then the shared engine (the default runtime's, reached by the unrouted property below).
    for _oid in state.org_registry.all_org_ids():
        _rt = state.org_registry.get(_oid)
        if _rt is not None and _rt.federation_engine is not None and _oid != state.org_id:
            with tolerate_shutdown_failure(f"org {_oid} federation engine close"):
                # close() reaches its terminal through the routed state shims, so the org must
                # be bound or the shims would resolve the SHARED engine's connection.
                _tok = set_current_org(_oid)
                try:
                    _rt.federation_engine.close()
                finally:
                    reset_current_org(_tok)
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

    from provisa.api.errors import ApiError as _ApiError

    @app.exception_handler(_ApiError)
    async def _api_error_handler(_req: _Request, exc: _ApiError):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # Hybrid server i18n (REQ-1350): English detail + stable code/params
        # so the UI can render a localized message from its own catalog.
        return _JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "code": exc.code, "params": exc.params},
            headers=exc.headers,
        )

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

    # REQ-693: high-security mode — refuse plaintext data requests lacking a client-side
    # decryption key. Reads state.security_high (set from config at load time).
    from provisa.security.high_security import HighSecurityMiddleware

    app.add_middleware(HighSecurityMiddleware, state=state)

    # REQ-1266: per-request org routing. Added BEFORE wire_auth so AuthMiddleware (added later →
    # outermost → runs first) has already resolved request.state.active_org_id when this runs. Binds
    # the current_org ContextVar for the selected org and lazily builds its data-plane runtime on a
    # miss (e.g. after a process restart — the registry is in-memory, no TTL).
    #
    # REQ-1355: registered UNCONDITIONALLY. This used to sit behind `if state.multitenancy:`, which
    # is always False here — the flag is assigned in _load_and_build, which lifespan runs AFTER
    # create_app returns — so the middleware never installed and no HTTP request ever bound its org.
    # _active_runtime() then resolved the DEFAULT org's data plane for every caller, i.e. a cross-org
    # read. The single-org case needs no guard: the dispatch below no-ops when the request carries no
    # org or carries the default one.
    # Plain ASGI middleware, not starlette.middleware.base.BaseHTTPMiddleware: that class relays
    # the inner app's response body through a background task + anyio memory stream, which fails
    # to signal completion to the client for unbounded StreamingResponse bodies (SSE subscriptions,
    # REQ-219) even after the inner generator has fully finished — the connection hangs open. A
    # pure ASGI middleware calls the inner app's `send` directly, so no such relay exists.
    class _OrgRoutingMiddleware:
        def __init__(self, app):
            self.app = app

        async def __call__(self, scope, receive, send):
            if scope["type"] != "http":
                await self.app(scope, receive, send)
                return

            request_state = scope.setdefault("state", {})
            active_org = request_state.get("active_org_id")
            # No org bound (unauthenticated, or a default-org request): the AppState shims resolve
            # the default-org runtime. Never fabricate a non-default org here.
            if active_org is None or active_org == state.org_id:
                await self.app(scope, receive, send)
                return
            # Keep existing tenant cache-key call sites (which read request.state.tenant_id)
            # pointed at the same id space as the org router.
            request_state["tenant_id"] = active_org

            await ensure_org_runtime(active_org)
            token = set_current_org(active_org)
            try:
                await self.app(scope, receive, send)
            finally:
                reset_current_org(token)
            # REQ-462: tag the trace with the org that served the request. Folded in from the
            # former _TenantSpanMiddleware, which was registered under the same dead guard.
            try:
                from opentelemetry import trace as _trace

                _span = _trace.get_current_span()
                if _span.is_recording():
                    _span.set_attribute("org_id", active_org)
            except (ImportError, AttributeError):
                # Best-effort span decoration: tolerate an absent OTel install or a no-op shim
                # span lacking is_recording/set_attribute. Never break a request for a tag.
                pass

    app.add_middleware(_OrgRoutingMiddleware)

    # Conditionally add auth middleware and routes
    from provisa.auth.wiring import wire_auth

    # ActiveOrgPool, not state.tenant_db: the middleware outlives the request, and the tenant
    # control plane it must read is whichever org the request binds (REQ-1266).
    wire_auth(app, state.auth_config, db_pool=ActiveOrgPool(), admin_pool=state.admin_db)

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
        # REQ-1137: post-trial license nag on the REST surface via an out-of-band header — never
        # touches the response body or any schema-typed field, never gates the request.
        from provisa.licensing import emit as _lic_emit

        if _lic_emit.should_nag():
            st = _lic_emit.current_state()
            if st is not None:
                response.headers["X-Provisa-License-Notice"] = st.nag_text.replace("\n", " ")
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
    from provisa.api.admin.lineage_router import router as lineage_router  # REQ-1160

    app.include_router(lineage_router)
    from provisa.api.admin.crawl_router import router as crawl_router

    app.include_router(crawl_router)
    from provisa.api.admin.settings_router import router as settings_router

    app.include_router(settings_router)
    from provisa.api.admin.ossie_router import router as ossie_router  # REQ-1316, REQ-1321

    app.include_router(ossie_router)
    from provisa.api.admin.security_router import router as security_router

    app.include_router(security_router)
    from provisa.api.admin.ai_models_router import router as ai_models_router

    app.include_router(ai_models_router)
    from provisa.api.admin.metadata_export_router import (  # REQ-1074
        router as metadata_export_router,
    )

    app.include_router(metadata_export_router)
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
    from provisa.api.mcp.status import router as mcp_status_router

    app.include_router(mcp_status_router)

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

    # REQ-1355: included unconditionally. The former `if state.multitenancy:` guard read the flag
    # before _load_and_build assigns it, so it was always False and the router never mounted —
    # every control-plane endpoint 404'd on the deployments that need it. Multitenancy is enforced
    # per request by the router's own _require_multitenancy(), which 403s when it is off.
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
            except (SQLAlchemyError, OSError, asyncio.TimeoutError):
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
    async def readiness(response: Response):  # noqa: F841  # pyright: ignore[reportUnusedFunction]
        # Readiness = the boot warmup probe has run (store attached, engine terminal warm), so the
        # first real query is not cold. 503 while warming holds traffic (and the launcher's browser
        # open) until then. Distinct from /live (process up) and /health (dependencies reachable).
        if not state.is_warm:
            response.status_code = 503
            return {"status": "warming"}
        return {"status": "ready"}

    return app
