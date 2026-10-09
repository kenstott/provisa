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
import hashlib
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response

from provisa.core import model_change
from provisa.core.config_location import config_path_str
from provisa.core.connection_loop import CrossLoopLock, LongLived, run_lifecycle_work
from provisa.api.data.endpoint import router as data_router
from provisa.api.data.redirect_unwrap import router as redirect_unwrap_router
from provisa.api.data.endpoint_dev import router as dev_router
from provisa.api.data.endpoint_grpc_proxy import router as grpc_proxy_router
from provisa.api.data.sdl import router as sdl_router
from provisa.api.app_loaders import (
    _META_TABLE_ALIAS,
    _apply_server_and_engine_config,
    _build_and_register_schemas,
    _load_kept,
    _build_source_pools_and_enums,
    _populate_source_catalog_names,
    _init_ingest_engines,
    _init_meta_rls,
    _load_graphql_remote_sources_from_db,
    _load_grpc_remote_sources_from_db,
    _check_fakes,
    _load_masking_rules,
    _load_mv_and_views_config,
    _load_openapi_specs,
    _load_tracked_functions_and_webhooks,
    _process_kafka_sources,
    _setup_approval_hook,
)
from provisa.api.app_rebuild import (
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
    _seed_sandbox_org,
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
from provisa.core.config_loader import (
    attach_store_sources,
    is_seeded,
    parse_store_raw,
    rebuild_from_config,
    seed_config,
    store_config,
    store_raw,
    parse_config_dict,
    read_config_with_includes,
)
from provisa.core.database import Database
from provisa.core import domain_policy, secrets_store
from provisa.executor import redirect as _redirect
from provisa.core.schema_org import (
    domains as _domains_t,
    naming_rules as _naming_rules_t,
    registered_tables as _registered_tables_t,
    roles as _roles_t,
    sources as _sources_t,
)
from provisa.executor.pool import SourcePool
from provisa.api.org_runtime import (
    ActiveOrgPool,
    OrgRegistry,
    OrgRuntime,
    runtime_key,
)
from provisa.core.runtime_gone import RuntimeNotBuilt, left_to_the_next_runtime
from provisa.core.request_context import (
    active_env,
    current_env,
    current_org,
    reset_current_env,
    require_current_org,
    reset_current_org,
    set_current_env,
    set_current_org,
)
from provisa.core.environments import PROD, org_schema
from provisa.compiler.mask_inject import MaskingRules
from provisa.cache.store import CacheStore, NoopCacheStore, RedisCacheStore
from provisa.api.admin.db_queries import (
    fetch_tables as _fetch_tables,
    fetch_relationships as _fetch_relationships,
)
from provisa.api.otel_setup import setup_otel as _setup_otel, shutdown_otel as _shutdown_otel
from provisa.mv.registry import MVRegistry
from provisa.apq.cache import APQCache, NoopAPQCache
from provisa.api_source.models import ApiEndpoint as ApiEndpoint, ApiSource as ApiSource
from provisa.core.models import ProvisaConfig  # noqa: F401
from typing import TYPE_CHECKING, Any, NamedTuple, cast  # noqa: F401

if TYPE_CHECKING:
    from provisa.compiler.compiled_query_cache import CompiledQueryCache
    from provisa.cache.hot_tables import HotTableManager
    from provisa.core.tenant_context import TenantContextCache
    from provisa.kafka.window import KafkaTableConfig
    from provisa.core.models import Source
    from provisa.core.database import Connection
    from provisa.federation.replica_address import ReplicaRoutes
    from sqlalchemy.engine import Engine
    import graphql

log = logging.getLogger(__name__)


class AppState:
    """Shared application state populated at startup."""

    # Control plane handles (SQLAlchemy-backed), two independent engines:
    # ``tenant_db`` is the per-org/tenant control plane (schema-scoped);
    # ``admin_db`` is the global platform control plane (orgs/users/invites/
    # billing), backed by its own SQLAlchemy URI.
    admin_db: Database | None = None
    # REQ-1916/1922: the PLATFORM STATE STORE's handle (provisa/core/platform_state): over the
    # platform database, holding only the deployment's own operating state (the node list).
    platform_state_db: Database | None = None
    # REQ-1316: ONE tenant-plane Engine shared by every org runtime on a schema-capable
    # backend. Database.acquire() issues the org's search_path on each checkout, so orgs need
    # separate handles, never separate pools. A pool per org multiplies connections by tenant
    # count and exhausts the server's max_connections (Cloud SQL db-f1-micro caps at 25 — two
    # orgs at pool_size=5/overflow=5 already blow past it).
    tenant_engine: Any | None = None  # Engine; Any avoids the runtime import here
    # engine_conn / engine_conn_kwargs / federation_engine are routed PROPERTIES (REQ-1244):
    # they live on the per-org OrgRuntime and resolve through the current_org ContextVar, falling
    # through to the default-org (shared) runtime for every org without a dedicated engine.
    flight_client: Any | None = None  # pyarrow.flight.FlightClient
    # REQ-1518: on a deployment that provisions its engines the Zaychik proxy is a sidecar in the
    # shard's pod, so there is no single address to hold — one connection per shard, keyed by the
    # coordinator generation it was opened against. `flight_client` above is the OTHER deployments'
    # single proxy, which sits beside the control plane at a stable name.
    flight_clients: dict[str, tuple[int, Any]] = {}  # shard → (generation, ADBC connection)
    # runtime_sources is org-routed; see the property below.
    # schema_build_cache is org-routed; see the property below.
    schema_version: int = (
        0  # bumped on every _rebuild_schemas; used by clients for cache invalidation
    )
    schema_boot_id: str = (
        ""  # random UUID set at startup; combined with schema_version for cache keys
    )
    # The DEPLOYMENT's default response TTL (cache.default_ttl in the config file). An org may
    # narrow it; the routed `response_cache_default_ttl` property below resolves the org's value
    # over this one. Assigned by _load_and_build, never read directly by the query path.
    deployment_cache_default_ttl: int = 300
    # REQ-1008: server-lifetime MCP catalog search index (DuckDB VSS HNSW), built lazily on first
    # search_catalog and invalidated (set None) on catalog reload. Any so the mcp package owns the type.
    mcp_catalog_index: Any = None
    mv_registry: MVRegistry = MVRegistry()
    _mv_refresh_task: LongLived | None = None
    proto_files: dict[str, str] = {}  # role_id → .proto content
    # The one SERVED wire descriptor: union of every role's surface (see
    # app_loaders._build_and_register_schemas). Governance is per-request, not per-descriptor.
    wire_proto: str | None = None
    table_path_maps: dict[
        str, dict[str, dict]
    ] = {}  # role_id → {gql_field_name → {schema_name, table_name, domain_id}}
    _grpc_server: Any | None = None
    _flight_server: Any | None = None  # ProvisaFlightServer
    _flight_relay: Any | None = None  # FlightRelay: the advertised Flight port (REQ-1900)
    # The other listeners this app starts and, at shutdown, stops (what an app starts, it stops).
    _pgwire_server: Any | None = None  # ProvisaServer
    _bolt_listener: Any | None = None  # BoltListener
    _airport_server: Any | None = None  # ProvisaAirportServer
    _airport_relay: Any | None = None  # FlightRelay: the advertised airport port (REQ-1900)
    _http_listener: Any | None = None  # WorkerHttpListener: this worker's own HTTP socket
    kafka_windows: dict[str, str] = {}  # source_id → default_window (e.g. "1h")
    kafka_bootstrap: dict[str, str] = {}  # source_id → its brokers, secrets resolved (REQ-812)
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
    # REQ-1905: server-wide Arrow Flight concurrency ceiling — protects pgwire/Bolt/gRPC/GraphQL
    # (which share the asyncio default executor with Flight) from being starved by a burst of
    # long-running Flight scans. Distinct from max_flight_streams (REQ-369, per-role fairness
    # among Flight callers only). Computed once at startup; see build_rate_limiter call site.
    flight_global_cap: int | None = None
    approval_hook: Any | None = None  # REQ-247: ApprovalHook instance (None = disabled)
    approval_hook_config: Any | None = None  # REQ-247: ApprovalHookConfig
    table_approval_hooks: dict[int, bool] = {}  # table_id → approval_hook flag
    source_approval_hooks: dict[str, bool] = {}  # source_id → approval_hook flag
    api_endpoints: dict[tuple[str, str], Any] = {}  # (source_id, table_name) → ApiEndpoint
    api_sources: dict[str, Any] = {}  # source_id → ApiSource
    hot_manager: HotTableManager | None = None
    _hot_refresh_task: LongLived | None = None
    _replica_hot_task: LongLived | None = None  # REQ-826: the Hot promotion evaluation
    # Readiness (REQ /ready): False until the boot warmup probe has primed the lazy per-request paths
    # (materialize-store attach + a warm engine terminal). /ready returns 503 while this is False so a
    # launcher/orchestrator holds traffic — and the browser open — until the first interaction is warm.
    is_warm: bool = False
    _warmup_task: LongLived | None = None
    apq_cache: APQCache = NoopAPQCache()  # Phase AN: Automatic Persisted Queries
    apq_ttl: int = 86400  # REQ-289: APQ cache TTL (apq.ttl config / PROVISA_APQ_TTL env)
    hostname: str = "localhost"  # publicly reachable hostname (PROVISA_HOSTNAME)
    engine_session_hints: dict[
        str, str
    ] = {}  # FTE session properties injected into every the engine query
    server_cfg: dict = {}  # raw server section from provisa.yaml
    server_limits: dict = {}  # resolved query/request limits (from config + env overrides)
    security_high: bool = (
        False  # REQ-693: high-security mode (pgwire off, data endpoints KMS-gated)
    )
    # REQ-885: deny-by-default egress allow-list for hosted http/grpc UDFs. host or host:port
    # entries; empty ⇒ all external egress denied (loopback/Provisa pgwire is always allowed).
    udf_egress_allowlist: list[str] = []
    pg_enum_types: dict = {}  # pg_name → GraphQLEnumType (REQ-221)
    _org_id: str = "default"  # REQ-697: org schema scope (ORG_ID env var); see the org_id property
    graphql_remote_sources: dict[str, dict] = {}  # source_id → GraphQL remote registration
    openapi_specs: dict[str, dict] = {}  # source_id → OpenAPI spec registration
    grpc_remote_sources: dict[str, dict] = {}  # source_id → gRPC remote registration
    # Phase AS — Ingest sources
    ingest_engines: dict[str, Engine] = {}  # source_id → Engine
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
    _scheduler_holder: Any | None = None  # SchedulerHolder: one worker runs the jobs (REQ-1900)
    # SchedulerHolder: one worker of this node's region runs its region's jobs (REQ-1922); the
    # deployment's own holder when the platform declares no regions.
    _region_holder: Any | None = None
    global_gql_naming_convention: str = (
        "apollo_graphql"  # runtime override; set via updateNamingConvention
    )
    global_sql_naming_convention: str = "snake"
    otel_compact_cron: str = "* * * * *"  # cron for Parquet→Iceberg compaction
    otel_compact_batch_size: int = 1000  # rows per INSERT batch during compaction
    otel_compact_file_chunk: int = 50  # Parquet files processed per compaction chunk
    otel_compact_max_files_per_run: int = 500  # per-signal file budget for one compaction run
    otel_s3_endpoint: str = "http://minio:9000"  # MinIO/S3 endpoint for compaction
    multitenancy: bool = False
    tenant_context_cache: TenantContextCache | None = None
    kafka_table_physical: dict[
        str, str
    ] = {}  # virtual gql table → physical the engine table (Kafka sources)
    # REQ-1919: the configuration the process runs — the file's settings, with every model
    # section read from the default org's model store (config_loader.store_config).
    config: Any = None
    # REQ-1919: the deployment's configuration file as it was read at startup, as written (its
    # settings are ``config``'s) and parsed (``seed_config``, what a new demo org is seeded from).
    raw_config: dict | None = None
    seed_config: Any = None
    # Live config export/diff/patch is opt-in (REQ-164) — coherent only where the generated/normalized
    # config is canonical (the demo), not a hand-authored file. Gates the boot snapshot + endpoints.
    config_live_export: bool = False
    # Normalized config generated ONCE at end of boot — after all runtime auto-derivation (FK tracking,
    # graphql-remote registration). The admin config-diff uses it as the baseline so it shows only
    # changes made SINCE startup, not derived entities that were never in the file (REQ-164).
    config_boot_snapshot: str | None = None
    otel_snapshot_retention_hours: int | None = None  # Iceberg snapshot expiry hours

    def __init__(self) -> None:
        # Mandatory terminal-execution binding (REQ-825, REQ-840): every AppState is born with its
        # federation engine, so the query path always routes through it — there is no unbound state
        # and no per-call-site fallback. The runtime reads self.engine_conn lazily at execute time,
        # so binding before the connection exists is correct; startup may swap the reference engine.
        from provisa.federation.engine import build_engine  # $PROVISA_ENGINE selects
        from provisa.federation.runtime import EngineRuntime

        # REQ-1266: per-request multi-org data plane. The routed maps below (source
        # pools, roles, compiled schemas/contexts, catalog names, masking, …) live on
        # a per-org OrgRuntime; the properties resolve the runtime of the org the work is bound
        # to and refuse unbound work. The deployment org's runtime is registered here so startup,
        # which binds that org once its id is resolved, has a target to build.
        self.org_registry = OrgRegistry()
        deployment = OrgRuntime(org_id=self.org_id)
        self.org_registry.set(self.org_id, deployment)
        # The deployment-wide stores below are held on the deployment org's runtime and written
        # there directly: nothing is bound yet, and the routed setters serve bound work only.
        # The deployment's response cache, until startup builds the configured one (REQ-829).
        deployment.response_cache_store = NoopCacheStore()
        # REQ-1909: every AppState is born with its live-read permit store — embedded (per process)
        # until startup rebinds it to the deployment's Redis once redis_url is resolved.
        from provisa.federation.live_concurrency import LivePermitStore

        self.live_permit_store = LivePermitStore(None)
        # REQ-826: and with its Hot-count store, rebound to the deployment's Redis the same way.
        from provisa.federation.replica_hot import HotCounts

        deployment.hot_counts = HotCounts(None)

        # REQ-1244: the SHARED engine every org without a dedicated binding resolves to.
        deployment.federation_engine = EngineRuntime(build_engine(), self)

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
            # The runtime says which org it serves: whoever binds a request context from it (the
            # config watcher's reloads, the Hot promotion evaluation) must bind THIS id, the one
            # requests bind and the one it is registered under — not the compile-time one.
            rt.org_id = value
            self.org_registry.set(value, rt)
            self.org_registry.invalidate(old)

    def _active_runtime(self) -> OrgRuntime:
        """The OrgRuntime for the org AND environment the current work is bound to.

        REQ-1266: every org is served by its own runtime and by no other. There is no default:
        work with no org bound is refused, and work bound to an org whose runtime is not built in
        this process (never yet, or dropped by an engine wake or an org deletion while a job for
        it kept running) is refused by name -- the entrypoint or job binds its org and builds the
        runtime first (ensure_org_runtime). Answering either from the deployment org's runtime
        would serve that org's roles, model and data to work that is not its own.

        REQ-1488/REQ-1529: the environment is part of the identity of a runtime, not a variation
        within one. A branch holds a separate copy of the model in a separate schema and reaches
        its sources through connections of its own, so serving it from its base's
        runtime would hand it the base's pools and compiled schemas. ``runtime_key`` keys prod on
        the bare org id, so an org that never created an environment resolves exactly as before."""
        org_id = require_current_org()
        env = current_env.get()
        rt = self.org_registry.get(runtime_key(org_id, env))
        if rt is None:
            if env is not None and env != PROD:
                raise RuntimeNotBuilt(
                    org_id,
                    env,
                    f"no runtime built for environment {env!r} of org {org_id!r}; "
                    "ensure_org_runtime must build it before the environment is bound",
                )
            raise RuntimeNotBuilt(
                org_id,
                None,
                f"no runtime built for org {org_id!r}; ensure_org_runtime must build it "
                "before work is bound to the org",
            )
        return rt

    def _default_runtime(self) -> OrgRuntime:
        rt = self.org_registry.get(self.org_id)
        assert rt is not None, "default-org runtime missing — AppState not initialized"
        return rt

    @property
    def shared_federation_engine(self) -> Any:
        """REQ-1243/REQ-1244: the deployment's SHARED engine -- the pooled lane every org without
        a dedicated one runs on, held on the deployment org's runtime. Named explicitly for the
        deployment-level work that serves no org (writing the engine's config at app creation)."""
        return self._default_runtime().federation_engine

    @property
    def platform_model_db(self) -> Database | None:
        """REQ-1297/REQ-1327: the model store the platform plane's roles are read from -- the
        deployment org's, where the platform grants live. Named explicitly for a request that acts
        in no org (a signed-in user with no membership yet); an org-bound request reads its own."""
        return self._default_runtime().model_db

    @property
    def platform_roles(self) -> dict[str, dict]:
        """REQ-1327/REQ-1337: the role definitions a caller's PLATFORM rights (cross_org) are read
        from before any org is bound -- the deployment org's, where the platform grants live. Named
        explicitly rather than routed: the org the caller acts in is what these rights decide."""
        return self._default_runtime().roles

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
        """The org id the current work is bound to; refused when none is bound (REQ-1266)."""
        return require_current_org()

    @property
    def active_isolated_org(self) -> str | None:
        """The active org's id IF that org runs a dedicated federation engine, else ``None`` —
        the seam engine lifecycle code (trino_lifecycle.provision) uses to resolve the dedicated
        coordinator endpoint without knowing about org routing."""
        rt = self._active_runtime()
        return rt.org_id if rt.isolated_engine else None

    @property
    def active_engine_endpoint(self) -> tuple[str, int] | None:
        """REQ-1412: the coordinator endpoint the active org OPERATES ITSELF (external engine), or
        ``None`` when the endpoint is the deployment's to resolve (shared or SaaS-dedicated)."""
        return self._active_runtime().engine_endpoint

    @property
    def active_engine_url(self) -> str | None:
        """REQ-1418: the DSN of the engine the active org OPERATES ITSELF, or ``None`` when the
        engine URL is the deployment's to resolve. ``configured_engine_url`` prefers this, which is
        what lets one org run on Databricks while the deployment runs Trino.

        Reads the registry directly rather than through ``_active_runtime`` because this is the one
        shim the engine layer calls from inside ``build_engine`` — which runs at startup, BEFORE the
        default org's runtime is registered, and in processes that never build one (desktop,
        tooling). An unregistered runtime means no org has claimed an engine of its own; that is the
        answer, not a value gone missing."""
        bound = current_org.get()
        if bound is None:
            return None
        rt = self.org_registry.get(bound)
        return rt.engine_url if rt is not None else None

    @property
    def tenant_db(self) -> Database | None:
        """The acting org's STATE store (this region's operating state) — REQ-1920/1922."""
        return self._active_runtime().tenant_db

    @property
    def foreign_regions(self) -> dict[str, Any]:
        """The active org's other regions (REQ-1922; ``OrgRuntime.foreign_regions``)."""
        return self._active_runtime().foreign_regions

    @property
    def record_db(self) -> Database | None:
        """The acting org's RECORD in this region (query_audit_log, query_sla_log) — REQ-1922."""
        return self._active_runtime().record_db

    @record_db.setter
    def record_db(self, value: Database | None) -> None:
        self._active_runtime().record_db = value

    @property
    def model_db(self) -> Database | None:
        """The acting org's MODEL store (its model, shared across its regions) — REQ-1919."""
        return self._active_runtime().model_db

    @model_db.setter
    def model_db(self, value: Database | None) -> None:
        self._active_runtime().model_db = value

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
    def runtime_sources(self) -> dict[str, dict]:
        """The active runtime's full source-row map, as _rebuild_schemas published it.

        REQ-1488/REQ-1529: routed rather than process-global because a branch's rows are not its
        base's — the connection values on them are whatever the branch's bindings resolved to, and
        a shared map would hand the last runtime that rebuilt its sources to every other one.
        """
        return self._active_runtime().runtime_sources

    @runtime_sources.setter
    def runtime_sources(self, value: dict[str, dict]) -> None:
        self._active_runtime().runtime_sources = value

    @property
    def source_binding_env(self) -> dict[str, str]:
        """source_id → the environment, for each source whose connection was given in it (REQ-1529,
        REQ-1942): a connection copied from the parent is not one, so a Direct mutation never
        writes through it to the parent's data. Whether the writer may write is their roles'
        answer, not this map's (REQ-1539). Empty for prod.
        """
        return self._active_runtime().source_binding_env

    @source_binding_env.setter
    def source_binding_env(self, value: dict[str, str]) -> None:
        self._active_runtime().source_binding_env = value

    @property
    def tracked_functions(self) -> dict[str, dict]:
        """The active environment's commands, by the names every surface calls them by."""
        return self._active_runtime().tracked_functions

    @tracked_functions.setter
    def tracked_functions(self, value: dict[str, dict]) -> None:
        self._active_runtime().tracked_functions = value

    @property
    def tracked_webhooks(self) -> dict[str, dict]:
        """The active environment's webhooks, by the names every surface calls them by."""
        return self._active_runtime().tracked_webhooks

    @tracked_webhooks.setter
    def tracked_webhooks(self, value: dict[str, dict]) -> None:
        self._active_runtime().tracked_webhooks = value

    @property
    def undefined_commands(self) -> dict[str, str]:
        """Command name -> why it is not defined in the active environment (REQ-1942)."""
        return self._active_runtime().undefined_commands

    @undefined_commands.setter
    def undefined_commands(self, value: dict[str, str]) -> None:
        self._active_runtime().undefined_commands = value

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
                f"{require_current_org()!r} — source not registered for this org"
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
    def ephemeral(self) -> bool:
        """REQ-1621: whether the environment being served has an expiry. Read-only — it is decided
        when the runtime is built (``env_registry.expires_at``) and no request may change it."""
        return self._active_runtime().ephemeral

    @property
    def live_engine(self) -> Any:
        """REQ-1266: the bound org's live-query engine (OrgRuntime.live_engine)."""
        return self._active_runtime().live_engine

    @live_engine.setter
    def live_engine(self, value: Any) -> None:
        self._active_runtime().live_engine = value

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
    def role_build_inputs(self) -> dict:
        """What any role's surface is built from (OrgRuntime.role_build_inputs)."""
        return self._active_runtime().role_build_inputs

    @role_build_inputs.setter
    def role_build_inputs(self, value: dict) -> None:
        self._active_runtime().role_build_inputs = value

    @property
    def meta_roles(self) -> dict:
        """meta-role id → the held roles it acts as (OrgRuntime.meta_roles)."""
        return self._active_runtime().meta_roles

    @property
    def view_context(self) -> CompilationContext | None:
        """The model-wide context view SQL is lowered against (OrgRuntime.view_context)."""
        return self._active_runtime().view_context

    @view_context.setter
    def view_context(self, value: CompilationContext | None) -> None:
        self._active_runtime().view_context = value

    @property
    def rls_contexts(self) -> dict[str, RLSContext]:
        return self._active_runtime().rls_contexts

    @rls_contexts.setter
    def rls_contexts(self, value: dict[str, RLSContext]) -> None:
        self._active_runtime().rls_contexts = value

    @property
    def role_chains(self) -> dict[str, list[str]]:
        # REQ-1677: each role's inheritance chain, nearest first, as folded into this build.
        return self._active_runtime().role_chains

    @role_chains.setter
    def role_chains(self, value: dict[str, list[str]]) -> None:
        self._active_runtime().role_chains = value

    @property
    def compiled_query_cache(self) -> "CompiledQueryCache":
        # REQ-1877: per-org compiled-query-outcome cache — see provisa/compiler/compiled_query_cache.py.
        return self._active_runtime().compiled_query_cache

    @property
    def routing_cache(self) -> "CompiledQueryCache":
        # REQ-1877 (routing addendum): per-org routing-decision cache — see
        # provisa/compiler/compiled_query_cache.py's "ROUTING-DECISION CACHING" section.
        return self._active_runtime().routing_cache

    @property
    def cypher_label_maps(self) -> dict:
        # REQ-1877: per-org, current-generation Cypher label maps — see
        # provisa/api/rest/cypher_plan.py.
        return self._active_runtime().cypher_label_maps

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
    def replica_routes(self) -> ReplicaRoutes:
        # REQ-1912: the active runtime's replica-served tables, as _rebuild_schemas published them.
        return self._active_runtime().replica_routes

    @replica_routes.setter
    def replica_routes(self, value: ReplicaRoutes) -> None:
        self._active_runtime().replica_routes = value

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

    @property
    def model_stamp(self) -> int | None:
        """The control plane's model stamp the acting runtime's copy was loaded at (REQ-1914).
        Read-only here: the schema build is the one writer, on the runtime it built."""
        return self._active_runtime().model_stamp

    @property
    def settings_overrides(self) -> dict:
        """The active org's ``org_settings`` rows (REQ-1349). Empty when it has overridden nothing.

        REQ-1914: read from the runtime's copy, never from the control plane. A setting the org
        changed through ANOTHER worker process advances the org's ``settings`` config stamp, and
        this process's config watcher reloads the copy within the reload interval
        (provisa/api/model_reload.py). The worker that made the change sets the copy itself
        (below) and sees it at once."""
        return self._active_runtime().settings_overrides

    @settings_overrides.setter
    def settings_overrides(self, value: dict) -> None:
        self._active_runtime().settings_overrides = value

    def _cache_runtime(self, field_name: str) -> OrgRuntime:
        """The runtime whose cache store answers for the active org: its own, when its region
        named one (REQ-1922); otherwise the default runtime, which holds the deployment's. With no
        platform regions no runtime has its own, and every org is served the deployment's cache
        (REQ-1922 amendment; REQ-829: one cache store per deployment)."""
        rt = self._active_runtime()
        return rt if getattr(rt, field_name) is not None else self._default_runtime()

    @property
    def response_cache_store(self) -> CacheStore:
        store = self._cache_runtime("response_cache_store").response_cache_store
        assert store is not None  # the default runtime always holds the deployment's store
        return store

    @response_cache_store.setter
    def response_cache_store(self, value: CacheStore) -> None:
        self._active_runtime().response_cache_store = value

    @property
    def hot_counts(self) -> Any:
        counts = self._cache_runtime("hot_counts").hot_counts
        assert counts is not None  # the default runtime always holds the deployment's counts
        return counts

    @hot_counts.setter
    def hot_counts(self, value: Any) -> None:
        self._active_runtime().hot_counts = value

    @property
    def response_cache_default_ttl(self) -> int:
        """The response-cache TTL for the active org: its own override, else the deployment's.

        Routed rather than a plain scalar because the TTL governs the ORG's results — on a shared
        shard a process-global scalar hands whichever org saved last a TTL every other org's
        queries then cache under.
        """
        override = self.settings_overrides.get("cache") or {}
        ttl = override.get("default_ttl")
        return int(ttl) if ttl is not None else self.deployment_cache_default_ttl

    @response_cache_default_ttl.setter
    def response_cache_default_ttl(self, value: int) -> None:
        self.deployment_cache_default_ttl = int(value)


state = AppState()

# REQ-1678: the engine layer asks for the active org's own engine DSN (REQ-1418) and the persisted
# platform config through core.request_context, never by importing this module.
from provisa.core.request_context import (  # noqa: E402
    register_active_engine_url_provider,
    register_org_store_provider,
    register_platform_config_provider,
)


# The platform config as last read, with the config generation it was read under. Asked on request
# paths (engine selection, the materialize-store URL), so it is answered from memory and replaced
# only when the config file is loaded or written (provisa.core.config_location) — never by
# asking the filesystem per request.
_platform_config_held: tuple[int, dict] | None = None


def _read_platform_config() -> dict:
    global _platform_config_held
    from provisa.api.admin import _config_io
    from provisa.core.config_location import config_generation

    generation = config_generation()
    held = _platform_config_held
    if held is not None and held[0] == generation:
        return held[1]
    config = _config_io.read_config() or {}
    _platform_config_held = (generation, config)
    return config


register_active_engine_url_provider(lambda: state.active_engine_url)


def _org_store_of(side: str) -> Database:
    """REQ-1922: the active org's handle for one store side (core.request_context.org_store)."""
    db = {"model": state.model_db, "state": state.tenant_db, "record": state.record_db}[side]
    assert db is not None, f"the active org's {side} store is not bound"
    return db


register_org_store_provider(_org_store_of)
register_platform_config_provider(_read_platform_config)

# REQ-1266: the domain mode is a tenant setting, so `provisa.core.domain_policy` keys its policy by
# the org whose request is running. `core` cannot import this ContextVar, so the API layer installs
# the resolver here — at import of the API package, before any request or startup build can read a
# policy. An installed single-tenant deployment binds no org and reads the one unscoped policy.
domain_policy.set_scope_resolver(current_org.get)


def _request_org_for_secrets() -> tuple[Database, str]:
    """Which org's vault a ``${secret:NAME}`` stored in tenant data resolves against (REQ-1580).

    The same org the request's tenant data came from: the bound ``current_org``, exactly as
    ``_active_runtime`` resolves it -- refused when none is bound. An environment resolves to its base
    org -- a branch is a copy of the model, not a second organization, and the vault is the org's.
    """
    assert state.admin_db is not None
    return state.admin_db, require_current_org()


secrets_store.set_request_org_resolver(_request_org_for_secrets)

# REQ-1557: a central secrets service is read inside the bound organisation's namespace where the
# deployment holds many organisations. The provider asks this state each time, so there is one
# answer and it is the running deployment's.
from provisa.core import secrets_providers as _secrets_providers  # noqa: E402

_secrets_providers.set_many_organisations_resolver(lambda: state.multitenancy)


def _org_redirect_overrides() -> dict:
    """The bound org's `redirect` override block, for RedirectConfig.from_env (REQ-1349)."""
    return state.settings_overrides.get("redirect") or {}


_redirect.set_org_overrides_resolver(_org_redirect_overrides)


async def _load_and_build(
    config_path: str | None = None,
    *,
    apply: bool = True,
) -> None:  # REQ-012, REQ-016, REQ-247, REQ-289, REQ-369, REQ-371, REQ-1900
    """Load config, introspect the engine, build schemas for all roles.

    ``apply=False`` (REQ-1900) is a worker of a `--workers N` launch whose once-per-launch work
    another worker has already completed (see ``provisa.core.boot_lock``). It runs only the
    per-worker half — pools, the registry read into memory, the compiled schemas — and skips every
    step that writes the control plane or the engine's shared catalogs: schema DDL, the built-in
    seeds, the config apply, role grants, primary-key resolution, the environment baselines.
    """
    if config_path is None:
        config_path = config_path_str()

    # REQ-1916/1922: the launch's mode and region, checked against the platform's regions before
    # any store is opened — a node the platform cannot place does not start. A first start with no
    # config file yet declares nothing, as the build below treats it (it returns at that point).
    from provisa.core import process_region

    _launch_config = Path(config_path)
    _launch_raw = read_config_with_includes(_launch_config) if _launch_config.exists() else {}
    process_region.bind_from_environment(_launch_raw)
    # REQ-1922: in a region deployment the boot org's engine is the one its region names, bound
    # before anything below wakes, seeds or attaches an engine.
    _bind_boot_engine(_launch_raw)

    # Use uvicorn's console logger — the root logger's only handler is the OTLP
    # exporter, so provisa.* logs never reach the console / backend.log.
    _startup_log = logging.getLogger("uvicorn.error")
    _startup_marks = [time.perf_counter()]

    def _mark(name: str) -> None:
        now = time.perf_counter()
        _startup_log.warning(
            # pid: `--workers N` interleaves N processes' phases in one log (REQ-1900).
            "startup phase %-20s +%6.2fs (total %6.2fs) pid=%d",
            name,
            now - _startup_marks[-1],
            now - _startup_marks[0],
            os.getpid(),
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

    pg_host, pg_port, pg_database, pg_user = await _init_control_planes(
        config_path, initialise=apply
    )

    _mark("pg-pool")
    _mark("schema-init")

    # REQ-1448: the wake precedes every use of the engine's address, and the seed below is the
    # first — it writes the endpoint into the built-in source rows. A shard's address exists only
    # between a wake and the next idle-to-zero (the provisioner reads it from the ready pod), so
    # asking for it before the wake asks for an address nothing holds. On any deployment that does
    # not provision engines — desktop, self-hosted, tests — this is a no-op.
    from provisa.federation.engine_wake import converge_boot_shard
    from provisa.federation.k8s_provisioner import K8sProvisioningError, provisioning_available

    # REQ-1619: set when the shard could not be allocated, and read by every boot step below that
    # needs the coordinator's address. It is NOT an error being swallowed — the failure is logged at
    # ERROR with its traceback, the engine phase of the boot is skipped rather than half-run, and the
    # query path does the work instead (see the except branch).
    engine_deferred = False

    if provisioning_available():
        try:
            shard = await converge_boot_shard()
        except K8sProvisioningError:
            # REQ-1619: THE CONTROL PLANE DOES NOT DEPEND ON THE ENGINE BEING ALLOCATABLE. A shard is
            # a pod the cluster must find a node for, and scale-to-zero means every cold start asks
            # for a brand-new one; a cluster that cannot supply it right now (quota, capacity) used
            # to take the whole site down with it — `Application startup failed. Exiting.` — even
            # though sign-in, org administration, settings, invites and every metadata surface owe
            # the engine nothing. The recovery is already written and already exercised on every
            # shard restart: ensure_engine_awake wakes the shard on the query path and
            # restore_shared_terminal rebuilds the terminal on whatever coordinator it lands on
            # (REQ-1448). So boot skips its engine phase and leaves default_rt.engine_generation
            # unstamped, which is exactly what that comparison reads as "restarted" — the first
            # query pays for the wake and the restore. Narrow on purpose: only the provisioner's own
            # failure, and only on a deployment that provisions engines.
            log.exception(
                "boot could not wake the shared engine shard; starting the control plane without "
                "an engine — the first query wakes it and rebuilds the shared terminal (REQ-1619)"
            )
            engine_deferred = True
        else:
            # Stamp the default runtime with the coordinator this boot is about to load the shared
            # terminal onto. The query path compares the two to decide whether the coordinator
            # holding the terminal's catalogs is still the one it connected to (REQ-1448); an
            # unstamped default runtime reads as "restarted" on the first query of every process.
            from provisa.federation.engine_wake import generation as _boot_generation

            default_rt = state._default_runtime()
            default_rt.shard = shard
            default_rt.engine_generation = _boot_generation(shard)

    _mark("engine-wake")

    if apply:
        await _seed_built_in_sources(
            pg_host,
            pg_port,
            pg_database,
            pg_user,
            org_id=state.org_id,
            engine_addressable=not engine_deferred,
        )

    _mark("pg+schema+seed")

    # REQ-1349: the default org's own settings rows, layered over the deployment config. Read here
    # rather than at first use because the query path (response TTL, redirect) reads them off the
    # runtime. build_org_runtime does the same for every other org. REQ-1914: read with the
    # ``settings`` stamp they were loaded at, which this worker's config watcher compares.
    from provisa.api.model_reload import load_org_settings as _load_org_settings

    await _load_org_settings(state._default_runtime())

    path = Path(config_path)
    if not path.exists():
        return

    # read_config_with_includes (not a bare yaml.safe_load) — config_path may be a wrapper file
    # that only carries `includes:` (start-ui-install.sh writes one for --source=<name> and
    # --demo <name>), and a bare load of that produces just {"includes": [...]} with none of the
    # real sources/domains/tables/roles, failing ProvisaConfig validation with "Field required"
    # for all of them. Confirmed live (REQ-1858 named-demo work): --demo perf's wrapper crashed
    # startup this way every time, silently past several log lines with no error until the
    # backend log was checked directly.
    raw_config = read_config_with_includes(path)

    # The shard was woken above, before the seed that first asked for its address;
    # _apply_server_and_engine_config CONNECTS the terminal (trino_lifecycle.provision opens a
    # dbapi connection and seeds the ops catalogs), so it depends on that same wake (REQ-1448).
    # REQ-1619: with no shard there is no address to dial, so the server settings are applied and
    # the terminal is left unconnected — restore_shared_terminal opens it on the first query.
    _apply_server_and_engine_config(
        raw_config, connect_engine=not engine_deferred, provision_engine=apply
    )

    _mark("engine-connect")

    # REQ-1912: replicas and materialized views each need a schema of their own, so a store with
    # no schemas stops the start here, naming the store. A deployment with no store configured
    # at all replicates nothing and has nothing to refuse.
    from contextlib import suppress

    from provisa.federation.engine import MaterializeStoreUnconfigured

    with suppress(MaterializeStoreUnconfigured):
        state.federation_engine.materialize_store_dsn()

    # Flight (Zaychik), the MinIO buckets, and the results schema are mutually independent
    # engine-terminal network setup, run concurrently to cut startup latency. the engine-terminal
    # infra: a native engine has no Zaychik/MinIO/results-schema, so provision_infra() is a
    # no-op there (it would otherwise block on absent services). REQ-1619: it is also the second
    # thing restore_shared_terminal does, so a deferred boot leaves it to that.
    if not engine_deferred:
        await state.federation_engine.provision_infra()

    _mark("infra: flight/minio/results")

    # The deployment's auth, for every surface (REQ-120): provider config, the flag the wire
    # surfaces read, and the generation that makes the HTTP middleware re-resolve.
    from provisa.auth.wiring import bind_auth_config

    bind_auth_config(state, raw_config.get("auth"))

    # Load config into PG (and create the engine catalogs)
    config = parse_config_dict(raw_config)
    state.config = config
    state.multitenancy = config.multitenancy
    # REQ-1337: multitenancy demands per-org schema isolation (org_<id> schema + search_path on
    # PostgreSQL). The portable/SQLite bootstrap (_init_schema_portable) writes every org into one
    # flat file with no per-org scoping, so a multitenant deployment on a non-PG tenant DB would
    # silently mix orgs' data. Fail loudly at startup instead of letting that combination run.
    if config.multitenancy and getattr(state.model_db, "dialect", "postgresql") != "postgresql":
        raise RuntimeError(
            "multitenancy=true requires a PostgreSQL TENANT_DATABASE_URL "
            f"(got dialect={getattr(state.model_db, 'dialect', None)!r}); "
            "the portable/SQLite bootstrap has no per-org schema isolation"
        )
    # REQ-1337: org_admin holds the platform_settings right only in a single-tenant deployment.
    # Asserted here rather than in _init_control_planes because the tenancy mode is only known once
    # the config is parsed, which happens after the root org's schema is created.
    from provisa.core.db import apply_tenancy_role_grants as _apply_tenancy_role_grants

    assert state.model_db is not None
    if apply:
        await _apply_tenancy_role_grants(
            state.model_db, state.org_id, multitenancy=config.multitenancy
        )
    if config.multitenancy:
        from provisa.core.tenant_context import TenantContextCache

        state.tenant_context_cache = TenantContextCache()
        if apply:
            model_db = state.model_db
            assert model_db is not None
            async with model_db.acquire() as _rls_conn:
                await _init_meta_rls(_rls_conn)

    # Apply the telemetry compaction settings to state (REQ-1913: operator settings).
    from provisa.api.app_loaders import apply_telemetry_settings

    apply_telemetry_settings(state)

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
    # REQ-1913: an operator setting — a value stored through the settings page replaces that union.
    from provisa.core import settings_registry

    state.udf_egress_allowlist = settings_registry.value("udf.egress_allowlist")

    # Resolve Redis URL regardless of response-cache enablement so rate limiting
    # (REQ-371) can use it even when the response cache is off. PROVISA_REDIS_EMBEDDED
    # forces the in-process fakeredis path (REQ-829) for the native desktop tier — an
    # explicit selection that ignores any configured URL, so no Redis server is needed.
    # REQ-1913: operator settings, resolved by the settings registry (stored, then environment,
    # then this config file, then the declared default).
    from provisa.api.app_loaders import apply_redis_settings
    from provisa.core import settings_registry

    apply_redis_settings(state)
    state.apq_ttl = settings_registry.value("apq.ttl")  # REQ-289
    # Default enabled=True: a store always exists — RedisCacheStore(None) falls back to
    # embedded fakeredis when no Redis URL is set, so there is never a "no cache" state.
    # Set cache.enabled: false explicitly to opt into the NoopCacheStore.
    # REQ-1909: live-read permits share the deployment's Redis (cluster-wide cap), or fakeredis.
    from provisa.federation.live_concurrency import LivePermitStore

    state.live_permit_store = LivePermitStore(state.redis_url)
    # REQ-826: Hot counts share it too — deployment-wide with a Redis, this process's own without.
    from provisa.federation.replica_hot import HotCounts

    state.hot_counts = HotCounts(state.redis_url)
    if settings_registry.value("cache.enabled"):
        # REQ-829: RedisCacheStore(None) transparently uses embedded fakeredis, so
        # desktop exercises the same result-cache code path as production.
        state.response_cache_store = RedisCacheStore(state.redis_url)
        state.response_cache_default_ttl = settings_registry.value("cache.default_ttl")

    model_db = state.model_db
    assert model_db is not None
    _seed_file = config
    state.raw_config = raw_config
    state.seed_config = _seed_file
    _seeded_now = False
    from provisa.core.secrets_store import bound_to_request_org

    async with model_db.acquire() as conn:
        # REQ-1919: the configuration file seeds the model store once, at the deployment's first
        # start, into an empty store. From then on the store alone owns the model: a restart,
        # redeploy or reload never applies the file again, and the process's configuration is the
        # file's settings with every model section read from the store. Only the launch's
        # once-per-launch worker writes (REQ-1900); a later launch, and a second node, find the
        # store seeded and write nothing.
        # A demo deployment's own org is a demo organisation (DEMO ORGANISATIONS ARE THEIR
        # CONFIG): every launch rebuilds its model from the file, so it starts as the file says.
        from provisa.core.demo import is_demo

        _seed_engine = None if engine_deferred else state.federation_engine
        if apply and is_demo():
            _populate_source_catalog_names(_seed_file)
            async with bound_to_request_org():
                await rebuild_from_config(_seed_file, conn, _seed_engine)
            _seeded_now = True
        elif apply and not await is_seeded(conn):
            # The org-prefixed catalog names the seed's registrations resolve under (REQ-1266).
            _populate_source_catalog_names(_seed_file)
            # REQ-1619: with no coordinator the seed settles no engine-specific table names;
            # engine=None is the seed's "register the metadata only" mode.
            _seeded_now = await seed_config(_seed_file, conn, _seed_engine)
        domain_policy.configure(_seed_file.naming.use_domains, _seed_file.naming.default_domain)

        # A stored source's password is a ${secret:NAME} reference the org's vault resolves. From
        # here on every reader of a model section (views, hot tables, the schema build) reads the
        # store's model, never the file's.
        raw_config = await store_raw(raw_config, conn)
        async with bound_to_request_org():
            config = parse_store_raw(raw_config)
        state.config = config
        # REQ-147: the Kafka sources the store holds — their topics' windows and discriminators,
        # and the engine's Kafka catalogs.
        _process_kafka_sources(raw_config, register_catalogs=apply)
        _populate_source_catalog_names(config)
        # Every launch reissues the engine catalog of each source the store holds: an engine's
        # catalogs do not outlive it (REQ-1900). REQ-1619: with no coordinator they are reissued
        # from state.config by restore_shared_terminal on the first query.
        if apply and not engine_deferred:
            async with bound_to_request_org():
                await attach_store_sources(config, state.federation_engine)

    _mark("load_config")

    # REQ-1266: the org's domain mode wins over the deployment's, applied after load_config (which
    # configured the scope from the config file) and before the schema build reads the policy.
    _naming_override = state.settings_overrides.get("naming") or {}
    if _naming_override:
        _use, _default = domain_policy.snapshot()
        domain_policy.configure(
            _naming_override.get("use_domains", _use),
            _naming_override.get("default_domain", _default),
        )

    state.source_dsns["provisa-admin"] = f"{pg_host}:{pg_port}/{pg_database}"

    await _build_source_pools_and_enums(config)

    # REQ-1730: Trino's prometheus connector exposes a FIXED per-metric schema (labels
    # MAP(VARCHAR,VARCHAR), timestamp, value) — a registered label column like "job" is only
    # reachable there as labels['job'], and TrinoBackend.transpile_physical rewrites a bare "job"
    # reference into that form by reading state.prometheus_label_columns. That cache is populated
    # ONLY by create_source's own mutation handler (_cache_prometheus_label_columns,
    # schema_common.py) — an in-memory dict, never reconstructed on boot — so a prometheus source
    # (config- or control-plane-only, either one) resolved correctly the moment it was registered,
    # then permanently lost the rewrite on the very next boot or reload, on whatever engine was
    # active: reproduced live via REQ-1730's own reboot-harness e2e, Trino raising
    # COLUMN_NOT_FOUND for "job" after a genuine restart with no mutation replay.
    from provisa.api.admin.schema_common import _cache_prometheus_label_columns as _cache_prom_cols

    for _prom_src in config.sources:
        if _prom_src.type.value == "prometheus":
            await _cache_prom_cols(state.model_db, state, _prom_src)

    await _init_ingest_engines()

    # Second pass — resolve PRIMARY KEYs from each native RDBMS source's own
    # information_schema. the engine normalizes column types and layers Provisa governance
    # on top, but its metadata model omits source constraints (there is no
    # information_schema.table_constraints in the engine catalog), so PKs are read here
    # through the source driver directly, now that the source pools are built. The DB
    # constraint is authoritative — config YAML need not restate is_primary_key.
    if apply:
        await _resolve_pk_from_sources()

    # Reload OpenAPI specs from DB into state (survives hot reloads and restarts)
    await _load_openapi_specs()

    # Load materialized view definitions, views, and auto-MV cross-source rels
    _config_views = _load_mv_and_views_config(raw_config)

    await _load_graphql_remote_sources_from_db()
    await _load_grpc_remote_sources_from_db()

    # The seed's relationships whose tables a remote registration brought only now (graphql_remote
    # tables are registered after the seed): the seed's own, on the boot that seeded (REQ-1919).
    if _seeded_now:
        from provisa.core import model_change
        from provisa.core.repositories import relationship as _rel_repo

        async with model_change.scope("configuration seed"):
            async with model_db.acquire() as _retry_conn:
                for _rel in _seed_file.relationships:
                    try:
                        await _rel_repo.upsert(_retry_conn, _rel)
                    except ValueError as exc:
                        log.warning("seeded relationship %r not registered: %s", _rel.id, exc)

    _mark("source-pools+ingest+remote")

    await _require_org_serves_here(state.org_id)  # REQ-1922
    await _refuse_boot_lane_conflict()
    await _bind_region_stores(state.org_id, PROD, initialise=apply)
    if apply:
        from provisa.api.model_reload import prune_region_state

        await prune_region_state(state._active_runtime())  # REQ-1922

    # Schema-currency reconcile (REQ-846/932), after the org's regions are bound (REQ-1922: which
    # tables are read here, and which from another region's store, asks for those regions):
    # converge the materialization store's landing tables to config for every MATERIALIZED source
    # and attach their read views — DDL only, no data landed (that is the refresh's job).
    # Best-effort at boot so a store hiccup never bricks startup (matches the live-engine reconcile
    # pattern); materialized sources become queryable once it succeeds.
    try:
        _landed = await state.federation_engine.reconcile_landed_tables()
        if _landed:
            log.info("reconciled %d landed table(s) into the materialization store", len(_landed))
    except Exception:
        log.exception("landed-table schema reconcile failed")
    _mark("reconcile landed tables")

    await _rebuild_schemas(raw_config)

    # A config view that reads an input the engine cannot read whole fails the load, naming the
    # view and the input. Checked here, not where the view is read from the config: the registry
    # it is checked against (tables, API endpoints) is loaded by the build above.
    # Both spellings of a config view: a ``views:`` / ``materialized_views:`` / materialized
    # relationship entry, and a table entry carrying ``view_sql`` with ``materialize: true``.
    from provisa.mv.readable_inputs import config_table_views, require_views_readable

    await require_views_readable(state, _config_views + config_table_views(state, raw_config))

    _mark("rebuild_schemas")

    # Initialize hot tables (Phase AD6)
    from provisa.cache.hot_tables import init_hot_tables

    hot_mgr = await init_hot_tables(
        raw_config, state.federation_engine, state.tables, state.api_endpoints, state.api_sources
    )
    if hot_mgr is not None:
        state.hot_manager = hot_mgr

    _mark("hot_tables")

    if apply:
        await _ensure_environment_baselines()

    _mark("prod-baseline")


@model_change.commits_itself  # REQ-1524: commits the model it writes itself
async def _ensure_environment_baselines() -> None:
    """Give every environment of the booted org the starting point its history is supposed to have.

    REQ-1543: AN ENVIRONMENT'S LINE IS NEVER STARTED BY AN EDIT. However the environment came to
    exist -- created through the admin API, assembled here from the config file, or restored beside a
    volume that outlived the process that made it -- it is standing on a model from the moment it
    exists, and that model is what its first commit records. Without it the first change somebody
    makes IS the first commit, and undoing that change has no parent to step back to: the change
    would be unundoable precisely because it was the first one, which is not a rule anybody asked
    for.

    So the guarantee is enforced here, against the environments that are actually there, rather than
    only inside the call that creates one. prod is ensured first because an org always has one and
    the org booted from a config file was never handed the registry row ``create_org`` writes.

    It runs at the END of the boot because the model is what is being committed, and the model is not
    finished until the config is loaded and the schemas are built. Idempotent, like the rest of
    provisioning: an environment whose line has already started is left exactly as it is, and an
    unchanged model writes no second commit (REQ-1526).
    """
    from provisa.core.env_repo import ensure_repo, has_branch, write_through
    from provisa.core.env_store import ensure_prod, list_envs
    from provisa.core.environments import PROD, org_schema

    assert state.admin_db is not None
    assert state.model_db is not None
    await ensure_prod(state.admin_db, state.org_id)
    repo = ensure_repo(state.org_id)
    unstarted = [
        row["name"]
        for row in await list_envs(state.admin_db, state.org_id)
        if not has_branch(repo, row["name"])
    ]
    # prod first: a branch created from it is seeded from its tip, so its line has to exist before
    # theirs can continue it rather than root beside it.
    unstarted.sort(key=lambda name: (name != PROD, name))
    for name in unstarted:
        async with state.model_db.acquire() as conn:
            await write_through(
                conn,
                state.admin_db,
                state.org_id,
                name,
                org_schema(state.org_id, name),
                "provisioned",
                None,
            )


class OrgLane(NamedTuple):
    """The admin-plane row that decides how one org's runtime is built."""

    seeded_demo: bool
    isolated_engine: bool
    external_engine: tuple[str, int] | None
    engine_kind: str | None
    engine_url: str | None
    shard: str
    storage_url: str | None


async def _read_org_flags(org_id: str) -> OrgLane:
    """The engine lane of ``org_id``, from the admin plane.

    The org registry is in-memory with no TTL, so after a process restart the per-request router
    must rebuild an org's runtime on first access. ``seeded_demo`` tells it whether to reload the
    demo sources; ``isolated_engine`` whether to bind a dedicated federation engine
    (REQ-1043/REQ-1067); ``external_engine`` is the org's own coordinator when it runs an external
    engine (REQ-1412); ``engine_kind``/``engine_url`` are that engine's kind and DSN when the org
    runs a kind of its own (REQ-1418) — no defaults, the row is authoritative; ``shard`` names the
    shared-lane engine shard that answers this org (REQ-1450), which is what the query-path wake
    brings back up when it has been released; ``storage_url`` is the org's own materialization
    store when it brings one (REQ-1048), which is what takes its bytes off the platform's disk and
    out of the storage quota."""
    from sqlalchemy import select as _select

    from provisa.core.schema_admin import orgs as _orgs

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        result = await conn.execute_core(
            _select(
                _orgs.c.seeded_demo,
                _orgs.c.isolated_engine,
                _orgs.c.external_engine_host,
                _orgs.c.external_engine_port,
                _orgs.c.engine_kind,
                _orgs.c.engine_url_enc,
                _orgs.c.shard,
                _orgs.c.storage_url_enc,
            ).where(_orgs.c.id == org_id)
        )
        row = result.fetchone()
    if row is None:
        raise KeyError(
            f"org {org_id!r} not found in the admin plane — cannot route a request to it"
        )
    external = (row[2], int(row[3])) if row[2] else None
    # REQ-1418: the DSN is stored encrypted; it is decrypted once here, at build time, rather than
    # per query — the same treatment source DSNs already get on the runtime.
    #
    # REQ-1048: the BYO materialization-store DSN gets exactly the same treatment — encrypted at
    # rest, decrypted once here, carried on the runtime so no write path decrypts per landing.
    from provisa.encryption.runtime import encryption_service

    engine_url = None
    if row[5] is not None:
        engine_url = encryption_service().decrypt(bytes(row[5])).decode("utf-8")
    storage_url = None
    if row[7] is not None:
        storage_url = encryption_service().decrypt(bytes(row[7])).decode("utf-8")
    return OrgLane(bool(row[0]), bool(row[1]), external, row[4], engine_url, row[6], storage_url)


async def ensure_org_encryption(org_id: str) -> None:  # REQ-1574
    """Resolve one org's encryption ring once per process, before any encrypted path reads it.

    Records the answer either way: a ring when the org set a key, ``None`` when it holds none --
    which is a real answer and not a miss, so the org is served the deployment service rather than
    refused.
    """
    from provisa.core.org_encryption import load_org_ring
    from provisa.encryption.runtime import org_encryption_loaded, set_org_encryption

    if org_encryption_loaded(org_id):
        return
    assert state.admin_db is not None, "the admin plane is required to resolve an org's key ring"
    set_org_encryption(org_id, await load_org_ring(state.admin_db, org_id))


async def ensure_serving_runtime(org_id: str, env: str | None = None) -> None:  # REQ-1266
    """Make the runtime an entrypoint is about to bind ready: the deployment org's prod runtime is
    the one the boot built, every other is built here on first use (``ensure_org_runtime``)."""
    if org_id == state.org_id and (env is None or env == PROD):
        return
    await ensure_org_runtime(org_id, env)


async def ensure_org_runtime(org_id: str, env: str | None = None) -> OrgRuntime:  # REQ-1266
    """Get the org ENVIRONMENT's data-plane runtime, building it (once, under the lock) on a miss.

    The single seam every entrypoint (HTTP middleware AND the pgwire/bolt/flight/gRPC/MCP protocol
    servers) uses to lazily materialize an org's runtime before binding ``current_org``. The
    registry is in-memory with no TTL, so first access after a process restart rebuilds; whether to
    reload the demo sources / bind a dedicated engine is read authoritatively from the admin plane
    (no default).

    REQ-1488/REQ-1529: ``env`` selects WHICH copy of the org's model is served, and each gets its
    own runtime under its own key. The org-level lane flags are the ORG's — an environment is a
    copy of the model, not a second tenant, so a branch runs on the same engine, the same shard and
    the same storage its org does; what differs is the schema it reads and the bindings its sources
    resolve through. ``None`` and ``prod`` are the same environment and the same key."""

    # REQ-1574: the org's key ring is resolved BEFORE anything reads its encrypted columns --
    # _read_org_flags decrypts the org's engine and storage DSNs, and those were written under
    # whichever key the org holds now.
    await ensure_org_encryption(org_id)

    async def _builder(_key: str) -> OrgRuntime:
        lane = await _read_org_flags(org_id)
        # REQ-1621: an environment with an expiry is ephemeral, and the mutation gate reads that off
        # the runtime rather than re-querying the registry per call. PROD never has one, so it is
        # not asked -- the row is the org's oldest and the answer is structural.
        from provisa.core.env_store import get_env

        _ephemeral = False
        # REQ-1942: the environment's data choices, read off the runtime by the query path. PROD
        # has none: it is always real, and its mutations change its own data.
        _data_mode: str | None = None
        _mutation_handling: str | None = None
        if env is not None and env != PROD:
            assert state.admin_db is not None, "the admin plane holds the environment registry"
            _row = await get_env(state.admin_db, org_id, env)
            if _row is None:
                raise KeyError(f"organization {org_id!r} has no environment {env!r}")
            _ephemeral = _row["expires_at"] is not None
            _data_mode, _mutation_handling = _row["data_mode"], _row["mutation_handling"]
        external_engine, engine_kind, engine_url, storage_url = (
            lane.external_engine,
            lane.engine_kind,
            lane.engine_url,
            lane.storage_url,
        )
        # REQ-1922: in a region deployment the org's region names its engine and its materialize
        # store, in its model; the admin-plane row may not name others (refuse_lane_conflict).
        bound = await _region_lane_of(org_id, env or PROD)
        if bound is not None:
            from provisa.core.region_stores import refuse_lane_conflict

            refuse_lane_conflict(
                org_id,
                engine_kind=lane.engine_kind,
                engine_url=lane.engine_url,
                external_engine=lane.external_engine,
                storage_url=lane.storage_url,
            )
            external_engine, engine_kind, engine_url = bound.endpoint, bound.kind, bound.url
            storage_url = bound.materialize_url
        return await build_org_runtime(
            org_id,
            env=env or PROD,
            ephemeral=_ephemeral,
            include_demo=lane.seeded_demo,
            isolated_engine=lane.isolated_engine,
            external_engine=external_engine,
            engine_kind=engine_kind,
            engine_url=engine_url,
            shard=lane.shard,
            storage_url=storage_url,
            data_mode=_data_mode,
            mutation_handling=_mutation_handling,
        )

    return await state.org_registry.get_or_build(runtime_key(org_id, env), _builder)


def _bind_boot_engine(raw_config: dict) -> None:
    """REQ-1922: bind the boot org's engine to the engine store its region names in the config
    file, the same external-engine lane every other org in a region deployment is built on
    (``ensure_org_runtime``). A no-op with no platform regions: the deployment's engine serves it
    as today."""
    from provisa.core.region_stores import region_lane
    from provisa.core.regions import OrgRegion, StoreConfig

    bound = region_lane(
        "the boot org",
        [OrgRegion.model_validate(r) for r in raw_config.get("regions") or []],
        [StoreConfig.model_validate(s) for s in raw_config.get("stores") or []],
    )
    if bound is None:
        return
    from provisa.federation.engine import build_engine
    from provisa.federation.runtime import EngineRuntime

    rt = state._active_runtime()
    rt.isolated_engine = True
    rt.engine_endpoint = bound.endpoint
    rt.engine_kind = bound.kind
    rt.engine_url = bound.url
    # REQ-1048 precedence: the org's own store first (provisa/storage/byo.py).
    rt.storage_url = bound.materialize_url
    rt.federation_engine = EngineRuntime(build_engine(bound.kind), state)
    rt.federation_engine.engine.pin_materialize_store(bound.materialize_url)
    rt.federation_engine.bind_terminal()


async def _refuse_boot_lane_conflict() -> None:
    """REQ-1922: the boot org's admin-plane row may not name an engine or store beside its
    region (read once the platform plane is up)."""
    from provisa.core import process_region
    from provisa.core.region_stores import refuse_lane_conflict
    from provisa.core.regions import DEFAULT_REGION

    if process_region.region() == DEFAULT_REGION:
        return
    lane = await _read_org_flags(state.org_id)
    refuse_lane_conflict(
        state.org_id,
        engine_kind=lane.engine_kind,
        engine_url=lane.engine_url,
        external_engine=lane.external_engine,
        storage_url=lane.storage_url,
    )


async def _region_lane_of(org_id: str, env: str):
    """REQ-1922: the engine and materialize store the org's model names for this node's region,
    read from its model store before its runtime (and so its engine) is built. None with no
    platform regions."""
    from provisa.core import process_region
    from provisa.core.config_loader import load_control_plane
    from provisa.core.database import Capabilities, create_engine_from_url
    from provisa.core.environments import org_schema
    from provisa.core.region_stores import region_lane
    from provisa.core.regions import DEFAULT_REGION
    from provisa.core.repositories.region import list_regions, list_stores

    if process_region.region() == DEFAULT_REGION:
        # REQ-1922 amendment: the one implicit region names no engine; the lane decides as today.
        return None
    shared = state.tenant_engine
    assert shared is not None, "tenant engine not built; _init_control_planes must run first"
    # A not-schema-capable backend keeps each org in its own file (see build_org_runtime).
    owned = None
    if not Capabilities.for_dialect(shared.dialect.name).schemas:
        cp = load_control_plane(config_path_str())
        owned = create_engine_from_url(cp.resolved_tenant_url(), pool_size=1, max_overflow=0)
    try:
        model_db = Database(
            owned or shared, name="org-model", search_path=org_schema(org_id, env), holds="model"
        )
        async with model_db.acquire() as conn:
            regions, stores = await list_regions(conn), await list_stores(conn)
    finally:
        if owned is not None:
            owned.dispose()
    return region_lane(org_id, regions, stores)


async def _bind_region_stores(org_id: str, env: str, *, initialise: bool) -> None:
    """REQ-1922: once the org's model is loaded, its state and record handles are bound to the
    stores the model names for this node's region (a no-op with no platform regions)."""
    from provisa.core.config_loader import load_control_plane
    from provisa.core.database import OrgStores
    from provisa.core.region_stores import bind_region_stores

    assert state.model_db is not None  # opened with the runtime, before its model was loaded
    cp = load_control_plane(config_path_str())
    await _bind_region_cache(org_id)
    from provisa.core.region_stores import bind_foreign_regions

    state._active_runtime().foreign_regions = await bind_foreign_regions(
        org_id, env, state.model_db, pool_size=cp.pool_max, max_overflow=cp.max_overflow
    )
    state.model_db, state.tenant_db, state.record_db = await bind_region_stores(
        org_id,
        env,
        OrgStores(state.model_db, state.tenant_db, state.record_db),
        pool_size=cp.pool_max,
        max_overflow=cp.max_overflow,
        schema_sql=(Path(__file__).parent.parent / "core" / "schema.sql").read_text(),
        initialise=initialise,
    )


_region_caches: dict[str, tuple[Any, Any]] = {}


async def _bind_region_cache(org_id: str) -> None:
    """REQ-1922: the org's response cache and Hot counts are kept on the cache store its region
    names (one client per store URL in this process). With no platform regions the runtime keeps
    none of its own and is served the deployment's (AppState._cache_runtime)."""
    from provisa.cache.store import NoopCacheStore, RedisCacheStore
    from provisa.core import settings_registry
    from provisa.core.region_stores import region_lane
    from provisa.core.repositories.region import list_regions, list_stores
    from provisa.federation.replica_hot import HotCounts

    assert state.model_db is not None  # opened with the runtime, before its model was loaded
    async with state.model_db.acquire() as conn:
        regions, stores = await list_regions(conn), await list_stores(conn)
    lane = region_lane(org_id, regions, stores)
    if lane is None:
        return
    if lane.cache_url not in _region_caches:
        store = (
            RedisCacheStore(lane.cache_url)
            if settings_registry.value("cache.enabled")
            else NoopCacheStore()
        )
        _region_caches[lane.cache_url] = (store, HotCounts(lane.cache_url))
    rt = state._active_runtime()
    rt.response_cache_store, rt.hot_counts = _region_caches[lane.cache_url]


async def _require_org_serves_here(org_id: str) -> None:
    """REQ-1922: a node serves its region of every org that selects it; an org whose model does
    not select this node's region is refused here, by name, before its schemas are built."""
    from provisa.core import process_region
    from provisa.core.repositories.region import require_serves_here

    # Both callers run after the org's control plane is up and its model loaded into it.
    assert state.model_db is not None
    async with state.model_db.acquire() as conn:
        await require_serves_here(conn, org_id, process_region.region())


async def _unbound_sources(conn: Any) -> set[str]:
    """The ids of the environment's sources with no connection (REQ-1491, REQ-1942): they get no
    pool, because an empty host is not an absent one. Every other source's connection is the
    environment's own row -- given in it or copied from its parent -- and nothing is resolved
    through the parent. A source bound to a synthetic store is among them: its tables are read
    from the store by the engine, never through a connection of the source's own."""
    from provisa.core.env_classes import BINDING_COLUMN, SYNTHETIC, UNBOUND
    from provisa.core.schema_org import sources as sources_t

    result = await conn.execute_core(
        select(sources_t.c.id).where(sources_t.c[BINDING_COLUMN].in_((UNBOUND, SYNTHETIC)))
    )
    return {r[0] for r in result.fetchall()}


async def build_org_runtime(
    org_id: str,
    *,
    env: str = PROD,
    ephemeral: bool = False,
    data_mode: str | None = None,
    mutation_handling: str | None = None,
    include_demo: bool = False,
    isolated_engine: bool = False,
    external_engine: tuple[str, int] | None = None,
    engine_kind: str | None = None,
    engine_url: str | None = None,
    shard: str = "",
    storage_url: str | None = None,
) -> OrgRuntime:  # REQ-1266, REQ-1524
    """Build the org's runtime (``_build_org_runtime``) as one model change.

    A build writes the org's model — its role grants, the built-in and demo rows it loads — and
    it is reached from every surface: an HTTP request (whose change it joins), a protocol server
    or a background job (none open one). Every model change commits (REQ-1524), so the build
    opens its own scope; under an outer one it is part of that one."""
    async with model_change.scope(f"build org {org_id}/{env}"):
        return await _build_org_runtime(
            org_id,
            env=env,
            ephemeral=ephemeral,
            data_mode=data_mode,
            mutation_handling=mutation_handling,
            include_demo=include_demo,
            isolated_engine=isolated_engine,
            external_engine=external_engine,
            engine_kind=engine_kind,
            engine_url=engine_url,
            shard=shard,
            storage_url=storage_url,
        )


async def _build_org_runtime(
    org_id: str,
    *,
    env: str = PROD,
    ephemeral: bool = False,
    data_mode: str | None = None,
    mutation_handling: str | None = None,
    include_demo: bool = False,
    isolated_engine: bool = False,
    external_engine: tuple[str, int] | None = None,
    engine_kind: str | None = None,
    engine_url: str | None = None,
    shard: str = "",
    storage_url: str | None = None,
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
    from provisa.core.region_stores import open_org_stores
    from provisa.core.db import apply_tenancy_role_grants, init_schema
    from provisa.audit.query_log import init_audit_schema

    rt = OrgRuntime(
        org_id=org_id,
        env=env,
        ephemeral=ephemeral,
        data_mode=data_mode,
        mutation_handling=mutation_handling,
    )
    key = runtime_key(org_id, env)
    # REQ-1448: sampled BEFORE the CREATE CATALOG statements below are issued, not after. A shard
    # that restarts part-way through this build must leave the runtime stamped with the OLD
    # generation, so the next query rebuilds it; stamping afterwards would record the new
    # coordinator as holding catalogs that were issued to the one it replaced.
    from provisa.federation.engine_wake import generation as _engine_generation

    # REQ-1448: every use of a shard's address is preceded by a wake, and this build has two —
    # _seed_built_in_sources below resolves the engine endpoint to write it into the org's built-in
    # source rows, and the catalog statements dial it. The address is recorded only between a wake
    # and the next idle-to-zero, so an org built after the shared lane was released — the first
    # sign-in of a session — resolves nothing unless the shard comes back first. Boot does exactly
    # this for the default org (_load_and_build); nothing else covers an org built later. The wake
    # precedes the generation sample below so the runtime is stamped with the coordinator that
    # actually receives its catalogs. An org on its own coordinator (REQ-1412/1418) is not served
    # by a shard this control plane operates, so there is nothing here to wake.
    effective_shard = shard
    if external_engine is None and engine_url is None:
        from provisa.federation.k8s_provisioner import provisioning_available

        if provisioning_available():
            from provisa.federation.engine_wake import (
                boot_shard,
                ensure_shard_awake,
                isolated_wake_size,
                restore_shared_terminal,
            )

            if isolated_engine:
                # REQ-1510: a dedicated engine is a shard of its own, woken at the size the org's
                # plan sells. ``shard`` stays the org's SHARED-lane placement in the admin plane
                # (REQ-1450) — what the runtime records is the shard that actually answers it, which
                # is what the query path wakes and stamps generations against.
                from provisa.federation.k8s_provisioner import isolated_shard

                effective_shard = isolated_shard(org_id)
                await ensure_shard_awake(
                    effective_shard,
                    lane="isolated",
                    size=await isolated_wake_size(state, org_id),
                )
            else:
                await ensure_shard_awake(shard or boot_shard())
            # REQ-1448: the wake brought a NEW coordinator up, and this build's own CREATE CATALOG
            # statements go out over the SHARED terminal — which is still connected to the pod that
            # was released while the lane sat idle. ensure_engine_awake restores it, but that runs
            # at _execute_plan, after this build; without the restore here the build itself dials
            # the released pod IP and every source registration times out before any query is ever
            # dispatched. The terminal belongs to the control plane's own shard, so that is the
            # generation to compare. The default org owns the terminal and is stamped by boot, so
            # it is not restored from inside its own rebuild.
            boot = boot_shard()
            default_rt = state.org_registry.get(state.org_id)
            if (
                # REQ-1510: an isolated org issues its catalogs over its OWN terminal, so the shared
                # one is neither used by this build nor restored by it.
                not isolated_engine
                and org_id != state.org_id
                and default_rt is not None
                and default_rt.engine_generation != _engine_generation(boot)
            ):
                await restore_shared_terminal(state, boot)

    rt.shard = effective_shard
    rt.engine_generation = _engine_generation(effective_shard)
    # REQ-1048: recorded before anything below can materialize, since materialize_store() reads it
    # off the bound runtime to decide whose disk the org's bytes land on.
    rt.storage_url = storage_url
    state.org_registry.set(key, rt)
    token = set_current_org(org_id)
    env_token = set_current_env(env)
    try:
        if external_engine is not None or engine_url is not None:
            # REQ-1412: the org runs its OWN coordinator. Same dedicated-EngineRuntime shape as an
            # isolated org — what changes is where the terminal points, which terminal_conn_kwargs
            # reads back off the runtime (state.active_engine_endpoint) instead of resolving the
            # deployment's shared or SaaS-dedicated host. Recorded BEFORE bind_terminal, which
            # reads it.
            #
            # REQ-1418: an external engine is addressed EITHER by an endpoint (a Trino coordinator)
            # OR by a DSN (Databricks, Snowflake, BigQuery, ClickHouse, Fabric, Synapse, any
            # SQLAlchemy URL), which is why either one alone puts the org on this lane. engine_kind
            # picks which of the twelve builders answers; None keeps the deployment's kind, so an
            # org that only moved its endpoint is unaffected.
            from provisa.federation.engine import build_engine
            from provisa.federation.runtime import EngineRuntime

            rt.isolated_engine = True
            rt.engine_endpoint = external_engine
            rt.engine_kind = engine_kind
            rt.engine_url = engine_url
            rt.federation_engine = EngineRuntime(build_engine(engine_kind), state)
            if storage_url is not None:
                # REQ-1922: the lane's engine lands in the lane's store, whoever its first user is.
                rt.federation_engine.engine.pin_materialize_store(storage_url)
            rt.federation_engine.bind_terminal()
        elif isolated_engine:
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
            if storage_url is not None:
                # REQ-1048: the org's own store, whoever the engine's first user is.
                rt.federation_engine.engine.pin_materialize_store(storage_url)
            rt.federation_engine.bind_terminal()
        # Tenant plane for THIS org: its own Database scoped to org_<id>. The platform/admin plane
        # (state.admin_db) is global and already up — never rebuilt here.
        config_path = config_path_str()
        cp = load_control_plane(config_path)
        # REQ-1316: reuse the process-wide tenant engine. Database.acquire() issues this org's
        # search_path on every checkout, so the org boundary is the handle, not the pool. Building
        # an engine per org multiplies open connections by tenant count against ONE server and
        # exhausts max_connections (the "remaining connection slots are reserved" failure).
        # A not-schema-capable backend (SQLite/DuckDB) puts each org in its own FILE, so there the
        # engine genuinely is per-org and a shared one would read the wrong database.
        shared_engine = state.tenant_engine
        assert shared_engine is not None, (
            "tenant engine not built; _init_control_planes must run first"
        )
        if Capabilities.for_dialect(shared_engine.dialect.name).schemas:
            tenant_engine = shared_engine
        else:
            tenant_engine = create_engine_from_url(
                cp.resolved_tenant_url(), pool_size=cp.pool_max, max_overflow=cp.max_overflow
            )
        # REQ-1488: the environment IS the schema. Every repository query this runtime issues goes
        # through this handle, so scoping it here is what makes an unmodified repository query read
        # the branch's copy of the model rather than prod's.
        from provisa.core.model_change import ModelPlane

        state.model_db, state.tenant_db, state.record_db = open_org_stores(
            org_schema(org_id, env), ModelPlane(org_id, env), model_engine=tenant_engine
        )

        schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
        if not schema_sql_path.exists():
            raise RuntimeError(
                f"control-plane schema.sql missing from the package: {schema_sql_path}"
            )
        await init_schema(state.model_db, schema_sql_path.read_text(), org_id=org_id, env=env)
        # REQ-1337: org_admin holds platform_settings only in a single-tenant deployment.
        # REQ-1623: asserted in the environment being built, whose roles table is its own.
        await apply_tenancy_role_grants(
            state.model_db, org_id, multitenancy=state.multitenancy, env=env
        )
        await init_audit_schema(state.model_db, org_id=org_id, env=env)

        # REQ-1349: this org's settings rows, read once here and refreshed by the settings router
        # when the org writes one. The query path (response-cache TTL, large-result redirect)
        # reads them off the runtime, so a control-plane round trip per query is not on that path.
        # REQ-1914: read with the ``settings`` stamp they were loaded at, which this worker's
        # config watcher compares.
        from provisa.api.model_reload import load_org_settings

        await load_org_settings(rt)

        host, port, database, username, _pw = cp.tenant_parts()
        assert database, "control_plane.tenant_url must specify a database"
        await _seed_built_in_sources(
            host or "", port, database, username or "", org_id=org_id, env=env
        )
        state.source_dsns["provisa-admin"] = f"{host}:{port}/{database}"

        # REQ-1919 (DEMO ORGANISATIONS ARE THEIR CONFIG): a demo organisation is defined by its
        # demo configuration — the deployment's file — and every build of its runtime rebuilds its
        # model from that configuration (its model removed, then the configuration applied), so a
        # demo starts exactly as its configuration says; what is changed in it lasts until its next
        # build. Every other organisation's store owns its model: nothing is applied here. Every
        # organisation's runtime reads its model from its own store, and its sources are issued
        # under org-prefixed engine catalogs (source_catalogs), so identically-named sources across
        # orgs never collide in the shared coordinator.
        if state.raw_config is not None:
            assert state.model_db is not None and state.seed_config is not None
            from provisa.core.secrets_store import bound_to_request_org

            seed = state.seed_config
            domain_policy.configure(seed.naming.use_domains, seed.naming.default_domain)
            async with state.model_db.acquire() as conn:
                if include_demo:
                    # The org's catalog names the registrations resolve under (REQ-1266).
                    _populate_source_catalog_names(seed)
                    async with bound_to_request_org():
                        await rebuild_from_config(seed, conn, state.federation_engine)
                async with bound_to_request_org():
                    org_config = await store_config(state.raw_config, conn)
                unbound: set[str] = set()
                if env != PROD:
                    # REQ-1942: an unbound source is reached through no connection.
                    unbound = await _unbound_sources(conn)
            # Populate the org-prefixed catalog-name map FIRST so the physical registration
            # attaches each source under the org's own catalog name (not the bare, default-org
            # name) — the cross-org collision guard (REQ-1266).
            _populate_source_catalog_names(org_config)
            async with bound_to_request_org():
                failed_catalogs = await attach_store_sources(
                    org_config, state.federation_engine, catalog_names=rt.source_catalogs
                )
            # REQ-1448: this build IS the repair a wake performs — a source whose catalog did not
            # come back leaves the org dispatching at a coordinator that has never heard of it, and
            # the next query answers a raw CATALOG_NOT_FOUND naming the source rather than the wake.
            # Fail the build so the caller sees the wake failure and the registry keeps no
            # half-built runtime (the except below drops it).
            if failed_catalogs:
                raise RuntimeError(
                    f"org {org_id!r}: {len(failed_catalogs)} source catalog(s) could not be issued "
                    f"on its engine: {', '.join(sorted(failed_catalogs))}"
                )
            # REQ-1491: a source reached through no connection gets no pool.
            await _build_source_pools_and_enums(
                org_config.model_copy(
                    update={"sources": [s for s in org_config.sources if s.id not in unbound]}
                )
            )
            await _resolve_pk_from_sources()

        await _require_org_serves_here(org_id)  # REQ-1922
        await _bind_region_stores(org_id, env, initialise=True)
        from provisa.api.model_reload import prune_region_state

        await prune_region_state(rt)  # REQ-1922

        # REQ-1266: the org's own domain mode, applied AFTER load_config — which configures the
        # scope from the DEPLOYMENT's naming block — and BEFORE _rebuild_schemas, which reads the
        # policy to decide whether the org's schema is domain-namespaced. current_org is bound, so
        # configure() writes this org's scope alone and no other org's policy moves.
        naming_override = rt.settings_overrides.get("naming") or {}
        if naming_override:
            use, default = domain_policy.snapshot()
            domain_policy.configure(
                naming_override.get("use_domains", use),
                naming_override.get("default_domain", default),
            )

        await _rebuild_schemas()

        # REQ-1882 (amended 2026-09-29): the wiring below starts process-lifetime scheduler jobs and
        # listener tasks. Inline on the process loop; from a connection-thread loop (this org built
        # on first pgwire/Bolt/Flight access) it is started on the process loop, which outlives the
        # request — the connection loop stops running when the request ends.
        async def _wire_org_lifecycle() -> None:
            try:
                await _wire_held_runtime()
            except RuntimeNotBuilt as gone:
                # Detached from the build (run_lifecycle_work), it can outlive the runtime it
                # wires: a change of the environment's data drops that runtime and builds another.
                left_to_the_next_runtime(gone, f"lifecycle wiring of {key}")

        async def _wire_held_runtime() -> None:
            if state.org_registry.get(key) is not rt:
                raise RuntimeNotBuilt(
                    org_id, env, f"the runtime {key} was replaced before its wiring ran"
                )
            # REQ-1266: wire this org's MV event loop onto the shared scheduler so its materialized
            # views refresh on their own cadence. Job ids are org-suffixed and each fire binds
            # current_org (register_runtime reads the bound org), so a second org never clobbers the
            # first's jobs. Best-effort — a missing scheduler (tests, engine not connected) skips it.
            scheduler = getattr(state, "_scheduler", None)
            if scheduler is not None:
                from provisa.events.app_wiring import wire_event_loop

                await wire_event_loop(scheduler, state=state, log=logging.getLogger(__name__))
                # REQ-1003: the org's own scheduled triggers -- its prod ones only.
                from provisa.scheduler.jobs import register_org_triggers

                await register_org_triggers(scheduler, org_id, env)
                # REQ-286/REQ-1266: the org's own live-query engine (prod only).
                from provisa.api.app_rebuild import start_org_live_engine

                await start_org_live_engine(scheduler)

            # REQ-1733: start (or, on a re-wire, top up) the kafka/websocket push-source CDC landing
            # listeners — a separate mechanism from wire_event_loop's poll/MV tick loop (CDC upsert/
            # delete-by-PK isn't expressible through the generic land_source_table write face). Best-
            # effort, same posture as wire_event_loop: never blocks or fails boot.
            from provisa.events.push_wiring import wire_push_listeners

            await wire_push_listeners(state=state, log=logging.getLogger(__name__))

            # REQ-1865: wire the row-materialize background refresh drain + cold-row reaper for every
            # row_materialize table, on the same scheduler — best-effort, same posture as the two calls
            # above (never blocks or fails boot).
            if scheduler is not None:
                from provisa.events.row_materialize_lifecycle import wire_row_materialize_background

                _rm_cfg = getattr(getattr(state, "config", None), "row_materialize", None)
                if _rm_cfg is not None:
                    await wire_row_materialize_background(
                        scheduler,
                        state=state,
                        log=logging.getLogger(__name__),
                        tick_seconds=_rm_cfg.refresh_tick_seconds,
                        reap_interval_seconds=_rm_cfg.reap_interval_seconds,
                        reap_grace_period=_rm_cfg.reap_grace_period,
                        reap_batch_size=_rm_cfg.reap_batch_size,
                    )

        await run_lifecycle_work(_wire_org_lifecycle(), name=f"org-lifecycle:{key}")
    except Exception:
        # The runtime was registered before this body ran (materialize_store() and the catalog-name
        # map are read off the registry while it builds), so a failure part-way leaves a runtime
        # whose catalogs, schemas or pools are incomplete cached under the org — and every later
        # query reads it as healthy. Drop it: the next request rebuilds from scratch, which is also
        # what makes a failed wake retry instead of sticking.
        state.org_registry.invalidate(key)
        raise
    finally:
        reset_current_env(env_token)
        reset_current_org(token)
    return rt


# REQ-1882 (amended 2026-09-29): an org runtime built on first access from a pgwire/Bolt/Flight
# connection thread rebuilds on that thread's own loop, so the serialization holds across loops.
_rebuild_schemas_lock = CrossLoopLock()


async def _rebuild_schemas(raw_config: dict | None = None, *, announce: bool = True) -> None:
    # Serialize rebuilds: the body below fetches DB state across many `await`s and then
    # publishes state.schema_build_cache/state.tables in one shot at the end. Two overlapping
    # callers (e.g. graphql_remote_router's back-to-back calls, or create_source's rebuild
    # racing another mutation's) can otherwise interleave so a call that started earlier -- and
    # fetched an earlier, incomplete table list -- finishes later and clobbers a newer call's
    # already-published state with stale data. Serializing guarantees each rebuild's own DB
    # fetch happens after every prior rebuild's writes have committed and been published.
    #
    # ``announce=False`` (REQ-1914) is a reload: another worker made the change and announced it.
    async with _rebuild_schemas_lock:
        await _rebuild_schemas_impl(raw_config, announce=announce)


async def _rebuild_schemas_impl(raw_config: dict | None = None, *, announce: bool = True) -> None:
    # Rebuild per-role schemas from DB state. Column types come from the authoritative
    # table_columns store (introspect_tables does NOT query the engine), so this runs on any
    # engine; a missing the engine connection only skips the engine-catalog ops seeding below.
    _rebuild_log = logging.getLogger(__name__)
    _rebuild_log.info("_rebuild_schemas called")
    if state.model_db is None:
        _rebuild_log.warning("_rebuild_schemas: model_db is None, returning")
        return

    # REQ-1914: the ``model`` stamp this build is loaded at, read BEFORE the model. A change that
    # lands while the build reads leaves the runtime at the older stamp, so the config watcher
    # rebuilds once more; reading it afterwards would record a stamp newer than the model built.
    from provisa.core import config_stamp as _config_stamp

    _stamped_runtime = state._active_runtime()
    _model_stamp = (await _config_stamp.read(state.model_db))[_config_stamp.MODEL]

    # REQ-1919: the process's configuration follows the default org's model store — the file's
    # settings with every model section as the store holds it now, so an admin's change governs.
    if (
        state.raw_config is not None
        and _stamped_runtime.org_id == state.org_id
        and _stamped_runtime.env == PROD
    ):
        from provisa.core.secrets_store import bound_to_request_org

        async with state.model_db.acquire() as _cfg_conn, bound_to_request_org():
            state.config = await store_config(state.raw_config, cast("Connection", _cfg_conn))

    kafka_physical = getattr(state, "kafka_table_physical", {})
    domain_prefix, raw_config = _resolve_naming_config(raw_config)

    # REQ-684/686, REQ-1557: install the process-wide EncryptionService and select the secrets
    # service from config before any encrypt/decrypt (API auth column, hot cache, audit) runs.
    from provisa.api.app_loaders import configure_encryption_and_secrets

    configure_encryption_and_secrets(raw_config or {})

    # REQ-1914: the masking rules are NOT cleared here. Requests run on their own threads while
    # this build reads the control plane, and one that found the rules empty would be answered
    # unmasked; _load_masking_rules builds the new set aside and publishes it in one assignment.
    # Invalidate the MCP catalog search index (REQ-1008) — the catalog is changing, so the
    # server-lifetime HNSW index is stale; next search_catalog rebuilds it from the new catalog.
    state.mcp_catalog_index = None

    # REQ-1685: a graphql_remote source registered since boot (an applied import, the Sources
    # page) must be known to the landing loader before its tables are queryable; the loader skips
    # ids already registered, so this is idempotent across rebuilds.
    await _load_graphql_remote_sources_from_db()
    await _load_grpc_remote_sources_from_db()

    async with state.model_db.acquire() as conn:
        _pg = cast("Connection", conn)
        tables = await _fetch_tables(_pg)
        # REQ-1942: a table generated into a synthetic store is read as an ordinary table.
        from provisa.synthetic.datasets import as_generated, synthetic_sources

        tables = as_generated(tables, await synthetic_sources(_pg))
        # REQ-1921: out of service — offered in no schema, refused by name when named.
        draft_tables = await _fetch_tables(_pg, draft=True)
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
                    param_policy=r["param_policy"],  # REQ-1467
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
                    command_name=r["command_name"],
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
        # REQ-1729: also backfill state.source_catalogs — a source registered through a REST
        # router (graphql-remote, openapi) writes straight to the ``sources`` table and never
        # runs create_source's catalog-naming step, so catalog_for() raised "no catalog in org"
        # for every one of them until this mirrored that step here too.
        from provisa.api.app_loaders import catalog_name_for_source

        # The rows as registered, kept for the engine's attach (published below as
        # ``runtime_sources``): the patch that follows replaces a postgresql row's ``database``
        # with its catalog name, and an attach handed that dials a database named after the
        # source id.
        _connection_rows = {_sid: dict(_row) for _sid, _row in sources.items()}
        for _sid, _src_dict in list(sources.items()):
            if _sid not in state.source_types and _src_dict.get("type"):
                state.source_types[_sid] = _src_dict["type"]
            if _src_dict.get("type") == "postgresql":
                sources[_sid] = {**_src_dict, "database": source_to_catalog(_sid)}
            if _sid not in state.source_catalogs and _src_dict.get("type"):
                state.source_catalogs[_sid] = catalog_name_for_source(
                    state, _src_dict["type"], _sid
                )
        # REQ-1491: a branch's ``sources`` row carries its own bound flag and, once somebody has
        # bound it, its own connection columns — there is nothing to resolve from another
        # environment. An unbound row is left exactly as it is; the write guard refuses it
        # (_reject_unbound_writes) and the query path reads its empty host as no source, not as
        # localhost.
        _env = active_env()
        # REQ-1622: ``${scope:ENV}`` in a source's address resolves HERE, where the environment is
        # known, rather than at config load where it is not yet. Only the scope provider is expanded
        # -- a ``${env:...}`` or ``${secret:...}`` in the same string is a credential that resolves
        # at its own use point, and pulling one forward would read a name nothing was going to ask
        # for. What this buys is REQ-1622's rule: an address carrying the environment's name is
        # somewhere the environment owns alone, so retiring it may remove what is there.
        from provisa.core.secrets import expand_scope

        for _rows in (sources, _connection_rows):
            for _sid, _src_dict in _rows.items():
                _rows[_sid] = {
                    k: expand_scope(v) if isinstance(v, str) and "${scope:" in v else v
                    for k, v in _src_dict.items()
                }
        if _env != PROD:
            from provisa.core.env_classes import BINDING_COLUMN, OWN

            state.source_binding_env = {
                sid: _env for sid, row in sources.items() if row[BINDING_COLUMN] == OWN
            }
        # Publish the full DB source map so NativeEngineBackend._attach_registered can attach
        # dynamically registered sources that are not in state.config (YAML-loaded only).
        state.runtime_sources = _connection_rows
        # REQ-1914: this worker's per-source state follows the rows — a source another worker
        # registered, changed or deleted gets its pool, dialect and catalog name here.
        from provisa.api.model_reload import reconcile_sources

        await reconcile_sources(_connection_rows)
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
                        _roles_t.c.parent_role_id,  # REQ-1677
                        # REQ-1921: the data_residency grant's covered values. state.roles feeds
                        # residency_values_for_claims (the region-edit gate); without this column a
                        # role holding data_residency resolves to a dict with no residency_values
                        # and every region change raises KeyError.
                        _roles_t.c.residency_values,
                        # REQ-005, REQ-1919: the role's own ceiling and join guard, as stored.
                        _roles_t.c.max_rows,
                        _roles_t.c.relationship_guard,
                    )
                )
            ).fetchall()
        ]
        # REQ-1677: fold each role's inheritance chain into the build's own copies here, once, so
        # every role-keyed lookup below (rights, visibility, writable_by, unmasked_to, RLS) sees
        # the inherited grants under the acting role's own id.
        from provisa.security.inheritance import (
            expand_column_grants,
            flatten_role_dicts,
            role_chains,
        )

        role_chains_by_id = role_chains(roles)
        roles = flatten_role_dicts(roles)
        expand_column_grants(tables, role_chains_by_id)
        state.role_chains = role_chains_by_id

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

        state.federation_engine.reseed_ops(_OPS_VIEWS)

        await _register_user_views_in_state(conn, raw_config)

        # Schema-currency reconcile (REQ-846/932): converge this environment's own landing tables
        # to the registered shape before introspection reads them. Boot and post-mutation registration
        # both do this for whatever env they touch, but neither runs against an environment created by
        # copying another (REQ-1529/env_create) — its MATERIALIZED/FETCH sources (e.g. an openapi
        # source landed via TrinoPgBackedConnector) would otherwise have no relation for
        # introspect_tables to read, and _build_visible_tables silently drops every table it can't
        # find column metadata for. DDL only, best-effort like the other two call sites.
        # REQ-1912: which tables are read from their replica, and where — published before the
        # reconcile and before any statement is lowered against this registry.
        from provisa.api.model_reload import publish_replica_routes

        await publish_replica_routes(_stamped_runtime)
        try:
            _landed = await state.federation_engine.reconcile_landed_tables()
            if _landed:
                _rebuild_log.info(
                    "reconciled %d landed table(s) into the materialization store", len(_landed)
                )
        except Exception:
            _rebuild_log.exception("landed-table schema reconcile failed")

        # Introspect the engine metadata
        col_types_converted: dict[int, list[ColumnMetadata]] = introspect_tables(
            state.engine_conn, tables, sources, {**_META_TABLE_ALIAS, **(kafka_physical or {})}
        )

        _gql_remote_srcs = getattr(state, "graphql_remote_sources", {})

        # Inject required GQL args as native filter columns for graphql_remote tables.
        _inject_gql_required_args(tables, _gql_remote_srcs)

        # Build gql_object_columns: {table_name: {col_name: [sub_field_names]}} for JSON extraction
        _gql_object_cols = _build_gql_object_columns(_gql_remote_srcs)

        # Synthesize ColumnMetadata for ops, provisa-admin and graphql_remote tables
        _synthesize_column_metadata(tables, col_types_converted, _gql_remote_srcs)

        # Load API sources and endpoints (Phase U)
        from provisa.api_source.loader import load_api_sources

        state.api_endpoints, state.api_sources = await load_api_sources(_pg, state.source_types)

        # REQ-1915: no boot fill of an API table's collection. Its replica is built by the
        # runner when the model declares one; a request's own calls are fills in the store.

        # Load RLS rules — domain_id is required so domain-scoped rules (REQ-402)
        # are not silently dropped by build_rls_context. Read through the repo so the
        # encrypted filter_expr (REQ-686) is decrypted back to SQL at this boundary.
        from provisa.core.repositories import rls as _rls_repo

        rls_rules = await _rls_repo.list_all(conn)

        await _load_masking_rules(conn, col_types_converted, roles, role_chains_by_id)
        await _load_kept(conn)  # REQ-1942
        await _check_fakes(conn)  # REQ-1494

        tracked_functions, tracked_webhooks = await _load_tracked_functions_and_webhooks(
            conn, raw_config
        )
        # REQ-1677: child-precedence RLS — the nearest role in the chain with a rule for a table
        # (REQ-1679: or an action) supplies that predicate for the child.
        from provisa.security.inheritance import expand_grants, materialize_rls

        rls_rules = materialize_rls(
            rls_rules, tables, role_chains_by_id, [*tracked_functions, *tracked_webhooks]
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
        expand_grants(_metric_dicts, role_chains_by_id)  # REQ-1677
        expand_grants(tracked_functions, role_chains_by_id)
        expand_grants(tracked_webhooks, role_chains_by_id)

        # REQ-1903: stable gRPC proto field numbers across schema regenerations — loaded from this
        # org's tenant DB before the build, persisted after it sees every role's + the wire
        # schema's columns.
        from provisa.grpc.field_numbering import (
            load_field_number_allocator,
            persist_field_numbers,
        )

        _field_numbers = await load_field_number_allocator(conn)

        # The data writes each table's source can take, decided once here and carried on its
        # record (executor/write_capability.py): the write admission, the GraphQL and gRPC write
        # surfaces and the admin table page all read it.
        from provisa.executor.write_capability import (
            table_write_ops,
            table_write_refused_forms,
            table_write_returns_rows,
        )

        for _t in tables:
            # A view has no source of its own to write to; every other table's source is typed.
            _stype = None if _t.get("view_sql") else state.source_types[_t["source_id"]]
            _t["write_ops"] = sorted(table_write_ops(_t, _stype, state.federation_engine.engine))
            _t["write_returns_rows"] = table_write_returns_rows(
                _t, _stype, state.federation_engine.engine
            )
            _t["write_refused_forms"] = sorted(
                table_write_refused_forms(_t, _stype, state.federation_engine.engine)
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
            metrics=_metric_dicts,  # REQ-1319
            field_numbers=_field_numbers,
            draft_tables=draft_tables,  # REQ-1921
        )

        await persist_field_numbers(conn, _field_numbers)

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

    # REQ-1921: a table or view that has gone draft since the last build keeps no copy in this
    # region: its cached responses go (its replicas retire with convergence, its view build with
    # the reclamation sweep). Entries are kept by place, not by table, so the place is purged.
    from provisa.cache.tenancy import purge_when_drafted

    await purge_when_drafted(state, frozenset(t["id"] for t in draft_tables))

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
    _stamped_runtime.model_stamp = _model_stamp  # REQ-1914
    await _finalize_rebuild_state(_rebuild_log)
    # REQ-1494: the measured values faked reads compute from, taken from the model just built.
    from provisa.fakes.measured import measure_model

    await measure_model(state)
    # REQ-1915: replicas converge to the model just built — a build is requested for every
    # declared replica that has none (or whose definition changed), and a replica the model no
    # longer declares is retired. This is the one place: the boot build, the build after this
    # worker's own change, and the reload of another worker's change all pass here, so no save
    # or delete path asks for a build or a drop itself. Detached: the model build must not
    # wait on, or be stopped by, a replica store or a source.
    from provisa.core import process_mode as _process_mode

    if _process_mode.runs_background_work():
        from provisa.core.connection_loop import spawn_background as _spawn_background
        from provisa.federation.replica_converge import converge_logged as _converge_replicas

        _spawn_background(_converge_replicas(state), name="replica-converge")
    # REQ-1072: the governed model just changed, so the external catalog is now stale. This is
    # the one chokepoint every model mutation passes through, which is why the event is posted
    # here rather than at each mutation — a new mutation cannot forget to publish.
    from provisa.api.metadata_export.publishing import notify_model_changed

    if announce:
        await notify_model_changed(state.active_org_id, reason="schema rebuild")

    # REQ-1882 (amended 2026-09-29): the re-wiring below starts process-lifetime listeners, ingest
    # engines and scheduler jobs. Run inline on the process loop; from a connection-thread loop (an
    # org runtime built on first pgwire/Bolt/Flight access) it is started on the process loop,
    # since the connection loop stops running when its request ends.
    async def _rewire_lifecycle() -> None:
        # REQ-1745: re-wire push-source landing and ingest engines on EVERY rebuild, not only
        # register_runtime's per-org build (that call site never fires for the default/single-tenant
        # path this function is on when a mutation calls `_rebuild_schemas()` directly — e.g.
        # schema_mutation.py's registerTable). Without this, a kafka/websocket/ingest source
        # registered live through the Sources+Register Table forms never got its listener started or
        # its ingest engine/DDL built at all: state.push_listener_disconnects/state.ingest_tables
        # stayed exactly as they were at process boot, forever, on a server that never restarts. Both
        # calls are idempotent/best-effort by design (wire_push_listeners skips nodes already running;
        # _init_ingest_engines rebuilds its maps fresh from the DB each call), so calling them again
        # here is never harmful, only occasionally redundant with register_runtime's own call.
        from provisa.events.push_wiring import wire_push_listeners

        try:
            await wire_push_listeners(state=state, log=logging.getLogger(__name__))
        except Exception:
            logging.getLogger(__name__).exception(
                "wire_push_listeners failed during schema rebuild"
            )
        try:
            await _init_ingest_engines()
        except Exception:
            logging.getLogger(__name__).exception(
                "_init_ingest_engines failed during schema rebuild"
            )
        # REQ-1770: a table on a poll-only adapter-fetch source (rss is the current example) only got
        # its poll job (re)registered by wire_event_loop, which register_runtime calls but this
        # function does not — a table registered against an already-running runtime (the common case
        # outside a fresh per-org build) had no poll job until the next org-runtime rebuild. Calling
        # wire_event_loop here too was tried and reverted: it re-derives adapter_loaders and re-walks
        # every registered source's poll-job registration on EVERY schema rebuild (not just ones
        # involving a new poll-only source), and broke an unrelated already-registered sqlite source's
        # live queries in testing. Fixed instead with wire_new_poll_jobs (provisa/events/app_wiring.py):
        # a per-node-scoped rewire mirroring wire_push_listeners' own state.push_listener_disconnects
        # idempotency via state.poll_jobs_registered — it registers a poll job ONLY for a node that
        # doesn't already have one, appending its processor into the SAME list object the running tick
        # job's closure already holds, rather than re-deriving/re-walking every other source's spec.
        from provisa.events.app_wiring import wire_new_poll_jobs

        try:
            await wire_new_poll_jobs(state=state, log=logging.getLogger(__name__))
        except RuntimeNotBuilt as gone:
            # The runtime this rebuild is for was dropped while it ran (a change of the
            # environment's data, an engine wake): the one built in its place wires its own.
            left_to_the_next_runtime(gone, "poll-job wiring after a schema rebuild")
        except Exception:
            logging.getLogger(__name__).exception("wire_new_poll_jobs failed during schema rebuild")

        # REQ-1865: re-wire the row-materialize background refresh drain + reaper on EVERY rebuild,
        # same posture as wire_push_listeners above (registered jobs are idempotent via
        # replace_existing=True, and the row_materialize table set is derived fresh each call, not
        # incrementally like wire_new_poll_jobs) — so a table that becomes row-level while the
        # server runs (a config reload; no admin mutation or UI field sets the flag) gets its
        # background jobs without requiring a restart.
        _scheduler = getattr(state, "_scheduler", None)
        if _scheduler is not None:
            from provisa.events.row_materialize_lifecycle import wire_row_materialize_background

            _rm_cfg = getattr(getattr(state, "config", None), "row_materialize", None)
            if _rm_cfg is not None:
                try:
                    await wire_row_materialize_background(
                        _scheduler,
                        state=state,
                        log=logging.getLogger(__name__),
                        tick_seconds=_rm_cfg.refresh_tick_seconds,
                        reap_interval_seconds=_rm_cfg.reap_interval_seconds,
                        reap_grace_period=_rm_cfg.reap_grace_period,
                        reap_batch_size=_rm_cfg.reap_batch_size,
                    )
                except Exception:
                    logging.getLogger(__name__).exception(
                        "wire_row_materialize_background failed during schema rebuild"
                    )

    await run_lifecycle_work(_rewire_lifecycle(), name="schema-rebuild-lifecycle")


class _DebugLogBufferHandler(logging.Handler):
    """In-memory ring buffer of routing/auth diagnostics, read back via /debug/logs."""

    def __init__(self, capacity: int = 5000) -> None:
        super().__init__()
        self.buffer: deque[str] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        # Routing/auth diagnostics only -- apscheduler/trino background chatter fires every
        # few seconds and would otherwise evict these entries long before anyone reads them.
        if not record.name.startswith("provisa.api."):
            return
        self.buffer.append(self.format(record))


def _boot_generation(launch: str | None) -> str | None:  # REQ-1900
    """The generation this process's once-per-launch boot work belongs to, or ``None`` for a
    process that is not a worker of a launch. Everything that work is derived from is in it — the
    control-plane schema, the config as it stands on disk, the engine — so a
    worker whose inputs differ from the ones the work was done for does the work itself."""
    if launch is None:
        return None
    from provisa.core.boot_lock import boot_generation

    path = Path(config_path_str())
    schema_sql = (Path(__file__).parent.parent / "core" / "schema.sql").read_text()
    return boot_generation(
        launch,
        schema=hashlib.sha256(schema_sql.encode()).hexdigest(),
        config=read_config_with_includes(path) if path.exists() else None,
        engine=state.federation_engine.name,
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):  # pyright: ignore[reportUnusedParameter, reportUnusedVariable]
    """App lifespan: load config and build schemas at startup."""
    _log = logging.getLogger("uvicorn.error")
    # Set up in-memory log buffer for debugging (max 1000 lines)
    buffer_handler = _DebugLogBufferHandler(capacity=5000)
    buffer_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logging.getLogger().addHandler(buffer_handler)
    # WARNING and above from every provisa.* module, printed on the server's own log: without
    # this they reach only the root handlers (the buffer above, the OTLP export), never stderr.
    from provisa.core.server_log import send_module_logs_to_server_log

    send_module_logs_to_server_log()
    state.schema_boot_id = uuid.uuid4().hex
    # REQ-074: the audit writer lives as long as the application — started here, before anything
    # serves a request, and stopped in the shutdown below. A request only enqueues to it.
    from provisa.audit.writer import start_audit_writer

    start_audit_writer()
    from provisa.api.setup_router import _auto_configure_idp, _idp_override

    async def _once_per_launch() -> None:
        """Everything the boot writes to the control plane and the engine's shared catalogs."""
        await _load_and_build()

        # REQ-1267: when PROVISA_IDP names an identity provider (e.g. firebase from the
        # GCP/installer deploy) but the loaded config carries no auth section, configure it now —
        # BEFORE any request reaches the middleware — so the server enforces auth from its first
        # request instead of serving an unsecured admin window until the UI happens to call
        # /setup/status.
        #
        # REQ-1472: the call is made whenever PROVISA_IDP names a provider, not only when the auth
        # section is absent. An already-configured deployment takes the function's reconcile path,
        # which fills in keys a deploy predating them never wrote (the break-glass account and its
        # signing key) and leaves everything else exactly as configured.
        #
        # REQ-1900: once per launch — it writes the config FILE, including a freshly generated
        # signing key. Run per worker, each worker would sign sessions with a key of its own.
        _idp = _idp_override()
        if _idp and state.admin_db is not None:
            await _auto_configure_idp(_idp, state.admin_db)

        await _auto_register_graphql_demo(_log)

        # REQ-1598: the org the public "Try it Out" invite admits visitors to. Seeded after the
        # registry and the org runtime are up, so a redemption never arrives at an org that is
        # missing.
        await _seed_sandbox_org(_log)

    try:
        # REQ-1900: `--workers N` runs this in N processes at once. The first worker to take the
        # boot lock does the once-per-launch work under it and records its generation complete;
        # every other worker takes the lock only to read that record, then does its per-worker
        # half with no lock held — all of them at the same time. A process outside a launch (no
        # launch id) has no generation and does the whole boot under the lock. Waiting for the
        # lock blocks this thread, which at startup has nothing else to serve.
        from provisa.core.boot_lock import control_plane_boot_lock, expected_workers, launch_id
        from provisa.core.config_loader import load_control_plane

        _cp = load_control_plane(config_path_str())
        _scope = _cp.resolved_org_id()
        # REQ-1266: the boot builds and serves the deployment's own org, so it runs bound to it,
        # and so does the background work it starts (spawned work copies the binding). Nothing
        # reads an org's runtime unbound.
        state.org_id = _scope
        _boot_org_token = set_current_org(_scope)
        _launch = launch_id()
        # A launcher that names a launch names its worker count with it; one without the other
        # fails here, at boot, rather than in the first /health request.
        expected_workers()
        with control_plane_boot_lock(_cp.resolved_platform_url()) as _boot:
            _applied_elsewhere = _boot.completed(_scope, _boot_generation(_launch))
            if not _applied_elsewhere:
                # REQ-1524: what the boot writes to the model is one change, committed at its end.
                async with model_change.scope("boot"):
                    await _once_per_launch()
                # Computed again: the work above may have rewritten the config file (the auth
                # section), and the generation the other workers compute is of the file as it now
                # stands.
                _boot.mark_completed(_scope, _boot_generation(_launch))
        if _applied_elsewhere:
            async with model_change.scope("boot"):
                await _load_and_build(apply=False)
        _log.warning(
            "startup phase %-20s %s pid=%d",
            "once-per-launch",
            "found complete" if _applied_elsewhere else "applied",
            os.getpid(),
        )
    except Exception:
        _log.exception("Startup failed during _load_and_build")
        raise

    # REQ-1574: teach the encryption accessor how to find the bound org, and record which orgs hold
    # a key ring of their own. The roster is what makes selection fail closed -- an org named here
    # is served its own key or refused, never quietly written under the deployment's.
    if state.admin_db is not None:
        from provisa.core.org_encryption import ring_owner_org_ids
        from provisa.encryption.runtime import bind_org_selector, note_org_rings

        bind_org_selector(current_org.get)
        note_org_rings(await ring_owner_org_ids(state.admin_db))

    # REQ-1913: the live operator settings this worker holds as state (its request-thread bounds)
    # are applied now, from the stored values. REQ-1914: this worker's config watcher applies
    # them again when the stored settings change, and reloads each org's model and settings when
    # theirs do — one mechanism, on the operator's reload interval.
    from provisa.api import model_reload
    from provisa.core import config_watch, settings_registry

    settings_registry.apply_changes()
    config_watch.start(model_reload.targets)

    await _start_background_tasks(_log)

    await _start_servers(_log)

    # Prime the lazy per-request paths in the background and flip /ready when warm. Background (not
    # awaited) so /health and /live serve immediately; a readiness-gated launcher waits on /ready.
    # REQ-1882: the probe blocks on the engine and control plane, so it runs on its own thread,
    # never on the process loop that relays request I/O.
    from provisa.core.connection_loop import spawn_long_lived

    state._warmup_task = spawn_long_lived(_warmup_readiness(_log), name="readiness-warmup")

    _start_scheduler(_log)
    # REQ-1003: the deployment org's scheduled triggers, from the model the boot just loaded (the
    # config file's among them, origin config). The boot runs bound to this org.
    if state._scheduler is not None:
        from provisa.scheduler.jobs import register_org_triggers

        await register_org_triggers(state._scheduler, state.org_id, None)
        # REQ-286/REQ-1266: the deployment org's live-query engine, on the process's scheduler.
        from provisa.api.app_rebuild import start_org_live_engine

        try:
            await start_org_live_engine(state._scheduler)
        except Exception:
            _log.exception("Live Query Engine startup failed")

    # Snapshot the config AFTER all boot-time auto-derivation, so the admin config-diff baseline
    # excludes runtime-derived entities (REQ-164). Opt-in; best-effort (the helper degrades and the
    # diff falls back to the on-disk file).
    await _capture_config_boot_snapshot(_log)

    # REQ-1900: when the launcher names the public HTTP address, this worker listens on it with a
    # socket of its own (SO_REUSEPORT) and serves this same app there, so the kernel spreads
    # connections over the workers instead of a few of them taking most (see http_listener.py).
    from provisa.api.http_listener import WorkerHttpListener, configured_address

    _http_address = configured_address()
    if _http_address is not None:
        state._http_listener = WorkerHttpListener(_app, *_http_address)
        state._http_listener.start()

    # One line per worker process when it starts serving (REQ-1900): `--workers N` boots N of
    # these, and the launch is ready when all N have logged it.
    _log.warning("startup phase %-20s ready pid=%d", "worker", os.getpid())

    # REQ-1900: /health reports how many of the launch's workers are serving, so it is counted in
    # the control plane, where every worker can read it.
    if _launch is not None and state.admin_db is not None:
        from provisa.core.boot_lock import register_ready_worker

        await register_ready_worker(state.admin_db, _launch)

    # REQ-1916: this node is in the cluster's node list (the platform state store) while it serves,
    # with its mode and region, beating so a node that dies without stopping drops off.
    assert state.platform_state_db is not None  # brought up with the control planes at boot
    from provisa.core.platform_state import nodes as _cluster_nodes
    from provisa.fakes import platform_key as _fake_key
    from provisa.fakes.digest import set_key as _set_fake_key

    # REQ-1494: the platform key every fake's digest is keyed by, created at the platform's first
    # start, held by this process for its engine's functions and exported to a separate engine.
    assert state.admin_db is not None  # the platform control plane holds the settings
    _set_fake_key(_fake_key.ensure(state.admin_db))

    await _cluster_nodes.register(state.platform_state_db)
    _node_heartbeat = spawn_long_lived(
        _cluster_nodes.heartbeat_loop(state.platform_state_db), name="node-heartbeat"
    )

    # REQ-1882/REQ-1905: while serving, a stop signal ends in-flight requests (their statements
    # are cancelled through the driver) before the server's own shutdown waits for them.
    from provisa.core.request_deadline import expire_on_stop_signals

    with expire_on_stop_signals():
        yield

    # REQ-1900: stop accepting on this worker's own HTTP socket and let its in-flight requests
    # finish, before anything they use below is closed.
    if state._http_listener is not None:
        await state._http_listener.stop()
        state._http_listener = None

    if _launch is not None and state.admin_db is not None:
        from provisa.core.boot_lock import unregister_worker

        await unregister_worker(state.admin_db, _launch)

    _node_heartbeat.cancel()
    await _cluster_nodes.unregister(state.platform_state_db)  # REQ-1916: this node leaves the list

    # REQ-1629: the engine idle reaper lives in this process, so a shard still up when the control
    # plane goes away has nothing left that can scale it down and bills until somebody notices.
    # Taking them down here is what makes the coordinator's own stop the shard's stop too.
    from provisa.federation.engine_wake import stop_provisioned_shards

    await stop_provisioned_shards(state)

    # The protocol listeners this app started stop accepting here, after the HTTP listener and
    # before anything a request uses is closed below. Stopping one ends its accept thread and
    # closes its listening socket; a connection it already accepted is not cut by this -- its
    # request runs to its own deadline (provisa/core/request_deadline.py). Left running, each
    # stayed bound to the shared port and kept accepting for an app that had shut down (four
    # leftover Bolt accept threads in the e2e lane's process).
    from provisa.pgwire.server import stop_pgwire_server

    if state._bolt_listener is not None:
        await asyncio.to_thread(state._bolt_listener.close)
        state._bolt_listener = None
    if state._pgwire_server is not None:
        await asyncio.to_thread(stop_pgwire_server, state._pgwire_server)
        state._pgwire_server = None

    # Stop the Arrow Flight servers: each relay (the advertised port) before the server behind it.
    if state._flight_relay:
        state._flight_relay.close()
    if state._flight_server:
        state._flight_server.shutdown()
    if state._airport_relay is not None:
        await asyncio.to_thread(state._airport_relay.close)
        state._airport_relay = None
    if state._airport_server is not None:
        await asyncio.to_thread(state._airport_server.shutdown)
        state._airport_server = None

    # Stop gRPC server
    if state._grpc_server:
        # grpc.Server.stop returns a threading.Event set once in-flight RPCs finish (or the grace
        # expires); waited off the loop so shutdown's other awaits are not blocked meanwhile.
        _grpc_stopped = state._grpc_server.stop(grace=5)
        await asyncio.to_thread(_grpc_stopped.wait)

    # Cancel the readiness warmup probe (it may still be priming if shutdown raced boot)
    if state._warmup_task:
        await _stop_long_lived(state._warmup_task)

    # Cancel hot-table refresh task (Phase AD6)
    if state._hot_refresh_task:
        await _stop_long_lived(state._hot_refresh_task)

    # Stop the Hot promotion evaluation (REQ-826)
    if state._replica_hot_task:
        await _stop_long_lived(state._replica_hot_task)
    if state.hot_manager is not None:
        from provisa.cache.hot_tables import HotTableManager

        assert isinstance(state.hot_manager, HotTableManager)
        await state.hot_manager.close()

    # Cancel MV refresh task
    if state._mv_refresh_task:
        await _stop_long_lived(state._mv_refresh_task)
    from provisa.api.startup_resilience import tolerate_shutdown_failure

    # REQ-1690: the Calcite pgwire servers a native engine attached live. FIRST, before the long
    # tail of database and engine closes below: each is a JVM in its own session (it has to be, so
    # that stopping it signals its own process group and not ours), so it does not die with this
    # process. A supervisor that SIGKILLs us partway through shutdown -- Playwright's webServer
    # teardown does exactly that -- would strand one holding its port and our stdout. Stopping
    # them here costs the rest of the shutdown nothing: what closes below are this process's own
    # handles, and a DETACH of an endpoint that has already gone is local to DuckDB.
    with tolerate_shutdown_failure("pgwire connector servers stop"):
        from provisa.federation.pgwire_replica import stop_all_servers

        stop_all_servers()

    # Stop every org's Live Query Engine (Phase AM, REQ-1266)
    for _live in state.org_registry.live_engines():
        with tolerate_shutdown_failure("live query engine stop"):
            await _live.stop()

    # Close APQ cache (Phase AN)
    with tolerate_shutdown_failure("APQ cache close"):
        await state.apq_cache.close()

    # Stop push-source (kafka/websocket) CDC landing listeners (REQ-1733)
    with tolerate_shutdown_failure("push listener shutdown"):
        from provisa.events.push_wiring import shutdown_push_listeners

        await shutdown_push_listeners(state)

    # Stop scheduler (Phase AX)
    if state._scheduler is not None:
        with tolerate_shutdown_failure("scheduler shutdown"):
            state._scheduler.shutdown(wait=False)
    # REQ-1900: end the holder's control-plane session, so a worker that held the scheduler lock
    # hands it on at shutdown rather than when its process is finally reaped.
    if state._region_holder is not None and state._region_holder is not state._scheduler_holder:
        state._region_holder.close()
    state._region_holder = None
    if state._scheduler_holder is not None:
        state._scheduler_holder.close()
        state._scheduler_holder = None

    # REQ-172, REQ-176: the change events and sink rows already handed over are sent, and the
    # process's Kafka producers stopped, while each thread is still its own to end -- before the long-lived threads are stopped below, and
    # after everything that writes (the listeners, the scheduler) has stopped.
    with tolerate_shutdown_failure("kafka producer stop"):
        from provisa.kafka import change_events
        from provisa.kafka import producer as kafka_producer

        change_events.stop()
        await kafka_producer.stop_all()

    # REQ-1882: stop the background worker pool, its timer and long-lived threads before the
    # databases and engines they use close below. Blocking (bounded) — run off the process loop.
    from provisa.audit.writer import shutdown_audit_writer
    from provisa.core.connection_loop import shutdown_background

    # REQ-074: the audit records still queued are written before their databases close.
    await asyncio.to_thread(shutdown_audit_writer)
    await asyncio.to_thread(shutdown_background)

    _shutdown_otel()

    await state.response_cache_store.close()
    for _region_store, _ in _region_caches.values():  # REQ-1922: the regions' cache stores
        await _region_store.close()
    _region_caches.clear()
    await state.source_pools.close_all()
    if state.tenant_db:
        await state.tenant_db.close()
    # REQ-1244: every org with a dedicated federation engine owns a live terminal — close each,
    # then the shared engine (the deployment org's, reached through the boot's binding below).
    # Each non-default org runtime also owns its own tenant_db pool (the default's is state.tenant_db,
    # already closed above) — leaving it open leaks a pool's worth of connections per org built
    # during this process's life.
    for _oid in state.org_registry.all_org_ids():
        _rt = state.org_registry.get(_oid)
        if _rt is not None and _oid != state.org_id:
            if _rt.tenant_db is not None:
                with tolerate_shutdown_failure(f"org {_oid} tenant_db close"):
                    await _rt.tenant_db.close()
            if _rt.federation_engine is not None:
                with tolerate_shutdown_failure(f"org {_oid} federation engine close"):
                    # close() reaches its terminal through the routed state shims, so the org must
                    # be bound or the shims would resolve the SHARED engine's connection.
                    _tok = set_current_org(_oid)
                    try:
                        _rt.federation_engine.close()
                    finally:
                        reset_current_org(_tok)
    state.federation_engine.close()
    if state.admin_db is not None:
        model_change.detach()
        with tolerate_shutdown_failure("admin_db close"):
            await state.admin_db.close()
    reset_current_org(_boot_org_token)


async def _stop_long_lived(handle: Any, timeout: float = 10.0) -> None:
    """Cancel a long-lived background thread and wait (bounded, off the loop) for it to end."""
    handle.cancel()
    if not await handle.wait(timeout):
        logging.getLogger(__name__).warning(
            "shutdown: long-lived task %s did not stop within %.0fs", handle.name, timeout
        )


def create_app() -> FastAPI:
    """Create the FastAPI application."""
    from fastapi.middleware.cors import CORSMiddleware
    from provisa.api.json_response import OrjsonResponse
    from strawberry.fastapi import GraphQLRouter

    from provisa.api.admin.schema import admin_schema

    # Swagger/OpenAPI live under /data/openapi/ (not the default /docs) so the UI can
    # own /docs for its in-app documentation reader.
    # REQ-1867: OrjsonResponse (orjson-backed) instead of FastAPI's stdlib-json default for every
    # route that doesn't explicitly return its own Response — orjson was already resolving
    # transitively (langsmith/trino pull it in) but nothing in this app actually used it; every
    # JSON response paid stdlib json's slower encode. A route building its own JSONResponse/
    # Response explicitly (there are several, e.g. endpoint_dev.py's stats-enabled branches)
    # keeps doing so unaffected — this only changes the class FastAPI reaches for on its own.
    app = FastAPI(
        title="Provisa",
        lifespan=lifespan,
        docs_url="/data/openapi/docs",
        redoc_url="/data/openapi/redoc",
        openapi_url="/data/openapi/openapi.json",
        default_response_class=OrjsonResponse,
    )
    state.shared_federation_engine.write_config(config_path_str())
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

    from provisa.core.read_refusal import ReadRefused as _ReadRefused

    @app.exception_handler(_ReadRefused)
    async def _read_refused_handler(_req: _Request, exc: _ReadRefused):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # A read the deployment cannot answer now (core/read_refusal.py): a table kept in a
        # region that cannot be reached or whose replica there is not built (REQ-1922), a
        # replica the read needs that could not be built (REQ-1661). 503: the statement is good
        # and the answer exists but cannot be given now; the body names the table and the reason.
        return _JSONResponse(
            status_code=503,
            content={"detail": str(exc), "code": exc.code, "params": exc.params},
        )

    from provisa.core.secrets_providers import SecretNameRefused as _SecretNameRefused

    @app.exception_handler(_SecretNameRefused)
    async def _secret_name_refused_handler(_req: _Request, exc: _SecretNameRefused):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        from provisa.api.errors import secret_name_refused_response

        return secret_name_refused_response(exc)

    from provisa.core.operator_floor import OperatorFloorError as _OperatorFloorError

    from provisa.compiler.complexity import ComplexityLimitExceeded as _ComplexityLimitExceeded

    @app.exception_handler(_ComplexityLimitExceeded)
    async def _complexity_limit_handler(_req: _Request, exc: _ComplexityLimitExceeded):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # REQ-1174: a statement over the role's complexity limit. A route that does not answer
        # the refusal itself answers it here: 413, the query asks for too much.
        return _JSONResponse(
            status_code=413,
            content={
                "detail": str(exc),
                "code": "data.query_too_complex",
                "params": {
                    "score": exc.complexity.score,
                    "limit": exc.limit,
                    "limit_of": exc.limit_of,
                },
            },
        )

    from provisa.compiler.write_admission import WriteNotSupported as _WriteNotSupported

    @app.exception_handler(_WriteNotSupported)
    async def _write_not_supported_handler(_req: _Request, exc: _WriteNotSupported):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # A write the table's source cannot take, refused by name (executor/write_capability.py).
        return _JSONResponse(
            status_code=400,
            content={
                "detail": str(exc),
                "code": "data.write_not_supported",
                "params": {"table": exc.table, "operation": exc.operation.upper()},
            },
        )

    from provisa.executor.errors import SystemCatalogUnavailable as _SystemCatalogUnavailable

    @app.exception_handler(_SystemCatalogUnavailable)
    async def _system_catalog_handler(_req: _Request, exc: _SystemCatalogUnavailable):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # The engine lacks one of Provisa's own catalogs at this moment (being re-created after
        # its spec changed) and still did when the statement's retries ran out: the deployment's
        # state, not the caller's error -- 503, to be tried again, never the engine's USER_ERROR.
        return _JSONResponse(
            status_code=503,
            content={
                "detail": (
                    f"the engine's {exc.catalog!r} catalog is being registered; try again shortly"
                ),
                "code": exc.code,
                "params": exc.params,
            },
            headers={"Retry-After": "1"},
        )

    from provisa.compiler.definitions import TableIsDraft as _TableIsDraft

    @app.exception_handler(_TableIsDraft)
    async def _table_is_draft_handler(_req: _Request, exc: _TableIsDraft):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # REQ-1921: a draft table or view is out of service, and the refusal says so by name.
        return _JSONResponse(
            status_code=409,
            content={
                "detail": str(exc),
                "code": "data.table_is_draft",
                "params": {"table": exc.table},
            },
        )

    from provisa.compiler.definitions import DefinitionNotAvailable as _DefinitionNotAvailable

    @app.exception_handler(_DefinitionNotAvailable)
    async def _definition_not_available_handler(_req: _Request, exc: _DefinitionNotAvailable):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # Nothing is defined through a query protocol (provisa/compiler/definitions.py). A route
        # that does not answer the refusal itself answers it here: a client error, in the one
        # message every surface gives.
        return _JSONResponse(
            status_code=400,
            content={
                "detail": str(exc),
                "code": "data.definition_not_available",
                "params": {"statement": exc.kind},
            },
        )

    @app.exception_handler(_OperatorFloorError)
    async def _operator_floor_handler(_req: _Request, exc: _OperatorFloorError):  # noqa: F841  # pyright: ignore[reportUnusedFunction, reportUnusedVariable]
        # REQ-030: a request below the operator's floor is refused, naming the setting.
        return _JSONResponse(
            status_code=403,
            content={"detail": str(exc), "code": "query.operator_floor", "params": {}},
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

    # Rate limiting (REQ-369-371): the per-role request middleware. Added BEFORE wire_auth so
    # the auth middleware (added later) runs first and populates request.state.role before the
    # rate-limit check sees it. The limiter itself is built when the config is applied
    # (app_loaders.apply_redis_settings) — here the Redis it counts in is not known yet.

    # REQ-1905: server-wide Flight stream limit, PER WORKER PROCESS, same env-var-over-server_cfg
    # pattern as GRPC_MAX_CONCURRENT_RPCS (provisa/grpc/server.py). The default is the host's
    # Flight budget (a third of min(32, cpu_count()+4)) divided among the launch's workers, floor
    # 2 — see provisa/api/flight/stream_slots.py. A stream over the limit waits for a slot.
    # REQ-1913: resolved by the settings registry and set on state when the server config is
    # applied (app_loaders._apply_server_and_engine_config) — here the config is not loaded yet.
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
    from fastapi.responses import JSONResponse

    from provisa.api.env_routing import (
        EnvironmentRightError,
        EnvironmentSelectionError,
        env_header_value,
    )
    from provisa.api.http_trace_scope import http_trace_scope

    class _OrgRoutingMiddleware:
        def __init__(self, app):
            self.app = app

        async def __call__(self, scope, receive, send):
            if scope["type"] != "http":
                await self.app(scope, receive, send)
                return

            request_state = scope.setdefault("state", {})
            active_org = request_state.get("active_org_id")
            if active_org is None:
                # REQ-1266: no org -- an unauthenticated route, or a user not yet a member of any
                # org. Served bound to nothing: a per-org read on it is refused, never answered
                # from some org's runtime, and no org's environment or trace window applies.
                await self.app(scope, receive, send)
                return
            # REQ-1487: the environment the request names, checked against the org that owns it
            # BEFORE anything is bound to it — see provisa.api.env_routing for why an unknown name
            # is a refusal and not a quiet fall back to prod. Read for the default org too: a
            # single-org deployment branches its model exactly as a multitenant one does.
            requested_env = env_header_value(scope.get("headers") or [])
            env_org = active_org
            import logging as _logging

            _log = _logging.getLogger(__name__)
            _log.info(
                f"RoutingMiddleware: active_org={active_org}, env_org={env_org}, requested_env={requested_env}"
            )
            # REQ-1573: being served by anything but prod is a right, checked here because this is
            # where the environment is bound — one gate for every surface. ``None`` means dev/no-auth
            # (no identity resolved), the exemption every capability gate makes.
            identity = request_state.get("identity")
            # REQ-1266: judged by the named org's own PROD role definitions.
            from provisa.auth.middleware import org_env_capabilities

            env_caps = await org_env_capabilities(identity, env_org)
            # REQ-1602/REQ-1596: sandbox ephemeral auto-select and the membership pin both live in
            # resolve_selected_env, shared with AuthMiddleware's role read (provisa.auth.middleware)
            # so the two always agree on which environment's schema a request is served from.
            from provisa.api.env_routing import resolve_selected_env

            try:
                selected_env = await resolve_selected_env(
                    state.admin_db,
                    env_org,
                    identity,
                    requested_env,
                    env_caps,
                    # REQ-1618: AuthMiddleware settles this on the platform plane and publishes it
                    # here; it is not derivable from ``env_caps``, which inside a tenant org has
                    # already had the control-plane roles stripped out of it (REQ-1327). Absent for
                    # a request that resolved no identity at all, which is not the control plane.
                    is_control_plane=bool(request_state.get("can_cross_org")),
                )
                _log.info(f"resolve_selected_env returned: {selected_env}")
            except EnvironmentSelectionError as exc:
                _log.info(
                    f"EnvironmentSelectionError: {exc}, env_org={env_org}, requested_env={requested_env}"
                )
                await JSONResponse(
                    {"error": {"code": "env.unknown", "message": str(exc)}}, status_code=404
                )(scope, receive, send)
                return
            except EnvironmentRightError as exc:
                _log.info(
                    f"EnvironmentRightError: {exc}, env_org={env_org}, requested_env={requested_env}"
                )

                await JSONResponse(
                    {"error": {"code": "env.switch_forbidden", "message": str(exc)}},
                    status_code=403,
                )(scope, receive, send)
                return

            # Keep existing tenant cache-key call sites (which read request.state.tenant_id)
            # pointed at the same id space as the org router.
            request_state["tenant_id"] = active_org

            # REQ-1266: every org is bound to its own runtime, the deployment's own org included.
            await ensure_serving_runtime(env_org, selected_env)
            token = set_current_org(env_org)
            env_token = set_current_env(selected_env)
            try:
                # REQ-1910: the org and role are bound and the body is unread — the request is
                # served under its debug-trace window from here, so its ASGI receive span follows it.
                async with http_trace_scope(state, scope):
                    await self.app(scope, receive, send)
            finally:
                reset_current_env(env_token)
                reset_current_org(token)
            # REQ-462: tag the trace with the org that served the request. Folded in from the
            # former _TenantSpanMiddleware, which was registered under the same dead guard.
            try:
                from opentelemetry import trace as _trace

                _span = _trace.get_current_span()
                if _span.is_recording():
                    _span.set_attribute("org_id", env_org)
                    if selected_env != PROD:
                        # REQ-1487: a trace from a branch must be readable as one; the org alone no
                        # longer says which copy of the model answered.
                        _span.set_attribute("env", selected_env)
            except (ImportError, AttributeError):
                # Best-effort span decoration: tolerate an absent OTel install or a no-op shim
                # span lacking is_recording/set_attribute. Never break a request for a tag.
                pass

    app.add_middleware(_OrgRoutingMiddleware)

    # REQ-1602: Public invite endpoint (before auth middleware) for sandbox onboarding.
    # Invites must be fetchable before login, so this route is NOT behind auth.
    @app.get("/public/invite-info/{token}")
    async def get_public_invite_info(token: str):
        from sqlalchemy import select

        from provisa.api.errors import ApiError
        from provisa.core.schema_admin import org_invites, orgs
        import datetime
        from datetime import timezone

        admin_db = state.admin_db
        if admin_db is None:
            raise ApiError(503, "service_unavailable", "Admin database not available")

        async with admin_db.acquire() as conn:
            result = await conn.execute_core(
                select(
                    org_invites.c.token,
                    org_invites.c.org_id,
                    orgs.c.name.label("org_name"),
                    org_invites.c.role_id,
                    org_invites.c.expires_at,
                    org_invites.c.uses,
                    org_invites.c.max_uses,
                )
                .select_from(org_invites.join(orgs, orgs.c.id == org_invites.c.org_id))
                .where(org_invites.c.token == token)
            )
            fetched = result.fetchone()

        row = dict(fetched._mapping) if fetched is not None else None
        if row is None:
            raise ApiError(404, "auth.invite_not_found", "Invite not found")

        now = datetime.datetime.now(tz=timezone.utc)
        if (
            row["uses"] is not None
            and row["max_uses"] is not None
            and row["uses"] >= row["max_uses"]
        ):
            raise ApiError(410, "auth.invite_already_used", "Invite already used")
        if row["expires_at"] < now:
            raise ApiError(410, "auth.invite_expired", "Invite expired")

        return {
            "token": row["token"],
            "org_id": row["org_id"],
            "org_name": row["org_name"],
            "role_id": row["role_id"],
            "valid": True,
        }

    # Conditionally add auth middleware and routes
    from provisa.auth.wiring import wire_auth

    # ActiveOrgPool, not state.tenant_db: the middleware outlives the request, and the tenant
    # control plane it must read is whichever org the request binds (REQ-1266).
    # Always None here, never state.auth_config: create_app() runs before the lifespan
    # (_load_and_build) resolves it, so state.auth_config at this point is either unset or —
    # when a prior create_app() call ran earlier in this same process (e2e tests share the
    # `state` singleton across apps) — a stale leftover from that EARLIER app. Passing it
    # would take AuthMiddleware's eager path and permanently latch onto that stale value,
    # never re-resolving. None always takes the lazy path, which reads state.auth_config
    # fresh on this app's own first request, after this app's own lifespan has run.
    wire_auth(app, None, db_pool=ActiveOrgPool(), admin_pool=state.admin_db)
    # REQ-124/REQ-1265: the password sign-in exchange. Mounted unconditionally; it answers for
    # whatever provider the lifespan binds (bind_auth_config), and 404s where there is none.
    from provisa.auth.login_router import router as login_router
    from provisa.auth.providers.saml import router as saml_router

    app.include_router(login_router)
    # REQ-1265: SAML sign-in; 404s unless the bound provider is saml.
    app.include_router(saml_router)

    # REQ-1452/REQ-1455: the egress byte meter. Registered LAST so it is the OUTERMOST middleware —
    # every response body, including the ones auth itself produces, passes through its `send`. It
    # reads the org out of scope state at flush time, so being outside auth costs it no attribution.
    from provisa.core.egress import EgressMeterMiddleware

    app.add_middleware(EgressMeterMiddleware)

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

    # X-Schema-Version on /admin/graphql, the post-trial license notice header (REQ-1137) and the
    # 499 for a client gone before any response — plain ASGI, so the data path pays no
    # BaseHTTPMiddleware machinery (child task, anyio streams, its own receive()) per request.
    from provisa.api.middleware.response_headers import ResponseHeadersMiddleware

    app.add_middleware(ResponseHeadersMiddleware, state=state)

    from provisa.api.admin.discovery import router as discovery_router

    app.include_router(discovery_router)
    from provisa.api.admin.discovery_schema import router as schema_discovery_router

    app.include_router(schema_discovery_router)
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
    from provisa.api.admin.settings_catalog_router import (  # REQ-1913
        router as settings_catalog_router,
    )

    app.include_router(settings_catalog_router)
    from provisa.api.admin.org_storage_router import (  # REQ-1046, REQ-1048, REQ-1049
        router as org_storage_router,
    )

    app.include_router(org_storage_router)
    from provisa.api.admin.org_encryption_router import (  # REQ-1574
        router as org_encryption_router,
    )

    app.include_router(org_encryption_router)
    from provisa.api.admin.import_router import router as hasura_import_router  # REQ-1483

    app.include_router(hasura_import_router)
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
    from provisa.api.admin.glossary_router import router as glossary_router  # REQ-1387

    app.include_router(glossary_router)
    from provisa.api.admin.report_router import router as model_report_router  # REQ-1592

    app.include_router(model_report_router)
    from provisa.api.admin.source_meta_router import router as source_meta_router

    app.include_router(source_meta_router)
    from provisa.api.admin.table_profile_router import router as table_profile_router

    app.include_router(table_profile_router)
    from provisa.api.admin.profiler_router import router as profiler_router  # REQ-1934

    app.include_router(profiler_router)
    from provisa.api.admin.profiler_checks_router import (  # REQ-1934
        router as profiler_checks_router,
    )

    app.include_router(profiler_checks_router)
    from provisa.api.admin.synthetic_router import router as synthetic_router  # REQ-1939

    app.include_router(synthetic_router)
    from provisa.api.admin.fakes_router import router as fakes_router  # REQ-1494

    app.include_router(fakes_router)
    from provisa.api.admin.lifecycle_router import router as lifecycle_router

    app.include_router(lifecycle_router)
    from provisa.api.admin.table_search_router import router as table_search_router

    app.include_router(table_search_router)
    from provisa.api.admin.local_users_router import router as local_users_router

    app.include_router(local_users_router)
    from provisa.api.admin.orgs_router import router as orgs_router

    app.include_router(orgs_router)
    from provisa.api.admin.environments_router import router as environments_router  # REQ-1487

    app.include_router(environments_router)
    from provisa.api.admin.environment_data_router import router as environment_data_router

    app.include_router(environment_data_router)  # REQ-1942
    from provisa.api.admin.secrets_router import router as secrets_router  # REQ-1558

    app.include_router(secrets_router)
    from provisa.api.admin.source_sign_in_router import router as source_sign_in_router

    app.include_router(source_sign_in_router)  # REQ-1923
    from provisa.api.admin.mail_platforms_router import router as mail_platforms_router

    app.include_router(mail_platforms_router)  # REQ-1923
    from provisa.api.branding_router import router as branding_router  # REQ-1486

    app.include_router(branding_router)
    from provisa.api.admin.maintenance_router import router as maintenance_router  # REQ-1466

    app.include_router(maintenance_router)
    from provisa.api.admin.debug_trace_router import router as debug_trace_router  # REQ-1910

    app.include_router(debug_trace_router)
    from provisa.api.admin.audit_query_text_router import router as audit_text_router  # REQ-1910

    app.include_router(audit_text_router)
    from provisa.api.admin.invites_router import router as invites_router

    app.include_router(invites_router)
    from provisa.api.admin.mail_router import router as mail_router  # REQ-1576

    app.include_router(mail_router)
    from provisa.api.admin.roles_router import router as roles_router

    app.include_router(roles_router)
    from provisa.api.admin.creation_requests_router import router as creation_requests_router

    app.include_router(creation_requests_router)
    from provisa.api.auth_router import router as auth_router

    app.include_router(auth_router)
    from provisa.api.admin.email_router import router as email_router

    app.include_router(email_router)
    from provisa.api.pat_router import router as pat_router  # REQ-1263

    app.include_router(pat_router)
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

    # The billing API belongs to the commercial plugin (provisa.core.commerce). An open-source or
    # demo deployment has no plugin and therefore no /billing routes — there is no subscription to
    # manage and no merchant of record to talk to.
    from provisa.core.commerce import include_routes

    include_routes(app)

    # REQ-1355: included unconditionally. The former `if state.multitenancy:` guard read the flag
    # before _load_and_build assigns it, so it was always False and the router never mounted —
    # every control-plane endpoint 404'd on the deployments that need it. Multitenancy is enforced
    # per request by the router's own _require_multitenancy(), which 403s when it is off.
    from provisa.control_plane.router import router as control_plane_router

    app.include_router(control_plane_router)

    @app.api_route("/health", methods=["GET", "HEAD"])
    async def health():  # noqa: F841  # pyright: ignore[reportUnusedFunction]
        # The worker's health report: dependencies, the launch's worker roll call (REQ-1900) and
        # the config stamps loaded beside the ones stored (REQ-1914). One function, so this route
        # and Arrow Flight's `healthcheck` action answer the same thing.
        from provisa.api.health_report import health_report

        return await health_report(state)

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

    @app.api_route("/debug/logs", methods=["GET"])
    async def debug_logs(pattern: str | None = None, limit: int = 100):  # noqa: F841  # pyright: ignore[reportUnusedFunction]
        """Return recent logs, optionally filtered by pattern. For debugging only."""
        # Get the root logger and look for our custom buffer handler
        root_logger = logging.getLogger()
        buffer_handler = next(
            (h for h in root_logger.handlers if isinstance(h, _DebugLogBufferHandler)), None
        )

        if buffer_handler is None:
            return {"error": "log buffer not initialized", "logs": []}

        logs = list(buffer_handler.buffer)
        if pattern:
            import re

            logs = [log for log in logs if re.search(pattern, log)]

        return {"count": len(logs), "logs": logs[-limit:]}

    # REQ-1882 (amended 2026-09-29): every HTTP and WebSocket request — data, admin, UI APIs,
    # discovery, static — runs entirely on its own request thread and loop. Registered LAST so it
    # is the outermost user middleware: auth, org routing, rate limiting, egress metering,
    # governance, execution and response streaming all run on the request thread; the front
    # (uvicorn) loop only accepts, parses and relays receive/send.
    # REQ-1905: name the transport an HTTP request is on (its data route: graphql, sql_http,
    # cypher_http, rest, jsonapi) and bind that transport's request deadline around the WHOLE
    # request — routing, auth, governance, execution, shaping, encoding and the send. This is the
    # transport's boundary: a response that reaches it after the deadline is answered with the
    # timeout (provisa.api.request_timeout). A route that is not a transport of its own binds
    # none here; its statements are timed where they run (pgwire._pipeline._execute_plan).
    # Registered just before RequestThreadMiddleware, so it runs on the request's own thread and
    # context.
    from provisa.api.request_timeout import serve_within_deadline
    from provisa.core import request_deadline as _request_deadline
    from provisa.core.limits import bound_request_transport, http_transport_for_path

    class _RequestTransportMiddleware:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
            if scope["type"] != "http":
                await self._inner(scope, receive, send)
                return
            transport = http_transport_for_path(scope.get("path", ""))
            if transport is None:
                with bound_request_transport(None):
                    await self._inner(scope, receive, send)
                return
            with (
                bound_request_transport(transport),
                _request_deadline.request(transport) as deadline,
            ):
                await serve_within_deadline(self._inner, scope, receive, send, deadline)

    # REQ-1524: one HTTP request is one model change. Registered before the transport middleware,
    # so it runs inside it, on the request's own thread: what the request writes to a model is
    # committed once, before its response starts, so the caller reads its answer after the commit.
    from provisa.api.model_change_middleware import ModelChangeMiddleware

    app.add_middleware(ModelChangeMiddleware)
    app.add_middleware(_RequestTransportMiddleware)
    # REQ-1916: a coordinator answers every /data request with its refusal, before the request
    # transport opens a deadline for it.
    from provisa.api.coordinator_gate import CoordinatorDataGate

    app.add_middleware(CoordinatorDataGate)

    from provisa.core.request_thread import RequestThreadMiddleware

    app.add_middleware(RequestThreadMiddleware)

    return app
