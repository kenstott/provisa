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
import json
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
from provisa.compiler.introspect import ColumnMetadata, introspect_tables
from provisa.compiler.naming import source_to_catalog
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.rls import RLSContext, build_rls_context
from provisa.compiler.sql_gen import CompilationContext, build_context
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.db import init_schema
from provisa.core.database import Database, create_engine_from_url
from provisa.core.control_plane import bring_up_platform
from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core.secrets import resolve_secrets
from provisa.executor.pool import SourcePool
from provisa.compiler.mask_inject import MaskingRules
from provisa.cache.store import CacheStore, NoopCacheStore, RedisCacheStore
from provisa.api.admin.db_queries import (
    fetch_tables as _fetch_tables,
    fetch_relationships as _fetch_relationships,
    parse_mask_value as _parse_mask_value,
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
    # Bound ports for the separate socket listeners (None = protocol not enabled). Health
    # probes TCP-connect to these to tell "running" from "down" independently of the HTTP app.
    _grpc_port: int | None = None
    _flight_port: int | None = None
    _pgwire_port: int | None = None
    _bolt_port: int | None = None
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


def _setup_approval_hook(st: AppState) -> None:
    """REQ-247: build ABAC approval hook + scope dicts from config (no-op when unconfigured)."""
    from provisa.auth.approval_hook import create_hook, load_approval_hook_config

    config = getattr(st, "config", None)
    if config is None:
        return
    hook_cfg = load_approval_hook_config(getattr(config.auth, "approval_hook", None))
    if hook_cfg is None:
        return

    st.approval_hook_config = hook_cfg
    st.approval_hook = create_hook(hook_cfg)
    st.source_approval_hooks = {
        s.id: True for s in config.sources if getattr(s, "approval_hook", False)
    }

    # Resolve per-table flags to table_ids via the compilation contexts.
    name_to_id: dict[tuple[str, str, str], int] = {}
    for ctx in st.contexts.values():
        for meta in ctx.tables.values():
            name_to_id[(meta.domain_id, meta.schema_name, meta.table_name)] = meta.table_id
    table_hooks: dict[int, bool] = {}
    for t in config.tables:
        if not getattr(t, "approval_hook", False):
            continue
        key = (t.domain_id, t.schema_name, t.table_name)
        if key in name_to_id:
            table_hooks[name_to_id[key]] = True
    st.table_approval_hooks = table_hooks


# Maps original table name → view name (or itself if no view needed).
_META_TABLE_ALIAS: dict[str, str] = {
    "registered_tables": "registered_tables_meta",
    "table_columns": "table_columns_meta",
    "roles": "roles_meta",
    "tracked_webhooks": "tracked_webhooks_meta",
    "tracked_functions": "tracked_functions_meta",
}
_META_TABLES = [
    "registered_tables",
    "table_columns",
    "domains",
    "relationships",
    "rls_rules",
    "roles",
    "roles_domain_access",
    "tracked_webhooks",
    "tracked_functions",
]

# Matches actual otlp2parquet output schema (https://github.com/smithclay/otlp2parquet).
# Timestamps are milliseconds. span_attributes is a JSON string.
# We add table_name/domain_id/role_id extracted from span_attributes during compaction.
# Ops telemetry schema — single source of truth, shared with otlp2sql so the
# receiver's tables and the ops-domain registration never drift.
from provisa.observability.ops_schema import OPS_TABLES as _OPS_TABLES  # noqa: E402
from provisa.compiler.type_map import OPS_PG_TO_PHYSICAL as _OPS_PG_TO_PHYSICAL  # noqa: E402

# Views registered in the ops domain alongside the raw Iceberg tables.
# Each entry: (view_name, [(col_name, data_type, is_pk)], ddl_sql)
_OPS_VIEWS: list[tuple[str, list[tuple[str, str, bool]], str]] = [
    (
        "queries",
        [
            ("trace_id", "text", True),
            ("span_id", "text", False),
            ("parent_span_id", "text", False),
            ("span_name", "text", False),
            ("service_name", "text", False),
            ("timestamp", "bigint", False),
            ("end_timestamp", "bigint", False),
            ("duration", "bigint", False),
            ("status_code", "integer", False),
            ("table_name", "text", False),
            ("domain_id", "text", False),
            ("role_id", "text", False),
            ("query_text", "text", False),
            ("_date", "date", False),
        ],
        """\
CREATE OR REPLACE VIEW otel.signals.queries AS
SELECT
    trace_id,
    span_id,
    parent_span_id,
    span_name,
    service_name,
    "timestamp",
    end_timestamp,
    duration,
    status_code,
    table_name,
    domain_id,
    role_id,
    query_text,
    _date
FROM otel.signals.traces
WHERE span_name LIKE 'provisa.query%'
""",
    ),
]


async def _seed_meta_domain(
    conn: asyncpg.Connection, org_id: str = "default"
) -> None:  # REQ-012, REQ-016, REQ-695
    """Register admin tables in the built-in meta domain (idempotent)."""
    schema_name = f"org_{org_id}"
    for ddl in _META_TABLE_VIEWS.values():
        await conn.execute(ddl)

    # Remove any stale view-named entries left by older code versions.
    for view_name in _META_TABLE_ALIAS.values():
        await conn.execute(
            "DELETE FROM registered_tables "
            "WHERE source_id = 'provisa-admin' AND schema_name = $1 AND table_name = $2",
            schema_name,
            view_name,
        )

    for tbl in _META_TABLES:
        table_id = await conn.fetchval(
            """
            INSERT INTO registered_tables
                (source_id, domain_id, schema_name, table_name)
            VALUES ('provisa-admin', 'meta', $1, $2)
            ON CONFLICT (source_id, schema_name, table_name)
                DO UPDATE SET domain_id = 'meta'
            RETURNING id
            """,
            schema_name,
            tbl,
        )
        pk_cols = {
            row["column_name"]
            for row in await conn.fetch(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = $1 AND tc.table_name = $2
                  AND tc.constraint_type = 'PRIMARY KEY'
                """,
                schema_name,
                tbl,
            )
        }
        # Use the view name when available so column list reflects the exposed schema.
        view_name = _META_TABLE_ALIAS.get(tbl, tbl)
        cols = await conn.fetch(
            """
            SELECT column_name,
                   CASE WHEN data_type IN ('ARRAY', 'jsonb', 'json') THEN 'text' ELSE data_type END AS data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            """,
            schema_name,
            view_name,
        )
        col_names = {col["column_name"] for col in cols}
        # Remove stale columns that no longer appear in the view.
        await conn.execute(
            "DELETE FROM table_columns WHERE table_id = $1 AND column_name != ALL($2::text[])",
            table_id,
            list(col_names),
        )
        for col in cols:
            await conn.execute(
                """
                INSERT INTO table_columns
                    (table_id, column_name, visible_to, data_type, is_primary_key)
                VALUES ($1, $2, '{}', $3, $4)
                ON CONFLICT (table_id, column_name) DO NOTHING
                """,
                table_id,
                col["column_name"],
                col["data_type"],
                col["column_name"] in pk_cols,
            )

    # Register registered_tables → table_columns relationship (table_id FK)
    await conn.execute(
        """
        INSERT INTO relationships (id, source_table_id, target_table_id,
                                   source_column, target_column, cardinality,
                                   materialize, refresh_interval,
                                   target_function_name, function_arg,
                                   alias, graphql_alias, disable_cypher, source_json_key)
        SELECT 'meta:registered_tables:table_columns', rt.id, tc.id,
               'id', 'table_id', 'one-to-many',
               false, 300, null, null, null, null, false, null
        FROM registered_tables rt, registered_tables tc
        WHERE rt.source_id = 'provisa-admin' AND rt.table_name = 'registered_tables'
          AND tc.source_id = 'provisa-admin' AND tc.table_name = 'table_columns'
        ON CONFLICT (id) DO NOTHING
        """
    )


async def _seed_meta_relationships(conn: asyncpg.Connection) -> None:
    """Seed FK relationships between meta and ops tables (idempotent, runs after both seeds)."""
    _REL_INSERT = """
        INSERT INTO relationships (id, source_table_id, target_table_id,
                                   source_column, target_column, cardinality,
                                   materialize, refresh_interval,
                                   target_function_name, function_arg,
                                   alias, graphql_alias, disable_cypher, source_json_key)
        SELECT $1, src.id, tgt.id,
               $4, $5, $6,
               false, 300, null, null, $9, $10, false, null
        FROM (SELECT id FROM registered_tables WHERE source_id = $2 AND table_name = $3 LIMIT 1) src
        CROSS JOIN (SELECT id FROM registered_tables WHERE source_id = $7 AND table_name = $8 LIMIT 1) tgt
        ON CONFLICT (id) DO UPDATE
            SET source_table_id = EXCLUDED.source_table_id,
                target_table_id = EXCLUDED.target_table_id,
                source_column   = EXCLUDED.source_column,
                target_column   = EXCLUDED.target_column,
                cardinality     = EXCLUDED.cardinality,
                alias           = EXCLUDED.alias,
                graphql_alias   = EXCLUDED.graphql_alias
    """
    from provisa.api._meta_seed import META_RELATIONSHIPS

    static = META_RELATIONSHIPS

    for row in static:
        await conn.execute(_REL_INSERT, *row)


async def _compute_and_store_clusters(conn: asyncpg.Connection) -> int:  # REQ-510
    """Run Louvain on the schema graph and write l1/l2/l3_cluster onto registered_tables."""
    from provisa.schema_clusters import compute_clusters

    rows = await conn.fetch("SELECT id FROM registered_tables")
    table_ids = [r["id"] for r in rows]

    rel_rows = await conn.fetch(
        "SELECT source_table_id, target_table_id FROM relationships "
        "WHERE source_table_id IS NOT NULL AND target_table_id IS NOT NULL"
    )
    edges = [(r["source_table_id"], r["target_table_id"]) for r in rel_rows]

    if not table_ids:
        return 0

    clusters = compute_clusters(table_ids, edges)

    await conn.executemany(
        """
        UPDATE registered_tables
        SET l1_cluster = $2, l2_cluster = $3, l3_cluster = $4,
            clusters_computed_at = NOW()
        WHERE id = $1
        """,
        [(tid, l1, l2, l3) for tid, (l1, l2, l3) in clusters.items()],
    )
    return len(clusters)


async def _seed_ops_pg(conn: asyncpg.Connection) -> None:  # REQ-016
    """Register ops tables/views in PG registered_tables + table_columns (idempotent)."""
    for tbl_name, cols in _OPS_TABLES.items():
        table_id = await conn.fetchval(
            """
            INSERT INTO registered_tables
                (source_id, domain_id, schema_name, table_name)
            VALUES ('provisa-otel', 'ops', 'signals', $1)
            ON CONFLICT (source_id, schema_name, table_name)
                DO UPDATE SET domain_id = 'ops'
            RETURNING id
            """,
            tbl_name,
        )
        for col_name, pg_type, is_pk in cols:
            await conn.execute(
                """
                INSERT INTO table_columns
                    (table_id, column_name, visible_to, data_type, is_primary_key)
                VALUES ($1, $2, '{}', $3, $4)
                ON CONFLICT (table_id, column_name) DO NOTHING
                """,
                table_id,
                col_name,
                pg_type,
                is_pk,
            )
    for view_name, cols, _ in _OPS_VIEWS:
        table_id = await conn.fetchval(
            """
            INSERT INTO registered_tables
                (source_id, domain_id, schema_name, table_name)
            VALUES ('provisa-otel', 'ops', 'signals', $1)
            ON CONFLICT (source_id, schema_name, table_name)
                DO UPDATE SET domain_id = 'ops'
            RETURNING id
            """,
            view_name,
        )
        for col_name, pg_type, is_pk in cols:
            await conn.execute(
                """
                INSERT INTO table_columns
                    (table_id, column_name, visible_to, data_type, is_primary_key)
                VALUES ($1, $2, '{}', $3, $4)
                ON CONFLICT (table_id, column_name) DO NOTHING
                """,
                table_id,
                col_name,
                pg_type,
                is_pk,
            )


async def _init_meta_rls(conn: asyncpg.Connection) -> None:  # REQ-041, REQ-402
    """Enable Postgres RLS on all _META_TABLES. Called only when multitenancy=True."""
    for tbl in _META_TABLES:
        await conn.execute(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY")
        await conn.execute(f"ALTER TABLE {tbl} FORCE ROW LEVEL SECURITY")
        await conn.execute(
            f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_policies
                    WHERE tablename = '{tbl}' AND policyname = 'tenant_isolation_{tbl}'
                ) THEN
                    CREATE POLICY tenant_isolation_{tbl}
                        ON {tbl}
                        USING (tenant_id IS NULL OR tenant_id = current_setting('app.tenant_id', true)::uuid);
                END IF;
            END $$
            """
        )


async def _init_control_planes(
    config_path: str | None,
) -> tuple[str, int, str, str]:  # REQ-057, REQ-837
    """Bring up both control planes from config and init tenant schema + audit.

    Returns the tenant DB connection parts (host, port, database, user) for the
    the engine self-catalog. All connection details come from the config layer
    (``control_plane``), which is the only place the environment is read — both
    planes are driven purely by SQLAlchemy, each by its own URI."""
    from provisa.core.config_loader import load_control_plane

    cp = load_control_plane(config_path)
    org_id = cp.resolved_org_id()
    state.org_id = org_id

    # Tenant plane: schema-scoped to ``org_<id>`` via search_path (the tenant-scope
    # mechanism). Platform plane (bring_up_platform): global registry + billing,
    # never org-scoped. Two independent engines, each its own SQLAlchemy URI.
    tenant_engine = create_engine_from_url(
        cp.resolved_tenant_url(), pool_size=cp.pool_max, max_overflow=cp.max_overflow
    )
    state.tenant_db = Database(tenant_engine, name="org", search_path=f"org_{org_id}")
    state.admin_db = await bring_up_platform(
        cp.resolved_platform_url(), pool_size=cp.pool_max, pool_min=cp.pool_min
    )

    schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
    if schema_sql_path.exists():
        await init_schema(state.tenant_db, schema_sql_path.read_text(), org_id=org_id)

    from provisa.audit.query_log import init_audit_schema

    await init_audit_schema(state.tenant_db, org_id=org_id)

    host, port, database, username, _pw = cp.tenant_parts()
    assert host and database and username, (
        "control_plane.tenant_url must specify host, database, and user"
    )
    return host, port, database, username


async def _seed_built_in_sources(  # REQ-012, REQ-016, REQ-510
    pg_host: str, pg_port: int, pg_database: str, pg_user: str
) -> None:
    """Seed provisa-admin, provisa-otel, and __provisa__ source rows; seed meta domain and ops; compute clusters."""
    assert state.tenant_db is not None
    from provisa.federation.engine import configured_engine_endpoint

    engine_host_early, engine_port_early = configured_engine_endpoint()
    async with state.tenant_db.acquire() as _conn:
        await _conn.execute(
            """
            INSERT INTO sources (id, type, host, port, database, username, dialect, description)
            VALUES ('provisa-admin', 'postgresql', $1, $2, $3, $4, 'postgresql', 'Provisa internal administration database — stores source registrations, table metadata, relationships, roles, and governance configuration')
            ON CONFLICT (id) DO UPDATE SET description = COALESCE(NULLIF(sources.description, ''), EXCLUDED.description)
            """,
            pg_host,
            pg_port,
            pg_database,
            pg_user,
        )
        _engine_name = state.federation_engine.name
        await _conn.execute(
            """
            INSERT INTO sources (id, type, host, port, database, username, dialect, description)
            VALUES ('provisa-otel', 'iceberg', $1, $2, 'otel', 'provisa', $3, 'Observability telemetry store — OpenTelemetry spans and traces collected from Provisa query execution, used for performance monitoring and query analytics')
            ON CONFLICT (id) DO UPDATE SET description = COALESCE(NULLIF(sources.description, ''), EXCLUDED.description)
            """,
            engine_host_early,
            engine_port_early,
            _engine_name,
        )
        await _conn.execute(
            """
            INSERT INTO sources (id, type, description)
            VALUES ('__provisa__', $1, 'Provisa-managed virtual views — cross-source SQL views defined and published by the data team as governed data products')
            ON CONFLICT (id) DO NOTHING
            """,
            _engine_name,
        )
        _pg_conn = cast(asyncpg.Connection, _conn)
        await _seed_meta_domain(_pg_conn, org_id=state.org_id)
        await _seed_ops_pg(_pg_conn)
        await _seed_meta_relationships(_pg_conn)
        needs_clusters = await _conn.fetchval(
            "SELECT COUNT(*) FROM registered_tables WHERE l1_cluster IS NULL"
        )
        if needs_clusters:
            await _compute_and_store_clusters(_pg_conn)


def _apply_server_and_engine_config(raw_config: dict) -> None:
    """Populate state.server_cfg, state.hostname, state.server_limits, state.engine_conn, and FTE hints."""
    state.server_cfg = raw_config.get("server", {}) if isinstance(raw_config, dict) else {}
    state.hostname = str(
        os.environ.get("PROVISA_HOSTNAME") or state.server_cfg.get("hostname", "localhost")
    )

    _limits_cfg = state.server_cfg.get("limits", {})
    state.server_limits = {
        "default_row_limit": int(
            os.environ.get(
                "PROVISA_DEFAULT_ROW_LIMIT", str(_limits_cfg.get("default_row_limit", 100))
            )
        ),
        "engine_query_timeout": int(
            os.environ.get(
                "PROVISA_ENGINE_QUERY_TIMEOUT", str(_limits_cfg.get("engine_query_timeout", 120))
            )
        ),
        "request_timeout": float(
            os.environ.get("PROVISA_REQUEST_TIMEOUT", str(_limits_cfg.get("request_timeout", 60)))
        ),
        "retry_budget_secs": float(
            os.environ.get(
                "PROVISA_RETRY_BUDGET_SECS", str(_limits_cfg.get("retry_budget_secs", 30))
            )
        ),
    }

    # The engine terminal is only provisioned when it has one (the engine connects a cluster and seeds
    # its otel catalog). A native engine (duckdb/embedded-pg/…) has nothing to connect here —
    # telemetry lands in the dedicated ops store (ops_schema/otlp2sql), so provision() is a no-op.
    state.federation_engine.provision(
        _OPS_VIEWS, getattr(state, "otel_snapshot_retention_hours", None)
    )

    # Engine session tuning (e.g. Fault-Tolerant Execution) — engine-specific, applied through the
    # lifecycle seam. Native engines have no per-session cluster tuning (no-op).
    state.federation_engine.configure_session(state.server_cfg)


def _process_kafka_sources(raw_config: dict) -> None:  # REQ-147, REQ-250
    """Register Kafka topics as virtual tables and populate state.kafka_table_configs/windows."""
    from provisa.kafka.window import KafkaTableConfig

    for ks in raw_config.get("kafka_sources", []):
        source_id = ks["id"]
        # Ensure the kafka source exists in raw_config["sources"] so the FK is satisfied
        # when registered_tables references it.
        existing_ids = {s["id"] for s in raw_config.get("sources", [])}
        if source_id not in existing_ids:
            raw_config.setdefault("sources", []).append(
                {
                    "id": source_id,
                    "type": "kafka",
                    "host": ks.get("bootstrap_servers", ""),
                }
            )
        # REQ-250/147: register the Kafka source as an engine catalog (the engine writes catalog files
        # + CREATE CATALOG so it loads regardless of start order; native engines no-op).
        state.federation_engine.register_kafka_catalog(ks)
        for topic in ks.get("topics", []):
            topic_id = topic.get("id", "")
            physical_table = topic.get("topic", "").replace(".", "_").replace("-", "_")
            gql_table_name = topic.get("table_name") or topic_id.replace("-", "_")

            window = topic.get("default_window", "1h")
            disc = topic.get("discriminator")
            disc_field = disc.get("field") if disc else None
            disc_value = disc.get("value") if disc else None

            state.kafka_table_configs[gql_table_name] = KafkaTableConfig(
                window=window,
                discriminator_field=disc_field,
                discriminator_value=disc_value,
            )

            if window:
                state.kafka_windows[source_id] = window

            topic_columns = topic.get("columns", [])
            table_entry = {
                "source_id": source_id,
                "domain_id": topic.get("domain_id", "support"),
                "schema": "default",
                "table": gql_table_name,
                "description": topic.get("description", ""),
                "columns": [
                    {
                        "name": col.get("name", col) if isinstance(col, dict) else col,
                        "visible_to": col.get("visible_to", ["admin", "analyst"])
                        if isinstance(col, dict)
                        else ["admin", "analyst"],
                        "writable_by": col.get("writable_by", []) if isinstance(col, dict) else [],
                        "description": col.get("description", "") if isinstance(col, dict) else "",
                    }
                    for col in topic_columns
                ],
            }
            raw_config.setdefault("tables", []).append(table_entry)

            state.kafka_table_physical = getattr(state, "kafka_table_physical", {})
            state.kafka_table_physical[gql_table_name] = physical_table


async def _build_source_pools_and_enums(config: ProvisaConfig) -> None:  # REQ-012, REQ-221
    """Build direct source connection pools, register websocket/rss sources, and fetch enum types."""
    from provisa.executor.drivers.registry import has_driver
    from provisa.cache.warm_tables import DEFAULT_ICEBERG_CATALOG as _DEFAULT_ICE_CAT

    # Seed system source catalogs
    state.source_catalogs["provisa-admin"] = source_to_catalog("provisa-admin")
    state.source_catalogs["provisa-otel"] = "otel"

    for src in config.sources:
        state.source_types[src.id] = src.type.value
        _pg_cat = (
            source_to_catalog(src.id)
            if src.type.value == "postgresql"
            else (src.database or source_to_catalog(src.id))
        )
        state.source_catalogs[src.id] = _pg_cat
        state.source_dialects[src.id] = src.dialect or ""
        state.source_cache[src.id] = {
            "cache_enabled": src.cache_enabled,
            "cache_ttl": src.cache_ttl,
        }
        if src.federation_hints:
            state.source_federation_hints[src.id] = dict(src.federation_hints)
        if src.allowed_domains:
            state.source_allowed_domains[src.id] = list(src.allowed_domains)
        if has_driver(src.type.value):
            resolved_pw = resolve_secrets(src.password)
            resolved_host = resolve_secrets(src.host) if src.host else "localhost"
            state.source_dsns[src.id] = f"{resolved_host}:{src.port}/{src.database}"
            try:
                await state.source_pools.add(
                    source_id=src.id,
                    source_type=src.type.value,
                    host=resolved_host,
                    port=src.port,
                    database=src.database,
                    user=src.username,
                    password=resolved_pw,
                    min_size=src.pool_min,
                    max_size=src.pool_max,
                    use_pgbouncer=src.use_pgbouncer,
                    pgbouncer_port=src.pgbouncer_port,
                )
            except Exception as _pool_err:
                logging.getLogger(__name__).warning(
                    "Direct pool for %r (%s:%s) failed: %s — the engine-routed queries still work.",
                    src.id,
                    resolved_host,
                    src.port,
                    _pool_err,
                )

    _known_engine_catalogs = set(state.source_catalogs.values()) | {
        _DEFAULT_ICE_CAT,
        "otel",
        "results",
    }
    for _dom in config.domains:
        _ddl_cat = _dom.ddl_catalog or _DEFAULT_ICE_CAT
        if _dom.ddl_catalog and _ddl_cat not in _known_engine_catalogs:
            raise ValueError(
                f"Domain {_dom.id!r} ddl_catalog={_dom.ddl_catalog!r} is not a registered source catalog"
            )
        _ddl_schema = _dom.ddl_schema or _dom.id
        state.domain_write_targets[_dom.id] = (_ddl_cat, _ddl_schema)

    # WebSocket + RSS sources — register for SSE subscription dispatch
    for _src in config.sources:
        if _src.type.value == "websocket":
            state.websocket_sources[_src.id] = _src
        elif _src.type.value == "rss":
            state.rss_sources[_src.id] = _src
        # REQ-824: register source-level CDC transport once per source
        if _src.cdc is not None:
            state.cdc_sources[_src.id] = _src

    # REQ-221: Fetch enum types from all PostgreSQL sources
    from provisa.compiler.enum_detect import build_enum_types

    _enum_registry: dict[str, list[str]] = {}
    for _src in config.sources:
        if _src.type.value == "postgresql" and state.source_pools.has(_src.id):
            _driver = state.source_pools.get(_src.id)
            if hasattr(_driver, "fetch_enums"):
                # Swallowing here silently mistypes enum columns — propagate.
                _reg = await cast(Any, _driver).fetch_enums()
                _enum_registry.update(_reg)
    state.pg_enum_types = build_enum_types(_enum_registry)


async def _resolve_pk_from_sources() -> None:
    """Second pass — resolve PRIMARY KEYs from each native RDBMS source's information_schema."""
    assert state.tenant_db is not None
    _startup_log = logging.getLogger("uvicorn.error")
    _PK_RDBMS_TYPES = ("postgresql", "mysql", "mariadb", "singlestore", "sqlserver", "redshift")
    _PK_SOURCE_TYPES = _PK_RDBMS_TYPES + ("sqlite",)
    async with state.tenant_db.acquire() as _pk_conn:
        _pk_rows = await _pk_conn.fetch(
            "SELECT rt.id, rt.source_id, rt.schema_name, rt.table_name, s.type AS source_type "
            "FROM registered_tables rt JOIN sources s ON s.id = rt.source_id "
            "WHERE s.type = ANY($1::text[])",
            list(_PK_SOURCE_TYPES),
        )
        for _pk_t in _pk_rows:
            _sch = _pk_t["schema_name"].replace("'", "''")
            _tbl = _pk_t["table_name"].replace("'", "''")
            _pk_sql = (
                "SELECT kcu.column_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.table_schema = kcu.table_schema "
                "WHERE tc.constraint_type = 'PRIMARY KEY' "
                f"  AND tc.table_schema = '{_sch}' AND tc.table_name = '{_tbl}'"
            )
            try:
                if _pk_t["source_type"] == "sqlite":
                    _pk_cols = [_r[0] for _r in await _pk_conn.fetch(_pk_sql)]
                elif state.source_pools.has(_pk_t["source_id"]):
                    _pk_res = await state.source_pools.execute(_pk_t["source_id"], _pk_sql, None)
                    _pk_cols = [_row[0] for _row in _pk_res.rows]
                else:
                    continue
            except Exception:
                _startup_log.warning(
                    "PK resolve failed for %s.%s.%s",
                    _pk_t["source_id"],
                    _pk_t["schema_name"],
                    _pk_t["table_name"],
                    exc_info=True,
                )
                continue
            if _pk_cols:
                await _pk_conn.execute(
                    "UPDATE table_columns SET is_primary_key = true "
                    "WHERE table_id = $1 AND column_name = ANY($2::text[])",
                    _pk_t["id"],
                    _pk_cols,
                )


async def _load_openapi_specs() -> None:
    """Reload OpenAPI specs from DB into state (survives hot reloads and restarts)."""
    assert state.tenant_db is not None
    async with state.tenant_db.acquire() as conn:
        openapi_rows = await conn.fetch(
            "SELECT id, path FROM sources WHERE type = 'openapi' AND path IS NOT NULL AND path != ''"
        )
    from provisa.openapi.loader import load_spec
    from provisa.core.secrets import resolve_secrets as _resolve_secrets

    state.openapi_specs = {}
    for _row in openapi_rows:
        try:
            _resolved_path = _resolve_secrets(_row["path"])
            _spec = load_spec(_resolved_path)
            _servers = _spec.get("servers", [])
            _base_url = _servers[0].get("url", "") if _servers else ""
            if (
                _base_url
                and not _base_url.startswith(("http://", "https://"))
                and _resolved_path.startswith(("http://", "https://"))
            ):
                from urllib.parse import urljoin

                _base_url = urljoin(_resolved_path, _base_url)
            state.openapi_specs[_row["id"]] = {
                "spec_path": _row["path"],
                "spec": _spec,
                "base_url": _base_url,
                "domain_id": "",
                "auth_config": None,
                "cache_ttl": 300,
            }
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "Failed to reload OpenAPI spec for %s: %s", _row["id"], exc
            )


def _load_mv_and_views_config(
    raw_config: dict,
) -> None:  # REQ-086, REQ-133, REQ-135, REQ-158, REQ-159, REQ-160
    """Load materialized_views, views, and auto-MV cross-source relationships into state."""
    from provisa.mv.models import MVDefinition, JoinPattern, SDLConfig

    mv_configs = raw_config.get("materialized_views", [])
    for mvc in mv_configs:
        jp = None
        if "join_pattern" in mvc:
            jp_cfg = mvc["join_pattern"]
            jp = JoinPattern(
                left_table=jp_cfg["left_table"],
                left_column=jp_cfg["left_column"],
                right_table=jp_cfg["right_table"],
                right_column=jp_cfg["right_column"],
                join_type=jp_cfg.get("join_type", "left"),
            )
        sdl_cfg = None
        if "sdl_config" in mvc:
            sc = mvc["sdl_config"]
            sdl_cfg = SDLConfig(
                domain_id=sc["domain_id"],
                columns=sc.get("columns"),
            )
        mv = MVDefinition(
            id=mvc["id"],
            source_tables=mvc.get("source_tables", []),
            target_catalog=mvc.get("target_catalog", "postgresql"),
            target_schema=mvc.get("target_schema", f"org_{state.org_id}_mv_cache"),
            target_table=mvc.get("target_table"),
            refresh_interval=mvc.get("refresh_interval", 300),
            enabled=mvc.get("enabled", True),
            join_pattern=jp,
            sql=mvc.get("sql"),
            expose_in_sdl=mvc.get("expose_in_sdl", False),
            sdl_config=sdl_cfg,
        )
        state.mv_registry.register(mv)

        # REQ-086: Expose MV as queryable table in schema
        if mv.expose_in_sdl and sdl_cfg:
            mv_table = {
                "source_id": mvc.get("target_catalog", "postgresql"),
                "domain_id": sdl_cfg.domain_id,
                "schema": mvc.get("target_schema", f"org_{state.org_id}_mv_cache"),
                "table": mv.target_table,
                "columns": sdl_cfg.columns or [],
            }
            raw_config.setdefault("tables", []).append(mv_table)

    # Process views — governed computed datasets
    views_config = raw_config.get("views", [])
    if views_config:
        _view_log = logging.getLogger(__name__)
        for view_cfg in views_config:
            view_id = view_cfg["id"]
            view_sql = view_cfg["sql"]
            materialize = view_cfg.get("materialize", False)
            domain_id = view_cfg.get("domain_id", "default")
            description = view_cfg.get("description")
            refresh_interval = view_cfg.get("refresh_interval", 300)

            view_source_id = view_cfg.get("source_id", "postgresql")
            view_table_name = f"view_{view_id.replace('-', '_')}"
            view_schema = f"org_{state.org_id}_mv_cache" if materialize else f"org_{state.org_id}"

            view_table = {
                "source_id": view_source_id,
                "domain_id": domain_id,
                "schema": view_schema,
                "table": view_table_name,
                "description": description,
                "alias": view_cfg.get("alias"),
                "columns": view_cfg.get("columns", []),
            }
            raw_config.setdefault("tables", []).append(view_table)

            if materialize:
                mv = MVDefinition(
                    id=f"view-{view_id}",
                    source_tables=[],
                    target_catalog="postgresql",
                    target_schema=f"org_{state.org_id}_mv_cache",
                    target_table=view_table_name,
                    refresh_interval=refresh_interval,
                    enabled=True,
                    sql=view_sql,
                    expose_in_sdl=False,
                )
                state.mv_registry.register(mv)
                _view_log.info("Registered materialized view: %s", view_id)
            else:
                state.view_sql_map[view_table_name] = view_sql.strip()
                _view_log.info("Registered inline view: %s", view_id)

    # Auto-generate MVs from cross-source relationships with materialize=true
    _table_source_map: dict[str, str] = {}
    for tbl_cfg in raw_config.get("tables", []):
        tbl_name = tbl_cfg.get("table") or tbl_cfg.get("table_name")
        if tbl_name and "source_id" in tbl_cfg:
            _table_source_map[tbl_name] = tbl_cfg["source_id"]

    for rel_cfg in raw_config.get("relationships", []):
        if not rel_cfg.get("materialize", False):
            continue

        src_table = rel_cfg["source_table_id"]
        tgt_table = rel_cfg["target_table_id"]
        src_source = _table_source_map.get(src_table)
        tgt_source = _table_source_map.get(tgt_table)

        if src_source and tgt_source and src_source != tgt_source:
            mv_id = f"auto-mv-{rel_cfg['id']}"
            if state.mv_registry.get(mv_id) is not None:
                continue

            jp = JoinPattern(
                left_table=src_table,
                left_column=rel_cfg["source_column"],
                right_table=tgt_table,
                right_column=rel_cfg["target_column"],
                join_type="left",
            )
            mv = MVDefinition(
                id=mv_id,
                source_tables=[src_table, tgt_table],
                target_catalog="postgresql",
                target_schema=f"org_{state.org_id}_mv_cache",
                refresh_interval=rel_cfg.get("refresh_interval", 300),
                enabled=True,
                join_pattern=jp,
            )
            state.mv_registry.register(mv)
            logging.getLogger(__name__).info(
                "Auto-materialized cross-source relationship %s (%s.%s → %s.%s)",
                rel_cfg["id"],
                src_source,
                src_table,
                tgt_source,
                tgt_table,
            )


async def _init_ingest_engines() -> None:
    """Phase AS: Initialize ingest engines and DDL for ingest sources."""
    assert state.tenant_db is not None
    try:
        from provisa.ingest.engine import get_engine as _get_ingest_engine
        from provisa.ingest.ddl import generate_create_table as _gen_ddl
        from provisa.core.secrets import resolve_secrets as _resolve_secrets

        _ingest_log = logging.getLogger(__name__)
        async with state.tenant_db.acquire() as _pg_conn:
            _ingest_sources = await _pg_conn.fetch(
                "SELECT id, host, port, database, username, dialect FROM sources WHERE type = 'ingest'"
            )
        for _isrc in _ingest_sources:
            _sid = _isrc["id"]
            _pw = _resolve_secrets("")
            _eng = _get_ingest_engine(
                source_id=_sid,
                dialect=_isrc["dialect"] or "postgresql+asyncpg",
                host=_isrc["host"] or "localhost",
                port=_isrc["port"] or 5432,
                database=_isrc["database"] or "",
                username=_isrc["username"] or "",
                password=_pw or "",
            )
            state.ingest_engines[_sid] = _eng
            async with state.tenant_db.acquire() as _pg_conn:
                _itables = await _pg_conn.fetch(
                    """
                    SELECT rt.table_name, tc.column_name, tc.path, tc.data_type
                    FROM registered_tables rt
                    JOIN table_columns tc ON tc.table_id = rt.id
                    WHERE rt.source_id = $1
                    ORDER BY rt.table_name, tc.id
                    """,
                    _sid,
                )
            _tbl_map: dict[str, list[dict]] = {}
            for _row in _itables:
                _tn = _row["table_name"]
                _tbl_map.setdefault(_tn, []).append(
                    {
                        "column_name": _row["column_name"],
                        "path": _row["path"],
                        "data_type": _row["data_type"],
                    }
                )
            state.ingest_tables[_sid] = _tbl_map
            for _tn, _cols in _tbl_map.items():
                _ddl = _gen_ddl(_tn, _cols)
                try:
                    async with _eng.begin() as _conn:
                        await _conn.execute(__import__("sqlalchemy").text(_ddl))
                except Exception as _exc:
                    _ingest_log.warning("Ingest DDL failed for %s.%s: %s", _sid, _tn, _exc)
    except Exception:
        logging.getLogger(__name__).warning("Ingest source init failed", exc_info=True)


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
    if cache_config.get("enabled"):
        # REQ-829: RedisCacheStore(None) transparently uses embedded fakeredis, so
        # desktop exercises the same result-cache code path as production.
        state.response_cache_store = RedisCacheStore(state.redis_url)
        state.response_cache_default_ttl = cache_config.get("default_ttl", 300)

    tenant_db = state.tenant_db
    assert tenant_db is not None
    async with tenant_db.acquire() as conn:
        _replace_mode = os.environ.get("PROVISA_CONFIG_REPLACE", "").lower() in ("1", "true", "yes")
        _conn = cast(asyncpg.Connection, conn)
        await load_config(config, _conn, state.federation_engine, replace=_replace_mode)

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
                    await _rel_repo.upsert(cast(asyncpg.Connection, _retry_conn), _rel)
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


def _filter_tables_by_schema_cfg(
    tables: list[dict],
    schema_cfg: dict,
    source_allowed_domains: dict[str, list[str]],
) -> list[dict]:
    """Filter registered tables based on schema visibility config and source domain restrictions."""
    if not schema_cfg.get("include_ops", True):
        tables = [t for t in tables if t.get("domain_id") != "ops"]
    elif not schema_cfg.get("include_metrics", True):
        tables = [
            t
            for t in tables
            if not (t.get("domain_id") == "ops" and t.get("table_name") == "metrics")
        ]

    if source_allowed_domains:
        tables = [
            t
            for t in tables
            if not source_allowed_domains.get(t["source_id"])
            or t.get("domain_id", "") in source_allowed_domains[t["source_id"]]
        ]

    return tables


async def _load_graphql_remote_sources_from_db() -> None:
    """Load persisted graphql_remote sources from DB into state.graphql_remote_sources."""
    if state.tenant_db is None:
        log.warning("[GQL REMOTE] tenant_db is None — skipping DB load")
        return
    try:
        async with state.tenant_db.acquire() as _conn:
            src_rows = await _conn.fetch(
                "SELECT id, path FROM sources WHERE type = 'graphql_remote'"
            )
            for src in src_rows:
                source_id = src["id"]
                url = src["path"] or ""
                if source_id in getattr(state, "graphql_remote_sources", {}):
                    continue
                tbl_rows = await _conn.fetch(
                    "SELECT id, table_name, domain_id, description FROM registered_tables "
                    "WHERE source_id = $1 AND schema_name = 'graphql'",
                    source_id,
                )
                tables: list[dict] = []
                for tr in tbl_rows:
                    col_rows = await _conn.fetch(
                        "SELECT column_name, data_type, object_fields, native_filter_type "
                        "FROM table_columns WHERE table_id = $1",
                        tr["id"],
                    )
                    columns = []
                    required_args: list[dict] = []
                    for cr in col_rows:
                        if cr["native_filter_type"] == "query_param":
                            required_args.append(
                                {
                                    "name": cr["column_name"],
                                    "gql_type": "String",
                                    "provisa_type": "text",
                                }
                            )
                            continue
                        col_dict: dict = {
                            "name": cr["column_name"],
                            "type": cr["data_type"] or "text",
                        }
                        raw_of = cr["object_fields"]
                        if raw_of:
                            try:
                                col_dict["gql_object_fields"] = (
                                    json.loads(raw_of) if isinstance(raw_of, str) else raw_of
                                )
                            except Exception:
                                pass
                        columns.append(col_dict)
                    tname = tr["table_name"]
                    _snake_field = tname.split("__", 1)[-1]
                    _camel_parts = _snake_field.split("_")
                    _field_name = _camel_parts[0] + "".join(
                        p.capitalize() for p in _camel_parts[1:]
                    )
                    tables.append(
                        {
                            "name": tname,
                            "sql_name": tname,
                            "field_name": _field_name,
                            "source_id": source_id,
                            "columns": columns,
                            "domain_id": tr["domain_id"] or "",
                            "description": tr["description"],
                            "required_args": required_args,
                        }
                    )
                if not tables:
                    continue
                namespace = ""
                if not hasattr(state, "graphql_remote_sources"):
                    state.graphql_remote_sources = {}
                state.graphql_remote_sources[source_id] = {
                    "source_id": source_id,
                    "url": url,
                    "namespace": namespace,
                    "domain_id": tables[0]["domain_id"],
                    "auth": None,
                    "cache_ttl": 300,
                    "tables": tables,
                    "functions": [],
                    "relationships": [],
                }
                log.warning(
                    "[GQL REMOTE] Loaded source %s from DB (%d tables)", source_id, len(tables)
                )
    except Exception:
        log.warning("Failed to load graphql_remote sources from DB", exc_info=True)


def _assert_domain_table_unique(tables: list[dict]) -> None:
    """Raise if two tables share (domain_id, effective_name) — ambiguous for GraphQL/Cypher."""
    locs: dict[tuple[str, str], list[str]] = {}
    for t in tables:
        effective_name = t.get("alias") or t["table_name"]
        locs.setdefault((t["domain_id"], effective_name), []).append(
            f"{t['source_id']}.{t['schema_name']}"
        )
    dupes = {k: v for k, v in locs.items() if len(v) > 1}
    if dupes:
        detail = "; ".join(
            f"{dom}.{tbl} ← {sorted(srcs)}" for (dom, tbl), srcs in sorted(dupes.items())
        )
        raise RuntimeError(f"Duplicate domain+table registration (must be unique): {detail}")


def _resolve_naming_config(raw_config: dict | None) -> tuple[bool, dict | None]:
    """Load naming config from raw_config or disk. Returns (domain_prefix, resolved_raw_config)."""
    from provisa.compiler import naming as _naming

    domain_prefix = False
    if raw_config:
        domain_prefix = raw_config.get("naming", {}).get("domain_prefix", False)
        if raw_config.get("naming", {}).get("convention"):
            state.global_gql_naming_convention = raw_config["naming"]["convention"]
        if raw_config.get("naming", {}).get("sql_convention"):
            state.global_sql_naming_convention = raw_config["naming"]["sql_convention"]
    else:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                raw_config = yaml.safe_load(f)
            if isinstance(raw_config, dict):
                domain_prefix = raw_config.get("naming", {}).get("domain_prefix", False)
                if raw_config.get("naming", {}).get("convention"):
                    state.global_gql_naming_convention = raw_config["naming"]["convention"]
                if raw_config.get("naming", {}).get("sql_convention"):
                    state.global_sql_naming_convention = raw_config["naming"]["sql_convention"]
    # Single-domain mode: domain prefixing is meaningless with one domain — force it off.
    if raw_config and raw_config.get("naming", {}).get("use_domains") is False:
        domain_prefix = False
    _naming.configure(
        gql=state.global_gql_naming_convention,
        sql=state.global_sql_naming_convention,
    )
    return domain_prefix, raw_config


def _inject_gql_required_args(tables: list[dict], gql_remote_srcs: dict) -> None:
    """Inject required GQL args as native filter columns for graphql_remote tables."""
    if not gql_remote_srcs:
        return
    from provisa.compiler.naming import apply_sql_name as _asn

    _gql_req_args: dict[tuple, list[dict]] = {}
    for _reg in gql_remote_srcs.values():
        _sid = _reg.get("source_id", "")
        for _tbl in _reg.get("tables", []):
            _req = _tbl.get("required_args", [])
            if _req:
                _sql_tbl_name = _tbl.get("sql_name") or _asn(_tbl["name"])
                _gql_req_args[(_sid, _sql_tbl_name)] = _req
    if _gql_req_args:
        for _tbl in tables:
            _key = (_tbl["source_id"], _tbl["table_name"])
            _req = _gql_req_args.get(_key, [])
            for _arg in _req:
                _tbl.setdefault("columns", [])
                _tbl["columns"].append(
                    {
                        "name": _arg["name"],
                        "column_name": _arg["name"],
                        "visible_to": [],
                        "native_filter_type": "query_param",
                        "description": None,
                    }
                )


def _build_gql_object_columns(gql_remote_srcs: dict) -> dict[str, dict[str, list[str]]]:
    """Build gql_object_columns: {table_name: {col_name: [sub_field_names]}} for JSON extraction."""
    _gql_object_cols: dict[str, dict[str, list[str]]] = {}
    for _reg in gql_remote_srcs.values():
        for _tbl in _reg.get("tables", []):
            _tbl_obj: dict[str, list[str]] = {}
            for _col in _tbl.get("columns", []):
                _sub = _col.get("gql_object_fields")
                if _sub:
                    _tbl_obj[_col["name"]] = _sub
            if _tbl_obj:
                _gql_object_cols[_tbl["name"]] = _tbl_obj
    return _gql_object_cols


def _synthesize_column_metadata(
    tables: list[dict],
    col_types_converted: dict[int, list[ColumnMetadata]],
    gql_remote_srcs: dict,
) -> None:
    """Synthesize ColumnMetadata for ops, provisa-admin, graphql_remote, and govdata tables."""
    # Ops tables: static columns when the engine introspection returns empty
    _ops_static_cols: dict[str, list[ColumnMetadata]] = {
        tbl_name: [
            ColumnMetadata(
                column_name=col_name,
                data_type=_OPS_PG_TO_PHYSICAL.get(pg_type, "VARCHAR").lower(),
                is_nullable=not is_pk,
            )
            for col_name, pg_type, is_pk in cols
        ]
        for tbl_name, cols in _OPS_TABLES.items()
    }
    for view_name, cols, _ in _OPS_VIEWS:
        _ops_static_cols[view_name] = [
            ColumnMetadata(
                column_name=col_name,
                data_type=_OPS_PG_TO_PHYSICAL.get(pg_type, "VARCHAR").lower(),
                is_nullable=not is_pk,
            )
            for col_name, pg_type, is_pk in cols
        ]
    for _tbl in tables:
        if _tbl["source_id"] != "provisa-otel":
            continue
        _vname = _tbl["table_name"]
        if _vname not in _ops_static_cols:
            continue
        _tid = _tbl["id"]
        if not col_types_converted.get(_tid):
            col_types_converted[_tid] = _ops_static_cols[_vname]

    # provisa-admin meta tables (no provisa_admin the engine catalog)
    _pg_to_physical: dict[str, str] = {
        "text": "varchar",
        "character varying": "varchar",
        "varchar": "varchar",
        "integer": "integer",
        "bigint": "bigint",
        "smallint": "smallint",
        "boolean": "boolean",
        "double precision": "double",
        "float8": "double",
        "numeric": "double",
        "date": "date",
        "timestamp": "timestamp",
        "timestamp without time zone": "timestamp",
        "json": "json",
        "jsonb": "json",
    }
    for _tbl in tables:
        if _tbl["source_id"] != "provisa-admin":
            continue
        _tid = _tbl["id"]
        if col_types_converted.get(_tid):
            continue
        _cols = _tbl.get("columns", [])
        if not _cols:
            continue
        col_types_converted[_tid] = [
            ColumnMetadata(
                column_name=c["column_name"],
                data_type=_pg_to_physical.get(c.get("data_type") or "text", "varchar"),
                is_nullable=not c.get("is_primary_key", False),
            )
            for c in _cols
        ]

    # graphql_remote tables (no the engine catalog)
    if gql_remote_srcs:
        _provisa_to_physical = {
            "text": "varchar",
            "integer": "integer",
            "numeric": "double",
            "boolean": "boolean",
            "jsonb": "json",
        }
        _tbl_lookup = {(t["source_id"], t["table_name"]): t["id"] for t in tables}
        for _reg in gql_remote_srcs.values():
            _sid = _reg.get("source_id", "")
            for _tbl in _reg.get("tables", []):
                _tid = _tbl_lookup.get((_sid, _tbl["name"]))
                if _tid is not None and _tid not in col_types_converted:
                    col_types_converted[_tid] = [
                        ColumnMetadata(
                            column_name=c["name"],
                            data_type=_provisa_to_physical.get(c.get("type", "text"), "varchar"),
                            is_nullable=True,
                        )
                        for c in _tbl.get("columns", [])
                    ]

    # govdata tables from registered columns
    for _tbl in tables:
        if state.source_types.get(_tbl["source_id"]) != "govdata":
            continue
        _tid = _tbl["id"]
        if col_types_converted.get(_tid):
            continue
        _cols = _tbl.get("columns", [])
        if not _cols:
            log.warning(
                "govdata table %s.%s has no registered columns — skipping",
                _tbl.get("schema_name", ""),
                _tbl.get("table_name", ""),
            )
            continue
        col_types_converted[_tid] = [
            ColumnMetadata(
                column_name=c["column_name"],
                data_type=c.get("data_type") or "varchar",
                is_nullable=True,
            )
            for c in _cols
        ]


async def _load_masking_rules(  # REQ-040, REQ-263
    conn: Any,
    col_types_converted: dict[int, list[ColumnMetadata]],
    roles: list[dict],
) -> None:
    """Load masking rules from table_columns and populate state.masking_rules."""
    from provisa.security.masking import MaskingRule, MaskType, validate_masking_rule

    masking_rows = await conn.fetch(
        "SELECT table_id, column_name, unmasked_to, mask_type, mask_pattern, "
        "mask_replace, mask_value, mask_precision FROM table_columns "
        "WHERE mask_type IS NOT NULL"
    )
    for mrow in masking_rows:
        mask_rule = MaskingRule(
            mask_type=MaskType(mrow["mask_type"]),
            pattern=mrow["mask_pattern"],
            replace=mrow["mask_replace"],
            value=_parse_mask_value(mrow["mask_value"]),
            precision=mrow["mask_precision"],
        )
        table_id = mrow["table_id"]
        col_name = mrow["column_name"]
        unmasked_to = list(mrow.get("unmasked_to") or [])
        col_metas = col_types_converted.get(table_id, [])
        data_type = "varchar"
        is_nullable = True
        for cm in col_metas:
            if cm.column_name == col_name:
                data_type = cm.data_type
                is_nullable = cm.is_nullable
                break
        validate_masking_rule(mask_rule, col_name, data_type, is_nullable)
        for role in roles:
            if role["id"] in unmasked_to:
                continue
            key = (table_id, role["id"])
            if key not in state.masking_rules:
                state.masking_rules[key] = {}
            state.masking_rules[key][col_name] = (mask_rule, data_type)


async def _load_tracked_functions_and_webhooks(  # REQ-042
    conn: Any, raw_config: dict | None
) -> tuple[list[dict], list[dict]]:
    """Load tracked functions and webhooks from DB; populate state.tracked_functions/webhooks."""
    from provisa.api.admin.actions_router import _ensure_tables

    await _ensure_tables(state.tenant_db)

    from provisa.discovery.catalog_cache import ensure_table as _ensure_catalog_cache

    await _ensure_catalog_cache(state.tenant_db)
    fn_rows = await conn.fetch("SELECT * FROM tracked_functions ORDER BY name")
    # REQ-209: only steward-approved webhooks are exposed and callable. A webhook is approved
    # when its most recent "webhook" creation_request is executed (editing enqueues a fresh
    # pending request, which resets approval). Tracked in creation_requests — no column on
    # tracked_webhooks.
    wh_rows = await conn.fetch(
        """
        SELECT w.* FROM tracked_webhooks w
        WHERE (
            SELECT c.status FROM creation_requests c
            WHERE c.request_type = 'webhook' AND c.payload->>'name' = w.name
            ORDER BY c.id DESC LIMIT 1
        ) = 'executed'
        ORDER BY w.name
        """
    )
    tracked_functions = [
        {
            **dict(r),
            "arguments": json.loads(r["arguments"])
            if isinstance(r["arguments"], str)
            else (r["arguments"] or []),
            "visible_to": list(r["visible_to"] or []),
        }
        for r in fn_rows
    ]
    tracked_webhooks = [
        {
            **dict(r),
            "arguments": json.loads(r["arguments"])
            if isinstance(r["arguments"], str)
            else (r["arguments"] or []),
            "inline_return_type": json.loads(r["inline_return_type"])
            if isinstance(r["inline_return_type"], str)
            else (r["inline_return_type"] or []),
            "visible_to": list(r["visible_to"] or []),
        }
        for r in wh_rows
    ]

    from provisa.compiler.naming import domain_to_sql_name as _d2sql

    _dp = raw_config.get("naming", {}).get("domain_prefix", False) if raw_config else False
    state.tracked_functions = {}
    for f in tracked_functions:
        state.tracked_functions[f["name"]] = f
        if _dp and f.get("domain_id"):
            prefixed = f"{_d2sql(f['domain_id'])}__{f['name']}"
            state.tracked_functions[prefixed] = f
    state.tracked_webhooks = {}
    for w in tracked_webhooks:
        state.tracked_webhooks[w["name"]] = w
        if _dp and w.get("domain_id"):
            prefixed = f"{_d2sql(w['domain_id'])}__{w['name']}"
            state.tracked_webhooks[prefixed] = w

    return tracked_functions, tracked_webhooks


def _build_and_register_schemas(  # REQ-016, REQ-021, REQ-038, REQ-041, REQ-221, REQ-262, REQ-263
    roles: list[dict],
    tables: list[dict],
    relationships: list[dict],
    col_types_converted: dict[int, list[ColumnMetadata]],
    naming_rules: list[dict],
    domains: list[dict],
    domain_prefix: bool,
    kafka_physical: dict,
    tracked_functions: list[dict],
    tracked_webhooks: list[dict],
    gql_object_cols: dict,
    rls_rules: list[dict],
) -> None:
    """Build and register GraphQL schemas, contexts, and protos for each role."""
    for role in roles:
        state.roles[role["id"]] = role
        _governed_gql_types = {
            tbl.get("gql_type_name")
            for reg in getattr(state, "graphql_remote_sources", {}).values()
            for tbl in reg.get("tables", [])
            if tbl.get("gql_type_name")
        }
        _tbl_id_map = {(t["source_id"], t["table_name"]): t["id"] for t in tables}
        _gov_obj_cols: set[tuple[int, str]] = set()
        for _reg in getattr(state, "graphql_remote_sources", {}).values():
            _src_id = _reg.get("source_id", "")
            for _tbl in _reg.get("tables", []):
                _tbl_id = _tbl_id_map.get((_src_id, _tbl.get("sql_name") or _tbl.get("name", "")))
                if _tbl_id is None:
                    continue
                for _col in _tbl.get("columns", []):
                    if _col.get("gql_object_type") in _governed_gql_types or _col.get(
                        "gql_object_fields"
                    ):
                        _gov_obj_cols.add((_tbl_id, _col["name"]))
        si = SchemaInput(
            tables=tables,
            relationships=relationships,
            column_types=col_types_converted,
            naming_rules=naming_rules,
            role=role,
            domains=domains,
            source_types=state.source_types,
            source_catalogs=state.source_catalogs,
            domain_prefix=domain_prefix,
            physical_table_map={**_META_TABLE_ALIAS, **(kafka_physical or {})},
            functions=tracked_functions,
            webhooks=tracked_webhooks,
            enum_types=state.pg_enum_types,
            gql_object_columns=gql_object_cols,
            governed_gql_types=_governed_gql_types,
            gql_governed_object_cols=_gov_obj_cols,
        )
        try:
            from provisa.compiler.schema_gen import build_table_path_map

            state.schemas[role["id"]] = generate_schema(si)
            state.table_path_maps[role["id"]] = build_table_path_map(si)
            state.contexts[role["id"]] = build_context(si)
            state.rls_contexts[role["id"]] = build_rls_context(
                rls_rules,
                role["id"],
            )
        except ValueError as _schema_err:
            logging.getLogger(__name__).error(
                "generate_schema failed for role %r: %s", role["id"], _schema_err
            )

        try:
            from provisa.grpc.proto_gen import generate_proto

            state.proto_files[role["id"]] = generate_proto(si)
        except ValueError:
            pass


async def _bg_hydrate_api_endpoints() -> None:
    """Background-hydrate zero-param API endpoints (no path params → full collection known at startup)."""
    _zero_param_eps = [
        (ep, state.api_sources[ep.source_id])
        for ep in state.api_endpoints.values()
        if "{" not in ep.path and ep.source_id in state.api_sources
    ]
    if not _zero_param_eps:
        return

    _hydrate_log = logging.getLogger(__name__)
    assert state.tenant_db is not None

    async def _bg_hydrate(eps=_zero_param_eps, pool: Database = state.tenant_db, _log=_hydrate_log):
        from provisa.openapi.pg_cache import fill_api_table

        async with pool.acquire() as _conn:
            for _ep, _src in eps:
                try:
                    await fill_api_table(
                        _src.base_url,
                        _ep.path,
                        _ep.default_params,
                        cast(asyncpg.Connection, _conn),
                        "default",
                        _ep.table_name,
                        _ep.ttl,
                        _ep.response_root,
                        _ep.error_path,
                        _ep.pk_column,
                    )
                except Exception as _e:
                    _log.warning("BG hydration failed for %s: %s", _ep.table_name, _e)

    asyncio.create_task(_bg_hydrate())


async def _reconcile_live_engine(conn: asyncpg.Connection) -> None:  # REQ-565, REQ-813
    """Reconcile the LiveEngine poll jobs from persisted per-table live config."""
    from provisa.live.reconcile import reconcile_live_engine

    await reconcile_live_engine(conn, state.live_engine)


async def _register_user_views_in_state(_pg: asyncpg.Connection, raw_config: dict | None) -> None:
    """Register __provisa__ views in mv_registry (REQ-199) or view_sql_map. Non-fatal."""
    try:
        _view_rows = await _pg.fetch(
            "SELECT table_name, view_sql, materialize, mv_refresh_interval FROM registered_tables"
            " WHERE source_id = '__provisa__' AND view_sql IS NOT NULL"
        )
        # REQ-199: MVs without an explicit interval fall back to the configured default TTL.
        _mv_default_ttl = int(
            (raw_config or {}).get("materialized_views", {}).get("default_ttl", 300)
        )
        for _vr in _view_rows:
            if _vr.get("materialize"):
                from provisa.mv.models import MVDefinition, MVStatus

                _mv_id = f"view-{_vr['table_name']}"
                if state.mv_registry.get(_mv_id) is None:
                    state.mv_registry.register(
                        MVDefinition(
                            id=_mv_id,
                            source_tables=[],
                            target_catalog="postgresql",
                            target_schema=f"org_{state.org_id}_mv_cache",
                            target_table=f"mv_{_vr['table_name']}",
                            refresh_interval=int(_vr.get("mv_refresh_interval") or _mv_default_ttl),
                            enabled=True,
                            sql=_vr["view_sql"].rstrip().rstrip(";"),
                            expose_in_sdl=False,
                            status=MVStatus.STALE,
                        )
                    )
            else:
                state.view_sql_map[_vr["table_name"]] = _vr["view_sql"].rstrip().rstrip(";")
    except Exception as _e:
        log.warning("Failed to load user views for inline expansion: %s", _e)


async def _finalize_rebuild_state(_rebuild_log: logging.Logger) -> None:
    """Reconcile live engine (REQ-565) and compile view SQLs after a schema rebuild."""
    # Re-drive the live poll engine from the now-current DB state so admin edits
    # to per-table live config take effect without a restart (REQ-565).
    if state.live_engine is not None and state.tenant_db is not None:
        try:
            async with state.tenant_db.acquire() as _lc:
                await _reconcile_live_engine(cast(asyncpg.Connection, _lc))
        except Exception:
            _rebuild_log.exception("live engine reconcile failed")

    # Compile inline view SQLs now that a context is available
    if state.view_sql_map and state.contexts:
        from provisa.compiler.sql_gen import (
            normalize_table_refs,
            rewrite_semantic_to_catalog_physical,
        )

        ctx = next(iter(state.contexts.values()))
        state.view_sql_map = {
            name: rewrite_semantic_to_catalog_physical(normalize_table_refs(sql, ctx), ctx)
            for name, sql in state.view_sql_map.items()
        }


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
            dict(r) for r in await conn.fetch("SELECT pattern, replacement FROM naming_rules")
        ]

        # Load per-table cache TTLs
        cache_rows = await conn.fetch(
            "SELECT id, cache_ttl FROM registered_tables WHERE cache_ttl IS NOT NULL"
        )
        state.table_cache = {r["id"]: r["cache_ttl"] for r in cache_rows}
        domains = [dict(r) for r in await conn.fetch("SELECT id, description FROM domains")]
        sources = {r["id"]: dict(r) for r in await conn.fetch("SELECT * FROM sources")}
        # Backfill state.source_types; patch postgresql sources to use the engine catalog names.
        for _sid, _src_dict in list(sources.items()):
            if _sid not in state.source_types and _src_dict.get("type"):
                state.source_types[_sid] = _src_dict["type"]
            if _src_dict.get("type") == "postgresql":
                sources[_sid] = {**_src_dict, "database": source_to_catalog(_sid)}
        roles = [
            dict(r) for r in await conn.fetch("SELECT id, capabilities, domain_access FROM roles")
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
        state.federation_engine.reseed_ops(
            _OPS_VIEWS, getattr(state, "otel_snapshot_retention_hours", None)
        )

        await _register_user_views_in_state(_pg, raw_config)

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


def _prewarm_govdata_jvm(_log: logging.Logger) -> None:
    """Start GovData JVM pre-warm in a background thread if govdata sources are active."""
    _govdata_active = any(v == "govdata" for v in state.source_types.values()) or bool(
        os.environ.get("ASKAMERICA_API_KEY")
    )
    if not _govdata_active:
        return
    import threading as _threading

    def _prewarm_jvm():
        try:
            from provisa.govdata.source import _jvm_lock as _lock
            from askamerica.engine import DEFAULT_SCHEMAS as _DS, start_jvm as _start_jvm  # type: ignore[import-untyped]

            with _lock:
                if "ASKAMERICA_SCHEMAS" not in os.environ:
                    os.environ["ASKAMERICA_SCHEMAS"] = _DS
                api_key = os.environ.get("ASKAMERICA_API_KEY", "")
                _start_jvm(api_key)
        except Exception:
            _log.exception("GovData JVM pre-warm failed")

    _threading.Thread(target=_prewarm_jvm, daemon=True, name="govdata-jvm-prewarm").start()


async def _start_background_tasks(_log: logging.Logger) -> None:
    """Start MV refresh, warm-table, hot-table refresh, and SQLite staleness background tasks."""
    if state.mv_registry.get_enabled() and state.engine_conn:
        from provisa.mv.refresh import refresh_loop

        state._mv_refresh_task = asyncio.create_task(
            refresh_loop(state.federation_engine, state.mv_registry),
        )

    if state.engine_conn:
        from provisa.compiler.sql_gen import query_counter as _qc

        # REQ-240: warm-tier thresholds + sweep interval come from config (warm_tables.*),
        # not Python constants. Per-table warm: true/false sets force/opt-out.
        _raw: dict = {}
        _warm_cfg_path = Path(os.environ.get("PROVISA_CONFIG", "config/provisa.yaml"))
        if _warm_cfg_path.exists():
            with open(_warm_cfg_path) as _wf:
                _raw = yaml.safe_load(_wf) or {}
        _wcfg = _raw.get("warm_tables", {})
        _warm_threshold = int(_wcfg.get("query_threshold", 100))
        _warm_max_rows = int(_wcfg.get("max_rows", 10_000_000))
        _warm_interval = int(_wcfg.get("refresh_interval", 60))
        _warm_forced: set[str] = set()
        _warm_excluded: set[str] = set()
        for _t in _raw.get("tables", []):
            _tn = _t.get("table") or _t.get("table_name")
            if _tn and "warm" in _t:
                (_warm_forced if _t["warm"] else _warm_excluded).add(_tn)

        async def _warm_loop() -> None:
            while True:
                try:
                    # REQ-241: hot-over-warm precedence — exclude tables the hot tier manages.
                    _hot_names = (
                        state.hot_manager.managed_tables()
                        if state.hot_manager is not None
                        else set()
                    )
                    await state.warm_manager.check_promotions(
                        _qc,
                        state.federation_engine,
                        threshold=_warm_threshold,
                        max_rows=_warm_max_rows,
                        hot_tables=_hot_names,
                        excluded=_warm_excluded,
                        forced=_warm_forced,
                    )
                    await state.warm_manager.check_demotions(
                        _qc, state.federation_engine, threshold=_warm_threshold
                    )
                except Exception:
                    _log.exception("Error in warm-table loop")
                await asyncio.sleep(_warm_interval)

        state._warm_task = asyncio.create_task(_warm_loop())

    if state.hot_manager is not None and state.engine_conn:
        from provisa.cache.hot_tables import HotTableManager

        hot_mgr = state.hot_manager
        assert isinstance(hot_mgr, HotTableManager)

        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        _hot_path = Path(config_path)
        _hot_interval = 300
        if _hot_path.exists():
            with open(_hot_path) as _hf:
                _hot_cfg = yaml.safe_load(_hf)
            _hot_interval = _hot_cfg.get("hot_tables", {}).get("refresh_interval", 300)

        async def _hot_refresh_loop() -> None:
            while True:
                await asyncio.sleep(_hot_interval)
                for entry in list(hot_mgr._hot_tables.values()):
                    if entry.is_api:
                        continue
                    try:
                        await hot_mgr.load_table(
                            state.federation_engine,
                            entry.table_name,
                            entry.schema,
                            entry.catalog,
                            entry.pk_column,
                        )
                    except Exception:
                        _log.exception("Hot table refresh failed: %s", entry.table_name)

        state._hot_refresh_task = asyncio.create_task(_hot_refresh_loop())

    _sqlite_check_interval = 60

    async def _sqlite_stale_loop() -> None:
        from provisa.file_source.pg_migrate import migrate_if_stale

        while True:
            await asyncio.sleep(_sqlite_check_interval)
            try:
                if state.tenant_db is None:
                    continue
                async with state.tenant_db.acquire() as conn:
                    _sc = cast(asyncpg.Connection, conn)
                    rows = await _sc.fetch(
                        """SELECT rt.id, rt.table_name, rt.schema_name, s.path
                           FROM registered_tables rt
                           JOIN sources s ON s.id = rt.source_id
                           WHERE s.type = 'sqlite' AND s.path IS NOT NULL"""
                    )
                    for r in rows:
                        try:
                            migrated = await migrate_if_stale(
                                r["id"],
                                r["path"],
                                r["table_name"],
                                _sc,
                                r["schema_name"],
                                r["table_name"],
                            )
                            if migrated:
                                _log.info(
                                    "SQLite stale: re-migrated table %d (%s)",
                                    r["id"],
                                    r["table_name"],
                                )
                        except Exception:
                            _log.exception("SQLite stale check failed for table %d", r["id"])
            except Exception:
                _log.exception("SQLite staleness loop error")

    state._stale_check_task = asyncio.create_task(_sqlite_stale_loop())


async def _start_servers(_log: logging.Logger) -> None:
    """Start gRPC, Arrow Flight, pgwire, Live Query Engine, and APQ cache servers."""
    if state.proto_files:
        try:
            import tempfile
            from provisa.grpc.schema_gen import compile_proto
            from provisa.grpc.server import start_grpc_server

            first_proto = next(iter(state.proto_files.values()))
            grpc_output_dir = tempfile.mkdtemp(prefix="provisa_grpc_")
            pb2_path, pb2_grpc_path = compile_proto(first_proto, grpc_output_dir)
            grpc_port = int(
                os.environ.get("GRPC_PORT", str(state.server_cfg.get("grpc_port", 50051)))
            )
            state._grpc_server = await start_grpc_server(
                grpc_port,
                state,
                pb2_path,
                pb2_grpc_path,
            )
            state._grpc_port = grpc_port
            _log.info("gRPC server listening on %s:%d", state.hostname, grpc_port)
        except Exception:
            _log.exception("gRPC server startup failed")

    try:
        from provisa.api.flight.server import ProvisaFlightServer

        flight_port = int(
            os.environ.get("FLIGHT_PORT", str(state.server_cfg.get("flight_port", 8815)))
        )
        flight_server = ProvisaFlightServer(
            state,
            location=f"grpc://0.0.0.0:{flight_port}",
            main_loop=asyncio.get_running_loop(),
        )
        import threading

        flight_thread = threading.Thread(
            target=flight_server.serve,
            daemon=True,
        )
        flight_thread.start()
        state._flight_server = flight_server
        state._flight_port = flight_port
        _log.info("Arrow Flight server listening on %s:%d", state.hostname, flight_port)
    except Exception:
        _log.exception("Arrow Flight server startup failed")

    pgwire_port = int(os.environ.get("PROVISA_PGWIRE_PORT", "0"))
    if pgwire_port:
        try:
            import ssl as _ssl
            from provisa.pgwire import catalog as _pgwire_catalog
            from provisa.pgwire.server import start_pgwire_server

            _pgwire_catalog._KNOWN_SETTINGS["search_path"] = f"org_{state.org_id}"  # REQ-695

            _ssl_ctx: _ssl.SSLContext | None = None
            _cert = os.environ.get("PROVISA_PGWIRE_CERT")
            _key = os.environ.get("PROVISA_PGWIRE_KEY")
            if _cert and _key:
                _ssl_ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
                _ssl_ctx.load_cert_chain(_cert, _key)

            start_pgwire_server(
                host="0.0.0.0",  # nosec B104 - pgwire server intentionally binds all interfaces
                port=pgwire_port,
                ssl_ctx=_ssl_ctx,
                loop=asyncio.get_running_loop(),
            )
            state._pgwire_port = pgwire_port
            _log.info(
                "pgwire server listening on 0.0.0.0:%d (TLS=%s)", pgwire_port, _ssl_ctx is not None
            )
        except Exception:
            _log.exception("pgwire server startup failed")

    bolt_port = int(os.environ.get("PROVISA_BOLT_PORT", "0"))
    if bolt_port:
        try:
            import ssl as _ssl_bolt
            from provisa.bolt.server import start_bolt_server

            _bolt_ssl_ctx: _ssl_bolt.SSLContext | None = None
            _bolt_cert = os.environ.get("PROVISA_BOLT_CERT")
            _bolt_key = os.environ.get("PROVISA_BOLT_KEY")
            if _bolt_cert and _bolt_key:
                _bolt_ssl_ctx = _ssl_bolt.SSLContext(_ssl_bolt.PROTOCOL_TLS_SERVER)
                _bolt_ssl_ctx.load_cert_chain(_bolt_cert, _bolt_key)

            start_bolt_server(
                host="0.0.0.0",  # nosec B104 - bolt server intentionally binds all interfaces
                port=bolt_port,
                ssl_ctx=_bolt_ssl_ctx,
                loop=asyncio.get_running_loop(),
            )
            state._bolt_port = bolt_port
            _log.info(
                "bolt server listening on 0.0.0.0:%d (TLS=%s)", bolt_port, _bolt_ssl_ctx is not None
            )
        except Exception:
            _log.exception("bolt server startup failed")

    try:
        from provisa.live.engine import LiveEngine

        live_engine = LiveEngine(tenant_db=state.tenant_db, engine=state.federation_engine)
        await live_engine.start()
        state.live_engine = live_engine
        _log.info("Live Query Engine started")

        # Reconcile poll jobs from persisted per-table live config (Phase AY).
        # Data polls route through the engine; CDC-delivered tables are driven by
        # subscription providers, not the poll engine.
        if state.tenant_db is not None:
            async with state.tenant_db.acquire() as _lc:
                await _reconcile_live_engine(cast(asyncpg.Connection, _lc))
    except Exception:
        _log.exception("Live Query Engine startup failed")

    # REQ-289: APQ cache uses the resolved cache.redis_url and apq.ttl (not raw env vars).
    # REQ-829: with no URL, RedisAPQCache(None) uses embedded fakeredis so desktop
    # exercises the same APQ code path as production.
    try:
        from provisa.apq.cache import RedisAPQCache

        state.apq_cache = RedisAPQCache(state.redis_url, ttl=state.apq_ttl)
        _log.info(
            "APQ cache initialized (Redis: %s, ttl=%ds)",
            state.redis_url or "embedded fakeredis",
            state.apq_ttl,
        )
    except Exception:
        _log.exception("APQ cache initialization failed")


def _start_scheduler(_log: logging.Logger) -> None:
    """Start APScheduler with config-based triggers, OTEL compaction, and the engine watcher."""
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = AsyncIOScheduler()
        _cfg_triggers = []
        try:
            with open(os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")) as _cfg_f:
                _raw = yaml.safe_load(_cfg_f.read())
            if isinstance(_raw, dict):
                from provisa.core.config_loader import parse_config_dict

                _cfg = parse_config_dict(_raw)
                _cfg_triggers = _cfg.scheduled_triggers if _cfg.scheduled_triggers else []
        except Exception:
            pass
        from provisa.scheduler.jobs import build_scheduler

        _cfg_scheduler = build_scheduler(_cfg_triggers)
        if _cfg_scheduler:
            for job in _cfg_scheduler.get_jobs():
                scheduler.add_job(
                    job.func,
                    trigger=job.trigger,
                    args=job.args,
                    id=job.id,
                    name=job.name,
                    replace_existing=True,
                )
        from provisa.scheduler.jobs import compact_otel_signals, watch_engine

        scheduler.add_job(
            compact_otel_signals,
            trigger=CronTrigger.from_crontab(state.otel_compact_cron),
            id="otel_compact",
            name="otel:compact_signals",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        scheduler.add_job(
            watch_engine,
            trigger=CronTrigger.from_crontab("* * * * *"),
            id="engine_watch",
            name="engine:watcher",
            replace_existing=True,
        )

        scheduler.start()
        state._scheduler = scheduler
        _log.info("APScheduler started")
    except Exception:
        _log.exception("APScheduler startup failed")


async def _auto_register_graphql_demo(_log: logging.Logger) -> None:
    """Auto-register graphql-demo source if GRAPHQL_DEMO_URL or GRAPHQL_DEMO_ENABLED is set."""
    _graphql_demo_url = os.environ.get("GRAPHQL_DEMO_URL", "http://graphql-demo:4000/graphql")
    if not (
        os.environ.get("GRAPHQL_DEMO_ENABLED", "").lower() in ("1", "true", "yes")
        or os.environ.get("GRAPHQL_DEMO_URL")
    ):
        return

    async def _register_graphql_demo() -> None:
        from provisa.api.admin.graphql_remote_router import (
            _introspect_and_map,
            _upsert_tables_to_semantic_layer,
            GraphQLRemoteRegistration,
        )

        try:
            tables, functions, relationships = await _introspect_and_map(
                "graphql-demo",
                _graphql_demo_url,
                "",
                "shelter",
                None,
            )
            reg = GraphQLRemoteRegistration(
                source_id="graphql-demo",
                url=_graphql_demo_url,
                namespace="",
                domain_id="shelter",
                cache_ttl=300,
                tables=tables,
                functions=functions,
                relationships=relationships,
            )
            if not hasattr(state, "graphql_remote_sources"):
                state.graphql_remote_sources = {}
            state.graphql_remote_sources["graphql-demo"] = reg.model_dump()
            _demo_pool = state.tenant_db
            if _demo_pool is not None:
                async with _demo_pool.acquire() as _conn:
                    await _conn.execute(
                        """
                        INSERT INTO sources (id, type, host, port, database, username, dialect, path, description)
                        VALUES ('graphql-demo', 'graphql_remote', '', 0, '', '', '', $1, 'Animal shelter GraphQL API — staff schedules, breed catalogue, and animal assignment records managed by shelter operations')
                        ON CONFLICT (id) DO UPDATE SET path = EXCLUDED.path, description = EXCLUDED.description
                        """,
                        _graphql_demo_url,
                    )
                    await _conn.execute(
                        "INSERT INTO domains (id, description) VALUES ('shelter', 'Animal shelter staff and breed management') ON CONFLICT (id) DO NOTHING",
                    )
                await _upsert_tables_to_semantic_layer(
                    "graphql-demo",
                    "shelter",
                    tables,
                    _demo_pool,
                )
                from provisa.api.admin.graphql_remote_router import (
                    _upsert_relationships_to_semantic_layer,
                )

                await _upsert_relationships_to_semantic_layer(relationships, _demo_pool, state)
                from provisa.core.models import Cardinality, Relationship
                from provisa.core.repositories import relationship as rel_repo

                async with _demo_pool.acquire() as _rel_conn:
                    _pg_rel = cast(asyncpg.Connection, _rel_conn)
                    for _rel_id, _src_tbl, _tgt_tbl, _src_col, _tgt_col, _card, _alias in [
                        (
                            "employees_to_assignments",
                            "employees",
                            "assignments",
                            "id",
                            "employee_id",
                            "one-to-many",
                            None,
                        ),
                        (
                            "pets-to-shelter-breed",
                            "pets",
                            "animal_breeds",
                            "breed_name",
                            "name",
                            "many-to-one",
                            "BREED_INFO",
                        ),
                        (
                            "shelter-breed-to-pets",
                            "animal_breeds",
                            "pets",
                            "name",
                            "breed_name",
                            "one-to-many",
                            "PETS_OF_BREED",
                        ),
                        (
                            "pets-to-shelter-assignments",
                            "pets",
                            "assignments",
                            "breed_name",
                            "breed_name",
                            "many-to-one",
                            None,
                        ),
                        (
                            "shelter-assignments-to-pets",
                            "assignments",
                            "pets",
                            "breed_name",
                            "breed_name",
                            "one-to-many",
                            None,
                        ),
                        (
                            "shelter-assignments-to-employees",
                            "assignments",
                            "employees",
                            "employee_id",
                            "id",
                            "many-to-one",
                            None,
                        ),
                    ]:
                        try:
                            await rel_repo.upsert(
                                _pg_rel,
                                Relationship(
                                    id=_rel_id,
                                    source_table_id=_src_tbl,
                                    target_table_id=_tgt_tbl,
                                    source_column=_src_col,
                                    target_column=_tgt_col,
                                    cardinality=Cardinality(_card),
                                    **({} if _alias is None else {"alias": _alias}),
                                ),
                            )
                        except Exception:
                            _log.warning("Failed to upsert %s", _rel_id, exc_info=True)
                    # schedules.employee is a JSONB blob with no employee_id scalar exposed in
                    # the GQL schema, so _infer_fk_columns returns ("", ""). Correct it here.
                    try:
                        await rel_repo.upsert(
                            _pg_rel,
                            Relationship(
                                id="gql_remote__graphql-demo__schedules__employee",
                                source_table_id="schedules",
                                target_table_id="employees",
                                source_column="employee",
                                target_column="id",
                                cardinality=Cardinality("many-to-one"),
                                alias="IS_EMPLOYEE",
                                graphql_alias="employee",
                                source_json_key="id",
                                disable_cypher=True,
                            ),
                        )
                    except Exception:
                        _log.warning(
                            "Failed to upsert gql_remote__graphql-demo__schedules__employee",
                            exc_info=True,
                        )
            _log.info(
                "Auto-registered graphql-demo source (%d tables, %d functions)",
                len(tables),
                len(functions),
            )
            await _rebuild_schemas()
        except Exception:
            _log.warning(
                "graphql-demo auto-registration failed (service may not be up yet)",
                exc_info=True,
            )

    asyncio.create_task(_register_graphql_demo())


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
    # Stop Live Query Engine (Phase AM)
    if state.live_engine is not None:
        try:
            await state.live_engine.stop()
        except Exception:
            pass

    # Close APQ cache (Phase AN)
    try:
        await state.apq_cache.close()
    except Exception:
        pass

    # Stop scheduler (Phase AX)
    if state._scheduler is not None:
        try:
            state._scheduler.shutdown(wait=False)
        except Exception:
            pass

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

    app = FastAPI(title="Provisa", lifespan=lifespan)
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
                    except Exception:
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
            except Exception:
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
