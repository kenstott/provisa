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

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

log = logging.getLogger(__name__)

import asyncpg
import trino
import yaml
from fastapi import FastAPI

from provisa.api.data.endpoint import router as data_router
from provisa.api.data.endpoint_dev import router as dev_router
from provisa.api.data.sdl import router as sdl_router
from provisa.compiler.introspect import ColumnMetadata, introspect_tables
from provisa.compiler.naming import source_to_catalog
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.rls import RLSContext, build_rls_context
from provisa.compiler.sql_gen import CompilationContext, build_context
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.db import create_pool, init_schema
from provisa.core.secrets import resolve_secrets
from provisa.executor.pool import SourcePool
from provisa.compiler.mask_inject import MaskingRules
from provisa.cache.store import CacheStore, NoopCacheStore, RedisCacheStore
from provisa.api.admin.db_queries import fetch_tables as _fetch_tables, fetch_relationships as _fetch_relationships, parse_mask_value as _parse_mask_value
from provisa.api.otel_setup import setup_otel as _setup_otel
from provisa.mv.registry import MVRegistry
from provisa.cache.warm_tables import WarmTableManager
from provisa.apq.cache import APQCache, NoopAPQCache


class AppState:
    """Shared application state populated at startup."""

    pg_pool: asyncpg.Pool | None = None
    trino_conn: trino.dbapi.Connection | None = None
    flight_client: object | None = None  # pyarrow.flight.FlightClient
    schemas: dict[str, object] = {}  # role_id → GraphQLSchema
    schema_build_cache: dict = {}  # raw data for on-demand domain-filtered schema building
    contexts: dict[str, CompilationContext] = {}  # role_id → CompilationContext
    rls_contexts: dict[str, RLSContext] = {}  # role_id → RLSContext
    roles: dict[str, dict] = {}  # role_id → role dict
    source_pools: SourcePool = SourcePool()
    source_types: dict[str, str] = {}  # source_id → source_type
    source_catalogs: dict[str, str] = {}  # source_id → Trino catalog name
    source_dialects: dict[str, str] = {}  # source_id → sqlglot dialect
    masking_rules: MaskingRules = {}  # (table_id, role_id) → {col: (rule, dtype)}
    response_cache_store: CacheStore = NoopCacheStore()
    response_cache_default_ttl: int = 300
    mv_registry: MVRegistry = MVRegistry()
    _mv_refresh_task: asyncio.Task | None = None
    proto_files: dict[str, str] = {}  # role_id → .proto content
    _grpc_server: object | None = None
    _flight_server: object | None = None  # ProvisaFlightServer
    kafka_windows: dict[str, str] = {}  # source_id → default_window (e.g. "1h")
    kafka_table_configs: dict[str, object] = {}  # table_name → KafkaTableConfig
    view_sql_map: dict[str, str] = {}  # view_table_name → SQL (for inline expansion)
    source_cache: dict[str, dict] = {}  # source_id → {cache_enabled, cache_ttl}
    table_cache: dict[int, int | None] = {}  # table_id → cache_ttl
    auth_config: dict | None = None  # auth section from provisa.yaml
    api_endpoints: dict[str, object] = {}  # table_name → ApiEndpoint
    api_sources: dict[str, object] = {}  # source_id → ApiSource
    hot_manager: object | None = None  # HotTableManager
    _hot_refresh_task: asyncio.Task | None = None
    warm_manager: WarmTableManager = WarmTableManager()
    _warm_task: asyncio.Task | None = None
    apq_cache: APQCache = NoopAPQCache()  # Phase AN: Automatic Persisted Queries
    live_engine: object | None = None  # Phase AM: LiveEngine instance
    hostname: str = "localhost"  # publicly reachable hostname (PROVISA_HOSTNAME)
    source_federation_hints: dict[str, dict[str, str]] = {}  # source_id → Trino session props (AL3)
    source_allowed_domains: dict[str, list[str]] = {}  # source_id → allowed domain ids (empty = unrestricted)
    trino_fte_hints: dict[str, str] = {}  # FTE session properties injected into every Trino query
    server_cfg: dict = {}  # raw server section from provisa.yaml
    server_limits: dict = {}  # resolved query/request limits (from config + env overrides)
    tracked_functions: dict[str, dict] = {}  # gql field name → fn dict
    tracked_webhooks: dict[str, dict] = {}   # gql field name → wh dict
    pg_enum_types: dict = {}  # pg_name → GraphQLEnumType (REQ-221)
    graphql_remote_sources: dict[str, dict] = {}  # source_id → GraphQL remote registration
    openapi_specs: dict[str, dict] = {}  # source_id → OpenAPI spec registration
    grpc_remote_sources: dict[str, dict] = {}  # source_id → gRPC remote registration
    # Phase AS — Ingest sources
    ingest_engines: dict[str, object] = {}  # source_id → AsyncEngine
    ingest_tables: dict[str, dict[str, list[dict]]] = {}  # source_id → {table_name → [col defs]}
    # WebSocket sources
    websocket_sources: dict[str, object] = {}  # source_id → Source
    # RSS/Atom feed sources
    rss_sources: dict[str, object] = {}  # source_id → Source
    pg_notify_tables: set[str] = set()  # table_names with pg_notify triggers installed
    table_watermarks: dict[str, str] = {}  # table_name → watermark_column (for polling fallback)
    _scheduler: object | None = None  # APScheduler instance for scheduled queries
    global_naming_convention: str = "apollo_graphql"  # runtime override; set via updateNamingConvention
    otel_compact_cron: str = "* * * * *"  # cron for Parquet→Iceberg compaction
    otel_compact_batch_size: int = 10  # rows per INSERT batch during compaction


state = AppState()

# Views replace tables that have text[] columns Trino can't surface; arrays cast to JSON text.
_META_TABLE_VIEWS: dict[str, str] = {
    "registered_tables": """
        DROP VIEW IF EXISTS public.registered_tables_meta CASCADE;
        CREATE VIEW public.registered_tables_meta AS
        SELECT id, source_id, domain_id, schema_name, table_name, governance,
               alias, description, cache_ttl, naming_convention, watermark_column,
               column_presets::text AS column_presets
        FROM public.registered_tables
    """,
    "table_columns": """
        DROP VIEW IF EXISTS public.table_columns_meta CASCADE;
        CREATE VIEW public.table_columns_meta AS
        SELECT id, table_id, column_name, data_type, is_primary_key,
               alias, description, path,
               mask_type, mask_pattern, mask_replace, mask_value, mask_precision,
               native_filter_type, is_foreign_key, is_alternate_key,
               object_fields::text AS object_fields,
               array_to_json(visible_to)::text  AS visible_to,
               array_to_json(unmasked_to)::text AS unmasked_to,
               array_to_json(writable_by)::text AS writable_by
        FROM public.table_columns
    """,
    "roles": """
        CREATE OR REPLACE VIEW public.roles_meta AS
        SELECT id, parent_role_id,
               array_to_json(capabilities)::text  AS capabilities,
               array_to_json(domain_access)::text AS domain_access
        FROM public.roles
    """,
}
# Maps original table name → view name (or itself if no view needed).
_META_TABLE_ALIAS: dict[str, str] = {
    "registered_tables": "registered_tables_meta",
    "table_columns": "table_columns_meta",
    "roles": "roles_meta",
}
_META_TABLES = [
    "registered_tables", "table_columns", "domains", "relationships",
    "rls_rules", "governed_queries", "roles",
]

_OPS_PG_TO_TRINO: dict[str, str] = {
    "text": "VARCHAR",
    "bigint": "BIGINT",
    "integer": "INTEGER",
    "float8": "DOUBLE",
    "date": "DATE",
    "boolean": "BOOLEAN",
}

# Matches actual otlp2parquet output schema (https://github.com/smithclay/otlp2parquet).
# Timestamps are milliseconds. span_attributes is a JSON string.
# We add table_name/domain_id/role_id extracted from span_attributes during compaction.
_OPS_TABLES: dict[str, list[tuple[str, str, bool]]] = {
    "traces": [
        ("trace_id", "text", True),
        ("span_id", "text", False),
        ("parent_span_id", "text", False),
        ("span_name", "text", False),
        ("span_kind", "integer", False),
        ("service_name", "text", False),
        ("service_namespace", "text", False),
        ("timestamp", "bigint", False),
        ("end_timestamp", "bigint", False),
        ("duration", "bigint", False),
        ("status_code", "integer", False),
        ("status_message", "text", False),
        ("scope_name", "text", False),
        ("span_attributes", "text", False),
        ("resource_attributes", "text", False),
        # extracted from span_attributes during compaction
        ("table_name", "text", False),
        ("domain_id", "text", False),
        ("role_id", "text", False),
        ("_date", "date", False),
    ],
    "metrics": [
        ("timestamp", "bigint", True),
        ("start_timestamp", "bigint", False),
        ("metric_name", "text", False),
        ("metric_description", "text", False),
        ("metric_unit", "text", False),
        ("metric_type", "text", False),
        ("service_name", "text", False),
        ("service_namespace", "text", False),
        ("scope_name", "text", False),
        ("metric_attributes", "text", False),
        ("resource_attributes", "text", False),
        ("value", "float8", False),
        ("_date", "date", False),
    ],
    "logs": [
        ("timestamp", "bigint", True),
        ("observed_timestamp", "bigint", False),
        ("trace_id", "text", False),
        ("span_id", "text", False),
        ("severity_number", "integer", False),
        ("severity_text", "text", False),
        ("body", "text", False),
        ("service_name", "text", False),
        ("service_namespace", "text", False),
        ("scope_name", "text", False),
        ("log_attributes", "text", False),
        ("resource_attributes", "text", False),
        ("_date", "date", False),
    ],
}

# Views registered in the ops domain alongside the raw Iceberg tables.
# Each entry: (view_name, [(col_name, data_type, is_pk)], ddl_sql)
_OPS_VIEWS: list[tuple[str, list[tuple[str, str, bool]], str]] = [
    (
        "provisa_queries",
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
            ("_date", "date", False),
        ],
        """\
CREATE OR REPLACE VIEW otel.signals.provisa_queries AS
SELECT
    trace_id,
    span_id,
    parent_span_id,
    span_name,
    service_name,
    timestamp,
    end_timestamp,
    duration,
    status_code,
    json_extract_scalar(span_attributes, '$.table_name') AS table_name,
    json_extract_scalar(span_attributes, '$.domain_id') AS domain_id,
    json_extract_scalar(span_attributes, '$.role_id') AS role_id,
    _date
FROM otel.signals.traces
WHERE span_name LIKE 'provisa.query%'
""",
    ),
]


async def _seed_meta_domain(conn: asyncpg.Connection) -> None:
    """Register admin tables in the built-in meta domain (idempotent)."""
    for ddl in _META_TABLE_VIEWS.values():
        await conn.execute(ddl)

    # Remove any stale view-named entries left by older code versions.
    for view_name in _META_TABLE_ALIAS.values():
        await conn.execute(
            "DELETE FROM registered_tables "
            "WHERE source_id = 'provisa-admin' AND schema_name = 'public' AND table_name = $1",
            view_name,
        )

    for tbl in _META_TABLES:
        table_id = await conn.fetchval(
            """
            INSERT INTO registered_tables
                (source_id, domain_id, schema_name, table_name, governance)
            VALUES ('provisa-admin', 'meta', 'public', $1, 'pre-approved')
            ON CONFLICT (source_id, schema_name, table_name)
                DO UPDATE SET domain_id = 'meta'
            RETURNING id
            """,
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
                WHERE tc.table_schema = 'public' AND tc.table_name = $1
                  AND tc.constraint_type = 'PRIMARY KEY'
                """,
                tbl,
            )
        }
        # Query the view (or table) for its column list; arrays and jsonb appear as 'text' after view casting.
        cols = await conn.fetch(
            """
            SELECT column_name,
                   CASE WHEN data_type IN ('ARRAY', 'jsonb', 'json') THEN 'text' ELSE data_type END AS data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
            """,
            tbl,
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


async def _seed_ops_pg(conn: asyncpg.Connection) -> None:
    """Register ops tables/views in PG registered_tables + table_columns (idempotent)."""
    for tbl_name, cols in _OPS_TABLES.items():
        table_id = await conn.fetchval(
            """
            INSERT INTO registered_tables
                (source_id, domain_id, schema_name, table_name, governance)
            VALUES ('provisa-otel', 'ops', 'signals', $1, 'pre-approved')
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
                table_id, col_name, pg_type, is_pk,
            )
    for view_name, cols, _ in _OPS_VIEWS:
        table_id = await conn.fetchval(
            """
            INSERT INTO registered_tables
                (source_id, domain_id, schema_name, table_name, governance)
            VALUES ('provisa-otel', 'ops', 'signals', $1, 'pre-approved')
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
                table_id, col_name, pg_type, is_pk,
            )


def _seed_ops_trino(trino_conn: object) -> None:
    """Create Iceberg schema/tables/views in Trino for the ops domain (idempotent)."""
    import logging as _ops_log
    _log = _ops_log.getLogger(__name__)

    def _exec(ddl: str) -> None:
        cur = trino_conn.cursor()  # type: ignore[union-attr]
        cur.execute(ddl)
        cur.fetchall()

    # Schema + physical tables — one exception aborts table creation (catalog not ready).
    try:
        _exec("CREATE SCHEMA IF NOT EXISTS otel.signals")
        for tbl_name, cols in _OPS_TABLES.items():
            col_defs = [
                f'"{col_name}" {_OPS_PG_TO_TRINO.get(pg_type, "VARCHAR")}'
                for col_name, pg_type, _ in cols
            ]
            _exec(
                f"CREATE TABLE IF NOT EXISTS otel.signals.{tbl_name} "
                f"({', '.join(col_defs)}) "
                f"WITH (partitioning = ARRAY['_date'], format = 'PARQUET')"
            )
    except Exception:
        _log.warning("ops Iceberg DDL failed — will retry before next schema introspection", exc_info=True)
        return

    # Column additions are non-fatal and isolated per table.
    for tbl_name, cols in _OPS_TABLES.items():
        try:
            cur = trino_conn.cursor()  # type: ignore[union-attr]
            cur.execute(f"SHOW COLUMNS FROM otel.signals.{tbl_name}")
            existing_cols = {row[0].lower() for row in cur.fetchall()}
            for col_name, pg_type, _ in cols:
                if col_name.lower() not in existing_cols:
                    trino_type = _OPS_PG_TO_TRINO.get(pg_type, "VARCHAR")
                    try:
                        _exec(f'ALTER TABLE otel.signals.{tbl_name} ADD COLUMN "{col_name}" {trino_type}')
                        _log.info("ops Iceberg: added column %s.%s", tbl_name, col_name)
                    except Exception:
                        _log.warning("ops Iceberg: could not add column %s.%s", tbl_name, col_name, exc_info=True)
        except Exception:
            _log.warning("ops Iceberg: could not inspect columns for %s", tbl_name, exc_info=True)

    # Views — always attempted; independent of column-addition failures.
    # Use DROP IF EXISTS + CREATE VIEW for broad Trino version compatibility
    # (CREATE OR REPLACE VIEW for Iceberg requires Trino 418+).
    for view_name, _, view_ddl in _OPS_VIEWS:
        try:
            _exec(f"DROP VIEW IF EXISTS otel.signals.{view_name}")
            clean_ddl = view_ddl.replace("CREATE OR REPLACE VIEW", "CREATE VIEW")
            _exec(clean_ddl)
        except Exception:
            _log.warning("ops view %s: create failed", view_name, exc_info=True)

    # In demo mode, expire old snapshots and orphan files to prevent MinIO disk exhaustion.
    import os as _os
    if _os.environ.get("GRAPHQL_DEMO_ENABLED", "").lower() in ("1", "true"):
        import datetime as _dt
        threshold = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S.000")
        for tbl_name in _OPS_TABLES:
            for proc, arg in [
                ("expire_snapshots", f"retention_threshold => TIMESTAMP '{threshold}'"),
                ("remove_orphan_files", f"retention_threshold => TIMESTAMP '{threshold}'"),
            ]:
                try:
                    _exec(f"ALTER TABLE otel.signals.{tbl_name} EXECUTE {proc}({arg})")
                    _log.info("ops Iceberg demo: %s on %s", proc, tbl_name)
                except Exception:
                    _log.warning("ops Iceberg demo: %s on %s failed (non-fatal)", proc, tbl_name, exc_info=True)


async def _load_and_build(config_path: str | None = None) -> None:
    """Load config, introspect Trino, build schemas for all roles."""
    if config_path is None:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")

    # Connect to PG and init schema unconditionally — pool must be available
    # even before a config file exists (admin UI needs it on first start).
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    pg_database = os.environ.get("PG_DATABASE", "provisa")
    pg_user = os.environ.get("PG_USER", "provisa")
    pg_password = os.environ.get("PG_PASSWORD", "provisa")
    pg_pool_min = int(os.environ.get("PG_POOL_MIN", "2"))
    pg_pool_max = int(os.environ.get("PG_POOL_MAX", "10"))

    state.pg_pool = await create_pool(
        pg_host, pg_port, pg_database, pg_user, pg_password,
        min_size=pg_pool_min, max_size=pg_pool_max,
    )

    schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
    if schema_sql_path.exists():
        schema_sql = schema_sql_path.read_text()
        await init_schema(state.pg_pool, schema_sql)

    # Seed built-in sources so the Data Sources page is never empty on first start.
    trino_host_early = os.environ.get("TRINO_HOST", "localhost")
    trino_port_early = int(os.environ.get("TRINO_PORT", "8080"))
    async with state.pg_pool.acquire() as _conn:
        await _conn.execute(
            """
            INSERT INTO sources (id, type, host, port, database, username, dialect)
            VALUES ('provisa-admin', 'postgresql', $1, $2, $3, $4, 'postgresql')
            ON CONFLICT (id) DO NOTHING
            """,
            pg_host, pg_port, pg_database, pg_user,
        )
        await _conn.execute(
            """
            INSERT INTO sources (id, type, host, port, database, username, dialect)
            VALUES ('provisa-otel', 'iceberg', $1, $2, 'otel', 'provisa', 'trino')
            ON CONFLICT (id) DO NOTHING
            """,
            trino_host_early, trino_port_early,
        )
        await _seed_meta_domain(_conn)
        await _seed_ops_pg(_conn)

    path = Path(config_path)
    if not path.exists():
        return

    with open(path) as f:
        raw_config = yaml.safe_load(f)

    # Resolve server hostname: env var > config file > default (localhost)
    state.server_cfg = raw_config.get("server", {}) if isinstance(raw_config, dict) else {}
    state.hostname = os.environ.get(
        "PROVISA_HOSTNAME",
        state.server_cfg.get("hostname", "localhost"),
    )

    # Resolve query/request limits: env var > provisa.yaml server.limits > hardcoded defaults
    _limits_cfg = state.server_cfg.get("limits", {})
    state.server_limits = {
        "default_row_limit": int(os.environ.get("PROVISA_DEFAULT_ROW_LIMIT", _limits_cfg.get("default_row_limit", 10000))),
        "trino_query_timeout": int(os.environ.get("PROVISA_TRINO_QUERY_TIMEOUT", _limits_cfg.get("trino_query_timeout", 120))),
        "request_timeout": float(os.environ.get("PROVISA_REQUEST_TIMEOUT", _limits_cfg.get("request_timeout", 60))),
    }

    # Connect to Trino
    trino_host = os.environ.get("TRINO_HOST", "localhost")
    trino_port = int(os.environ.get("TRINO_PORT", "8080"))
    state.trino_conn_kwargs = dict(
        host=trino_host,
        port=trino_port,
        user="provisa",
        catalog="postgresql",
        schema="public",
        http_scheme="http",
        request_timeout=10,
    )
    state.trino_conn = trino.dbapi.connect(**state.trino_conn_kwargs)
    from provisa.compiler import schema_service
    schema_service.init(state.trino_conn)

    _seed_ops_trino(state.trino_conn)

    # Create Arrow Flight SQL connection to Trino (separate gRPC port)
    trino_flight_port = int(os.environ.get("TRINO_FLIGHT_PORT", "8480"))
    try:
        from provisa.executor.trino_flight import create_flight_connection
        state.flight_client = create_flight_connection(
            host=trino_host, port=trino_flight_port,
        )
    except Exception:
        import logging
        logging.getLogger(__name__).warning(
            "Arrow Flight SQL unavailable — falling back to REST",
            exc_info=True,
        )

    # Fault-Tolerant Execution (FTE) — env var > server.federation_fte config > disabled
    _fte_cfg = state.server_cfg.get("federation_fte", {})
    _fte_enabled = os.environ.get(
        "TRINO_FTE_ENABLED", str(_fte_cfg.get("enabled", False))
    ).lower() not in ("0", "false", "no")
    if _fte_enabled:
        _retry_policy = os.environ.get(
            "TRINO_FTE_RETRY_POLICY", _fte_cfg.get("retry_policy", "TASK")
        ).upper()
        state.trino_fte_hints = {"retry_policy": _retry_policy}
        for _k, _v in _fte_cfg.items():
            if _k in ("enabled", "retry_policy"):
                continue
            state.trino_fte_hints.setdefault(_k, str(_v))

    # Ensure MinIO results bucket exists (REQ-171)
    from provisa.executor.redirect import RedirectConfig, ensure_results_bucket
    await ensure_results_bucket(RedirectConfig.from_env())

    # Ensure MinIO OTEL bucket exists for otlp2parquet
    try:
        import logging as _lb_log
        import boto3
        from botocore.config import Config as BotoConfig
        _otel_endpoint = os.environ.get("PROVISA_OTEL_S3_ENDPOINT", "http://minio:9000")
        _otel_bucket = os.environ.get("PROVISA_OTEL_BUCKET", "provisa-otel")
        _s3 = boto3.client(
            "s3",
            endpoint_url=_otel_endpoint,
            aws_access_key_id=os.environ.get("PROVISA_OTEL_S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("PROVISA_OTEL_S3_SECRET_KEY", "minioadmin"),
            region_name="us-east-1",
            config=BotoConfig(signature_version="s3v4"),
        )
        existing = [b["Name"] for b in _s3.list_buckets().get("Buckets", [])]
        if _otel_bucket not in existing:
            _s3.create_bucket(Bucket=_otel_bucket)
            _lb_log.getLogger(__name__).info("Created MinIO bucket: %s", _otel_bucket)
    except Exception:
        import logging as _lb_log2
        _lb_log2.getLogger(__name__).warning(
            "Could not ensure OTEL bucket — otlp2parquet storage may fail", exc_info=True
        )

    # Ensure results schema exists for CTAS redirects
    try:
        from provisa.executor.trino_write import ensure_results_schema
        ensure_results_schema(state.trino_conn)
    except Exception:
        import logging
        logging.getLogger(__name__).warning(
            "Could not create results schema — CTAS redirect unavailable",
            exc_info=True,
        )

    # Load Kafka source configs and auto-register tables
    # Each topic config with a discriminator becomes a separate table.
    # The discriminator is injected as a WHERE clause at query time.
    # Column definitions from the topic config define the GraphQL schema.
    # NOTE: This must run BEFORE parse_config_dict / load_config so that
    # Kafka-derived tables are present when relationships are validated.
    from provisa.kafka.window import KafkaTableConfig
    for ks in raw_config.get("kafka_sources", []):
        source_id = ks["id"]
        for topic in ks.get("topics", []):
            # Determine the GraphQL table name:
            # - Use topic config's table_name if set
            # - Otherwise derive from topic config id
            topic_id = topic.get("id", "")
            physical_table = topic.get("topic", "").replace(".", "_").replace("-", "_")
            gql_table_name = topic.get("table_name") or topic_id.replace("-", "_")

            window = topic.get("default_window", "1h")
            disc = topic.get("discriminator")
            disc_field = disc.get("field") if disc else None
            disc_value = disc.get("value") if disc else None

            # Register the Kafka table config for WHERE injection
            state.kafka_table_configs[gql_table_name] = KafkaTableConfig(
                window=window,
                discriminator_field=disc_field,
                discriminator_value=disc_value,
            )

            if window:
                state.kafka_windows[source_id] = window

            # Auto-register as a Provisa table entry
            # Use topic ID as table name (unique per registration),
            # backed by the physical Trino table for introspection
            topic_columns = topic.get("columns", [])
            table_entry = {
                "source_id": source_id,
                "domain_id": topic.get("domain_id", "support"),
                "schema": "default",
                "table": gql_table_name,  # unique name per topic config
                "description": topic.get("description", ""),
                "governance": "pre-approved",
                "columns": [
                    {
                        "name": col.get("name", col) if isinstance(col, dict) else col,
                        "visible_to": col.get("visible_to", ["admin", "analyst"]) if isinstance(col, dict) else ["admin", "analyst"],
                        "writable_by": col.get("writable_by", []) if isinstance(col, dict) else [],
                        "description": col.get("description", "") if isinstance(col, dict) else "",
                    }
                    for col in topic_columns
                ],
            }
            raw_config.setdefault("tables", []).append(table_entry)

            # Map the virtual table name to the physical Trino table
            # so introspection and SQL compilation use the right table
            state.kafka_table_physical = getattr(state, "kafka_table_physical", {})
            state.kafka_table_physical[gql_table_name] = physical_table

    # Store auth config for middleware setup
    state.auth_config = raw_config.get("auth")

    # Load config into PG (and create Trino catalogs)
    config = parse_config_dict(raw_config)
    state.config = config

    # Apply observability config to state
    if config.observability:
        state.otel_compact_cron = config.observability.compact_cron
        state.otel_compact_batch_size = config.observability.compact_batch_size

    # Initialize cache store — REDIS_URL env var overrides config
    cache_config = raw_config.get("cache", {})
    if cache_config.get("enabled"):
        redis_url = os.environ.get("REDIS_URL", "")
        if not redis_url:
            redis_url = resolve_secrets(cache_config.get("redis_url", ""))
        if redis_url:
            state.response_cache_store = RedisCacheStore(redis_url)
        state.response_cache_default_ttl = cache_config.get("default_ttl", 300)

    async with state.pg_pool.acquire() as conn:
        _replace_mode = os.environ.get("PROVISA_CONFIG_REPLACE", "").lower() in ("1", "true", "yes")
        await load_config(config, conn, state.trino_conn, replace=_replace_mode)

    # Seed system source catalogs (not in config.sources, so must be explicit).
    state.source_catalogs["provisa-admin"] = source_to_catalog("provisa-admin")  # provisa_admin
    state.source_catalogs["provisa-otel"] = "otel"

    # Build source metadata and direct connection pools
    from provisa.executor.drivers.registry import has_driver
    for src in config.sources:
        state.source_types[src.id] = src.type.value
        # PostgreSQL sources: database is the PG db name, not the Trino catalog name.
        _pg_cat = source_to_catalog(src.id) if src.type.value == "postgresql" else (src.database or source_to_catalog(src.id))
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
                import logging as _pool_log
                _pool_log.getLogger(__name__).warning(
                    "Direct pool for %r (%s:%s) failed: %s — Trino-routed queries still work.",
                    src.id, resolved_host, src.port, _pool_err,
                )

    # Phase AS: Initialize ingest engines and DDL for ingest sources
    try:
        from provisa.ingest.engine import get_engine as _get_ingest_engine
        from provisa.ingest.ddl import generate_create_table as _gen_ddl
        from provisa.core.secrets import resolve_secrets as _resolve_secrets
        import logging as _logging
        _ingest_log = _logging.getLogger(__name__)
        async with state.pg_pool.acquire() as _pg_conn:
            _ingest_sources = await _pg_conn.fetch(
                "SELECT id, host, port, database, username, dialect FROM sources WHERE type = 'ingest'"
            )
        for _isrc in _ingest_sources:
            _sid = _isrc["id"]
            _pw = _resolve_secrets(None)  # password resolved via secrets provider if needed
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
            # Load tables + columns for this ingest source
            async with state.pg_pool.acquire() as _pg_conn:
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
                _tbl_map.setdefault(_tn, []).append({
                    "column_name": _row["column_name"],
                    "path": _row["path"],
                    "data_type": _row["data_type"],
                })
            state.ingest_tables[_sid] = _tbl_map
            # Run CREATE TABLE IF NOT EXISTS for each ingest table
            for _tn, _cols in _tbl_map.items():
                _ddl = _gen_ddl(_tn, _cols)
                try:
                    async with _eng.begin() as _conn:
                        await _conn.execute(__import__("sqlalchemy").text(_ddl))
                except Exception as _exc:
                    _ingest_log.warning("Ingest DDL failed for %s.%s: %s", _sid, _tn, _exc)
    except Exception:
        import logging as _logging
        _logging.getLogger(__name__).warning("Ingest source init failed", exc_info=True)

    # WebSocket + RSS sources — register for SSE subscription dispatch
    for _src in config.sources:
        if _src.type.value == "websocket":
            state.websocket_sources[_src.id] = _src
        elif _src.type.value == "rss":
            state.rss_sources[_src.id] = _src

    # REQ-221: Fetch enum types from all PostgreSQL sources
    from provisa.compiler.enum_detect import build_enum_types
    _enum_registry: dict[str, list[str]] = {}
    for _src in config.sources:
        if _src.type.value == "postgresql" and state.source_pools.has(_src.id):
            _driver = state.source_pools.get(_src.id)
            if hasattr(_driver, "fetch_enums"):
                try:
                    _reg = await _driver.fetch_enums()
                    _enum_registry.update(_reg)
                except Exception:
                    pass
    state.pg_enum_types = build_enum_types(_enum_registry)

    # Reload OpenAPI specs from DB into state (survives hot reloads and restarts)
    async with state.pg_pool.acquire() as conn:
        openapi_rows = await conn.fetch(
            "SELECT id, path FROM sources WHERE type = 'openapi' AND path IS NOT NULL AND path != ''"
        )
    from provisa.openapi.loader import load_spec
    from provisa.openapi.mapper import parse_spec as _parse_spec
    from provisa.core.secrets import resolve_secrets as _resolve_secrets
    state.openapi_specs = {}
    for _row in openapi_rows:
        try:
            _spec = load_spec(_resolve_secrets(_row["path"]))
            _servers = _spec.get("servers", [])
            _base_url = _servers[0].get("url", "") if _servers else ""
            state.openapi_specs[_row["id"]] = {
                "spec_path": _row["path"],
                "spec": _spec,
                "base_url": _base_url,
                "domain_id": "",
                "auth_config": None,
                "cache_ttl": 300,
            }
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("Failed to reload OpenAPI spec for %s: %s", _row["id"], exc)

    # Load materialized view definitions
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
                governance=sc.get("governance", "pre-approved"),
                columns=sc.get("columns"),
            )
        mv = MVDefinition(
            id=mvc["id"],
            source_tables=mvc.get("source_tables", []),
            target_catalog=mvc.get("target_catalog", "postgresql"),
            target_schema=mvc.get("target_schema", "mv_cache"),
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
                "schema": mvc.get("target_schema", "mv_cache"),
                "table": mv.target_table,
                "governance": sdl_cfg.governance,
                "columns": sdl_cfg.columns or [],
            }
            raw_config.setdefault("tables", []).append(mv_table)

    # Process views — governed computed datasets
    # Each view becomes a registered table (for governance) + MV (for execution)
    views_config = raw_config.get("views", [])
    if views_config:
        import logging
        _view_log = logging.getLogger(__name__)
        for view_cfg in views_config:
            view_id = view_cfg["id"]
            view_sql = view_cfg["sql"]
            materialize = view_cfg.get("materialize", False)
            domain_id = view_cfg.get("domain_id", "default")
            governance = view_cfg.get("governance", "pre-approved")
            description = view_cfg.get("description")
            refresh_interval = view_cfg.get("refresh_interval", 300)

            # Determine the source — views run through Trino, backed by postgresql catalog
            view_source_id = view_cfg.get("source_id", "postgresql")
            view_table_name = f"view_{view_id.replace('-', '_')}"
            view_schema = "mv_cache" if materialize else "public"

            # Register the view as a table entry in the YAML tables list
            # so it gets picked up by the normal table loading pipeline
            view_table = {
                "source_id": view_source_id,
                "domain_id": domain_id,
                "schema": view_schema,
                "table": view_table_name,
                "governance": governance,
                "description": description,
                "alias": view_cfg.get("alias"),
                "columns": view_cfg.get("columns", []),
            }
            # Append to the tables list so it's processed during schema build
            raw_config.setdefault("tables", []).append(view_table)

            if materialize:
                # Create a materialized MV
                mv = MVDefinition(
                    id=f"view-{view_id}",
                    source_tables=[],  # SQL defines its own sources
                    target_catalog="postgresql",
                    target_schema="mv_cache",
                    target_table=view_table_name,
                    refresh_interval=refresh_interval,
                    enabled=True,
                    sql=view_sql,
                    expose_in_sdl=False,  # Exposed via the registered table instead
                )
                state.mv_registry.register(mv)
                _view_log.info("Registered materialized view: %s", view_id)
            else:
                # Store semantic SQL; compiled to Trino-physical after contexts are built
                state.view_sql_map[view_table_name] = view_sql.strip()
                _view_log.info("Registered inline view: %s", view_id)

    # Auto-generate MVs from cross-source relationships with materialize=true
    _table_source_map: dict[str, str] = {}  # table_name → source_id
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

        # Only auto-materialize cross-source relationships
        if src_source and tgt_source and src_source != tgt_source:
            mv_id = f"auto-mv-{rel_cfg['id']}"
            if state.mv_registry.get(mv_id) is not None:
                continue  # Already registered (e.g., from explicit MV config)

            # Determine left/right based on cardinality
            # many-to-one: source has FK → source is left, target is right
            # one-to-many: source is parent → source is left, target is right
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
                target_schema="mv_cache",
                refresh_interval=rel_cfg.get("refresh_interval", 300),
                enabled=True,
                join_pattern=jp,
            )
            state.mv_registry.register(mv)
            import logging
            logging.getLogger(__name__).info(
                "Auto-materialized cross-source relationship %s (%s.%s → %s.%s)",
                rel_cfg["id"], src_source, src_table, tgt_source, tgt_table,
            )

    await _load_graphql_remote_sources_from_db()

    await _rebuild_schemas(raw_config)

    # Initialize hot tables (Phase AD6)
    from provisa.cache.hot_tables import init_hot_tables
    hot_mgr = await init_hot_tables(raw_config, state.trino_conn)
    if hot_mgr is not None:
        state.hot_manager = hot_mgr


def _filter_tables_by_schema_cfg(
    tables: list[dict],
    schema_cfg: dict,
    source_allowed_domains: dict[str, list[str]],
) -> list[dict]:
    """Filter registered tables based on schema visibility config and source domain restrictions."""
    if not schema_cfg.get("include_ops", True):
        tables = [t for t in tables if t.get("domain_id") != "ops"]
    elif not schema_cfg.get("include_metrics", True):
        tables = [t for t in tables if not (
            t.get("domain_id") == "ops" and t.get("table_name") == "metrics"
        )]

    if source_allowed_domains:
        tables = [
            t for t in tables
            if not source_allowed_domains.get(t["source_id"])
            or t.get("domain_id", "") in source_allowed_domains[t["source_id"]]
        ]

    return tables


async def _load_graphql_remote_sources_from_db() -> None:
    """Load persisted graphql_remote sources from DB into state.graphql_remote_sources."""
    if state.pg_pool is None:
        log.warning("[GQL REMOTE] pg_pool is None — skipping DB load")
        return
    try:
        async with state.pg_pool.acquire() as _conn:
            src_rows = await _conn.fetch("SELECT id, path FROM sources WHERE type = 'graphql_remote'")
            for src in src_rows:
                source_id = src["id"]
                url = src["path"] or ""
                if source_id in getattr(state, "graphql_remote_sources", {}):
                    continue
                tbl_rows = await _conn.fetch(
                    "SELECT id, table_name, domain_id, description FROM registered_tables "
                    "WHERE source_id = $1 AND schema_name = 'graphql_remote'",
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
                    import json as _json
                    for cr in col_rows:
                        if cr["native_filter_type"] == "query_param":
                            required_args.append({"name": cr["column_name"], "gql_type": "String", "provisa_type": "text"})
                            continue
                        col_dict: dict = {"name": cr["column_name"], "type": cr["data_type"] or "text"}
                        raw_of = cr["object_fields"]
                        if raw_of:
                            try:
                                col_dict["gql_object_fields"] = _json.loads(raw_of) if isinstance(raw_of, str) else raw_of
                            except Exception:
                                pass
                        columns.append(col_dict)
                    tname = tr["table_name"]
                    tables.append({
                        "name": tname,
                        "field_name": tname.split("__", 1)[-1] if "__" in tname else tname,
                        "source_id": source_id,
                        "columns": columns,
                        "domain_id": tr["domain_id"] or "",
                        "description": tr["description"],
                        "required_args": required_args,
                    })
                if not tables:
                    continue
                namespace = tables[0]["name"].split("__", 1)[0] if "__" in tables[0]["name"] else ""
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
                log.warning("[GQL REMOTE] Loaded source %s from DB (%d tables)", source_id, len(tables))
    except Exception:
        log.warning("Failed to load graphql_remote sources from DB", exc_info=True)


async def _rebuild_schemas(raw_config: dict | None = None) -> None:
    """Re-introspect Trino and rebuild schemas for all roles from current DB state.

    Called after _load_and_build() during startup, and independently after
    admin mutations that change tables/relationships/roles.
    """
    if state.pg_pool is None or state.trino_conn is None:
        return

    kafka_physical = getattr(state, "kafka_table_physical", {})
    domain_prefix = False
    if raw_config:
        domain_prefix = raw_config.get("naming", {}).get("domain_prefix", False)
        if raw_config.get("naming", {}).get("convention"):
            state.global_naming_convention = raw_config["naming"]["convention"]
    else:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                raw_config = yaml.safe_load(f)
            domain_prefix = raw_config.get("naming", {}).get("domain_prefix", False)
            if raw_config.get("naming", {}).get("convention"):
                state.global_naming_convention = raw_config["naming"]["convention"]

    # Clear mutable state before rebuild
    state.masking_rules = {}

    async with state.pg_pool.acquire() as conn:
        tables = await _fetch_tables(conn)
        relationships = await _fetch_relationships(conn)

        # Apply schema visibility filters (schema.include_ops / schema.include_metrics)
        tables = _filter_tables_by_schema_cfg(
            tables,
            raw_config.get("schema", {}) if raw_config else {},
            state.source_allowed_domains,
        )

        # Install LISTEN/NOTIFY triggers on pre-approved PostgreSQL tables
        from provisa.subscriptions.pg_triggers import ensure_pg_notify_triggers
        state.pg_notify_tables = await ensure_pg_notify_triggers(conn, tables, state.source_types)
        state.table_watermarks = {
            tbl["table_name"]: tbl["watermark_column"]
            for tbl in tables
            if tbl.get("watermark_column")
        }
        naming_rules = [
            dict(r) for r in await conn.fetch(
                "SELECT pattern, replacement FROM naming_rules"
            )
        ]

        # Load per-table cache TTLs
        cache_rows = await conn.fetch(
            "SELECT id, cache_ttl FROM registered_tables WHERE cache_ttl IS NOT NULL"
        )
        state.table_cache = {r["id"]: r["cache_ttl"] for r in cache_rows}
        domains = [
            dict(r) for r in await conn.fetch("SELECT id, description FROM domains")
        ]
        sources = {
            r["id"]: dict(r) for r in await conn.fetch("SELECT * FROM sources")
        }
        # PostgreSQL sources store database=<pg_db_name>, but Trino accesses them via
        # a catalog named source_to_catalog(source_id). Patch all postgresql-type sources
        # so introspect_tables uses the right Trino catalog name.
        # Use the DB-fetched type field (not state.source_types) so system sources like
        # provisa-admin (not in config.sources) are also patched.
        for _sid, _src_dict in list(sources.items()):
            if _src_dict.get("type") == "postgresql":
                sources[_sid] = {**_src_dict, "database": source_to_catalog(_sid)}
        roles = [
            dict(r) for r in await conn.fetch(
                "SELECT id, capabilities, domain_access FROM roles"
            )
        ]

        # Inject source-level naming convention into table dicts for hierarchical resolution
        # Also merge PG-stored allowed_domains into state (PG overrides config-file values).
        for src_id, src_row in sources.items():
            pg_domains = list(src_row.get("allowed_domains") or [])
            if pg_domains:
                state.source_allowed_domains[src_id] = pg_domains
        for tbl in tables:
            src = sources.get(tbl["source_id"], {})
            tbl["source_naming_convention"] = src.get("naming_convention")

        # Ensure ops Iceberg tables exist before introspection — idempotent, self-healing
        # if _seed_ops_trino failed at startup because the otel catalog wasn't ready yet.
        _seed_ops_trino(state.trino_conn)

        # Introspect Trino metadata
        kafka_physical = getattr(state, "kafka_table_physical", {})
        column_types = introspect_tables(state.trino_conn, tables, sources, {**_META_TABLE_ALIAS, **(kafka_physical or {})})
        col_types_converted: dict[int, list[ColumnMetadata]] = column_types

        # Inject required GQL args as native filter columns for graphql_remote tables.
        # This lets the Cypher translator rewrite WHERE n.id → WHERE n._nf_id so the
        # GQL executor can pick them up as variables (e.g. employee(id: $id)).
        _gql_remote_srcs = getattr(state, "graphql_remote_sources", {})
        if _gql_remote_srcs:
            _gql_req_args: dict[tuple, list[dict]] = {}
            for _reg in _gql_remote_srcs.values():
                _sid = _reg.get("source_id", "")
                for _tbl in _reg.get("tables", []):
                    _req = _tbl.get("required_args", [])
                    if _req:
                        _gql_req_args[(_sid, _tbl["name"])] = _req
            if _gql_req_args:
                for _tbl in tables:
                    _key = (_tbl["source_id"], _tbl["table_name"])
                    _req = _gql_req_args.get(_key, [])
                    for _arg in _req:
                        _tbl.setdefault("columns", [])
                        _tbl["columns"].append({
                            "name": _arg["name"],
                            "column_name": _arg["name"],
                            "visible_to": [],
                            "native_filter_type": "query_param",
                        })

        # Build gql_object_columns: {table_name: {col_name: [sub_field_names]}} for JSON extraction
        _gql_object_cols: dict[str, dict[str, list[str]]] = {}
        for _reg in _gql_remote_srcs.values():
            for _tbl in _reg.get("tables", []):
                _tbl_obj: dict[str, list[str]] = {}
                for _col in _tbl.get("columns", []):
                    _sub = _col.get("gql_object_fields")
                    if _sub:
                        _tbl_obj[_col["name"]] = _sub
                if _tbl_obj:
                    _gql_object_cols[_tbl["name"]] = _tbl_obj

        # Synthesize ColumnMetadata for ops views when Trino introspection returns empty.
        # Iceberg JDBC information_schema.columns does not expose view columns — physical
        # tables (traces/logs/metrics) introspect correctly; views need static synthesis.
        _ops_view_cols: dict[str, list[ColumnMetadata]] = {
            view_name: [
                ColumnMetadata(
                    column_name=col_name,
                    data_type=_OPS_PG_TO_TRINO.get(pg_type, "VARCHAR").lower(),
                    is_nullable=not is_pk,
                )
                for col_name, pg_type, is_pk in cols
            ]
            for view_name, cols, _ in _OPS_VIEWS
        }
        for _tbl in tables:
            if _tbl["source_id"] != "provisa-otel":
                continue
            _vname = _tbl["table_name"]
            if _vname not in _ops_view_cols:
                continue
            _tid = _tbl["id"]
            if not col_types_converted.get(_tid):
                col_types_converted[_tid] = _ops_view_cols[_vname]

        # Synthesize ColumnMetadata for graphql_remote tables (no Trino catalog)
        if _gql_remote_srcs:
            _provisa_to_trino = {
                "text": "varchar", "integer": "integer",
                "numeric": "double", "boolean": "boolean", "jsonb": "json",
            }
            _tbl_lookup = {(t["source_id"], t["table_name"]): t["id"] for t in tables}
            for _reg in _gql_remote_srcs.values():
                _sid = _reg.get("source_id", "")
                for _tbl in _reg.get("tables", []):
                    _tid = _tbl_lookup.get((_sid, _tbl["name"]))
                    if _tid is not None and _tid not in col_types_converted:
                        col_types_converted[_tid] = [
                            ColumnMetadata(
                                column_name=c["name"],
                                data_type=_provisa_to_trino.get(c.get("type", "text"), "varchar"),
                                is_nullable=True,
                            )
                            for c in _tbl.get("columns", [])
                        ]

        # Load API sources and endpoints (Phase U)
        from provisa.api_source.loader import load_api_sources
        state.api_endpoints, state.api_sources = await load_api_sources(
            conn, tables, col_types_converted, roles, state.source_types,
        )

        # Background hydration for zero-param API endpoints (no path params → full collection known at startup)
        _zero_param_eps = [
            (ep, state.api_sources[ep.source_id])
            for ep in state.api_endpoints.values()
            if "{" not in ep.path and ep.source_id in state.api_sources
        ]
        if _zero_param_eps:
            import logging as _hydrate_logging
            _hydrate_log = _hydrate_logging.getLogger(__name__)
            async def _bg_hydrate(eps=_zero_param_eps, pool=state.pg_pool, _log=_hydrate_log):
                from provisa.openapi.pg_cache import fill_api_table
                async with pool.acquire() as _conn:
                    for _ep, _src in eps:
                        try:
                            await fill_api_table(
                                _src.base_url, _ep.path, _ep.default_params, _conn,
                                "default", _ep.table_name, _ep.ttl,
                                _ep.response_root, _ep.error_path, _ep.pk_column,
                            )
                        except Exception as _e:
                            _log.warning("BG hydration failed for %s: %s", _ep.table_name, _e)
            asyncio.create_task(_bg_hydrate())

        # Load RLS rules
        rls_rules = [
            dict(r) for r in await conn.fetch(
                "SELECT table_id, role_id, filter_expr FROM rls_rules"
            )
        ]

        # Load masking rules from table_columns (inline masking)
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
            # Build per-role masking entries for all roles NOT in unmasked_to
            for role in roles:
                if role["id"] in unmasked_to:
                    continue
                key = (table_id, role["id"])
                if key not in state.masking_rules:
                    state.masking_rules[key] = {}
                state.masking_rules[key][col_name] = (mask_rule, data_type)

        # Load tracked functions and webhooks for action schema fields
        from provisa.api.admin.actions_router import _ensure_tables
        await _ensure_tables(state.pg_pool)
        fn_rows = await conn.fetch("SELECT * FROM tracked_functions ORDER BY name")
        wh_rows = await conn.fetch("SELECT * FROM tracked_webhooks ORDER BY name")
        import json as _json
        tracked_functions = [
            {
                **dict(r),
                "arguments": _json.loads(r["arguments"]) if isinstance(r["arguments"], str) else (r["arguments"] or []),
                "visible_to": list(r["visible_to"] or []),
            }
            for r in fn_rows
        ]
        tracked_webhooks = [
            {
                **dict(r),
                "arguments": _json.loads(r["arguments"]) if isinstance(r["arguments"], str) else (r["arguments"] or []),
                "inline_return_type": _json.loads(r["inline_return_type"]) if isinstance(r["inline_return_type"], str) else (r["inline_return_type"] or []),
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

        approved_query_rows = await conn.fetch(
            "SELECT id, stable_id, query_text, target_tables, business_purpose "
            "FROM persisted_queries WHERE status = 'approved' AND stable_id IS NOT NULL"
        )
        approved_queries = [dict(r) for r in approved_query_rows]

        for role in roles:
            state.roles[role["id"]] = role
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
                naming_convention=state.global_naming_convention,
                functions=tracked_functions,
                webhooks=tracked_webhooks,
                enum_types=state.pg_enum_types,
                approved_queries=approved_queries,
                gql_object_columns=_gql_object_cols,
            )
            try:
                state.schemas[role["id"]] = generate_schema(si)
                state.contexts[role["id"]] = build_context(si)
                state.rls_contexts[role["id"]] = build_rls_context(
                    rls_rules, role["id"],
                )
            except ValueError as _schema_err:
                import logging as _log
                _log.getLogger(__name__).error(
                    "generate_schema failed for role %r: %s", role["id"], _schema_err
                )

            # Generate proto for this role
            try:
                from provisa.grpc.proto_gen import generate_proto
                state.proto_files[role["id"]] = generate_proto(si)
            except ValueError:
                pass

    # Cache raw build data for on-demand domain-filtered schema generation
    state.schema_build_cache = {
        "tables": tables,
        "relationships": relationships,
        "column_types": col_types_converted,
        "naming_rules": naming_rules,
        "domains": domains,
        "domain_prefix": domain_prefix,
        "naming_convention": state.global_naming_convention,
        "functions": tracked_functions,
        "webhooks": tracked_webhooks,
        "enum_types": state.pg_enum_types,
        "approved_queries": approved_queries,
        "physical_table_map": {**_META_TABLE_ALIAS, **(kafka_physical or {})},
    }

    # Compile inline view SQLs now that a context is available
    if state.view_sql_map and state.contexts:
        from provisa.compiler.sql_gen import rewrite_semantic_to_trino_physical
        ctx = next(iter(state.contexts.values()))
        state.view_sql_map = {
            name: rewrite_semantic_to_trino_physical(sql, ctx)
            for name, sql in state.view_sql_map.items()
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    """App lifespan: load config and build schemas at startup."""
    import logging
    _log = logging.getLogger(__name__)
    try:
        await _load_and_build()
    except Exception:
        _log.exception("Startup failed during _load_and_build")
        raise

    # Start MV refresh background task
    if state.mv_registry.get_enabled() and state.trino_conn:
        from provisa.mv.refresh import refresh_loop
        state._mv_refresh_task = asyncio.create_task(
            refresh_loop(state.trino_conn, state.mv_registry),
        )

    # Start warm-table background task (REQ-AD5)
    if state.trino_conn:
        from provisa.cache.warm_tables import QueryCounter as _QC
        from provisa.compiler.sql_gen import query_counter as _qc

        async def _warm_loop() -> None:
            while True:
                try:
                    state.warm_manager.check_promotions(_qc, state.trino_conn)
                    state.warm_manager.check_demotions(_qc, state.trino_conn)
                except Exception:
                    _log.exception("Error in warm-table loop")
                await asyncio.sleep(60)

        state._warm_task = asyncio.create_task(_warm_loop())

    # Start hot-table periodic refresh (Phase AD6)
    if state.hot_manager is not None and state.trino_conn:
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
                            state.trino_conn,
                            entry.table_name,
                            entry.schema,
                            entry.catalog,
                            entry.pk_column,
                        )
                    except Exception:
                        _log.exception("Hot table refresh failed: %s", entry.table_name)

        state._hot_refresh_task = asyncio.create_task(_hot_refresh_loop())

    # Start SQLite staleness check loop
    _sqlite_check_interval = 60

    async def _sqlite_stale_loop() -> None:
        from provisa.file_source.pg_migrate import migrate_if_stale
        while True:
            await asyncio.sleep(_sqlite_check_interval)
            try:
                async with state.pg_pool.acquire() as conn:
                    rows = await conn.fetch(
                        """SELECT rt.id, rt.table_name, rt.schema_name, s.path
                           FROM registered_tables rt
                           JOIN sources s ON s.id = rt.source_id
                           WHERE s.type = 'sqlite' AND s.path IS NOT NULL"""
                    )
                    for r in rows:
                        try:
                            migrated = await migrate_if_stale(
                                r["id"], r["path"], r["table_name"],
                                conn, r["schema_name"], r["table_name"],
                            )
                            if migrated:
                                _log.info("SQLite stale: re-migrated table %d (%s)", r["id"], r["table_name"])
                        except Exception:
                            _log.exception("SQLite stale check failed for table %d", r["id"])
            except Exception:
                _log.exception("SQLite staleness loop error")

    state._sqlite_stale_task = asyncio.create_task(_sqlite_stale_loop())

    # Start gRPC server if protos were generated
    if state.proto_files:
        try:
            import tempfile
            from provisa.grpc.schema_gen import compile_proto
            from provisa.grpc.server import start_grpc_server

            # Use the first role's proto to compile stubs (service is the same)
            first_proto = next(iter(state.proto_files.values()))
            grpc_output_dir = tempfile.mkdtemp(prefix="provisa_grpc_")
            pb2_path, pb2_grpc_path = compile_proto(first_proto, grpc_output_dir)
            grpc_port = int(os.environ.get("GRPC_PORT", str(state.server_cfg.get("grpc_port", 50051))))
            state._grpc_server = await start_grpc_server(
                grpc_port, state, pb2_path, pb2_grpc_path,
            )
            _log.info("gRPC server listening on %s:%d", state.hostname, grpc_port)
        except Exception:
            import logging
            logging.getLogger(__name__).exception("gRPC server startup failed")

    # Start Arrow Flight server
    try:
        from provisa.api.flight.server import ProvisaFlightServer
        flight_port = int(os.environ.get("FLIGHT_PORT", str(state.server_cfg.get("flight_port", 8815))))
        flight_server = ProvisaFlightServer(
            state, location=f"grpc://0.0.0.0:{flight_port}",
            main_loop=asyncio.get_running_loop(),
        )
        import threading
        flight_thread = threading.Thread(
            target=flight_server.serve, daemon=True,
        )
        flight_thread.start()
        state._flight_server = flight_server
        _log.info("Arrow Flight server listening on %s:%d", state.hostname, flight_port)
    except Exception:
        _log.exception("Arrow Flight server startup failed")

    # Start Live Query Engine (Phase AM)
    try:
        from provisa.live.engine import LiveEngine
        live_engine = LiveEngine(pg_pool=state.pg_pool)
        await live_engine.start()
        state.live_engine = live_engine
        _log.info("Live Query Engine started")
    except Exception:
        _log.exception("Live Query Engine startup failed")

    # Initialize APQ cache (Phase AN)
    redis_url = os.environ.get("REDIS_URL")
    if redis_url:
        try:
            from provisa.apq.cache import RedisAPQCache
            state.apq_cache = RedisAPQCache(redis_url)
            _log.info("APQ cache initialized (Redis: %s)", redis_url)
        except Exception:
            _log.exception("APQ cache initialization failed")

    # Start scheduler for config-based triggers and approved query schedules (Phase AX)
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger
        scheduler = AsyncIOScheduler()
        # Register config-based triggers if present
        _cfg_triggers = []
        try:
            _raw = yaml.safe_load(open(os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")).read())
            if isinstance(_raw, dict):
                from provisa.core.config_loader import parse_config_dict
                _cfg = parse_config_dict(_raw)
                _cfg_triggers = _cfg.scheduler.triggers if _cfg.scheduler else []
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
        # OTEL compaction: Parquet → Iceberg on configured schedule
        from provisa.scheduler.jobs import compact_otel_signals
        scheduler.add_job(
            compact_otel_signals,
            trigger=CronTrigger.from_crontab(state.otel_compact_cron),
            id="otel_compact",
            name="otel:compact_signals",
            replace_existing=True,
        )

        scheduler.start()
        state._scheduler = scheduler
        _log.info("APScheduler started")
    except Exception:
        _log.exception("APScheduler startup failed")

    # Auto-register graphql-demo source if GRAPHQL_DEMO_URL is set (or we're in docker-compose)
    _graphql_demo_url = os.environ.get(
        "GRAPHQL_DEMO_URL", "http://graphql-demo:4000/graphql"
    )
    if os.environ.get("GRAPHQL_DEMO_ENABLED", "").lower() in ("1", "true", "yes") or os.environ.get("GRAPHQL_DEMO_URL"):
        async def _register_graphql_demo() -> None:
            from provisa.api.admin.graphql_remote_router import (
                _introspect_and_map,
                _upsert_tables_to_semantic_layer,
                GraphQLRemoteRegistration,
            )
            try:
                tables, functions, relationships = await _introspect_and_map(
                    "graphql-demo", _graphql_demo_url, "shelter", "shelter", None,
                )
                reg = GraphQLRemoteRegistration(
                    source_id="graphql-demo",
                    url=_graphql_demo_url,
                    namespace="shelter",
                    domain_id="shelter",
                    cache_ttl=300,
                    tables=tables,
                    functions=functions,
                    relationships=relationships,
                )
                if not hasattr(state, "graphql_remote_sources"):
                    state.graphql_remote_sources = {}
                state.graphql_remote_sources["graphql-demo"] = reg.model_dump()
                if getattr(state, "pg_pool", None) is not None:
                    async with state.pg_pool.acquire() as _conn:
                        # Ensure source row exists before registering tables (FK constraint)
                        await _conn.execute(
                            """
                            INSERT INTO sources (id, type, host, port, database, username, dialect, path)
                            VALUES ('graphql-demo', 'graphql_remote', '', 0, '', '', '', $1)
                            ON CONFLICT (id) DO UPDATE SET path = EXCLUDED.path
                            """,
                            _graphql_demo_url,
                        )
                        # Ensure shelter domain exists (FK constraint on registered_tables.domain_id)
                        await _conn.execute(
                            "INSERT INTO domains (id, description) VALUES ('shelter', 'Animal shelter staff and breed management') ON CONFLICT (id) DO NOTHING",
                        )
                    await _upsert_tables_to_semantic_layer(
                        "graphql-demo", "shelter", tables, state.pg_pool,
                    )
                    from provisa.api.admin.graphql_remote_router import _upsert_relationships_to_semantic_layer
                    await _upsert_relationships_to_semantic_layer(relationships, state.pg_pool, state)
                    from provisa.core.models import Cardinality, Relationship
                    from provisa.core.repositories import relationship as rel_repo
                    async with state.pg_pool.acquire() as _rel_conn:
                        try:
                            await rel_repo.upsert(_rel_conn, Relationship(
                                id="employees_to_assignments",
                                source_table_id="shelter__employees",
                                target_table_id="shelter__assignments",
                                source_column="id",
                                target_column="employeeId",
                                cardinality=Cardinality("one-to-many"),
                            ))
                        except Exception:
                            _log.warning("Failed to upsert employees_to_assignments", exc_info=True)
                _log.info(
                    "Auto-registered graphql-demo source (%d tables, %d functions)",
                    len(tables), len(functions),
                )
                await _rebuild_schemas()
            except Exception:
                _log.warning("graphql-demo auto-registration failed (service may not be up yet)", exc_info=True)

        asyncio.create_task(_register_graphql_demo())

    yield

    # Stop Arrow Flight server
    if state._flight_server:
        state._flight_server.shutdown()

    # Stop gRPC server
    if state._grpc_server:
        await state._grpc_server.stop(grace=5)

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

    await state.response_cache_store.close()
    await state.source_pools.close_all()
    if state.pg_pool:
        await state.pg_pool.close()
    if state.flight_client:
        state.flight_client.close()
    if state.trino_conn:
        state.trino_conn.close()


def create_app() -> FastAPI:
    """Create the FastAPI application."""
    from fastapi.middleware.cors import CORSMiddleware
    from strawberry.fastapi import GraphQLRouter

    from provisa.api.admin.schema import admin_schema

    app = FastAPI(title="Provisa", lifespan=lifespan)
    _setup_otel(app)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    import traceback as _tb
    from fastapi import Request as _Request
    from fastapi.responses import JSONResponse as _JSONResponse
    from fastapi.exception_handlers import http_exception_handler as _http_exc_handler

    @app.exception_handler(Exception)
    async def _global_exception_handler(_req: _Request, exc: Exception):
        log.exception("Unhandled exception on %s %s", _req.method, _req.url.path)
        return _JSONResponse(
            status_code=500,
            content={"detail": "Internal server error", "type": type(exc).__name__},
        )

    @app.exception_handler(asyncio.TimeoutError)
    async def _timeout_handler(_req: _Request, exc: asyncio.TimeoutError):
        log.error("Request timeout on %s %s", _req.method, _req.url.path)
        return _JSONResponse(status_code=504, content={"detail": "Request timed out"})

    # Conditionally add auth middleware and routes
    from provisa.auth.wiring import wire_auth
    wire_auth(app, state.auth_config)

    app.include_router(data_router)
    app.include_router(dev_router)
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
    admin_router = GraphQLRouter(admin_schema)
    app.include_router(admin_router, prefix="/admin/graphql")

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

    from provisa.api.admin.views import router as views_router
    app.include_router(views_router)

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

    @app.api_route("/health", methods=["GET", "HEAD"])
    async def health():
        return {"status": "ok"}

    return app
