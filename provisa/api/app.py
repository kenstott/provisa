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
import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import trino
import yaml
from fastapi import FastAPI

from provisa.api.data.endpoint import router as data_router
from provisa.api.data.endpoint_dev import router as dev_router
from provisa.api.data.sdl import router as sdl_router
from provisa.compiler.introspect import ColumnMetadata, introspect_tables
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.rls import RLSContext, build_rls_context
from provisa.compiler.sql_gen import CompilationContext, build_context
from provisa.core.config_loader import load_config, parse_config_dict
from provisa.core.db import create_pool, init_schema
from provisa.core.secrets import resolve_secrets
from provisa.executor.pool import SourcePool
from provisa.compiler.mask_inject import MaskingRules
from provisa.cache.store import CacheStore, NoopCacheStore, RedisCacheStore
from provisa.mv.registry import MVRegistry
from provisa.cache.warm_tables import WarmTableManager
from provisa.apq.cache import APQCache, NoopAPQCache


class AppState:
    """Shared application state populated at startup."""

    pg_pool: asyncpg.Pool | None = None
    trino_conn: trino.dbapi.Connection | None = None
    flight_client: object | None = None  # pyarrow.flight.FlightClient
    schemas: dict[str, object] = {}  # role_id → GraphQLSchema
    contexts: dict[str, CompilationContext] = {}  # role_id → CompilationContext
    rls_contexts: dict[str, RLSContext] = {}  # role_id → RLSContext
    roles: dict[str, dict] = {}  # role_id → role dict
    source_pools: SourcePool = SourcePool()
    source_types: dict[str, str] = {}  # source_id → source_type
    source_dialects: dict[str, str] = {}  # source_id → sqlglot dialect
    masking_rules: MaskingRules = {}  # (table_id, role_id) → {col: (rule, dtype)}
    cache_store: CacheStore = NoopCacheStore()
    cache_default_ttl: int = 300
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
    server_cfg: dict = {}  # raw server section from provisa.yaml


state = AppState()


def _parse_mask_value(raw: str | None) -> object:
    """Parse a stored mask value string back to a Python value."""
    if raw is None:
        return None
    if raw == "None":
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


async def _load_and_build(config_path: str | None = None) -> None:
    """Load config, introspect Trino, build schemas for all roles."""
    if config_path is None:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")

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

    # Connect to PG
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))
    pg_database = os.environ.get("PG_DATABASE", "provisa")
    pg_user = os.environ.get("PG_USER", "provisa")
    pg_password = os.environ.get("PG_PASSWORD", "provisa")

    state.pg_pool = await create_pool(
        pg_host, pg_port, pg_database, pg_user, pg_password,
    )

    # Init schema
    schema_sql_path = Path(__file__).parent.parent / "core" / "schema.sql"
    if schema_sql_path.exists():
        schema_sql = schema_sql_path.read_text()
        await init_schema(state.pg_pool, schema_sql)

    # Connect to Trino
    trino_host = os.environ.get("TRINO_HOST", "localhost")
    trino_port = int(os.environ.get("TRINO_PORT", "8080"))
    state.trino_conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user="provisa",
        catalog="postgresql",
        schema="public",
        http_scheme="http",
        request_timeout=10,
    )

    # Create Arrow Flight SQL connection to Trino (separate gRPC port)
    trino_flight_port = int(os.environ.get("TRINO_FLIGHT_PORT", "8480"))
    try:
        from provisa.executor.trino_flight import create_flight_connection
        state.flight_client = create_flight_connection(
            host=trino_host, port=trino_flight_port,
        )
        import logging
        logging.getLogger(__name__).info(
            "Arrow Flight SQL connected to %s:%d", trino_host, trino_flight_port,
        )
    except Exception:
        import logging
        logging.getLogger(__name__).warning(
            "Arrow Flight SQL unavailable — falling back to REST",
            exc_info=True,
        )

    # Ensure MinIO results bucket exists (REQ-171)
    from provisa.executor.redirect import RedirectConfig, ensure_results_bucket
    await ensure_results_bucket(RedirectConfig.from_env())

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

    # Initialize cache store — REDIS_URL env var overrides config
    cache_config = raw_config.get("cache", {})
    if cache_config.get("enabled"):
        redis_url = os.environ.get("REDIS_URL", "")
        if not redis_url:
            redis_url = cache_config.get("redis_url", "")
            if redis_url.startswith("${env:"):
                env_key = redis_url[6:-1]
                redis_url = os.environ.get(env_key, "")
        if redis_url:
            state.cache_store = RedisCacheStore(redis_url)
        state.cache_default_ttl = cache_config.get("default_ttl", 300)

    async with state.pg_pool.acquire() as conn:
        await load_config(config, conn, state.trino_conn)

    # Build source metadata and direct connection pools
    from provisa.executor.drivers.registry import has_driver
    for src in config.sources:
        state.source_types[src.id] = src.type.value
        state.source_dialects[src.id] = src.dialect or ""
        state.source_cache[src.id] = {
            "cache_enabled": src.cache_enabled,
            "cache_ttl": src.cache_ttl,
        }
        if src.federation_hints:
            state.source_federation_hints[src.id] = dict(src.federation_hints)
        if has_driver(src.type.value):
            resolved_pw = resolve_secrets(src.password)
            await state.source_pools.add(
                source_id=src.id,
                source_type=src.type.value,
                host=src.host if src.host != "postgres" else os.environ.get("PG_HOST", "localhost"),
                port=src.port,
                database=src.database,
                user=src.username,
                password=resolved_pw,
                min_size=src.pool_min,
                max_size=src.pool_max,
                use_pgbouncer=src.use_pgbouncer,
                pgbouncer_port=src.pgbouncer_port,
            )

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
                # Store SQL for inline expansion at query time — no DDL needed
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

    await _rebuild_schemas(raw_config)

    # Initialize hot tables (Phase AD6)
    from provisa.cache.hot_tables import init_hot_tables
    hot_mgr = await init_hot_tables(raw_config, state.trino_conn)
    if hot_mgr is not None:
        state.hot_manager = hot_mgr


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
    else:
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        if path.exists():
            with open(path) as f:
                raw_config = yaml.safe_load(f)
            domain_prefix = raw_config.get("naming", {}).get("domain_prefix", False)

    # Clear mutable state before rebuild
    state.masking_rules = {}

    async with state.pg_pool.acquire() as conn:
        tables = await _fetch_tables(conn)
        relationships = await _fetch_relationships(conn)
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
        roles = [
            dict(r) for r in await conn.fetch(
                "SELECT id, capabilities, domain_access FROM roles"
            )
        ]

        # Inject source-level naming convention into table dicts for hierarchical resolution
        for tbl in tables:
            src = sources.get(tbl["source_id"], {})
            tbl["source_naming_convention"] = src.get("naming_convention")

        # Introspect Trino metadata
        kafka_physical = getattr(state, "kafka_table_physical", {})
        column_types = introspect_tables(state.trino_conn, tables, sources, kafka_physical)
        col_types_converted: dict[int, list[ColumnMetadata]] = column_types

        # Load API sources and endpoints (Phase U)
        from provisa.api_source.loader import load_api_sources
        state.api_endpoints, state.api_sources = await load_api_sources(
            conn, tables, col_types_converted, roles, state.source_types,
        )

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
                domain_prefix=domain_prefix,
                physical_table_map=kafka_physical or None,
                naming_convention=raw_config.get("naming", {}).get("convention", "snake_case") if raw_config else "snake_case",
            )
            try:
                state.schemas[role["id"]] = generate_schema(si)
                state.contexts[role["id"]] = build_context(si)
                state.rls_contexts[role["id"]] = build_rls_context(
                    rls_rules, role["id"],
                )
            except ValueError:
                # Role has no visible tables — skip
                pass

            # Generate proto for this role
            try:
                from provisa.grpc.proto_gen import generate_proto
                state.proto_files[role["id"]] = generate_proto(si)
            except ValueError:
                pass


async def _fetch_tables(conn: asyncpg.Connection) -> list[dict]:
    """Fetch registered tables with columns."""
    rows = await conn.fetch(
        "SELECT id, source_id, domain_id, schema_name, table_name, governance, "
        "alias, description "
        "FROM registered_tables ORDER BY id"
    )
    tables = []
    for row in rows:
        table = dict(row)
        col_rows = await conn.fetch(
            "SELECT column_name, visible_to, writable_by, unmasked_to, "
            "mask_type, alias, description, path "
            "FROM table_columns WHERE table_id = $1 ORDER BY id",
            row["id"],
        )
        table["columns"] = [
            {
                "column_name": r["column_name"],
                "visible_to": list(r["visible_to"]),
                "writable_by": list(r.get("writable_by") or []),
                "unmasked_to": list(r.get("unmasked_to") or []),
                "mask_type": r.get("mask_type"),
                "alias": r["alias"],
                "description": r["description"],
                "path": r["path"],
            }
            for r in col_rows
        ]
        tables.append(table)
    return tables


async def _fetch_relationships(conn: asyncpg.Connection) -> list[dict]:
    """Fetch relationships."""
    rows = await conn.fetch(
        "SELECT id, source_table_id, target_table_id, source_column, "
        "target_column, cardinality FROM relationships"
    )
    return [dict(r) for r in rows]


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

    await state.cache_store.close()
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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Conditionally add auth middleware and routes
    from provisa.auth.wiring import wire_auth
    wire_auth(app, state.auth_config)

    app.include_router(data_router)
    app.include_router(dev_router)
    app.include_router(sdl_router)

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

    @app.get("/admin/config")
    async def download_config():
        """Download the current config YAML."""
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        if not path.exists():
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Config file not found")
        from fastapi.responses import Response
        return Response(
            content=path.read_text(),
            media_type="application/x-yaml",
            headers={"Content-Disposition": f"attachment; filename={path.name}"},
        )

    @app.put("/admin/config")
    async def upload_config(request):
        """Upload a revised config YAML and reload."""
        body = await request.body()
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        path = Path(config_path)
        # Write a backup before overwriting
        if path.exists():
            backup = path.with_suffix(".yaml.bak")
            backup.write_text(path.read_text())
        path.write_bytes(body)
        # Reload config
        try:
            await _load_and_build(config_path)
            return {"success": True, "message": "Config uploaded and reloaded"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    from provisa.api.admin.views import router as views_router
    app.include_router(views_router)

    @app.get("/admin/settings")
    async def get_settings():
        """Return current platform settings."""
        from provisa.executor.redirect import RedirectConfig
        from provisa.compiler.sampling import get_sample_size
        rc = RedirectConfig.from_env()
        # Read domain_prefix from config
        config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
        domain_prefix = False
        try:
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
            naming_cfg = cfg.get("naming", {})
            domain_prefix = naming_cfg.get("domain_prefix", False)
            convention = naming_cfg.get("convention", "snake_case")
        except Exception:
            pass
        return {
            "redirect": {
                "enabled": rc.enabled,
                "threshold": rc.threshold,
                "default_format": rc.default_format,
                "ttl": rc.ttl,
            },
            "sampling": {
                "default_sample_size": get_sample_size(),
            },
            "cache": {
                "default_ttl": state.cache_default_ttl,
            },
            "naming": {
                "domain_prefix": domain_prefix,
                "convention": convention,
            },
        }

    @app.put("/admin/settings")
    async def update_settings(request):
        """Update platform settings at runtime."""
        body = await request.json()
        updated = []

        if "redirect" in body:
            r = body["redirect"]
            if "enabled" in r:
                os.environ["PROVISA_REDIRECT_ENABLED"] = str(r["enabled"]).lower()
                updated.append("redirect.enabled")
            if "threshold" in r:
                os.environ["PROVISA_REDIRECT_THRESHOLD"] = str(r["threshold"])
                updated.append("redirect.threshold")
            if "default_format" in r:
                os.environ["PROVISA_REDIRECT_FORMAT"] = r["default_format"]
                updated.append("redirect.default_format")
            if "ttl" in r:
                os.environ["PROVISA_REDIRECT_TTL"] = str(r["ttl"])
                updated.append("redirect.ttl")

        if "sampling" in body:
            s = body["sampling"]
            if "default_sample_size" in s:
                os.environ["PROVISA_SAMPLE_SIZE"] = str(s["default_sample_size"])
                updated.append("sampling.default_sample_size")

        if "cache" in body:
            c = body["cache"]
            if "default_ttl" in c:
                state.cache_default_ttl = int(c["default_ttl"])
                updated.append("cache.default_ttl")

        if "naming" in body:
            n = body["naming"]
            needs_reload = False
            config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
            path = Path(config_path)
            with open(path) as f:
                cfg = yaml.safe_load(f)
            if "domain_prefix" in n:
                cfg.setdefault("naming", {})["domain_prefix"] = bool(n["domain_prefix"])
                updated.append("naming.domain_prefix")
                needs_reload = True
            if "convention" in n:
                valid = ("none", "snake_case", "camelCase", "PascalCase")
                if n["convention"] not in valid:
                    return {"success": False, "message": f"Invalid convention: {n['convention']!r}"}
                cfg.setdefault("naming", {})["convention"] = n["convention"]
                updated.append("naming.convention")
                needs_reload = True
            if needs_reload:
                backup = path.with_suffix(".yaml.bak")
                backup.write_text(path.read_text())
                with open(path, "w") as f:
                    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
                try:
                    await _load_and_build(config_path)
                except Exception:
                    pass

        return {"success": True, "updated": updated}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
