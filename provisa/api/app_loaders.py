# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Config application and source/engine loaders for app startup.

Parses config into the app state singleton and builds source pools, enums,
OpenAPI specs, MV/view config, and ingest engines. Reaches the app state
singleton lazily (from provisa.api.app import state) to avoid a load cycle;
external-resource setup is guarded by tolerate_startup_failure.
"""

from __future__ import annotations


import json
import logging
import os


from provisa.api.startup_resilience import tolerate_startup_failure
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import source_to_catalog
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context
from provisa.compiler.rls import build_rls_context
from sqlalchemy import select
from provisa.core.schema_org import (
    registered_tables as _registered_tables_t,
    sources as _sources_t,
    table_columns as _table_columns_t,
    tracked_functions as _tracked_functions_t,
)
from provisa.core.secrets import resolve_secrets
from provisa.api._meta_views import _OPS_LOG_TABLE_ALIAS  # REQ-884
from provisa.api.admin.db_queries import parse_mask_value as _parse_mask_value
from provisa.api_source.models import ApiEndpoint as ApiEndpoint, ApiSource as ApiSource
from provisa.core.models import ProvisaConfig  # noqa: F401
from typing import TYPE_CHECKING, Any, cast  # noqa: F401

if TYPE_CHECKING:
    import asyncpg
    from provisa.api.app import AppState

log = logging.getLogger(__name__)

# Meta-domain physical table aliasing: the *_meta views shadow the real control-plane
# tables so registered-table introspection reads metadata, not live rows.
_META_TABLE_ALIAS: dict[str, str] = {
    "registered_tables": "registered_tables_meta",
    "table_columns": "table_columns_meta",
    "roles": "roles_meta",
    "tracked_webhooks": "tracked_webhooks_meta",
    "tracked_functions": "tracked_functions_meta",
}


def _apply_server_and_engine_config(raw_config: dict) -> None:
    """Populate state.server_cfg, state.hostname, state.server_limits, state.engine_conn, and FTE hints."""
    from provisa.api.app import state

    state.server_cfg = raw_config.get("server", {}) if isinstance(raw_config, dict) else {}
    # REQ-693: high-security mode (env override wins so airgapped deploys can force it).
    _sec_cfg = raw_config.get("security", {}) if isinstance(raw_config, dict) else {}
    _sec_mode = os.environ.get("PROVISA_SECURITY_MODE") or _sec_cfg.get("mode", "standard")
    state.security_high = str(_sec_mode).lower() == "high"
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
    from provisa.api.startup_seed import _OPS_VIEWS

    state.federation_engine.provision(
        _OPS_VIEWS, getattr(state, "otel_snapshot_retention_hours", None)
    )

    # Engine session tuning (e.g. Fault-Tolerant Execution) — engine-specific, applied through the
    # lifecycle seam. Native engines have no per-session cluster tuning (no-op).
    state.federation_engine.configure_session(state.server_cfg)


def _process_kafka_sources(raw_config: dict) -> None:  # REQ-147, REQ-250
    """Register Kafka topics as virtual tables and populate state.kafka_table_configs/windows."""
    from provisa.api.app import state
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
    from provisa.api.app import state
    from provisa.executor.drivers.registry import has_driver
    from provisa.transpiler.router import VIRTUAL_SOURCES
    from provisa.cache.warm_tables import DEFAULT_ICEBERG_CATALOG as _DEFAULT_ICE_CAT

    # Seed system source catalogs
    state.source_catalogs["provisa-admin"] = source_to_catalog("provisa-admin")
    state.source_catalogs["provisa-otel"] = "otel"

    # A fixed-catalog warehouse (BigQuery project.dataset.table; Fabric/Synapse database.schema.table)
    # pins EVERY source's catalog to the one warehouse catalog — the runtime lands/attaches, and the
    # governed query reads, at exactly <catalog>.<schema>.<table>.
    import os

    _fixed_catalog = None
    _engine = getattr(state, "federation_engine", None)
    _engine_name = getattr(getattr(_engine, "engine", None), "name", "")
    if _engine_name == "bigquery":
        _fixed_catalog = os.environ.get("GOOGLE_CLOUD_PROJECT")
    elif _engine_name == "fabric":
        _fixed_catalog = os.environ.get("FABRIC_DATABASE")
    elif _engine_name == "synapse":
        _fixed_catalog = os.environ.get("SYNAPSE_DATABASE")

    for src in config.sources:
        state.source_types[src.id] = src.type.value
        _pg_cat = (
            source_to_catalog(src.id)
            if src.type.value == "postgresql"
            else (src.database or source_to_catalog(src.id))
        )
        state.source_catalogs[src.id] = _fixed_catalog or _pg_cat
        state.source_dialects[src.id] = src.dialect or ""
        state.source_cache[src.id] = {
            "cache_enabled": src.cache_enabled,
            "cache_ttl": src.cache_ttl,
        }
        if src.federation_hints:
            state.source_federation_hints[src.id] = dict(src.federation_hints)
        if src.allowed_domains:
            state.source_allowed_domains[src.id] = list(src.allowed_domains)
        # Engine-attached sources (file-based sqlite, NoSQL, lake) are reached only through the
        # engine's ATTACH — they have no network direct pool. Attempting one builds an invalid DSN
        # (e.g. sqlite has a file ``path``, not host/port) and would leave the source routable-as-
        # direct with no driver behind it. Never pool them; the router routes them to the engine.
        if has_driver(src.type.value) and src.type.value not in VIRTUAL_SOURCES:
            resolved_pw = resolve_secrets(src.password)
            resolved_host = resolve_secrets(src.host) if src.host else "localhost"
            state.source_dsns[src.id] = f"{resolved_host}:{src.port}/{src.database}"
            # Best-effort: an unreachable/misconfigured source must not abort startup —
            # the engine-routed path still works. See startup_resilience.
            with tolerate_startup_failure(
                f"direct pool for {src.id!r} ({resolved_host}:{src.port})"
            ):
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
                    # Warehouse connection extras (Databricks http_path, Snowflake account/warehouse,
                    # ClickHouse scheme) the standard args can't carry (REQ-986/987/988).
                    extra={k: resolve_secrets(v) for k, v in src.federation_hints.items()},
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


async def _load_openapi_specs() -> None:
    """Reload OpenAPI specs from DB into state (survives hot reloads and restarts)."""
    from provisa.api.app import state

    assert state.tenant_db is not None
    async with state.tenant_db.acquire() as conn:
        openapi_rows = [
            dict(_r._mapping)
            for _r in (
                await conn.execute_core(
                    select(_sources_t.c.id, _sources_t.c.path).where(
                        _sources_t.c.type == "openapi",
                        _sources_t.c.path.is_not(None),
                        _sources_t.c.path != "",
                    )
                )
            ).fetchall()
        ]
    from provisa.openapi.loader import load_spec
    from provisa.core.secrets import resolve_secrets as _resolve_secrets

    state.openapi_specs = {}
    for _row in openapi_rows:
        # Best-effort: a malformed or unreachable spec must not abort startup.
        with tolerate_startup_failure(f"OpenAPI spec for {_row['id']!r}"):
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


def _load_mv_and_views_config(
    raw_config: dict,
) -> None:  # REQ-086, REQ-133, REQ-135, REQ-158, REQ-159, REQ-160
    """Load materialized_views, views, and auto-MV cross-source relationships into state."""
    from provisa.api.app import state
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
        # Default the target to the store the ACTIVE engine materializes into (DuckDB → mat_store,
        # not postgresql); an explicit config value still wins.
        _def_cat, _def_schema = state.federation_engine.materialize_store_target(state.org_id)
        mv = MVDefinition(
            id=mvc["id"],
            source_tables=mvc.get("source_tables", []),
            target_catalog=mvc.get("target_catalog", _def_cat),
            target_schema=mvc.get("target_schema", _def_schema),
            target_table=mvc.get("target_table"),
            refresh_interval=mvc.get("refresh_interval", 300),
            enabled=mvc.get("enabled", True),
            join_pattern=jp,
            sql=mvc.get("sql"),
            expose_in_sdl=mvc.get("expose_in_sdl", False),
            sdl_config=sdl_cfg,
            preprocess=mvc.get("preprocess"),  # REQ-957 (purity-checked at boot compile)
        )
        state.mv_registry.register(mv)

        # REQ-086: Expose MV as queryable table in schema. Source catalog/schema mirror the MV's
        # resolved target (engine store), not a hardcoded postgresql.
        if mv.expose_in_sdl and sdl_cfg:
            mv_table = {
                "source_id": mvc.get("target_catalog", _def_cat),
                "domain_id": sdl_cfg.domain_id,
                "schema": mvc.get("target_schema", _def_schema),
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

            # EVERY view is inline-expandable (live path); a materialized view is ALSO registered as
            # an MV for acceleration when fresh. Keeping the view_sql_map entry makes a materialized
            # view queryable before/without a refresh (its raw source catalog would otherwise reach
            # the engine → "Catalog does not exist").
            state.view_sql_map[view_table_name] = view_sql.strip()
            if materialize:
                # Target the store the ACTIVE engine materializes into (never a hardcoded catalog).
                _tgt_cat, _tgt_schema = state.federation_engine.materialize_store_target(
                    state.org_id
                )
                mv = MVDefinition(
                    id=f"view-{view_id}",
                    source_tables=[],
                    target_catalog=_tgt_cat,
                    target_schema=_tgt_schema,
                    target_table=view_table_name,
                    refresh_interval=refresh_interval,
                    enabled=True,
                    sql=view_sql,
                    expose_in_sdl=False,
                    preprocess=view_cfg.get("preprocess"),  # REQ-957 (purity-checked at boot compile)
                )
                state.mv_registry.register(mv)
                _view_log.info("Registered materialized view: %s", view_id)
            else:
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
    from provisa.api.app import state

    assert state.tenant_db is not None
    # Best-effort: ingest-source setup failing must not abort whole-server startup.
    with tolerate_startup_failure("ingest source init", exc_info=True):
        from provisa.ingest.engine import get_engine as _get_ingest_engine
        from provisa.ingest.ddl import generate_create_table as _gen_ddl
        from provisa.core.secrets import resolve_secrets as _resolve_secrets

        async with state.tenant_db.acquire() as _pg_conn:
            _ingest_sources = [
                dict(_r._mapping)
                for _r in (
                    await _pg_conn.execute_core(
                        select(
                            _sources_t.c.id,
                            _sources_t.c.host,
                            _sources_t.c.port,
                            _sources_t.c.database,
                            _sources_t.c.username,
                            _sources_t.c.dialect,
                        ).where(_sources_t.c.type == "ingest")
                    )
                ).fetchall()
            ]
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
                _itables = [
                    dict(_r._mapping)
                    for _r in (
                        await _pg_conn.execute_core(
                            select(
                                _registered_tables_t.c.table_name,
                                _table_columns_t.c.column_name,
                                _table_columns_t.c.path,
                                _table_columns_t.c.data_type,
                            )
                            .select_from(
                                _registered_tables_t.join(
                                    _table_columns_t,
                                    _table_columns_t.c.table_id == _registered_tables_t.c.id,
                                )
                            )
                            .where(_registered_tables_t.c.source_id == _sid)
                            .order_by(_registered_tables_t.c.table_name, _table_columns_t.c.id)
                        )
                    ).fetchall()
                ]
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
                with tolerate_startup_failure(f"ingest DDL for {_sid}.{_tn}"):
                    async with _eng.begin() as _conn:
                        await _conn.execute(__import__("sqlalchemy").text(_ddl))


async def _load_graphql_remote_sources_from_db() -> None:
    """Load persisted graphql_remote sources from DB into state.graphql_remote_sources."""
    from provisa.api.app import state

    if state.tenant_db is None:
        log.warning("[GQL REMOTE] tenant_db is None — skipping DB load")
        return
    # Best-effort: a DB/spec failure here must not abort startup — the server
    # comes up without the graphql_remote sources and logs why.
    with tolerate_startup_failure("graphql_remote sources from DB", exc_info=True):
        async with state.tenant_db.acquire() as _conn:
            src_rows = [
                dict(_r._mapping)
                for _r in (
                    await _conn.execute_core(
                        select(_sources_t.c.id, _sources_t.c.path).where(
                            _sources_t.c.type == "graphql_remote"
                        )
                    )
                ).fetchall()
            ]
            for src in src_rows:
                source_id = src["id"]
                url = src["path"] or ""
                if source_id in getattr(state, "graphql_remote_sources", {}):
                    continue
                tbl_rows = [
                    dict(_r._mapping)
                    for _r in (
                        await _conn.execute_core(
                            select(
                                _registered_tables_t.c.id,
                                _registered_tables_t.c.table_name,
                                _registered_tables_t.c.domain_id,
                                _registered_tables_t.c.description,
                            ).where(
                                _registered_tables_t.c.source_id == source_id,
                                _registered_tables_t.c.schema_name == "graphql",
                            )
                        )
                    ).fetchall()
                ]
                tables: list[dict] = []
                for tr in tbl_rows:
                    col_rows = [
                        dict(_r._mapping)
                        for _r in (
                            await _conn.execute_core(
                                select(
                                    _table_columns_t.c.column_name,
                                    _table_columns_t.c.data_type,
                                    _table_columns_t.c.object_fields,
                                    _table_columns_t.c.native_filter_type,
                                ).where(_table_columns_t.c.table_id == tr["id"])
                            )
                        ).fetchall()
                    ]
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
                            # Malformed optional object-field metadata is skipped; any
                            # other error is a real bug and must propagate.
                            try:
                                col_dict["gql_object_fields"] = (
                                    json.loads(raw_of) if isinstance(raw_of, str) else raw_of
                                )
                            except json.JSONDecodeError:
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


async def _load_masking_rules(  # REQ-040, REQ-263
    conn: Any,
    col_types_converted: dict[int, list[ColumnMetadata]],
    roles: list[dict],
) -> None:
    """Load masking rules from table_columns and populate state.masking_rules."""
    from provisa.api.app import state
    from provisa.security.masking import MaskingRule, MaskType, validate_masking_rule

    masking_rows = [
        dict(_r._mapping)
        for _r in (
            await conn.execute_core(
                select(
                    _table_columns_t.c.table_id,
                    _table_columns_t.c.column_name,
                    _table_columns_t.c.unmasked_to,
                    _table_columns_t.c.mask_type,
                    _table_columns_t.c.mask_pattern,
                    _table_columns_t.c.mask_replace,
                    _table_columns_t.c.mask_value,
                    _table_columns_t.c.mask_precision,
                ).where(_table_columns_t.c.mask_type.is_not(None))
            )
        ).fetchall()
    ]
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


def _json_list(value: Any) -> list:
    """Coerce a JSON list-column to a Python list, tolerating raw-SQL string returns.

    ``conn.fetch`` executes raw ``text()`` SQL, which bypasses SQLAlchemy's JSON type and returns
    JSON columns as strings on SQLite/asyncpg. A JSON string must be ``json.loads``-ed, never
    ``list()``-ed: ``list('["admin"]')`` yields the CHARACTERS ``['[','"','a',...]``, which silently
    drops every role-restricted function/webhook from the generated schema (its ``visible_to`` no
    longer contains the role id). See the ``arguments`` handling this mirrors.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return list(value)


async def _load_tracked_functions_and_webhooks(  # REQ-042
    conn: Any, raw_config: dict | None
) -> tuple[list[dict], list[dict]]:
    """Load tracked functions and webhooks from DB; populate state.tracked_functions/webhooks."""
    from provisa.api.app import state
    from provisa.api.admin.actions_router import _ensure_tables

    assert state.tenant_db is not None, (
        "tenant_db must be initialized before loading tracked functions"
    )
    await _ensure_tables(state.tenant_db)

    from provisa.discovery.catalog_cache import ensure_table as _ensure_catalog_cache

    await _ensure_catalog_cache(state.tenant_db)
    fn_rows = [
        dict(_r._mapping)
        for _r in (
            await conn.execute_core(
                select(_tracked_functions_t).order_by(_tracked_functions_t.c.name)
            )
        ).fetchall()
    ]
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
            "arguments": _json_list(r["arguments"]),
            "visible_to": _json_list(r["visible_to"]),
            "writable_by": _json_list(r["writable_by"]),
        }
        for r in fn_rows
    ]
    tracked_webhooks = [
        {
            **dict(r),
            "arguments": _json_list(r["arguments"]),
            "inline_return_type": _json_list(r["inline_return_type"]),
            "visible_to": _json_list(r["visible_to"]),
        }
        for r in wh_rows
    ]

    # The prefixed alias MUST match the GraphQL field name the schema generator emits, which uses
    # domain_gql_alias (e.g. pet-store -> "ps"), NOT domain_to_sql_name (-> "pet_store"). Otherwise
    # _split_action_fields can't find the domain-prefixed field and the command falls through to the
    # table compiler as an "Unknown root query field" (REQ-1156).
    from provisa.compiler.naming import domain_gql_alias as _dgql

    _domains_cfg = (raw_config or {}).get("domains", []) or []
    _alias_stored = {
        d["id"]: d.get("graphql_alias")
        for d in _domains_cfg
        if isinstance(d, dict) and d.get("id")
    }

    def _domain_prefix(domain_id: str) -> str:
        return _dgql(domain_id, _alias_stored.get(domain_id))

    _dp = raw_config.get("naming", {}).get("domain_prefix", False) if raw_config else False
    state.tracked_functions = {}
    for f in tracked_functions:
        state.tracked_functions[f["name"]] = f
        if _dp and f.get("domain_id"):
            state.tracked_functions[f"{_domain_prefix(f['domain_id'])}__{f['name']}"] = f
    state.tracked_webhooks = {}
    for w in tracked_webhooks:
        state.tracked_webhooks[w["name"]] = w
        if _dp and w.get("domain_id"):
            state.tracked_webhooks[f"{_domain_prefix(w['domain_id'])}__{w['name']}"] = w

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
    from provisa.api.app import state

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
            physical_table_map={
                **_META_TABLE_ALIAS,
                **_OPS_LOG_TABLE_ALIAS,
                **(kafka_physical or {}),
            },
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

        # No swallow: an unmapped column type is a real gap in the proto type map, not a reason to
        # silently disable gRPC for the role. Let generate_proto raise so it surfaces at startup and
        # gets fixed at the source (the type map) — never patched around here.
        from provisa.grpc.proto_gen import generate_proto

        state.proto_files[role["id"]] = generate_proto(si)


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
