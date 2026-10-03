# Copyright (c) 2026 Kenneth Stott
# Canary: ad70c8f7-e306-44ac-83d9-dfa18051d101
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


from provisa.api.startup_resilience import tolerate_startup_failure
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import source_to_catalog
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.security.rights import Capability, reaches_all_domains
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
from provisa.core.models import ProvisaConfig, Source  # noqa: F401
from typing import TYPE_CHECKING, Any, cast  # noqa: F401

if TYPE_CHECKING:
    from provisa.api.app import AppState
    from provisa.core.database import Connection
    from provisa.mv.models import MVDefinition, TableIdentity

log = logging.getLogger(__name__)

# Meta-domain physical table aliasing: the *_meta views shadow the real control-plane
# tables so registered-table introspection reads metadata, not live rows.
_META_TABLE_ALIAS: dict[str, str] = {
    "registered_tables": "registered_tables_meta",
    "table_columns": "table_columns_meta",
    "roles": "roles_meta",
    "glossary_terms": "glossary_terms_meta",  # REQ-1584
    "glossary_term_refs": "glossary_term_refs_meta",  # REQ-1584
    "glossary_term_edges": "glossary_term_edges_meta",  # REQ-1584
    "glossary_term_experts": "glossary_term_experts_meta",  # REQ-1584
    "tracked_webhooks": "tracked_webhooks_meta",
    "tracked_functions": "tracked_functions_meta",
    "tags": "tags_meta",  # REQ-1373
    "tag_assignments": "tag_assignments_meta",  # REQ-1377
}


def configure_encryption_and_secrets(raw_config: dict) -> None:
    """Install the process-wide EncryptionService and select the secrets service, from config.

    REQ-684/686: before any encrypt/decrypt (API auth column, hot cache, audit) runs. An unset
    provider is the passthrough one. REQ-1557: an unset secrets provider is not "unconfigured" —
    it selects Provisa's own encrypted per-org store; the backend is built on first use, so this
    is only the selection.

    Called when the server config is applied (REQ-1913: a stored secret setting is sealed by this
    provider and is read as soon as the restart settings are fixed) and again by every schema
    rebuild, where a changed ``encryption``/``secrets`` block takes effect. Idempotent.
    """
    from provisa.core.secrets_runtime import configure_secrets
    from provisa.encryption import configure_encryption

    enc_cfg = raw_config.get("encryption", {}) or {}
    enc_provider = enc_cfg.get("provider")
    configure_encryption(
        enc_provider,
        key_id=enc_cfg.get("key_id"),
        config=enc_cfg.get(enc_provider, {}) if enc_provider else {},
    )
    sec_cfg = raw_config.get("secrets", {}) or {}
    sec_provider = sec_cfg.get("provider")
    configure_secrets(sec_provider, config=sec_cfg.get(sec_provider, {}) if sec_provider else {})


def apply_telemetry_settings(state: "AppState") -> None:
    """Publish the telemetry compaction settings the scheduler and the compaction job read off
    state (REQ-545). Operator settings (REQ-1913), fixed at start."""
    from provisa.core import settings_registry

    value = settings_registry.value
    state.otel_compact_cron = value("otel.compact_cron")
    state.otel_compact_batch_size = value("otel.compact_batch_size")
    state.otel_compact_file_chunk = value("otel.compact_file_chunk")
    state.otel_compact_max_files_per_run = value("otel.compact_max_files_per_run")
    state.otel_snapshot_retention_hours = value("otel.ops_snapshot_retention_hours")
    state.otel_s3_endpoint = value("otel.s3_endpoint")


def apply_redis_settings(state: "AppState") -> None:
    """Resolve which Redis the deployment uses and build the rate limiter on it.

    The URL is resolved whether or not the response cache is enabled, because rate limiting
    (REQ-371) needs it either way. The limiter is built here, once per process — where the config
    is loaded and the control plane bound — and not while the app object is created, when neither
    is: built there it always counted in its own process's embedded Redis.
    """
    from provisa.api import rate_limit
    from provisa.core.redis_location import redis_url

    state.redis_url = redis_url()
    if state.rate_limiter is None:
        state.rate_limiter = rate_limit.build_rate_limiter(state.redis_url)


def _apply_server_and_engine_config(
    raw_config: dict, connect_engine: bool = True, provision_engine: bool = True
) -> None:
    """Populate state.server_cfg, state.hostname, state.server_limits, state.engine_conn, and FTE hints.

    ``provision_engine=False`` (REQ-1900) is a worker whose launch has already provisioned the
    engine: it opens its own terminal connection and seeds nothing.

    ``connect_engine=False`` (REQ-1619) applies the server settings and stops short of the terminal:
    boot passes it when the shard could not be allocated, so there is no coordinator address to dial
    and no ops catalog to seed on it. The terminal is opened by ``restore_shared_terminal`` on the
    first query, after the wake that gives it an address.
    """
    from provisa.api.app import state

    state.server_cfg = raw_config.get("server", {}) if isinstance(raw_config, dict) else {}
    # REQ-1913: the operator settings resolve their config-file values from this config. A config
    # that is not a mapping (an empty file) states none.
    from provisa.core import settings_registry

    _config = raw_config if isinstance(raw_config, dict) else {}
    settings_registry.bind_config(_config)
    # A stored secret setting is sealed by the deployment's encryption provider, and may be a
    # reference into its secrets service: both are configured before the first one is read.
    configure_encryption_and_secrets(_config)
    # The restart settings are fixed here, once: the control plane is bound (_init_control_planes
    # runs first) and the config is named, and nothing has read one yet.
    settings_registry.freeze_at_boot()
    # REQ-1882: size the background worker pool from config before anything is submitted to it
    # (the first boot step that has the config). A value different from a pool already running
    # raises in configure_background_workers — a running pool is never silently left mis-sized.
    from provisa.core.connection_loop import configure_background_workers

    configure_background_workers(settings_registry.value("concurrency.background_workers"))
    # REQ-1905: the server-wide Flight stream limit, per worker process. A stream over the limit
    # waits for a slot (provisa/api/flight/stream_slots.py).
    state.flight_global_cap = settings_registry.value("concurrency.flight_max_concurrent_streams")
    # REQ-693: high-security mode (env override wins so airgapped deploys can force it).
    state.security_high = settings_registry.value("security.mode") == "high"
    state.hostname = settings_registry.value("server.hostname")

    from provisa.core.limits import set_server_limits

    state.server_limits = {
        # REQ-1913: declared in provisa/core/settings_catalog.py and resolved by the registry;
        # their readers ask the registry on every use, so a stored change needs no reload.
        "default_row_limit": settings_registry.value("limits.default_row_limit"),
        "engine_query_timeout": settings_registry.value("limits.engine_query_timeout"),
        # REQ-1905: the DEFAULT request timeout; a transport's own is asked of
        # provisa.core.limits.request_timeout_for where its deadline is bound.
        "request_timeout": settings_registry.value("limits.request_timeout"),
        "retry_budget_secs": settings_registry.value("limits.retry_budget_secs"),
    }
    set_server_limits(state.server_limits)  # REQ-1678: the compiler reads the cap from core

    # The engine terminal is only provisioned when it has one (the engine connects a cluster and seeds
    # its otel catalog). A native engine (duckdb/embedded-pg/…) has nothing to connect here —
    # telemetry lands in the dedicated ops store (ops_schema/otlp2sql), so provision() is a no-op.
    from provisa.api.startup_seed import _OPS_VIEWS

    if connect_engine:
        if provision_engine:
            state.federation_engine.provision(_OPS_VIEWS)
        else:
            state.federation_engine.connect_terminal()

        # Engine session tuning (e.g. Fault-Tolerant Execution) — engine-specific, applied through
        # the lifecycle seam. Native engines have no per-session cluster tuning (no-op).
        state.federation_engine.configure_session(state.server_cfg)


def _process_kafka_sources(
    raw_config: dict, register_catalogs: bool = True
) -> None:  # REQ-147, REQ-250
    """Register Kafka topics as virtual tables and populate state.kafka_table_configs/windows.

    ``register_catalogs=False`` (REQ-1900) is a worker whose launch has already issued the engine's
    Kafka catalogs: it builds this process's table maps and issues nothing."""
    from provisa.api.app import state
    from provisa.kafka.window import KafkaTableConfig

    for ks in raw_config.get("kafka_sources", []):
        source_id = ks["id"]
        # A Kafka source names its brokers; a subscription to its topics reads them from here.
        if not ks.get("bootstrap_servers"):
            raise ValueError(f"Kafka source {source_id!r} names no bootstrap_servers")
        state.kafka_bootstrap[source_id] = resolve_secrets(ks["bootstrap_servers"])
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
        if register_catalogs:
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
                        # REQ-1426: the topic's declared type is the design-time type of the
                        # registered column. Dropping it here made the table repository refuse the
                        # synthesized table, since nothing downstream infers a type.
                        "data_type": col.get("data_type") if isinstance(col, dict) else None,
                        "visible_to": col.get("visible_to", ["org_admin", "analyst"])
                        if isinstance(col, dict)
                        else ["org_admin", "analyst"],
                        "writable_by": col.get("writable_by", []) if isinstance(col, dict) else [],
                        "description": col.get("description", "") if isinstance(col, dict) else "",
                    }
                    for col in topic_columns
                ],
            }
            raw_config.setdefault("tables", []).append(table_entry)

            state.kafka_table_physical = getattr(state, "kafka_table_physical", {})
            state.kafka_table_physical[gql_table_name] = physical_table


def fixed_catalog_for_engine(state: "AppState") -> str | None:
    """The one physical catalog every source is pinned to, for a single-store engine — every source
    on that engine, config-loaded or created dynamically via ``createSource``, must share this one
    catalog name rather than the per-source name ``source_to_catalog`` would derive.

    Thin wrapper: ``provisa.federation.engine.fixed_catalog_for`` is the single naming authority
    for this decision (REQ-1730) — it must never be duplicated or re-derived here."""
    from provisa.federation.engine import fixed_catalog_for

    engine = getattr(getattr(state, "federation_engine", None), "engine", None)
    if engine is None:
        return None
    return fixed_catalog_for(engine)


def catalog_name_for_source(state: "AppState", source_type: str, source_id: str) -> str:  # REQ-1730
    """The physical catalog a registered source's tables resolve to under the ACTIVE engine.

    Order: (1) a fixed-catalog warehouse engine (see ``fixed_catalog_for_engine``) pins every
    source to one name; (2) a source Trino cannot read in place (REQ-826 ``_MATERIALIZE_ONLY``, or
    a connector that only fetches) has no catalog of its own — REQ-842: none is ever provisioned
    for a type with no LIVE Trino connector — so the compiler names it under Trino's
    materialize-store catalog (``materialize_store_target``'s catalog, ``provisa_admin``). Its
    reads are then addressed to its replica in that store's replicas schema (REQ-1912); the name
    emitted here only identifies the table to that rewrite.
    (3) otherwise the per-source org-scoped name every other engine (DuckDB's own per-source
    ATTACH, a warehouse's per-source external-table catalog, or Trino's OWN live connector for a
    type that has one) actually provisions.

    REQ-1730: (2) checks the Trino connector's OWN ``mechanism`` (``LIVE_IN_PLACE`` — ATTACH_RW/
    ATTACH_R/SCAN — vs FETCH, or no Trino connector at all) — a type can produce its rows by
    running a query on another engine while STILL having its own LIVE Trino connector
    (prometheus, whose rows Trino scrapes directly; google_sheets is the same case, ATTACH_R).
    """
    from provisa.compiler.naming import org_prefixed_catalog
    from provisa.core.request_context import active_env, current_org
    from provisa.federation.connector_base import LIVE_IN_PLACE

    fixed = fixed_catalog_for_engine(state)
    if fixed:
        return fixed
    if source_type == "ingest":  # REQ-1771
        # ingest has NO live connector on any engine (no TRINO_CONNECTORS entry, and the native/
        # DuckDB tier's own ATTACH loop — native_backend.py's _attach_registered — never attaches
        # one either): its rows land straight into the tenant control-plane DB (provisa/ingest/
        # engine.py writes there directly), so the only catalog the compiler can ever reach them
        # through is provisa_admin — the SAME catalog duckdb_runtime.py's _rebuild_control_plane
        # exposes the tenant DB under on the native tier, and Trino's own control-plane catalog
        # (PROVISA_ADMIN_CATALOG) on that tier. A per-source name here would resolve to a catalog
        # nothing ever provisions for ingest, on either engine.
        return "provisa_admin"
    engine_rt = state.federation_engine
    engine_name = getattr(getattr(engine_rt, "engine", None), "name", "")
    if engine_name == "trino":
        from provisa.federation.trino_connectors import TRINO_CONNECTORS

        connector = TRINO_CONNECTORS.get(source_type)
        if connector is None or connector.mechanism not in LIVE_IN_PLACE:
            org_id = current_org.get() or state.org_id
            return engine_rt.materialize_store_target(org_id)[0]
    return org_prefixed_catalog(
        current_org.get() or state.org_id,
        source_to_catalog(source_id),
        default_org=state.org_id,
        env=active_env(),
    )


def _populate_source_catalog_names(
    config: ProvisaConfig, extra_sources: list[Source] | None = None
) -> None:  # REQ-012, REQ-1266, REQ-1730
    """Populate the org-scoped engine-catalog name map (+ source types/dialects/cache/hints).

    Split out of :func:`_build_source_pools_and_enums` so it can run BEFORE ``load_config``: the
    physical catalog registration (``engine.register_source`` → ``catalog.create_catalog``) must
    write each source under the SAME name the compiler later emits, and that name lives in
    ``state.source_catalogs``. Registering first and populating the map second would attach a
    non-default org's source under the bare (default-org) catalog name — a cross-org collision in
    the one shared coordinator's catalog namespace. Idempotent; safe to call again from
    :func:`_build_source_pools_and_enums`.

    ``extra_sources`` (REQ-1730): a source created purely through the ``createSource`` mutation
    (no ``sources:`` entry in the YAML this boot/reload loaded) is invisible to ``config.sources``
    — this function used to populate NOTHING for it, so a fresh boot or a live config reload left
    ``state.source_catalogs``/``source_types``/etc. never knowing it existed, regardless of engine.
    The one place that DID populate it correctly was `create_source`'s own mutation handler, at
    the moment of registration — which is why this gap was invisible as long as the SAME process
    that registered a source kept running: change the active engine and restart (or reload) that
    process, and every source registered purely through the UI silently lost this state, on ANY
    engine, not just a "different" one. Callers pass the control-plane's full source list here
    (``registry_view.registered_sources``, already config-aware) filtered to just the ids
    ``config.sources`` does not already cover.
    """
    from provisa.api.app import state

    # Seed system source catalogs (never org-prefixed — one physical catalog shared across orgs).
    state.source_catalogs["provisa-admin"] = source_to_catalog("provisa-admin")
    # `otel` is a Trino dynamic catalog; a native engine has no such catalog (see
    # EngineBackend.has_otel_catalog), and startup_seed registers no provisa-otel tables there.
    if state.federation_engine.has_otel_catalog:
        state.source_catalogs["provisa-otel"] = "otel"

    for src in (*config.sources, *(extra_sources or ())):
        state.source_types[src.id] = src.type.value
        # Fixed-warehouse catalogs pin every source to one physical catalog (not org-scoped);
        # an adapter-fetched source under Trino resolves through Trino's OWN materialize-store
        # catalog (REQ-1730); otherwise namespace the org-scoped catalog for non-default orgs.
        # The base name comes from the source id alone — that is what create_catalog physically
        # names the catalog (provisa/core/catalog.py:116) and what native engines attach by.
        # `src.database` is the remote database/tenant the connector talks to, not a catalog.
        state.source_catalogs[src.id] = catalog_name_for_source(state, src.type.value, src.id)
        state.source_dialects[src.id] = src.dialect or ""
        state.source_cache[src.id] = {
            "cache_enabled": src.cache_enabled,
            "cache_ttl": src.cache_ttl,
        }
        if src.federation_hints:
            state.source_federation_hints[src.id] = dict(src.federation_hints)
        if src.allowed_domains:
            state.source_allowed_domains[src.id] = list(src.allowed_domains)


async def _build_source_pools_and_enums(
    config: ProvisaConfig, extra_sources: list[Source] | None = None
) -> None:  # REQ-012, REQ-221, REQ-1730
    """Build direct source connection pools, register websocket/rss sources, and fetch enum types.

    ``extra_sources`` (REQ-1730): control-plane-only sources ``config.sources`` doesn't declare —
    see ``_populate_source_catalog_names``'s own doc for the general gap this closes. Without it, a
    control-plane-only RDBMS source (postgresql/mysql/...) never got a direct connection pool built
    on ANY boot or reload, so every query against it fell through to whatever fallback executor
    handles a poolless source — reproduced live via REQ-1730's own reboot-harness e2e: DuckDB's own
    catalog attach succeeded, but the query still failed with a raw asyncpg/SQLAlchemy error
    naming a table that plainly existed, because the actual read path for these types is this
    pool, not the engine's ATTACH.
    """
    from provisa.api.app import state
    from provisa.executor.drivers.registry import has_driver
    from provisa.transpiler.router import VIRTUAL_SOURCES
    from provisa.compiler.sql_rewrite import FLAT_NAMESPACE_SOURCES
    from provisa.core.secrets_store import bound_to_request_org

    # Catalog names + source types must exist before pools/domains read them (idempotent — also
    # run before load_config so physical registration uses the org-prefixed name).
    _populate_source_catalog_names(config, extra_sources=extra_sources)

    # REQ-1730: a control-plane-only source's password is a ${secret:NAME} vault reference
    # (persist_source_password writes it that way — see StoredSecretsProvider), unlike a YAML
    # source's password, which is a literal or a ${env:...} ref that needs no org binding at all.
    # Nothing bound an org here before because nothing calling resolve_secrets during BOOT ever
    # held a vault-backed reference until extra_sources started flowing through this loop —
    # reproduced live: KeyError "no organization is bound to this context", uncaught, crashing the
    # whole boot outright (Application startup failed) for the very first control-plane-only
    # source with a real password. Resolves to state.org_id, the boot org, when nothing more
    # specific is bound (see _request_org_for_secrets) — correct for this single-org boot path.
    async with bound_to_request_org():
        for src in (*config.sources, *(extra_sources or ())):
            # Engine-attached sources (NoSQL, lake) are reached only through the engine's ATTACH —
            # they have no direct driver at all. FLAT_NAMESPACE_SOURCES (sqlite) DO have a direct
            # driver (the generic SQLAlchemy fallback, REQ-031/REQ-1361: mutations always route
            # direct, and the engine terminal takes no writes) — they just connect by file
            # ``path``, not host/port, so they need the pool built from that field instead of
            # skipping registration entirely.
            _is_flat_file = src.type.value in FLAT_NAMESPACE_SOURCES
            if has_driver(src.type.value) and (
                src.type.value not in VIRTUAL_SOURCES or _is_flat_file
            ):
                resolved_pw = resolve_secrets(src.password)
                resolved_host = (
                    ""
                    if _is_flat_file
                    else (resolve_secrets(src.host) if src.host else "localhost")
                )
                resolved_database = (src.path or src.database) if _is_flat_file else src.database
                state.source_dsns[src.id] = (
                    resolved_database
                    if _is_flat_file
                    else f"{resolved_host}:{src.port}/{src.database}"
                )
                # Best-effort: an unreachable/misconfigured source must not abort startup —
                # the engine-routed path still works. See startup_resilience.
                with tolerate_startup_failure(
                    f"direct pool for {src.id!r} ({resolved_database if _is_flat_file else f'{resolved_host}:{src.port}'})"
                ):
                    await state.source_pools.add(
                        source_id=src.id,
                        source_type=src.type.value,
                        host=resolved_host,
                        port=src.port,
                        database=resolved_database,
                        user=src.username,
                        password=resolved_pw,
                        min_size=src.pool_min,
                        max_size=src.pool_max,
                        use_pgbouncer=src.use_pgbouncer,
                        pgbouncer_port=src.pgbouncer_port,
                        # Warehouse connection extras (Databricks http_path, Snowflake account/
                        # warehouse, ClickHouse scheme) the standard args can't carry (REQ-986/987/988).
                        extra={k: resolve_secrets(v) for k, v in src.federation_hints.items()},
                    )

    # WebSocket + RSS sources — register for SSE subscription dispatch
    for _src in (*config.sources, *(extra_sources or ())):
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
    for _src in (*config.sources, *(extra_sources or ())):
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

    assert state.model_db is not None
    async with state.model_db.acquire() as conn:
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


def _config_identities(raw_config: dict) -> dict[str, list[TableIdentity]]:
    """The config's tables by every name a join-pattern view may use for one (its name and SQL
    name), each with its identity."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.mv.models import TableIdentity

    out: dict[str, list[TableIdentity]] = {}
    for tbl in raw_config.get("tables", []):
        name = tbl.get("table") or tbl.get("table_name")
        if not name or tbl.get("view_sql"):
            continue
        identity = TableIdentity(tbl["source_id"], tbl.get("schema") or tbl["schema_name"], name)
        for spelled in {name, apply_sql_name(name)}:
            out.setdefault(spelled, []).append(identity)
    return out


def _bind_join_inputs(
    view_id: str, names: list[str], identities: dict[str, list[TableIdentity]]
) -> list[TableIdentity]:
    """A join-pattern view's inputs, bound when it is declared (REQ-939): each table it joins is
    the one config table of that name. A name no table, or more than one, answers to is refused,
    naming the view — it would not say which table the view reads. A name may be written
    qualified, ``source_id/schema.table`` (as an export of the view writes it), to say which."""
    known = {i for found in identities.values() for i in found}
    bound = []
    for name in names:
        if "/" in name:
            source_id, _, rest = name.partition("/")
            schema, _, table = rest.partition(".")
            found = [
                i
                for i in known
                if (i.source_id, i.schema_name, i.table_name) == (source_id, schema, table)
            ]
        else:
            found = identities.get(name, [])
        if len(found) != 1:
            why = (
                "no table has that name"
                if not found
                else "more than one table has that name ("
                + ", ".join(sorted(i.label for i in found))
                + ")"
            )
            raise ValueError(f"materialized view {view_id!r} joins {name!r}: {why}")
        bound.append(found[0])
    return bound


def _joined_names(listed: list[str], jp: Any) -> list[str]:
    """The tables a join-pattern view reads: those it lists, then any its pattern joins that the
    list leaves out (a listed name may be qualified, ``source_id/schema.table``)."""
    if jp is None:
        return list(listed)
    bare = {name.partition("/")[2].partition(".")[2] if "/" in name else name for name in listed}
    joined = [jp.left_table, jp.right_table] + ([jp.via_table] if jp.via_table else [])
    return list(listed) + [t for t in joined if t not in bare]


def _load_mv_and_views_config(
    raw_config: dict,
) -> list[MVDefinition]:  # REQ-086, REQ-133, REQ-135, REQ-158, REQ-159, REQ-160
    """Load materialized_views, views, and auto-MV cross-source relationships into state.

    Returns the views it registered: the caller checks each reads only inputs the engine can
    read whole (provisa/mv/readable_inputs.py) once the registry they are checked against is
    loaded, and fails the load on one that does not."""
    from provisa.api.app import state
    from provisa.mv.models import MVDefinition, JoinPattern, SDLConfig

    loaded: list[MVDefinition] = []
    identities = _config_identities(raw_config)

    # REQ-1443/description pull-forward: base-table column descriptions, keyed by table name, as
    # declared BEFORE this function appends any MV/view-derived table entries — a pass-through MV
    # column with no explicit description inherits its base column's description from here rather
    # than shipping undocumented.
    _base_table_columns: dict[str, dict] = {}
    for _t in raw_config.get("tables", []):
        _tname = _t.get("table") or _t.get("table_name")
        if _tname:
            _base_table_columns[_tname] = {
                c["name"]: c["description"]
                for c in _t.get("columns", [])
                if isinstance(c, dict) and c.get("description")
            }

    def _pull_forward_descriptions(columns: list[dict], candidate_tables: list[str]) -> None:
        for col in columns:
            if not isinstance(col, dict) or col.get("description"):
                continue
            name = col.get("name")
            for tname in candidate_tables:
                desc = _base_table_columns.get(tname, {}).get(name)
                if desc:
                    col["description"] = desc
                    break

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
                # REQ-1586: an explicitly declared MV may cover a junction hop as well
                via_table=jp_cfg.get("via_table"),
                via_left_column=jp_cfg.get("via_left_column"),
                via_right_column=jp_cfg.get("via_right_column"),
                via_type_column=jp_cfg.get("via_type_column"),
                via_type_value=jp_cfg.get("via_type_value"),
            )
        sdl_cfg = None
        if "sdl_config" in mvc:
            sc = mvc["sdl_config"]
            sdl_columns = sc.get("columns") or []
            _candidate_tables = [jp.left_table, jp.right_table] if jp else []
            if jp and jp.via_table:
                _candidate_tables.append(jp.via_table)
            for _st in mvc.get("source_tables", []):
                if _st not in _candidate_tables:
                    _candidate_tables.append(_st)
            _pull_forward_descriptions(sdl_columns, _candidate_tables)
            sdl_cfg = SDLConfig(
                domain_id=sc["domain_id"],
                columns=sdl_columns,
            )
        # Default the target to the store the ACTIVE engine materializes into (DuckDB → mat_store,
        # not postgresql); an explicit config value still wins.
        _def_cat, _def_schema = state.federation_engine.materialize_store_target(state.org_id)
        # A view with SQL names its inputs in it; a join-pattern view's are bound here — the
        # tables it lists and any its pattern joins that the list leaves out.
        inputs = (
            []
            if mvc.get("sql")
            else _bind_join_inputs(
                mvc["id"], _joined_names(mvc.get("source_tables", []), jp), identities
            )
        )
        mv = MVDefinition(
            id=mvc["id"],
            source_tables=(
                [i.table_name for i in inputs] if inputs else mvc.get("source_tables", [])
            ),
            inputs=inputs,
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
        loaded.append(mv)

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

    # A ``views:`` block was turned into table entries where the config was read
    # (core/config_loader.py views_as_tables): those are stored by the load and registered by the
    # schema build like every view, so nothing of it is left to do here.

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
        if not src_source or not tgt_source:
            continue

        # REQ-1586: a junction-backed edge is a two-hop traversal, so the associative table
        # is a third leg of the join and its source counts when deciding whether the edge
        # crosses sources at all. A junction sitting in a different source from the tables it
        # links is exactly the case worth materializing even when those two tables agree.
        via_table = rel_cfg.get("via_table")
        if via_table:
            via_source = _table_source_map.get(via_table)
            if not via_source:
                raise ValueError(
                    f"Relationship {rel_cfg['id']!r} declares materialize: true through "
                    f"junction table {via_table!r}, which is not a registered table"
                )
            legs = {src_source, tgt_source, via_source}
        else:
            via_source = None
            legs = {src_source, tgt_source}

        if len(legs) > 1:
            mv_id = f"auto-mv-{rel_cfg['id']}"
            if state.mv_registry.get(mv_id) is not None:
                continue

            jp = JoinPattern(
                left_table=src_table,
                left_column=rel_cfg["source_column"],
                right_table=tgt_table,
                right_column=rel_cfg["target_column"],
                join_type="left",
                via_table=via_table,
                via_left_column=rel_cfg.get("via_source_column"),
                via_right_column=rel_cfg.get("via_target_column"),
                via_type_column=rel_cfg.get("via_type_column"),
                via_type_value=rel_cfg.get("via_type_value"),
            )
            source_tables = [src_table, tgt_table]
            if via_table:
                source_tables.insert(1, via_table)
            # The store the ACTIVE engine materializes into — never a hardcoded catalog
            # (Trino → provisa_admin, DuckDB → mat_store).
            _rel_cat, _rel_schema = state.federation_engine.materialize_store_target(state.org_id)
            mv = MVDefinition(
                id=mv_id,
                source_tables=source_tables,
                inputs=_bind_join_inputs(mv_id, source_tables, identities),
                target_catalog=_rel_cat,
                target_schema=_rel_schema,
                refresh_interval=rel_cfg.get("refresh_interval", 300),
                enabled=True,
                join_pattern=jp,
            )
            state.mv_registry.register(mv)
            loaded.append(mv)
            if via_table:
                logging.getLogger(__name__).info(
                    "Auto-materialized cross-source junction relationship %s (%s.%s → %s.%s → %s.%s)",
                    rel_cfg["id"],
                    src_source,
                    src_table,
                    via_source,
                    via_table,
                    tgt_source,
                    tgt_table,
                )
            else:
                logging.getLogger(__name__).info(
                    "Auto-materialized cross-source relationship %s (%s.%s → %s.%s)",
                    rel_cfg["id"],
                    src_source,
                    src_table,
                    tgt_source,
                    tgt_table,
                )

    return loaded


async def _init_ingest_engines() -> None:
    """Phase AS: Initialize ingest engines and DDL for ingest sources."""
    from provisa.api.app import state

    assert state.model_db is not None
    # Best-effort: ingest-source setup failing must not abort whole-server startup.
    with tolerate_startup_failure("ingest source init", exc_info=True):
        from provisa.ingest.engine import get_engine as _get_ingest_engine
        from provisa.ingest.ddl import generate_create_table as _gen_ddl
        from provisa.core.secrets import resolve_secrets as _resolve_secrets

        async with state.model_db.acquire() as _pg_conn:
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
        # REQ-1745: an ingest source (NO_CONNECTION_TYPES in the Sources form — REQ-1739
        # deliberately gives it no host/port/database fields, "since a checker's target lives on
        # the Table") has nothing of its own to connect with. It previously defaulted to the
        # literal localhost:5432 with an empty database/username — wrong on any deployment whose
        # control-plane postgres isn't on the machine-default port (the e2e harness's is docker-
        # assigned and ephemeral; REAL prod deployments dial a managed Cloud SQL host, never
        # localhost), so a UI-registered ingest source silently wrote rows nowhere reachable. The
        # only sensible default for "no connection configured" is the SAME tenant database
        # Provisa itself is already connected to. On Postgres that means reading off
        # state.model_db's own engine URL and opening a SEPARATE pool with it (ingest write
        # traffic gets its own pool, isolated from the admin-plane one). On SQLite -- the e2e
        # "core" lane and any local-dev ``--demo`` install both run PROVISA_DEMO's SQLite control
        # plane -- host/port/username don't exist to decompose, AND a second engine opened
        # against the SAME sqlite FILE deadlocks against state.model_db's own WAL-mode
        # connection (SQLAlchemy's default rollback-journal pool has no busy_timeout of its own
        # and never gets the WAL pragma _on_sqlite_connect sets on state.model_db's engine — see
        # provisa/core/database.py). So the SQLite/embedded case reuses state.model_db.engine
        # directly instead of opening a second engine at all -- "the SAME tenant database" taken
        # literally, not a look-alike connection to the same file.
        _tenant_url = state.model_db.engine.url
        _tenant_is_pg = _tenant_url.get_backend_name() == "postgresql"
        for _isrc in _ingest_sources:
            _sid = _isrc["id"]
            if _isrc["host"]:
                _pw = _resolve_secrets("")
                _eng = _get_ingest_engine(
                    source_id=_sid,
                    dialect=_isrc["dialect"] or "postgresql",
                    # The row holds what was written: a reference is resolved here, at its use.
                    host=_resolve_secrets(_isrc["host"]),
                    port=_isrc["port"] or 5432,
                    database=_resolve_secrets(_isrc["database"] or ""),
                    username=_resolve_secrets(_isrc["username"] or ""),
                    password=_pw or "",
                    # An ingest source row carries no PgBouncer setting (the sources table has no
                    # such column; SourceConfig.use_pgbouncer is config-only), so it is a direct
                    # connection — the same default SourceConfig.use_pgbouncer has.
                    use_pgbouncer=False,
                )
            elif _tenant_is_pg:
                # REQ-1730: state.model_db.acquire() scopes every control-plane connection to the
                # org's schema via a per-acquire `SET search_path` (core/database.py's
                # Database.acquire) — this raw engine has no such wrapper, so without an explicit
                # search_path its DDL/INSERT land wherever the role's own default resolves
                # (typically "public"), not the org schema `register_table` already stamped onto
                # this table's `registered_tables.schema_name` (schema_mutation_ops.py). Under a
                # SQLite control plane this was invisible — DuckDB's own sqlite ATTACH flattens
                # every schema into "main" regardless — but a Postgres control plane's real schema
                # boundaries (DuckDB's postgres ATTACH respects them, and Trino's postgres
                # connector requires them) exposed it: rows landed in "public", the compiled query
                # asked provisa_admin.org_<id> for them, found the (empty) table, "No results."
                from provisa.core.database import pg_uses_pgbouncer as _pg_uses_pgbouncer
                from provisa.core.environments import active_org_schema

                _pw = _resolve_secrets(_tenant_url.password or "")
                _sp = active_org_schema(state.org_id, "")
                _eng = _get_ingest_engine(
                    source_id=_sid,
                    dialect=_isrc["dialect"] or "postgresql",
                    host=_tenant_url.host or "localhost",
                    port=_tenant_url.port or 5432,
                    database=_tenant_url.database or "",
                    username=_tenant_url.username or "",
                    password=_pw or "",
                    search_path=_sp,
                    # Same database as the tenant control plane, so the same PgBouncer path.
                    use_pgbouncer=_pg_uses_pgbouncer(state.model_db.engine),
                )
            else:
                _eng = state.model_db.engine
            state.ingest_engines[_sid] = _eng
            _sqlite_backed = _eng.dialect.name == "sqlite"
            async with state.model_db.acquire() as _pg_conn:
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
            # NOTE (REQ-1730 investigation): the SQL page's compiler resolves a table through its
            # OWN compiled semantic name (compiler.sql_rewrite.semantic_table_name), derived from a
            # GraphQL field name -- and that round trip has no way to mark a word boundary right
            # before a digit, so it silently drops an underscore immediately followed by digits
            # (e.g. registered_tables.table_name "foo_123" compiles to the query-time name
            # "foo123"). ingest is the one type whose physical DDL uses table_name verbatim rather
            # than that same compiled name (every other type's landing/attach path creates its
            # physical table via the compiled name already, so physical == query-time name by
            # construction there) -- reproduced live via Trino: a row committed and was visible via
            # a fresh Postgres connection immediately after the POST, yet the SQL page's compiled
            # query always answered zero rows for a table_name containing "_<digits>". A fix
            # sourcing the compiled name from state.contexts here was tried and reverted: that
            # snapshot is only sometimes populated for this source_id at the moment a schema
            # rebuild calls this function (this function runs multiple times per rebuild), landing
            # on the WRONG table_name every other time and making the corruption non-deterministic
            # instead of consistent. Filed as a real, narrow gap (never register an ingest table
            # whose name contains an underscore immediately before a digit) rather than patched
            # here; REQ-1730's own swap-harness registrar works around it by choosing a sourceId
            # with no such boundary.
            _tbl_map: dict[str, list[dict]] = {}
            for _row in _itables:
                _tn = _row["table_name"] or ""
                _tbl_map.setdefault(_tn, []).append(
                    {
                        "column_name": _row["column_name"],
                        "path": _row["path"],
                        "data_type": _row["data_type"],
                    }
                )
            state.ingest_tables[_sid] = _tbl_map
            for _tn, _cols in _tbl_map.items():
                _ddl = _gen_ddl(_tn, _cols, sqlite=_sqlite_backed)
                with tolerate_startup_failure(f"ingest DDL for {_sid}.{_tn}"):
                    with _eng.begin() as _conn:
                        _conn.execute(__import__("sqlalchemy").text(_ddl))


def _graphql_remote_field_name(table_name: str) -> str:
    """The remote-schema field name a landed table's physical name derives to (REQ-1685):
    ``<domain>__<snake_words>`` collapses to camelCase on the words past the domain prefix."""
    snake_field = table_name.split("__", 1)[-1]
    parts = snake_field.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def _build_graphql_remote_table(tr: dict, col_rows: list[dict], source_id: str) -> dict:
    """One landed table's registration entry for ``state.graphql_remote_sources`` (REQ-1685), from
    its ``registered_tables`` row and its ``table_columns`` rows. A ``query_param`` column is a
    required call argument, not a selected field, so it is split into ``required_args`` instead of
    ``columns``. Malformed ``object_fields`` JSON is dropped rather than raised — the column still
    registers without its object-field shape rather than losing the whole table over one bad row.
    """
    columns: list[dict] = []
    required_args: list[dict] = []
    for cr in col_rows:
        if cr["native_filter_type"] == "query_param":
            required_args.append(
                {"name": cr["column_name"], "gql_type": "String", "provisa_type": "text"}
            )
            continue
        col_dict: dict = {"name": cr["column_name"], "type": cr["data_type"] or "text"}
        if cr["gql_selection"]:
            col_dict["gql_selection"] = cr["gql_selection"]
        raw_of = cr["object_fields"]
        if raw_of:
            # Malformed optional object-field metadata is skipped; any other error is a real
            # bug and must propagate.
            try:
                col_dict["gql_object_fields"] = (
                    json.loads(raw_of) if isinstance(raw_of, str) else raw_of
                )
            except json.JSONDecodeError:
                pass
        columns.append(col_dict)
    tname = tr["table_name"]
    return {
        "name": tname,
        "sql_name": tname,
        "field_name": _graphql_remote_field_name(tname),
        "source_id": source_id,
        "columns": columns,
        "domain_id": tr["domain_id"] or "",
        "description": tr["description"],
        "required_args": required_args,
    }


async def _graphql_remote_auth(state, src: dict) -> dict | None:
    """Rebuild a graphql_remote source's auth from its row: the scheme recorded in
    ``federation_hints`` at registration (graphql_remote_router._persist_source) and the
    credential its ``password_ref`` names, read from the org's vault (REQ-1695). None for a
    source registered with no auth."""
    from provisa.core import secrets_store
    from provisa.core.secrets import resolve_secrets

    auth_type = (src["federation_hints"] or {}).get("auth_type")
    if not auth_type:
        return None
    ref = src["password_ref"] or ""
    if "${secret:" in ref and secrets_store.bound_org_id() != state.active_org_id:
        async with secrets_store.bound(state.admin_db, state.active_org_id):
            secret = resolve_secrets(ref)
    else:
        secret = resolve_secrets(ref)
    if auth_type == "bearer":
        return {"type": "bearer", "token": secret}
    if auth_type == "basic":
        return {"type": "basic", "username": src["username"], "password": secret}
    raise ValueError(f"graphql_remote source {src['id']!r}: unknown auth_type {auth_type!r}")


def _apply_brand_table_spec(table: dict, brand, namespace: str) -> bool:
    """Give a branded source's registered table the way it is read (REQ-1923): its root field,
    row path, required arguments and page arguments come from the brand's shipped schema, which
    is where they were taken from when the table was registered. False when that schema no
    longer offers the table."""
    from provisa.graphql_remote.brands import table_spec

    spec = table_spec(brand, namespace, table["sql_name"])
    if spec is None:
        return False
    for key in ("name", "field_name", "gql_type_name", "required_args", "pagination", "rows_path"):
        if key in spec:
            table[key] = spec[key]
    return True


async def _load_graphql_remote_sources_from_db() -> None:
    """Load persisted graphql_remote sources from DB into state.graphql_remote_sources."""
    from provisa.api.app import state
    from provisa.core.secrets import resolve_secrets
    from provisa.graphql_remote.brands import NAMESPACE_HINT, brand_of

    if state.model_db is None:
        log.warning("[GQL REMOTE] model_db is None — skipping DB load")
        return
    # Best-effort: a DB/spec failure here must not abort startup — the server
    # comes up without the graphql_remote sources and logs why.
    with tolerate_startup_failure("graphql_remote sources from DB", exc_info=True):
        async with state.model_db.acquire() as _conn:
            src_rows = [
                dict(_r._mapping)
                for _r in (
                    await _conn.execute_core(
                        select(
                            _sources_t.c.id,
                            _sources_t.c.path,
                            _sources_t.c.username,
                            _sources_t.c.password_ref,
                            _sources_t.c.federation_hints,
                            _sources_t.c.mapping,
                        ).where(_sources_t.c.type == "graphql_remote")
                    )
                ).fetchall()
            ]
            for src in src_rows:
                source_id = src["id"]
                # A config-declared path may be a secret reference (${env:...}); the loader posts
                # to the resolved endpoint (REQ-1685).
                url = resolve_secrets(src["path"] or "")
                hints = src["federation_hints"] or {}
                brand = brand_of(hints)
                # A plain source's entry is kept up to date in this process as its tables are
                # registered (admin/_graphql_table_registration.remember_table), and holds the
                # schema read from its endpoint; it is loaded here only when this process never
                # saw it. A branded source's entry is rebuilt from the registry every time: how
                # its tables are read comes from the brand's shipped schema (REQ-1923).
                if brand is None and source_id in getattr(state, "graphql_remote_sources", {}):
                    continue
                # How each table a plain source registered one at a time is read, as stored
                # with the source when it was registered (REQ-308).
                specs = (src["mapping"] or {}).get("tables") or {}
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
                namespace = hints.get(NAMESPACE_HINT, "")
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
                                    _table_columns_t.c.gql_selection,
                                ).where(_table_columns_t.c.table_id == tr["id"])
                            )
                        ).fetchall()
                    ]
                    table = _build_graphql_remote_table(tr, col_rows, source_id)
                    # A table with no stored spec is a root field of its own name, as a model
                    # written by hand declares it; one with a spec is read as the spec says.
                    table.update(specs.get(tr["table_name"]) or {})
                    if brand is not None and not _apply_brand_table_spec(table, brand, namespace):
                        log.error(
                            "[GQL REMOTE] %s source %s: registered table %s is not in the "
                            "shipped schema and cannot be read",
                            brand.label,
                            source_id,
                            tr["table_name"],
                        )
                        continue
                    tables.append(table)
                if not hasattr(state, "graphql_remote_sources"):
                    state.graphql_remote_sources = {}
                state.graphql_remote_sources[source_id] = {
                    "source_id": source_id,
                    "url": url,
                    "namespace": namespace,
                    "domain_id": tables[0]["domain_id"] if tables else "",
                    "auth": await _graphql_remote_auth(state, src),
                    "cache_ttl": 300,
                    "tables": tables,
                    "functions": [],
                    "relationships": [],
                    **(
                        {
                            "brand": brand.id,
                            "error_policy": brand.error_policy,
                        }
                        if brand is not None
                        else {}
                    ),
                }
                log.warning(
                    "[GQL REMOTE] Loaded source %s from DB (%d tables)", source_id, len(tables)
                )


async def _load_grpc_remote_sources_from_db() -> None:  # REQ-1730
    """Rebuild ``state.grpc_remote_sources`` for every persisted grpc_remote source.

    Unlike graphql_remote's reload (``_load_graphql_remote_sources_from_db`` above), a live
    reconstruction here needs more than a stateless URL: it must recompile the proto stubs and
    reopen a gRPC channel, since ``state.grpc_remote_sources`` — built only by
    ``grpc_remote_router.register_grpc_remote_source`` — is pure in-process state that a process
    which never itself handled that POST (a fresh worker, a restart, or another engine's own
    backend under REQ-1730's DuckDB->Trino swap harness) starts with empty. Without this, a query
    against an already-registered grpc_remote table on such a process silently finds no
    registration to read from — verified live: the query hangs waiting on results that never land,
    with no error surfaced anywhere.

    Reuses ``_load_and_register`` (the exact logic the original POST ran) rather than duplicating
    it — its own upserts into ``sources``/``registered_tables``/``table_columns`` are already
    idempotent (ON CONFLICT DO UPDATE), so replaying it is safe. ``proto_path``/``namespace``/
    ``tls``/``import_paths``/``cache_ttl`` are read back from ``sources.path`` and
    ``sources.federation_hints`` (written by the original registration); ``auth_config``,
    ``method_overrides``, and ``relationships`` are not persisted and are not reconstructed here —
    best-effort, matching the reload's own framing elsewhere in this module."""
    from provisa.api.app import state

    if state.model_db is None:
        log.warning("[GRPC REMOTE] model_db is None — skipping DB load")
        return
    with tolerate_startup_failure("grpc_remote sources from DB", exc_info=True):
        async with state.model_db.acquire() as _conn:
            src_rows = [
                dict(_r._mapping)
                for _r in (
                    await _conn.execute_core(
                        select(
                            _sources_t.c.id,
                            _sources_t.c.host,
                            _sources_t.c.path,
                            _sources_t.c.federation_hints,
                        ).where(_sources_t.c.type == "grpc_remote")
                    )
                ).fetchall()
            ]
        for src in src_rows:
            source_id = src["id"]
            if source_id in getattr(state, "grpc_remote_sources", {}):
                continue
            # The row holds what was written: a reference is resolved here, at its use.
            proto_path = resolve_secrets(src["path"] or "")
            server_address = resolve_secrets(src["host"] or "")
            if not proto_path or not server_address:
                continue  # a row from before this reload path existed — nothing to rebuild from
            hints = src["federation_hints"] or {}
            namespace = hints.get("namespace", "")
            tls = hints.get("tls") == "true"
            import_paths = [p for p in (hints.get("import_paths") or "").split(",") if p]
            cache_ttl = int(hints.get("cache_ttl") or 300)
            from provisa.api.admin.grpc_remote_router import _load_and_register

            await _load_and_register(
                source_id,
                proto_path,
                server_address,
                namespace,
                "",
                import_paths,
                tls,
                None,
                cache_ttl,
                state,
            )
            log.warning("[GRPC REMOTE] Loaded source %s from DB", source_id)


async def _load_masking_rules(  # REQ-040, REQ-263, REQ-1677
    conn: Any,
    col_types_converted: dict[int, list[ColumnMetadata]],
    roles: list[dict],
    role_chains: dict[str, list[str]] | None = None,
) -> None:
    """Load masking rules from table_columns and publish them as state.masking_rules.

    REQ-1677: a role is exempt from a mask when it or an ancestor is in ``unmasked_to``.

    REQ-1914: the new set is built aside and published in ONE assignment at the end. Every
    worker now rebuilds on every model change, with requests running on their own threads
    meanwhile; a set cleared first and refilled here would answer those requests unmasked.
    """
    from provisa.api.app import state
    from provisa.security.inheritance import holds_grant
    from provisa.security.masking import MaskingRule, MaskType, validate_masking_rule

    chains = role_chains if role_chains is not None else {r["id"]: [r["id"]] for r in roles}
    rules: dict[Any, dict[str, Any]] = {}

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
            if holds_grant(role["id"], unmasked_to, chains):
                continue
            rules.setdefault((table_id, role["id"]), {})[col_name] = (mask_rule, data_type)
    state.masking_rules = rules


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

    assert state.model_db is not None, (
        "model_db must be initialized before loading tracked functions"
    )

    from provisa.discovery.catalog_cache import ensure_table as _ensure_catalog_cache

    await _ensure_catalog_cache(state.model_db)
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
    from provisa.compiler.naming import apply_convention as _apply_conv
    from provisa.compiler.naming import domain_gql_alias as _dgql

    _domains_cfg = (raw_config or {}).get("domains", []) or []
    _alias_stored = {
        d["id"]: d.get("graphql_alias") for d in _domains_cfg if isinstance(d, dict) and d.get("id")
    }

    def _domain_prefix(domain_id: str) -> str:
        return _dgql(domain_id, _alias_stored.get(domain_id))

    _dp = raw_config.get("naming", {}).get("domain_prefix", False) if raw_config else False
    # The GraphQL field name applies the active naming convention to the command name (add_pet ->
    # addPet under apollo_graphql), matching the schema generator (actions_schema.apply_gql_name).
    # Register under BOTH the raw registered name (the SQL/Cypher/gRPC/REST surfaces call it that)
    # AND the convention-cased GraphQL field name, so every surface's lookup resolves (REQ-1156).
    _conv = (raw_config or {}).get("naming", {}).get("convention", "apollo_graphql")

    def _register(target: dict, item: dict) -> None:
        target[item["name"]] = item  # raw registered name — non-GraphQL surfaces
        gql = _apply_conv(item["name"], _conv)
        key = (
            f"{_domain_prefix(item['domain_id'])}__{gql}" if _dp and item.get("domain_id") else gql
        )
        if key != item["name"]:
            target[key] = item  # GraphQL field name (convention-cased, maybe domain-prefixed)

    state.tracked_functions = {}
    for f in tracked_functions:
        _register(state.tracked_functions, f)
    state.tracked_webhooks = {}
    for w in tracked_webhooks:
        _register(state.tracked_webhooks, w)

    return tracked_functions, tracked_webhooks


def _drop_data_surface(state, role_id: str) -> None:
    """Remove whatever an earlier build registered for a role that now gets no data surface.

    The maps are built up across rebuilds of a live runtime, so "this build did not generate a
    schema for the role" is not the same as "the role has none": a role that HAD a domain when the
    last build ran still has that build's schema, context and proto in place. Leaving them would
    go on showing the role a catalog it no longer reaches.
    """
    for surface in (
        state.schemas,
        state.contexts,
        state.rls_contexts,
        state.table_path_maps,
        state.proto_files,
    ):
        surface.pop(role_id, None)


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
    metrics: list[dict],  # REQ-1319: config metric registry for schema projection
    field_numbers=None,  # FieldNumberAllocator | None (REQ-1903) — the caller owns load/persist
) -> None:
    """Build and register GraphQL schemas, contexts, and protos for each role."""
    from provisa.api.app import state

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

    from provisa.grpc.proto_gen import generate_proto

    def _schema_input(role: dict, tbls: list[dict], mtrcs: list[dict]) -> SchemaInput:
        return SchemaInput(
            tables=tbls,
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
            metrics=mtrcs,  # REQ-1319
        )

    # ``roles`` is every role the control plane holds now. The registry and the per-role maps
    # are built up across rebuilds of a live runtime, so a role deleted since the last build is
    # still in them: without this it keeps its rights (capability resolution reads state.roles)
    # and its schema, and goes on being served to whoever still names it.
    current = {role["id"] for role in roles}
    for gone in [role_id for role_id in state.roles if role_id not in current]:
        del state.roles[gone]
        _drop_data_surface(state, gone)

    for role in roles:
        state.roles[role["id"]] = role
        # REQ-1327: platform_admin is control-plane only and holds zero data capabilities in every
        # org, root included, so it gets NO data surface at all. Generating one produced a schema
        # whose every fully-granted table was filtered out by the column-visibility gate, leaving
        # only the native-filter endpoint tables — a silently truncated dataset instead of a refusal.
        # Without an entry, /data/graphql, the JSON:API, REST, Flight and SDL surfaces all answer
        # "No schema available for role 'platform_admin'". The role dict is still registered above:
        # control-plane capability resolution reads state.roles.
        #
        # REQ-1337: the test is the `cross_org` RIGHT the role carries, not its name — a deployment
        # that mints another control-plane role is kept off the data plane on the same terms.
        if Capability.CROSS_ORG.value in (role.get("capabilities") or []):
            _drop_data_surface(state, role["id"])
            continue
        # A role reaches the domains it lists, and one that lists NONE reaches no data: it gets no
        # data surface, on the same terms as the control-plane role above — every surface answers
        # "No schema available for role ...", which is a refusal, rather than a schema with
        # nothing in it. A role is refused at save with an empty list (rights.role_domain_problem);
        # this is the backstop for one that has ended up with none anyway, and it must not take
        # the org's whole build down with it. reaches_all_domains decides the single-domain
        # exemption.
        if not role["domain_access"] and not reaches_all_domains(role["domain_access"]):
            _drop_data_surface(state, role["id"])
            continue
        si = _schema_input(role, tables, metrics)
        from provisa.compiler.schema_gen import build_table_path_map

        # No swallow: a role missing from state.schemas here is served with a permanently cached
        # "no schema available" for every request to this org runtime (the build is cache-only-on-
        # success). Let generate_schema raise so a bad role definition fails the build loudly and
        # gets fixed at the source, matching generate_proto below.
        state.schemas[role["id"]] = generate_schema(si)
        state.table_path_maps[role["id"]] = build_table_path_map(si)
        state.contexts[role["id"]] = build_context(si)
        state.rls_contexts[role["id"]] = build_rls_context(
            rls_rules,
            role["id"],
        )

        # No swallow: an unmapped column type is a real gap in the proto type map, not a reason to
        # silently disable gRPC for the role. Let generate_proto raise so it surfaces at startup and
        # gets fixed at the source (the type map) — never patched around here.
        state.proto_files[role["id"]] = generate_proto(si, field_numbers=field_numbers)

    # REQ-045/REQ-143: the SERVED gRPC wire descriptor. A grpc.aio server registers exactly one
    # generated service and stock reflection serves exactly one descriptor pool, so the wire
    # descriptor must be the UNION of every role's surface — otherwise the compiled descriptor is
    # whichever role happens to come first (arbitrary dict order) and roles whose tables are absent
    # from it cannot be served at all. Governance is NOT expressed by the descriptor: every RPC
    # projects through state.contexts[role] (grpc/server.py _handle_query_bound) and is governed by
    # _govern_and_route_compiled, so a column the role cannot see is never SELECTed and its proto
    # field is left unset. Per-role client stubs still come from GET /data/proto/{role}.
    # A view's SQL is a model object: it is lowered to physical against the WHOLE model, not
    # against whichever role's context came first (which named only the tables THAT role could
    # see, and left a view over any other table unresolved — its refresh failed "schema does not
    # exist", and an inline view's meaning depended on role-id order). This context belongs to no
    # role and never answers a request; a view's rows are governed when the view is read.
    _model_role = {
        "id": "__model__",
        "domain_access": ["*"],
        # The catalog's governance columns are part of the model a view may read.
        "capabilities": [Capability.VIEW_GOVERNANCE.value],
    }
    _model_tables = [
        {**t, "columns": [{**c, "visible_to": ["*"]} for c in t["columns"]]} for t in tables
    ]
    state.view_context = build_context(
        _schema_input(_model_role, _model_tables, [{**m, "visible_to": []} for m in metrics])
    )

    _wire_role = {
        "id": "__wire__",
        "domain_access": ["*"],
        "capabilities": sorted({c for r in roles for c in (r.get("capabilities") or [])}),
    }
    _wire_tables = [
        {**t, "columns": [{**c, "visible_to": []} for c in t["columns"]]} for t in tables
    ]
    _wire_metrics = [{**m, "visible_to": []} for m in metrics]
    # REQ-1903: authoritative=True — this is the only call in the build that sees every column of
    # every table (visible_to=[]), so it's the only one allowed to retire a field number.
    state.wire_proto = generate_proto(
        _schema_input(_wire_role, _wire_tables, _wire_metrics),
        field_numbers=field_numbers,
        authoritative=True,
    )

    # REQ-1443: a YAML-registered checker table's contract names pgwire (semantic) names, which
    # exist only now that the contexts are compiled — config load could not check them.
    from types import SimpleNamespace

    from provisa.dq.contract import contract_dataset
    from provisa.dq.registration import check_contract_target, is_checker_source_type

    for t in tables:
        checker = state.source_types[t["source_id"]]
        if t.get("dq_contract") and is_checker_source_type(checker):
            check_contract_target(
                SimpleNamespace(schema_name=t["schema_name"], table_name=t["table_name"]),
                contract_dataset(t["dq_contract"], checker),
                state.contexts,
            )


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
    "tags",  # REQ-1373: the tag registry (system tags unioned in by the view)
    "tag_assignments",  # REQ-1377
    "tag_param_values",  # REQ-1467: the closed value list a parameterized tag accepts
    "tag_expiry",  # REQ-1375: expiring/expired assignments report (view-only, like roles_domain_access)
    "derived_tags",  # REQ-1443: computed tags (fact/dimension/data_quality); view-only
    "glossary_terms",  # REQ-1584: the business vocabulary, with its admission rule as a column
    "glossary_term_refs",  # REQ-1584: term -> column bindings, resolved to their address
    "glossary_term_edges",  # REQ-1584
    "glossary_term_experts",  # REQ-1584
]

# Only these carry a `tenant_id` column (schema.sql:581-586), so only these can be RLS-scoped
# by the `tenant_id`-based policy. The other three in _META_TABLES are deliberately excluded:
# `roles_domain_access` is a VIEW (ALTER TABLE ... ENABLE ROW LEVEL SECURITY errors on it), and
# `tracked_webhooks`/`tracked_functions` have no `tenant_id` (the policy's tenant_id reference
# would fail). All three are isolated TRANSITIVELY via their FK to a scoped parent in the app
# layer (meta_rls.py:37-39), so they need no Postgres policy.
_RLS_TENANT_TABLES = [
    "registered_tables",
    "table_columns",
    "domains",
    "relationships",
    "rls_rules",
    "roles",
    "tags",  # carries tenant_id (schema.sql tenant block)
    "tag_assignments",
    "tag_param_values",  # REQ-1467
    # REQ-1584: all four glossary base tables carry tenant_id, so all four take the policy. Their
    # meta views do not, for the same reason roles_domain_access does not — ALTER TABLE ... ENABLE
    # ROW LEVEL SECURITY errors on a view.
    "glossary_terms",
    "glossary_term_refs",
    "glossary_term_edges",
    "glossary_term_experts",
]


async def _init_meta_rls(conn: "Connection") -> None:  # REQ-041, REQ-402
    """Enable Postgres RLS on the tenant_id-bearing meta tables. Called only when multitenancy=True."""
    for tbl in _RLS_TENANT_TABLES:
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
