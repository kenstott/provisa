# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Application startup orchestration (REQ boot sequence).

Background tasks, protocol servers, scheduler, JVM prewarm, and demo
auto-registration, invoked by app.lifespan. Extracted from app.py.
state / _rebuild_schemas / _reconcile_live_engine are imported lazily inside
each function to avoid an app <-> app_startup import cycle.
"""

# complexity-gate: allow-ble=16 reason="startup orchestration relocated verbatim from app.py; each broad except makes a boot phase (background task/server/scheduler/prewarm/demo-registration) best-effort — it logs and degrades that phase, never crashing boot/serve"

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

import asyncpg
import yaml

from sqlalchemy import select
from provisa.core.schema_org import (
    domains as _domains_t,
    registered_tables as _registered_tables_t,
    sources as _sources_t,
)
from provisa.api_source.models import ApiEndpoint as ApiEndpoint, ApiSource as ApiSource
from provisa.core.models import ProvisaConfig  # noqa: F401
from typing import TYPE_CHECKING, Any, cast  # noqa: F401

if TYPE_CHECKING:
    pass


log = logging.getLogger(__name__)


def _prewarm_govdata_jvm(_log: logging.Logger) -> None:
    """Start GovData JVM pre-warm in a background thread if govdata sources are active."""
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle

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
    """Start MV storage reclamation, warm-table, hot-table refresh, and SQLite staleness tasks."""
    # Start the MV reclamation loop whenever an engine terminal exists — not gated on MVs already
    # being registered. It idles cheaply on an empty registry and reaps removed/orphaned MV tables.
    # MV COMPUTE is the event loop's job now (REQ-966); this loop no longer refreshes MVs, so the two
    # never double-compute the same target table (Phase 6: legacy periodic CTAS refresh retired).
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle

    if state.engine_conn:
        from provisa.mv.refresh import reclamation_loop

        state._mv_refresh_task = asyncio.create_task(
            reclamation_loop(state.federation_engine, state.mv_registry),
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
                    rows = [
                        dict(_r._mapping)
                        for _r in (
                            await conn.execute_core(
                                select(
                                    _registered_tables_t.c.id,
                                    _registered_tables_t.c.table_name,
                                    _registered_tables_t.c.schema_name,
                                    _sources_t.c.path,
                                )
                                .select_from(
                                    _registered_tables_t.join(
                                        _sources_t,
                                        _sources_t.c.id == _registered_tables_t.c.source_id,
                                    )
                                )
                                .where(
                                    _sources_t.c.type == "sqlite",
                                    _sources_t.c.path.is_not(None),
                                )
                            )
                        ).fetchall()
                    ]
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
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle
    from provisa.api.app_rebuild import _reconcile_live_engine

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
                await _reconcile_live_engine(_lc)
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
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle

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
        # Wire the event loop onto the same scheduler (REQ-941) — best-effort, never bricks boot.
        try:
            import asyncio

            from provisa.events.app_wiring import wire_event_loop

            asyncio.ensure_future(wire_event_loop(scheduler, state=state, log=_log))
        except (ImportError, RuntimeError):
            _log.exception("event loop wiring could not be scheduled")
    except Exception:
        _log.exception("APScheduler startup failed")


async def _auto_register_graphql_demo(_log: logging.Logger) -> None:
    """Auto-register graphql-demo source if GRAPHQL_DEMO_URL or GRAPHQL_DEMO_ENABLED is set."""
    from provisa.api.app import _rebuild_schemas, state  # lazy: avoid app<->app_startup cycle

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
                    await _conn.upsert(
                        _sources_t,
                        {
                            "id": "graphql-demo",
                            "type": "graphql_remote",
                            "host": "",
                            "port": 0,
                            "database": "",
                            "username": "",
                            "dialect": "",
                            "path": _graphql_demo_url,
                            "description": (
                                "Animal shelter GraphQL API — staff schedules, breed catalogue, "
                                "and animal assignment records managed by shelter operations"
                            ),
                        },
                        index_elements=["id"],
                        update_columns=["path", "description"],
                    )
                    await _conn.upsert(
                        _domains_t,
                        {
                            "id": "shelter",
                            "description": "Animal shelter staff and breed management",
                        },
                        index_elements=["id"],
                        update_columns=[],
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
                    _pg_rel = _rel_conn
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
