# Copyright (c) 2026 Kenneth Stott
# Canary: 32761564-4291-462b-a2d6-d4f1bb5d249f
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

# complexity-gate: allow-ble=17 reason="startup orchestration relocated verbatim from app.py; each broad except makes a boot phase (background task/server/scheduler/prewarm/demo-registration/config-snapshot) best-effort — it logs and degrades that phase, never crashing boot/serve"

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

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
    from provisa.core.database import Connection


log = logging.getLogger(__name__)


async def _warmup_readiness(_log: logging.Logger) -> None:
    """Prime the lazy per-request paths so a user's FIRST interaction is not the cold one, then flip
    readiness (state.is_warm → /ready returns 200).

    Lifespan completing (and /health) only means dependencies are up. The first data query still
    lazily attaches the materialize store, opens the engine terminal, and initializes the transpiler
    — tens of seconds under load, which read as "the UI hung." This runs those once at boot:

      - cache_catalog() attaches AND boot-validates the API-result-cache / materialization store, so a
        misconfigured store fails in the STARTUP log, not mid-query after the browser already opened
        (the exact class of failure that once surfaced as a broken app).
      - a SELECT 1 engine probe warms the engine connection, the transpile path, and the result
        pipeline.

    Best-effort: warmup is an optimization, so a failure must NOT wedge readiness (a launcher would
    never open the browser). It logs loudly and still flips ready; the same operation re-runs and
    surfaces any genuine error on the real query.
    """
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle

    try:
        if state.federation_engine.is_connected():
            state.federation_engine.cache_catalog()  # attach + boot-validate the store
            await state.federation_engine.execute_engine("SELECT 1")  # warm the engine terminal
    except Exception:
        _log.exception("readiness warmup probe failed; serving anyway")

    # Prime the admin GraphQL landing queries the UI hits first (Tables, Relationships,
    # Sources). Their resolvers open the control-plane pool and read per-table
    # columns for every registered table — cold on the first request, which reads as
    # "Loading tables…" hanging. Running them here (behind /ready) means the browser
    # opens onto warm pages. context_value={} → anonymous identity (dev/demo allows all).
    #
    # `{ domains { id } }` is NOT in this list: its resolver calls _resolve_admin_context, which
    # requires a request-bound active org (REQ-1293 — the tenant plane is isolated by schema), so
    # at boot it raised GraphQLError("'request'") on every start. There is no org to warm it for,
    # and the three queries above already open the same control-plane pool.
    try:
        from provisa.api.admin.schema import admin_schema

        for _q in (
            "{ tables { id } }",
            "{ relationships { id } }",
            "{ sources { id } }",
        ):
            _res = await admin_schema.execute(_q, context_value={})
            if _res.errors:
                _log.warning("warmup admin query %s: %s", _q, _res.errors)
    except Exception:
        _log.exception("admin warmup queries failed; serving anyway")

    state.is_warm = True
    _log.warning("startup phase %-20s ready", "warmup")


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
    # Start the MV reclamation loop whenever the engine is connected — not gated on MVs already
    # being registered. It idles cheaply on an empty registry and reaps removed/orphaned MV tables.
    # MV COMPUTE is the event loop's job now (REQ-966); this loop no longer refreshes MVs, so the two
    # never double-compute the same target table (Phase 6: legacy periodic CTAS refresh retired).
    # Gate on engine connectivity, not state.engine_conn: the latter is the Trino-only terminal and
    # is None for native engines (DuckDB), which still register MVs and accumulate orphan tables.
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle

    if state.federation_engine.is_connected():
        from provisa.mv.refresh import reclamation_loop

        state._mv_refresh_task = asyncio.create_task(
            reclamation_loop(state.federation_engine, state.mv_registry),
        )

    if state.federation_engine.is_connected():
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

    if state.hot_manager is not None and state.federation_engine.is_connected():
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
                    _sc = cast("Connection", conn)  # core Connection (proxies asyncpg)
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

    # REQ-1448: release the node under any engine shard that stops being queried. Started here with
    # the other background loops; it returns immediately on a deployment that does not provision its
    # own engines, where there is no node to release.
    from provisa.federation.engine_wake import start_idle_reaper

    start_idle_reaper(state)


def _evaluate_licensing(_log: logging.Logger) -> None:
    """Evaluate the offline trial/license once at startup; install state + shell banner (REQ-1137).

    The evaluated state is shared with every protocol surface via ``licensing.emit``; the surfaces
    emit the nag through their own out-of-band notice channels. When the trial has expired with no
    valid license, the "persistent shell banner" is the startup log line here. Fully offline —
    never blocks boot."""
    try:
        import datetime

        from provisa.licensing import emit
        from provisa.licensing.state import evaluate

        today = datetime.date.today()
        state = evaluate(now_epoch=today.toordinal() * 86400, today_iso=today.isoformat())
        emit.set_state(state)
        if state.should_nag:
            _log.warning("[Provisa] %s", state.nag_text)
    except Exception:
        # Licensing must NEVER block or degrade the product (REQ-1137) — a failure just skips the nag.
        _log.exception("licensing evaluation failed; continuing without a nag")


def _resolve_tls(cert_env: str, key_env: str) -> tuple[str, str] | None:
    """Per-server cert/key from its own env vars, else the node-wide PROVISA_TLS_CERT/KEY pair.

    REQ-1226: every protocol endpoint serves TLS in a cluster deploy. Certs are provisioned once per
    node — first-launch.sh generates a self-signed pair when none is supplied — and every server
    points at the same pair unless a per-protocol override is set."""
    cert = os.environ.get(cert_env) or os.environ.get("PROVISA_TLS_CERT")
    key = os.environ.get(key_env) or os.environ.get("PROVISA_TLS_KEY")
    if cert and key:
        return cert, key
    return None


async def _start_servers(_log: logging.Logger) -> None:
    """Start gRPC, Arrow Flight, pgwire, Live Query Engine, and APQ cache servers."""
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle
    from provisa.api.app_rebuild import _reconcile_live_engine

    _evaluate_licensing(_log)  # REQ-1135–1139: offline trial/license check + shell banner

    if state.wire_proto:
        try:
            import tempfile
            from provisa.grpc.schema_gen import compile_proto
            from provisa.grpc.server import start_grpc_server

            # state.wire_proto is the UNION of every role's surface (app_loaders builds it). A
            # per-role proto would make the served descriptor depend on dict order and leave the
            # roles it omits unservable; governance is enforced per RPC from state.contexts[role],
            # never by which fields the wire descriptor happens to declare.
            grpc_output_dir = tempfile.mkdtemp(prefix="provisa_grpc_")
            pb2_path, pb2_grpc_path = compile_proto(state.wire_proto, grpc_output_dir)
            grpc_port = int(
                os.environ.get("GRPC_PORT", str(state.server_cfg.get("grpc_port", 50051)))
            )
            _grpc_tls = _resolve_tls("PROVISA_GRPC_CERT", "PROVISA_GRPC_KEY")
            state._grpc_server = await start_grpc_server(
                grpc_port,
                state,
                pb2_path,
                pb2_grpc_path,
                tls=_grpc_tls,
            )
            _log.info(
                "gRPC server listening on %s:%d (TLS=%s)",
                state.hostname,
                grpc_port,
                _grpc_tls is not None,
            )
        except Exception:
            _log.exception("gRPC server startup failed")

    try:
        from provisa.api.flight.server import ProvisaFlightServer

        flight_port = int(
            os.environ.get("FLIGHT_PORT", str(state.server_cfg.get("flight_port", 8815)))
        )
        _flight_tls = _resolve_tls("PROVISA_FLIGHT_CERT", "PROVISA_FLIGHT_KEY")
        if _flight_tls is not None:
            _fc, _fk = _flight_tls
            with open(_fc, "rb") as _f:
                _flight_cert_bytes = _f.read()
            with open(_fk, "rb") as _f:
                _flight_key_bytes = _f.read()
            # grpc+tls scheme + tls_certificates make FlightServerBase bind a TLS listener (REQ-1226).
            # REQ-1228 adds verify_client + root_certificates when a client CA is configured.
            from provisa.security.mtls import flight_tls_kwargs
            from provisa.security.mtls import resolve_client_auth as _resolve_client_auth

            flight_server = ProvisaFlightServer(
                state,
                location=f"grpc+tls://0.0.0.0:{flight_port}",
                main_loop=asyncio.get_running_loop(),
                tls_certificates=[(_flight_cert_bytes, _flight_key_bytes)],
                **flight_tls_kwargs(
                    _resolve_client_auth(
                        "PROVISA_FLIGHT_CLIENT_CA",
                        "PROVISA_FLIGHT_MTLS_MODE",
                        "PROVISA_FLIGHT_MTLS_BIND_PRINCIPAL",
                    )
                ),
            )
        else:
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
        _log.info(
            "Arrow Flight server listening on %s:%d (TLS=%s)",
            state.hostname,
            flight_port,
            _flight_tls is not None,
        )
    except Exception:
        _log.exception("Arrow Flight server startup failed")

    from provisa.security.high_security import bolt_start_allowed, pgwire_start_allowed
    from provisa.security.mtls import apply_to_context, resolve_client_auth
    from provisa.security.sni import install as install_sni_capture

    pgwire_port = int(os.environ.get("PROVISA_PGWIRE_PORT", "0"))
    if pgwire_port and not pgwire_start_allowed(state, pgwire_port):
        # REQ-693: high-security mode never starts the pgwire server — the pgwire transport
        # has no per-connection client-side-decrypt handshake, so it cannot satisfy the
        # backend-never-sees-plaintext guarantee. Data reaches clients over KMS-gated HTTP only.
        _log.warning("pgwire server not started: security.mode=high (REQ-693)")
        pgwire_port = 0
    if pgwire_port:
        try:
            import ssl as _ssl
            from provisa.pgwire import catalog as _pgwire_catalog
            from provisa.pgwire.server import start_pgwire_server

            _pgwire_catalog._KNOWN_SETTINGS["search_path"] = f"org_{state.org_id}"  # REQ-695

            _ssl_ctx: _ssl.SSLContext | None = None
            _pgwire_tls = _resolve_tls("PROVISA_PGWIRE_CERT", "PROVISA_PGWIRE_KEY")
            _pgwire_mtls = None
            if _pgwire_tls is not None:
                _ssl_ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
                _ssl_ctx.load_cert_chain(*_pgwire_tls)
                # REQ-1228: client-certificate verification, when the deployment configures a CA.
                _pgwire_mtls = resolve_client_auth(
                    "PROVISA_PGWIRE_CLIENT_CA",
                    "PROVISA_PGWIRE_MTLS_MODE",
                    "PROVISA_PGWIRE_MTLS_BIND_PRINCIPAL",
                )
                apply_to_context(_ssl_ctx, _pgwire_mtls)
                # REQ-1234: record the hostname the client dialed, so a pgwire connection to
                # acme.provisa.dev requests org 'acme' the way an HTTP Host header does.
                install_sni_capture(_ssl_ctx)

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
    if bolt_port and not bolt_start_allowed(state, bolt_port):
        # REQ-693: high-security mode never starts the Bolt server — Bolt's HELLO/LOGON exchange
        # negotiates a credential, not a decryption context, so a Cypher result would cross the
        # wire as plaintext rows the backend had already seen.
        _log.warning("bolt server not started: security.mode=high (REQ-693)")
        bolt_port = 0
    if bolt_port:
        try:
            import ssl as _ssl_bolt
            from provisa.bolt.server import start_bolt_server

            _bolt_ssl_ctx: _ssl_bolt.SSLContext | None = None
            _bolt_tls = _resolve_tls("PROVISA_BOLT_CERT", "PROVISA_BOLT_KEY")
            if _bolt_tls is not None:
                _bolt_ssl_ctx = _ssl_bolt.SSLContext(_ssl_bolt.PROTOCOL_TLS_SERVER)
                _bolt_ssl_ctx.load_cert_chain(*_bolt_tls)
                # REQ-1228: same client-certificate policy pgwire applies, on Bolt's listener.
                apply_to_context(
                    _bolt_ssl_ctx,
                    resolve_client_auth(
                        "PROVISA_BOLT_CLIENT_CA",
                        "PROVISA_BOLT_MTLS_MODE",
                        "PROVISA_BOLT_MTLS_BIND_PRINCIPAL",
                    ),
                )
                # REQ-1234: the same hostname capture pgwire installs, on Bolt's listener.
                install_sni_capture(_bolt_ssl_ctx)

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

    # REQ-1008: MCP server (opt-in via PROVISA_MCP_PORT). Isolated one-line hook;
    # touches no scheduler/freshness/audit/meta-view code.
    try:
        from provisa.api.mcp import start_mcp_server

        start_mcp_server(state, _log)
    except (ImportError, OSError, RuntimeError, ValueError):
        # Opt-in server (PROVISA_MCP_PORT). Missing SDK (ImportError), port bind (OSError),
        # config/validation (ValueError/RuntimeError) must not abort app boot; anything else is
        # unexpected and propagates loudly.
        _log.exception("MCP server startup failed")

    # REQ-1120: airport Flight service (opt-in via PROVISA_AIRPORT_PORT). Serves the DuckDB
    # `airport` community-extension protocol over the governed query pipeline. Isolated hook.
    try:
        from provisa.api.airport import start_airport_server

        start_airport_server(state, _log)
    except (ImportError, OSError, RuntimeError, ValueError):
        # Opt-in server (PROVISA_AIRPORT_PORT). Missing dep (ImportError), port bind (OSError),
        # config/validation (ValueError/RuntimeError) must not abort app boot; anything else is
        # unexpected and propagates loudly.
        _log.exception("airport server startup failed")

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
        from apscheduler.triggers.interval import IntervalTrigger

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
        from provisa.scheduler.jobs import (
            compact_otel_signals,
            reclaim_otel_storage,
            watch_engine,
        )

        scheduler.add_job(
            compact_otel_signals,
            trigger=CronTrigger.from_crontab(state.otel_compact_cron),
            id="otel_compact",
            name="otel:compact_signals",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        # Hourly, not per-minute: expire_snapshots + remove_orphan_files rewrite table metadata and
        # list the whole object store. Nothing ran them before, so 93 MiB of data sat behind 57 GiB
        # of unreferenced files and filled the coordinator disk (REQ-303).
        scheduler.add_job(
            reclaim_otel_storage,
            trigger=CronTrigger.from_crontab("0 * * * *"),
            id="otel_reclaim",
            name="otel:reclaim_storage",
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
        # REQ-1452/REQ-1455: drain the in-memory egress reports into the meter. Registered here
        # rather than by the plugin because the transports that report bytes are core code, and
        # the drain no-ops without the plugin (``meter_egress`` is a plugin passthrough).
        from provisa.core.egress import DRAIN_INTERVAL_SECONDS, drain_job

        scheduler.add_job(
            drain_job,
            trigger=IntervalTrigger(seconds=DRAIN_INTERVAL_SECONDS),
            id="egress_drain",
            name="billing:egress_drain",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        # The commercial plugin's own scheduled billing work (the REQ-1455 trial sweep). A
        # deployment without the plugin registers nothing.
        from provisa.core.commerce import schedule_jobs

        schedule_jobs(scheduler)

        scheduler.start()
        state._scheduler = scheduler
        _log.info("APScheduler started")
        # REQ-1072: the metadata-export drain + per-org reconcile. Scheduled after start so a
        # config read that needs the running loop has one.
        try:
            import asyncio

            from provisa.api.metadata_export.publishing import register_all_orgs

            asyncio.ensure_future(register_all_orgs(scheduler))
        except (ImportError, RuntimeError):
            _log.exception("metadata export sync jobs could not be scheduled")
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
    """Auto-register the graphql-demo source when GRAPHQL_DEMO_ENABLED is truthy.

    GRAPHQL_DEMO_ENABLED is the only switch. It previously also fired on a set GRAPHQL_DEMO_URL,
    but docker-compose.app.yml:33 always injects that variable (defaulting to the compose service
    hostname), so ``GRAPHQL_DEMO_ENABLED=false`` never took effect: every deploy without a
    graphql-demo container introspected ``graphql-demo:4000`` at startup and raised
    ``Errno -3 Temporary failure in name resolution``. The URL says WHERE the demo lives, not
    WHETHER it exists.
    """
    from provisa.api.app import _rebuild_schemas, state  # lazy: avoid app<->app_startup cycle

    if os.environ.get("GRAPHQL_DEMO_ENABLED", "").lower() not in ("1", "true", "yes"):
        return
    _graphql_demo_url = os.environ.get("GRAPHQL_DEMO_URL", "http://graphql-demo:4000/graphql")

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


async def _capture_config_boot_snapshot(_log: logging.Logger) -> None:
    """Snapshot the config generated from live state ONCE at end of boot — after all runtime
    auto-derivation (FK tracking, graphql-remote) — as the admin config-diff baseline, so the diff
    shows only changes made SINCE startup (REQ-164). Opt-in via ``config_live_export``; another boot
    phase best-effort — a failure degrades the diff to the on-disk file, never bricking boot."""
    from provisa.api.app import state

    if not getattr(state, "config_live_export", False):
        return
    try:
        from provisa.api.admin.config_export import build_live_config_yaml

        state.config_boot_snapshot = await build_live_config_yaml()
    except Exception:
        _log.exception("Failed to capture config boot snapshot")
