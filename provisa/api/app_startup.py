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

from provisa.federation.execution_auth import system_auth


from provisa.core.schema_org import (
    domains as _domains_t,
    sources as _sources_t,
)
from provisa.api_source.models import ApiEndpoint as ApiEndpoint, ApiSource as ApiSource
from provisa.core.config_location import config_path_str
from provisa.core.connection_loop import spawn_background, spawn_long_lived
from provisa.core.models import ProvisaConfig  # noqa: F401
from typing import TYPE_CHECKING, Any, cast  # noqa: F401

if TYPE_CHECKING:
    from provisa.scheduler.holder import SchedulerHolder

    pass


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
            await state.federation_engine.execute_engine(
                "SELECT 1", authorization=system_auth("startup warm-up")
            )  # warm the engine terminal
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


def _scheduler_holder(state: Any) -> "SchedulerHolder":  # REQ-1900
    """This process's claim on the deployment's scheduled and shared background work, created on
    first use and closed at shutdown (app.py lifespan)."""
    if state._scheduler_holder is None:
        from provisa.core.config_loader import load_control_plane
        from provisa.scheduler.holder import SchedulerHolder

        cp = load_control_plane(config_path_str())
        state._scheduler_holder = SchedulerHolder(cp.resolved_platform_url(), cp.resolved_org_id())
    return state._scheduler_holder


async def _start_background_tasks(_log: logging.Logger) -> None:
    """Start MV storage reclamation, warm-table, hot-table refresh, and SQLite staleness tasks."""

    # Start the MV reclamation loop whenever the engine is connected — not gated on MVs already
    # being registered. It idles cheaply on an empty registry and reaps removed/orphaned MV tables.
    # MV COMPUTE is the event loop's job now (REQ-966); this loop no longer refreshes MVs, so the two
    # never double-compute the same target table (Phase 6: legacy periodic CTAS refresh retired).
    # Gate on engine connectivity, not state.engine_conn: the latter is the Trino-only terminal and
    # is None for native engines (DuckDB), which still register MVs and accumulate orphan tables.
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle

    # REQ-1900: every worker process starts these loops. The ones that act on SHARED state run
    # only in the worker holding the scheduler lock (provisa/scheduler/holder.py):
    #   mv-reclamation     shared — drops tables in the shared materialization store.
    #   hot-table-refresh  shared when a Redis is configured (it rewrites the cached rows there);
    #                      this process's own when Redis is the embedded, in-process one.
    #   replica-hot        shared — it promotes and demotes busy tables in the shared replica
    #                      state (federation/replica_hot.evaluation_loop).
    #   idle reaper        NOT gated — it measures idleness from THIS process's activity.
    _holder = _scheduler_holder(state)

    # REQ-1915: spool files a build left when its process died are removed at node start. A
    # file another worker's running build holds is locked and left alone.
    from provisa.federation.replica_spool import spool_directory, sweep as _sweep_spool

    _sweep_spool(spool_directory())

    if state.federation_engine.is_connected():
        from provisa.mv.refresh import reclamation_loop

        # REQ-1882: long-lived loops that touch the engine/control plane run on their own
        # threads (spawn_long_lived), never on the process loop that relays request I/O.
        state._mv_refresh_task = spawn_long_lived(
            reclamation_loop(state.federation_engine, state.mv_registry, should_run=_holder.holds),
            name="mv-reclamation",
        )

    if state.federation_engine.is_connected():
        from provisa.core.boot_lock import expected_workers
        from provisa.federation.replica_hot import evaluation_loop as _hot_evaluation_loop

        # REQ-826: which busy tables are replicated is a decision taken once per deployment —
        # the holder's. It counts through the engine and writes the shared replica state.
        state._replica_hot_task = spawn_long_lived(
            _hot_evaluation_loop(state, should_run=_holder.holds, workers=expected_workers()),
            name="replica-hot",
        )

    if state.hot_manager is not None and state.federation_engine.is_connected():
        from provisa.cache.hot_tables import HotTableManager

        hot_mgr = state.hot_manager
        assert isinstance(hot_mgr, HotTableManager)

        # REQ-1913/REQ-231: the hot tier's interval, else the materialized-view default TTL.
        from provisa.cache.hot_tables import refresh_interval as _hot_refresh_interval

        _hot_interval = _hot_refresh_interval()

        async def _hot_refresh_loop() -> None:
            while True:
                await asyncio.sleep(_hot_interval)
                if state.redis_url is not None and not _holder.holds():
                    continue
                for entry in list(hot_mgr._hot_tables.values()):
                    if entry.is_api:
                        continue
                    try:
                        await hot_mgr.load_table(
                            state.federation_engine,
                            entry.table_id,
                            entry.table_name,
                            entry.schema,
                            entry.catalog,
                            entry.pk_column,
                        )
                    except Exception:
                        _log.exception("Hot table refresh failed: %s", entry.table_name)

        state._hot_refresh_task = spawn_long_lived(_hot_refresh_loop(), name="hot-table-refresh")

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
    never blocks boot.

    REQ-1793: skipped entirely on the SaaS instance (PROVISA_MULTITENANCY). The nag and the
    "Unregistered" badge both exist to capture registration contact details in place of telemetry
    (REQ-1137) — on the hosted SaaS instance a user already supplied those at sign-in/sign-up, so
    both would be redundant and are never shown there."""
    try:
        import os

        from provisa.licensing import emit

        if os.environ.get("PROVISA_MULTITENANCY", "").strip().lower() in ("1", "true", "yes"):
            from provisa.licensing.machine_id import stable_machine_id
            from provisa.licensing.state import LicensingState

            emit.set_state(
                LicensingState(
                    machine_id=stable_machine_id(),
                    first_seen="",
                    elapsed_days=0.0,
                    trial_expired=False,
                    licensed=True,
                    license_reason="SaaS instance — registration already captured at sign-up",
                )
            )
            return

        import datetime

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
    # REQ-1913: each of these is an operator setting, named here by its environment variable.
    from provisa.core import settings_registry

    def _path(own_env: str, node_key: str) -> str | None:
        own = settings_registry.value(settings_registry.key_for_env(own_env))
        return own if own is not None else settings_registry.value(node_key)

    cert, key = _path(cert_env, "tls.cert"), _path(key_env, "tls.key")
    if cert is not None and key is not None:
        return cert, key
    return None


async def _start_servers(_log: logging.Logger) -> None:
    """Start gRPC, Arrow Flight, pgwire, and APQ cache servers."""
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle

    _evaluate_licensing(_log)  # REQ-1135–1139: offline trial/license check + shell banner

    # REQ-1882 (amended 2026-09-29): every request runs on its own thread and loop; the process
    # loop only accepts connections and relays ASGI I/O. Work that outlives a request runs on the
    # background worker pool (server.background_workers), never here.
    from provisa.core.connection_loop import set_process_loop

    set_process_loop(asyncio.get_running_loop())

    if state.wire_proto:
        try:
            import tempfile
            from google.protobuf.internal import api_implementation
            from provisa.grpc.schema_gen import compile_proto
            from provisa.grpc.server import start_grpc_server

            # REQ-1904: the pure-Python protobuf backend is a silent multi-x serialization
            # slowdown versus the C++/upb backend — every gRPC row this process serializes pays
            # it, with nothing in a passing test or a working RPC to reveal it. Fails loudly here
            # (this repo's fail-closed convention, CLAUDE.md) rather than letting a misconfigured
            # environment (e.g. PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python, or a protobuf wheel
            # built without the upb extension) silently ship a working-but-slow server.
            _protobuf_impl = api_implementation.Type()
            if _protobuf_impl != "upb":
                raise RuntimeError(
                    f"protobuf backend is {_protobuf_impl!r}, not 'upb' — the pure-Python/cpp "
                    "backend is a multi-x serialization slowdown for gRPC. Check the installed "
                    "protobuf wheel and PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION."
                )

            # state.wire_proto is the UNION of every role's surface (app_loaders builds it). A
            # per-role proto would make the served descriptor depend on dict order and leave the
            # roles it omits unservable; governance is enforced per RPC from state.contexts[role],
            # never by which fields the wire descriptor happens to declare.
            grpc_output_dir = tempfile.mkdtemp(prefix="provisa_grpc_")
            pb2_path, pb2_grpc_path = compile_proto(state.wire_proto, grpc_output_dir)
            from provisa.core import settings_registry

            grpc_port = settings_registry.value("server.grpc_port")  # REQ-1913
            _grpc_tls = _resolve_tls("PROVISA_GRPC_CERT", "PROVISA_GRPC_KEY")
            state._grpc_server = start_grpc_server(
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

        from provisa.core import settings_registry

        flight_port_base = settings_registry.value("server.flight_port")  # REQ-1913
        _flight_tls = _resolve_tls("PROVISA_FLIGHT_CERT", "PROVISA_FLIGHT_KEY")
        if _flight_tls is not None:
            _fc, _fk = _flight_tls
            with open(_fc, "rb") as _f:
                _flight_cert_bytes = _f.read()
            with open(_fk, "rb") as _f:
                _flight_key_bytes = _f.read()
            from provisa.security.mtls import flight_tls_kwargs
            from provisa.security.mtls import resolve_client_auth as _resolve_client_auth

            def _build_flight_server(port: int) -> "ProvisaFlightServer":
                # grpc+tls scheme + tls_certificates make FlightServerBase bind a TLS listener
                # (REQ-1226). REQ-1228 adds verify_client + root_certificates when a client CA is
                # configured.
                return ProvisaFlightServer(
                    state,
                    location=f"grpc+tls://127.0.0.1:{port}",
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

            def _build_flight_server(port: int) -> "ProvisaFlightServer":
                return ProvisaFlightServer(
                    state,
                    location=f"grpc://127.0.0.1:{port}",
                )

        # REQ-1900: pyarrow's Flight server cannot share a port between processes (Arrow builds
        # its gRPC server with SO_REUSEPORT off and offers no switch), so under `--workers N` a
        # server per worker on the advertised port is not possible, and a port per worker leaves
        # the advertised one reaching a single worker. Each worker therefore runs its Flight
        # server on a loopback port the kernel picks (port 0 — nothing outside the host can dial
        # it, and no two workers can collide on it) and binds the ADVERTISED port itself with
        # SO_REUSEPORT, relaying each connection to its own server. See provisa/api/flight/relay.py
        # for what the relay does and does not touch (TLS stays end to end; the request still runs
        # on the Flight handler thread).
        flight_server = _build_flight_server(0)

        import threading

        flight_thread = threading.Thread(
            target=flight_server.serve,
            daemon=True,
        )
        flight_thread.start()
        state._flight_server = flight_server

        from provisa.api.flight.relay import FlightRelay

        state._flight_relay = FlightRelay(
            "0.0.0.0",  # nosec B104 - the Flight endpoint intentionally binds all interfaces
            flight_port_base,
            flight_server.port,
        )
        _log.info(
            "Arrow Flight server listening on %s:%d (TLS=%s)",
            state.hostname,
            flight_port_base,
            _flight_tls is not None,
        )
    except Exception:
        _log.exception("Arrow Flight server startup failed")

    from provisa.security.high_security import bolt_start_allowed, pgwire_start_allowed
    from provisa.security.mtls import apply_to_context, resolve_client_auth
    from provisa.security.sni import install as install_sni_capture

    from provisa.core import settings_registry

    pgwire_port = settings_registry.value("server.pgwire_port")  # REQ-1913; 0 = not started
    if pgwire_port and not pgwire_start_allowed(state, pgwire_port):
        # REQ-693: high-security mode never starts the pgwire server — the pgwire transport
        # has no per-connection client-side-decrypt handshake, so it cannot satisfy the
        # backend-never-sees-plaintext guarantee. Data reaches clients over KMS-gated HTTP only.
        _log.warning("pgwire server not started: security.mode=high (REQ-693)")
        pgwire_port = 0
    if pgwire_port:
        try:
            import ssl as _ssl
            from provisa.pgwire.server import start_pgwire_server

            # REQ-1623: the pgwire surface's search_path is NOT the control plane's. REQ-695 scopes
            # the internal asyncpg pool to org_<id>; a pgwire client never sees that schema — the
            # catalog presents tables under ``public`` (or a domain id) and ``current_schema()``
            # answers ``public``. Writing org_<id> into the reported setting here pointed every
            # client at a namespace holding none of its tables, named the BOOT org for every
            # session whatever org it was serving, and was a process-global one environment could
            # not have differed from anyway. The default ``public, "$user"`` is what this surface
            # actually presents.

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
            )
            _log.info(
                "pgwire server listening on 0.0.0.0:%d (TLS=%s)", pgwire_port, _ssl_ctx is not None
            )
        except Exception:
            _log.exception("pgwire server startup failed")

    bolt_port = settings_registry.value("server.bolt_port")  # REQ-1913; 0 = not started
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

    # Live Query Engines are each org's, started with its prod runtime once the process's
    # scheduler runs (provisa.api.app_rebuild.start_org_live_engine; REQ-1266).

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
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        from provisa.scheduler.jobs import new_scheduler

        # REQ-1900: every worker process starts this scheduler, and the deployment's jobs must
        # run once, not once per worker: the worker holding the scheduler lock on the platform
        # control plane runs them (provisa/scheduler/holder.py).
        # new_scheduler: a wakeup chain that never inherits a request's trace context -- see there.
        scheduler = new_scheduler(_scheduler_holder(state))
        # REQ-1003: scheduled triggers are each org's, in its model store, scheduled per org by
        # register_org_triggers (the deployment org's once the boot has loaded its model, every
        # other org's when its runtime is built) -- never read from the config file here.
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
        # REQ-1523: an environment carrying an expiry is deleted with its schema and its store when
        # it passes. Per-minute, because the expiry is a deadline an org was told — an hour-grained
        # sweep would make a one-hour environment last up to two. Registered only where the
        # environments table is: it lives on the platform registry beside ``orgs``, so a deployment
        # with no admin plane bound has no expiries to fire. ``max_instances=1`` because retirement
        # drops schemas, and a second sweep starting mid-drop would retire what the first is holding.
        if state.admin_db is not None:
            from provisa.scheduler.jobs import reap_environments

            scheduler.add_job(
                reap_environments,
                trigger=CronTrigger.from_crontab("* * * * *"),
                id="env_reap",
                name="environments:reap_expired",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
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
            from provisa.api.metadata_export.publishing import register_all_orgs

            # REQ-1882: reads the control plane — a background worker, not the process loop.
            spawn_background(register_all_orgs(scheduler), name="metadata-export-register")
        except (ImportError, RuntimeError):
            _log.exception("metadata export sync jobs could not be scheduled")
        # Wire the event loop onto the same scheduler (REQ-941) — best-effort, never bricks boot.
        try:
            from provisa.events.app_wiring import wire_event_loop

            spawn_background(
                wire_event_loop(scheduler, state=state, log=_log), name="event-loop-wiring"
            )
        except (ImportError, RuntimeError):
            _log.exception("event loop wiring could not be scheduled")
    except Exception:
        _log.exception("APScheduler startup failed")


async def _seed_sandbox_org(_log: logging.Logger) -> None:  # REQ-1598
    """Build the hosted platform's sandbox org, if this deployment is the hosted platform.

    The switch is the commerce seam, not an environment variable of its own: the sandbox org exists
    to hand a stranger the product the platform SELLS, so the deployments that need one are exactly
    the deployments that sell it. A self-hosted install has no public sign-in page, no open invite
    on it, and no reason to carry a spare org's schema.
    """
    from provisa.api.app import state  # lazy: avoid app<->app_startup cycle
    from provisa.core.commerce import enabled as commerce_enabled

    if not commerce_enabled():
        return
    if state.admin_db is None:
        raise RuntimeError(
            "REQ-1598: the sandbox org needs the control-plane registry, which is "
            "not bound — a commercial deployment always has one"
        )
    from provisa.api.sandbox_org import SANDBOX_ORG_ID, ensure_sandbox_org

    outcome = await ensure_sandbox_org(state.admin_db)
    _log.info("sandbox org %s: %s", SANDBOX_ORG_ID, outcome)


# (rel_id, source_table, target_table, source_column, target_column, cardinality, alias).
# Seeded for the auto-registered graphql-demo source. Every source_column MUST be a column the demo
# config lands for source_table (asserted by tests/unit/test_demo_relationship_keys.py): the mapper's
# _detect_relationships keys a many-to-one on the raw GQL object field and cannot name-match it
# (employee != employeeId), so it emits an empty source_column that these rows correct. schedules
# keys on the landed employee_id scalar (config gql_selection "employee_id: employee { id }"), not
# the raw employee object field -- keying on a column the config does not land refuses the startup
# column-drop of the stale object field.
_DEMO_GRAPHQL_RELATIONSHIPS: tuple[tuple[str, str, str, str, str, str, str | None], ...] = (
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
    (
        "gql_remote__graphql-demo__schedules__employee",
        "schedules",
        "employees",
        "employee_id",
        "id",
        "many-to-one",
        "IS_EMPLOYEE",
    ),
)


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
            _demo_pool = state.model_db
            if _demo_pool is not None:
                async with _demo_pool.acquire() as _conn:
                    await _conn.upsert(
                        _sources_t,
                        {
                            "origin": "seed",  # REQ-1919: written when the row is created
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
                            "origin": "seed",  # REQ-1919: written when the row is created
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
                    for (
                        _rel_id,
                        _src_tbl,
                        _tgt_tbl,
                        _src_col,
                        _tgt_col,
                        _card,
                        _alias,
                    ) in _DEMO_GRAPHQL_RELATIONSHIPS:
                        # No per-relationship catch: a seed that names a table/column the registry
                        # does not hold is a bug in the seed, not a tolerable miss -- let it fail the
                        # registration by name (rel_repo.upsert raises) rather than warn and leave
                        # the relationship silently absent.
                        await rel_repo.upsert(
                            _pg_rel,
                            Relationship(
                                id=_rel_id,
                                source_table_id=_src_tbl,
                                target_table_id=_tgt_tbl,
                                source_column=_src_col,
                                target_column=_tgt_col,
                                cardinality=Cardinality(_card),
                                alias=_alias,
                            ),
                            origin="seed",
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

    async def _register_graphql_demo_as_one_change() -> None:
        # REQ-1524: the seed writes the model (its source, tables and relationships), and every
        # model write is part of a change. This worker runs after the boot's change has closed,
        # so it opens its own.
        from provisa.core import model_change

        async with model_change.scope("register graphql-demo"):
            await _register_graphql_demo()

    # REQ-1882: introspects the demo service and rebuilds schemas — a background worker.
    spawn_background(_register_graphql_demo_as_one_change(), name="graphql-demo-register")


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
