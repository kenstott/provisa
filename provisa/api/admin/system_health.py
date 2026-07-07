# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""System health probe for the admin dashboard.

Collects federation-engine, PG pool, cache, and per-protocol liveness into a
``SystemHealthType``. Lives outside ``schema.py`` to keep that module within its
complexity budget.

The separate socket listeners (gRPC, Arrow Flight, pgwire, bolt) fail
independently of the HTTP app, so each is probed with a short TCP connect:
port ``None`` = the protocol was never started (disabled); otherwise the probe
distinguishes "running" from "down". gRPC/Flight report started via their server
handle on ``AppState``; pgwire/bolt are env-port gated.
"""

from __future__ import annotations

import asyncio
import os

from redis.exceptions import RedisError

from provisa.api.admin.types import ProtocolHealthType, SystemHealthType

_GRPC_DEFAULT_PORT = 50051
_FLIGHT_DEFAULT_PORT = 8815
_PROBE_TIMEOUT_S = 0.5


async def _probe_tcp(port: int | None) -> str:
    """TCP-connect liveness for one listener. None port => disabled."""
    if port is None:
        return "disabled"
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", port), timeout=_PROBE_TIMEOUT_S
        )
        writer.close()
        await writer.wait_closed()
        return "running"
    except (OSError, asyncio.TimeoutError):
        return "down"


async def collect_system_health() -> SystemHealthType:
    """Assemble the current system health snapshot."""
    from provisa.api.app import state
    from provisa.cache.store import RedisCacheStore

    engine_ok, worker_count, active = state.federation_engine.cluster_diagnostics()

    # Tenant metadata DB (control-plane, org-scoped) — any SQLAlchemy dialect, not just PG.
    md_size = md_free = 0
    md_dialect = ""
    if state.tenant_db is not None:
        md_size = state.tenant_db.get_size()
        md_free = state.tenant_db.get_idle_size()
        md_dialect = state.tenant_db.dialect

    # Cache: NoopCacheStore = disabled; RedisCacheStore with no URL = embedded fakeredis
    # (always up, in-process); with a URL = a real server that may be online or not.
    cache_mode, cache_ok = "disabled", False
    store = state.response_cache_store
    if isinstance(store, RedisCacheStore):
        if store._redis_url:
            cache_mode = "server"
            try:
                await store._connect()
                assert store._redis is not None
                await store._redis.ping()
                cache_ok = True
            except (RedisError, OSError, AssertionError):
                pass
        else:
            cache_mode, cache_ok = "embedded", True

    cfg = state.server_cfg
    ports = {
        "gRPC": int(os.environ.get("GRPC_PORT") or cfg.get("grpc_port") or _GRPC_DEFAULT_PORT)
        if state._grpc_server is not None
        else None,
        "Arrow Flight": int(
            os.environ.get("FLIGHT_PORT") or cfg.get("flight_port") or _FLIGHT_DEFAULT_PORT
        )
        if state._flight_server is not None
        else None,
        "pgwire": int(os.environ.get("PROVISA_PGWIRE_PORT", "0")) or None,
        "bolt": int(os.environ.get("PROVISA_BOLT_PORT", "0")) or None,
    }
    statuses = await asyncio.gather(*(_probe_tcp(p) for p in ports.values()))
    protocols = [
        ProtocolHealthType(name=name, status=status, port=port)
        for (name, port), status in zip(ports.items(), statuses)
    ]

    return SystemHealthType(
        engine_connected=engine_ok,
        engine_worker_count=worker_count,
        engine_active_workers=active,
        metadata_pool_size=md_size,
        metadata_pool_free=md_free,
        metadata_dialect=md_dialect,
        cache_mode=cache_mode,
        cache_connected=cache_ok,
        protocols=protocols,
        mv_refresh_loop_running=hasattr(state, "_mv_refresh_task")
        and state._mv_refresh_task is not None,
    )
