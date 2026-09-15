# Copyright (c) 2026 Kenneth Stott
# Canary: 1e6983ba-6d63-40bf-a7a1-847d19a798ff
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wire kafka/websocket push-source listeners into the live app at boot (REQ-1733).

Closes a documented gap: ``provisa/events/app_wiring.py``'s own docstring says "Other API/push
types (ingest, websocket, ...) still have no wired fetch" and ``register_runtime``'s says "Push
nodes' listeners are started by the processor (``consume_kafka``) — the app wires the consumer" —
nothing did. The pieces already existed and were individually unit-tested
(``KafkaNotificationProvider``, ``WebSocketNotificationProvider``,
``subscriptions.cdc_landing.consume_cdc_into_store``) but nothing at boot ever built a real
provider and started the drain loop, so registering a kafka/websocket source landed nothing,
forever, with one buried log line (the exact symptom ``app_wiring.py`` describes for
``UnsupportedSourceFetch``).

This is a SEPARATE mechanism from ``app_wiring.wire_event_loop``'s poll/MV tick loop: CDC landing
(upsert/delete by primary key, ``store_writer.py``'s own docstring: "Hard-delete CDC is the
separate streaming path") is not expressible through the generic ``land_source_table`` write face
(replace/append shapes only), so it runs as its own independent asyncio task per push table,
draining ``provider.watch()`` straight into the landed table via ``consume_cdc_into_store``.

Best-effort per table, matching ``wire_event_loop``'s own posture: one misconfigured push table
(no primary key, no topic, unreachable broker) is logged and skipped, never aborts wiring for
every other table.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

log = logging.getLogger(__name__)

_PUSH_SOURCE_TYPES = frozenset({"kafka", "websocket"})


def _build_provider(src: Any, tbl: dict, *, node: str) -> tuple[Any, str] | None:
    """A (NotificationProvider, watch_target) pair for *src*, or None (logged) when its config is
    incomplete. ``watch_target`` is what gets passed to ``provider.watch()`` — the Kafka topic for
    kafka, the registered table name for websocket (one socket, one stream, no per-topic routing)."""
    from provisa.subscriptions.registry import get_provider

    source_type = src.type.value if hasattr(src.type, "value") else str(src.type)

    if source_type == "kafka":
        live = tbl.get("live") or {}
        kafka_cfg = (live.get("kafka") or {}) if isinstance(live, dict) else {}
        topic = kafka_cfg.get("topic")
        if not topic:
            log.warning(
                "push listener %s: kafka source %r has no live.kafka.topic configured — skipping "
                "(register the table with live={strategy: kafka, kafka: {topic: ...}})",
                node,
                src.id,
            )
            return None
        if not src.host:
            log.warning(
                "push listener %s: kafka source %r has no bootstrap_servers (Source.host) — "
                "skipping",
                node,
                src.id,
            )
            return None
        provider = get_provider(
            "kafka", {"bootstrap_servers": src.host, "group_id": f"provisa-{src.id}"}
        )
        return provider, topic

    if source_type == "websocket":
        # REQ-1733: websocket has no dedicated URL field in the Sources form yet — base_url (an
        # explicit override) else derive ws://host:port from host+port, the SAME override-else-
        # derive shape airport/neo4j already use (REQ-1730, DuckDBAirportConnector /
        # neo4j_config_from_source) rather than inventing a third convention.
        url = src.base_url or (f"ws://{src.host}:{src.port}" if src.host else None)
        if not url:
            log.warning(
                "push listener %s: websocket source %r has no base_url or host — skipping",
                node,
                src.id,
            )
            return None
        hints = src.federation_hints or {}
        provider = get_provider(
            "websocket",
            {
                "url": url,
                "subscribe_payload": hints.get("subscribe_payload"),
                "event_path": hints.get("event_path"),
                "reconnect_interval": float(hints.get("reconnect_interval") or 5.0),
            },
        )
        return provider, node

    return None


async def wire_push_listeners(*, state: Any, log: Any) -> list[asyncio.Task]:
    """Start one background CDC-landing task per registered table on a kafka/websocket source.

    Idempotent: a node already running (tracked in ``state.push_listener_disconnects``) is
    skipped, so calling this again after a runtime re-wire (e.g. a new table registered) only
    starts listeners for tables that don't have one yet. Returns the tasks started THIS call
    (empty on a pure re-wire where every push table already has a listener)."""
    db = getattr(state, "tenant_db", None)
    engine = getattr(state, "federation_engine", None)
    if db is None or engine is None:
        return []
    from provisa.federation.engine import MaterializeStoreUnconfigured

    try:
        engine.materialize_store_dsn()
    except MaterializeStoreUnconfigured:
        return []

    from provisa.api.admin.db_queries import fetch_tables
    from provisa.federation.backend import _env_store_schema
    from provisa.federation.registry_view import registered_sources

    async with db.acquire() as conn:
        tables = await fetch_tables(conn)
        sources = {s.id: s for s in await registered_sources(state, conn)}

    if not hasattr(state, "push_listener_disconnects"):
        state.push_listener_disconnects = {}
    if not hasattr(state, "push_listener_tasks"):
        state.push_listener_tasks = []

    store_schema = _env_store_schema(engine.materialize_store_dsn())
    started: list[asyncio.Task] = []

    for tbl in tables:
        src = sources.get(tbl["source_id"])
        if src is None:
            continue
        source_type = src.type.value if hasattr(src.type, "value") else str(src.type)
        if source_type not in _PUSH_SOURCE_TYPES:
            continue
        node = f"{tbl['schema_name']}.{tbl['table_name']}"
        if node in state.push_listener_disconnects:
            continue  # already running from a prior wire

        pk_columns = [c["column_name"] for c in tbl["columns"] if c.get("is_primary_key")]
        if not pk_columns:
            log.warning(
                "push listener %s: no primary key column declared — CDC landing (upsert/delete "
                "by PK) requires one, skipping",
                node,
            )
            continue
        columns = [
            (c["column_name"], c["data_type"])
            for c in tbl["columns"]
            if c.get("native_filter_type") is None and c.get("data_type")
        ]
        if not columns:
            log.warning("push listener %s: no typed columns — skipping", node)
            continue

        built = _build_provider(src, tbl, node=node)
        if built is None:
            continue  # _build_provider already logged why
        provider, watch_target = built

        land_schema, land_table = engine.landing_target(
            store_schema=store_schema,
            source_id=src.id,
            source_type=src.type,
            schema_name=tbl["schema_name"],
            table_name=tbl["table_name"],
        )
        disconnect = asyncio.Event()
        state.push_listener_disconnects[node] = disconnect
        debounce_quiet = float(tbl.get("push_debounce_quiet") or 0.0)
        debounce_max_delay = float(tbl.get("push_debounce_max_delay") or 5.0)

        task = asyncio.create_task(
            _run_listener(
                engine=engine,
                provider=provider,
                watch_target=watch_target,
                land_schema=land_schema,
                land_table=land_table,
                columns=columns,
                pk_columns=pk_columns,
                disconnect=disconnect,
                debounce_quiet=debounce_quiet,
                debounce_max_delay=debounce_max_delay,
                node=node,
                log=log,
            ),
            name=f"push-listener:{node}",
        )
        started.append(task)
        state.push_listener_tasks.append(task)
        log.info(
            "push listener started for %s (source=%r type=%s, debounce_quiet=%.1fs "
            "debounce_max_delay=%.1fs)",
            node,
            src.id,
            source_type,
            debounce_quiet,
            debounce_max_delay,
        )

    return started


async def _run_listener(
    *,
    engine: Any,
    provider: Any,
    watch_target: str,
    land_schema: str,
    land_table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    disconnect: asyncio.Event,
    debounce_quiet: float,
    debounce_max_delay: float,
    node: str,
    log: Any,
) -> None:
    """One push table's whole lifetime: drain the provider into the landed table through the
    engine's own write face (``EngineRuntime.apply_cdc_events``, REQ-989/REQ-1733 — never a raw
    ``store_connection()`` held here, which would open a SECOND connection onto an embedded
    single-writer DuckDB store the engine already has ATTACHed and deadlock/error against it).
    Never let an unhandled exception escape (this runs detached — nothing awaits its result)."""
    from provisa.subscriptions.cdc_landing import consume_cdc_into_store

    async def _land(events: list) -> dict[str, int]:
        return await engine.apply_cdc_events(
            schema=land_schema,
            table=land_table,
            columns=columns,
            pk_columns=pk_columns,
            events=events,
        )

    try:
        await consume_cdc_into_store(
            provider,
            _land,
            schema=land_schema,
            table=land_table,
            disconnect=disconnect,
            watch_target=watch_target,
            debounce_quiet=debounce_quiet,
            debounce_max_delay=debounce_max_delay,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("push listener %s crashed", node)


async def shutdown_push_listeners(state: Any) -> None:
    """Signal every running push listener to stop and wait for them to finish (app shutdown)."""
    for disconnect in getattr(state, "push_listener_disconnects", {}).values():
        disconnect.set()
    tasks = getattr(state, "push_listener_tasks", [])
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
