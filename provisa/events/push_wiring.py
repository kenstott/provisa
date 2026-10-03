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
import threading
from typing import Any

from provisa.core.connection_loop import LongLived, spawn_long_lived

log = logging.getLogger(__name__)

_PUSH_SOURCE_TYPES = frozenset({"kafka", "websocket"})
# REQ-1861: sources whose own change feed says THAT a table changed. The listener carries no
# rows: each burst of changes asks for a build of the table's replica (one builder, one path).
_CHANGE_STREAM_SOURCE_TYPES = frozenset({"mongodb"})
# The change signal that opts a table into its source's change feed (REQ-929).
_CHANGE_FEED_SIGNAL = "native"
# Seconds a change-stream listener waits before it watches again after its stream fails.
_CHANGE_STREAM_RETRY_SECONDS = 5.0


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
                "push listener %s: kafka source %r has no host configured — skipping",
                node,
                src.id,
            )
            return None
        # REQ-1766: was `src.host` alone — the Sources form (now HOST_PORT_ONLY, see
        # provisa-ui/src/pages/sources/constants.ts) captures host and port as SEPARATE fields,
        # the same shape websocket's own ws://host:port derivation uses just below. Passing bare
        # host as bootstrap_servers silently used aiokafka's default port (9092) regardless of
        # what the user actually registered, rather than failing loudly or connecting correctly.
        bootstrap_servers = f"{src.host}:{src.port}" if src.port else src.host
        provider = get_provider(
            "kafka", {"bootstrap_servers": bootstrap_servers, "group_id": f"provisa-{src.id}"}
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


async def wire_push_listeners(*, state: Any, log: Any) -> list[LongLived]:
    """Start one CDC-landing listener per registered table on a kafka/websocket source.

    REQ-1882: each listener runs for the process on its own dedicated thread and loop
    (spawn_long_lived) — landing writes block on the store, so never on the process loop.

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
    from provisa.federation.registry_view import registered_sources

    async with db.acquire() as conn:
        tables = await fetch_tables(conn)
        sources = {s.id: s for s in await registered_sources(state, conn)}

    if not hasattr(state, "push_listener_disconnects"):
        state.push_listener_disconnects = {}
    if not hasattr(state, "push_listener_tasks"):
        state.push_listener_tasks = []

    started: list[LongLived] = []

    from provisa.events.app_wiring import replica_write_lock_factory
    from provisa.events.nodes import source_node

    locks = replica_write_lock_factory(state)
    for tbl in tables:
        src = sources.get(tbl["source_id"])
        if src is None:
            continue
        source_type = src.type.value if hasattr(src.type, "value") else str(src.type)
        if source_type in _CHANGE_STREAM_SOURCE_TYPES:
            task = _start_change_stream(state, src, tbl, log=log)
            if task is not None:
                started.append(task)
            continue
        if source_type not in _PUSH_SOURCE_TYPES:
            continue
        node = source_node(tbl["source_id"], tbl["schema_name"], tbl["table_name"])
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
        row_materialize = bool(tbl.get("row_materialize"))  # REQ-1865

        address = engine.replica_address(
            source_id=src.id, schema_name=tbl["schema_name"], table_name=tbl["table_name"]
        )
        land_schema, land_table = address.schema, address.table
        # A threading.Event: the listener polls is_set() on its own thread, and shutdown sets it
        # from another — an asyncio.Event is not safe to set across threads.
        disconnect = threading.Event()
        state.push_listener_disconnects[node] = disconnect
        debounce_quiet = float(tbl.get("push_debounce_quiet") or 0.0)
        debounce_max_delay = float(tbl.get("push_debounce_max_delay") or 5.0)

        task = spawn_long_lived(
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
                row_materialize=row_materialize,
                log=log,
                write_lock=(
                    None
                    if row_materialize  # a row-level table has no whole-table replica to build
                    else (lambda key=(src.id, tbl["schema_name"], tbl["table_name"]): locks(key))
                ),
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


def _start_change_stream(state: Any, src: Any, tbl: dict, *, log: Any) -> LongLived | None:
    """Start the change-stream listener of one table of a change-feed source (REQ-1861), when the
    table's effective change signal opts it in and no listener is running for it."""
    from provisa.events.nodes import source_node

    signal = tbl["change_signal"] if tbl["change_signal"] is not None else src.change_signal
    if signal != _CHANGE_FEED_SIGNAL:
        return None
    node = source_node(tbl["source_id"], tbl["schema_name"], tbl["table_name"])
    if node in state.push_listener_disconnects:
        return None  # already running from a prior wire
    disconnect = threading.Event()
    state.push_listener_disconnects[node] = disconnect
    quiet = float(tbl.get("push_debounce_quiet") or 0.0)
    max_delay = float(tbl.get("push_debounce_max_delay") or 5.0)
    task = spawn_long_lived(
        _run_change_stream(
            state=state,
            source=src,
            table_id=tbl["id"],
            collection=tbl["table_name"],
            database=src.database or tbl["schema_name"],
            disconnect=disconnect,
            debounce_quiet=quiet,
            debounce_max_delay=max_delay,
            node=node,
            log=log,
        ),
        name=f"change-stream:{node}",
    )
    state.push_listener_tasks.append(task)
    log.info(
        "change stream listener started for %s (source=%r, debounce_quiet=%.1fs "
        "debounce_max_delay=%.1fs)",
        node,
        src.id,
        quiet,
        max_delay,
    )
    return task


async def follow_changes(
    stream: Any,
    on_change: Any,
    disconnect: threading.Event,
    *,
    quiet: float,
    max_delay: float,
    clock: Any = None,
) -> None:
    """Call ``on_change`` once per burst of changes on ``stream`` until ``disconnect`` is set.

    ``stream.try_next()`` returns the next change, or None when none arrived within the stream's
    wait. A burst ends once no change has arrived for ``quiet`` seconds, or ``max_delay`` seconds
    after its first change, whichever is sooner -- so a collection that never goes quiet still
    asks. A stream that is no longer alive raises: the caller watches again."""
    import time

    now = clock or time.monotonic
    first: float | None = None
    last = 0.0
    while not disconnect.is_set():
        change = await stream.try_next()
        at = now()
        if change is not None:
            last = at
            if first is None:
                first = at
        elif not stream.alive:
            raise ConnectionError("the change stream was closed by the server")
        if first is not None and (at - last >= quiet or at - first >= max_delay):
            await on_change()
            first = None


async def _run_change_stream(
    *,
    state: Any,
    source: Any,
    table_id: int,
    collection: str,
    database: str,
    disconnect: threading.Event,
    debounce_quiet: float,
    debounce_max_delay: float,
    node: str,
    log: Any,
) -> None:
    """One change-feed table's whole lifetime (REQ-1861): watch the collection's change stream
    and ask for a build of the table's replica once per burst of changes, through the one request
    every change to a replicated table makes (``replica_builds.request_if_replicated``).

    A build is asked for each time the stream opens, too: what changed while nothing was watching
    is unknown. A stream that fails -- the server is down, or is a standalone mongod, which serves
    no change streams -- is logged with the server's own reason and watched again after
    ``_CHANGE_STREAM_RETRY_SECONDS``. Never lets an exception escape (it runs detached)."""
    from provisa.core.secrets import resolve_secrets
    from provisa.federation import replica_builds, replica_state
    from provisa.federation.source_vault import org_vault
    from provisa.subscriptions.mongo_provider import open_change_stream

    async def _ask() -> None:
        await replica_builds.request_if_replicated(
            state, table_id, source.id, replica_state.REASON_REFRESH
        )

    async def _feed(error: str | None) -> None:
        # The listener's state on the table's replica record, where its status is shown.
        from datetime import UTC, datetime

        from provisa.federation.registry_view import registered_tables

        table = {t.id: t for t in await registered_tables(state)}[table_id]
        async with state.tenant_db.acquire() as conn:
            await replica_state.record_feed(
                conn,
                (source.id, table.schema_name, table.table_name),
                error=error,
                now=datetime.now(UTC),
            )

    # The stream's wait bounds how long a burst's end and a shutdown go unnoticed.
    wait_ms = int(max(0.05, min(1.0, debounce_quiet or 1.0)) * 1000)
    while not disconnect.is_set():
        try:
            async with org_vault(state, [source]):
                host = resolve_secrets(source.host or "localhost")
                password = resolve_secrets(source.password or "") or None
            async with open_change_stream(
                host=host,
                port=int(source.port or 27017),
                username=source.username or None,
                password=password,
                database=database,
                collection=collection,
                wait_ms=wait_ms,
            ) as stream:
                await _feed(None)
                await _ask()  # watching from here on: what changed before is in this build
                await follow_changes(
                    stream, _ask, disconnect, quiet=debounce_quiet, max_delay=debounce_max_delay
                )
        except asyncio.CancelledError:
            raise
        except Exception as failed:
            log.warning(
                "change stream %s: %s; watching again in %.0fs",
                node,
                failed,
                _CHANGE_STREAM_RETRY_SECONDS,
            )
            try:
                # A driver error can carry no text (a timeout); its class then names the cause.
                await _feed(str(failed) or type(failed).__name__)
            except Exception:
                log.exception("change stream %s: its down state could not be recorded", node)
        deadline = _CHANGE_STREAM_RETRY_SECONDS
        while deadline > 0 and not disconnect.is_set():
            await asyncio.sleep(0.2)
            deadline -= 0.2


async def _run_listener(
    *,
    engine: Any,
    provider: Any,
    watch_target: str,
    land_schema: str,
    land_table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    disconnect: threading.Event,
    debounce_quiet: float,
    debounce_max_delay: float,
    node: str,
    log: Any,
    row_materialize: bool = False,
    write_lock: Any = None,
) -> None:
    """One push table's whole lifetime: drain the provider into the landed table through the
    engine's own write face (``EngineRuntime.apply_cdc_events``, REQ-989/REQ-1733 — never a raw
    ``store_connection()`` held here, which would open a SECOND connection onto an embedded
    single-writer DuckDB store the engine already has ATTACHed and deadlock/error against it).
    Never let an unhandled exception escape (this runs detached — nothing awaits its result).

    ``row_materialize`` (REQ-1865) routes the batch through the row-materialize CDC branch
    (design doc section 5) instead of the ordinary upsert-every-event land."""
    from provisa.subscriptions.cdc_landing import consume_cdc_into_store

    from provisa.events.handlers import _held

    async def _land(events: list) -> dict[str, int]:
        # REQ-1915: a batch of change events is applied under the replica's own lock, so it is
        # never written into a table a build is about to replace. A batch that finds a build
        # running waits for it and lands in the new table; applied by key, a change the build
        # already copied is applied again harmlessly.
        async with _held(write_lock):
            return await engine.apply_cdc_events(
                schema=land_schema,
                table=land_table,
                columns=columns,
                pk_columns=pk_columns,
                events=events,
                row_materialize=row_materialize,
                node=node,
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


async def shutdown_push_listeners(state: Any, timeout: float = 10.0) -> None:
    """Stop every running push listener and wait (bounded) for its thread to end (app shutdown).

    The disconnect flag lets a listener between events flush its partial batch and exit; the
    cancel ends one blocked waiting for the next event."""
    for disconnect in getattr(state, "push_listener_disconnects", {}).values():
        disconnect.set()
    handles = list(getattr(state, "push_listener_tasks", []))
    for handle in handles:
        handle.cancel()
    for handle in handles:
        if not await handle.wait(timeout):
            logging.getLogger(__name__).warning(
                "shutdown: push listener %s did not stop within %.0fs", handle.name, timeout
            )
