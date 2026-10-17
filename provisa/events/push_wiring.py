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
import time
from typing import Any

from provisa.core.change_signal import PUSH_SOURCE_TYPES
from provisa.core.connection_loop import LongLived, spawn_long_lived

log = logging.getLogger(__name__)

# The push source types that run a background notification LISTENER, derived from the one canonical
# PUSH_SOURCE_TYPES. ``ingest`` is push-fed too, but it is a synchronous HTTP inbound endpoint
# (ingest/router.py) with no listener to start, so it is not wired here.
_LISTENER_SOURCE_TYPES = PUSH_SOURCE_TYPES - frozenset({"ingest"})
# REQ-1861: sources whose own change feed says THAT a table changed. The listener carries no
# rows: each burst of changes asks for a build of the table's replica (one builder, one path).
# REQ-1861 MongoDB change streams; REQ-1982 the Cosmos DB change feed.
_CHANGE_STREAM_SOURCE_TYPES = frozenset({"mongodb", "cosmosdb"})
# The change signal that opts a table into its source's change feed (REQ-929).
_CHANGE_FEED_SIGNAL = "native"
# Seconds a change-stream listener waits before it watches again after its stream fails.
_CHANGE_STREAM_RETRY_SECONDS = 5.0
# Seconds a logical replication listener waits between reads of a slot that had nothing new, and
# the most messages one read consumes.
_LOGICAL_POLL_SECONDS = 1.0
_LOGICAL_READ_LIMIT = 10000
_LOGICAL_MAX_DELAY_SECONDS = 5.0


def _effective_signal(src: Any, tbl: dict) -> str | None:
    """The table's change signal, inheriting its source's (REQ-929)."""
    return tbl["change_signal"] if tbl["change_signal"] is not None else src.change_signal


# The signals whose changes arrive on a non-push source's CDC transport (REQ-824).
_CDC_TRANSPORT_SIGNALS = frozenset({"debezium", "kafka"})


def _consumer_group(base: str) -> str:
    """The bound org's (and environment's) own consumer group, from ``base``: each org lands a
    topic into its own table, so each must read every message, and one group shared by every
    org would split the topic among them. An operator's configured group is the base."""
    org, env, _ = _listener_key("")
    return f"{base}-{org}" if env is None else f"{base}-{org}-{env}"


def _listener_key(node: str) -> tuple[str, str | None, str]:
    """A running listener's key: the org and environment it lands for, and its node. One process
    serves many orgs (a demo's sandboxes) and each org's environments; a node name alone is the
    same in every one of them, so a second org's table would be taken as already running, and
    one org's wiring would stop another's listeners as unwired."""
    from provisa.core.request_context import require_current_org

    return require_current_org(), _bound_env(), node


def _bound_env() -> str | None:
    """The bound environment, ``None`` for prod: an unbound environment and ``prod`` are one
    environment (REQ-1487), so a wire under either finds the other's listeners and poll."""
    from provisa.core.environments import PROD
    from provisa.core.request_context import active_env

    env = active_env()
    return None if env == PROD else env


def _captured_by_cdc(src: Any, tbl: dict) -> bool:
    """Whether a table's changes arrive on its source's CDC transport (REQ-261, REQ-813,
    REQ-824): its effective signal is ``debezium`` or ``kafka`` on a source that is not itself a
    push type. Config load and the admin save refuse either on a source with no cdc block, and
    ``kafka`` on a table that names no topic."""
    return (
        _source_type_of(src) not in PUSH_SOURCE_TYPES
        and _effective_signal(src, tbl) in _CDC_TRANSPORT_SIGNALS
    )


def _cdc_provider(state: Any, src: Any, tbl: dict) -> tuple[Any, str]:
    """The consumer of one captured table on its source's CDC transport, with Provisa's
    consumer group for it (REQ-931: the source's own, else the Provisa-level default): the
    Debezium topic its connector writes the table to, or the delta topic the table names."""
    from provisa.kafka.avro_registry import RegistrySettings
    from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider
    from provisa.subscriptions.registry import get_provider

    from provisa.kafka.landed_offsets import landed_group

    cdc = src.cdc
    group = _consumer_group(cdc.consumer_group_id or state.config.cdc_consumer_group_id)
    # REQ-1733: its first start reads from where the table's landing stopped on the topic.
    landed = landed_group(src.id)
    if _effective_signal(src, tbl) == "kafka":
        topic = ((tbl.get("live") or {}).get("kafka") or {})["topic"]
        provider = get_provider(
            "kafka",
            {
                "bootstrap_servers": cdc.bootstrap_servers,
                "group_id": group,
                "registry": RegistrySettings.of(cdc),
                "field_mapping": ((tbl.get("live") or {}).get("kafka") or {}).get("field_mapping")
                or {},
                "landed": landed,
            },
        )
        return provider, topic
    provider = DebeziumNotificationProvider(
        bootstrap_servers=cdc.bootstrap_servers,
        topic_prefix=cdc.topic_prefix,
        database=src.database,
        consumer_group_id=group,
        registry=RegistrySettings.of(cdc),
        source_type=_source_type_of(src),
        pg_schema=tbl["schema_name"],
        landed=landed,
    )
    return provider, tbl["table_name"]


def _field(holder: Any, name: str) -> Any:
    """``name`` of a registered-table row (a dict) or of a model object; None when absent."""
    if holder is None:
        return None
    return holder.get(name) if isinstance(holder, dict) else getattr(holder, name, None)


def cdc_capture(src: Any, tbl: Any) -> tuple[str, str] | None:
    """The brokers and topic a captured table's changes arrive on (:func:`_captured_by_cdc`), for
    a table row or a model table; None for a table that is not captured. Its build records where
    it stopped on that topic (REQ-1733)."""
    from provisa.subscriptions.debezium_provider import debezium_topic

    signal = _field(tbl, "change_signal")
    signal = signal if signal is not None else src.change_signal
    if _source_type_of(src) in PUSH_SOURCE_TYPES or signal not in _CDC_TRANSPORT_SIGNALS:
        return None
    cdc = src.cdc
    if signal == "kafka":
        topic = _field(_field(_field(tbl, "live"), "kafka"), "topic")
    else:
        topic = debezium_topic(
            cdc.topic_prefix,
            src.database,
            _source_type_of(src),
            _field(tbl, "schema_name"),
            _field(tbl, "table_name"),
        )
    return cdc.bootstrap_servers, topic


def _build_provider(src: Any, tbl: dict, *, node: str) -> tuple[Any, str] | None:
    """A (NotificationProvider, watch_target) pair for *src*, or None (logged) when its config is
    incomplete. ``watch_target`` is what gets passed to ``provider.watch()`` — the Kafka topic for
    kafka, the registered table name for websocket (one socket, one stream, no per-topic routing)."""
    from provisa.core.secrets import resolve_secrets
    from provisa.kafka.avro_registry import RegistrySettings
    from provisa.kafka.landed_offsets import landed_group
    from provisa.subscriptions.registry import get_provider

    source_type = src.type.value if hasattr(src.type, "value") else str(src.type)
    # The address as configured may name an environment variable or a secret (${env:...}), as any
    # source's does: resolved here, where the listener connects, as a source pool's is.
    host = resolve_secrets(src.host) if src.host else src.host

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
        if not host:
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
        bootstrap_servers = f"{host}:{src.port}" if src.port else host
        # Each org (and each of its environments) lands the topic into its own table, so each
        # reads every message: its own consumer group. One shared by every org would split the
        # topic's messages among them.
        org, env, _ = _listener_key(node)
        group = f"provisa-{org}-{src.id}" if env is None else f"provisa-{org}-{env}-{src.id}"
        provider = get_provider(
            "kafka",
            {
                "bootstrap_servers": bootstrap_servers,
                "group_id": group,
                "registry": RegistrySettings(url=resolve_secrets(src.database))
                if src.database
                else None,
                "field_mapping": kafka_cfg.get("field_mapping") or {},
                # REQ-1733: its first start reads from where the table's landing stopped.
                "landed": landed_group(src.id),
            },
        )
        return provider, topic

    if source_type == "websocket":
        # REQ-1733: websocket has no dedicated URL field in the Sources form yet — base_url (an
        # explicit override) else derive ws://host:port from host+port, the SAME override-else-
        # derive shape airport/neo4j already use (REQ-1730, DuckDBAirportConnector /
        # neo4j_config_from_source) rather than inventing a third convention.
        url = resolve_secrets(src.base_url) if src.base_url else None
        url = url or (f"ws://{host}:{src.port}" if host else None)
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
    (empty on a pure re-wire where every push table already has a listener).

    REQ-1921: a listener whose table is no longer wired is stopped. That covers a table set
    as draft (it leaves ``fetch_tables``), a deleted one, and a change stream whose signal no
    longer opts in. Every schema rebuild re-runs this, so a table that goes draft stops landing
    there."""
    db = getattr(state, "model_db", None)
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
    if not hasattr(state, "push_listener_handles"):
        state.push_listener_handles = {}

    started: list[LongLived] = []

    from provisa.events.app_wiring import replica_write_lock_factory
    from provisa.events.nodes import source_node

    locks = replica_write_lock_factory(state)
    wired: set[str] = set()  # every node this pass wants running, started now or before
    # REQ-929: each PostgreSQL source with tables on the native signal, read from one slot.
    logical: dict[str, tuple[Any, list[tuple[str, str, int]]]] = {}
    from provisa.federation.replica_routing import resolved_cache_ttl
    from provisa.core.cache_ttl import NEVER_EXPIRES

    for tbl in tables:
        src = sources.get(tbl["source_id"])
        if src is None:
            continue
        if resolved_cache_ttl(src, tbl) == NEVER_EXPIRES:
            continue  # REQ-2031: lands once; no change at its source is listened for
        source_type = src.type.value if hasattr(src.type, "value") else str(src.type)
        if source_type == "postgresql" and _effective_signal(src, tbl) == _CHANGE_FEED_SIGNAL:
            followed = logical.setdefault(src.id, (src, []))[1]
            followed.append((tbl["schema_name"], tbl["table_name"], tbl["id"]))
            continue
        if source_type in _CHANGE_STREAM_SOURCE_TYPES:
            if _change_feed_opted_in(src, tbl):
                wired.add(source_node(tbl["source_id"], tbl["schema_name"], tbl["table_name"]))
            task = _start_change_stream(state, src, tbl, log=log)
            if task is not None:
                started.append(task)
            continue
        captured = _captured_by_cdc(src, tbl)
        if source_type not in _LISTENER_SOURCE_TYPES and not captured:
            continue
        node = source_node(tbl["source_id"], tbl["schema_name"], tbl["table_name"])
        wired.add(node)
        if _listener_key(node) in state.push_listener_disconnects:
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
        if _lands_by_pipeline(engine, src, tbl):
            # REQ-990: the store lands this topic through its own pipeline; no listener relays it.
            await _wire_pipeline(
                state, engine, src, tbl, node=node, columns=columns, pk_columns=pk_columns, log=log
            )
            continue

        built = _cdc_provider(state, src, tbl) if captured else _build_provider(src, tbl, node=node)
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
        state.push_listener_disconnects[_listener_key(node)] = disconnect
        debounce_quiet = float(tbl.get("push_debounce_quiet") or 0.0)
        debounce_max_delay = float(tbl.get("push_debounce_max_delay") or 5.0)

        task = spawn_long_lived(
            _run_listener(
                state=state,
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
                key=(src.id, tbl["schema_name"], tbl["table_name"]),
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
        state.push_listener_handles[_listener_key(node)] = task
        log.info(
            "push listener started for %s (source=%r type=%s, debounce_quiet=%.1fs "
            "debounce_max_delay=%.1fs)",
            node,
            src.id,
            source_type,
            debounce_quiet,
            debounce_max_delay,
        )

    started.extend(_start_logical_streams(state, logical, wired, log=log))
    await _stop_unwired(state, wired, log=log)
    await _drop_unfollowed_slots(state, logical, sources, log=log)
    await _settle_pipelines(state, engine, wired, log=log)
    return started


def _start_logical_streams(
    state: Any,
    logical: dict[str, tuple[Any, list[tuple[str, str, int]]]],
    wired: set[str],
    *,
    log: Any,
) -> list[LongLived]:
    """Start one logical replication listener per PostgreSQL source with tables on the native
    signal (REQ-929). The listener's node names the tables it follows, so a change to that set
    stops it and starts one that publishes the new set."""
    started: list[LongLived] = []
    for source_id, (src, followed) in sorted(logical.items()):
        node = "logical:" + source_id + ":" + ",".join(sorted(f"{s}.{t}" for s, t, _ in followed))
        wired.add(node)
        if _listener_key(node) in state.push_listener_disconnects:
            continue  # already running from a prior wire
        disconnect = threading.Event()
        state.push_listener_disconnects[_listener_key(node)] = disconnect
        task = spawn_long_lived(
            _run_logical_stream(
                state=state, source=src, followed=followed, disconnect=disconnect, log=log
            ),
            name=f"logical-stream:{source_id}",
        )
        state.push_listener_tasks.append(task)
        state.push_listener_handles[_listener_key(node)] = task
        started.append(task)
        log.info(
            "logical replication listener started for source %r (%d tables)", src.id, len(followed)
        )
    return started


async def _drop_unfollowed_slots(
    state: Any,
    logical: dict[str, tuple[Any, list[tuple[str, str, int]]]],
    sources: dict[str, Any],
    *,
    log: Any,
) -> None:
    """Drop the slot and publication of each PostgreSQL source no native table follows: a slot
    nobody reads holds the source's WAL until its disk fills. A process's first wiring checks
    every registered PostgreSQL source, so one left by a configuration changed while no process
    ran is found however the change was made; later wirings check the sources this process
    followed before -- set off the native signal, or deleted -- reached as they were
    registered then."""
    from provisa.federation.source_vault import org_vault
    from provisa.subscriptions import pg_logical

    first = not hasattr(state, "logical_slot_sources")
    held: dict[str, Any] = (
        {sid: s for sid, s in sources.items() if _source_type_of(s) == "postgresql"}
        if first
        else state.logical_slot_sources
    )
    for source_id, src in sorted(held.items()):
        if source_id in logical:
            continue
        try:
            async with org_vault(state, [src]):
                dropped = await asyncio.to_thread(_drop_slot_of, src)
            if dropped:
                log.info("logical replication slot of source %r dropped", source_id)
        except Exception as failed:  # allow-ble: named in the log with the slot to drop by hand; one source's server being down must not stop the wiring of every other
            log.error(
                "source %r could not be checked for Provisa's logical replication slot %r (%s): "
                "a slot left there holds the source's WAL; drop it on the source",
                source_id,
                pg_logical.slot_name(source_id),
                failed,
            )
    state.logical_slot_sources = {sid: src for sid, (src, _t) in logical.items()}


def _drop_slot_of(src: Any) -> bool:
    from provisa.subscriptions import pg_logical

    with pg_logical.PgConnection.from_source(src).connect() as conn:
        return pg_logical.drop_slot(conn, pg_logical.slot_name(src.id))


async def _run_logical_stream(
    *,
    state: Any,
    source: Any,
    followed: list[tuple[str, str, int]],
    disconnect: threading.Event,
    log: Any,
) -> None:
    """One PostgreSQL source's logical replication listener (REQ-929): make its slot and the
    publication of the tables it follows, then read the slot, and ask for a build of each table
    a burst of changes touched -- through the one request every change to a replicated table
    makes. A build of every followed table is asked for each time the slot is opened: what
    changed while nothing read it is unknown. A failure is logged with its cause and the slot is
    read again after ``_CHANGE_STREAM_RETRY_SECONDS``. Never lets an exception escape."""
    from provisa.federation import replica_builds, replica_state
    from provisa.federation.source_vault import org_vault
    from provisa.subscriptions import pg_logical

    ids = {(schema, table): table_id for schema, table, table_id in followed}
    name = pg_logical.slot_name(source.id)

    async def _ask(changed: set[tuple[str, str]]) -> None:
        # A burst of changes: each table's build, and the commands that run on it (REQ-2054).
        from provisa.scheduler.table_triggers import source_changed

        for key in sorted(changed & ids.keys()):
            await source_changed(state, ids[key], source.id, replica_state.REASON_REFRESH)

    async def _opened() -> None:
        # What changed while nothing read the slot is unknown: a build of each table, and no
        # command runs for a change nothing reported.
        for key in sorted(ids):
            await replica_builds.request_if_replicated(
                state, ids[key], source.id, replica_state.REASON_REFRESH
            )

    while not disconnect.is_set():
        try:
            async with org_vault(state, [source]):
                conn = await asyncio.to_thread(pg_logical.PgConnection.from_source(source).connect)
            try:
                await asyncio.to_thread(pg_logical.ensure_slot, conn, name, list(ids))
                await _opened()
                # A burst ends after a read with nothing new, or _LOGICAL_MAX_DELAY_SECONDS after
                # its first change: a table written to without pause still asks.
                pending: set[tuple[str, str]] = set()
                first = 0.0
                while not disconnect.is_set():
                    changed = await asyncio.to_thread(
                        pg_logical.read_changes, conn, name, _LOGICAL_READ_LIMIT
                    )
                    now = time.monotonic()
                    if changed and not pending:
                        first = now
                    pending |= changed
                    if pending and (not changed or now - first >= _LOGICAL_MAX_DELAY_SECONDS):
                        await _ask(pending)
                        pending = set()
                    if not changed:
                        await asyncio.sleep(_LOGICAL_POLL_SECONDS)
            finally:
                await asyncio.to_thread(conn.close)
        except asyncio.CancelledError:
            raise
        except Exception as failed:
            log.warning(
                "logical replication of source %r failed (%s); reading its slot again in %.0fs",
                source.id,
                failed,
                _CHANGE_STREAM_RETRY_SECONDS,
            )
            await asyncio.sleep(_CHANGE_STREAM_RETRY_SECONDS)


def _lands_by_pipeline(engine: Any, src: Any, tbl: dict) -> bool:
    """Whether this Kafka table lands through a SingleStore pipeline: the engine's own store is
    SingleStore and the table's origin is in the pipeline's scope (REQ-848 PIPELINE_LAND)."""
    from sqlalchemy import make_url

    from provisa.federation.materialization import WriteFace, select_write_face
    from provisa.federation.singlestore_pipeline import PIPELINE_STORE, origin_of

    bare = engine.engine
    if bare.native_store != PIPELINE_STORE:
        return False
    store = make_url(bare.materialize_store()).get_backend_name()
    return select_write_face(bare, store, origin_of(src, tbl)) is WriteFace.PIPELINE_LAND


def _pipeline_store(state: Any, engine: Any) -> Any:
    """The SingleStore store's SQLAlchemy engine the pipelines are managed on (one per runtime)."""
    store = getattr(state, "kafka_pipeline_store", None)
    if store is None:
        from provisa.core.database import create_engine_from_url

        store = create_engine_from_url(engine.materialize_store_dsn())
        state.kafka_pipeline_store = store
    return store


async def _wire_pipeline(
    state: Any,
    engine: Any,
    src: Any,
    tbl: dict,
    *,
    node: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    log: Any,
) -> None:
    """Make the table's continuous pipeline exist and run, recorded by node. A table whose
    pipeline is already recorded with the same definition is left alone. A pipeline that cannot
    be created is logged by name; the table does not land, and is never handed to the relay."""
    import asyncio

    from provisa.federation import singlestore_kafka, singlestore_pipeline as sp

    kafka = (
        ((tbl.get("live") or {}).get("kafka") or {}) if isinstance(tbl.get("live"), dict) else {}
    )
    origin = sp.origin_of(src, tbl)
    address = engine.replica_address(
        source_id=src.id, schema_name=tbl["schema_name"], table_name=tbl["table_name"]
    )
    bootstrap = f"{src.host}:{src.port}" if src.port else src.host
    registry = src.cdc.schema_registry_url if getattr(src, "cdc", None) else None
    field_mapping = kafka.get("field_mapping") or None
    expected = sp.kafka_pipeline_name(
        address.schema,
        address.table,
        sp.kafka_definition(origin, columns, pk_columns, field_mapping, registry, bootstrap),
    )
    if not hasattr(state, "kafka_pipelines"):
        state.kafka_pipelines = {}
        state.kafka_pipeline_delays = {}
        state.kafka_pipeline_tables = {}
    # Keyed as the listeners are (``_listener_key``): one process serves many orgs and their
    # environments, each with its own pipeline for a table of the same name.
    key = _listener_key(node)
    state.kafka_pipeline_delays[key] = float(tbl.get("push_debounce_max_delay") or 5.0)
    state.kafka_pipeline_tables[key] = (address.schema, address.table)
    if state.kafka_pipelines.get(key) == expected:
        return
    from provisa.kafka.landed_offsets import landed_group, recorded

    try:
        # REQ-1733: a new pipeline starts where the table's landing stopped on the topic.
        landed = await recorded(bootstrap, landed_group(src.id), origin.location)
        name = await asyncio.to_thread(
            singlestore_kafka.ensure,
            _pipeline_store(state, engine),
            schema=address.schema,
            table=address.table,
            columns=columns,
            pk_columns=pk_columns,
            origin=origin,
            bootstrap=bootstrap,
            field_mapping=field_mapping,
            schema_registry=registry,
            landed=landed,
        )
    except Exception:
        log.exception(
            "kafka pipeline for %s (topic %r) could not be created or started; the table does "
            "not land until it can",
            node,
            origin.location,
        )
        state.kafka_pipelines.setdefault(key, None)  # keep whatever pipeline it had
        return
    state.kafka_pipelines[key] = name
    log.info("kafka pipeline %s lands %s (topic %r)", name, node, origin.location)


async def _settle_pipelines(state: Any, engine: Any, wired: set[str], *, log: Any) -> None:
    """After a wiring pass on a SingleStore store: forget and drop the pipelines of tables no
    longer wired (draft, deleted, retired), only in this org and region's replicas schema, and keep
    the region's batch poll scheduled while any pipeline lands (REQ-990, REQ-1922)."""
    import asyncio

    from provisa.federation import singlestore_kafka, singlestore_pipeline as sp

    if engine.engine.native_store != sp.PIPELINE_STORE:
        return
    from provisa.core.request_context import require_current_org
    from provisa.federation.replica_address import replica_schema

    all_pipelines: dict = getattr(state, "kafka_pipelines", {}) or {}
    all_delays: dict = getattr(state, "kafka_pipeline_delays", {}) or {}
    all_tables: dict = getattr(state, "kafka_pipeline_tables", {}) or {}
    # Only this org and environment's pipelines: another runtime's are settled by its own pass.
    org, env, _ = _listener_key("")
    for key in [k for k in all_pipelines if k[:2] == (org, env) and k[2] not in wired]:
        all_pipelines.pop(key)
        all_delays.pop(key, None)
        all_tables.pop(key, None)
    pipelines = {k: v for k, v in all_pipelines.items() if k[:2] == (org, env)}
    delays = {k: v for k, v in all_delays.items() if k[:2] == (org, env)}
    tables = {k: v for k, v in all_tables.items() if k[:2] == (org, env)}
    # REQ-1935: the org being wired is bound (no implied org); its replicas schema in this region
    # is the only one swept.
    org_id = require_current_org()
    schema = replica_schema(org_id)
    # A wired table whose pipeline could not be (re)created this pass keeps any it already has.
    failed = {
        sp.kafka_table_prefix(*tables[key]) for key, name in pipelines.items() if name is None
    }
    keep = {name for name in pipelines.values() if name is not None}
    store = _pipeline_store(state, engine)

    def _sweep() -> list[str]:
        return singlestore_kafka.sweep(store, schema=schema, keep=keep, keep_prefixes=failed)

    dropped = await asyncio.to_thread(_sweep)
    for name in dropped:
        log.info("kafka pipeline %s dropped: its table is no longer wired", name)
    _schedule_batch_poll(state, store, schema, delays, org_id=org_id)


def _schedule_batch_poll(state: Any, store: Any, schema: str, delays: dict, *, org_id: str) -> None:
    """The region's one scheduled poll of ``org_id``'s pipeline batches (``poll:`` runs on the
    region's claim). Its cadence is the shortest push_debounce_max_delay among the pipeline
    tables; with no pipeline left, the job is removed."""
    scheduler = getattr(state, "_scheduler", None)
    if scheduler is None:
        return
    env = _bound_env()
    # One poll per org and environment: each has its own pipelines (``_listener_key``).
    job_id = f"poll:singlestore-kafka-pipelines:org_{org_id}" + ("" if env is None else f":{env}")
    if not delays:
        if scheduler.get_job(job_id) is not None:
            scheduler.remove_job(job_id)
        return
    from apscheduler.triggers.interval import IntervalTrigger

    from provisa.federation import singlestore_kafka

    async def _fire() -> None:
        # A scheduled fire runs outside any request: it binds the org it was wired for
        # (REQ-1935), every time, so its reads and ripples are that org's and no other's.
        from provisa.core.request_context import (
            reset_current_env,
            reset_current_org,
            set_current_env,
            set_current_org,
        )

        tok = set_current_org(org_id)
        env_tok = set_current_env(env)
        try:
            await singlestore_kafka.ripple_new_batches(state, store, schema=schema)
        finally:
            reset_current_env(env_tok)
            reset_current_org(tok)

    scheduler.add_job(
        _fire,
        trigger=IntervalTrigger(seconds=min(delays.values())),
        id=job_id,
        replace_existing=True,
    )


async def _stop_unwired(state: Any, wired: set[str], *, log: Any, timeout: float = 10.0) -> None:
    """Stop every running listener whose node is not in ``wired`` (REQ-1921): its table went
    draft, was deleted, or no longer opts into its change feed. The disconnect flag lets a
    listener between events flush its partial batch and exit; the cancel ends one blocked
    waiting for the next event. A later wire starts it again if its table comes back."""
    handles = state.push_listener_handles
    org, env, _ = _listener_key("")
    # Only this org and environment's listeners: another runtime's are wired by its own pass.
    unwired = [
        key
        for key in state.push_listener_disconnects
        if key[:2] == (org, env) and key[2] not in wired
    ]
    for key in unwired:
        node = key[2]
        state.push_listener_disconnects.pop(key).set()
        handle = handles.pop(key, None)
        if handle is None:
            continue
        handle.cancel()
        if handle in state.push_listener_tasks:
            state.push_listener_tasks.remove(handle)
        if not await handle.wait(timeout):
            log.warning("push listener %s did not stop within %.0fs", node, timeout)
        log.info(
            "push listener stopped for %s: its table is no longer wired (draft, deleted, or "
            "its change feed is off)",
            node,
        )


def _source_type_of(src: Any) -> str:
    return src.type.value if hasattr(src.type, "value") else str(src.type)


def _change_feed_opted_in(src: Any, tbl: dict) -> bool:
    """Whether a change-feed source's table opts into its change stream (REQ-1861)."""
    signal = tbl["change_signal"] if tbl["change_signal"] is not None else src.change_signal
    return signal == _CHANGE_FEED_SIGNAL


def _start_change_stream(state: Any, src: Any, tbl: dict, *, log: Any) -> LongLived | None:
    """Start the change-stream listener of one table of a change-feed source (REQ-1861), when the
    table's effective change signal opts it in and no listener is running for it."""
    from provisa.events.nodes import source_node

    if not _change_feed_opted_in(src, tbl):
        return None
    node = source_node(tbl["source_id"], tbl["schema_name"], tbl["table_name"])
    if _listener_key(node) in state.push_listener_disconnects:
        return None  # already running from a prior wire
    disconnect = threading.Event()
    state.push_listener_disconnects[_listener_key(node)] = disconnect
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
    state.push_listener_handles[_listener_key(node)] = task
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
    from provisa.federation import replica_builds, replica_state
    from provisa.federation.source_vault import org_vault
    from provisa.cosmosdb.change_feed import open_change_feed
    from provisa.cosmosdb.fetch import CosmosConnection
    from provisa.mongodb.fetch import MongoConnection
    from provisa.subscriptions.mongo_provider import open_change_stream

    async def _ask() -> None:
        # A burst of changes: the table's build, and the commands that run on it (REQ-2054).
        from provisa.scheduler.table_triggers import source_changed

        await source_changed(state, table_id, source.id, replica_state.REASON_REFRESH)

    async def _opened() -> None:
        # What changed before the stream opened is unknown: a build, and no command runs.
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
                if _source_type_of(source) == "cosmosdb":
                    opened = open_change_feed(
                        connection=CosmosConnection.from_source(source),
                        container=collection,
                        wait_ms=wait_ms,
                    )
                else:
                    opened = open_change_stream(
                        connection=MongoConnection.from_source(source),
                        database=database,
                        collection=collection,
                        wait_ms=wait_ms,
                    )
            async with opened as stream:
                await _feed(None)
                await _opened()  # watching from here on: what changed before is in this build
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
    state: Any,
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
    key: tuple[str, str, str],
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
    from provisa.federation.commit_time import SOURCE_COMMITTED_AT

    carries_commit_time = any(name == SOURCE_COMMITTED_AT for name, _type in columns)

    async def _land(batch: list) -> dict[str, int]:
        # A change without its key has no row to upsert or delete: landed, it would be a row of
        # NULLs. It is reported and passed over; its offset is committed with the batch's.
        events = []
        for event in batch:
            if event.operation == "delete" and not event.row:
                continue  # a compaction tombstone, after the delete it marks: nothing to apply
            missing = [c for c in pk_columns if event.row.get(c) is None]
            if missing:
                log.error(
                    "push listener %s: a change carries no %s, the table's key; passed over "
                    "(at %s)",
                    node,
                    ", ".join(missing),
                    event.ack_token,
                )
                continue
            events.append(event)
        if not events:
            return {"upsert": 0, "delete": 0}
        if carries_commit_time:
            # REQ-1163: the landed row carries the commit time its change event says.
            for event in events:
                event.row = {**event.row, SOURCE_COMMITTED_AT: event.committed_at}
        # REQ-1915: a batch of change events is applied under the replica's own lock, so it is
        # never written into a table a build is about to replace. A batch that finds a build
        # running waits for it and lands in the new table; applied by key, a change the build
        # already copied is applied again harmlessly.
        async with _held(write_lock):
            counts = await engine.apply_cdc_events(
                schema=land_schema,
                table=land_table,
                columns=columns,
                pk_columns=pk_columns,
                events=events,
                row_materialize=row_materialize,
                node=node,
            )
        # REQ-1733: an upsert of the row the table already held changed nothing -- a change the
        # build already copied, applied again -- so it is no change to tell anyone of. A
        # row-level table's apply refreshes keys from the source and reports no such upsert.
        unchanged = set() if row_materialize else set(counts.pop("unchanged"))
        changed = [e for position, e in enumerate(events) if position not in unchanged]
        # A landed batch is this table's change: the views that read it are told and its
        # freshness moves, as for any other landing (REQ-961). A row-level table has no node of its
        # own; its keys are refreshed by the row cache's own work item (REQ-1865).
        if not row_materialize and any(counts.values()):
            from provisa.events.ripple import ripple

            await ripple(state, node, event_type="delta", payload={"landed": counts})
        if any(counts.values()):
            # Its subscriptions re-run on the landed table now that the batch is in it.
            from provisa.subscriptions.landed import landed

            landed(*key)
            # REQ-2054: a landed batch is a burst of changes to the table.
            from provisa.scheduler.table_triggers import table_changed

            table_changed(
                state,
                key,
                changes=[{"operation": e.operation, "row": e.row} for e in changed],
            )
        return counts

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
