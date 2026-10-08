# Copyright (c) 2026 Kenneth Stott
# Canary: 721f6403-65b1-4ac1-a015-a3b4909b4c9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SSE subscription endpoint via provider-based change notifications (REQ-AB2).

GET /data/subscribe/{table} streams server-sent events for INSERT, UPDATE,
and DELETE operations on the subscribed table.  Resolves the appropriate
NotificationProvider from the source type via the subscription registry.

Falls back to PostgreSQL LISTEN/NOTIFY when source type is ``postgresql``.
"""

# Requirements: REQ-258, REQ-260, REQ-336, REQ-338, REQ-342, REQ-369, REQ-371

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse

from provisa.api.data.stream_end import ending_by_name, new_subscription_id
from provisa.api.errors import ApiError
from provisa.kafka.avro_registry import RegistrySettings, SchemaRegistry, SchemaRegistryRefusal


log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])

CHANNEL_PREFIX = "provisa_"


def _tables_key(table: str, tables: dict, state) -> str | None:
    """Resolve *table* (the bare physical table name from the URL) to its key in a
    CompilationContext's ``tables`` dict.

    Compiled schemas prefix GraphQL field names by domain alias (e.g. physical table
    "pets" becomes field "ps__pets") so two domains can register same-named tables —
    REST/JSON:API already resolve through ``state.table_path_maps`` (REQ-256). The
    ``/data/subscribe/{table}`` route stays domain-agnostic per REQ-219, so this checks
    the bare name first (pre-domain-prefixing schemas, e.g. test-built contexts), then
    falls back to the first ``table_path_maps`` entry across all roles whose
    ``table_name`` matches.
    """
    if table in tables:
        return table
    for path_map in (getattr(state, "table_path_maps", None) or {}).values():
        for gql_field, meta in path_map.items():
            if meta.get("table_name") == table and gql_field in tables:
                return gql_field
    return None


def _build_postgresql_config(state) -> dict:
    return {"pool": state.tenant_db}


def _build_mongodb_config(state, source_id: str) -> dict:
    source_pool = state.source_pools.get(source_id) if state.source_pools else None
    return {"database": source_pool}


def _build_kafka_config(state, source_id: str) -> dict:
    """The brokers of the Kafka source the table belongs to, as its configuration names them."""
    bootstrap = (state.kafka_bootstrap or {}).get(source_id)
    if not bootstrap:
        raise ValueError(f"Kafka source {source_id!r} has no brokers configured for a subscription")
    return {"bootstrap_servers": bootstrap}


def _build_ingest_config(state, source_id: str) -> dict:
    ingest_engine = state.ingest_engines.get(source_id) if state.ingest_engines else None
    return {"engine": ingest_engine}


def _build_rss_feed_url(rss_src, hints: dict) -> str:
    feed_url = hints.get("feed_url")
    if feed_url:
        return feed_url
    use_ssl = hints.get("use_ssl", "true").lower() == "true"
    scheme = "https" if use_ssl else "http"
    path = getattr(rss_src, "path", None) or "/"
    return f"{scheme}://{rss_src.host}:{rss_src.port}{path}"


def _build_rss_config(state, source_id: str) -> dict:  # REQ-342, REQ-344
    rss_src = (state.rss_sources or {}).get(source_id)
    if not rss_src:
        raise ValueError(f"RSS source {source_id!r} is not loaded; no feed to subscribe to")
    hints = getattr(rss_src, "federation_hints", {}) or {}
    config: dict = {"url": _build_rss_feed_url(rss_src, hints)}
    if hints.get("poll_interval"):
        config["poll_interval"] = float(hints["poll_interval"])
    return config


def _parse_ws_subscribe_payload(raw_payload: str) -> dict | None:
    import json as _json

    try:
        return _json.loads(raw_payload)
    except (ValueError, TypeError):
        return None


def _build_websocket_config(state, source_id: str) -> dict:  # REQ-338, REQ-341
    ws_src = (state.websocket_sources or {}).get(source_id)
    if not ws_src:
        raise ValueError(f"WebSocket source {source_id!r} is not loaded; no stream to subscribe to")
    hints = getattr(ws_src, "federation_hints", {}) or {}
    use_ssl = hints.get("use_ssl", "false").lower() == "true"
    scheme = "wss" if use_ssl else "ws"
    path = getattr(ws_src, "path", None) or "/"
    config: dict = {"url": f"{scheme}://{ws_src.host}:{ws_src.port}{path}"}
    raw_payload = hints.get("subscribe_payload")
    if raw_payload:
        parsed = _parse_ws_subscribe_payload(raw_payload)
        if parsed is not None:
            config["subscribe_payload"] = parsed
    if hints.get("event_path"):
        config["event_path"] = hints["event_path"]
    return config


def _build_fallback_config(state, source_id: str, tbl_meta) -> dict:
    from provisa.subscriptions.polling_provider import SourcePoolRowSource

    config: dict = {"row_source": SourcePoolRowSource(state.source_pools, source_id)}
    if tbl_meta is not None:
        wc = getattr(tbl_meta, "watermark_column", None)
        if wc:
            config["watermark_column"] = wc
    return config


# REQ-824: non-PG RDBMS reached via a Debezium connector (no native push mechanism).
# PostgreSQL uses LISTEN/NOTIFY, Kafka/MongoDB use their own consumers — none of them
# route here even when a source-level cdc block is present.
_CDC_DEBEZIUM_SOURCE_TYPES = {"mysql", "mariadb", "sqlserver", "oracle"}


def _build_cdc_config(state, source_id: str) -> dict:  # REQ-824
    """Build the Debezium provider config from source-level CDC transport.

    Transport (bootstrap_servers/topic_prefix/schema_registry_url/consumer_group_id)
    is entered once on the source, never per-table. Fails loud if a table selects
    Debezium CDC on a source that never declared a cdc block.
    """
    src = state.cdc_sources.get(source_id) if state.cdc_sources else None
    if src is None or src.cdc is None:
        raise ValueError(
            f"Source {source_id!r} routes live delivery through Debezium but has no source-level "
            f"cdc transport config (bootstrap_servers/topic_prefix)."
        )
    cdc = src.cdc
    # REQ-931: consumer group resolves source-override → Provisa-level default. The transport
    # fields are sender-dictated (per source); the consumer group is Provisa's receiver identity.
    # state.config.cdc_consumer_group_id always carries the model default ("provisa-debezium").
    return {
        "bootstrap_servers": cdc.bootstrap_servers,
        "topic_prefix": cdc.topic_prefix,
        "schema_registry": RegistrySettings.of(cdc),  # REQ-1951
        "consumer_group_id": cdc.consumer_group_id or state.config.cdc_consumer_group_id,
        "database": src.database,
        "source_type": src.type.value,
    }


async def _refuse_unreachable_registry(source_type: str, source_id: str, tbl_meta, state) -> None:
    """Before the stream opens: a Debezium source that names a schema registry is refused by name
    when that registry is down, does not answer in time, or refuses the source's credentials
    (REQ-1951). Bounded by the registry client's own timeouts and this request's deadline."""
    if source_type == "postgresql":
        return
    if _resolve_provider_type(source_type, source_id, tbl_meta, state) != "debezium":
        return
    src = state.cdc_sources.get(source_id) if state.cdc_sources else None
    settings = RegistrySettings.of(src.cdc) if src is not None and src.cdc is not None else None
    if settings is None:
        return
    registry = SchemaRegistry(settings)
    try:
        await registry.reach()
    except SchemaRegistryRefusal as refused:
        raise ApiError(refused.status, refused.code, str(refused), **refused.params) from refused
    finally:
        await registry.close()


def _resolve_provider_type(source_type: str, source_id: str, tbl_meta, state) -> str:  # REQ-932
    """Resolve the subscription provider from the table's change_signal (REQ-932).

    change_signal is the single inbound axis; ``to_provider`` maps its push values to their
    providers and poll/native to source-type dispatch. A legacy ``live.strategy`` is read through
    until the field is deleted (Phase 4). When a table declares neither, we keep the legacy
    source_type dispatch (rss/websocket/ingest/etc.) plus the REQ-824 heuristic that routes a
    cdc-declaring RDBMS to Debezium.
    """
    from provisa.core.change_signal import (  # noqa: PLC0415
        resolve_effective,
        signal_from_strategy,
        to_provider,
    )

    live = getattr(tbl_meta, "live", None)
    live_strategy = getattr(live, "strategy", None) if live is not None else None
    table_signal = getattr(tbl_meta, "change_signal", None)

    if table_signal is None and signal_from_strategy(live_strategy) is None:
        # No explicit signal and none implied by live.strategy: source_type dispatch + REQ-824 cdc.
        if (
            source_type in _CDC_DEBEZIUM_SOURCE_TYPES
            and state.cdc_sources
            and source_id in state.cdc_sources
        ):
            return "debezium"
        return source_type

    source_signal = next(
        (
            getattr(s, "change_signal", None)
            for s in getattr(state.config, "sources", [])
            if s.id == source_id
        ),
        None,
    )
    sig = resolve_effective(table_signal, source_signal, live_strategy)
    return to_provider(sig, source_type)


def _build_provider_config(  # REQ-258
    source_type: str,
    source_id: str,
    table: str,
    tbl_meta,
    state,
) -> dict:
    if source_type == "postgresql":
        return _build_postgresql_config(state)
    if source_type == "mongodb":
        return _build_mongodb_config(state, source_id)
    if source_type == "kafka":
        return _build_kafka_config(state, source_id)
    if source_type == "ingest":
        return _build_ingest_config(state, source_id)
    if source_type == "rss":
        return _build_rss_config(state, source_id)
    if source_type == "websocket":
        return _build_websocket_config(state, source_id)
    if source_type == "debezium":  # REQ-824
        return _build_cdc_config(state, source_id)
    return _build_fallback_config(state, source_id, tbl_meta)


# A change event as a source reports it: (operation, row). (None, None) is a keepalive tick.
ChangeEvent = tuple[str | None, dict | None]


async def _stream_provider_events(  # REQ-258, REQ-336
    provider,
    table: str,
    disconnect: asyncio.Event,
) -> AsyncGenerator[ChangeEvent, None]:
    """The change events a subscription provider reports for *table*, until the client leaves."""
    try:
        async for event in provider.watch(table):
            if disconnect.is_set():
                break
            yield event.operation, event.row
    finally:
        await provider.close()


async def _provider_sse_generator(  # REQ-258, REQ-260
    table: str,
    source_id: str,
    source_type: str,
    tbl_meta,
    disconnect: asyncio.Event,
) -> AsyncGenerator[ChangeEvent, None]:
    """The change events of the provider *table*'s source and change signal resolve to."""
    from provisa.api.app import state
    from provisa.subscriptions.registry import get_provider, supports_polling_fallback

    provider_type = _resolve_provider_type(source_type, source_id, tbl_meta, state)  # REQ-814
    provider_config = _build_provider_config(provider_type, source_id, table, tbl_meta, state)

    if supports_polling_fallback(provider_type) and "watermark_column" not in provider_config:
        # REQ-260: without an explicit watermark_column, poll subscriptions are unavailable
        # for this source. The stream still opens (REQ-219) but has nothing to deliver —
        # guessing a watermark column that may not exist on the table crashes the connection.
        from provisa.subscriptions.polling_provider import NullNotificationProvider

        provider = NullNotificationProvider()
    else:
        provider = get_provider(provider_type, provider_config)

    async for event in _stream_provider_events(provider, table, disconnect):
        yield event


async def _sse_generator(  # REQ-219, REQ-258
    pool,
    table: str,
    disconnect: asyncio.Event,
) -> AsyncGenerator[ChangeEvent, None]:
    """The change events of a PostgreSQL LISTEN channel, with a keepalive tick every 30s.

    Subscribes to the ``provisa_{table}`` channel on *pool* (the control-plane
    ``Database``, whose listener thread holds the LISTEN connection) until the client leaves.
    """
    channel = f"{CHANNEL_PREFIX}{table}"
    queue: asyncio.Queue[str] = asyncio.Queue()

    def _on_notify(  # pyright: ignore[reportUnusedParameter]
        _conn: object,  # object-ok: NOTIFY callback — the Database handle, opaque at this boundary
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        queue.put_nowait(payload)

    # LISTEN is served by the Database's listener thread on its own connection, delivering to
    # _on_notify on this stream's loop — the stream holds no pooled connection while it waits.
    await pool.add_listener(channel, _on_notify)
    try:
        log.info("SSE: listening on channel %s", channel)
        while not disconnect.is_set():
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                yield None, None
                continue
            try:
                parsed = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                log.warning("SSE: channel %s sent a payload that is not JSON; dropped", channel)
                continue
            yield parsed.get("op"), parsed.get("row")
    finally:
        await pool.remove_listener(channel, _on_notify)
        log.info("SSE: disconnected from channel %s", channel)


def _resolve_tbl_meta(table: str, state):
    """The model's table *table* names (the bare physical name from the URL), from the bound
    org's model-wide context; None when the model has no such table."""
    ctx = state.view_context
    if ctx is None:
        return None
    key = _tables_key(table, ctx.tables, state)
    return ctx.tables[key] if key is not None else None


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, default=str)}\n\n"


async def _governed_changes(  # REQ-336, REQ-286
    events: AsyncGenerator[ChangeEvent, None],
    key,
    ref: str,
    pk: list[str],
) -> AsyncGenerator[str, None]:
    """Each change event's row, as the subscriber's key may read it.

    A source reports the changed row raw. It is never forwarded: an insert or update is read back
    by its key through the one governed pipeline as the subscriber -- row rules, column visibility
    and masks -- and delivered as that read returns it; a row the key may not read is not
    delivered. A delete is delivered only for a row this stream delivered, with the key columns
    as they were delivered; a row that leaves the key's view on update is delivered as a delete.
    """
    from provisa.live.governed import governed_rows

    where = " AND ".join(f'"{c}" = ${i + 1}' for i, c in enumerate(pk))
    sql = f"SELECT * FROM {ref} WHERE {where}"
    shown: dict[tuple, dict] = {}
    yield ": connected\n\n"
    async for op, row in events:
        if op is None or row is None:
            yield ": keepalive\n\n"
            continue
        ident = tuple(row.get(c) for c in pk)
        if any(v is None for v in ident):
            log.warning("SSE: a %s event on %s carries no key; dropped", op, ref)
            continue
        operation = op.upper()
        if operation == "DELETE":
            was = shown.pop(ident, None)
            if was is not None:
                yield _sse({"op": "DELETE", "row": was})
            continue
        rows = await governed_rows(sql, key, params=list(ident))
        if not rows:
            was = shown.pop(ident, None)
            if was is not None:
                yield _sse({"op": "DELETE", "row": was})
            continue
        for governed in rows:
            shown[ident] = {c: governed[c] for c in pk if c in governed}
            yield _sse({"op": operation, "row": governed})


async def _acquire_sse_slot(state, role_id: str) -> str | None:  # REQ-369, REQ-371
    """REQ-369: acquire a concurrent-SSE-subscription slot for the bound org's role.

    Returns the limiter key (to release later) or None when no cap applies. The slot is the org's
    role's: one org's subscribers never take another's (REQ-1266).
    Raises HTTP 429 when the role is at its ``max_sse_subscriptions`` limit.
    """
    from provisa.core.request_context import require_current_org

    limiter = getattr(state, "rate_limiter", None)
    if limiter is None:
        return None
    cap = (state.roles[role_id].get("rate_limit") or {}).get("max_sse_subscriptions")
    if not cap:
        return None
    key = f"rl:sse:{require_current_org()}:{role_id}"
    if not await limiter.acquire(key, cap):
        raise ApiError(
            429, "subscribe.sse_limit_reached", "max concurrent SSE subscriptions reached"
        )
    return key


async def _release_slot_when_done(gen, state, key: str | None):
    """Wrap an SSE generator so the concurrency slot is released when it ends."""
    try:
        async for chunk in gen:
            yield chunk
    finally:
        if key:
            await state.rate_limiter.release(key)


_SSE_HEADERS = {"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}


@router.get("/subscribe/{table}")  # REQ-258, REQ-260, REQ-286, REQ-336, REQ-369
async def subscribe(
    table: str,
    request: Request,
    x_provisa_role: str | None = Header(None),
    query_id: str | None = None,
):
    """Stream SSE events for *table*, governed as the subscriber (REQ-286, REQ-336).

    With ``query_id``, streams that live query's polled rows from the bound org's live engine;
    without, streams the table's change notifications. Either way every row is read through the
    one governed pipeline as the subscriber's key -- its role and the session values its rules
    read -- and a refusal (the table or a column it needs not visible) is answered by name before
    the stream opens.
    """
    from provisa.api.app import state
    from provisa.live.governed import subscriber_key

    role_id = getattr(request.state, "role", None) or x_provisa_role
    key = subscriber_key(role_id)
    assert role_id is not None  # subscriber_key refuses a subscription that acts as no role

    if query_id is not None:
        engine = state.live_engine
        if engine is None or not engine.is_registered(query_id):
            raise ApiError(
                404,
                "subscribe.live_query_not_registered",
                f"Live query {query_id!r} not registered",
                query_id=query_id,
            )
        slot = await _acquire_sse_slot(state, role_id)
        try:
            queue = await engine.subscribe(query_id, key)
        except BaseException:
            if slot:
                assert state.rate_limiter is not None  # the slot was taken from it
                await state.rate_limiter.release(slot)
            raise

        async def _live_event_stream():
            try:
                while True:
                    try:
                        rows = await asyncio.wait_for(queue.get(), timeout=30.0)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    if rows is None:  # the engine ended the stream (its spec changed or stopped)
                        return
                    for row in rows:
                        yield _sse(row)
            finally:
                engine.unsubscribe(query_id, key, queue)

        return StreamingResponse(
            _release_slot_when_done(_live_event_stream(), state, slot),
            media_type="text/event-stream",
            headers=_SSE_HEADERS,
        )

    from provisa.live.governed import governed_rows, primary_key, table_ref

    tbl_meta = _resolve_tbl_meta(table, state)
    if tbl_meta is None:
        raise ApiError(404, "subscribe.table_not_found", f"Table {table!r} not found", table=table)
    pk = primary_key(tbl_meta)
    if not pk:
        raise ApiError(
            422,
            "subscribe.table_has_no_key",
            f"Table {table!r} has no primary key: a change is read back by its key, as the "
            "subscriber may read it",
            table=table,
        )
    ref = table_ref(tbl_meta)
    # Governed once before the stream opens: a refusal reaches the subscriber by name.
    await governed_rows(f"SELECT * FROM {ref} LIMIT 0", key)
    if state.tenant_db is None:
        raise ApiError(503, "subscribe.db_pool_unavailable", "Database pool not available")
    source_id = tbl_meta.source_id
    source_type = state.source_types[source_id]

    await _refuse_unreachable_registry(source_type, source_id, tbl_meta, state)  # REQ-1951

    # REQ-369: enforce the per-role concurrent SSE subscription cap (released when the
    # stream ends, in the return path below).
    _sse_slot = await _acquire_sse_slot(state, role_id)
    disconnect = asyncio.Event()
    subscription_id = new_subscription_id()

    async def on_disconnect() -> None:
        while True:
            if await request.is_disconnected():
                disconnect.set()
                return
            await asyncio.sleep(1)

    async def wrapped_generator() -> AsyncGenerator[str, None]:
        task = asyncio.create_task(on_disconnect())
        try:
            if source_type != "postgresql":
                events = _provider_sse_generator(
                    table, source_id, source_type, tbl_meta, disconnect
                )
            else:
                events = _sse_generator(state.tenant_db, table, disconnect)
            # The stream has answered 200: when it ends on a failure the subscriber is told why in
            # one last frame (provisa/api/data/stream_end.py), never left with a silent close.
            changes = _governed_changes(events, key, ref, pk)
            async for chunk in ending_by_name(changes, subscription_id, f"table {table}"):
                yield chunk
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    return StreamingResponse(
        _release_slot_when_done(wrapped_generator(), state, _sse_slot),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )
