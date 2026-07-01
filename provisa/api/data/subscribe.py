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

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import StreamingResponse


log = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])

CHANNEL_PREFIX = "provisa_"


def _resolve_table_source(table: str) -> tuple[str, str] | None:
    """Return (source_id, source_type) for *table* if it exists in config.

    Returns None if the table is not found in any role's schema.
    """
    from provisa.api.app import state

    # Check contexts — each context has table metadata keyed by table name
    for ctx in state.contexts.values():
        tables = getattr(ctx, "tables", {})
        if table in tables:
            tbl = tables[table]
            source_id = getattr(tbl, "source_id", None)
            if source_id and source_id in state.source_types:
                return source_id, state.source_types[source_id]

    # Fallback: check source_types for a default postgresql source
    for sid, stype in state.source_types.items():
        return sid, stype

    return None


def _build_postgresql_config(state) -> dict:
    return {"pool": state.pg_pool}


def _build_mongodb_config(state, source_id: str) -> dict:
    source_pool = state.source_pools.get(source_id) if state.source_pools else None
    return {"database": source_pool}


def _build_kafka_config(state, table: str) -> dict:
    ks = state.kafka_table_configs.get(table)
    bootstrap = getattr(ks, "bootstrap_servers", "localhost:9092") if ks else "localhost:9092"
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
    rss_src = state.rss_sources.get(source_id) if state.rss_sources else None
    if not rss_src:
        return {}
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
    ws_src = state.websocket_sources.get(source_id) if state.websocket_sources else None
    if not ws_src:
        return {}
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


def _build_fallback_config(state, tbl_meta) -> dict:
    config: dict = {"pool": state.pg_pool}
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
    return {
        "bootstrap_servers": cdc.bootstrap_servers,
        "topic_prefix": cdc.topic_prefix,
        "schema_registry_url": cdc.schema_registry_url,
        "consumer_group_id": cdc.consumer_group_id,
        "database": src.database,
        "source_type": src.type.value,
    }


def _resolve_provider_type(source_type: str, source_id: str, state) -> str:  # REQ-824
    """Route non-PG RDBMS sources with a source-level cdc block to the Debezium provider."""
    if (
        source_type in _CDC_DEBEZIUM_SOURCE_TYPES
        and state.cdc_sources
        and source_id in state.cdc_sources
    ):
        return "debezium"
    return source_type


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
        return _build_kafka_config(state, table)
    if source_type == "ingest":
        return _build_ingest_config(state, source_id)
    if source_type == "rss":
        return _build_rss_config(state, source_id)
    if source_type == "websocket":
        return _build_websocket_config(state, source_id)
    if source_type == "debezium":  # REQ-824
        return _build_cdc_config(state, source_id)
    return _build_fallback_config(state, tbl_meta)


def _resolve_tbl_meta(table: str, state):
    for ctx in state.contexts.values():
        tables = getattr(ctx, "tables", {})
        if table in tables:
            return tables[table]
    return None


async def _stream_provider_events(  # REQ-258, REQ-336
    provider,
    table: str,
    table_id: int | None,
    role_id: str | None,
    rls_contexts: dict,
    masking_rules,
    disconnect: asyncio.Event,
) -> AsyncGenerator[str, None]:
    yield ": connected\n\n"
    try:
        async for event in provider.watch(table):
            if disconnect.is_set():
                break
            if role_id and rls_contexts:
                rls_ctx = rls_contexts.get(role_id)
                if rls_ctx and rls_ctx.has_rules():
                    if not _rls_matches(event.row, rls_ctx, table):
                        continue
            row = _mask_row(event.row, table_id, role_id, masking_rules)
            payload = json.dumps({"op": event.operation.upper(), "row": row}, default=str)
            yield f"data: {payload}\n\n"
    finally:
        await provider.close()


async def _provider_sse_generator(  # REQ-258, REQ-260
    table: str,
    source_id: str,
    source_type: str,
    role_id: str | None,
    rls_contexts: dict,
    masking_rules,
    disconnect: asyncio.Event,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted events from the appropriate subscription provider."""
    from provisa.api.app import state
    from provisa.subscriptions.registry import get_provider

    tbl_meta = _resolve_tbl_meta(table, state)
    table_id = getattr(tbl_meta, "table_id", None) if tbl_meta else None
    provider_type = _resolve_provider_type(source_type, source_id, state)  # REQ-824
    provider_config = _build_provider_config(provider_type, source_id, table, tbl_meta, state)
    provider = get_provider(provider_type, provider_config)

    async for chunk in _stream_provider_events(
        provider, table, table_id, role_id, rls_contexts, masking_rules, disconnect
    ):
        yield chunk


async def _sse_generator(  # REQ-219, REQ-258
    pool,
    table: str,
    table_id: int | None,
    role_id: str | None,
    rls_contexts: dict,
    masking_rules,
    disconnect: asyncio.Event,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted events from a PostgreSQL LISTEN channel.

    Acquires a dedicated connection from *pool*, subscribes to the
    ``provisa_{table}`` channel, and forwards notifications as SSE events
    until the client disconnects.
    """
    channel = f"{CHANNEL_PREFIX}{table}"
    queue: asyncio.Queue[str] = asyncio.Queue()

    def _on_notify(  # pyright: ignore[reportUnusedParameter]
        _conn: object,  # object-ok: asyncpg notify callback — connection type is opaque at this boundary
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        queue.put_nowait(payload)

    conn = await pool.acquire()
    try:
        await conn.add_listener(channel, _on_notify)
        log.info("SSE: listening on channel %s (role=%s)", channel, role_id)

        # Initial keepalive so the client sees headers immediately
        yield ": connected\n\n"

        while not disconnect.is_set():
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                # Send SSE comment as keepalive
                yield ": keepalive\n\n"
                continue

            # RLS filtering + column masking on the notify payload (REQ-336): subscriptions
            # enforce the same row-level and column-level governance as local-table queries.
            # Full SQL-level RLS is also enforced at query time; this is the serving-layer
            # filter on streamed change events.
            try:
                parsed = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                parsed = None

            if parsed is not None and role_id:
                row = parsed.get("row", {})
                if rls_contexts:
                    rls_ctx = rls_contexts.get(role_id)
                    if rls_ctx and rls_ctx.has_rules() and not _rls_matches(row, rls_ctx, table):
                        continue
                masked = _mask_row(row, table_id, role_id, masking_rules)
                if masked is not row:
                    parsed["row"] = masked
                    payload = json.dumps(parsed, default=str)

            yield f"data: {payload}\n\n"

    finally:
        try:
            await conn.remove_listener(channel, _on_notify)
        except Exception:
            log.debug("Failed to remove listener on %s", channel, exc_info=True)
        await pool.release(conn)
        log.info("SSE: disconnected from channel %s", channel)


def _resolve_table_id(table: str, state) -> int | None:
    meta = _resolve_tbl_meta(table, state)
    return getattr(meta, "table_id", None) if meta else None


def _mask_row(row: dict, table_id: int | None, role_id: str | None, masking_rules) -> dict:
    """Apply column masking to a change-event row (REQ-336).

    Mirrors local-table governance: a role not in a column's ``unmasked_to`` receives the
    masked value, computed by the same rules Stage 2 injects into SQL. Returns the row
    unchanged when no masking applies.
    """
    if not (role_id and masking_rules and table_id is not None):
        return row
    col_rules = masking_rules.get((table_id, role_id))
    if not col_rules:
        return row
    from provisa.security.masking import apply_mask_to_value

    masked = dict(row)
    for col, (rule, dtype) in col_rules.items():
        if col in masked:
            masked[col] = apply_mask_to_value(rule, masked[col], dtype)
    return masked


def _rls_matches(row: dict, rls_ctx, _table: str) -> bool:  # pyright: ignore[reportUnusedParameter]
    """Best-effort RLS check on a notification row.

    Checks simple ``column = 'value'`` filters from the RLS context.
    Non-matching or unparseable rules are treated as passing (permissive).
    """
    import re

    for _, expr in rls_ctx.rules.items():
        match = re.match(r"^(\w+)\s*=\s*'([^']*)'$", expr.strip())
        if match:
            col, val = match.group(1), match.group(2)
            if col in row and str(row[col]) != val:
                return False
    return True


async def _acquire_sse_slot(state, role_id: str | None) -> str | None:  # REQ-369, REQ-371
    """REQ-369: acquire a concurrent-SSE-subscription slot for the role.

    Returns the limiter key (to release later) or None when no cap applies.
    Raises HTTP 429 when the role is at its ``max_sse_subscriptions`` limit.
    """
    limiter = getattr(state, "rate_limiter", None)
    if not (limiter and role_id):
        return None
    role = state.roles.get(role_id) or {}
    cap = (role.get("rate_limit") or {}).get("max_sse_subscriptions")
    if not cap:
        return None
    key = f"rl:sse:{role_id}"
    if not await limiter.acquire(key, cap):
        raise HTTPException(status_code=429, detail="max concurrent SSE subscriptions reached")
    return key


async def _release_slot_when_done(gen, state, key: str | None):
    """Wrap an SSE generator so the concurrency slot is released when it ends."""
    try:
        async for chunk in gen:
            yield chunk
    finally:
        if key:
            await state.rate_limiter.release(key)


@router.get("/subscribe/{table}")  # REQ-258, REQ-260, REQ-336, REQ-369
async def subscribe(
    table: str,
    request: Request,
    x_provisa_role: str | None = Header(None),
    query_id: str | None = None,
):
    """Stream SSE events for changes on *table*.

    When ``query_id`` is provided, streams live query results from the
    LiveEngine (Phase AM) rather than database change notifications.
    The query_id must match an approved persisted query registered with
    the live engine.

    Without ``query_id``, validates the table exists in the role's schema,
    resolves the appropriate notification provider, and streams SSE events.
    """
    from provisa.api.app import state

    auth_role = getattr(request.state, "role", None)
    role_id = auth_role or x_provisa_role

    if query_id is not None:
        # Route to live engine SSE output
        if state.live_engine is None or not state.live_engine.is_registered(query_id):
            raise HTTPException(status_code=404, detail=f"Live query {query_id!r} not registered")
        _engine = state.live_engine
        queue = _engine.subscribe(query_id)

        async def _live_event_stream():
            try:
                while True:
                    rows = await asyncio.wait_for(queue.get(), timeout=30.0)
                    for row in rows:
                        yield f"data: {json.dumps(row, default=str)}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
            except asyncio.CancelledError:
                pass
            finally:
                _engine.unsubscribe(query_id, queue)

        return StreamingResponse(
            _live_event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # REQ-369: enforce the per-role concurrent SSE subscription cap (released when the
    # stream ends, in the return path below).
    _sse_slot = await _acquire_sse_slot(state, role_id)

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database pool not available")

    # Validate table exists in role's schema
    table_found = False
    if role_id and role_id in state.contexts:
        ctx = state.contexts[role_id]
        tables = getattr(ctx, "tables", {})
        if table in tables:
            table_found = True
    # If no role context, check all contexts
    if not table_found:
        for ctx in state.contexts.values():
            tables = getattr(ctx, "tables", {})
            if table in tables:
                table_found = True
                break

    if state.contexts and not table_found:
        raise HTTPException(status_code=404, detail=f"Table {table!r} not found")

    # Resolve provider from source type
    source_info = _resolve_table_source(table)

    table_id = _resolve_table_id(table, state)
    disconnect = asyncio.Event()

    async def on_disconnect() -> None:
        while True:
            if await request.is_disconnected():
                disconnect.set()
                return
            await asyncio.sleep(1)

    async def wrapped_generator() -> AsyncGenerator[str, None]:
        task = asyncio.create_task(on_disconnect())
        try:
            if source_info and source_info[1] != "postgresql":
                gen = _provider_sse_generator(
                    table,
                    source_info[0],
                    source_info[1],
                    role_id,
                    state.rls_contexts,
                    state.masking_rules,
                    disconnect,
                )
            else:
                gen = _sse_generator(
                    state.pg_pool,
                    table,
                    table_id,
                    role_id,
                    state.rls_contexts,
                    state.masking_rules,
                    disconnect,
                )
            async for chunk in gen:
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
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
