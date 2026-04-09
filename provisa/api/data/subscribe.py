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

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from provisa.subscriptions.base import ChangeEvent

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


async def _provider_sse_generator(
    table: str,
    source_id: str,
    source_type: str,
    role_id: str | None,
    rls_contexts: dict,
    disconnect: asyncio.Event,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted events from the appropriate subscription provider."""
    from provisa.api.app import state
    from provisa.subscriptions.registry import get_provider

    # Resolve table-level subscription config (watermark_column, soft_delete_column, etc.)
    tbl_meta = None
    for ctx in state.contexts.values():
        tables = getattr(ctx, "tables", {})
        if table in tables:
            tbl_meta = tables[table]
            break

    provider_config: dict = {}
    if source_type == "postgresql":
        provider_config["pool"] = state.pg_pool
    elif source_type == "mongodb":
        source_pool = state.source_pools.get(source_id) if state.source_pools else None
        provider_config["database"] = source_pool
    elif source_type == "kafka":
        ks = state.kafka_table_configs.get(table)
        bootstrap = getattr(ks, "bootstrap_servers", "localhost:9092") if ks else "localhost:9092"
        provider_config["bootstrap_servers"] = bootstrap
    elif source_type == "ingest":
        ingest_engine = state.ingest_engines.get(source_id) if state.ingest_engines else None
        provider_config["engine"] = ingest_engine
    elif source_type == "websocket":
        ws_src = state.websocket_sources.get(source_id) if state.websocket_sources else None
        if ws_src:
            hints = getattr(ws_src, "federation_hints", {}) or {}
            use_ssl = hints.get("use_ssl", "false").lower() == "true"
            scheme = "wss" if use_ssl else "ws"
            path = getattr(ws_src, "path", None) or "/"
            provider_config["url"] = f"{scheme}://{ws_src.host}:{ws_src.port}{path}"
            raw_payload = hints.get("subscribe_payload")
            if raw_payload:
                import json as _json
                try:
                    provider_config["subscribe_payload"] = _json.loads(raw_payload)
                except (ValueError, TypeError):
                    pass
            if hints.get("event_path"):
                provider_config["event_path"] = hints["event_path"]
    else:
        provider_config["pool"] = state.pg_pool
        if tbl_meta is not None:
            wc = getattr(tbl_meta, "watermark_column", None)
            if wc:
                provider_config["watermark_column"] = wc

    provider = get_provider(source_type, provider_config)

    yield ": connected\n\n"

    try:
        async for event in provider.watch(table):
            if disconnect.is_set():
                break

            # RLS filtering
            if role_id and rls_contexts:
                rls_ctx = rls_contexts.get(role_id)
                if rls_ctx and rls_ctx.has_rules():
                    if not _rls_matches(event.row, rls_ctx, table):
                        continue

            payload = json.dumps({
                "op": event.operation.upper(),
                "row": event.row,
            })
            yield f"data: {payload}\n\n"
    finally:
        await provider.close()


async def _sse_generator(
    pool,
    table: str,
    role_id: str | None,
    rls_contexts: dict,
    disconnect: asyncio.Event,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted events from a PostgreSQL LISTEN channel.

    Acquires a dedicated connection from *pool*, subscribes to the
    ``provisa_{table}`` channel, and forwards notifications as SSE events
    until the client disconnects.
    """
    channel = f"{CHANNEL_PREFIX}{table}"
    queue: asyncio.Queue[str] = asyncio.Queue()

    def _on_notify(
        conn: object,
        pid: int,
        channel: str,
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

            # Optional RLS filtering: if the role has RLS rules for this
            # table, drop events whose row doesn't match.  Full SQL-level
            # RLS is enforced at query time; here we do a best-effort
            # client-side filter on the notify payload.
            if role_id and rls_contexts:
                rls_ctx = rls_contexts.get(role_id)
                if rls_ctx and rls_ctx.has_rules():
                    try:
                        parsed = json.loads(payload)
                        if not _rls_matches(parsed.get("row", {}), rls_ctx, table):
                            continue
                    except (json.JSONDecodeError, TypeError):
                        pass

            yield f"data: {payload}\n\n"

    finally:
        try:
            await conn.remove_listener(channel, _on_notify)
        except Exception:
            log.debug("Failed to remove listener on %s", channel, exc_info=True)
        await pool.release(conn)
        log.info("SSE: disconnected from channel %s", channel)


def _rls_matches(row: dict, rls_ctx, table: str) -> bool:
    """Best-effort RLS check on a notification row.

    Checks simple ``column = 'value'`` filters from the RLS context.
    Non-matching or unparseable rules are treated as passing (permissive).
    """
    import re

    for _table_id, expr in rls_ctx.rules.items():
        match = re.match(r"^(\w+)\s*=\s*'([^']*)'$", expr.strip())
        if match:
            col, val = match.group(1), match.group(2)
            if col in row and str(row[col]) != val:
                return False
    return True


@router.get("/subscribe/{table}")
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

    # --- Live query path (Phase AM) ---
    if query_id is not None:
        live_engine = getattr(state, "live_engine", None)
        if live_engine is None:
            raise HTTPException(status_code=503, detail="Live query engine not available")
        if not live_engine.is_registered(query_id):
            raise HTTPException(
                status_code=404,
                detail=f"Live query {query_id!r} not registered",
            )

        # Check visible_to access for approved persisted queries
        if state.pg_pool is not None and role_id:
            async with state.pg_pool.acquire() as _conn:
                _row = await _conn.fetchrow(
                    "SELECT visible_to FROM persisted_queries WHERE stable_id = $1 AND status = 'approved'",
                    query_id,
                )
            if _row is not None:
                _visible = list(_row["visible_to"] or [])
                if _visible and "*" not in _visible and role_id not in _visible:
                    raise HTTPException(status_code=403, detail="Access denied to this query")

        disconnect = asyncio.Event()

        async def on_disconnect_live() -> None:
            while True:
                if await request.is_disconnected():
                    disconnect.set()
                    return
                await asyncio.sleep(1)

        async def live_generator() -> AsyncGenerator[str, None]:
            q = live_engine.subscribe(query_id)
            task = asyncio.create_task(on_disconnect_live())
            yield ": connected\n\n"
            try:
                while not disconnect.is_set():
                    try:
                        batch = await asyncio.wait_for(q.get(), timeout=30.0)
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                        continue
                    if batch is None:
                        break
                    payload = json.dumps({"rows": batch})
                    yield f"data: {payload}\n\n"
            finally:
                live_engine.unsubscribe(query_id, q)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        return StreamingResponse(
            live_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

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
                    disconnect,
                )
            else:
                gen = _sse_generator(
                    state.pg_pool,
                    table,
                    role_id,
                    state.rls_contexts,
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
        wrapped_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
