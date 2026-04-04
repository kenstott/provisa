# Copyright (c) 2025 Kenneth Stott
# Canary: 721f6403-65b1-4ac1-a015-a3b4909b4c9e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SSE subscription endpoint via PostgreSQL LISTEN/NOTIFY (REQ-AB2).

GET /data/subscribe/{table} streams server-sent events for INSERT, UPDATE,
and DELETE operations on the subscribed table.  Uses asyncpg `.add_listener()`
on the channel ``provisa_{table}`` and emits JSON payloads.

Requires a PostgreSQL trigger that calls ``pg_notify('provisa_<table>', payload)``
on each DML event.  The payload is a JSON string with keys: op, row.
"""

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
):
    """Stream SSE events for changes on *table*.

    The endpoint opens a PostgreSQL LISTEN on ``provisa_{table}`` and
    forwards each NOTIFY payload as an SSE ``data:`` frame.
    """
    from provisa.api.app import state

    if state.pg_pool is None:
        raise HTTPException(status_code=503, detail="Database pool not available")

    auth_role = getattr(request.state, "role", None)
    role_id = auth_role or x_provisa_role

    disconnect = asyncio.Event()

    async def on_disconnect() -> None:
        """Wait until the client drops the connection, then set the event."""
        while True:
            if await request.is_disconnected():
                disconnect.set()
                return
            await asyncio.sleep(1)

    async def wrapped_generator() -> AsyncGenerator[str, None]:
        task = asyncio.create_task(on_disconnect())
        try:
            async for chunk in _sse_generator(
                state.pg_pool,
                table,
                role_id,
                state.rls_contexts,
                disconnect,
            ):
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
