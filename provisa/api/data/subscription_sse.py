# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3a2d1e-9b4c-4f7e-a1d2-3c5e7f9b2d4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SSE handler for GraphQL subscription operations over POST /data/graphql."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from fastapi import Request
from fastapi.responses import StreamingResponse
from graphql.language.ast import OperationDefinitionNode
from graphql.language import print_ast

log = logging.getLogger(__name__)


async def handle_subscription_sse(
    document,
    ctx,
    rls,
    state,
    variables: dict | None,
    role,
    role_id: str,
    raw_request: Request,
) -> StreamingResponse:
    """Execute a GraphQL subscription and stream results as SSE.

    The client sends POST /data/graphql with Accept: text/event-stream.
    Each table change triggers a re-execution of the equivalent query and
    streams the result as `data: {json}\\n\\n`.
    """
    # Extract subscription field names and selection set
    sub_fields: list[str] = []
    sub_selection = None
    for defn in document.definitions:
        if isinstance(defn, OperationDefinitionNode):
            sub_selection = defn.selection_set
            for sel in defn.selection_set.selections:
                if hasattr(sel, "name"):
                    sub_fields.append(sel.name.value)

    if not sub_fields or not sub_selection:
        return _error_stream({"errors": [{"message": "No subscription fields"}]})

    # Find table metadata for the first subscription field
    table_meta = ctx.tables.get(sub_fields[0])
    if table_meta is None:
        return _error_stream(
            {"errors": [{"message": f"Unknown subscription field: {sub_fields[0]!r}"}]}
        )

    table_name = table_meta.table_name
    source_id = table_meta.source_id
    source_type = (state.source_types or {}).get(source_id, "postgresql")

    # Convert subscription selection set → equivalent query string
    selection_text = print_ast(sub_selection)
    query_text = f"query {selection_text}"

    schema = state.schemas[role_id]

    disconnect = asyncio.Event()

    async def _on_disconnect() -> None:
        while True:
            if await raw_request.is_disconnected():
                disconnect.set()
                return
            await asyncio.sleep(1)

    async def _run_query() -> dict:
        from provisa.compiler.parser import parse_query as _parse
        from provisa.api.data.endpoint import _handle_query
        q_doc = _parse(schema, query_text, variables)
        result = await _handle_query(q_doc, ctx, rls, state, variables, role, "json", role_id)
        # JSONResponse stores serialized bytes in .body
        if hasattr(result, "body"):
            return json.loads(result.body)
        return result

    async def generate() -> AsyncGenerator[str, None]:
        task = asyncio.create_task(_on_disconnect())
        try:
            # Initial result
            try:
                data = await _run_query()
                yield f"data: {json.dumps(data)}\n\n"
            except Exception as exc:
                log.warning("Subscription initial query failed: %s", exc)
                yield f"data: {json.dumps({'errors': [{'message': str(exc)}]})}\n\n"
                return

            # Watch for table changes
            provider_config: dict = {}
            if source_type == "postgresql" and state.pg_pool:
                provider_config["pool"] = state.pg_pool
            elif source_type == "mongodb":
                source_pool = state.source_pools.get(source_id) if state.source_pools else None
                provider_config["database"] = source_pool

            try:
                from provisa.subscriptions.registry import get_provider
                provider = get_provider(source_type, provider_config)
            except Exception as exc:
                log.warning("Subscription provider unavailable: %s", exc)
                return

            try:
                async for _event in provider.watch(table_name):
                    if disconnect.is_set():
                        break
                    try:
                        data = await _run_query()
                        yield f"data: {json.dumps(data)}\n\n"
                    except Exception as exc:
                        log.warning("Subscription re-query failed: %s", exc)
                        yield f"data: {json.dumps({'errors': [{'message': str(exc)}]})}\n\n"
            finally:
                try:
                    await provider.close()
                except Exception:
                    pass

        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _error_stream(payload: dict) -> StreamingResponse:
    async def _gen() -> AsyncGenerator[str, None]:
        yield f"data: {json.dumps(payload)}\n\n"
    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
