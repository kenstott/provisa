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

# Requirements: REQ-176, REQ-177, REQ-219, REQ-258, REQ-260, REQ-261, REQ-282, REQ-286

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import TYPE_CHECKING, AsyncGenerator, cast

if TYPE_CHECKING:
    from provisa.subscriptions.pg_provider import PgNotificationProvider
from urllib.parse import urlparse

from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse
from graphql.language.ast import FieldNode, OperationDefinitionNode, SelectionSetNode
from graphql.language import print_ast

from provisa.core.operator_floor import OperatorFloorError

log = logging.getLogger(__name__)


class SubscriptionFloorViolation(OperatorFloorError):
    """A subscription request would move a setting the operator owns (REQ-030)."""


def sink_broker_within_floor(requested: str | None) -> str:  # REQ-030, REQ-176
    """The Kafka cluster a sink writes to: the operator's ``KAFKA_BOOTSTRAP_SERVERS``. A request may
    repeat it; any other broker — or a sink when the operator configured no cluster — is refused."""
    operator = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not operator:
        raise SubscriptionFloorViolation(
            "a Kafka sink needs the operator's broker: KAFKA_BOOTSTRAP_SERVERS is not configured"
        )
    if requested and requested != operator:
        raise SubscriptionFloorViolation(
            f"sink broker {requested!r} is outside the operator's floor: the operator set "
            f"KAFKA_BOOTSTRAP_SERVERS={operator!r}. Omit the broker or use that one."
        )
    return operator


def watermark_within_floor(  # REQ-030, REQ-260
    requested: str | None, registered: str | None, table_name: str
) -> str | None:
    """The column ``table_name`` is polled by: the operator's registered watermark. Whether and how
    a table is polled sets its upstream load, so a request may only repeat it — a different column,
    or one on a table the operator gave none, is refused."""
    if requested is None or requested == registered:
        return registered
    raise SubscriptionFloorViolation(
        f"@watermark({requested}) on {table_name!r} is outside the operator's floor: the operator "
        + (f"polls it by {registered!r}." if registered else "registered no watermark for it.")
        + " Remove the @watermark directive."
    )


def _collect_related_tables(selection_set: SelectionSetNode, type_name: str, ctx) -> set[str]:
    """Recursively collect physical table names referenced via joins in the selection."""
    tables: set[str] = set()
    for sel in selection_set.selections:
        if not isinstance(sel, FieldNode):
            continue
        join_key = (type_name, sel.name.value)
        if join_key in ctx.joins:
            join_meta = ctx.joins[join_key]
            tables.add(join_meta.target.table_name)
            if sel.selection_set:
                tables |= _collect_related_tables(
                    sel.selection_set, join_meta.target.type_name, ctx
                )
    return tables


async def handle_subscription_sse(  # REQ-219, REQ-258, REQ-260, REQ-282
    document,
    ctx,
    rls,
    state,
    variables: dict | None,
    role,
    role_id: str,
    raw_request: Request,
    directives=None,
) -> StreamingResponse | JSONResponse:
    """Execute a GraphQL subscription and stream results as SSE.

    The client sends POST /data/graphql with Accept: text/event-stream.
    Each table change triggers a re-execution of the equivalent query and
    streams the result as `data: {json}\\n\\n`.

    If the request includes ``X-Provisa-Sink: kafka://[broker:port]/topic``,
    results are published to the named Kafka topic instead of streamed back.
    The response is ``202 Accepted`` and the sink runs as a background task
    for the lifetime of the server process.
    """
    # Extract subscription field names and selection set
    sub_fields: list[str] = []
    sub_selection = None
    for defn in document.definitions:
        if isinstance(defn, OperationDefinitionNode):
            sub_selection = defn.selection_set
            for sel in defn.selection_set.selections:
                if isinstance(sel, FieldNode):
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
    if not state.source_types or source_id not in state.source_types:
        raise KeyError(f"Unknown source_id {source_id!r} in source_types")
    source_type = state.source_types[source_id]

    # Collect all tables referenced in the selection (root + related via joins)
    ctx = state.contexts[role_id]
    related_tables = (
        _collect_related_tables(
            sub_selection.selections[0].selection_set,  # type: ignore[union-attr]
            table_meta.type_name,
            ctx,
        )
        if (
            sub_selection.selections
            and isinstance(sub_selection.selections[0], FieldNode)
            and sub_selection.selections[0].selection_set
        )
        else set()
    )
    all_watch_tables = [table_name] + sorted(related_tables - {table_name})

    # Convert subscription selection set → equivalent query string
    selection_text = print_ast(sub_selection)
    query_text = f"query {selection_text}"

    schema = state.schemas[role_id]

    # Resolve directives: extract from AST if not passed in (e.g. direct SSE calls)
    if directives is None:
        from provisa.compiler.directives import (
            extract_directives,
            extract_directives_from_sql_comments,
            merge_directives,
        )

        _sql_directives = extract_directives_from_sql_comments(print_ast(document))
        directives = merge_directives(_sql_directives, extract_directives(document))

    # REQ-030: the watermark is the operator's (state.table_watermarks) — a request may only repeat it.
    _watermark = watermark_within_floor(
        directives.watermark_column if directives else None,
        (state.table_watermarks or {}).get(table_name),
        table_name,
    )

    # Kafka sink redirect — @sink directive or X-Provisa-Sink header
    sink_topic = (directives.sink_topic if directives else None) or None
    sink_broker = (directives.sink_broker if directives else None) or None
    sink_header = raw_request.headers.get("x-provisa-sink", "")
    if not sink_topic and sink_header:
        # Parse header URI into topic/broker for _launch_kafka_sink
        _parsed = urlparse(sink_header)
        sink_topic = _parsed.path.lstrip("/") or None
        sink_broker = _parsed.netloc or None
    if sink_topic:
        _broker = sink_broker_within_floor(sink_broker)  # REQ-030: the operator's cluster only
        return await _launch_kafka_sink(
            sink_header=f"kafka://{_broker}/{sink_topic}",
            table_name=table_name,
            table_meta=table_meta,
            source_id=source_id,
            source_type=source_type,
            all_watch_tables=all_watch_tables,
            query_text=query_text,
            schema=schema,
            ctx=ctx,
            rls=rls,
            state=state,
            variables=variables,
            role=role,
            role_id=role_id,
        )

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
        result = await _handle_query(
            q_doc,
            ctx,
            rls,
            state,
            variables,
            role,
            "json",
            role_id,
            cache_ttl=None,
            cache_opt_in=False,  # REQ-544: a subscription poll never reads or writes the cache
        )
        # JSONResponse stores serialized bytes in .body
        if isinstance(result, JSONResponse):
            body = result.body
            if isinstance(body, memoryview):
                return json.loads(bytes(body))
            return json.loads(body)
        return result  # type: ignore[return-value]

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

            effective_watermark = _watermark

            # Watch for table changes — pg_notify triggers preferred; poll as fallback
            use_polling_fallback = (
                source_type == "postgresql"
                and table_name not in (state.pg_notify_tables or set())
                and effective_watermark is not None
            )
            use_engine_polling = (
                not use_polling_fallback
                and source_type != "postgresql"
                and effective_watermark is not None
            )

            provider_config: dict = {}
            if use_polling_fallback:
                from provisa.subscriptions.polling_provider import DatabaseRowSource

                provider_config["row_source"] = DatabaseRowSource(state.tenant_db)
                provider_config["watermark_column"] = effective_watermark
            elif use_engine_polling:
                pass
            elif source_type == "postgresql" and state.tenant_db:
                provider_config["pool"] = state.tenant_db
            elif source_type == "mongodb":
                source_pool = state.source_pools.get(source_id) if state.source_pools else None
                provider_config["database"] = source_pool

            try:
                if use_polling_fallback:
                    from provisa.subscriptions.polling_provider import PollingNotificationProvider

                    watermark_col: str = cast(str, provider_config["watermark_column"])
                    provider = PollingNotificationProvider(
                        row_source=provider_config["row_source"],
                        watermark_column=watermark_col,
                    )
                elif use_engine_polling:
                    assert effective_watermark is not None
                    # The engine supplies its change-polling provider through the seam; a native
                    # engine with no catalog-polling transport returns None (subscription inactive).
                    provider = state.federation_engine.polling_provider(
                        table_meta.catalog_name or "hive",
                        table_meta.schema_name or "default",
                        table_meta.table_name,
                        effective_watermark,
                    )
                    if provider is None:
                        return
                else:
                    from provisa.subscriptions.registry import get_provider

                    provider = get_provider(source_type, provider_config)
            except Exception as exc:
                log.warning("Subscription provider unavailable: %s", exc)
                return

            try:
                use_many = (
                    not use_polling_fallback
                    and source_type == "postgresql"
                    and len(all_watch_tables) > 1
                    and hasattr(provider, "watch_many")
                )
                watcher = (
                    cast("PgNotificationProvider", provider).watch_many(all_watch_tables)
                    if use_many
                    else provider.watch(table_name)
                )
                async for _ in watcher:
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


def _parse_sink_uri(sink_header: str) -> tuple[str, str]:
    """Parse ``kafka://[broker:port]/topic`` → (bootstrap_servers, topic).

    If broker is omitted, falls back to ``KAFKA_BOOTSTRAP_SERVERS`` env var
    or ``localhost:9092``.
    """
    parsed = urlparse(sink_header)
    topic = parsed.path.lstrip("/")
    if not topic:
        raise ValueError(f"No topic in sink URI: {sink_header!r}")
    broker = parsed.netloc or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return broker, topic


async def _launch_kafka_sink(  # REQ-176, REQ-177, REQ-286
    sink_header: str,
    table_name: str,
    table_meta,
    source_id: str,
    source_type: str,
    all_watch_tables: list[str],
    query_text: str,
    schema,
    ctx,
    rls,
    state,
    variables: dict | None,
    role,
    role_id: str,
) -> JSONResponse:
    """Start a background task that publishes subscription results to Kafka.

    Returns ``202 Accepted`` immediately. The sink runs for the lifetime of
    the server process (or until shutdown).
    """
    try:
        broker, topic = _parse_sink_uri(sink_header)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    async def _run_query() -> dict:
        from provisa.compiler.parser import parse_query as _parse
        from provisa.api.data.endpoint import _handle_query

        q_doc = _parse(schema, query_text, variables)
        result = await _handle_query(
            q_doc,
            ctx,
            rls,
            state,
            variables,
            role,
            "json",
            role_id,
            cache_ttl=None,
            cache_opt_in=False,  # REQ-544: a subscription poll never reads or writes the cache
        )
        if isinstance(result, JSONResponse):
            body = result.body
            if isinstance(body, memoryview):
                return json.loads(bytes(body))
            return json.loads(body)
        return result  # type: ignore[return-value]

    async def _sink_loop() -> None:
        from provisa.kafka.sink import KafkaProducer

        producer = KafkaProducer(bootstrap_servers=broker)
        log.info("Kafka sink started: %s → %s", table_name, topic)

        use_polling_fallback = (
            source_type == "postgresql"
            and table_name not in (state.pg_notify_tables or set())
            and table_name in (state.table_watermarks or {})
        )
        use_engine_polling = (
            not use_polling_fallback
            and source_type != "postgresql"
            and table_name in (state.table_watermarks or {})
        )

        try:
            if use_polling_fallback:
                from provisa.subscriptions.polling_provider import (
                    DatabaseRowSource,
                    PollingNotificationProvider,
                )

                provider = PollingNotificationProvider(
                    row_source=DatabaseRowSource(state.tenant_db),
                    watermark_column=state.table_watermarks[table_name],
                )
            elif use_engine_polling:
                # Engine-provided change-polling; a native engine without a catalog-polling
                # transport returns None (subscription inactive for this table).
                provider = state.federation_engine.polling_provider(
                    table_meta.catalog_name or "hive",
                    table_meta.schema_name or "default",
                    table_meta.table_name,
                    state.table_watermarks[table_name],
                )
                if provider is None:
                    return
            else:
                from provisa.subscriptions.registry import get_provider

                provider_config: dict = {}
                if source_type == "postgresql" and state.tenant_db:
                    provider_config["pool"] = state.tenant_db
                elif source_type == "mongodb":
                    source_pool = state.source_pools.get(source_id) if state.source_pools else None
                    provider_config["database"] = source_pool
                provider = get_provider(source_type, provider_config)
        except Exception as exc:
            log.warning("Kafka sink: provider unavailable: %s", exc)
            return

        try:
            use_many = (
                not use_polling_fallback
                and not use_engine_polling
                and source_type == "postgresql"
                and len(all_watch_tables) > 1
                and hasattr(provider, "watch_many")
            )
            watcher = (
                cast("PgNotificationProvider", provider).watch_many(all_watch_tables)
                if use_many
                else provider.watch(table_name)
            )
            async for _ in watcher:
                try:
                    data = await _run_query()
                    rows = data.get("data", data) if isinstance(data, dict) else data
                    payload = rows if isinstance(rows, list) else [rows]
                    await producer.publish_rows(topic, payload, columns=[])
                    log.debug("Kafka sink: published to %s", topic)
                except Exception as exc:
                    log.warning("Kafka sink publish failed: %s", exc)
        finally:
            try:
                await provider.close()
            except Exception:
                pass
            producer.close()
            log.info("Kafka sink stopped: %s → %s", table_name, topic)

    from provisa.core.connection_loop import spawn_long_lived

    # REQ-1882: the sink outlives this request (the response is a 202) and runs until its source
    # disconnects, so it gets its own long-lived thread and loop — never the process loop, and
    # never a pooled worker it would pin indefinitely. Its provider/producer are built inside it.
    spawn_long_lived(_sink_loop(), name=f"kafka-sink:{table_name}")
    return JSONResponse(
        status_code=202,
        content={
            "status": "streaming",
            "sink": sink_header,
            "table": table_name,
        },
    )
