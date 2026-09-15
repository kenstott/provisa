# Copyright (c) 2026 Kenneth Stott
# Canary: 8f3c2a17-6b40-4d19-9e52-1a7c0d3b8e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CDC landing consumer (REQ-932): apply a provider's change stream to a landed table.

When a table's change_signal is a push signal (debezium/kafka) and its source is materialized,
the provider's ChangeEvents are applied to the landed copy by primary key — upsert on
insert/update, tombstone on delete. This is the streaming counterpart to the periodic
replace/append refresh, and the only landing path that carries hard deletes.

REQ-1733: an optional debounce batches consecutive events into one ``apply_cdc`` call instead of
one per message — the same quiet+max_delay shape REQ-963 already uses for live-MV recompute, but
independent of it (this is the CDC streaming path, not the poll/MV tick loop). ``debounce_quiet=0``
(the default) preserves the original one-event-per-apply_cdc behavior exactly — no batching.

REQ-1734: after each successful flush, ``provider.ack(buffer)`` is called on exactly the events
just landed — for Kafka this commits those events' offsets specifically (never "wherever the
consumer currently is"), closing the crash-between-yield-and-land gap debounce/backpressure
buffering would otherwise widen. No-op for a provider with no offset/position concept.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

_SENTINEL = object()  # marks a clean end-of-stream on the pump queue


async def consume_cdc_into_store(
    provider,
    land_fn: Callable[[list[Any]], Awaitable[dict[str, int]]],
    *,
    schema: str,
    table: str,
    disconnect: asyncio.Event,
    watch_target: str | None = None,
    debounce_quiet: float = 0.0,
    debounce_max_delay: float = 5.0,
    queue_maxsize: int = 10_000,
) -> dict[str, int]:
    """Drain ``provider.watch(watch_target)`` into the landed ``schema.table``, applying each batch
    through ``land_fn(events) -> {"upsert": int, "delete": int}`` — the caller's own write face
    (REQ-1733/REQ-989: an embedded single-writer store lands through the engine's own connection;
    every other store opens its own per-batch connection), never a connection this function holds
    itself. ``columns``/``schema``/``table`` are carried for logging only; the actual DDL/landing
    shape lives behind ``land_fn``. ``watch_target`` defaults to ``table`` (the common case: the CDC
    target and the landed table share a name), but must be passed explicitly whenever they differ —
    e.g. Kafka, where ``table`` is the LANDED table's physical (often mangled) name but
    ``provider.watch()`` needs the Kafka TOPIC (KafkaNotificationProvider.watch() uses its argument
    directly as the topic name), never the landed name.

    ``debounce_quiet=0`` (default): every event is applied to the store as it arrives — no
    batching, one ``apply_cdc`` call per message, byte-identical to the pre-REQ-1733 behavior.

    ``debounce_quiet>0``: events are buffered and applied together once ``debounce_quiet`` seconds
    have elapsed with no new event (a quiet period), or ``debounce_max_delay`` seconds have elapsed
    since the first buffered event, whichever comes first — the same deadline formula REQ-963 uses
    for live-MV debounce: ``deadline = min(last_event + quiet, first_event + max_delay)``. This
    trades landing latency for fewer, larger writes under bursty traffic while capping staleness.

    A background task pumps ``provider.watch()`` into a BOUNDED internal queue (``queue_maxsize``)
    so a debounce timeout (``asyncio.wait_for`` on the QUEUE, never on the generator itself) never
    cancels the generator's in-flight ``__anext__()`` — doing that directly kills the generator
    permanently (a timed-out ``wait_for`` injects ``CancelledError`` into whatever the generator
    was awaiting), which would silently stop the stream after the first debounce timeout. The
    bound is the BACKPRESSURE mechanism: once full, the pump's ``queue.put()`` blocks until
    ``_flush()`` drains it, which stalls ``provider.watch()`` itself — for Kafka this stalls
    aiokafka's own internal fetch behind the unconsumed ``async for``; for WebSocket it simply
    stops reading the socket, ordinary TCP flow control back to the sender. Landing (a DB write)
    is never allowed to fall arbitrarily far behind ingestion and exhaust memory.

    REQ-1734: KafkaNotificationProvider's consumer runs with auto-commit OFF — the offset only
    advances via ``provider.ack(buffer)`` below, called after a batch is durably landed, for
    exactly those events. This closes the crash-between-yield-and-land gap debounce/backpressure
    buffering would otherwise widen (a time-based auto-commit advances regardless of whether the
    buffered message was ever landed).

    Returns cumulative {upsert, delete} counts. Stops when ``disconnect`` is set or the provider
    stream ends (flushing any partial batch first); always closes the provider. A primary key is
    required (enforced downstream)."""
    totals = {"upsert": 0, "delete": 0}
    loop = asyncio.get_event_loop()
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_maxsize)
    target = watch_target if watch_target is not None else table

    async def _pump() -> None:
        try:
            async for event in provider.watch(target):
                await queue.put(event)
        finally:
            await queue.put(_SENTINEL)

    pump_task = asyncio.create_task(_pump())
    buffer: list[Any] = []
    batch_started_at: float | None = None
    stream_ended = False

    async def _flush() -> None:
        nonlocal buffer, batch_started_at
        if not buffer:
            return
        counts = await land_fn(buffer)
        totals["upsert"] += counts["upsert"]
        totals["delete"] += counts["delete"]
        # REQ-1734: ack ONLY after the batch is durably landed, and only the events in THIS batch —
        # never "wherever the provider's read position currently is", which could be past other
        # events the pump has already queued but this flush didn't include.
        await provider.ack(buffer)
        buffer = []
        batch_started_at = None

    try:
        while not disconnect.is_set() and not stream_ended:
            if buffer:
                elapsed = loop.time() - (batch_started_at or loop.time())
                timeout = min(debounce_quiet, max(0.0, debounce_max_delay - elapsed))
            else:
                timeout = None  # block for the first event of the next batch
            try:
                item = await asyncio.wait_for(queue.get(), timeout=timeout)
            except TimeoutError:
                await _flush()
                continue
            if item is _SENTINEL:
                stream_ended = True
                break
            buffer.append(item)
            if batch_started_at is None:
                batch_started_at = loop.time()
            if debounce_quiet <= 0:
                await _flush()
        await _flush()  # a partial batch at disconnect/stream-end still lands
        log.info(
            "CDC landing %s.%s: applied %d upserts, %d deletes",
            schema,
            table,
            totals["upsert"],
            totals["delete"],
        )
    finally:
        pump_task.cancel()
        try:
            await pump_task
        except (asyncio.CancelledError, Exception):
            pass
        await provider.close()
    return totals
