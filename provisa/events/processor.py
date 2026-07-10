# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Table processors (REQ-941) — the workers that own a node and process its events.

A ``TableProcessor`` owns one node (a data source table or an MV) and has a LIFECYCLE: ``start``
sets up how the node's changes arrive (a push listener for native/debezium/kafka, and/or a scheduled
poll job on the embedded scheduler), then it runs the common claim loop; ``stop`` tears it down. The
common loop — claim the node's pending work (exactly-once, lease), coalesce, ``handle`` it, complete
the set, and re-post the node's OWN change event to its dependents (the self-feeding DAG) — lives in
the base. The variants supply only ``handle``:

- ``SourceTableProcessor`` — land the source's rows into the store (write face).
- ``MVTableProcessor``     — generate the MV (SQL on the engine) and land the result.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from provisa.events import injector, queue


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TableProcessor(ABC):
    """Base: owns a node, its lifecycle, and the common claim→handle→complete→re-post loop.

    ``dependents_of(node) -> list[str]`` is the SQLGlot-derived fan-out target set (lineage). ``name``
    is the lease owner. The queue runs on the control-plane ``Database`` (``db``)."""

    def __init__(
        self,
        node: str,
        *,
        change_signal: str,
        watermark_column: str | None,
        dependents_of: Callable[[str], list[str]],
        db: Any,
        name: str,
        probe_type: str | None = None,
    ) -> None:
        self.node = node
        self.change_signal = change_signal
        self.watermark_column = watermark_column
        self.probe_type = (
            probe_type  # REQ-982: input-probe method → drives the injected event shape
        )
        self._dependents_of = dependents_of
        self._db = db
        self.name = name

    # -- lifecycle -------------------------------------------------------------
    async def stop(self) -> None:
        """Tear down the listener / schedule job. Overridden where a resource must be released."""

    # -- ingestion patterns (common; a node uses one by its change_signal) -----
    def register_poll_job(
        self, scheduler: Any, *, seconds: int, probe_factory: Callable[[], injector.Probe]
    ) -> None:
        """POLL pattern (ttl/probe/ttl_probe): register an interval job on the embedded scheduler
        (APScheduler) that runs the injector action at the node's cadence. ``probe_factory`` yields a
        fresh probe per fire. One job per node, replace-existing (idempotent re-register)."""
        from apscheduler.triggers.interval import IntervalTrigger

        async def _fire() -> None:
            await self.inject(probe_factory())

        scheduler.add_job(
            _fire,
            trigger=IntervalTrigger(seconds=seconds),
            id=f"poll:{self.node}",
            replace_existing=True,
        )

    async def consume_kafka(self, consumer: Any) -> None:
        """KAFKA-PUSH pattern (change_signal=kafka/debezium): drain the push stream — each message IS
        a change, so post this node's ``delta`` event (payload = the message's changed rows) and fan
        out, no probe. Runs until the consumer ends or ``stop`` closes it. ``consumer`` is any async
        iterator of message dicts."""
        async with self._db.acquire() as conn:
            async for message in consumer:
                event_id = await queue.post_event(
                    conn, source_table=self.node, event_type="delta", payload=message
                )
                await queue.fan_out(conn, event_id, self._dependents_of(self.node))

    # -- injector side (this node changed → post) ------------------------------
    async def inject(self, probe: injector.Probe) -> int | None:
        """Poll-job / listener entry: probe for change and, if changed, post this node's event and
        fan out (token-gated). Delegates to the injector action so poll and push share one path."""
        async with self._db.acquire() as conn:
            return await injector.check_node(
                conn,
                node=self.node,
                change_signal=self.change_signal,
                watermark_column=self.watermark_column,
                probe=probe,
                dependents=self._dependents_of(self.node),
                probe_type=self.probe_type,
            )

    # -- processor side (claim → handle → complete → re-post) ------------------
    async def process_pending(self, conn: Any) -> int | None:
        """Claim this node's pending work (exactly-once via the lease), coalesce, ``handle`` it,
        complete the drained set, and — if the node's table actually changed — re-post the node's OWN
        change event to its dependents (self-feeding DAG). Returns the re-posted event id, or None.

        The content-hash output gate (REQ-981): the prior land's hash is read and passed to ``handle``,
        which returns the new hash alongside its change; a replace whose hash matches prior returns None
        from ``handle`` (unchanged → no land, no ripple). A returned hash is persisted as the new
        baseline before re-posting."""
        now = _now()
        claimed = await queue.claim(
            conn, dependent_table=self.node, processor_name=self.name, now=now
        )
        if not claimed:
            return None
        pending = await queue.get_events(conn, claimed)
        prior = await queue.get_node_state(conn, self.node)
        prior_hash = prior["content_hash"] if prior else None
        # (event_type, payload, content_hash|None) when changed, else None.
        result = await self.handle(pending, prior_hash=prior_hash)
        for eid in claimed:
            await queue.complete(conn, event_id=eid, dependent_table=self.node, now=now)
        if result is None:
            return None  # gate: node output unchanged (content hash matched / nothing landed)
        event_type, payload, new_hash = result
        if new_hash is not None:
            await queue.set_node_state(conn, self.node, content_hash=new_hash)
        my_event = await queue.post_event(
            conn, source_table=self.node, event_type=event_type, payload=payload
        )
        await queue.fan_out(conn, my_event, self._dependents_of(self.node))
        return my_event

    @abstractmethod
    async def handle(
        self, pending: list[dict], *, prior_hash: str | None
    ) -> tuple[str, dict, str | None] | None:
        """Do the node's work from its claimed events (land / generate). Return ``(event_type, payload,
        content_hash)`` when the node's table changed → re-post; None when unchanged. ``content_hash``
        is the digest of the landed replace-shaped content (None for append/CDC deltas, which are new
        by definition). ``prior_hash`` is the last land's digest — a replace matching it returns None
        (the REQ-981 output gate)."""


class SourceTableProcessor(TableProcessor):
    """Lands a data source's rows into the materialization store (the write face)."""

    def __init__(self, *args: Any, land: Callable[..., Any], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._land = land  # async (pending, *, prior_hash) -> (event_type, payload, hash) | None

    async def handle(
        self, pending: list[dict], *, prior_hash: str | None
    ) -> tuple[str, dict, str | None] | None:
        """Coalesce the claimed events, land the source's rows via the write face, and report the
        landing shape as this node's change event (append/delta/replace) with the content hash — or
        None if nothing landed or (replace) the content matched ``prior_hash`` (REQ-981 gate)."""
        return await self._land(pending, prior_hash=prior_hash)


class MVTableProcessor(TableProcessor):
    """Generates an MV by running its SQL on the engine and landing the result."""

    def __init__(self, *args: Any, generate: Callable[..., Any], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._generate = (
            generate  # async (pending, *, prior_hash) -> (event_type, payload, hash) | None
        )

    async def handle(
        self, pending: list[dict], *, prior_hash: str | None
    ) -> tuple[str, dict, str | None] | None:
        """Generate the MV (engine runs its SQL) and land the result; report the resulting change as
        this node's event (replace) with the content hash — or None if the recomputed output matched
        ``prior_hash`` (REQ-981 gate: an unchanged MV does not ripple its dependents)."""
        return await self._generate(pending, prior_hash=prior_hash)
