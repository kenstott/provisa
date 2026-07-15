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
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from provisa.events import injector, queue
from provisa.events.calendars import Window
from provisa.events.deadlines import DeadlineSource, LiveDebounce
from provisa.events.freshness_contract import evaluate_contract
from provisa.freshness.subject import FreshnessSubject


def _now() -> datetime:
    return datetime.now(timezone.utc)


class OwnershipLost(Exception):
    """REQ-959: raised inside the commit transaction when the ownership CAS fails — a peer reclaimed
    this node's work (deadline/heartbeat) mid-flight. Aborts the commit so no ripple double-commits;
    the loop treats it as a no-op (the peer now owns the work)."""


class PreprocessError(Exception):
    """REQ-957: a user ``preprocess`` hook raised — a FATAL data outcome, not an infra crash. The loop
    catches it, emits an ``error`` event about the node, short-circuits the land, and fans the error to
    dependents (poison propagation). Distinct from any other exception, which is a genuine crash that
    MUST propagate for at-least-once resume (REQ-960)."""


@dataclass
class NodeContext:
    """REQ-957: the read-only processing envelope handed to a node's ``preprocess(rows, ctx)`` hook.

    Carries what the hook may inspect — node identity/kind, the claimed-event summary, the landing
    columns/schema, the forced-refresh flag, and the last-landed content hash — plus the ``warn``
    channel. ``window``/``window_id``/``frontier`` are the REQ-958 windowed-processing peg: the
    half-open ``[start, end)`` and its calendar-addressable id, set at fire from the node's deadline
    source (None for a live/real-time node with no calendar peg). ``produce`` reads as-of
    ``window[1]`` (window.end) to stay deterministic/replayable, and the result lands keyed on
    ``window_id``. ``warn`` is the ONLY mutation: it records advisory reasons the loop emits as a
    non-fatal ``warn`` event; every data field is informational."""

    node: str
    kind: str
    claimed: list[dict]
    prior_hash: str | None
    forced: bool = False
    columns: list[tuple[str, str]] | None = None
    window: tuple[datetime, datetime] | None = None
    window_id: str | None = None
    frontier: dict[str, Any] | None = None
    warns: list[str] = field(default_factory=list)

    def warn(self, reasons: str | Iterable[str]) -> None:
        """REQ-957: record advisory reason(s). Non-fatal — the loop emits a single ``warn`` event and
        still lands the rows. Accepts a string or an iterable of strings."""
        if isinstance(reasons, str):
            self.warns.append(reasons)
        else:
            self.warns.extend(reasons)


class TableProcessor(ABC):
    """Base: owns a node, its lifecycle, and the common claim→handle→complete→re-post loop.

    ``dependents_of(node) -> list[str]`` is the SQLGlot-derived fan-out target set (lineage). ``name``
    is the lease owner. The queue runs on the control-plane ``Database`` (``db``)."""

    kind = "node"  # REQ-957: node kind surfaced on NodeContext (overridden: source / mv)

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
        debounce_quiet: float = 0.0,
        debounce_max_delay: float | None = None,
        deadline_source: DeadlineSource | None = None,
        expected_events: list[str] | None = None,
        freshness_of: Callable[[str], FreshnessSubject] | None = None,
        preprocess: Callable[..., Any] | None = None,
        emit_outcomes: frozenset[str] | None = None,
        subscribers_of: Callable[[str, str], list[str]] | None = None,
    ) -> None:
        self.node = node
        # REQ-965: the declared downstream EMIT-outcome set (a subset of {replace, append, delta}),
        # independent of the persistence outcome. None → the default single-shape path (post one event,
        # fan to every dependent). When set, the loop resolves the demand-driven emit (declared ∩
        # subscribed) and routes each shape to its shape-matched dependents via ``subscribers_of``.
        if emit_outcomes is not None and subscribers_of is None:
            raise ValueError(
                f"{node}: a declared emit-outcome set requires a subscribers_of(node, shape) router"
            )
        self._emit_outcomes = emit_outcomes
        self._subscribers_of = subscribers_of
        # REQ-957: the one optional user hook preprocess(rows, ctx) -> rows, run after produce and
        # before land. None = identity. Honored by the variant handle, which threads it into the
        # produce→land closure so it runs between the two.
        self._preprocess = preprocess
        self.change_signal = change_signal
        self.watermark_column = watermark_column
        self.probe_type = (
            probe_type  # REQ-982: input-probe method → drives the injected event shape
        )
        self._dependents_of = dependents_of
        self._db = db
        self.name = name
        # The claim's deadline SOURCE (REQ-961/962/963): periodic (calendar boundary + lateness) or
        # live (trailing-edge debounce with mandatory cap) — both feed the SAME REQ-959 claim
        # primitive; only the deadline derivation differs. An explicit ``deadline_source`` wins;
        # otherwise a ``debounce_quiet > 0`` builds the live source (mandatory max_delay cap). None =
        # real-time / event-driven fire.
        if deadline_source is not None:
            self._deadline: DeadlineSource | None = deadline_source
        elif debounce_quiet > 0:
            if debounce_max_delay is None:
                raise ValueError(
                    f"{node}: REQ-963 live debounce (quiet>0) requires a max_delay staleness cap"
                )
            self._deadline = LiveDebounce(quiet=debounce_quiet, max_delay=debounce_max_delay)
        else:
            self._deadline = None
        # REQ-961 freshness contract: the declared expected-events input list (verified fresh-through
        # window.end at fire) + the per-input freshness-state reader. None = no contract (live/event
        # nodes verify nothing). Requires a window (periodic) to be meaningful.
        self._expected_events = expected_events
        self._freshness_of = freshness_of

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
        # REQ-959 reassert-on-restart: resume any claim this processor still owns from a prior
        # (crashed) run before taking new work, and refresh its lease so the reaper does not race us.
        # Resumed work is already owned → it is processed regardless of the debounce gate below.
        resumed = await queue.resume_claims(
            conn, dependent_table=self.node, processor_name=self.name
        )
        if resumed:
            await queue.heartbeat(
                conn, dependent_table=self.node, processor_name=self.name, now=now
            )
        else:
            # REQ-962 existence gate: a business-day grain on a holiday has NO window → the periodic
            # MV deterministically does not fire and raises no alarm. Applies only to NEW work; a
            # resumed in-flight claim already opened its window.
            if self._deadline is not None and self._deadline.gated(now):
                return None
            # REQ-958/963 window/debounce gate: peek the unclaimed fan-ins WITHOUT claiming; if the
            # deadline (calendar boundary+lateness, or debounce quiet/max_delay) has not passed, defer
            # (leave them unclaimed so more fan-ins coalesce into ONE recompute — fan-in collapse).
            peeked = await queue.peek_pending(conn, dependent_table=self.node)
            if not peeked:
                return None
            if not self._ready(peeked, now):
                return None
        newly = await queue.claim(
            conn,
            dependent_table=self.node,
            processor_name=self.name,
            now=now,
            deadline=self._claim_deadline(now),
        )
        claimed = sorted(set(resumed) | set(newly))
        if not claimed:
            return None
        pending = await queue.get_events(conn, claimed)
        # REQ-957 pre-produce poison short-circuit (a built-in, NOT the hook): a claimed upstream
        # ``error`` event skips produce entirely — this node emits its own error and fans it forward
        # (poison propagation) without landing. Distinct from a preprocess-raised error.
        if any(e["event_type"] == "error" for e in pending):
            upstream = sorted({e["source_table"] for e in pending if e["event_type"] == "error"})
            return await self._emit_error(
                conn, claimed, now, {"poison": True, "upstream": upstream}
            )
        prior = await queue.get_node_state(conn, self.node)
        prior_hash = prior["content_hash"] if prior else None
        # REQ-958: open the processing window this fire is pegged to (calendar-addressable for a
        # periodic node; None for a live node with no peg). ``produce`` reads as-of window.end and the
        # result lands keyed on window_id.
        window = self._deadline.window(now) if self._deadline is not None else None
        ctx = NodeContext(
            node=self.node,
            kind=self.kind,
            claimed=[
                {"id": e["id"], "event_type": e["event_type"], "source_table": e["source_table"]}
                for e in pending
            ],
            prior_hash=prior_hash,
            window=(window.start, window.end) if window is not None else None,
            window_id=window.window_id if window is not None else None,
        )
        # REQ-961: the freshness CONTRACT — a PULL against per-input freshness at fire time (NOT
        # event receipt). A listed input not fresh-through window.end is an outage → warn/hold, no
        # seal, no ripple (never a silent skip). Only meaningful with a window (periodic).
        if window is not None and self._expected_events is not None:
            if await self._contract_outage(conn, window, claimed, now, ctx):
                return None
        # LAND runs inside ``handle`` against the STORE database — outside the control-plane
        # transaction below (different DB, no shared txn) and idempotent on the node key, so a
        # re-run after a crash re-lands harmlessly. (event_type, payload, content_hash|None) | None.
        # REQ-957: preprocess runs inside handle (after produce, before land); a raise surfaces as
        # PreprocessError → fatal error event; ctx.warn accumulates advisory reasons on ctx.
        try:
            result = await self.handle(pending, prior_hash=prior_hash, ctx=ctx)
        except PreprocessError as exc:
            return await self._emit_error(conn, claimed, now, {"error": str(exc)})
        # REQ-960: post + fan_out + complete run AFTER land in ONE control-plane transaction,
        # post-BEFORE-complete. A crash between land and this commit re-claims and re-runs (the
        # land is idempotent), so the downstream ripple is never lost and the claim never orphaned.
        # REQ-959: each complete is an ownership CAS (processor_name = self); a lost CAS raises
        # OwnershipLost → the whole commit rolls back (a peer took over → no double effect).
        try:
            async with conn.transaction():
                if result is None:
                    # Gate hit / preprocess []-no-op (content unchanged / nothing landed): no ripple,
                    # but advisory warns still emit and the claimed events are completed.
                    await self._emit_warns(conn, ctx, now)
                    await self._complete_all(conn, claimed, now)
                    return None
                event_type, payload, new_hash = result
                if new_hash is not None:
                    await queue.set_node_state(conn, self.node, content_hash=new_hash)
                my_event = await self._post_and_route(conn, event_type, payload)
                await self._emit_warns(conn, ctx, now)
                await self._complete_all(conn, claimed, now)
        except OwnershipLost:
            return None
        return my_event

    async def _post_and_route(self, conn: Any, event_type: str, payload: dict) -> int | None:
        """Post this node's change and fan it to the right dependents. Two modes:

        - default single-shape (``emit_outcomes`` None): post ONE ``event_type`` event, fan to EVERY
          dependent (``dependents_of``) — the pre-REQ-965 behavior, unchanged.
        - REQ-965 demand-driven emit set: resolve the shapes actually produced this fire = the
          declared set ∩ the shapes some dependent SUBSCRIBES to (pay-per-consumer), then post each
          produced shape and route it ONLY to its shape-matched dependents. Emit-NONE (no produced
          shape) posts nothing — the MV persisted but tells no one. Returns the last posted event id
          (drives drain propagation), or None when nothing was emitted."""
        if self._emit_outcomes is None:
            eid = await queue.post_event(
                conn, source_table=self.node, event_type=event_type, payload=payload
            )
            await queue.fan_out(conn, eid, self._dependents_of(self.node))
            return eid
        from provisa.events import outcomes

        assert self._subscribers_of is not None  # guaranteed by __init__ when emit_outcomes is set
        subscribed = {
            shape for shape in outcomes.EMIT_OUTCOMES if self._subscribers_of(self.node, shape)
        }
        last: int | None = None
        for shape in outcomes.resolve_emitted(self._emit_outcomes, subscribed):
            eid = await queue.post_event(
                conn, source_table=self.node, event_type=shape, payload=payload
            )
            await queue.fan_out(conn, eid, self._subscribers_of(self.node, shape))
            last = eid
        return last

    async def _emit_error(
        self, conn: Any, claimed: list[int], now: datetime, payload: dict
    ) -> int | None:
        """REQ-957: emit this node's ``error`` event and fan it to dependents (poison propagation),
        then complete the claimed set — all in one REQ-960 CAS-guarded transaction (post-before-
        complete, no land). Returns the error event id (so a drain keeps propagating), or None when a
        peer had reclaimed the work (OwnershipLost)."""
        try:
            async with conn.transaction():
                event_id = await queue.post_event(
                    conn, source_table=self.node, event_type="error", payload=payload
                )
                await queue.fan_out(conn, event_id, self._dependents_of(self.node))
                await self._complete_all(conn, claimed, now)
        except OwnershipLost:
            return None
        return event_id

    async def _emit_warns(self, conn: Any, ctx: NodeContext, now: datetime) -> None:
        """REQ-957: if the hook called ctx.warn, post ONE advisory ``warn`` event about the node
        (recorded in the event log for a quality MV to read; not fanned — a warn does not poison
        dependents). Runs inside the same commit transaction as the land's ripple. ``now`` is unused
        here but kept for a uniform commit-helper signature."""
        del now
        if ctx.warns:
            await queue.post_event(
                conn, source_table=self.node, event_type="warn", payload={"reasons": ctx.warns}
            )

    def _ready(self, peeked: list[dict], now: datetime) -> bool:
        """True when the deadline SOURCE says fire now (deadline passed, or no source → real-time);
        False → defer so more fan-ins coalesce into the SAME recompute (REQ-958 fan-in collapse /
        REQ-961 calendar boundary / REQ-963 debounce)."""
        if self._deadline is None:
            return True
        deadline = self._deadline.deadline(now, peeked)
        return deadline is None or now >= deadline

    def _claim_deadline(self, now: datetime) -> datetime | None:
        """The per-claim fire-by stamped on the claim for the REQ-959 reaper — derived from the
        deadline source (calendar boundary+lateness, or now+max_delay); None with no source."""
        return None if self._deadline is None else self._deadline.claim_deadline(now)

    async def _contract_outage(
        self, conn: Any, window: Window, claimed: list[int], now: datetime, ctx: NodeContext
    ) -> bool:
        """REQ-961: verify the expected-events freshness contract by a PULL against per-input
        freshness at fire time. Every listed input fresh-through window.end → trusted (return False,
        proceed to seal). Any not fresh-through → an OUTAGE: emit a warn (expected-but-absent) and
        HOLD — complete the claim with NO land and NO ripple, never a silent skip (return True)."""
        assert self._expected_events is not None  # guarded by the caller
        if self._freshness_of is None:
            raise ValueError(
                f"{self.node}: an expected-events freshness contract requires a freshness_of reader"
            )
        result = evaluate_contract(
            self._expected_events, self._freshness_of, window.end.timestamp()
        )
        if result.trusted:
            return False
        ctx.warn(
            f"REQ-961 outage: inputs not fresh-through {window.window_id}: "
            f"{', '.join(result.outages)}"
        )
        try:
            async with conn.transaction():
                await self._emit_warns(conn, ctx, now)
                await self._complete_all(conn, claimed, now)
        except OwnershipLost:
            return True
        return True

    async def _complete_all(self, conn: Any, claimed: list[int], now: datetime) -> None:
        """Complete every claimed item under the REQ-959 ownership CAS. A failed CAS on ANY item means
        a peer reclaimed this work → raise OwnershipLost to abort the commit (no partial completion,
        no ripple)."""
        for eid in claimed:
            ok = await queue.complete(
                conn,
                event_id=eid,
                dependent_table=self.node,
                processor_name=self.name,
                now=now,
            )
            if not ok:
                raise OwnershipLost(self.node)

    @abstractmethod
    async def handle(
        self, pending: list[dict], *, prior_hash: str | None, ctx: NodeContext | None = None
    ) -> tuple[str, dict, str | None] | None:
        """Do the node's work from its claimed events (land / generate). Return ``(event_type, payload,
        content_hash)`` when the node's table changed → re-post; None when unchanged. ``content_hash``
        is the digest of the landed replace-shaped content (None for append/CDC deltas, which are new
        by definition). ``prior_hash`` is the last land's digest — a replace matching it returns None
        (the REQ-981 output gate). ``ctx`` is the REQ-957 envelope; when a preprocess hook is set the
        variant threads it into the produce→land closure (raising surfaces as PreprocessError)."""


class SourceTableProcessor(TableProcessor):
    """Lands a data source's rows into the materialization store (the write face)."""

    kind = "source"

    def __init__(self, *args: Any, land: Callable[..., Any] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # None is valid for a push-only (kafka/debezium) source that drains via consume_kafka and
        # never reaches handle; a poll/land source supplies the write-face callable.
        self._land = land  # async (pending, *, prior_hash) -> (event_type, payload, hash) | None

    async def handle(
        self, pending: list[dict], *, prior_hash: str | None, ctx: NodeContext | None = None
    ) -> tuple[str, dict, str | None] | None:
        """Coalesce the claimed events, land the source's rows via the write face, and report the
        landing shape as this node's change event (append/delta/replace) with the content hash — or
        None if nothing landed or (replace) the content matched ``prior_hash`` (REQ-981 gate). When a
        preprocess hook is set it is threaded into the land closure (REQ-957: after fetch, before
        land)."""
        assert self._land is not None, "SourceTableProcessor.handle requires a land callable"
        if self._preprocess is None:
            return await self._land(pending, prior_hash=prior_hash)
        return await self._land(
            pending, prior_hash=prior_hash, ctx=ctx, preprocess=self._preprocess
        )


class MVTableProcessor(TableProcessor):
    """Generates an MV by running its SQL on the engine and landing the result."""

    kind = "mv"

    def __init__(self, *args: Any, generate: Callable[..., Any], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._generate = (
            generate  # async (pending, *, prior_hash) -> (event_type, payload, hash) | None
        )

    async def handle(
        self, pending: list[dict], *, prior_hash: str | None, ctx: NodeContext | None = None
    ) -> tuple[str, dict, str | None] | None:
        """Generate the MV (engine runs its SQL) and land the result; report the resulting change as
        this node's event (replace) with the content hash — or None if the recomputed output matched
        ``prior_hash`` (REQ-981 gate: an unchanged MV does not ripple its dependents). When a
        preprocess hook is set it is threaded into the generate closure (REQ-957: after the MV SQL,
        before land)."""
        if self._preprocess is None:
            return await self._generate(pending, prior_hash=prior_hash)
        return await self._generate(
            pending, prior_hash=prior_hash, ctx=ctx, preprocess=self._preprocess
        )
