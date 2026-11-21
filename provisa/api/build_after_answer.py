# Copyright (c) 2026 Kenneth Stott
# Canary: 7e0c1a54-93bd-4f2e-8a6b-5d21c9f0e3a7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The after-answer pass of a runtime's schema builds (REQ-1914, REQ-2129).

A build answers its caller once the compiled model is published. What it derives that no request
is governed by -- the lineage edges and checker targets, the stored names table, the offers'
contracts and the catalog notice, the lifecycle wiring, the role surfaces' warm-up -- is the
build's :class:`Pass`, run after the answer:

* one runtime runs one pass at a time, on a background worker (``spawn_background``);
* a build that publishes supersedes the pass of every build before it: one that has not begun
  never runs, one that is running stops before its next phase, and the newest build's pass runs
  every phase. Two quick builds give one pass, for the newer build's stamps;
* a phase that raises ends the pass: it is recorded on the pass with the phase and logged with
  its traceback. The write it followed was already answered and stands. Nothing is put in its
  place: the next build's pass runs every phase again;
* at lifespan startup (``run_lifecycle_work``'s rule: off a connection loop) the build runs its
  pass itself, up to the warm-up, so boot stays ordered and a failure there fails the boot build.

What a request reads between a build's answer and its pass is stated where the phases are
defined (``app._after_answer_steps``).
"""

# Requirements: REQ-1914, REQ-2129

from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from provisa.api import rebuild_trace
from provisa.core.connection_loop import spawn_background
from provisa.core.runtime_gone import RuntimeNotBuilt, left_to_the_next_runtime

log = logging.getLogger(__name__)

#: A pass that took this long is logged at WARNING, as a slow build is.
SLOW_PASS_S = 2.0


@dataclass
class Pass:
    """One build's after-answer work: a step per phase of ``rebuild_trace.AFTER_PHASES``."""

    record: rebuild_trace.AfterAnswer
    steps: dict[str, Callable[[], Awaitable[None]]]
    answered: float = field(default_factory=time.perf_counter)
    _began: float | None = None
    _running: bool = False
    _done: int = 0  # the phases it has run

    def __post_init__(self) -> None:
        if tuple(self.steps) != rebuild_trace.AFTER_PHASES:
            raise ValueError(
                f"a pass runs the after-answer phases in order {rebuild_trace.AFTER_PHASES}, "
                f"not {tuple(self.steps)}"
            )

    @property
    def key(self) -> str:
        """The runtime it is the pass of, as the runtime is registered (``runtime_key``)."""
        return self.record.runtime

    @property
    def ended(self) -> bool:
        return self.record.outcome != rebuild_trace.PENDING


_guard = threading.Lock()
_latest: dict[str, Pass] = {}  # the pass of the newest build published, per runtime
_draining: dict[str, concurrent.futures.Future[None]] = {}  # the worker running a runtime's passes


def publish(new: Pass) -> None:
    """``new``'s build has published its model: it is the runtime's pass, and the pass of every
    build before it is superseded. One that is not running ends here; one that is running finds
    out before its next phase (:func:`run`)."""
    with _guard:
        old = _latest.get(new.key)
        _latest[new.key] = new
        if old is not None and not old.ended and not old._running:
            _end(old, rebuild_trace.SUPERSEDED)


def superseded(held: Pass) -> bool:
    with _guard:
        return _latest.get(held.key) is not held


def _end(held: Pass, outcome: str) -> None:
    record = held.record
    now = time.perf_counter()
    began = held._began if held._began is not None else now
    record.waited_s = began - held.answered
    record.took_s = now - began
    if outcome == rebuild_trace.SUPERSEDED:
        record.stopped_before = rebuild_trace.AFTER_PHASES[held._done]
    record.outcome = outcome
    rebuild_trace.pass_ended(record, slow_s=SLOW_PASS_S)


async def run(held: Pass, *, before: str | None = None) -> None:
    """Run ``held``'s phases in order from where it stands, to its end or to the phase ``before``.
    It stops before a phase once a newer build has superseded it. A phase that raises ends it,
    recorded with the phase, and raises here."""
    with _guard:
        if held.ended:
            return
        held._running = True
        if held._began is None:
            held._began = time.perf_counter()
    try:
        for name in rebuild_trace.AFTER_PHASES[held._done :]:
            if name == before:
                return
            if superseded(held):
                return
            began = time.perf_counter()
            try:
                await held.steps[name]()
            except RuntimeNotBuilt as gone:
                # The runtime this pass is for was dropped while it ran: the one built in its
                # place runs its own pass.
                held.record.phases.append((name, time.perf_counter() - began))
                left_to_the_next_runtime(gone, f"the after-answer pass ({name})")
                _end(held, rebuild_trace.SUPERSEDED)
                return
            except BaseException as failure:
                held.record.phases.append((name, time.perf_counter() - began))
                held.record.failed_in = name
                held.record.error = repr(failure)
                _end(held, rebuild_trace.FAILED)
                raise
            held.record.phases.append((name, time.perf_counter() - began))
            held._done += 1
        _end(held, rebuild_trace.RAN)
    finally:
        with _guard:
            held._running = False
            if not held.ended and _latest.get(held.key) is not held:
                _end(held, rebuild_trace.SUPERSEDED)


def start(key: str) -> None:
    """Have the runtime's passes run on a background worker, unless one already is."""
    with _guard:
        if key in _draining:
            return
        held = _latest.get(key)
        if held is None or held.ended:
            return
        _draining[key] = spawn_background(_drain(key), name=f"after-answer pass {key}")


async def _drain(key: str) -> None:
    """Run the runtime's newest pass, and then any published meanwhile, until none is left."""
    try:
        while True:
            with _guard:
                held = _latest.get(key)
                if held is None or held.ended:
                    del _draining[key]
                    return
            try:
                await run(held)
            except Exception:
                # Recorded on the pass (``failed_in``, ``error``). The build it follows was
                # answered and stands; the next build's pass runs every phase again.
                log.exception(
                    "after-answer pass of %s failed in %s; %s",
                    key,
                    held.record.failed_in,
                    held.record.line(),
                )
    except BaseException:
        # Cancelled (shutdown): the worker is gone, so a later build starts another.
        with _guard:
            _draining.pop(key, None)
        raise


def draining(key: str) -> concurrent.futures.Future[None] | None:
    """The worker running the runtime's passes now, done once none is left; None when idle."""
    with _guard:
        return _draining.get(key)
