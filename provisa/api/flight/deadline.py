# Copyright (c) 2026 Kenneth Stott
# Canary: c8a15e72-4f09-4b3d-9e68-0d7b2f5a1c94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One request deadline for a whole Arrow Flight query ticket (REQ-1905).

A request on every other transport has one deadline (``provisa.core.request_deadline``): whatever
it waits for and whatever it runs draw on the same budget, and a statement still running when it
expires is cancelled. A Flight ticket had none — its wait for a stream slot was bounded, and then
its execution and its stream ran for as long as they took.

``request_budget`` binds that deadline around ``do_get``: the wait for a slot, governance and
execution all read it. A Flight result is often a LAZY stream that pyarrow pulls after ``do_get``
has returned, so the deadline cannot end with ``do_get``; ``stream_within_deadline`` carries it
through the pull — rebinding it around each batch (so the engine statement feeding the stream is
still cancellable) and ending the stream with an error naming the deadline once it has passed.

The budget is Flight's own request timeout (``limits.request_timeouts.flight``, shipped at 3600 s
because Flight carries large data transfers; ``provisa.core.limits.request_timeout_for``); a
tighter deadline the caller has already bound is kept.
"""

# Requirements: REQ-1905

from __future__ import annotations

import contextvars
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from typing import Any

import pyarrow.flight as flight

from provisa.core import request_deadline


class FlightDeadlineExceeded(RuntimeError):
    """The Flight request's deadline passed."""

    def __init__(self, timeout: float) -> None:
        from provisa.core.limits import request_timeout_setting

        super().__init__(
            f"flight request exceeded its {timeout:g}s request deadline "
            f"({request_timeout_setting('flight')})"
        )


class _Budget:
    """The deadline of the Flight request being served on this thread."""

    def __init__(self, deadline: request_deadline.Deadline, owned: bool) -> None:
        self.deadline = deadline
        # Whether this request created the deadline (and so stops it) or the caller did.
        self.owned = owned
        # Set once a lazy stream has taken the deadline over: it then outlives ``do_get``.
        self.streaming = False

    def exceeded(self) -> FlightDeadlineExceeded:
        return FlightDeadlineExceeded(self.deadline.timeout)


_budget: contextvars.ContextVar[_Budget | None] = contextvars.ContextVar(
    "provisa_flight_budget", default=None
)


@contextmanager
def request_budget(timeout: float) -> Generator[None]:
    """Bind one deadline of ``timeout`` seconds for the enclosed Flight request."""
    outer = request_deadline.current()
    if outer is not None and outer.remaining() <= timeout:
        budget = _Budget(outer, owned=False)
    else:
        from provisa.core.limits import request_timeout_setting

        # The same kind of deadline every transport's request has: its expiry names Flight and
        # the setting. Flight words its own error (FlightDeadlineExceeded) from it.
        budget = _Budget(
            request_deadline.Deadline(
                timeout, transport="flight", setting=request_timeout_setting("flight")
            ),
            owned=True,
        )
    token = _budget.set(budget)
    try:
        with request_deadline.bound(budget.deadline):
            yield
    except TimeoutError as exc:
        # What a statement cancelled by the deadline's watchdog raises. A timeout that is not the
        # deadline's (it has not fired) is somebody else's and is left as it is.
        if budget.deadline.fired:
            raise budget.exceeded() from exc
        raise
    finally:
        _budget.reset(token)
        if budget.owned and not budget.streaming:
            budget.deadline.stop()


def stream_within_deadline(batches: Iterable[Any]) -> Iterable[Any]:
    """``batches``, pulled under the current Flight request's deadline.

    Called while ``request_budget`` is bound; the returned stream keeps the deadline after
    ``do_get`` returns. Outside a Flight request there is no deadline and ``batches`` is returned
    as it is."""
    budget = _budget.get()
    if budget is None:
        return batches
    budget.streaming = True
    return _pull(budget, iter(batches))


def _pull(budget: _Budget, batches: Iterator[Any]) -> Iterator[Any]:
    deadline = budget.deadline
    try:
        while True:
            if deadline.fired:
                raise flight.FlightServerError(str(budget.exceeded()))
            # Bound only for the pull: pyarrow drives this generator from the RPC's thread, and
            # a deadline left bound there would be inherited by that thread's next RPC.
            with request_deadline.bound(deadline):
                try:
                    batch = next(batches)
                except StopIteration:
                    return
                except TimeoutError as exc:
                    if deadline.fired:
                        raise flight.FlightServerError(str(budget.exceeded())) from exc
                    raise
            yield batch
    finally:
        close = getattr(batches, "close", None)
        if close is not None:
            close()
        if budget.owned:
            deadline.stop()
