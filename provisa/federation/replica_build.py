# Copyright (c) 2026 Kenneth Stott
# Canary: 3b6e0a94-c2d7-4f51-8a39-e7d1f5c04b26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One build of a replica at a time, outliving the read that asked for it (REQ-826, REQ-1661).

The first read of a replicated table builds its replica, and a large table takes longer to copy
than one request may wait. So the build is not the request's own work: it is a job this process
runs once per replica, however many requests ask for it, and that keeps running when a request
gives up. A request waits for the job up to its own deadline and then fails with
:class:`ReplicaBuilding`, which names the table; the next request joins the same job rather than
starting the copy again.

Across worker processes the job takes the store's own lock for that replica before it copies
(``PgFederationRuntime.replica_lock``), so the workers build one replica between them."""

# Requirements: REQ-826, REQ-1661

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


class ReplicaBuilding(TimeoutError):
    """A read's deadline passed while the replica it needs was still being built."""

    def __init__(self, replica: str, running_for: float) -> None:
        self.replica = replica
        self.running_for = running_for
        super().__init__(
            f"the replica of {replica} is still being built (running for {running_for:.0f}s); "
            "the build continues, and a read succeeds once it has finished"
        )


@dataclass
class _Build:
    replica: str
    started: float = field(default_factory=time.monotonic)
    done: concurrent.futures.Future[Any] = field(default_factory=concurrent.futures.Future)


_ANSWER_RESERVE_SECONDS = 0.5

_builds: dict[str, _Build] = {}
_guard = threading.Lock()


def _start(replica: str, build: Callable[[], Coroutine[Any, Any, Any]]) -> _Build:
    """This process's running build of ``replica``, started now when there is none."""
    from provisa.core.connection_loop import spawn_background

    with _guard:
        running = _builds.get(replica)
        if running is not None and not running.done.done():
            return running
        job = _Build(replica)
        _builds[replica] = job

    async def _run() -> None:
        from provisa.core import request_deadline

        # Started in a copy of the asking request's context: the build is not that request's
        # work, and that request's deadline must not cancel it.
        request_deadline.unbind()
        try:
            result = await build()
        except BaseException as exc:  # allow-ble: the job's failure is handed to every request waiting on it, whatever its type
            log.error("replica build of %s failed: %s", replica, exc, exc_info=exc)
            job.done.set_exception(exc)
            return
        log.info("replica build of %s finished in %.1fs", replica, time.monotonic() - job.started)
        job.done.set_result(result)

    spawn_background(_run(), name=f"replica-build:{replica}")
    return job


async def build_once(
    replica: str, build: Callable[[], Coroutine[Any, Any, Any]], *, budget: float | None
) -> Any:
    """The result of this process's one build of ``replica``, waited for no longer than ``budget``
    seconds (None: no bound). A build that fails raises its error to every request waiting on it;
    one still running at the budget raises :class:`ReplicaBuilding` and keeps running."""
    job = _start(replica, build)
    if budget is not None:
        # The request answers with ReplicaBuilding before its own deadline cuts it off with a
        # timeout that names nothing: the wait ends this much ahead of it.
        budget = max(0.0, budget - _ANSWER_RESERVE_SECONDS)
    try:
        # Shielded: the wait is the request's, the build is not — a timeout must not cancel it.
        return await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(job.done)), budget)
    except TimeoutError as exc:
        raise ReplicaBuilding(replica, time.monotonic() - job.started) from exc
