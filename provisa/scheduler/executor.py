# Copyright (c) 2026 Kenneth Stott
# Canary: 5b9e3d17-2c4a-4f6e-8a1d-9e7c0b3f2a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""APScheduler executor that runs every job on a background worker thread (REQ-1882).

The stock ``AsyncIOExecutor`` runs coroutine jobs as tasks on the scheduler's event loop — the
process (front) loop. Scheduled jobs (MV refresh ticks, poll jobs, reapers, OTEL compaction, egress
drains) read and write the control plane and the engine, which block their calling thread, so on
that loop every job would stall every request's I/O. This executor hands each job to the
background worker pool, where it runs on the worker's own connection loop. The scheduler's own
wakeup timer stays on the process loop: it only computes due times and submits, never blocks.
"""

# Requirements: REQ-1882, REQ-1900

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from apscheduler.executors.base import BaseExecutor, run_coroutine_job, run_job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.util import iscoroutinefunction_partial

from provisa.core.connection_loop import spawn_background


if TYPE_CHECKING:
    from provisa.scheduler.holder import SchedulerHolder

# Jobs that are this PROCESS's own work and so run in every worker (REQ-1900). Every other job is
# the deployment's and runs only in the worker that holds the scheduler lock.
#   egress_drain — drains THIS process's in-memory egress counters into the meter.
#   engine_watch — replaces THIS process's engine connection when it is dead.
#   replica:builds — THIS process's replica build pass (REQ-1915): every node that does
#                    background work builds, so capacity grows with nodes. One job per org
#                    (``replica:builds:org_<id>``); the per-node, per-engine and per-replica locks
#                    are what keep builds single, not the scheduler's holder.
PER_WORKER_JOB_IDS = frozenset({"egress_drain", "engine_watch", "replica:builds"})


def runs_in_every_worker(job_id: str) -> bool:
    """Whether the job ``job_id`` runs in every worker, not only the scheduler's holder. An
    org's copy of a job carries an ``:org_<id>`` suffix."""
    return job_id.partition(":org_")[0] in PER_WORKER_JOB_IDS


class BackgroundJobExecutor(BaseExecutor):
    """Run each APScheduler job on a background worker's connection loop.

    With a ``holder`` (the server's own scheduler), the deployment's jobs run only in the worker
    that holds the scheduler lock — see ``provisa.scheduler.holder``. Without one, every job runs:
    a scheduler whose jobs all belong to this process (the live-query engine's polls feed
    subscribers connected to THIS process)."""

    def __init__(self, holder: "SchedulerHolder | None" = None) -> None:
        super().__init__()
        self._holder = holder

    def _do_submit_job(self, job: Any, run_times: list[Any]) -> None:
        logger_name = self._logger.name

        async def _run() -> None:
            # Asked here, on the background worker: the question is a control-plane statement,
            # and the process loop that submitted this job must not wait on one.
            holder = self._holder
            if holder is not None and not runs_in_every_worker(job.id) and not holder.holds():
                # Another worker holds the lock and runs this firing. Reported as a run with no
                # events so the scheduler releases the job's instance slot.
                self._run_job_success(job.id, [])
                return
            try:
                if iscoroutinefunction_partial(job.func):
                    events = await run_coroutine_job(
                        job, job._jobstore_alias, run_times, logger_name
                    )
                else:
                    events = run_job(job, job._jobstore_alias, run_times, logger_name)
            except BaseException:
                self._run_job_error(job.id, *sys.exc_info()[1:])
                return
            self._run_job_success(job.id, events)

        # Submitted from the scheduler's detached wakeup chain (new_scheduler), so the job's copied
        # context carries no request trace or org binding — each job binds its own.
        spawn_background(_run(), name=f"scheduled:{job.id}")


def background_scheduler(holder: "SchedulerHolder | None" = None) -> AsyncIOScheduler:
    """An ``AsyncIOScheduler`` whose jobs run on background worker threads."""
    return AsyncIOScheduler(executors={"default": BackgroundJobExecutor(holder)})
