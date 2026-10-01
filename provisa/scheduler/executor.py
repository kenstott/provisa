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

# Requirements: REQ-1882

from __future__ import annotations

import sys
from typing import Any

from apscheduler.executors.base import BaseExecutor, run_coroutine_job, run_job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.util import iscoroutinefunction_partial

from provisa.core.connection_loop import spawn_background


class BackgroundJobExecutor(BaseExecutor):
    """Run each APScheduler job on a background worker's connection loop."""

    def _do_submit_job(self, job: Any, run_times: list[Any]) -> None:
        logger_name = self._logger.name

        async def _run() -> None:
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


def background_scheduler() -> AsyncIOScheduler:
    """An ``AsyncIOScheduler`` whose jobs run on background worker threads."""
    return AsyncIOScheduler(executors={"default": BackgroundJobExecutor()})
