# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b1d63-9c27-4a50-b6f4-2d0a7e5c3f81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One worker process's scheduler, as the server starts it: the real scheduler, the real
executor, the real holder lock on the control plane — and one 1-second job that records each run.

    python -m tests.integration.scheduler_holder_worker <platform-url> <scope> <out-file>

Run by tests/integration/test_scheduler_single_holder.py, several at a time."""

# Requirements: REQ-1900

from __future__ import annotations

import asyncio
import os
import sys
import time

from apscheduler.triggers.interval import IntervalTrigger


def main() -> None:
    url, scope, out = sys.argv[1:4]

    def tick() -> None:
        # O_APPEND: one short write per run is atomic across the processes sharing the file.
        with open(out, "a") as f:
            f.write(f"{os.getpid()} {time.time():.3f}\n")

    async def serve() -> None:
        from provisa.core.connection_loop import set_process_loop
        from provisa.scheduler.holder import Holders, SchedulerHolder
        from provisa.scheduler.jobs import new_scheduler

        set_process_loop(asyncio.get_running_loop())
        # A deployment with no regions: its deployment and region claims are the same (REQ-1922).
        holder = SchedulerHolder(url, scope)
        scheduler = new_scheduler(Holders(deployment=holder, region=holder))
        scheduler.add_job(tick, trigger=IntervalTrigger(seconds=1), id="tick", max_instances=1)
        scheduler.start()
        print("started", flush=True)
        await asyncio.Event().wait()

    asyncio.run(serve())


if __name__ == "__main__":
    main()
