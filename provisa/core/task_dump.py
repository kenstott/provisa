# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2f4d61-7c3b-4e85-b0d9-1f6e8c2a5b37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What every request is awaiting, written to the server log on a timer (REQ-1882).

Each request runs on its own loop (provisa.core.connection_loop), so a stalled server whose loops
are alive shows nothing in a thread dump: every request thread sits in its loop's select. This
names each task on each live loop and the frames its coroutine is suspended in, and writes every
thread's stack beside it (a loop whose own thread is blocked has tasks that cannot say so).
Diagnostics only: started when ``PROVISA_TASK_DUMP_S`` names an interval, as ``PROVISA_LEAK_PROBE``
mounts the leak probe; the frames name other callers' requests, so it goes to the server log
alone.
"""

# Requirements: REQ-1882

from __future__ import annotations

import asyncio
import faulthandler
import logging
import os
import sys
import threading
import time

log = logging.getLogger(__name__)

_STACK_LIMIT = 15


def _frames(task: asyncio.Task) -> list[str]:
    return [
        f"    {f.f_code.co_filename}:{f.f_lineno} {f.f_code.co_name}"
        for f in task.get_stack(limit=_STACK_LIMIT)
    ]


def dump_tasks() -> str:
    """Every task of every live loop, with where it is suspended."""
    from provisa.core import connection_loop

    lines: list[str] = []
    for loop in connection_loop.live_loops():
        if loop.is_closed():
            continue
        # Read from another thread: a task set that changes under the copy is read again.
        for _ in range(3):
            try:
                tasks = list(asyncio.all_tasks(loop))
                break
            except RuntimeError:
                continue
        else:
            continue
        for task in tasks:
            coro = task.get_coro()
            lines.append(f"  task {task.get_name()} {getattr(coro, '__qualname__', coro)}")
            lines.extend(_frames(task))
    return "\n".join(lines)


def dump_once() -> None:
    """One dump: the tasks to the server log, every thread's stack to stderr."""
    log.warning("task dump:\n%s", dump_tasks())
    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)


def start_from_env() -> threading.Thread | None:
    """Start the timed dump when ``PROVISA_TASK_DUMP_S`` names an interval in seconds."""
    raw = os.environ.get("PROVISA_TASK_DUMP_S")
    if not raw:
        return None
    interval = float(raw)

    def _run() -> None:
        while True:
            time.sleep(interval)
            dump_once()

    thread = threading.Thread(target=_run, name="provisa-task-dump", daemon=True)
    thread.start()
    return thread
