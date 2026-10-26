# Copyright (c) 2026 Kenneth Stott
# Canary: a0433b57-85f9-4dec-b2f0-30ef45580ae3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a leak test reads of this process from outside it (REQ-2149): ``GET /debug/leak-probe``.

Mounted only when ``PROVISA_LEAK_PROBE=1`` -- the leak test's server sets it, and no deployment
does -- because what it answers is the process's own: its asyncio tasks across every loop, its
threads, how many entries each org-kept map holds (``org_forget.sizes``, the same set an org's
deletion clears) and each process-wide map (``held_sizes``), each connection pool's open and
checked-out connections, and, when the process was started under ``PYTHONTRACEMALLOC``, the memory its
Python allocations hold and the allocation sites that grew since the last call. Resident memory, file descriptors and
sockets the test reads of the process from outside.
"""

from __future__ import annotations

import asyncio
import collections
import gc
import os
import re
import threading
import tracemalloc
from typing import Any

#: The flag that mounts the probe.
ENV = "PROVISA_LEAK_PROBE"


def enabled() -> bool:
    return os.environ.get(ENV) == "1"


def _grew(last: list[tracemalloc.Snapshot], top: int) -> tuple[int, list[dict[str, Any]]]:
    """What the program's allocations hold now, and the ``top`` allocation sites that grew since
    ``last`` (none on the first call) -- in any module, a library's included: growth a library
    holds for Provisa is Provisa's to find -- with ``last`` replaced by this snapshot. Both leave
    out tracemalloc's own blocks: the snapshot kept for the next call is as large as the number
    of blocks traced, and counted it read as growth of the program."""
    gc.collect()
    snapshot = tracemalloc.take_snapshot().filter_traces(
        [tracemalloc.Filter(False, tracemalloc.__file__), tracemalloc.Filter(False, "<unknown>")]
    )
    held = sum(stat.size for stat in snapshot.statistics("filename"))
    grew = []
    if last:
        for stat in snapshot.compare_to(last[0], "lineno")[:top]:
            if stat.size_diff > 0:
                grew.append(
                    {
                        "site": str(stat.traceback[0]),
                        "size_diff": stat.size_diff,
                        "count_diff": stat.count_diff,
                    }
                )
    last[:] = [snapshot]
    return held, grew


def _thread_names() -> dict[str, int]:
    """Live threads by name, a pool's worker number left off: which pool or server a rise in
    ``threads`` is in."""
    return dict(
        collections.Counter(re.sub(r"(_\d+|-\d+.*)$", "", t.name) for t in threading.enumerate())
    )


def _thread_stacks() -> list[dict[str, Any]]:
    """Every live thread's name and stack, innermost frame last: what a thread that never goes
    idle is waiting on."""
    import sys
    import traceback

    frames = sys._current_frames()  # noqa: SLF001 - the interpreter's own per-thread frames
    return [
        {"name": t.name, "stack": traceback.format_stack(frames[t.ident])}
        for t in threading.enumerate()
        if t.ident in frames
    ]


def mount(app: Any) -> None:
    """Mount ``/debug/leak-probe`` on ``app`` when :data:`ENV` is set; otherwise nothing."""
    if not enabled():
        return
    from provisa.core.connection_loop import live_loops
    from provisa.core import held_sizes
    from provisa.core.org_forget import sizes
    from provisa.core.sync_pool import pools

    from concurrent.futures import ThreadPoolExecutor

    last: list[tracemalloc.Snapshot] = []
    # One thread of the probe's own, there from its first call on: work handed to the loop's
    # default executor started threads of its own, read as the process's growth.
    worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="leak-probe")

    @app.get("/debug/leak-probe")
    async def leak_probe(top: int = 15, stacks: bool = False) -> dict[str, Any]:  # pyright: ignore[reportUnusedFunction]
        if stacks:
            # What each thread is doing, without a snapshot: the next growth reading still
            # compares against the last one.
            return {"stacks": _thread_stacks()}
        # Garbage first: cyclic garbage awaiting its collection is not held, and counted as if it
        # were it looked like growth (a SPARQL request's vocabulary is freed only by a full pass).
        # The snapshot walks every traced block: off the loop, which goes on serving.
        running = asyncio.get_running_loop()
        if tracemalloc.is_tracing():
            held, grew = await running.run_in_executor(worker, _grew, last, top)
        else:
            await running.run_in_executor(worker, gc.collect)
            held, grew = 0, []
        loops = [loop for loop in live_loops() if not loop.is_closed()]
        return {
            "tasks": sum(len(asyncio.all_tasks(loop)) for loop in loops),
            "loops": len(loops),
            "threads": threading.active_count(),
            "thread_names": _thread_names(),
            "org_maps": sizes(),
            "maps": held_sizes.sizes(),
            "pools": pools(),
            "tracing": tracemalloc.is_tracing(),
            # What Python allocations hold now: beside the process's resident memory, it says
            # whether growth is Python's (and so named in ``grew``) or a native library's.
            "traced_bytes": held,
            "grew": grew,
        }
