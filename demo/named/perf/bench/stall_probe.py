# Copyright (c) 2026 Kenneth Stott
# Canary: e2986025-6cb3-4725-bb92-7f87f905d5d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""When a process stalled (REQ-1911): its garbage collections, and the moments a thread that only
sleeps woke late -- the process held (the GIL, a collection, a page fault, the scheduler) for that
long. Each is a record with its wall-clock end, so a slow request can be put beside what its server
and its client were doing at the time.

The server's workers load it from ``stallprobe/sitecustomize.py`` (serve_three_device.sh puts that
directory on their PYTHONPATH), writing one JSON line per record to a file per process; a client
process installs it with a list to append to."""

from __future__ import annotations

import gc
import json
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

TICK_S = 0.01
# A tick this much later than asked is a stall; a collection this long is recorded.
LATE_S = 0.05
GC_S = 0.005


def install(emit: Callable[[dict[str, Any]], None]) -> None:
    """Record this process's collections and stalls through ``emit``, from now on."""
    pid = os.getpid()
    started: list[float] = []

    def on_gc(phase: str, info: dict[str, Any]) -> None:
        if phase == "start":
            started.append(time.perf_counter())
            return
        took = time.perf_counter() - started.pop()
        if took >= GC_S:
            emit(
                {
                    "kind": "gc",
                    "t": time.time(),
                    "ms": round(took * 1000, 3),
                    "gen": info["generation"],
                    "pid": pid,
                }
            )

    gc.callbacks.append(on_gc)

    def ticks() -> None:
        while True:
            before = time.perf_counter()
            time.sleep(TICK_S)
            late = time.perf_counter() - before - TICK_S
            if late >= LATE_S:
                emit({"kind": "stall", "t": time.time(), "ms": round(late * 1000, 3), "pid": pid})

    threading.Thread(target=ticks, name="stall-probe", daemon=True).start()


def install_to_dir(directory: Path) -> None:
    """Record this process's collections and stalls as JSON lines in ``directory``."""
    directory.mkdir(parents=True, exist_ok=True)
    out = (directory / f"stall-{os.getpid()}.jsonl").open("a", buffering=1)
    lock = threading.Lock()

    def emit(record: dict[str, Any]) -> None:
        with lock:
            out.write(json.dumps(record) + "\n")

    install(emit)


def read_dir(directory: Path, since: float, until: float) -> list[dict[str, Any]]:
    """Every record in ``directory`` that ended between ``since`` and ``until``, in time order."""
    records = []
    for path in directory.glob("stall-*.jsonl"):
        for line in path.read_text().splitlines():
            record = json.loads(line)
            if since <= record["t"] <= until:
                records.append(record)
    return sorted(records, key=lambda r: r["t"])
