# Copyright (c) 2026 Kenneth Stott
# Canary: 937aa8ef-8802-4e93-88e6-082f188295ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Whether a step paged (REQ-2092): the host's major faults and swap traffic, each process class's
major faults, and the memory available at the start and end of the step.

CPU per request that rises with concurrency on a small machine can be paging rather than work; a
step's paging figures sit beside its CPU per request so a run can confirm or rule that out. Read
from ``/proc`` (Linux). A host without it (a developer's Mac) reports the reason under ``error``,
never a guessed figure.
"""

# Requirements: REQ-2092

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

PROC = Path("/proc")
VMSTAT_FIELDS = ("pgmajfault", "pswpin", "pswpout")


def _vmstat(proc: Path) -> dict[str, int]:
    values = dict(line.split() for line in (proc / "vmstat").read_text().splitlines())
    return {f: int(values[f]) for f in VMSTAT_FIELDS}


def _mem_available_kb(proc: Path) -> int:
    for line in (proc / "meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1])
    raise ValueError(f"no MemAvailable in {proc / 'meminfo'}")


def _children(proc: Path, pid: int) -> list[int]:
    out = []
    for task in (proc / str(pid) / "task").iterdir():
        kids = task / "children"
        if kids.exists():
            out.extend(int(p) for p in kids.read_text().split())
    return out


def pid_tree(proc: Path, root: int) -> list[int]:
    pids, todo = [], [root]
    while todo:
        pid = todo.pop()
        pids.append(pid)
        try:
            todo.extend(_children(proc, pid))
        except FileNotFoundError:
            continue  # exited while walked: its faults went to its parent's cmajflt
    return pids


def majflt(proc: Path, root: int) -> int:
    """Major faults of ``root`` and every live descendant, with the faults of the children each
    has reaped (``cmajflt``), from field 12 and 13 of ``/proc/<pid>/stat``."""
    total = 0
    for pid in pid_tree(proc, root):
        try:
            text = (proc / str(pid) / "stat").read_text()
        except FileNotFoundError:
            continue  # exited between the walk and the read
        # Fields after the ") " that ends the command name (which may hold spaces) start at 3.
        fields = text.rsplit(") ", 1)[1].split()
        total += int(fields[12 - 3]) + int(fields[13 - 3])
    return total


def snapshot(classes: Mapping[str, int], proc: Path = PROC) -> dict[str, Any]:
    """The host's counters now and each class's major faults (``classes``: name to root pid).
    ``{"error": reason}`` when the host has no ``/proc`` to read them from."""
    try:
        return {
            **_vmstat(proc),
            "mem_available_kb": _mem_available_kb(proc),
            "majflt": {name: majflt(proc, pid) for name, pid in classes.items()},
        }
    except OSError as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    """A step's paging: counter deltas, each class's major faults over the step, and the memory
    available at its start and end (MB)."""
    for snap in (before, after):
        if "error" in snap:
            return {"error": snap["error"]}
    return {
        **{f: after[f] - before[f] for f in VMSTAT_FIELDS},
        "majflt": {k: after["majflt"][k] - before["majflt"][k] for k in before["majflt"]},
        "mem_available_mb_start": round(before["mem_available_kb"] / 1024),
        "mem_available_mb_end": round(after["mem_available_kb"] / 1024),
    }
