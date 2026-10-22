#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Release the System V semaphore sets of PostgreSQL postmasters that are no longer running.

A postmaster killed outright (SIGKILL) leaves its semaphore sets on macOS until the machine
restarts (provisa/core/pg_shared_memory.py), and test servers killed that way exhausted the
machine's semaphores: no PostgreSQL could start ("could not create semaphores: No space left on
device"). This lists every set PostgreSQL made (its mark on the set's last semaphore), by the
postmaster that made it, and with ``--apply`` removes the sets of each postmaster that is not
running. A running postmaster's sets are never touched, nor those of local-dev's control-pg
postmaster (``--spare``, by default ~/.provisa/demo/control-pg), whether it runs or not.

    python scripts/release_dead_pg_semaphores.py           # what it would remove
    python scripts/release_dead_pg_semaphores.py --apply   # remove it
"""

from __future__ import annotations

import argparse
import collections
import ctypes
import ctypes.util
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from provisa.core import pg_shared_memory as shm  # noqa: E402


def _spared(datadir: Path) -> int | None:
    """The pid local-dev's control-pg postmaster recorded, or None when it records none."""
    pid_file = datadir / "postmaster.pid"
    return int(pid_file.read_text().splitlines()[0]) if pid_file.is_file() else None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Release the semaphore sets of PostgreSQL postmasters no longer running."
    )
    parser.add_argument("--apply", action="store_true", help="remove them (default: list only)")
    parser.add_argument(
        "--spare",
        type=Path,
        default=Path.home() / ".provisa" / "demo" / "control-pg",
        help="a data directory whose postmaster's sets are never removed",
    )
    args = parser.parse_args()
    if not shm.SYSV_SEMAPHORES:
        print("PostgreSQL makes no System V semaphore sets on this platform: nothing to release")
        return 0
    spared = _spared(args.spare)
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    by_postmaster: dict[int, list[int]] = collections.defaultdict(list)
    others = 0
    for semid, nsems in shm._semaphore_sets():
        if libc.semctl(semid, nsems - 1, shm._GETVAL) == shm._PG_SEMA_MAGIC:
            by_postmaster[libc.semctl(semid, nsems - 1, shm._GETPID)].append(semid)
        else:
            others += 1
    dead = {
        pid: sets for pid, sets in by_postmaster.items() if pid != spared and not shm._running(pid)
    }
    print(
        f"{sum(len(s) for s in by_postmaster.values())} PostgreSQL semaphore sets from "
        f"{len(by_postmaster)} postmasters; {others} other sets (never touched)"
    )
    print(
        f"spared: postmaster {spared} ({args.spare})" if spared else f"spared: none ({args.spare})"
    )
    print(f"not running: {len(dead)} postmasters, {sum(len(s) for s in dead.values())} sets")
    if not args.apply:
        print("dry run: nothing removed (--apply removes them)")
        return 0
    removed = 0
    for pid in sorted(dead):
        # release_semaphores checks again that the postmaster is not running, and removes only
        # the sets whose last semaphore carries PostgreSQL's mark and that pid.
        removed += shm.release_semaphores(pid)
    print(f"removed {removed} sets")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
