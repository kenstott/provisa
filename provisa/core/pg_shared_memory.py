# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9f5c27-b8e1-4d06-92c4-6f1e0d7b8a53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The System V shared memory segment an embedded PostgreSQL leaves when it is killed.

Every postmaster holds one small System V segment (its interlock header; the main shared memory is
mmap). A postmaster that exits removes it, and one started again on the same data directory
removes its predecessor's, by the key and id it recorded on line 7 of ``postmaster.pid``. One
killed (SIGKILL) whose data directory is then deleted leaves it for good: nothing else records it.
macOS allows 32 segments (``kern.sysv.shmmni``), so a few dozen killed servers stopped every later
``initdb`` with "could not create shared memory segment: No space left on device".

:func:`release_segment` removes the segment a data directory's dead postmaster recorded, before
that directory goes. It touches nothing else: not a segment of a running postmaster, not one whose
recorded key no longer names that id, and no segment any other data directory recorded.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

#: ``postmaster.pid`` line 7 (1-based): the segment's key and id, as the postmaster wrote them.
_SHMEM_LINE = 6


def _running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # another user's process: running
    return True


def recorded_segment(datadir: str | Path) -> tuple[int, int] | None:
    """``(key, id)`` of the segment the postmaster of ``datadir`` recorded, or None when it
    recorded none (no ``postmaster.pid``, or one written before its segment was made)."""
    pid_file = Path(datadir) / "postmaster.pid"
    if not pid_file.is_file():
        return None
    lines = pid_file.read_text().splitlines()
    if len(lines) <= _SHMEM_LINE:
        return None
    fields = lines[_SHMEM_LINE].split()
    if len(fields) != 2:
        return None
    return int(fields[0]), int(fields[1])


def _listed(key: int, shmid: int) -> bool:
    """Whether the system lists segment ``shmid`` under ``key`` (``ipcs -m``; the columns differ
    between macOS and Linux, and both list a segment's id and its key in hexadecimal)."""
    listing = subprocess.run(["ipcs", "-m"], capture_output=True, text=True, check=True).stdout
    hex_key = f"0x{key:08x}"
    return any(
        str(shmid) in fields and hex_key in fields
        for fields in (line.split() for line in listing.splitlines())
    )


def release_segment(datadir: str | Path) -> bool:
    """Remove the segment ``datadir``'s postmaster recorded when that postmaster is not running:
    True when one was removed. A running postmaster's is left to it, and so is an id the system
    no longer lists under the recorded key (removed already, or since reused by another)."""
    pid_file = Path(datadir) / "postmaster.pid"
    recorded = recorded_segment(datadir)
    if recorded is None:
        return False
    if _running(int(pid_file.read_text().splitlines()[0])):
        return False
    key, shmid = recorded
    if not _listed(key, shmid):
        return False
    subprocess.run(["ipcrm", "-m", str(shmid)], capture_output=True, check=True)
    return True
