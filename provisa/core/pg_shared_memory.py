# Copyright (c) 2026 Kenneth Stott
# Canary: 3a9f5c27-b8e1-4d06-92c4-6f1e0d7b8a53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The System V shared memory segment and semaphore sets an embedded PostgreSQL leaves when it is
killed.

Every postmaster holds one small System V segment (its interlock header; the main shared memory is
mmap). A postmaster that exits removes it, and one started again on the same data directory
removes its predecessor's, by the key and id it recorded on line 7 of ``postmaster.pid``. One
killed (SIGKILL) whose data directory is then deleted leaves it for good: nothing else records it.
macOS allows 32 segments (``kern.sysv.shmmni``), so a few dozen killed servers stopped every later
``initdb`` with "could not create shared memory segment: No space left on device".

Its System V semaphore sets leak the same way. PostgreSQL reclaims a dead creator's sets only when
a later postmaster probes their keys, which start at the data directory's inode: a server on a
fresh directory never probes them again. macOS allows 87381 semaphores (``kern.sysv.semmns``), 21
sets of 17 per postmaster, so a few thousand killed servers stopped every later ``initdb`` with
"could not create semaphores: No space left on device".

:func:`release_segment` removes the segment a data directory's dead postmaster recorded, and that
postmaster's semaphore sets (:func:`release_semaphores`), before that directory goes. It touches
nothing else: not a segment or set of a running postmaster, not a segment whose recorded key no
longer names that id, and nothing any other postmaster made.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import subprocess
import sys
from pathlib import Path

#: ``postmaster.pid`` line 7 (1-based): the segment's key and id, as the postmaster wrote them.
_SHMEM_LINE = 6

#: The value PostgreSQL leaves on the last semaphore of every set it makes (``PGSemaMagic``,
#: sysv_sema.c). Only the creating postmaster operates on that semaphore, so the pid of the last
#: operation on it is the creator's.
_PG_SEMA_MAGIC = 537

#: ``semctl`` commands (<sys/sem.h>): they differ between macOS and Linux.
_GETPID, _GETVAL = (4, 5) if sys.platform == "darwin" else (11, 12)


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


def _semaphore_sets() -> list[tuple[int, int]]:
    """``(semid, nsems)`` of every semaphore set the system lists (``ipcs -s``; macOS lists them
    under ``s`` rows with ``-a`` for the counts, Linux under their hexadecimal key)."""
    if sys.platform == "darwin":
        listing = subprocess.run(["ipcs", "-s", "-a"], capture_output=True, text=True, check=True)
        rows = (line.split() for line in listing.stdout.splitlines())
        return [(int(f[1]), int(f[8])) for f in rows if f and f[0] == "s"]
    listing = subprocess.run(["ipcs", "-s"], capture_output=True, text=True, check=True)
    rows = (line.split() for line in listing.stdout.splitlines())
    return [(int(f[1]), int(f[4])) for f in rows if f and f[0].startswith("0x")]


def postmaster_semaphore_sets(postmaster_pid: int) -> list[int]:
    """The ids of the semaphore sets postmaster ``postmaster_pid`` made: those whose last semaphore
    holds PostgreSQL's mark and was last operated on by that pid."""
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    return [
        semid
        for semid, nsems in _semaphore_sets()
        if libc.semctl(semid, nsems - 1, _GETVAL) == _PG_SEMA_MAGIC
        and libc.semctl(semid, nsems - 1, _GETPID) == postmaster_pid
    ]


def release_semaphores(postmaster_pid: int) -> int:
    """Remove the semaphore sets postmaster ``postmaster_pid`` made when it is not running: the
    number removed. A running postmaster's are left to it."""
    if _running(postmaster_pid):
        return 0
    sets = postmaster_semaphore_sets(postmaster_pid)
    for semid in sets:
        subprocess.run(["ipcrm", "-s", str(semid)], capture_output=True, check=True)
    return len(sets)


def release_segment(datadir: str | Path) -> bool:
    """Remove the segment ``datadir``'s postmaster recorded, and its semaphore sets, when that
    postmaster is not running: True when a segment was removed. A running postmaster's are left
    to it, and so is an id the system no longer lists under the recorded key (removed already, or
    since reused by another)."""
    pid_file = Path(datadir) / "postmaster.pid"
    recorded = recorded_segment(datadir)
    if recorded is None:
        # PostgreSQL makes its semaphores after its segment: none recorded, none made.
        return False
    postmaster = int(pid_file.read_text().splitlines()[0])
    if _running(postmaster):
        return False
    release_semaphores(postmaster)
    key, shmid = recorded
    if not _listed(key, shmid):
        return False
    subprocess.run(["ipcrm", "-m", str(shmid)], capture_output=True, check=True)
    return True
