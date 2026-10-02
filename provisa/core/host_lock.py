# Copyright (c) 2026 Kenneth Stott
# Canary: cd3fffd6-e4bd-4cce-98b7-2cb4087c0a73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An exclusive lock among the processes of one host: ``flock`` on a lock file.

The operating system holds the lock for an open file and releases it when that file is closed,
which it is when the holding process ends, however it ends. So nothing is renewed and nothing
has to notice a dead holder. Two opens of the same file are two holders, in one process or in
several, so the lock also excludes between threads.

Used where a control plane cannot guarantee a lock itself (a SQLite control plane is a file on
one host) and for limits that are per host by nature.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import tempfile
from pathlib import Path

from provisa.core import request_deadline


class FileLock:
    """One exclusive lock, on the file at ``path``. Not re-entrant."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def _take(self, flags: int) -> bool:
        if self._fd is not None:
            raise RuntimeError(f"{self.path} is already held by this lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, flags)
            except BlockingIOError:
                os.close(fd)
                return False
            except BaseException:
                os.close(fd)
                raise
            self._fd = fd
        return True

    def try_acquire(self) -> bool:
        """Take the lock if nobody holds it. Never waits."""
        return self._take(fcntl.LOCK_EX | fcntl.LOCK_NB)

    def acquire(self) -> None:
        """Take the lock, waiting for the holder to release it. For work outside a request (the
        boot sequence): the wait is not bounded by a deadline."""
        if self._fd is not None:
            raise RuntimeError(f"{self.path} is already held by this lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    @property
    def held(self) -> bool:
        return self._fd is not None

    def release(self) -> None:
        """Release the lock. A lock that is not held is left alone."""
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            fd, self._fd = self._fd, None
            if fd is not None:
                os.close(fd)  # closing the file releases its lock


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def lock_name(kind: str, identity: str) -> str:
    """A file name for the lock of ``identity`` (any text) of the given ``kind``."""
    return f"{kind}-{_digest(identity)}.lock"


def host_lock_dir(deployment: str) -> Path:
    """The directory holding the per-host locks of the deployment identified by ``deployment``
    (its platform control-plane URL): two deployments on one host do not share locks. It lives
    in the host's temporary directory: a lock is held only while its holder runs, so nothing in
    it has to outlive a restart of the host."""
    return Path(tempfile.gettempdir()) / "provisa-locks" / _digest(deployment)


def control_plane_lock_dir(url: str) -> Path:
    """Where the locks of a control plane that is a file on this host live: beside the file. A
    control plane held in memory belongs to one process, whose locks go in the host directory."""
    from sqlalchemy import make_url

    from provisa.core.database import sync_store_url

    database = make_url(sync_store_url(url)).database
    if not database or database == ":memory:":
        return host_lock_dir(f"{url}#{os.getpid()}")
    return Path(database).resolve().parent / ".provisa-locks"
