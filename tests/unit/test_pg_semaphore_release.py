# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1b9d47-2c83-4a05-b7f1-9d3e5a0c8f21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A killed embedded postmaster's semaphore sets are released with its segment, and nothing else is
(pg_shared_memory). The end-to-end case, a real postmaster killed and reaped, is in
test_embedded_pg_orphans; these hold the listing and the choice of sets without a server."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest

from provisa.core import pg_shared_memory as shm

_DARWIN = """IPC status from <running system> as of Wed Oct 21 16:33:08 EDT 2026
T     ID     KEY        MODE       OWNER    GROUP  CREATOR   CGROUP NSEMS   OTIME    CTIME
Semaphores:
s 1572864 0x3e22795c --ra------- ken    staff ken    staff     17 15:40:44 15:40:44
s 1572865 0x3e22795d --ra------- ken    staff ken    staff     17 15:40:44 15:40:44
"""

_LINUX = """
------ Semaphore Arrays --------
key        semid      owner      perms      nsems
0x0135a0a1 3          runner     600        17
0x0135a0a2 4          runner     600        17
"""


def _listing(monkeypatch, platform: str, text: str, removed: list | None = None) -> None:
    monkeypatch.setattr(shm.sys, "platform", platform)

    def run(cmd, **_kw):
        if cmd[0] == "ipcrm":
            assert removed is not None, cmd
            removed.append(int(cmd[2]))
            return SimpleNamespace(stdout="", returncode=0)
        return SimpleNamespace(stdout=text, returncode=0)

    monkeypatch.setattr(subprocess, "run", run)


@pytest.mark.parametrize(
    ("platform", "text", "expected"),
    [("darwin", _DARWIN, [(1572864, 17), (1572865, 17)]), ("linux", _LINUX, [(3, 17), (4, 17)])],
)
def test_the_listing_is_read_on_each_platform(monkeypatch, platform, text, expected):
    _listing(monkeypatch, platform, text)
    assert shm._semaphore_sets() == expected


class _Sets:
    """A stand-in for libc.semctl over sets: semid -> (value of the last semaphore, its last pid)."""

    def __init__(self, sets: dict[int, tuple[int, int]]) -> None:
        self.sets = sets

    def semctl(self, semid: int, _semnum: int, cmd: int) -> int:
        value, pid = self.sets[semid]
        return value if cmd == shm._GETVAL else pid


def test_only_the_dead_postmasters_own_pg_sets_are_released(monkeypatch):
    removed: list[int] = []
    _listing(
        monkeypatch, "linux", _LINUX + "0x0135a0a3 5          runner     600        17\n", removed
    )
    monkeypatch.setattr(shm, "_GETVAL", 12)
    monkeypatch.setattr(shm, "_GETPID", 11)
    # 3: the dead postmaster's; 4: another postmaster's; 5: not PostgreSQL's (no mark).
    libc = _Sets({3: (537, 4242), 4: (537, 5151), 5: (1, 4242)})
    monkeypatch.setattr(shm.ctypes, "CDLL", lambda *_a, **_k: libc)
    monkeypatch.setattr(shm, "_running", lambda pid: False)

    assert shm.release_semaphores(4242) == 1
    assert removed == [3]


def test_a_running_postmasters_sets_are_left_to_it(monkeypatch):
    _listing(monkeypatch, "linux", _LINUX)  # no ipcrm may run
    monkeypatch.setattr(shm, "_running", lambda pid: True)
    assert shm.release_semaphores(4242) == 0
