# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e4a5b-9478-4e36-9449-a238ecf66a29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A test session's embedded Postgres does not outlive the session that started it.

A session ended by a signal (a job stopped, a runner's timeout) never runs its fixtures' teardown,
and the Postgres it started kept running, holding the machine's shared memory, until no further
one could start ("could not create shared memory segment: No space left on device"). Each one now
records the process that started it, and the next start stops every one whose starter is gone; a
running session's is left alone."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from tests import embedded_pg
from tests.embedded_pg import start_local_postgres

_REPO = Path(__file__).resolve().parents[2]

_STARTER = """
import sys, time
sys.path.insert(0, {repo!r})
from tests.embedded_pg import start_local_postgres
pg = start_local_postgres()
print(pg.root, flush=True)
time.sleep(600)
"""


def _postmaster(root: Path) -> int | None:
    pid_file = root / "data" / "postmaster.pid"
    return int(pid_file.read_text().splitlines()[0]) if pid_file.exists() else None


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _start_elsewhere() -> tuple[subprocess.Popen, Path]:
    """A process that starts an embedded Postgres and holds it, as a test session does."""
    starter = subprocess.Popen(
        [sys.executable, "-c", _STARTER.format(repo=str(_REPO))],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert starter.stdout is not None
    return starter, Path(starter.stdout.readline().strip())


def test_a_postgres_whose_session_was_killed_is_stopped_by_the_next_start():
    starter, orphan_root = _start_elsewhere()
    postmaster = _postmaster(orphan_root)
    assert postmaster is not None and _alive(postmaster)
    starter.send_signal(signal.SIGTERM)  # no teardown runs: the session simply ends
    starter.wait(timeout=30)
    assert _alive(postmaster)  # what leaked before

    pg = start_local_postgres()
    try:
        deadline = time.monotonic() + 30
        while _alive(postmaster) and time.monotonic() < deadline:
            time.sleep(0.2)
        assert not _alive(postmaster)
        assert not orphan_root.exists()
    finally:
        pg.stop()


def test_a_running_session_s_postgres_is_left_alone():
    starter, root = _start_elsewhere()
    try:
        postmaster = _postmaster(root)
        pg = start_local_postgres()
        pg.stop()
        assert postmaster is not None and _alive(postmaster)
        assert root.exists()
    finally:
        starter.send_signal(signal.SIGTERM)
        starter.wait(timeout=30)
        start_local_postgres().stop()  # and it is reaped once its session is gone
    assert not root.exists()


def _ownerless(tmp_path: Path, name: str, age_seconds: float) -> Path:
    root = tmp_path / f"pv-pg-{name}"
    (root / "data").mkdir(parents=True)
    stamp = time.time() - age_seconds
    os.utime(root, (stamp, stamp))
    return root


def test_a_directory_with_no_recorded_starter_is_removed_once_it_is_a_day_old(
    tmp_path, monkeypatch
):
    """A directory made before starters recorded themselves has no owner.pid and was never
    removed: tests' temp space filled with them."""
    monkeypatch.setattr(embedded_pg.tempfile, "gettempdir", lambda: str(tmp_path))
    day = embedded_pg.OWNERLESS_AFTER_SECONDS
    old = _ownerless(tmp_path, "old", day + 60)
    new = _ownerless(tmp_path, "new", 60)  # a starter that has not recorded itself yet
    owned = _ownerless(tmp_path, "owned", day + 60)
    (owned / "owner.pid").write_text(str(os.getpid()))  # its starter runs
    embedded_pg._stop_orphans()
    assert not old.exists()
    assert new.exists() and owned.exists()


_REAPER = """
import sys, time
from pathlib import Path
sys.path.insert(0, {repo!r})
from tests.embedded_pg import _stop_orphans
go = Path({go!r})
while not go.exists():
    time.sleep(0.01)
_stop_orphans()
"""


def _orphan() -> tuple[int, Path]:
    """A running Postgres whose session was killed: what every worker's start reaps."""
    starter, root = _start_elsewhere()
    postmaster = _postmaster(root)
    assert postmaster is not None and _alive(postmaster)
    starter.send_signal(signal.SIGTERM)
    starter.wait(timeout=30)
    return postmaster, root


def test_workers_starting_together_reap_one_orphan_once(tmp_path):
    """Every pytest worker reaps at its own start. Two reaping the same orphan at once failed:
    one stopped it, and the other's pg_ctl found no PID file ("Is server running?") or a half
    removed data directory, and that worker's session fixture errored every test on it."""
    postmaster, root = _orphan()
    go = tmp_path / "go"
    reapers = [
        subprocess.Popen(
            [sys.executable, "-c", _REAPER.format(repo=str(_REPO), go=str(go))],
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(6)
    ]
    time.sleep(2)  # every reaper is waiting
    go.write_text("")
    for reaper in reapers:
        _, stderr = reaper.communicate(timeout=60)
        assert reaper.returncode == 0, stderr
    assert not _alive(postmaster)
    assert not root.exists()


def test_an_orphan_whose_postmaster_is_gone_is_removed():
    """A postmaster killed outright leaves its PID file; there is nothing to stop, and pg_ctl
    refuses to stop it, so the directory is removed without a stop."""
    postmaster, root = _orphan()
    os.kill(postmaster, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(postmaster) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert (root / "data" / "postmaster.pid").exists()
    embedded_pg._stop_orphans()
    assert not root.exists()


# -- the System V segment a killed postmaster leaves -----------------------------------------------


def test_a_killed_postmasters_segment_is_released_when_its_directory_is_reaped():
    """A session killed and its postmaster killed too (SIGKILL: no exit runs) leave the
    postmaster's System V segment. Reaped with the directory that recorded it, it held a slot of
    the machine's 32 until restart; a few dozen such stopped every initdb."""
    from provisa.core.pg_shared_memory import (
        SYSV_SEMAPHORES,
        _listed,
        postmaster_semaphore_sets,
        recorded_segment,
    )

    starter, orphan_root = _start_elsewhere()
    postmaster = _postmaster(orphan_root)
    assert postmaster is not None
    segment = recorded_segment(orphan_root / "data")
    assert segment is not None and _listed(*segment)
    semaphores = postmaster_semaphore_sets(postmaster)
    # macOS: System V sets, which outlive a kill. Linux: none (POSIX, in the shared memory).
    assert bool(semaphores) is SYSV_SEMAPHORES
    starter.send_signal(signal.SIGTERM)
    starter.wait(timeout=30)
    os.kill(postmaster, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(postmaster) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _listed(*segment)  # what leaked before
    assert (
        postmaster_semaphore_sets(postmaster) == semaphores
    )  # and these, until the machine restarts

    start_local_postgres().stop()  # every start reaps

    assert not orphan_root.exists()
    assert not _listed(*segment)
    assert postmaster_semaphore_sets(postmaster) == []


def test_a_running_postmasters_segment_is_never_released():
    from provisa.core.pg_shared_memory import (
        SYSV_SEMAPHORES,
        _listed,
        postmaster_semaphore_sets,
        recorded_segment,
        release_segment,
    )

    pg = start_local_postgres()
    try:
        segment = recorded_segment(pg.data)
        assert segment is not None
        postmaster = _postmaster(pg.root)
        assert postmaster is not None
        semaphores = postmaster_semaphore_sets(postmaster)
        assert bool(semaphores) is SYSV_SEMAPHORES
        assert release_segment(pg.data) is False
        assert _listed(*segment)
        assert postmaster_semaphore_sets(postmaster) == semaphores
    finally:
        pg.stop()
    assert not _listed(*segment)  # its own clean stop removed it
    assert postmaster_semaphore_sets(postmaster) == []  # and its semaphores


def test_a_killed_session_with_a_temp_directory_of_its_own_leaves_nothing_once_reaped(tmp_path):
    """Agents and CI jobs give each session a temporary directory of its own. A server's
    directory was made there: another session's reaper never saw it, and once that temporary
    directory was removed, so was the one record of the killed postmaster's semaphore sets --
    about 5,000 sets from some 230 killed servers, until no server on the machine could start.
    Every server's directory is under the machine's RUN_ROOT instead."""
    import shutil

    from provisa.core.pg_shared_memory import SYSV_SEMAPHORES, postmaster_semaphore_sets

    own_temp = tmp_path / "session-temp"
    own_temp.mkdir()
    starter = subprocess.Popen(
        [sys.executable, "-c", _STARTER.format(repo=str(_REPO))],
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "TMPDIR": str(own_temp)},
    )
    assert starter.stdout is not None
    root = Path(starter.stdout.readline().strip())
    assert root.parent == embedded_pg.RUN_ROOT  # not the session's own temporary directory
    postmaster = _postmaster(root)
    assert postmaster is not None
    semaphores = postmaster_semaphore_sets(postmaster)
    assert bool(semaphores) is SYSV_SEMAPHORES
    starter.send_signal(signal.SIGTERM)
    starter.wait(timeout=30)
    os.kill(postmaster, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(postmaster) and time.monotonic() < deadline:
        time.sleep(0.05)
    shutil.rmtree(own_temp)  # its session's temporary directory goes with it

    embedded_pg._stop_orphans()  # any later session's start, whatever its own TMPDIR

    assert not root.exists()
    assert postmaster_semaphore_sets(postmaster) == []


def test_a_pid_recorded_before_its_process_started_is_not_that_process(tmp_path):
    """A server's directory now outlives a restart of the machine, after which the pid it
    recorded may name another process: one started after the record is not the one that wrote
    it, and is never signalled as its postmaster."""
    record = tmp_path / "owner.pid"
    record.write_text(str(os.getpid()))
    long_ago = time.time() - 10 * 24 * 3600
    os.utime(record, (long_ago, long_ago))
    assert embedded_pg._running(os.getpid(), record.stat().st_mtime) is False
    assert embedded_pg._running(os.getpid(), time.time()) is True


def test_a_key_recorded_sign_extended_is_the_key_the_system_lists(monkeypatch):
    """PostgreSQL writes a segment key whose top bit is set sign-extended to 64 bits
    (18446744071564963621); ``ipcs`` lists the 32-bit key (0x802c2f25). Read as written, such a
    segment was never found, so it was never released."""
    from types import SimpleNamespace

    from provisa.core import pg_shared_memory

    listing = (
        "IPC status from <running system> as of Sun Oct 25 2026\n"
        "T     ID     KEY        MODE       OWNER    GROUP\n"
        "Shared Memory:\n"
        "m 4915208 0x802c2f25 --rw------- runner   staff\n"
    )
    monkeypatch.setattr(
        pg_shared_memory.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(stdout=listing),
    )
    assert pg_shared_memory._listed(18446744071564963621, 4915208)
    assert pg_shared_memory._listed(0x802C2F25, 4915208)
    assert not pg_shared_memory._listed(0x802C2F26, 4915208)
