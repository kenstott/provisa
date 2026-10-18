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
    from provisa.core.pg_shared_memory import _listed, recorded_segment

    starter, orphan_root = _start_elsewhere()
    postmaster = _postmaster(orphan_root)
    assert postmaster is not None
    segment = recorded_segment(orphan_root / "data")
    assert segment is not None and _listed(*segment)
    starter.send_signal(signal.SIGTERM)
    starter.wait(timeout=30)
    os.kill(postmaster, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(postmaster) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _listed(*segment)  # what leaked before

    start_local_postgres().stop()  # every start reaps

    assert not orphan_root.exists()
    assert not _listed(*segment)


def test_a_running_postmasters_segment_is_never_released():
    from provisa.core.pg_shared_memory import _listed, recorded_segment, release_segment

    pg = start_local_postgres()
    try:
        segment = recorded_segment(pg.data)
        assert segment is not None
        assert release_segment(pg.data) is False
        assert _listed(*segment)
    finally:
        pg.stop()
    assert not _listed(*segment)  # its own clean stop removed it
