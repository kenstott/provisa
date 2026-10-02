# Copyright (c) 2026 Kenneth Stott
# Canary: f1063a62-41b3-4667-9160-fa6fbcc38037
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A file lock is exclusive among holders and is freed when its holding process ends."""

import subprocess
import sys
import textwrap

from provisa.core.boot_lock import boot_generation, control_plane_boot_lock
from provisa.core.host_lock import FileLock, control_plane_lock_dir
from provisa.scheduler.holder import SchedulerHolder


def test_one_holder_at_a_time_and_release_frees_it(tmp_path):
    first, second = FileLock(tmp_path / "x.lock"), FileLock(tmp_path / "x.lock")
    assert first.try_acquire()
    assert not second.try_acquire()
    first.release()
    assert second.try_acquire()
    second.release()
    second.release()  # releasing a lock that is not held is left alone


def test_a_killed_holder_frees_the_lock(tmp_path):
    path = tmp_path / "x.lock"
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                f"""
                import sys, time
                from pathlib import Path
                from provisa.core.host_lock import FileLock
                assert FileLock(Path({str(path)!r})).try_acquire()
                print("held", flush=True)
                time.sleep(60)
                """
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "held"
        mine = FileLock(path)
        assert not mine.try_acquire()
        holder.kill()
        holder.wait()
        assert mine.try_acquire()
        mine.release()
    finally:
        holder.kill()


def test_one_scheduler_holder_among_the_workers_of_a_sqlite_control_plane(tmp_path):
    url = f"sqlite+pysqlite:///{tmp_path / 'platform.db'}"
    workers = [SchedulerHolder(url, "default") for _ in range(3)]
    try:
        assert [w.holds() for w in workers] == [True, False, False]
        assert workers[0].holds()  # it keeps holding
        workers[0].close()  # the holder stops: the next to ask takes over
        assert [w.holds() for w in workers[1:]] == [True, False]
        other_scope = SchedulerHolder(url, "another-deployment")
        assert other_scope.holds()
        other_scope.close()
    finally:
        for w in workers:
            w.close()
    assert control_plane_lock_dir(url) == tmp_path / ".provisa-locks"


def test_the_boot_lock_on_a_sqlite_control_plane_records_the_generation(tmp_path):
    url = f"sqlite+pysqlite:///{tmp_path / 'platform.db'}"
    generation = boot_generation("launch-1", schema="s")
    with control_plane_boot_lock(url) as lock:
        assert not lock.completed("default", generation)  # the first worker does the work
        lock.mark_completed("default", generation)
    with control_plane_boot_lock(url) as lock:
        assert lock.completed("default", generation)  # the next worker skips it
        assert not lock.completed("default", boot_generation("launch-2", schema="s"))
