# Copyright (c) 2026 Kenneth Stott
# Canary: c41e9a06-5d7b-4f83-b2a9-0e6f1d3c8b57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The once-per-launch half of the boot runs once; every worker does its own half (REQ-1900).

``uvicorn --workers N`` used to run the whole boot in every worker, one at a time under the boot
lock: time-to-all-ready was N times one boot. The control plane now records which launch's
once-per-launch work is complete, so the first worker does it and the rest go straight to the
per-worker half — at the same time."""

# Requirements: REQ-1900

from __future__ import annotations

import os
import signal
import threading
import time
import uuid

import pytest
import sqlalchemy as sa

from provisa.core.boot_lock import boot_generation, control_plane_boot_lock
from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_PG_USER = os.environ.get("PG_USER", "provisa")
_PG_PASSWORD = os.environ.get("PG_PASSWORD", "provisa")
_BASE = f"postgresql+psycopg://{_PG_USER}:{_PG_PASSWORD}@{_PG_HOST}:{_PG_PORT}"
_ADMIN_URL = f"{_BASE}/{os.environ.get('PG_DATABASE', 'provisa')}"
_WORKERS = 4


@pytest.fixture
def fresh_database():
    name = f"boot_gen_{uuid.uuid4().hex[:10]}"
    admin = sa.create_engine(_ADMIN_URL, isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    try:
        yield f"{_BASE}/{name}"
    finally:
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def _boot_together(url: str, generations: list[str | None]) -> list[str]:
    """Run one simulated boot per generation, all released together; return who did the
    once-per-launch work."""
    start = threading.Barrier(len(generations))
    applied: list[str] = []
    failures: list[BaseException] = []

    def _boot(generation: str | None) -> None:
        try:
            start.wait(timeout=30)
            with control_plane_boot_lock(url) as lock:
                if not lock.completed("default", generation):
                    time.sleep(0.05)  # the once-per-launch work
                    applied.append(str(generation))
                    lock.mark_completed("default", generation)
        except BaseException as exc:  # collected and asserted on below
            failures.append(exc)

    threads = [threading.Thread(target=_boot, args=(g,)) for g in generations]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not failures, failures
    return applied


def test_workers_of_one_launch_do_the_once_work_once(fresh_database):
    generation = boot_generation("launch-a", config="c1", schema="s1")
    assert _boot_together(fresh_database, [generation] * 12) == [generation]
    # A worker that boots later in the same launch (a respawn) finds the work done.
    assert _boot_together(fresh_database, [generation]) == []


def test_a_new_launch_or_a_changed_config_is_a_new_generation(fresh_database):
    first = boot_generation("launch-a", config="c1", schema="s1")
    assert _boot_together(fresh_database, [first] * 3) == [first]
    relaunch = boot_generation("launch-b", config="c1", schema="s1")
    assert _boot_together(fresh_database, [relaunch] * 3) == [relaunch]
    reconfigured = boot_generation("launch-b", config="c2", schema="s1")
    assert _boot_together(fresh_database, [reconfigured] * 3) == [reconfigured]


def test_a_process_outside_a_launch_always_does_the_whole_boot(fresh_database):
    assert _boot_together(fresh_database, [None] * 3) == ["None"] * 3


def test_a_file_control_plane_records_no_generation(tmp_path):
    url = f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    generation = boot_generation("launch-a", config="c1")
    with control_plane_boot_lock(url) as lock:
        assert not lock.completed("default", generation)
        lock.mark_completed("default", generation)
    with control_plane_boot_lock(url) as lock:
        assert not lock.completed("default", generation)


@pytest.fixture
def four_workers():
    boot = WorkerBoot(_WORKERS, pg_host=_PG_HOST, pg_port=_PG_PORT)
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _error_lines(log: str) -> list[str]:
    return [ln for ln in log.splitlines() if "ERROR" in ln or "Traceback" in ln]


def test_four_workers_boot_together_on_a_fresh_control_plane(four_workers):
    boot = four_workers
    log = boot.log_text()
    assert _error_lines(log) == []
    assert log.count("startup phase once-per-launch      applied") == 1
    assert log.count("startup phase once-per-launch      found complete") == _WORKERS - 1
    # Only the applying worker runs the control-plane DDL + seed phase; the rest skip it.
    seeded = [pid for pid, rows in boot.phases().items() if dict(rows).get("pg+schema+seed")]
    assert len(seeded) == 1
    health = boot.health()
    assert health is not None
    assert health["workers"] == {"ready": _WORKERS, "expected": _WORKERS}


def test_a_respawned_worker_skips_the_once_work(four_workers):
    boot = four_workers
    victim = sorted(boot.ready_pids())[0]
    os.kill(victim, signal.SIGKILL)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if len(boot.ready_pids()) == _WORKERS + 1:
            break
        time.sleep(0.2)
    else:
        raise AssertionError(f"no worker replaced {victim}:\n{boot.log_text()[-3000:]}")
    log = boot.log_text()
    assert log.count("startup phase once-per-launch      applied") == 1
    assert log.count("startup phase once-per-launch      found complete") == _WORKERS
    assert _error_lines(log) == []
    health = boot.health()
    assert health is not None
    assert health["workers"] == {"ready": _WORKERS, "expected": _WORKERS}
