# Copyright (c) 2026 Kenneth Stott
# Canary: a05c7f38-2e61-4d9b-8f14-7b3d9e0c6a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A scheduled job runs once per firing, however many worker processes there are (REQ-1900).

Every worker of a `--workers N` launch starts its own scheduler, so every scheduled job — MV
refresh ticks, source polls, reapers, OTEL compaction, config triggers — fired N times. One
worker now holds a session advisory lock on the control plane and runs them; the others keep
their schedulers ticking and take over the moment the holder's session ends."""

# Requirements: REQ-1900

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration]

_REPO_ROOT = Path(__file__).parents[2]
_URL = (
    f"postgresql+psycopg://{os.environ.get('PG_USER', 'provisa')}"
    f":{os.environ.get('PG_PASSWORD', 'provisa')}"
    f"@{os.environ.get('PG_HOST', 'localhost')}:{os.environ.get('PG_PORT', '5432')}"
    f"/{os.environ.get('PG_DATABASE', 'provisa')}"
)
_WORKERS = 4


def _runs(out: Path) -> list[tuple[int, float]]:
    if not out.exists():
        return []
    return [(int(p), float(t)) for p, t in (line.split() for line in out.read_text().splitlines())]


@pytest.fixture
def workers(tmp_path):
    out = tmp_path / "runs.txt"
    scope = f"sched-{uuid.uuid4().hex[:8]}"
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests.integration.scheduler_holder_worker",
                _URL,
                scope,
                str(out),
            ],
            cwd=str(_REPO_ROOT),
            env={**os.environ, "OTEL_SDK_DISABLED": "true"},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for _ in range(_WORKERS)
    ]
    try:
        for proc in procs:
            assert proc.stdout is not None
            line = proc.stdout.readline()
            assert line.strip() == "started", line + proc.stdout.read()
        yield procs, out
    finally:
        for proc in procs:  # only the processes this test started
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=20)


def test_a_one_second_job_fires_once_per_second_across_four_workers(workers):
    _procs, out = workers
    time.sleep(8)
    runs = _runs(out)
    span = runs[-1][1] - runs[0][1]
    # One run per second of the span (one worker's cadence), not four.
    assert abs(len(runs) - 1 - span) <= 1.5, runs
    assert len({pid for pid, _ in runs}) == 1, runs


def test_another_worker_takes_over_when_the_holder_dies(workers):
    procs, out = workers
    time.sleep(4)
    before = _runs(out)
    holder = before[-1][0]
    assert {pid for pid, _ in before} == {holder}
    victim = next(p for p in procs if p.pid == holder)
    victim.kill()
    victim.wait(timeout=20)
    killed_at = time.time()
    time.sleep(6)
    after = [(pid, t) for pid, t in _runs(out) if t > killed_at]
    successors = {pid for pid, _ in after}
    assert len(successors) == 1 and holder not in successors, after
    # It took over within a couple of firings and kept the one-per-second cadence.
    assert after[0][1] - killed_at < 3, after
    assert 3 <= len(after) <= 7, after
