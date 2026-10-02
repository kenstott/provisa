# Copyright (c) 2026 Kenneth Stott
# Canary: c75a53b6-f103-4802-b489-7bdfa710cdb2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which processes count as harness servers a dead session left running.

``uvicorn --workers N`` is a supervisor and N workers, and a worker does not exit when its
supervisor dies. Workers orphaned by a killed session (or a killed supervisor) kept retrying their
control plane at the Postgres port of a test stack that no longer existed — the first port of a
lease block — and later sessions were leased that port number for their own listeners: 131 such
processes produced hundreds of pgwire connections a minute to a server no test was talking to.
``tests.itest_stack.reap_orphaned_server_processes`` ends them; these tests pin what it SELECTS,
on stand-in processes of their own, and end only those — never whatever else is on the machine."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

import pytest

from tests.itest_stack import _end, _orphaned_harness_processes

# What a harness worker looks like to `ps`: a Python process with "uvicorn" on its command line.
_STAND_IN = [sys.executable, "-c", "import time; time.sleep(600)", "uvicorn"]


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _await(condition, what: str) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(what)


def _parent_pid(pid: int) -> str:
    return subprocess.run(
        ["ps", "-o", "ppid=", "-p", str(pid)], capture_output=True, text=True, check=False
    ).stdout.strip()


def _orphan(data_dir: str) -> int:
    """Start a stand-in worker whose parent exits at once; return it once init has adopted it."""
    launcher = (
        "import os, subprocess, sys\n"
        "child = subprocess.Popen(\n"
        "    sys.argv[2:], env=dict(os.environ, PROVISA_DATA_DIR=sys.argv[1]),\n"
        "    start_new_session=True,\n"
        # Its own output: holding the launcher's pipe open would keep run() below waiting on it.
        "    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,\n"
        ")\n"
        "print(child.pid)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", launcher, data_dir, *_STAND_IN],
        capture_output=True,
        text=True,
        check=True,
    )
    pid = int(out.stdout)
    _await(lambda: _parent_pid(pid) == "1", f"process {pid} was never adopted by init")
    return pid


@pytest.fixture
def started():
    pids: list[int] = []
    yield pids
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass  # already ended, which is what the reaping test expects


def test_an_orphaned_harness_server_is_selected_and_can_be_ended(tmp_path, started):
    pid = _orphan(str(tmp_path / "provisa-wboot-abc123"))
    started.append(pid)

    assert pid in _orphaned_harness_processes()
    _end([pid])
    _await(lambda: not _alive(pid), f"orphaned harness process {pid} is still running")


def test_an_orphan_outside_the_harness_is_not_selected(tmp_path, started):
    # The same orphaned process, with a data directory that is not a harness launch's — a server
    # someone else started, such as the maintainer's own instance.
    pid = _orphan(str(tmp_path / "demo"))
    started.append(pid)

    assert pid not in _orphaned_harness_processes()
    assert _alive(pid)


def test_a_harness_server_whose_session_is_alive_is_not_selected(tmp_path, started):
    # Its parent is this test process: a launch a live session still owns.
    proc = subprocess.Popen(  # noqa: S603 - a fixed command line
        _STAND_IN, env=dict(os.environ, PROVISA_DATA_DIR=str(tmp_path / "provisa-wboot-live"))
    )
    started.append(proc.pid)
    try:
        assert proc.pid not in _orphaned_harness_processes()
        assert proc.poll() is None
    finally:
        proc.kill()
        proc.wait(timeout=10)
