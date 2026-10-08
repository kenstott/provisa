# Copyright (c) 2026 Kenneth Stott
# Canary: 5590f67d-26ef-41a5-ad00-992e5291b52e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A run's Postgres container is its own, is removed when the run ends, and a run that starts one
first removes those whose run is gone (tests/unit_postgres.py). Three were once alive at once,
days old, and were being removed by hand."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests import unit_postgres

REPO = Path(__file__).resolve().parents[2]


def test_the_project_is_named_for_the_run_and_shared_by_its_workers():
    """Stamped once by the first process and inherited: an xdist worker reads the controller's."""
    assert unit_postgres.PROJECT == os.environ["PROVISA_UNITPG_PROJECT"]
    assert unit_postgres.PROJECT.startswith("provisa-unitpg-pid")
    assert unit_postgres.PROJECT.removeprefix("provisa-unitpg-pid").isdigit()


def test_the_sweep_removes_the_projects_of_runs_that_are_gone_and_no_others():
    alive = {4242, 21143}
    names = [
        "provisa-unitpg-pid4242",  # a live run's: never touched
        "provisa-unitpg-pid777",  # its run is gone
        "provisa-unitpg-21143",  # the earlier scheme named the port: a leftover, whatever
        # process happens to carry that number now
        "provisa-unitpg-pid21143",  # a live run whose pid is in the port range: never touched
        "provisa-unitpg-pid900",  # this run's own
        "provisa-unitpg-5432",  # not a leased port: not the earlier scheme's, left alone
        "provisa-itest-abc-777",  # another harness's project
        "provisa-unitpg-",  # no owner
        "provisa-unitpg-keep",  # neither scheme's
        "provisa",  # the maintainer's own
    ]
    gone = unit_postgres.orphaned(
        names, ours="provisa-unitpg-pid900", alive=lambda pid: pid in alive
    )
    assert gone == ["provisa-unitpg-pid777", "provisa-unitpg-21143"]


def test_the_earlier_schemes_ports_are_the_harnesss_leased_range():
    from tests import port_lease

    assert unit_postgres._OLD_PORTS == range(port_lease.LEASE_FIRST, port_lease.LEASE_LAST + 1)  # noqa: SLF001


def test_a_run_that_started_nothing_removes_nothing(monkeypatch, tmp_path):
    called: list = []
    monkeypatch.setattr(unit_postgres, "_STATE_DIR", tmp_path)
    monkeypatch.setattr(unit_postgres.subprocess, "run", lambda *a, **k: called.append(a))
    unit_postgres.remove()
    assert called == []


def test_a_run_that_started_one_takes_it_down_with_its_volume(monkeypatch, tmp_path):
    called: list = []
    monkeypatch.setattr(unit_postgres, "_STATE_DIR", tmp_path)
    monkeypatch.setattr(unit_postgres.subprocess, "run", lambda args, **k: called.append(args))
    unit_postgres._marker(unit_postgres.PROJECT).touch()  # noqa: SLF001 - as start() leaves it
    unit_postgres.remove()
    assert called == [
        ["docker", "compose", "-p", unit_postgres.PROJECT, "down", "--volumes", "--remove-orphans"]
    ]
    assert not unit_postgres._marker(unit_postgres.PROJECT).exists()  # noqa: SLF001
    unit_postgres.remove()  # once
    assert len(called) == 1


_RUN = textwrap.dedent(
    """
    import os, sys, time
    from pathlib import Path
    sys.path.insert(0, {repo!r})
    os.environ["PROVISA_UNITPG_PROJECT"] = "provisa-unitpg-pid" + str(os.getpid())
    from tests import unit_postgres
    unit_postgres._STATE_DIR = Path({state!r})
    unit_postgres._down = lambda project: Path({state!r}, "removed").write_text(project)
    unit_postgres._marker(unit_postgres.PROJECT).touch()
    unit_postgres.remove_when_this_process_exits()
    print("ready", flush=True)
    {ending}
    """
)


@pytest.mark.parametrize(
    ("ending", "sent"),
    [
        ("sys.exit(0)", None),  # a normal finish
        ("sys.exit(1)", None),  # a failing run
        ("raise KeyboardInterrupt", None),  # Ctrl-C
        ("time.sleep(60)", signal.SIGTERM),  # a supervisor, or the memory killer's SIGTERM
    ],
)
def test_however_the_run_ends_its_postgres_is_removed(tmp_path, ending, sent):
    script = _RUN.format(repo=str(REPO), state=str(tmp_path), ending=ending)
    run = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    assert run.stdout is not None and run.stdout.readline().strip() == "ready"
    if sent is not None:
        run.send_signal(sent)
    run.wait(timeout=30)
    if sent is not None:
        assert run.returncode == -sent  # the signal still ended it, after the removal
    assert (tmp_path / "removed").read_text() == f"provisa-unitpg-pid{run.pid}"
