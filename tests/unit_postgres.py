# Copyright (c) 2026 Kenneth Stott
# Canary: 7277ac38-d7de-418f-a414-f6dce4ae43b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Postgres container a test run starts for itself, and its removal.

A run that needs Postgres and finds none on its port starts one as a compose project of its own.
That project once carried the port in its name and was taken down only by the fixture that started
it, at a clean session end: a run that was killed, or whose starting xdist worker was not the last
to finish, left it up, and three were found alive at once, days old.

The project is named for the run -- the pytest controller's PID, which its xdist workers inherit
-- so the workers of one run share one container and anyone can tell whose it is. The controller
takes it down, with its volume, when its process exits, however it exits: a normal finish, a
failure, Ctrl-C, or a SIGTERM. A SIGKILL cannot be caught, so a run that starts a container first
removes every such project whose owning process is gone. Runs are serialised machine-wide, so
that leaves at most one on the machine."""

from __future__ import annotations

import atexit
import fcntl
import json
import os
import signal
import subprocess
from collections.abc import Callable
from pathlib import Path

PREFIX = "provisa-unitpg"
# This run's project: "provisa-unitpg-pid<controller pid>". Stamped by the first process of the run
# (the controller) and inherited by everything it spawns (setdefault, as tests/itest_stack.py
# names its stacks: workers read the controller's).
PROJECT = os.environ.setdefault("PROVISA_UNITPG_PROJECT", f"{PREFIX}-pid{os.getpid()}")

# The earlier scheme named a project for its port, "provisa-unitpg-<port>", and nothing took it
# down. No run creates one any more, so every project so named is a leftover; the ports were
# leased from this range (tests/port_lease.py LEASE_FIRST..LEASE_LAST).
_OLD_PORTS = range(21100, 25600)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_COMPOSE_FILE = str(_REPO_ROOT / "docker-compose.core.yml")
# Not under /tmp, which a restart clears: a marker that vanished would leave a container unowned.
_STATE_DIR = Path.home() / ".provisa" / "unitpg"


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by another user
    return True


def _marker(project: str) -> Path:
    """Present while ``project`` may be up: written before its container is started."""
    return _STATE_DIR / f"{project}.started"


def orphaned(names: list[str], *, ours: str, alive: Callable[[int], bool]) -> list[str]:
    """The projects among ``names`` that are a run's Postgres with no run behind them.

    ``provisa-unitpg-pid<n>`` is the Postgres of the run whose controller is process ``n``: it is
    one of these when that process is gone. This run's own is never one, and neither is a live
    run's -- a live run's container is never touched. ``provisa-unitpg-<port>``, the earlier
    scheme's name, is always one."""
    gone = []
    for name in names:
        if name == ours or not name.startswith(f"{PREFIX}-"):
            continue
        owner = name[len(PREFIX) + 1 :]
        if owner.startswith("pid") and owner[3:].isdigit():
            if not alive(int(owner[3:])):
                gone.append(name)
        elif owner.isdigit() and int(owner) in _OLD_PORTS:
            gone.append(name)
    return gone


def _down(project: str) -> None:
    subprocess.run(
        ["docker", "compose", "-p", project, "down", "--volumes", "--remove-orphans"],
        cwd=_REPO_ROOT,
        check=False,
        capture_output=True,
    )
    _marker(project).unlink(missing_ok=True)


def sweep() -> None:
    """Remove every run's Postgres whose run is gone."""
    listed = subprocess.run(
        ["docker", "compose", "ls", "--all", "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if listed.returncode != 0 or not listed.stdout.strip():
        return
    names = [entry.get("Name", "") for entry in json.loads(listed.stdout)]
    for name in orphaned(names, ours=PROJECT, alive=_pid_alive):
        _down(name)


def start(reachable: Callable[[], bool]) -> bool:
    """Start this run's Postgres unless ``reachable`` says one already answers. True when this
    call started it. One worker at a time (the others wait and find it answering)."""
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(_STATE_DIR / f"{PROJECT}.lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if reachable():
            return False
        sweep()
        _marker(PROJECT).touch()
        subprocess.run(
            ["docker", "compose", "-p", PROJECT, "-f", _COMPOSE_FILE, "up", "postgres", "-d"],
            check=True,
        )
        return True


def remove() -> None:
    """Take this run's Postgres down with its volume, if this run started one."""
    if _marker(PROJECT).exists():
        _down(PROJECT)
    (_STATE_DIR / f"{PROJECT}.lock").unlink(missing_ok=True)


def remove_when_this_process_exits() -> None:
    """Called in the run's controller: its exit, however it comes, removes the run's Postgres.

    atexit covers a normal finish, a failing run and Ctrl-C. A SIGTERM ends a process without
    atexit, so it is handled: the container is removed and the signal then takes its course
    (the handler that was there before, or the default)."""
    atexit.register(remove)
    previous = signal.getsignal(signal.SIGTERM)

    def _on_sigterm(signum, frame) -> None:
        remove()
        if callable(previous):
            previous(signum, frame)
        else:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(os.getpid(), signum)

    signal.signal(signal.SIGTERM, _on_sigterm)
