# Copyright (c) 2026 Kenneth Stott
# Canary: be5aefb1-047c-45bf-bbd3-3d7280b5f906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The isolated-stack identity: one compose project per pytest SESSION.

Two pytest sessions started from the same checkout used to compute the same project name
(``provisa-itest-<slug>-<checkout-hash>``), so they shared one set of containers. Each session
tears its project down before ``up`` and again at ``sessionfinish``, which means the second
session's bring-up SIGTERMed the first session's containers mid-run (observed as 36 setup
ERRORs plus ``dependency failed to start: ... exited (143)``). Host ports were already
per-session ephemeral, so the shared name was the last remaining point of contention.

The name now carries the session's PID, making concurrent sessions — same checkout, different
worktrees, or nested lanes of ``scripts/test-all`` — fully independent. It is exported into the
environment at import time so every child process (xdist workers, ``IsolatedServer``
subprocesses, compose shell-outs) resolves the SAME project as its parent, and an explicit
``PROVISA_ITEST_PROJECT`` from an orchestrator still wins.

A PID-suffixed name cannot be reclaimed by the next run the way a fixed one was, so
:func:`reap_orphaned_projects` removes the projects of sessions that died before their own
teardown ran.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CORE_COMPOSE = os.path.join(_REPO_ROOT, "docker-compose.core.yml")
TEST_COMPOSE = os.path.join(_REPO_ROOT, "docker-compose.test.yml")


def _checkout_prefix() -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", os.path.basename(_REPO_ROOT).lower()).strip("-") or "repo"
    return f"provisa-itest-{slug}-{hashlib.sha1(_REPO_ROOT.encode()).hexdigest()[:6]}"


def session_project(prefix: str, env_var: str) -> str:
    """This session's compose project name for ``prefix``, stamped once and inherited.

    setdefault, not plain assignment: the FIRST process in the session (the pytest controller)
    stamps the name, and everything it spawns — xdist workers, ``IsolatedServer`` subprocesses,
    compose shell-outs — inherits that exact value rather than stamping its own PID and
    provisioning a second stack. An orchestrator that exports ``env_var`` still wins.
    """
    return os.environ.setdefault(env_var, f"{prefix}-{os.getpid()}")


_ITEST_PREFIX = _checkout_prefix()
# The e2e lane provisions a SECOND stack (docker-compose.e2e.yml) alongside the shared one, so it
# needs its own per-session name for the same reason.
_E2E_PREFIX = "provisa-e2e"
_PREFIXES = (_ITEST_PREFIX, _E2E_PREFIX)

ITEST_PROJECT = session_project(_ITEST_PREFIX, "PROVISA_ITEST_PROJECT")
E2E_PROJECT = session_project(_E2E_PREFIX, "PROVISA_E2E_PROJECT")
COMPOSE_ARGS = ["-p", ITEST_PROJECT, "-f", CORE_COMPOSE, "-f", TEST_COMPOSE]


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by another user
    return True


def reap_orphaned_projects() -> None:
    """Tear down this checkout's stacks whose owning pytest session is gone.

    A session killed with SIGKILL (or a machine that lost power) never reaches
    ``pytest_sessionfinish``, and its PID-suffixed project would otherwise sit there holding
    containers and memory forever. Only projects matching one of this scheme's prefixes AND
    carrying a dead PID are removed — a live sibling session is never touched, and neither are
    the fixed-name projects (``provisa``, ``provisa-itest``) that predate this scheme.
    """
    ours = {ITEST_PROJECT, E2E_PROJECT}
    listed = subprocess.run(
        ["docker", "compose", "ls", "--all", "--format", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if listed.returncode != 0 or not listed.stdout.strip():
        return
    for entry in json.loads(listed.stdout):
        name = entry.get("Name", "")
        if name in ours:
            continue
        prefix = next((p for p in _PREFIXES if name.startswith(f"{p}-")), None)
        if prefix is None:
            continue
        suffix = name[len(prefix) + 1 :]
        if not suffix.isdigit() or _pid_alive(int(suffix)):
            continue
        subprocess.run(
            ["docker", "compose", "-p", name, "down", "--volumes", "--remove-orphans"],
            cwd=_REPO_ROOT,
            check=False,
        )
