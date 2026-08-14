# Copyright (c) 2026 Kenneth Stott
# Canary: a92e01b5-f1db-40a2-8b67-4e52fb2d0fb5
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

Isolation is necessary but not sufficient: independent stacks still share ONE Docker VM's
memory, and two full stacks do not fit in the 11.7 GiB VM this suite runs against — the second
session's containers are OOM-killed (exit 137) rather than merely slowed. :func:`acquire_stack_slot`
is the admission gate that turns that into serialization: a session waits for a free slot before
provisioning, and the slot count is derived from the VM's actual memory, so a larger host still
runs sessions concurrently.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import time

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


def _session_trino_etc() -> str:
    """The per-session host directory Trino's mutable config is mounted from.

    ``trino/catalog``, ``trino/kafka`` and ``trino/redis`` are WRITTEN by the running app —
    dynamic catalog ``.properties`` (FileCatalogStore) and the Kafka/Redis table-description
    JSON. Sharing the repo copy across sessions let a previous session's catalog store outlive
    it: with ``catalog.management=dynamic`` Trino loads ``<name>.properties`` at container start,
    so the Kafka catalog existed BEFORE this session's app wrote its description file. The FILE
    supplier had already cached a table with no columns, the connector fell back to topic ==
    table name, and the resulting auto-created topic then blocked the real dotted topic
    (``collides with existing topic``). One empty directory per session removes the carry-over;
    ``PROVISA_TRINO_ETC_DIR`` points the app's writers at the same place the containers mount.
    """
    path = os.path.join(_REPO_ROOT, ".itest-trino", ITEST_PROJECT)
    for sub in ("catalog", "kafka", "redis"):
        os.makedirs(os.path.join(path, sub), exist_ok=True)
    os.environ.setdefault("PROVISA_TRINO_ETC_HOST", path)
    os.environ.setdefault("PROVISA_TRINO_ETC_DIR", path)
    return path


TRINO_ETC_DIR = _session_trino_etc()


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
        # The dead session's Trino config directory goes with its containers.
        shutil.rmtree(os.path.join(_REPO_ROOT, ".itest-trino", name), ignore_errors=True)


# ---------------------------------------------------------------------------
# Cross-session admission control
# ---------------------------------------------------------------------------

# Peak resident memory one pytest session's stack occupies in the Docker VM: the core services
# (postgres, trino, redis, pgbouncer, minio, zaychik) plus the non-heavy marker services
# (kafka/schema-registry/debezium, elasticsearch, neo4j, mongodb, fuseki, …) plus the single
# heavy engine _heavy_db_service keeps live during a heavy-marked test. Measured against the
# 11.7 GiB Docker VM this suite runs on, where a second concurrent session's containers were
# OOM-killed (exit 137) rather than merely slowed — so on that host this budget deliberately
# yields ONE slot and sessions serialize. A 32 GiB CI host gets 5 and stays concurrent.
_SESSION_MEMORY_BUDGET = 6 * 1024**3

# NOT under /tmp: that is cleared on restart, and a slot directory that vanishes mid-run would
# silently un-gate every session holding a slot.
_SLOT_DIR = os.path.join(os.path.expanduser("~"), ".provisa", "test-stack-slots")

_slot_fd: int | None = None


def _docker_vm_memory() -> int:
    """Total memory the Docker VM can hand to containers, in bytes."""
    out = subprocess.run(
        ["docker", "info", "--format", "{{.MemTotal}}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    return int(out)


def concurrent_session_limit() -> int:
    """How many pytest sessions this host's Docker VM can hold stacks for at once."""
    return max(1, _docker_vm_memory() // _SESSION_MEMORY_BUDGET)


def acquire_stack_slot(timeout: float = 7200.0) -> None:
    """Block until this session may provision a Docker stack, then hold the slot until exit.

    Per-session projects and ephemeral ports make concurrent sessions independent of each
    OTHER, but they all draw on the same Docker VM memory. Where that memory cannot hold two
    stacks the sessions must serialize, not merely isolate — otherwise the second bring-up
    OOM-kills containers in both.

    The slots are ``flock``-held files, so a session that dies by SIGKILL — or a machine that
    loses power — releases its slot through the kernel rather than leaving a stale marker for
    the next run to reason about. Idempotent: the itest and e2e stacks both call this, and a
    session that provisions both must not deadlock against its own slot.
    """
    global _slot_fd
    if _slot_fd is not None:
        return
    limit = concurrent_session_limit()
    os.makedirs(_SLOT_DIR, exist_ok=True)
    deadline = time.monotonic() + timeout
    announced = False
    while True:
        for slot in range(limit):
            fd = os.open(os.path.join(_SLOT_DIR, f"slot-{slot}"), os.O_RDWR | os.O_CREAT, 0o644)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)
                continue
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode())
            _slot_fd = fd
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"waited {timeout:.0f}s for one of {limit} Docker stack slot(s) in {_SLOT_DIR}; "
                "another pytest session is still holding every slot"
            )
        if not announced:
            print(
                f"[conftest] {limit} Docker stack slot(s) for this VM's memory are taken; "
                "waiting for a concurrent pytest session to finish"
            )
            announced = True
        time.sleep(5)


def release_stack_slot() -> None:
    """Hand this session's slot to whoever is waiting."""
    global _slot_fd
    if _slot_fd is None:
        return
    fcntl.flock(_slot_fd, fcntl.LOCK_UN)
    os.close(_slot_fd)
    _slot_fd = None
