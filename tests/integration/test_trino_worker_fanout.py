# Copyright (c) 2026 Kenneth Stott
# Canary: 9e52314d-12c2-409f-93ac-b1849d577c58
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trino coordinator/worker fan-out integration test (REQ-055).

The e2e Helm test only asserts a worker *pod* exists. This test proves the
stronger property the requirement actually cares about: that when a worker is
present, distributed query execution places tasks on the non-coordinator node
— i.e. work genuinely fans out rather than running entirely on the coordinator.

Owns its own infra: scales the `trino-worker` service (deploy.replicas=0 by
default in docker-compose.core.yml) up to 1 for the module, then back to 0.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest
import trino.dbapi

pytestmark = [pytest.mark.integration]

_REPO_ROOT = Path(__file__).parents[2]
# Operate on the SAME isolated stack the integration lane provisions (conftest's
# provisa-itest project on core+test compose) — TRINO_PORT points at THAT
# coordinator, so the worker must be scaled within THAT project to register.
_ITEST_PROJECT = os.environ.get("PROVISA_ITEST_PROJECT", "provisa-itest")
_COMPOSE_FILES = [
    _REPO_ROOT / "docker-compose.core.yml",
    _REPO_ROOT / "docker-compose.test.yml",
]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


def _compose(*args: str) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-p", _ITEST_PROJECT]
    for f in _COMPOSE_FILES:
        cmd += ["-f", str(f)]
    cmd += list(args)
    return subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True, timeout=300)


def _new_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=_TRINO_HOST,
        port=_TRINO_PORT,
        user="test",
        catalog="sales_pg",
        schema="public",
    )


def _active_worker_count() -> int:
    conn = _new_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM system.runtime.nodes "
            "WHERE coordinator = false AND state = 'active'"
        )
        return cur.fetchone()[0]
    finally:
        conn.close()


@pytest.fixture(scope="module")
def trino_worker():
    """Scale trino-worker to 1 and wait for it to register; scale back to 0 after."""
    if shutil.which("docker") is None:
        pytest.skip("docker not available")

    # --no-deps / --no-recreate: only add the worker, never touch the already-running
    # coordinator (the dev stack treats core services as shared and never-torn-down).
    scale = _compose(
        "up", "-d", "--no-deps", "--no-recreate", "--scale", "trino-worker=1", "trino-worker"
    )
    if scale.returncode != 0:
        pytest.skip(f"could not start trino-worker:\n{scale.stderr}")

    try:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                if _active_worker_count() >= 1:
                    break
            except Exception:
                pass
            time.sleep(3)
        else:
            raise RuntimeError("trino-worker did not register within 180s")
        yield
    finally:
        _compose("up", "-d", "--scale", "trino-worker=0", "trino-worker")


@pytest.mark.usefixtures("trino_worker")
def test_worker_registers_with_coordinator():
    """A non-coordinator node joins the cluster and is active."""
    assert _active_worker_count() >= 1


@pytest.mark.usefixtures("trino_worker")
def test_distributed_query_fans_out_to_worker():
    """A hash-distributed query places at least one task on the worker node.

    A JOIN + GROUP BY forces a hash-partitioned exchange stage, which the
    scheduler fans out across every active node (one task per node in classic
    execution mode). We then confirm via system.runtime.tasks that a task for
    this exact query ran on a non-coordinator node.
    """

    def _worker_tasks_for(conn, query_id: str) -> int:
        # Task records persist for the info-expiry window after completion; poll briefly
        # to absorb the settling race between query completion and task-stat visibility.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            probe = conn.cursor()
            probe.execute(
                """
                SELECT count(*)
                FROM system.runtime.tasks t
                JOIN system.runtime.nodes n ON t.node_id = n.node_id
                WHERE t.query_id = ? AND n.coordinator = false
                """,
                params=(query_id,),
            )
            n = probe.fetchone()[0]
            if n >= 1:
                return n
            time.sleep(2)
        return 0

    conn = _new_conn()
    try:
        cur = conn.cursor()
        # Force a hash-PARTITIONED join (not BROADCAST) so the small join fans out across
        # every node rather than collapsing onto the coordinator.
        cur.execute("SET SESSION join_distribution_type = 'PARTITIONED'")
        cur.fetchall()

        # A single run can still land entirely on the coordinator (tiny inputs) or run
        # before a freshly-scaled worker is schedulable. Re-run the whole query until one
        # run demonstrably places a task on the worker — the property under test is "work
        # CAN fan out", which one fan-out run proves; retrying removes the scheduler/warm-up
        # race without weakening the assertion.
        worker_tasks = 0
        overall_deadline = time.monotonic() + 120
        while time.monotonic() < overall_deadline:
            cur.execute(
                """
                SELECT c.region, count(*)
                FROM sales_pg.public.orders o
                JOIN sales_pg.public.customers c ON o.customer_id = c.id
                GROUP BY c.region
                """
            )
            cur.fetchall()
            query_id = cur.query_id
            assert query_id, "no query_id returned by cursor"
            worker_tasks = _worker_tasks_for(conn, query_id)
            if worker_tasks >= 1:
                break
            time.sleep(3)

        assert worker_tasks >= 1, (
            "no query run placed a task on any worker node within the deadline — "
            "work did not fan out to the coordinator/worker topology"
        )
    finally:
        conn.close()
