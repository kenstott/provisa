# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3b5e28-1f94-4c06-a8d7-5e2c9f0b6a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A launch's Flight port answers ``healthcheck`` with what its HTTP ``/health`` reports."""

from __future__ import annotations

import json
import os
import time

import pyarrow.flight as fl
import pytest

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_WORKERS = 2


def _flight_health(port: int) -> dict:
    client = fl.connect(f"grpc://127.0.0.1:{port}")
    try:
        results = list(client.do_action(fl.Action("healthcheck", b"")))
        assert len(results) == 1
        return json.loads(results[0].body.to_pybytes())
    finally:
        client.close()


def _loaded(report: dict) -> bool:
    """Whether the answering worker holds all it stores: a worker is ready, and answers, while it
    is still loading the model (run 40139104347: /health read 970 of 1059 loaded, the action
    then 1059)."""
    return all(part["loaded"] == part["stored"] for part in report["config"].values())


def _beat_free(report: dict) -> dict:
    """The report without each node's ``last_seen``: a node beats every 15 s (HEARTBEAT in
    provisa/core/platform_state/nodes.py), so two reads a beat apart differ there and nowhere
    else (NixOS run 38139824180). Every node still carries the field."""
    nodes = report["nodes"]
    assert all("last_seen" in n for n in nodes), nodes
    return {**report, "nodes": [{k: v for k, v in n.items() if k != "last_seen"} for n in nodes]}


def test_flight_healthcheck_reports_what_http_health_reports():
    boot = WorkerBoot(
        _WORKERS,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        # A worker logs "ready" and then registers in the control plane, which is what /health
        # counts, and writes its node row (its first beat), which is what /health lists; the two
        # land apart (suite 38198316000: two ready, one node). Read /health once every worker has
        # done both (bounded), not at the log line.
        # Each read is a fresh connection, answered by either worker, so every worker has loaded
        # what it stores only once a run of reads, twice the workers long, all say so.
        deadline = time.monotonic() + 60
        settled = 0
        http = boot.health()
        while settled < 2 * _WORKERS and time.monotonic() < deadline:
            ready = (
                http is not None
                and http["workers"]["ready"] == _WORKERS
                and len(http["nodes"]) == _WORKERS
                and _loaded(http)
            )
            settled = settled + 1 if ready else 0
            if settled < 2 * _WORKERS:
                time.sleep(0.5)
                http = boot.health()
        assert http is not None
        assert http["workers"] == {"ready": _WORKERS, "expected": _WORKERS}
        assert len(http["nodes"]) == _WORKERS, http["nodes"]
        assert _loaded(http), http["config"]
        # Fresh connections, so the action is answered by whichever worker the kernel hands
        # each one to: every answer is the launch's health, as /health's is.
        for _ in range(12):
            assert _beat_free(_flight_health(boot.ports["flight"])) == _beat_free(http)
        client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
        try:
            assert [a.type for a in client.list_actions()] == ["healthcheck"]
        finally:
            client.close()
    finally:
        boot.cleanup()
