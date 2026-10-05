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
        # counts; read /health once every worker has registered (bounded), not at the log line.
        deadline = time.monotonic() + 60
        http = boot.health()
        while (http is None or http["workers"]["ready"] < _WORKERS) and time.monotonic() < deadline:
            time.sleep(0.5)
            http = boot.health()
        assert http is not None
        assert http["workers"] == {"ready": _WORKERS, "expected": _WORKERS}
        # Fresh connections, so the action is answered by whichever worker the kernel hands
        # each one to: every answer is the launch's health, as /health's is.
        for _ in range(12):
            assert _flight_health(boot.ports["flight"]) == http
        client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
        try:
            assert [a.type for a in client.list_actions()] == ["healthcheck"]
        finally:
            client.close()
    finally:
        boot.cleanup()
