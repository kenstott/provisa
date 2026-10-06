# Copyright (c) 2026 Kenneth Stott
# Canary: e8f0700f-3d86-4988-9e2c-0c33411e0032
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A branch reads through its base's bindings; a base reads through none (REQ-1491, REQ-1529,
REQ-1538), on a real server: a created environment's sources land unbound, and a statement routed
to a source directly runs in the branch on the connection prod bound."""

# Requirements: REQ-1491, REQ-1529, REQ-1538

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

import pytest

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def boot():
    b = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    b.create_database()
    try:
        b.start()
        b.wait_all_ready(timeout=300)
        yield b
    finally:
        b.cleanup()


def _call(b, method: str, path: str, body: Any = None, env: str | None = None):
    headers = {"Content-Type": "application/json", "x-provisa-role": "org_admin"}
    if env is not None:
        headers["x-provisa-env"] = env
    req = urllib.request.Request(
        f"http://127.0.0.1:{b.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, {"error": exc.read().decode()}


def _environment(b, name: str, inherit: bool) -> None:
    status, body = _call(
        b,
        "POST",
        f"/admin/orgs/{b.org_id}/environments",
        {"name": name, "inherit_connections": inherit},
    )
    assert status == 200, body


def _orders(b, env: str):
    return _call(b, "POST", "/data/sql", {"sql": "SELECT COUNT(*) AS n FROM sales.orders"}, env)


def test_a_branch_reads_the_real_table_through_its_bases_binding(boot):
    before = len(boot.log_text())
    _environment(boot, "branch", inherit=True)
    status, body = _orders(boot, "branch")
    since = boot.log_text()[before:]
    assert status == 200, (body, since[-4000:])
    assert body["data"]["sql"] == [{"n": 2}]
    # The branch's pool dialled prod's coordinates, not the stripped row's.
    assert "direct pool for 'sales-pg'" not in since, since[-4000:]


def test_a_base_reads_through_no_binding_of_prods(boot):
    _environment(boot, "base", inherit=False)
    status, body = _orders(boot, "base")
    assert status != 200, body
