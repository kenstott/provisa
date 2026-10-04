# Copyright (c) 2026 Kenneth Stott
# Canary: e6bf9c37-7b30-41ab-8d65-8e48c2acf561
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two processes of one deployment boot together on one Trino coordinator, and both come up.

Each registers the coordinator's system catalogs at boot, and registering one drops and recreates
it. Started together, the two once interleaved — one process's CREATE met the other's and it
failed to boot ("Catalog 'provisa_admin' already exists"). The deployment's registration lock
(``trino_system_catalogs.one_registrar``) makes them take turns."""

from __future__ import annotations

import asyncio
import threading
import uuid

import httpx
import pytest

pytestmark = [pytest.mark.integration]

_CONFIG = "tests/fixtures/hot_replication_config.yaml"


def _why(servers, failures) -> str:
    """The boot errors each server logged (its whole stderr, not the harness's tail)."""
    from pathlib import Path

    lines = [
        line
        for server in servers
        for line in Path(server._stderr_file.name).read_text(errors="replace").splitlines()
        if "Error" in line or "ALREADY_EXISTS" in line or "already exists" in line
    ]
    return "\n".join([*map(str, failures), *lines[:40]])


def test_two_processes_started_together_both_come_up():
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    org = f"bootpair_{uuid.uuid4().hex[:8]}"
    kwargs: dict = {"engine": "trino", "config": _CONFIG, "control_plane": "postgres"}
    servers = [IsolatedServer(org, **kwargs), IsolatedServer(org, **kwargs)]
    failures: list[BaseException] = []

    def _boot(server) -> None:
        try:
            server.start()
        except BaseException as exc:  # allow-ble: collected and re-raised as the test's failure
            failures.append(exc)

    threads = [threading.Thread(target=_boot, args=(s,)) for s in servers]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert failures == [], _why(servers, failures)
        for server in servers:
            health = httpx.get(f"{server.base_url}/health", timeout=30.0)
            assert health.status_code == 200, health.text
    finally:
        for server in servers:
            server.stop_process()
        asyncio.run(drop_org_schema(org))
