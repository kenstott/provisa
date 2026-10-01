# Copyright (c) 2026 Kenneth Stott
# Canary: b2f47a90-5d3c-4e18-96a7-3c1d8e0f6b25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: an error logged by a provisa module reaches a started server's stderr, once.

A real server (DuckDB engine, SQLite control plane and source) is sent a Cypher statement its
engine refuses at run time. ``provisa.api.rest.cypher_router`` reports that on its module logger;
the line must appear on the process's stderr — the log an operator reads — exactly once."""

from __future__ import annotations

import tempfile
from pathlib import Path

import httpx
import pytest
import yaml

from tests.integration.test_cypher_literal_types_e2e import _LABEL, _ROLE, _config, _sqlite_source

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")


@pytest.fixture(scope="module")
def server():
    from tests.integration.isolated_server import IsolatedServer

    work = tempfile.TemporaryDirectory()
    directory = Path(work.name)
    config_path = directory / "config.yaml"
    config_path.write_text(yaml.safe_dump(_config(_sqlite_source(directory), "default")))
    srv = IsolatedServer(
        "server_log_e2e",
        engine="duckdb",
        config=str(config_path),
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{directory / 'materialize.duckdb'}",
    )
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        work.cleanup()


def test_a_module_loggers_error_is_on_the_servers_stderr_exactly_once(server):
    # 'seven' is not a number: the engine refuses the comparison when it runs the statement.
    response = httpx.post(
        f"{server.base_url}/data/cypher",
        json={"query": f"MATCH (t:{_LABEL}) WHERE t.name = 7 RETURN t.name AS name", "params": {}},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 10,
    )
    assert response.status_code in (400, 500), response.text
    reported = [
        line
        for line in server.dump_stderr_debug().splitlines()
        if "Cypher execution" in line and line.startswith(("WARNING:", "ERROR:"))
    ]
    assert len(reported) == 1, reported
