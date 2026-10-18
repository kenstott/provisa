# Copyright (c) 2026 Kenneth Stott
# Canary: 9014970c-8633-4391-8cea-55179ffca987
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the engine's catalogs live in Trino's memory alone, and a restart is visible.

A catalog carries its source's password. On the catalog store the engine runs
(trino/etc/config.properties: catalog.store=memory) Trino writes no ``<name>.properties`` file
anywhere, and a coordinator that restarts comes back without the catalog under a new start id
(``coordinatorId``), which is what the query path compares to reissue it
(provisa/federation/engine_wake.py, reissue_after_coordinator_restart).

The coordinator here is the test's own container, started on the shipped config's catalog lines.
"""

# Requirements: REQ-2073

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_IMAGE = "trinodb/trino:481"
_SECRET_VALUE = "probe-password-7c1e"


def _shipped_catalog_lines() -> str:
    config = (_REPO / "trino" / "etc" / "config.properties").read_text().splitlines()
    return "\n".join(line for line in config if line.startswith("catalog."))


class _Trino:
    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.port = lease_ports(1)[0]
        self.name = f"provisa-itest-memory-catalogs-{os.getpid()}"
        self.etc = Path(tempfile.mkdtemp(prefix="trino-memory-catalogs-"))

    def start(self) -> None:
        (self.etc / "config.properties").write_text(
            "coordinator=true\n"
            "node-scheduler.include-coordinator=true\n"
            "http-server.http.port=8080\n"
            "discovery.uri=http://localhost:8080\n"
            f"{_shipped_catalog_lines()}\n"
        )
        subprocess.run(
            ["docker", "run", "-d", "--name", self.name, "-p", f"127.0.0.1:{self.port}:8080"]
            + ["-v", f"{self.etc / 'config.properties'}:/etc/trino/config.properties:ro", _IMAGE],
            check=True,
            capture_output=True,
        )
        self.wait()

    def wait(self) -> None:
        from provisa.core.catalog import wait_until_ready

        wait_until_ready(self.connect(), 180)

    def connect(self):
        import trino

        return trino.dbapi.connect(host="127.0.0.1", port=self.port, user="provisa")

    def run(self, sql: str) -> list:
        cur = self.connect().cursor()
        cur.execute(sql)
        return cur.fetchall()

    def files_naming(self, text: str) -> str:
        out = subprocess.run(
            ["docker", "exec", self.name, "sh", "-c"]
            + [f"grep -rl {text} /etc/trino /data/trino /tmp 2>/dev/null || true"],
            check=True,
            capture_output=True,
            text=True,
        )
        return out.stdout.strip()

    def restart(self) -> None:
        subprocess.run(["docker", "restart", self.name], check=True, capture_output=True)
        time.sleep(2)
        self.wait()

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name], check=True, capture_output=True)


@pytest.fixture
def coordinator():
    trino_node = _Trino()
    trino_node.start()
    try:
        yield trino_node
    finally:
        trino_node.stop()


def test_the_shipped_engine_keeps_catalogs_in_memory():
    assert "catalog.store=memory" in _shipped_catalog_lines()


def test_a_catalog_is_never_written_to_disk_and_a_restart_drops_it(coordinator):
    from provisa.federation.trino_lifecycle import coordinator_start

    kwargs = {"host": "127.0.0.1", "port": coordinator.port, "http_scheme": "http"}
    before = coordinator_start(kwargs, 60, fresh=True)

    # A catalog whose properties hold a password, as every source catalog's do.
    coordinator.run(
        "CREATE CATALOG probe USING postgresql WITH ("
        "\"connection-url\" = 'jdbc:postgresql://nowhere:5432/db', "
        "\"connection-user\" = 'probe', "
        f"\"connection-password\" = '{_SECRET_VALUE}')"
    )
    assert ["probe"] in coordinator.run("SHOW CATALOGS")
    assert coordinator.files_naming(_SECRET_VALUE) == ""

    coordinator.restart()

    assert ["probe"] not in coordinator.run("SHOW CATALOGS")
    assert coordinator_start(kwargs, 60, fresh=True) != before


def _shipped_worker_catalog_lines() -> str:
    config = (_REPO / "trino" / "etc" / "worker" / "config.properties").read_text().splitlines()
    return "\n".join(line for line in config if line.startswith("catalog."))


def test_a_worker_starts_on_the_shipped_catalog_lines(tmp_path):
    """The coordinator alone stores catalogs. A worker given ``catalog.store`` refused to start
    ("Configuration property 'catalog.store' was not used"), and with no worker the coordinator ran
    no query: the app's boot step timed out under the chart."""
    from tests.port_lease import lease_ports

    config = tmp_path / "config.properties"
    config.write_text(
        "coordinator=false\n"
        "http-server.http.port=8080\n"
        f"discovery.uri=http://127.0.0.1:{lease_ports(1)[0]}\n"
        f"{_shipped_worker_catalog_lines()}\n"
    )
    name = f"provisa-itest-memory-catalogs-worker-{os.getpid()}"
    subprocess.run(
        ["docker", "run", "-d", "--name", name]
        + ["-v", f"{config}:/etc/trino/config.properties:ro", _IMAGE],
        check=True,
        capture_output=True,
    )
    try:
        deadline = time.monotonic() + 180
        logs = ""
        while time.monotonic() < deadline:
            read = subprocess.run(
                ["docker", "logs", name], capture_output=True, text=True, check=True
            )
            logs = read.stdout + read.stderr  # the launcher logs to either
            if "SERVER STARTED" in logs or "Configuration is invalid" in logs:
                break
            time.sleep(2)
        assert "Configuration is invalid" not in logs, logs[-2000:]
        assert "SERVER STARTED" in logs, logs[-2000:]
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=True, capture_output=True)
