# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2040: the named demo ``audit`` works as shipped.

Its own docker-compose.yml brings the audit firm's Postgres up on a port of this test's own, under
a compose project of its own. A real Provisa server (DuckDB engine, its own control plane) loads
the demo's config.yaml as is: the firm's database read live, and the govdata tables landed from
AskAmerica as TTL -1 replicas on their first read, each through its landing filter.

Credentials
-----------
govdata is a real account (``ASKAMERICA_API_KEY``, a secret of the warehouse lane), so this runs
there. A missing key fails the module by name.
"""

# Requirements: REQ-2040, REQ-2031, REQ-2062

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
from tests.integration.serving_wait import await_tables_serving

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "audit"
_ROLE = "org_admin"
#: The govdata tables, by their alias in the demo.
_GOVDATA = ("filed_awards",)


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the audit demo's govdata tables need a real key "
            "(a secret of the warehouse lane)"
        )
    return key


class AuditStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"audit-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"AUDIT_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

    def _compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "-f",
                str(_DEMO / "docker-compose.yml"),
                *args,
            ],
            env={**os.environ, **self.env},
            capture_output=True,
            text=True,
            check=check,
            timeout=600,
        )

    def up(self) -> None:
        try:
            self._compose("up", "-d", "--wait", "postgres")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the audit stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = AuditStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("audit_demo")
    srv = IsolatedServer(
        "audit_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **stack.env,
            "ASKAMERICA_API_KEY": _api_key(),
            "PROVISA_DATA_DIR": str(home / "provisa"),
        },
    )
    # The govdata server mounts its schemas from object storage before it accepts a connection,
    # which takes minutes on a cold cache.
    srv.start(timeout=900)
    try:
        # Each replica lands on its first read, longer than one request lasts.
        await_tables_serving(srv, _ROLE, [f'"audit"."{table}"' for table in _GOVDATA], timeout=1800)
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict, role: str = _ROLE) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": role}, timeout=900
    )


def query(server, sql: str, role: str = _ROLE) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, role)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_the_filed_awards_land_matched_and_unmatched_lines(server):
    """The Clearinghouse's award lines land beside USAspending's obligations, the lines with no
    USAspending match among them."""
    rows = query(
        server,
        "SELECT matched_usaspending AS matched, COUNT(*) AS lines, MIN(audit_year) AS lo, "
        'MAX(audit_year) AS hi FROM "audit"."filed_awards" GROUP BY matched_usaspending',
    )
    by_match = {r["matched"]: r for r in rows}
    assert set(by_match) == {True, False}, rows
    assert {(r["lo"], r["hi"]) for r in rows} == {(2023, 2023)}, rows
