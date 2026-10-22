# Copyright (c) 2026 Kenneth Stott
# Canary: b0ab13da-4384-437b-a527-8acf4f9a7c86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2052: the named demo ``profiling`` works as shipped.

Its own docker-compose.yml brings the acquired CRM's Postgres up on a port of this test's own,
under a compose project of its own (db/init.sql generates the customers and orders). A real
Provisa server (DuckDB engine, its own control plane) loads the demo's config.yaml as is and runs
its setup.yaml, as the demo's template build does: the profiler's first run. Its result tables,
named in the config by their member and kind, are registered under the member's seeded id; the
profile finds what the data was generated with; the demo's example queries, its tour's statements
and its REST call answer rows.
"""

# Requirements: REQ-2052

from __future__ import annotations

import importlib.util
import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "profiling"
_ROLE = "org_admin"


def _setup():
    spec = importlib.util.spec_from_file_location(
        "run_demo_setup", _REPO / "scripts" / "run_demo_setup.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProfilingStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"profiling-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"PROFILING_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
                f"the profiling stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = ProfilingStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("profiling_demo")
    srv = IsolatedServer(
        "profiling_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **stack.env,
            "PROVISA_DATA_DIR": str(home / "provisa"),
        },
    )
    srv.start(timeout=600)
    # The demo's setup, as its template build runs it: the profiler's first run (setup.yaml).
    assert _setup().run(_DEMO, srv.base_url) == 0
    try:
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=900
    )


def query(server, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_each_result_table_is_named_by_its_member_s_seeded_id(server):
    resp = _post(server, "/admin/graphql", {"query": "{ tables { id tableName } }"})
    ids = {t["tableName"]: t["id"] for t in resp.json()["data"]["tables"]}
    assert f"customers_{ids['customers']}_profile_columns" in ids
    assert f"orders_{ids['orders']}_profile_runs" in ids


def test_the_profile_finds_what_the_data_holds(server):
    columns = {
        r["column_name"]: r
        for r in query(
            server,
            'SELECT column_name, null_share, min_value, max_value FROM "harbor"."view_customer_columns"',
        )
    }
    assert 0.15 < columns["marketing_opt_in"]["null_share"] < 0.25
    assert columns["signup_date"]["min_value"].startswith("1900-01-01")
    assert float(columns["lifetime_value"]["max_value"]) == 999999.99
    [runs] = query(server, 'SELECT duplicate_rows FROM "harbor"."customer_profile_runs" LIMIT 1')
    assert runs["duplicate_rows"] == 150
    phones = query(
        server, 'SELECT shape FROM "harbor"."view_customer_formats" WHERE column_name = \'phone\''
    )
    assert len(phones) >= 4


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(server, path):
    resp = _post(server, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize(
    "step", [s for s in _TOUR if {"sql", "api"} & set(s)], ids=lambda s: s["key"]
)
def test_each_tour_statement_answers_rows(server, step):
    if "sql" in step:
        assert query(server, step["sql"]), step["key"]
        return
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"], step["key"]
