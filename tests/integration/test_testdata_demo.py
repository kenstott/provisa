# Copyright (c) 2026 Kenneth Stott
# Canary: 2afe3bfa-4a17-4cee-942c-b036b99481cf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2053, REQ-1942: the named demo ``testdata`` works as shipped.

Its own docker-compose.yml brings the shop's Postgres up on a port of this test's own, under a
compose project of its own, its production data loaded. A real Provisa server (DuckDB engine, its
own control plane) loads the demo's config.yaml as is, and scripts/run_demo_setup.py runs its
setup.yaml against it, as the launcher does: prod is profiled, preprod is created in Test (fake)
and dev in Test (synthetic), generated from prod's profile. Then the same statements are read in
each: prod's real rows; preprod's, every sensitive column faked for every role and the rest real,
so its totals are prod's; dev's, generated, holding no real value, with its generation's report.
"""

# Requirements: REQ-2053, REQ-1942

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "testdata"
_CUSTOMERS = 'SELECT id, name, email, city, region, joined_on FROM "shop"."customers" ORDER BY id'
_SALES = 'SELECT region, category, orders, revenue FROM "shop"."view_sales_by_region"'
_ORDERS_JOINED = (
    'SELECT COUNT(*) AS n FROM "shop"."orders" AS o JOIN "shop"."customers" AS c '
    "ON c.id = o.customer_id"
)


class TestdataStack:
    """The demo's database, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"testdata-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"TESTDATA_PG_PORT": str(self.pg_port)}

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
            timeout=900,
        )

    def up(self) -> None:
        try:
            self._compose("up", "-d", "--wait")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the testdata stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = TestdataStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def set_up(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("testdata_demo")
    srv = IsolatedServer(
        "testdata_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**stack.env, "PROVISA_DATA_DIR": str(home / "provisa")},
    )
    srv.start(timeout=400)
    try:
        done = subprocess.run(
            [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(_DEMO)]
            + ["--url", srv.base_url],
            capture_output=True,
            text=True,
            timeout=1800,
        )
        assert done.returncode == 0, f"{done.stdout}\n{done.stderr}"
        yield srv
    finally:
        srv.stop_process()


def _post(
    server, path: str, body: dict, env: str | None, role: str = "org_admin"
) -> httpx.Response:
    headers = {"X-Provisa-Role": role}
    if env is not None:
        headers["x-provisa-env"] = env
    return httpx.post(f"{server.base_url}{path}", json=body, headers=headers, timeout=300)


def query(
    server, sql: str, env: str | None = None, role: str = "org_admin"
) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, env, role)
    assert resp.status_code == 200, f"{env} {sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _by_value(rows: list[dict[str, Any]], column: str) -> set[str]:
    return {str(r[column]) for r in rows}


def _sorted(rows: list[dict[str, Any]]) -> list[tuple]:
    return sorted(tuple(str(v) for v in r.values()) for r in rows)


def _org(server) -> str:
    me = httpx.get(f"{server.base_url}/auth/me", headers={"X-Provisa-Role": "org_admin"})
    return me.json()["active_org_id"]


def _detail(server, env: str) -> dict[str, Any]:
    resp = httpx.get(
        f"{server.base_url}/admin/orgs/{_org(server)}/environments/{env}/detail",
        headers={"X-Provisa-Role": "org_admin"},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_the_environments_are_prod_preprod_and_dev(set_up):
    listed = httpx.get(
        f"{set_up.base_url}/admin/orgs/{_org(set_up)}/environments",
        headers={"X-Provisa-Role": "org_admin"},
        timeout=60,
    ).json()["environments"]
    modes = {e["name"]: (e["data_mode"], e["data_status"]) for e in listed}
    assert modes["preprod"][0] == "test_fake"
    assert modes["dev"] == ("test_synthetic", "ready")


@pytest.mark.parametrize("role", ["org_admin", "analyst"])
def test_no_real_identity_appears_in_any_preprod_answer(set_up, role):
    real = query(set_up, _CUSTOMERS)
    faked = query(set_up, _CUSTOMERS, "preprod", role)
    assert len(faked) == len(real)
    for column in ("id", "name", "email", "city"):
        assert not _by_value(faked, column) & _by_value(real, column), (role, column)
    # A faked date is drawn from the same span as the real ones, so one may fall on a real day; the
    # dates read are not production's.
    assert sorted(_by_value(faked, "joined_on")) != sorted(_by_value(real, "joined_on")), role
    assert all("@" in r["email"] for r in faked)


def test_preprod_keeps_every_other_value_real(set_up):
    real = query(set_up, _CUSTOMERS)
    faked = query(set_up, _CUSTOMERS, "preprod")
    assert sorted(r["region"] for r in faked) == sorted(r["region"] for r in real)
    assert _sorted(query(set_up, _SALES, "preprod")) == _sorted(query(set_up, _SALES))


@pytest.mark.parametrize("env", [None, "preprod"])
def test_keys_faked_alike_still_join(set_up, env):
    assert query(set_up, _ORDERS_JOINED, env) == [{"n": 120}]


def test_dev_s_generated_keys_line_up(set_up):
    [joined] = query(set_up, _ORDERS_JOINED, "dev")
    [orders] = query(set_up, 'SELECT COUNT(*) AS n FROM "shop"."orders"', "dev")
    assert joined["n"] == orders["n"] > 0, "every generated order finds its generated customer"


def test_dev_is_generated_and_holds_no_real_value(set_up):
    real = query(set_up, _CUSTOMERS)
    generated = query(set_up, _CUSTOMERS, "dev")
    assert generated
    for column in ("name", "email", "city"):
        assert not _by_value(generated, column) & _by_value(real, column), column
    assert _by_value(generated, "region") <= {"North", "South", "East", "West"}


def test_dev_carries_the_report_of_its_generation(set_up):
    synthetic = _detail(set_up, "dev")["test_data"]["synthetic"]
    assert synthetic["status"] == "ready", synthetic
    assert synthetic["report"]["status"] == "ready", synthetic
    resp = httpx.get(
        f"{set_up.base_url}/admin/synthetic-datasets/model/report",
        headers={"X-Provisa-Role": "org_admin", "x-provisa-env": "dev"},
        timeout=60,
    )
    assert resp.status_code == 200 and resp.json(), resp.text


def test_no_sensitive_column_is_left_without_a_fake(set_up):
    assert _detail(set_up, "preprod")["test_data"]["sensitive_without_fake"] == []


@pytest.mark.parametrize("env", [None, "preprod", "dev"])
@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(set_up, env, path):
    resp = _post(set_up, "/data/graphql", {"query": path.read_text()}, env)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


def test_each_tour_statement_answers_rows_in_its_environment(set_up):
    env, role = None, "org_admin"
    for step in yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]:
        if "env" in step:
            env = None if step["env"] == "prod" else step["env"]
        if "role" in step:
            role = "org_admin" if step["role"] == "all" else step["role"]
        if "sql" in step:
            assert query(set_up, step["sql"], env, role), step["key"]
