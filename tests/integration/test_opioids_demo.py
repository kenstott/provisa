# Copyright (c) 2026 Kenneth Stott
# Canary: eeda4cd3-d646-4986-9142-616f71990b59
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2042: the named demo ``opioids`` works as shipped.

Its own docker-compose.yml brings the settlement fund's Postgres up on a port of this test's own,
under a compose project of its own (db/init.sql generates the fund's counties and allocations). A
real Provisa server (DuckDB engine, its own control plane) loads the demo's config.yaml as is: the
fund read live, and the four govdata county tables landed from AskAmerica as TTL -1 replicas on
their first read, each through its landing filter (Ohio's counties, 2023). The answer's view is
read live and built; the public role reads it and none of the fund's allocations; and the demo's
example queries, its tour's statements and its REST call answer rows.

Credentials
-----------
govdata is a real account (``ASKAMERICA_API_KEY``, a secret of the warehouse lane), so this runs
there. A missing key fails the module by name.
"""

# Requirements: REQ-2042, REQ-2031, REQ-2062

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from tests.integration.serving_wait import await_tables_serving

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "opioids"
_ROLE = "org_admin"
#: What db/init.sql generates: the fund's allocations and the counties it pays, Ohio's 88.
_ALLOCATIONS = 1194
_COUNTIES = 88
_VIEW = '"opioids"."view_overdose_burden_by_county"'
#: Each govdata table, its alias in the demo and the column naming the county.
_GOVDATA = {
    "overdose_deaths": "fips",
    "overdose_rates": "geoid",
    "population": "county_fips",
    "federal_spending": "county_fips",
}


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the opioids demo's govdata tables need a real key "
            "(a secret of the warehouse lane)"
        )
    return key


class OpioidsStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"opioids-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"OPIOIDS_PG_PORT": str(self.pg_port)}

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
                f"the opioids stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = OpioidsStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("opioids_demo")
    srv = IsolatedServer(
        "opioids_demo",
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
        # Each govdata table answers before the first read: the server may still be mounting
        # schemas past what one statement waits, and each replica lands on its first read,
        # longer than one request lasts.
        await_tables_serving(
            srv.base_url,
            _ROLE,
            [f'"opioids"."{table}"' for table in _GOVDATA],
            timeout=1800,
        )
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


def test_the_fund_is_read_live(server):
    [row] = query(
        server,
        "SELECT COUNT(*) AS allocations, COUNT(DISTINCT county_fips) AS counties "
        'FROM "opioids"."allocations"',
    )
    assert (row["allocations"], row["counties"]) == (_ALLOCATIONS, _COUNTIES)


@pytest.mark.parametrize(("table", "county"), sorted(_GOVDATA.items()))
def test_each_govdata_table_lands_ohios_counties_alone(server, table, county):
    [row] = query(
        server,
        f"SELECT COUNT(DISTINCT {county}) AS counties, MIN({county}) AS lowest, "
        f'MAX({county}) AS highest FROM "opioids"."{table}"',
    )
    assert row["counties"] == _COUNTIES, table
    assert row["lowest"].startswith("39") and row["highest"].startswith("39"), row


def test_every_county_has_its_residents_and_a_rate_or_cdc_publishes_none(server):
    rows = query(
        server, f"SELECT county_fips, population, deaths, death_rate, rate_modelled FROM {_VIEW}"
    )
    assert len(rows) == _COUNTIES
    assert [r["county_fips"] for r in rows if not r["population"]] == []
    # A suppressed count (1 to 9) has CDC's modelled rate in its place, or no rate at all.
    unexplained = [
        r["county_fips"]
        for r in rows
        if r["deaths"] is None and r["death_rate"] is not None and not r["rate_modelled"]
    ]
    assert unexplained == [], unexplained


def test_the_answer_is_the_same_live_and_built(server):
    sql = f"SELECT county_fips, treatment_spent, death_rate FROM {_VIEW} ORDER BY county_fips"
    live = query(server, sql)
    resp = _post(
        server,
        "/admin/graphql",
        {
            "query": 'mutation { refreshMv(mvId: "view-view_overdose_burden_by_county") '
            "{ success message } }"
        },
    )
    assert resp.json()["data"]["refreshMv"]["success"], resp.text
    assert query(server, sql) == live


def test_the_public_reads_the_dashboard_and_no_allocation(server):
    assert len(query(server, f"SELECT county_name, death_rate FROM {_VIEW}", "public")) == _COUNTIES
    resp = _post(
        server, "/data/sql", {"sql": 'SELECT provider FROM "opioids"."allocations"'}, "public"
    )
    assert resp.status_code != 200, resp.text


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


def _statements_as_run() -> list[tuple[dict, str]]:
    """Each statement of the tour with the role it runs as: a role step picks the role the
    statements after it run as (``all`` is every role, as org_admin)."""
    role, out = _ROLE, []
    for step in _TOUR:
        if "role" in step:
            role = _ROLE if step["role"] == "all" else step["role"]
        if "sql" in step:
            out.append((step, role))
    return out


@pytest.mark.parametrize(
    ("step", "role"), _statements_as_run(), ids=lambda v: v["key"] if isinstance(v, dict) else v
)
def test_each_tour_statement_answers_rows(server, step, role):
    assert query(server, step["sql"], role), step["key"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "api" in s], ids=lambda s: s["key"])
def test_each_tour_rest_call_answers_rows(server, step):
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"], step["key"]
