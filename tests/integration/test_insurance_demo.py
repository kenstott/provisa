# Copyright (c) 2026 Kenneth Stott
# Canary: 96519416-fddb-49ee-83d1-ba5591290a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2037: the named demo ``insurance`` works as shipped.

Its own docker-compose.yml brings the insurer's Postgres up on a port of this test's own, under a
compose project of its own (db/init.sql generates the book). A real Provisa server (DuckDB engine,
its own control plane) loads the demo's config.yaml as is: the book read live, and the three
govdata county tables landed from AskAmerica as TTL -1 replicas on their first read. The answer's
view is read live and built, and the demo's example queries, its tour's statements and its REST
call answer rows.

Credentials
-----------
govdata is a real account (``ASKAMERICA_API_KEY``, a secret of the warehouse lane), so this runs
there. A missing key fails the module by name.
"""

# Requirements: REQ-2037, REQ-2031

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
_DEMO = _REPO / "demo" / "named" / "insurance"
_ROLE = "org_admin"
#: What db/init.sql generates: its policies and the counties they are written in.
_POLICIES = 6000
_COUNTIES = 48
_VIEW = '"insurance"."view_exposure_by_county"'


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the insurance demo's govdata tables need a real key "
            "(a secret of the warehouse lane)"
        )
    return key


class InsuranceStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"insurance-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"INSURANCE_PG_PORT": str(self.pg_port)}

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
                f"the insurance stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = InsuranceStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("insurance_demo")
    srv = IsolatedServer(
        "insurance_demo",
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
            srv,
            _ROLE,
            [
                '"insurance"."hazard_risk"',
                '"insurance"."wildfire_risk"',
                '"insurance"."storm_damage"',
            ],
            timeout=1800,
        )
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


def test_the_book_is_read_live(server):
    [row] = query(
        server,
        "SELECT COUNT(*) AS policies, COUNT(DISTINCT county_fips) AS counties "
        'FROM "insurance"."policies"',
    )
    assert (row["policies"], row["counties"]) == (_POLICIES, _COUNTIES)


@pytest.mark.parametrize(
    ("table", "at_least"),
    [("hazard_risk", 3000), ("wildfire_risk", 3000), ("storm_damage", 1000)],
)
def test_each_govdata_table_is_landed_with_rows(server, table, at_least):
    [row] = query(server, f'SELECT COUNT(*) AS n FROM "insurance"."{table}"')
    assert row["n"] >= at_least, table


def test_every_county_of_the_book_has_its_hazard_risk(server):
    rows = query(server, f"SELECT county_fips, risk_rating, wildfire_direct_exposure FROM {_VIEW}")
    assert len(rows) == _COUNTIES
    missing = [r["county_fips"] for r in rows if r["risk_rating"] is None]
    assert missing == [], f"counties of the book with no NRI row: {missing}"


def test_the_answer_is_the_same_live_and_built(server):
    sql = f"SELECT county_fips, policies, tiv, risk_rating FROM {_VIEW} ORDER BY county_fips"
    live = query(server, sql)
    resp = _post(
        server,
        "/admin/graphql",
        {
            "query": 'mutation { refreshMv(mvId: "view-view_exposure_by_county") { success message } }'
        },
    )
    assert resp.json()["data"]["refreshMv"]["success"], resp.text
    assert query(server, sql) == live
    assert sum(r["policies"] for r in live) == _POLICIES


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


@pytest.mark.parametrize("step", [s for s in _TOUR if "sql" in s], ids=lambda s: s["key"])
def test_each_tour_statement_answers_rows(server, step):
    assert query(server, step["sql"]), step["key"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "api" in s], ids=lambda s: s["key"])
def test_each_tour_rest_call_answers_rows(server, step):
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"], step["key"]
