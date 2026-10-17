# Copyright (c) 2026 Kenneth Stott
# Canary: d38d30e8-959d-4ab3-a211-d47eae705b3e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2039: the named demo ``agriculture`` works as shipped.

Its own docker-compose.yml brings the crop insurer's Postgres up on a port of this test's own, under
a compose project of its own (db/init.sql generates the portfolio). A real Provisa server (DuckDB
engine, its own control plane) loads the demo's config.yaml as is: the portfolio read live, and the
three govdata tables landed from AskAmerica as TTL -1 replicas on their first read, each only the
slice its landing filter names. The answer's view is read live and built; it shows what the demo
claims -- 2012, the season with the least rain, is the portfolio's worst year and the one the USDA
puts most of the market's losses down to drought and heat -- and the demo's example queries, its
tour's statements and its REST call answer rows.

Credentials
-----------
govdata is a real account (``ASKAMERICA_API_KEY``, a secret of the warehouse lane), so this runs
there. A missing key fails the module by name.
"""

# Requirements: REQ-2039

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
_DEMO = _REPO / "demo" / "named" / "agriculture"
_ROLE = "org_admin"
_FARMS = 1200
_COUNTIES = 40
_YEARS = list(range(2010, 2018))
_VIEW = '"agriculture"."view_losses_and_weather"'
_GOVDATA = [
    '"agriculture"."crop_losses"',
    '"agriculture"."daily_weather"',
    '"agriculture"."crop_yields"',
]


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the agriculture demo's govdata tables need a real key "
            "(a secret of the warehouse lane)"
        )
    return key


class AgricultureStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"agriculture-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"AGRICULTURE_PG_PORT": str(self.pg_port)}

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
                f"the agriculture stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = AgricultureStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("agriculture_demo")
    srv = IsolatedServer(
        "agriculture_demo",
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
        # Each govdata table answers before the first read: each replica lands on its first read,
        # longer than one request lasts.
        await_tables_serving(srv, _ROLE, _GOVDATA, timeout=1800)
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


def test_the_portfolio_is_read_live(server):
    [row] = query(
        server,
        "SELECT COUNT(*) AS farms, COUNT(DISTINCT county_fips) AS counties "
        'FROM "agriculture"."farms"',
    )
    assert (row["farms"], row["counties"]) == (_FARMS, _COUNTIES)
    [row] = query(server, 'SELECT COUNT(*) AS n FROM "agriculture"."policies"')
    assert row["n"] == _FARMS * len(_YEARS)


@pytest.mark.parametrize(
    ("table", "at_least"),
    [("crop_losses", 1000), ("daily_weather", 10000), ("crop_yields", 200)],
)
def test_each_govdata_table_is_landed_with_rows(server, table, at_least):
    [row] = query(server, f'SELECT COUNT(*) AS n FROM "agriculture"."{table}"')
    assert row["n"] >= at_least, table


@pytest.mark.parametrize("table", ["crop_losses", "daily_weather", "crop_yields"])
def test_each_govdata_table_holds_only_its_landing_slice(server, table):
    [row] = query(
        server,
        "SELECT MIN(year) AS first, MAX(year) AS last, "
        f'COUNT(DISTINCT county_fips) AS counties FROM "agriculture"."{table}"',
    )
    assert _YEARS[0] <= row["first"] and row["last"] <= _YEARS[-1], row
    assert row["counties"] <= _COUNTIES, row
    if table == "daily_weather":
        months = query(
            server, 'SELECT DISTINCT SUBSTRING(date, 6, 2) AS m FROM "agriculture"."daily_weather"'
        )
        assert {r["m"] for r in months} <= {"05", "06", "07", "08"}


def test_every_county_season_has_its_weather_and_market_losses(server):
    rows = query(
        server, f"SELECT county_fips, year, season_rain_mm, market_loss_ratio FROM {_VIEW}"
    )
    assert len(rows) == _COUNTIES * len(_YEARS)
    no_weather = sorted({r["county_fips"] for r in rows if r["season_rain_mm"] is None})
    no_market = sorted({r["county_fips"] for r in rows if r["market_loss_ratio"] is None})
    # A county with no reporting station some season has no weather that season; most have.
    assert len(no_weather) <= _COUNTIES // 4, f"counties with a season of no weather: {no_weather}"
    assert no_market == [], f"counties with a season of no USDA losses: {no_market}"


def test_2012_is_the_dry_year_the_losses_follow(server):
    rows = query(
        server,
        "SELECT year, SUM(indemnity) / SUM(premium) AS loss_ratio, AVG(season_rain_mm) AS rain, "
        f"AVG(market_dry_share) AS market_dry_share FROM {_VIEW} GROUP BY year",
    )
    by_year = {r["year"]: r for r in rows}
    assert max(by_year, key=lambda y: by_year[y]["loss_ratio"]) == 2012
    assert min(by_year, key=lambda y: by_year[y]["rain"]) == 2012
    assert max(by_year, key=lambda y: by_year[y]["market_dry_share"]) == 2012


def test_the_answer_is_the_same_live_and_built(server):
    sql = f"SELECT county_fips, year, premium, indemnity FROM {_VIEW} ORDER BY county_fips, year"
    live = query(server, sql)
    resp = _post(
        server,
        "/admin/graphql",
        {
            "query": 'mutation { refreshMv(mvId: "view-view_losses_and_weather") '
            "{ success message } }"
        },
    )
    assert resp.json()["data"]["refreshMv"]["success"], resp.text
    assert query(server, sql) == live


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
