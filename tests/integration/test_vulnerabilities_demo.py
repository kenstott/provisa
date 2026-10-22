# Copyright (c) 2026 Kenneth Stott
# Canary: 93cbfad2-309a-4d5b-b105-e09807bbbd13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2036: the named demo ``vulnerabilities`` works as shipped.

Its own docker-compose.yml brings the company's Postgres up on a port of this test's own, under a
compose project of its own (db/init.sql generates the repositories, their dependencies and the
Dependabot alerts). A real Provisa server (DuckDB engine, its own control plane) loads the demo's
config.yaml as is: the code read live, and the govdata vulnerability tables landed from AskAmerica
as TTL -1 replicas on their first read -- the NVD's two through their landing filter (REQ-2062),
the KEV and CWE catalogs whole. The answer's view is read live and built, and the demo's example
queries, its tour's statements, its Cypher and its REST call answer rows.

Credentials
-----------
govdata is a real account (``ASKAMERICA_API_KEY``, a secret of the warehouse lane), so this runs
there. A missing key fails the module by name.
"""

# Requirements: REQ-2036, REQ-2062

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

from tests.integration.serving_wait import await_tables_serving

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "vulnerabilities"
_ROLE = "org_admin"
_REPOSITORIES = 24
_DEPENDENCIES = 151
_ALERTS = 46
_VIEW = '"security"."view_exploited_in_our_code"'
# The CVEs the alerts name: what the NVD tables' landing filter keeps (config.yaml).
_ALERT_CVES = {
    "CVE-2017-5638", "CVE-2019-11358", "CVE-2020-14343", "CVE-2020-1938", "CVE-2021-23337",
    "CVE-2021-3129", "CVE-2021-33503", "CVE-2021-44228", "CVE-2021-44906", "CVE-2021-45046",
    "CVE-2022-1471", "CVE-2022-22817", "CVE-2022-22965", "CVE-2022-24785", "CVE-2022-25845",
    "CVE-2022-42889", "CVE-2023-32681", "CVE-2023-44487", "CVE-2023-46604",
}  # fmt: skip


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the vulnerabilities demo's govdata tables need a real "
            "key (a secret of the warehouse lane)"
        )
    return key


class VulnerabilitiesStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"vulnerabilities-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"VULNERABILITIES_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
                f"the vulnerabilities stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = VulnerabilitiesStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("vulnerabilities_demo")
    srv = IsolatedServer(
        "vulnerabilities_demo",
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
                '"security"."cves"',
                '"security"."cve_weaknesses"',
                '"security"."weaknesses"',
                '"security"."kev"',
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


def test_the_code_is_read_live(server):
    [row] = query(
        server,
        'SELECT (SELECT COUNT(*) FROM "security"."repositories") AS repositories, '
        '(SELECT COUNT(*) FROM "security"."dependencies") AS dependencies, '
        '(SELECT COUNT(*) FROM "security"."dependabot_alerts") AS alerts',
    )
    assert (row["repositories"], row["dependencies"], row["alerts"]) == (
        _REPOSITORIES,
        _DEPENDENCIES,
        _ALERTS,
    )


def test_the_nvd_tables_land_only_the_cves_the_alerts_name(server):
    cves = {r["cve_id"] for r in query(server, 'SELECT cve_id FROM "security"."cves"')}
    assert cves == _ALERT_CVES
    weakness_cves = {
        r["cve_id"]
        for r in query(server, 'SELECT DISTINCT cve_id FROM "security"."cve_weaknesses"')
    }
    assert weakness_cves and weakness_cves <= _ALERT_CVES


@pytest.mark.parametrize(("table", "at_least"), [("kev", 1000), ("weaknesses", 900)])
def test_each_catalog_is_landed_whole(server, table, at_least):
    [row] = query(server, f'SELECT COUNT(*) AS n FROM "security"."{table}"')
    assert row["n"] >= at_least, table


def test_the_answer_names_the_exploited_vulnerabilities_left_open(server):
    rows = query(server, f"SELECT repository, cve_id, days_past_due FROM {_VIEW}")
    found = {(r["repository"], r["cve_id"]) for r in rows}
    # Log4Shell, Spring4Shell, the struts console nobody touches and the billing portal's ignition.
    assert {
        ("payments-api", "CVE-2021-44228"),
        ("ledger-service", "CVE-2022-22965"),
        ("legacy-struts-admin", "CVE-2017-5638"),
        ("billing-portal", "CVE-2021-3129"),
    } <= found
    assert all(r["days_past_due"] > 0 for r in rows)


def test_the_answer_is_the_same_live_and_built(server):
    sql = f"SELECT alert_id, repository, cve_id, cisa_due_date FROM {_VIEW} ORDER BY alert_id"
    live = query(server, sql)
    resp = _post(
        server,
        "/admin/graphql",
        {
            "query": 'mutation { refreshMv(mvId: "view-view_exploited_in_our_code") '
            "{ success message } }"
        },
    )
    assert resp.json()["data"]["refreshMv"]["success"], resp.text
    assert query(server, sql) == live
    assert live


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
    "step",
    [s for s in _TOUR if {"sql", "cypher", "api"} & set(s)],
    ids=lambda s: s["key"],
)
def test_each_tour_statement_answers_rows(server, step):
    if "sql" in step:
        assert query(server, step["sql"]), step["key"]
        return
    if "cypher" in step:
        resp = _post(server, "/data/cypher", {"query": step["cypher"]})
        assert resp.status_code == 200, resp.text
        assert resp.json()["rows"], step["key"]
        return
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"], step["key"]
