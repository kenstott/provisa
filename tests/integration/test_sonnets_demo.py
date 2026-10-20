# Copyright (c) 2026 Kenneth Stott
# Canary: c2062c1b-b88d-4665-ae2a-871b83dc9a97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2016: the named demo ``sonnets`` works as shipped.

Its own docker-compose.yml brings the sonnets' Postgres and an empty Qdrant up on ports of this
test's own, under a compose project of its own; the seeder makes the empty collection. A real
Provisa server (DuckDB engine, its own control plane) loads the demo's config.yaml as is, and
scripts/run_demo_setup.py runs the demo's setup.yaml against it, as start-ui-install.sh does:
every line embedded into Qdrant (the demo's local sentence-transformers model -- the vector
extra), each line's three nearest lines in other sonnets written to the echo table, the five
theme views built. Then the demo's example queries, its tour's statements and its Cypher are run.
"""

# Requirements: REQ-2016

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_vector]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "sonnets"
_ROLE = "org_admin"
_LINES = 2155


class SonnetsStack:
    """The demo's compose stack, on leased ports."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.pg_port, self.qdrant_port = lease_ports(2)
        self.project = f"sonnets-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {
            "SONNETS_PG_PORT": str(self.pg_port),
            "SONNETS_QDRANT_PORT": str(self.qdrant_port),
            **store_namespaces(_DEMO),
        }

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
            self._compose("up", "-d", "--build", "--wait", "postgres", "qdrant")
            self._compose("up", "--exit-code-from", "seeder", "seeder")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the sonnets stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = SonnetsStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("sonnets_demo")
    srv = IsolatedServer(
        "sonnets_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**stack.env, "PROVISA_DATA_DIR": str(home / "provisa")},
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


def _setup(server) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(_DEMO)]
        + ["--url", server.base_url],
        capture_output=True,
        text=True,
        timeout=1800,
    )


@pytest.fixture(scope="module")
def set_up(server):
    done = _setup(server)
    assert done.returncode == 0, f"{done.stdout}\n{done.stderr}"
    return server


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=300
    )


def query(server, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_the_setup_runs_once(set_up):
    again = _setup(set_up)
    assert again.returncode == 0, again.stderr
    assert "already set up" in again.stdout


def test_every_line_is_embedded_and_searched_by_meaning(set_up):
    rows = query(
        set_up,
        'SELECT l.sonnet_number, l.line_no, s.score FROM "sonnets"."lines_like" AS s '
        'JOIN "sonnets"."line" AS l ON l.id = s.id '
        "WHERE s._nf_query = 'But do not so, I love thee in such sort,' AND s._nf_k = 2 "
        "ORDER BY s.score DESC",
    )
    assert [(r["sonnet_number"], r["line_no"]) for r in rows] == [(36, 13), (96, 13)]


def test_each_line_has_three_echoes_in_other_sonnets(set_up):
    [count] = query(set_up, 'SELECT COUNT(*) AS n FROM "sonnets"."echo"')
    assert count["n"] == 3 * _LINES
    top = query(
        set_up,
        'SELECT sonnet, line_no, echo_sonnet, echo_line_no, score FROM "sonnets"."view_echo_lines" '
        "ORDER BY score DESC, sonnet LIMIT 100",
    )
    # An echo is a line of another sonnet (the guard refuses comparing the two lines' sonnets in
    # SQL: it relates two readings of line outside a relationship).
    assert all(r["sonnet"] != r["echo_sonnet"] for r in top)
    assert (top[0]["sonnet"], top[0]["echo_sonnet"]) == (36, 96)
    assert top[0]["score"] > 0.95


@pytest.mark.parametrize("theme", ["time", "beauty", "mortality", "jealousy", "verse"])
def test_each_theme_view_holds_its_search(set_up, theme):
    rows = query(set_up, f'SELECT line_id, score FROM "sonnets"."view_theme_{theme}"')
    assert len(rows) == 20


def test_the_echo_network_is_walked_in_cypher(set_up):
    resp = _post(
        set_up,
        "/data/cypher",
        {
            "query": "MATCH (s:S__Sonnet)<-[:IN]-(a:S__Line)-[e:ECHOES]->(b:S__Line)-[:IN]->"
            "(t:S__Sonnet) WHERE e.score > 0.8 RETURN s.number AS sonnet, t.number AS echoes, "
            "count(e) AS lines ORDER BY lines DESC, sonnet"
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"][0] == {"sonnet": 36, "echoes": 96, "lines": 2}


def test_a_theme_is_walked_in_cypher(set_up):
    resp = _post(
        set_up,
        "/data/cypher",
        {
            "query": "MATCH (l:S__Line)-[:EVOKES]->(t:S__Theme) "
            "RETURN t.name AS theme, count(l) AS lines ORDER BY theme"
        },
    )
    assert resp.status_code == 200, resp.text
    assert {r["theme"]: r["lines"] for r in resp.json()["rows"]} == {
        "Beauty": 20,
        "Jealousy": 20,
        "Mortality": 20,
        "Time": 20,
        "Verse outlives death": 20,
    }


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(set_up, path):
    resp = _post(set_up, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize(
    "step", [s for s in _TOUR if "sql" in s or "cypher" in s], ids=lambda s: s["key"]
)
def test_each_tour_statement_answers_rows(set_up, step):
    if "sql" in step:
        assert query(set_up, step["sql"]), step["key"]
        return
    resp = _post(set_up, "/data/cypher", {"query": step["cypher"]})
    assert resp.status_code == 200, resp.text
    assert resp.json()["rows"], step["key"]


@pytest.fixture(scope="module")
def sandbox_tier(set_up, stack, tmp_path_factory):
    """A sandbox tier offering the demo, on the same stores and the collection the setup filled
    (REQ-2006): a visitor's sandbox has the demo's own embedding model."""
    from tests.integration.isolated_server import IsolatedServer
    from tests.integration.test_demo_sandboxes_e2e import _JWT_SECRET, _deployment_config

    work = tmp_path_factory.mktemp("sonnets_sandbox")
    demos = work / "named"
    demos.mkdir()
    (demos / "sonnets").symlink_to(_DEMO, target_is_directory=True)
    config = work / "config.yaml"
    _deployment_config(config)
    srv = IsolatedServer(
        "sonnets_sandbox",
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            **stack.env,
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_NAMED_DEMOS_DIR": str(demos),
            "PROVISA_DATA_DIR": str(work / "data"),
        },
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


def test_a_visitors_sandbox_searches_lines_by_meaning(sandbox_tier):
    """The demo declares its embedding model; a sandbox of it is not refused by the model
    allowlist, and its search answers as the demo's own does."""
    import time

    deadline = time.monotonic() + 300
    while True:
        opened = httpx.post(f"{sandbox_tier.base_url}/public/demos/sonnets", timeout=300)
        if opened.status_code != 503 or opened.json().get("code") != "sandbox.demo_not_ready":
            break
        assert time.monotonic() < deadline, sandbox_tier.dump_stderr_debug()[-6000:]
        time.sleep(2)
    assert opened.status_code == 200, opened.text
    sandbox = opened.json()
    resp = httpx.post(
        f"{sandbox_tier.base_url}/data/sql",
        json={
            "sql": 'SELECT l.sonnet_number, l.line_no FROM "sonnets"."lines_like" AS s '
            'JOIN "sonnets"."line" AS l ON l.id = s.id '
            "WHERE s._nf_query = 'But do not so, I love thee in such sort,' AND s._nf_k = 2 "
            "ORDER BY s.score DESC"
        },
        headers={"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]},
        timeout=300,
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["data"]["sql"]
    assert [(r["sonnet_number"], r["line_no"]) for r in rows] == [(36, 13), (96, 13)]
